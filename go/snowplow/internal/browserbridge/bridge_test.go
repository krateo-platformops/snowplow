//go:build js && wasm

package browserbridge

// Run with GOOS=js GOARCH=wasm and Go's own wasm runner (Node.js):
//
//	GOOS=js GOARCH=wasm go test -exec "$(go env GOROOT)/lib/wasm/go_js_wasm_exec" ./internal/browserbridge
//
// scripts/check_js_wasm.sh does, in CI.

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

// event is one call a recordingSink saw.
type event struct {
	kind   string // head | chunk | end | error
	status int
	text   string
	header [][2]string
}

type recordingSink struct {
	mu     sync.Mutex
	events []event
	notify chan event
}

func newSink() *recordingSink { return &recordingSink{notify: make(chan event, 64)} }

func (s *recordingSink) add(e event) {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
	s.notify <- e
}

func (s *recordingSink) Head(status int, statusText string, header [][2]string) {
	s.add(event{kind: "head", status: status, text: statusText, header: header})
}
func (s *recordingSink) Chunk(b []byte)       { s.add(event{kind: "chunk", text: string(b)}) }
func (s *recordingSink) End()                 { s.add(event{kind: "end"}) }
func (s *recordingSink) Error(message string) { s.add(event{kind: "error", text: message}) }

func (s *recordingSink) next(t *testing.T) event {
	t.Helper()
	select {
	case e := <-s.notify:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("no sink call within 5s")
		return event{}
	}
}

func (s *recordingSink) all() []event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]event(nil), s.events...)
}

// serve starts handler on the in-process network and returns its address.
func serve(t *testing.T, handler http.Handler) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: handler}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return ln.Addr().String()
}

func get(path string, header ...[2]string) Request {
	return Request{Method: http.MethodGet, URL: "https://snowplow.sandbox.invalid" + path, Header: header}
}

// TestForward_EachFlushIsAChunkBeforeTheHandlerReturns: the second write is
// held until the first has reached the sink, so a bridge that buffered the
// body until the handler returned would deadlock here instead of passing.
func TestForward_EachFlushIsAChunkBeforeTheHandlerReturns(t *testing.T) {
	release := make(chan struct{})
	addr := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Snowplow-Refresh-Key", "k1")
		_, _ = io.WriteString(w, "data: one\n\n")
		w.(http.Flusher).Flush()
		<-release
		_, _ = io.WriteString(w, "data: two\n\n")
		w.(http.Flusher).Flush()
	}))
	sink := newSink()
	go Forward(context.Background(), addr, get("/refreshes"), sink)

	head := sink.next(t)
	if head.kind != "head" || head.status != 200 || head.text != "OK" {
		t.Fatalf("first call = %+v, want head 200 OK", head)
	}
	if !hasHeader(head.header, "X-Snowplow-Refresh-Key", "k1") {
		t.Errorf("head headers %v lost X-Snowplow-Refresh-Key", head.header)
	}
	if got := readBody(t, sink, "data: one\n\n"); got != "data: one\n\n" {
		t.Fatalf("first frame = %q before the handler went on", got)
	}
	close(release)
	if got := readBody(t, sink, "data: two\n\n"); got != "data: two\n\n" {
		t.Fatalf("second frame = %q", got)
	}
	if e := sink.next(t); e.kind != "end" {
		t.Fatalf("after the body: %+v, want end", e)
	}
}

// readBody collects chunks until they add up to want's length.
func readBody(t *testing.T, sink *recordingSink, want string) string {
	t.Helper()
	var b strings.Builder
	for b.Len() < len(want) {
		e := sink.next(t)
		if e.kind != "chunk" {
			t.Fatalf("got %+v while reading %q", e, want)
		}
		b.WriteString(e.text)
	}
	return b.String()
}

// TestForward_CancelEndsTheHandlersContextAndReportsNothing: a long-lived
// stream must return when the page stops reading it.
func TestForward_CancelEndsTheHandlersContextAndReportsNothing(t *testing.T) {
	returned := make(chan struct{})
	addr := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(returned)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, ": connected\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	sink := newSink()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { Forward(ctx, addr, get("/refreshes"), sink); close(done) }()

	if e := sink.next(t); e.kind != "head" {
		t.Fatalf("first call = %+v, want head", e)
	}
	readBody(t, sink, ": connected\n\n")
	cancel()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler did not return within 5s of cancel: its request context never ended")
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Forward did not return within 5s of cancel")
	}
	for _, e := range sink.all()[2:] {
		t.Errorf("after cancel the sink got %+v; a cancelled request reports nothing", e)
	}
}

// TestForward_A401IsAnErrorNeverAHead: the page must never see a 401.
func TestForward_A401IsAnErrorNeverAHead(t *testing.T) {
	addr := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"kind":"Status","status":"Failure","message":"user alice: token is expired","reason":"Unauthorized","code":401,"details":{"causes":[{"reason":"TokenExpired"}]}}`)
	}))
	sink := newSink()
	Forward(context.Background(), addr, get("/call?extras=%7B%22username%22%3A%22alice%22%7D"), sink)
	evs := sink.all()
	if len(evs) != 1 || evs[0].kind != "error" {
		t.Fatalf("sink got %+v, want exactly one error and no head", evs)
	}
	msg := evs[0].text
	for _, want := range []string{"401", "TokenExpired", "GET /call"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q does not name %q", msg, want)
		}
	}
	if strings.Contains(msg, "alice") {
		t.Errorf("error %q carries the identity from the query or the message", msg)
	}
}

// TestRelay_A401ReachesTheCallerAsTheServerGaveIt: a Target (the fake
// apiserver) answers snowplow's egress, not the page, so its 401 is relayed
// as a head and body for snowplow to handle as it handles a real server's.
func TestRelay_A401ReachesTheCallerAsTheServerGaveIt(t *testing.T) {
	const status = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}`
	addr := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, status)
	}))
	sink := newSink()
	Relay(context.Background(), addr, "apiserver", get("/api/v1/namespaces"), sink)
	if h := sink.next(t); h.kind != "head" || h.status != http.StatusUnauthorized {
		t.Fatalf("first event %+v, want the 401 head", h)
	}
	if got := readBody(t, sink, status); got != status {
		t.Fatalf("body %q, want %q", got, status)
	}
}

// TestForward_TheRequestArrivesAsThePageSentIt: method, path, query, headers,
// body and the page's host; no header the page did not send.
func TestForward_TheRequestArrivesAsThePageSentIt(t *testing.T) {
	type seen struct {
		method, uri, host, auth, ua, body string
		hasUA                             bool
	}
	got := make(chan seen, 1)
	addr := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		_, hasUA := r.Header["User-Agent"]
		got <- seen{r.Method, r.RequestURI, r.Host, r.Header.Get("Authorization"), r.Header.Get("User-Agent"), string(b), hasUA}
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"metadata":{"uid":"u-1"}}`)
	}))
	sink := newSink()
	Forward(context.Background(), addr, Request{
		Method: http.MethodPost,
		URL:    "https://snowplow.sandbox.invalid/call?resource=x&name=y",
		Header: [][2]string{{"authorization", "Bearer t"}, {"content-type", "application/json"}},
		Body:   []byte(`{"a":1}`),
	}, sink)
	var s seen
	select {
	case s = <-got:
	case <-time.After(5 * time.Second):
		t.Fatalf("the server saw no request within 5s; the sink got %+v", sink.all())
	}
	want := seen{method: "POST", uri: "/call?resource=x&name=y", host: "snowplow.sandbox.invalid", auth: "Bearer t", body: `{"a":1}`}
	if s != want {
		t.Errorf("server saw %+v, want %+v", s, want)
	}
	evs := sink.all()
	if len(evs) < 2 || evs[0].kind != "head" || evs[0].status != 201 || evs[len(evs)-1].kind != "end" {
		t.Fatalf("sink got %+v, want head 201 … end", evs)
	}
}

// TestForward_NoServerIsAnErrorBeforeTheHead: a request the server never
// answered fails like a network failure.
func TestForward_NoServerIsAnErrorBeforeTheHead(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	sink := newSink()
	Forward(context.Background(), addr, get("/health"), sink)
	if evs := sink.all(); len(evs) != 1 || evs[0].kind != "error" {
		t.Fatalf("sink got %+v, want one error", evs)
	}
}

// TestWaitAccepting_ReturnsOnceTheServerListens.
func TestWaitAccepting_ReturnsOnceTheServerListens(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := WaitAccepting(ctx, addr); err == nil {
		t.Fatal("WaitAccepting returned nil with nothing listening")
	}

	srvc := make(chan *http.Server, 1)
	go func() {
		time.Sleep(30 * time.Millisecond)
		srv := &http.Server{Handler: http.NotFoundHandler()}
		srvc <- srv
		ln2, err := net.Listen("tcp", addr)
		if err != nil {
			return
		}
		_ = srv.Serve(ln2)
	}()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	err = WaitAccepting(ctx2, addr)
	_ = (<-srvc).Close()
	if err != nil {
		t.Fatalf("WaitAccepting: %v, want nil once the server listens", err)
	}
}

func hasHeader(h [][2]string, name, value string) bool {
	for _, kv := range h {
		if kv[0] == name && kv[1] == value {
			return true
		}
	}
	return false
}

// L3: once the request is cancelled, nothing reaches the sink, whichever call
// comes next. Each of the four calls is made after the cancel.
func TestCancelGuard_PassesNothingOnceCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	inner := newSink()
	g := cancelGuard{ctx: ctx, sink: inner}

	g.Head(200, "OK", nil)
	g.Chunk([]byte("x"))
	g.End()
	g.Error("e")
	if got := len(inner.all()); got != 4 {
		t.Fatalf("PRE: before the cancel all four calls pass, got %d", got)
	}

	cancel()
	for name, call := range map[string]func(){
		"head":  func() { g.Head(200, "OK", nil) },
		"chunk": func() { g.Chunk([]byte("x")) },
		"end":   func() { g.End() },
		"error": func() { g.Error("e") },
	} {
		before := len(inner.all())
		call()
		if len(inner.all()) != before {
			t.Errorf("%s reached the sink after the cancel", name)
		}
	}
}

// cancellingSink cancels the request when the head arrives: the body, already
// buffered, is then read after the cancel.
type cancellingSink struct {
	*recordingSink
	cancel context.CancelFunc
}

func (s cancellingSink) Head(status int, statusText string, header [][2]string) {
	s.recordingSink.Head(status, statusText, header)
	s.cancel()
}

// L3, through Forward: a request cancelled once its head has been reported
// reports nothing more, not even the chunk that arrived with the head.
func TestForward_NothingAfterACancelThatFollowsTheHead(t *testing.T) {
	addr := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "body that arrives with the head")
	}))
	ctx, cancel := context.WithCancel(context.Background())
	sink := cancellingSink{recordingSink: newSink(), cancel: cancel}
	Forward(ctx, addr, get("/x"), sink)
	evs := sink.all()
	if len(evs) == 0 || evs[0].kind != "head" {
		t.Fatalf("sink got %+v, want the head first", evs)
	}
	for _, e := range evs[1:] {
		t.Errorf("after the cancel the sink got %+v", e)
	}
}

// L2: every value of every header, names sorted, each name's values in order.
func TestHeaderList_SortedWithEveryValue(t *testing.T) {
	h := http.Header{}
	for _, name := range []string{"X-J", "X-C", "X-H", "X-A", "X-F", "X-D", "X-I", "X-B", "X-G", "X-E"} {
		h.Add(name, name+"-1")
	}
	h.Add("X-C", "X-C-2")
	h.Add("X-C", "X-C-0")
	want := [][2]string{
		{"X-A", "X-A-1"}, {"X-B", "X-B-1"}, {"X-C", "X-C-1"}, {"X-C", "X-C-2"}, {"X-C", "X-C-0"},
		{"X-D", "X-D-1"}, {"X-E", "X-E-1"}, {"X-F", "X-F-1"}, {"X-G", "X-G-1"}, {"X-H", "X-H-1"},
		{"X-I", "X-I-1"}, {"X-J", "X-J-1"},
	}
	// Map order varies from walk to walk: ten walks make an unsorted one pass
	// by chance with odds of (1/10!)^10.
	for i := 0; i < 10; i++ {
		if got := headerList(h); !equalHeaders(got, want) {
			t.Fatalf("headerList = %v, want %v", got, want)
		}
	}
}

// L2, through Forward: a header the handler sent twice reaches the page twice.
func TestForward_EveryValueOfAHeaderReachesTheHead(t *testing.T) {
	addr := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Set-Cookie", "a=1")
		w.Header().Add("Set-Cookie", "b=2")
		w.WriteHeader(http.StatusNoContent)
	}))
	sink := newSink()
	Forward(context.Background(), addr, get("/x"), sink)
	evs := sink.all()
	if len(evs) == 0 || evs[0].kind != "head" {
		t.Fatalf("sink got %+v, want a head", evs)
	}
	if !hasHeader(evs[0].header, "Set-Cookie", "a=1") || !hasHeader(evs[0].header, "Set-Cookie", "b=2") {
		t.Fatalf("head headers %v, want both Set-Cookie values", evs[0].header)
	}
}

func equalHeaders(a, b [][2]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// L1: the error names the cause the answer gives. snowplow's own 401s carry a
// reason code from a closed set, with its fixed sentence
// (middleware/credential_status.go); a relayed 401 carries none. Only some of
// the causes are about the token and the JWKS.
func TestForward_A401NamesItsCauseFromTheReasonCode(t *testing.T) {
	const tokenInvalid = `{"kind":"Status","status":"Failure","message":"user alice: bad signature","reason":"Unauthorized","code":401,` +
		`"details":{"causes":[{"reason":"TokenInvalid","message":"the bearer token is not valid; log in again"}]}}`
	const sessionMissing = `{"kind":"Status","status":"Failure","message":"secret alice-clientconfig not found","reason":"Unauthorized","code":401,` +
		`"details":{"causes":[{"reason":"SessionCredentialMissing","message":"the session credential is missing; log in again"}]}}`
	const stepUp = `{"kind":"Status","status":"Failure","message":"secret alice-stepup not found","reason":"Unauthorized","code":401,` +
		`"details":{"causes":[{"reason":"StepUpRequired","message":"the step-up credential is missing or expired; step up again"}]}}`
	const relayed = `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"Unauthorized: user alice","reason":"Unauthorized","code":401}`
	for _, tc := range []struct {
		name, body string
		want       []string
	}{
		{"TokenInvalid", tokenInvalid, []string{"refused GET /call", "TokenInvalid: the bearer token is not valid"}},
		{"SessionCredentialMissing", sessionMissing, []string{"refused GET /call", "SessionCredentialMissing: the session credential is missing"}},
		{"StepUpRequired", stepUp, []string{"refused GET /call", "StepUpRequired: the step-up credential is missing"}},
		{"relayed", relayed, []string{"relayed a 401 to GET /call", "upstream", "no cause"}},
		{"not JSON", "Unauthorized\n", []string{"relayed a 401 to GET /call"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr := serve(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = io.WriteString(w, tc.body)
			}))
			sink := newSink()
			Forward(context.Background(), addr, get("/call?extras=%7B%22username%22%3A%22alice%22%7D"), sink)
			evs := sink.all()
			if len(evs) != 1 || evs[0].kind != "error" {
				t.Fatalf("sink got %+v, want exactly one error and no head", evs)
			}
			msg := evs[0].text
			for _, w := range tc.want {
				if !strings.Contains(msg, w) {
					t.Errorf("error %q does not say %q", msg, w)
				}
			}
			if strings.Contains(msg, "JWKS") {
				t.Errorf("error %q blames the JWKS, which the reason code does not name", msg)
			}
			if strings.Contains(msg, "alice") {
				t.Errorf("error %q carries the identity from the query or the message", msg)
			}
		})
	}
}
