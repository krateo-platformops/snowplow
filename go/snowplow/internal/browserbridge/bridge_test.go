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
