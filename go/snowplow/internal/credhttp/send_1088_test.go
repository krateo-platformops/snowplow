package credhttp

// send_1088_test.go — #1088 arms for the endpoint send policy and the
// per-identity client cache, at the credhttp level. The stage-level arms (one
// connection per identity through the real fetch) are in
// internal/resolvers/restactions/api/endpoint_reuse_1088_test.go.

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
)

// fastRetries keeps plumbing's RetryClient (env-configured) quick.
func fastRetries(t *testing.T, max string) {
	t.Setenv("CLIENT_MAX_RETRIES", max)
	t.Setenv("CLIENT_BASE_BACKOFF", "2ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "4ms")
}

// acceptThenDrop is a server that reads each request WHOLE (headers and body)
// and then closes the connection without answering: the request certainly
// reached the server. It counts the requests it received.
func acceptThenDrop(t *testing.T) (addr string, received func() int) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var n atomic.Int32
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r, err := http.ReadRequest(bufio.NewReader(c))
				if err != nil {
					return
				}
				_, _ = io.ReadAll(r.Body)
				n.Add(1)
			}(c)
		}
	}()
	return ln.Addr().String(), func() int { return int(n.Load()) }
}

func retryCell(site, cause string) uint64 {
	for _, c := range EndpointRetriesSnapshot() {
		if c.Site == site && c.Cause == cause {
			return c.Count
		}
	}
	return 0
}

// ARM 6 (credhttp level): a write is never re-sent once its request may have
// been written; a read is (positive control, so the server really drops).
func TestSend1088_WriteNotResentAfterRequestWritten(t *testing.T) {
	fastRetries(t, "3")
	addr, received := acceptThenDrop(t)
	cases := []struct {
		verb, body string
		want       int
	}{
		{http.MethodPost, "payload", 1},
		{http.MethodPut, "payload", 1},
		{http.MethodPatch, "payload", 1},
		{http.MethodDelete, "payload", 1},
		{http.MethodDelete, "", 1}, // plumbing re-sent this one 5 times
		{http.MethodGet, "", 4},    // a read is retried: 1 + CLIENT_MAX_RETRIES
	}
	for _, tc := range cases {
		before := received()
		var body io.Reader
		if tc.body != "" {
			body = strings.NewReader(tc.body)
		}
		req, _ := http.NewRequest(tc.verb, "http://"+addr+"/x", body)
		_, err := Send(SendSiteStage, &http.Client{}, req)
		if err == nil {
			t.Fatalf("%s: want the dropped connection's error", tc.verb)
		}
		// The server goroutine counts after reading the body; give the last
		// one the chance to land.
		deadline := time.Now().Add(2 * time.Second)
		for received()-before < tc.want && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		time.Sleep(20 * time.Millisecond)
		if got := received() - before; got != tc.want {
			t.Fatalf("%s body=%q: the server received the request %d times, want %d", tc.verb, tc.body, got, tc.want)
		}
	}
}

// ARM 6 at the /call passthrough: credhttp.Do sends a DELETE once.
func TestDo1088_CallPassthroughDeleteSentOnce(t *testing.T) {
	fastRetries(t, "3")
	addr, received := acceptThenDrop(t)
	ep := endpoints.Endpoint{ServerURL: "http://" + addr}
	st := Do(context.Background(), request.RequestOptions{
		RequestInfo: request.RequestInfo{Path: "/api/v1/namespaces/ns/configmaps/x", Verb: ptr.To(http.MethodDelete)},
		Endpoint:    &ep,
	})
	if st == nil || st.Code != http.StatusInternalServerError {
		t.Fatalf("want a 500 envelope for the dropped connection, got %+v", st)
	}
	time.Sleep(50 * time.Millisecond)
	if got := received(); got != 1 {
		t.Fatalf("/call DELETE reached the apiserver %d times, want exactly 1", got)
	}
}

// ARM 5: each retry is counted with the cause of the attempt it replaces, and
// logged once with it.
func TestSend1088_EachRetryCountedWithCause(t *testing.T) {
	fastRetries(t, "2")
	var logs bytes.Buffer
	var logMu sync.Mutex
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithLogger(slog.New(slog.NewJSONHandler(&lockedWriter{w: &logs, mu: &logMu}, nil))))

	t.Run("status_5xx", func(t *testing.T) {
		var n atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if n.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.Write([]byte("{}"))
		}))
		defer srv.Close()
		before, other := retryCell(SendSiteStage, RetryCause5xx), retryCell(SendSiteStage, RetryCauseConnection)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		resp, err := Send(SendSiteStage, &http.Client{}, req)
		if err != nil || resp.StatusCode != 200 {
			t.Fatalf("want the retry's 200, got %v %v", resp, err)
		}
		resp.Body.Close()
		if d := retryCell(SendSiteStage, RetryCause5xx) - before; d != 1 {
			t.Fatalf("status_5xx retries = %d, want 1", d)
		}
		if d := retryCell(SendSiteStage, RetryCauseConnection) - other; d != 0 {
			t.Fatalf("a 5xx retry was also counted as connection (%d)", d)
		}
	})

	t.Run("connection", func(t *testing.T) {
		var n atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if n.Add(1) == 1 {
				c, _, _ := w.(http.Hijacker).Hijack()
				c.Close()
				return
			}
			w.Write([]byte("{}"))
		}))
		defer srv.Close()
		before := retryCell(SendSiteCall, RetryCauseConnection)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
		resp, err := Send(SendSiteCall, &http.Client{}, req)
		if err != nil {
			t.Fatalf("want the retry's answer, got %v", err)
		}
		resp.Body.Close()
		if d := retryCell(SendSiteCall, RetryCauseConnection) - before; d != 1 {
			t.Fatalf("call/connection retries = %d, want 1", d)
		}
	})

	t.Run("dial", func(t *testing.T) {
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		addr := ln.Addr().String()
		ln.Close() // nothing listens: every connect is refused
		before := retryCell(SendSiteStage, RetryCauseDial)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr, nil)
		if _, err := Send(SendSiteStage, &http.Client{}, req); err == nil {
			t.Fatal("want the refused connect's error")
		}
		if d := retryCell(SendSiteStage, RetryCauseDial) - before; d != 2 {
			t.Fatalf("dial retries = %d, want 2 (CLIENT_MAX_RETRIES)", d)
		}
	})

	t.Run("redirect hop is not an attempt", func(t *testing.T) {
		mux := http.NewServeMux()
		mux.HandleFunc("/a", func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, "/b", http.StatusFound) })
		mux.HandleFunc("/b", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("{}")) })
		srv := httptest.NewServer(mux)
		defer srv.Close()
		before := EndpointRetriesSnapshot()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/a", nil)
		resp, err := Send(SendSiteStage, &http.Client{}, req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if after := EndpointRetriesSnapshot(); !reflect.DeepEqual(before, after) {
			t.Fatalf("a followed redirect was counted as a retry: %v -> %v", before, after)
		}
	})

	logMu.Lock()
	out := logs.String()
	logMu.Unlock()
	for _, cause := range []string{RetryCause5xx, RetryCauseConnection, RetryCauseDial} {
		if !strings.Contains(out, `"cause":"`+cause+`"`) {
			t.Fatalf("no retry WARN carries cause %q:\n%s", cause, out)
		}
	}
	if strings.Contains(out, "127.0.0.1") {
		t.Fatalf("a retry WARN carries the host itself, not its digest:\n%s", out)
	}
}

type lockedWriter struct {
	w  io.Writer
	mu *sync.Mutex
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// ARM 7: the key folds every Endpoint field (read by reflection, so a field
// plumbing adds is folded too) and every provenance input.
func TestEndpointKey1088_FoldsEveryFieldAndMark(t *testing.T) {
	base := endpoints.Endpoint{}
	k0 := EndpointKeyOf(&base, "", "")
	v := reflect.ValueOf(&base).Elem()
	for i := 0; i < v.NumField(); i++ {
		ep := base
		f := reflect.ValueOf(&ep).Elem().Field(i)
		switch f.Kind() {
		case reflect.String:
			f.SetString("x")
		case reflect.Bool:
			f.SetBool(true)
		default:
			t.Fatalf("Endpoint.%s has kind %s: give this arm a non-zero value for it", v.Type().Field(i).Name, f.Kind())
		}
		if EndpointKeyOf(&ep, "", "") == k0 {
			t.Fatalf("the key does not fold Endpoint.%s", v.Type().Field(i).Name)
		}
	}
	if EndpointKeyOf(&base, "held", "") == k0 || EndpointKeyOf(&base, "", "/token") == k0 ||
		EndpointKeyOf(&base, "x", "") == EndpointKeyOf(&base, "", "x") {
		t.Fatal("the key does not fold each provenance input in its own position")
	}
	// Field boundaries are length-prefixed: moving bytes between two fields
	// changes the key.
	a := endpoints.Endpoint{Token: "ab", Username: "c"}
	b := endpoints.Endpoint{Token: "a", Username: "bc"}
	if EndpointKeyOf(&a) == EndpointKeyOf(&b) {
		t.Fatal("two endpoints that differ only in a field boundary share a key")
	}
}

// ARM 7: one build per key, a copy per Get, and the idle sweep.
func TestEndpointClients1088_ReuseCopyAndIdleSweep(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	c := NewEndpointClientsForTest(90*time.Second, clock)
	kA := EndpointKeyOf(&endpoints.Endpoint{Token: "a"})
	kB := EndpointKeyOf(&endpoints.Endpoint{Token: "b"})
	base := &http.Transport{}
	builds := 0
	build := func() (*http.Client, bool, error) {
		builds++
		return &http.Client{Transport: base}, true, nil
	}
	// Every Get (the miss that builds, and each hit) hands out a copy: a
	// per-fetch wrapper a caller puts on its copy never reaches the next fetch.
	for i := 0; i < 3; i++ {
		ci, _ := c.Get(kA, build)
		if ci.Transport != base {
			t.Fatalf("Get %d: a caller's change to its copy reached the cached client", i)
		}
		ci.Transport = http.NewFileTransport(http.Dir("/"))
	}
	if builds != 1 {
		t.Fatalf("builds = %d for three Gets of one key, want 1", builds)
	}
	if _, _ = c.Get(kB, build); builds != 2 {
		t.Fatalf("a second identity reused the first's client (builds = %d)", builds)
	}

	// Not-kept builds are handed out and not stored.
	kC := EndpointKeyOf(&endpoints.Endpoint{Token: "c"})
	for i := 0; i < 3; i++ {
		_, _ = c.Get(kC, func() (*http.Client, bool, error) { builds++; return &http.Client{}, false, nil })
	}
	if builds != 5 {
		t.Fatalf("a client that may not be kept was kept (builds = %d, want 5)", builds)
	}

	now = now.Add(60 * time.Second)
	_, _ = c.Get(kB, build) // B used again; A idle 60 s
	now = now.Add(40 * time.Second)
	_, _ = c.Get(kB, build) // A idle 100 s ≥ 90 s: swept; B idle 40 s: kept
	if c.Len() != 1 || c.Evictions() != 1 {
		t.Fatalf("after the sweep: entries = %d, evictions = %d; want 1, 1", c.Len(), c.Evictions())
	}
	_, _ = c.Get(kA, build)
	if builds != 6 {
		t.Fatalf("an evicted identity was not rebuilt (builds = %d)", builds)
	}
}

// R-d: the key fails closed. A field kind the fold cannot read exactly (a
// pointer, an int, an unexported field) makes the type inexact, and an inexact
// key keeps nothing: every Get builds.
func TestEndpointKey1101_FailsClosedOnAnUnfoldableField(t *testing.T) {
	if !foldsExactly(reflect.TypeOf(endpoints.Endpoint{})) || !endpointKeyExact {
		t.Fatal("plumbing's Endpoint is exported strings and bools today: the fold must be exact")
	}
	type withPtr struct {
		A string
		P *string
	}
	type withInt struct {
		A string
		N int
	}
	type withUnexported struct {
		A string
		b string
	}
	for name, typ := range map[string]reflect.Type{"pointer": reflect.TypeOf(withPtr{}), "int": reflect.TypeOf(withInt{}), "unexported": reflect.TypeOf(withUnexported{})} {
		if foldsExactly(typ) {
			t.Fatalf("a struct with a %s field reads as exactly foldable", name)
		}
	}
	prev := endpointKeyExact
	endpointKeyExact = false
	defer func() { endpointKeyExact = prev }()
	c := NewEndpointClientsForTest(time.Hour, time.Now)
	k := EndpointKeyOf(&endpoints.Endpoint{Token: "a"})
	builds := 0
	for i := 0; i < 3; i++ {
		if _, err := c.Get(k, func() (*http.Client, bool, error) { builds++; return &http.Client{}, true, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if builds != 3 || c.Len() != 0 {
		t.Fatalf("an inexact key kept a client: builds = %d, entries = %d; want 3, 0", builds, c.Len())
	}
}

// The idle bound is the transports' own idle timeout, never a number chosen
// here: net/http's DefaultTransport value, which client-go installs.
func TestEndpointClients1088_IdleIsTheTransportIdleTimeout(t *testing.T) {
	want := http.DefaultTransport.(*http.Transport).IdleConnTimeout
	if got := NewEndpointClients().idle; got != want || got == 0 {
		t.Fatalf("idle = %v, want DefaultTransport.IdleConnTimeout %v", got, want)
	}
}

// A write sent once meets Do's non-2xx shaping directly (the RetryClient no
// longer turns its 5xx into a transport error), so a non-2xx answer must be a
// Failure with the status line's code whatever its body: an object body, an
// empty one, and a code response.New does not list.
func TestDo1088_NonOKAnswerIsAlwaysAFailure(t *testing.T) {
	for _, tc := range []struct {
		code int
		body string
	}{
		{http.StatusInternalServerError, `{"kind":"ConfigMap","metadata":{"name":"x"}}`},
		{http.StatusBadGateway, ""},
		{http.StatusGatewayTimeout, "<html>bad gateway</html>"},
		{http.StatusForbidden, `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"forbidden: cm x","reason":"Forbidden","code":403}`},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(tc.code)
			_, _ = w.Write([]byte(tc.body))
		}))
		ep := endpoints.Endpoint{ServerURL: srv.URL}
		st := Do(context.Background(), request.RequestOptions{
			RequestInfo: request.RequestInfo{Path: "/api/v1/namespaces/ns/configmaps/x", Verb: ptr.To(http.MethodPut), Payload: ptr.To(`{"a":1}`)},
			Endpoint:    &ep,
		})
		srv.Close()
		if st.Status != "Failure" || st.Code != tc.code {
			t.Fatalf("%d %q: envelope status=%q code=%d, want Failure %d", tc.code, tc.body, st.Status, st.Code, tc.code)
		}
		if tc.code == http.StatusForbidden && (st.Message != "forbidden: cm x" || st.Reason != "Forbidden") {
			t.Fatalf("a Status body lost its message or reason: %+v", st)
		}
		// A body that names no reason gets plumbing's reason for the code; a
		// code plumbing does not map keeps none (never an invented one).
		wantReason := map[int]string{http.StatusInternalServerError: "InternalError", http.StatusBadGateway: "", http.StatusGatewayTimeout: "", http.StatusForbidden: "Forbidden"}[tc.code]
		if string(st.Reason) != wantReason {
			t.Fatalf("%d: reason %q, want %q (derived from the code)", tc.code, st.Reason, wantReason)
		}
	}
}
