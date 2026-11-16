package credhttp

// leaving_1509_test.go — #1509 (a): a 429 that closes its connection (HTTP/1.x
// Connection: close; HTTP/2 GOAWAY, which Go's server sends for a handler's
// Connection: close exactly as kube-apiserver's shutdown filter sets it) is
// redone at once on a fresh connection; every other 429 still waits its
// Retry-After. Real servers, real transports: the HTTP/2 arms use client-go's
// transport with the owned dialer, as the stage fetch does.

import (
	"encoding/pem"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"k8s.io/client-go/transport"
)

// leavingScript is one answer per request, in order; past the script it
// answers 200. "leave" = 429, Retry-After: retryAfter, Connection: close (the
// apiserver's shutdown answer); "busy" = 429, Retry-After: retryAfter, the
// connection kept.
type leavingScript struct {
	steps      []string
	retryAfter string
	n          atomic.Int32
	conns      atomic.Int32
	open       atomic.Int32
	mu         sync.Mutex
	remotes    []string // the connection (client address) of each request
}

// distinctConns is how many connections the requests arrived on.
func (s *leavingScript) distinctConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]bool{}
	for _, r := range s.remotes {
		seen[r] = true
	}
	return len(seen)
}

func (s *leavingScript) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.remotes = append(s.remotes, r.RemoteAddr)
	s.mu.Unlock()
	i := int(s.n.Add(1)) - 1
	step := "ok"
	if i < len(s.steps) {
		step = s.steps[i]
	}
	switch step {
	case "leave":
		w.Header().Set("Connection", "close")
		fallthrough
	case "busy":
		w.Header().Set("Retry-After", s.retryAfter)
		http.Error(w, "the server is busy or shutting down", http.StatusTooManyRequests)
	default:
		_, _ = w.Write([]byte(`{}`))
	}
}

func (s *leavingScript) connState(_ net.Conn, st http.ConnState) {
	switch st {
	case http.StateNew:
		s.conns.Add(1)
		s.open.Add(1)
	case http.StateClosed, http.StateHijacked:
		s.open.Add(-1)
	}
}

// leavingServer starts s over HTTP/1.1, or over HTTP/2 with TLS (h2 true).
func leavingServer(t *testing.T, s *leavingScript, h2 bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewUnstartedServer(http.HandlerFunc(s.handler))
	srv.Config.ConnState = s.connState
	if h2 {
		srv.EnableHTTP2 = true
		srv.StartTLS()
	} else {
		srv.Start()
	}
	t.Cleanup(srv.Close)
	return srv
}

// h2EndpointClient is a client built the way the stage fetch builds one:
// client-go's transport, TLS to srv, the owned dialer (endpointDial).
func h2EndpointClient(t *testing.T, srv *httptest.Server) *http.Client {
	t.Helper()
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	rt, err := transport.New(&transport.Config{
		TLS:        transport.TLSConfig{CAData: ca},
		Proxy:      http.ProxyFromEnvironment,
		DialHolder: EndpointDialHolder,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &http.Client{Transport: rt}
}

func sendFor1509(t *testing.T, cli *http.Client, url string) (*http.Response, time.Duration) {
	t.Helper()
	seedReserve(t, 50*time.Millisecond)
	start := time.Now()
	resp, err := Send(SendSiteStage, cli, getReq(t, budgetLeft(9*time.Second), url))
	took := time.Since(start)
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp, took
}

// (a), HTTP/1.1: the shutdown answer is redone at once, on a new connection.
func TestIssue1509_A_LeavingAnswerRedoneAtOnce_HTTP1(t *testing.T) {
	s := &leavingScript{steps: []string{"leave"}, retryAfter: "5"}
	srv := leavingServer(t, s, false)
	resp, took := sendFor1509(t, srv.Client(), srv.URL)
	if resp.StatusCode != http.StatusOK || s.n.Load() != 2 || s.distinctConns() != 2 {
		t.Fatalf("status %d, %d requests on %d connections; want 200 after 2 requests on 2 connections", resp.StatusCode, s.n.Load(), s.distinctConns())
	}
	if took >= time.Second {
		t.Fatalf("took %v: the leaving server's Retry-After (5 s) was waited, want the attempt redone at once", took)
	}
}

// (a), HTTP/2: Go's server sends GOAWAY for the handler's Connection: close;
// the client closes that connection once idle, and the attempt is redone at
// once on a new one.
func TestIssue1509_A_LeavingAnswerRedoneAtOnce_HTTP2(t *testing.T) {
	s := &leavingScript{steps: []string{"leave"}, retryAfter: "5"}
	srv := leavingServer(t, s, true)
	resp, took := sendFor1509(t, h2EndpointClient(t, srv), srv.URL)
	if resp.ProtoMajor != 2 {
		t.Fatalf("answered over %s; the arm needs HTTP/2", resp.Proto)
	}
	if resp.StatusCode != http.StatusOK || s.n.Load() != 2 || s.distinctConns() != 2 {
		t.Fatalf("status %d, %d requests on %d connections; want 200 after 2 requests on 2 connections", resp.StatusCode, s.n.Load(), s.distinctConns())
	}
	if took >= time.Second {
		t.Fatalf("took %v: the leaving server's Retry-After (5 s) was waited, want the attempt redone at once", took)
	}
}

// Unchanged: a 429 whose connection stays open waits its Retry-After, on the
// same connection, over HTTP/1.1 and HTTP/2.
func TestIssue1509_A_OrdinaryTooManyRequestsStillWaitsRetryAfter(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		s := &leavingScript{steps: []string{"busy"}, retryAfter: "1"}
		srv := leavingServer(t, s, h2)
		cli := srv.Client()
		if h2 {
			cli = h2EndpointClient(t, srv)
		}
		resp, took := sendFor1509(t, cli, srv.URL)
		if resp.StatusCode != http.StatusOK || s.n.Load() != 2 {
			t.Fatalf("h2=%v: status %d after %d requests; want 200 after 2", h2, resp.StatusCode, s.n.Load())
		}
		if took < 900*time.Millisecond {
			t.Fatalf("h2=%v: took %v; an ordinary 429 must still wait Retry-After (1 s)", h2, took)
		}
		if s.distinctConns() != 1 {
			t.Fatalf("h2=%v: requests on %d connections; an ordinary 429 keeps its connection", h2, s.distinctConns())
		}
	}
}

// The redo happens once: when the fresh attempt answers 429 too, Retry-After
// applies to it, whether that 429 keeps its connection or closes it again.
func TestIssue1509_A_FreshAttemptThatAnswers429WaitsRetryAfter(t *testing.T) {
	for _, second := range []string{"busy", "leave"} {
		for _, h2 := range []bool{false, true} {
			s := &leavingScript{steps: []string{"leave", second}, retryAfter: "1"}
			srv := leavingServer(t, s, h2)
			cli := srv.Client()
			if h2 {
				cli = h2EndpointClient(t, srv)
			}
			resp, took := sendFor1509(t, cli, srv.URL)
			if resp.StatusCode != http.StatusOK || s.n.Load() != 3 {
				t.Fatalf("%s h2=%v: status %d after %d requests; want 200 after 3", second, h2, resp.StatusCode, s.n.Load())
			}
			if took < 900*time.Millisecond {
				t.Fatalf("%s h2=%v: took %v; the fresh attempt's 429 must wait its Retry-After (1 s)", second, h2, took)
			}
		}
	}
}

// A write is still sent once: a leaving server's 429 to a PUT is its answer.
func TestIssue1509_A_WriteIsNotRedone(t *testing.T) {
	s := &leavingScript{steps: []string{"leave"}, retryAfter: "5"}
	srv := leavingServer(t, s, false)
	seedReserve(t, 50*time.Millisecond)
	req, err := http.NewRequestWithContext(budgetLeft(9*time.Second), http.MethodPut, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := Send(SendSiteStage, srv.Client(), req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests || s.n.Load() != 1 {
		t.Fatalf("status %d after %d requests; a write is sent once", resp.StatusCode, s.n.Load())
	}
}

// The redone attempt is reported as a retry, not as an answer within budget.
func TestIssue1509_A_RedoneAttemptIsNotedRetried(t *testing.T) {
	s := &leavingScript{steps: []string{"leave"}, retryAfter: "5"}
	srv := leavingServer(t, s, false)
	seedReserve(t, 50*time.Millisecond)
	ctx := budgetLeft(9 * time.Second)
	resp, err := Send(SendSiteStage, srv.Client(), getReq(t, ctx, srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	b, _ := pagebudget.From(ctx)
	if got := b.Result(); got != pagebudget.Retried {
		t.Fatalf("budget result %v, want %v", got, pagebudget.Retried)
	}
}
