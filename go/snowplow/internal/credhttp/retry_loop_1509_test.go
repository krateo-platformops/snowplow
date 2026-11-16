package credhttp

// retry_loop_1509_test.go — #1509 (a) on the unbudgeted (background) send:
// RetryClient.Do's loop, transcribed (sendRetrying), with its one deviation.
// The leaving-server arms mirror leaving_1509_test.go without a page budget;
// the parity arms pin that the rest of the loop is still plumbing's.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func backgroundSend(t *testing.T, cli *http.Client, url string) (*http.Response, time.Duration, error) {
	t.Helper()
	start := time.Now()
	resp, err := Send(SendSiteStage, cli, getReq(t, context.Background(), url))
	if resp != nil {
		t.Cleanup(func() { _ = resp.Body.Close() })
	}
	return resp, time.Since(start), err
}

// The deviation: a leaving server's 429 is redone at once on a new connection,
// over HTTP/1.1 and HTTP/2.
func TestIssue1509_Background_LeavingAnswerRedoneAtOnce(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		s := &leavingScript{steps: []string{"leave"}, retryAfter: "5"}
		srv := leavingServer(t, s, h2)
		cli := srv.Client()
		if h2 {
			cli = h2EndpointClient(t, srv)
		}
		resp, took, err := backgroundSend(t, cli, srv.URL)
		if err != nil || resp.StatusCode != http.StatusOK || s.n.Load() != 2 || s.distinctConns() != 2 {
			t.Fatalf("h2=%v: err %v, %d requests on %d connections; want 200 after 2 requests on 2 connections", h2, err, s.n.Load(), s.distinctConns())
		}
		if took >= time.Second {
			t.Fatalf("h2=%v: took %v; the leaving server's Retry-After (5 s) was waited", h2, took)
		}
	}
}

// Plumbing's 429 wait, unchanged for every other 429: Retry-After on the same
// connection; and the redo happens once, so a fresh attempt's 429 waits too.
func TestIssue1509_Background_OtherTooManyRequestsWaitRetryAfter(t *testing.T) {
	for _, tc := range []struct {
		steps []string
		reqs  int32
	}{
		{[]string{"busy"}, 2},
		{[]string{"leave", "busy"}, 3},
		{[]string{"leave", "leave"}, 3},
	} {
		for _, h2 := range []bool{false, true} {
			s := &leavingScript{steps: tc.steps, retryAfter: "1"}
			srv := leavingServer(t, s, h2)
			cli := srv.Client()
			if h2 {
				cli = h2EndpointClient(t, srv)
			}
			resp, took, err := backgroundSend(t, cli, srv.URL)
			if err != nil || resp.StatusCode != http.StatusOK || s.n.Load() != tc.reqs {
				t.Fatalf("%v h2=%v: err %v, %d requests; want 200 after %d", tc.steps, h2, err, s.n.Load(), tc.reqs)
			}
			if took < 900*time.Millisecond {
				t.Fatalf("%v h2=%v: took %v; this 429 must wait its Retry-After (1 s)", tc.steps, h2, took)
			}
		}
	}
}

// The redo is not an attempt: it happens even with no retry left.
func TestIssue1509_Background_RedoIsNotAnAttempt(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	s := &leavingScript{steps: []string{"leave"}, retryAfter: "5"}
	srv := leavingServer(t, s, false)
	resp, _, err := backgroundSend(t, srv.Client(), srv.URL)
	if err != nil || resp.StatusCode != http.StatusOK || s.n.Load() != 2 {
		t.Fatalf("err %v, %d requests; want 200 after the redo with CLIENT_MAX_RETRIES=0", err, s.n.Load())
	}
}

// Parity with plumbing's RetryClient.Do: retries run out after
// CLIENT_MAX_RETRIES with plumbing's errors, for 429 and 5xx.
func TestIssue1509_Background_RetriesRunOutAsPlumbings(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "2")
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	for _, tc := range []struct {
		status  int
		wantErr string
	}{
		{http.StatusTooManyRequests, "too many retries after 429 Too Many Requests"},
		{http.StatusServiceUnavailable, "server error after 2 retries: 503"},
	} {
		var n atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			n.Add(1)
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(tc.status)
		}))
		resp, _, err := backgroundSend(t, srv.Client(), srv.URL)
		srv.Close()
		if err == nil || err.Error() != tc.wantErr || resp == nil || resp.StatusCode != tc.status || n.Load() != 3 {
			t.Fatalf("status %d: resp %v, err %v, %d requests; want plumbing's %q after 3 requests", tc.status, resp, err, n.Load(), tc.wantErr)
		}
	}
}

// Parity: a network error is retried with the backoff, and the last error is
// returned when the retries run out.
func TestIssue1509_Background_NetworkErrorRetriedAsPlumbings(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "2")
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		hj, _ := w.(http.Hijacker)
		c, _, _ := hj.Hijack()
		_ = c.Close() // no answer: a network error at the client
	}))
	defer srv.Close()
	resp, _, err := backgroundSend(t, srv.Client(), srv.URL)
	if err == nil || resp != nil || n.Load() != 3 {
		t.Fatalf("resp %v, err %v, %d requests; want the network error after 3 attempts", resp, err, n.Load())
	}
}

// transcribedBackoff is plumbing's backoff(attempt, base, max): doubled per
// attempt, capped, jittered into [d/2, d).
func TestIssue1509_TranscribedBackoffMatchesPlumbings(t *testing.T) {
	base, maxB := 100*time.Millisecond, 350*time.Millisecond
	for attempt, d := range []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 350 * time.Millisecond, 350 * time.Millisecond} {
		for range 50 {
			if got := transcribedBackoff(attempt, base, maxB); got < d/2 || got >= d {
				t.Fatalf("attempt %d: backoff %v, want in [%v, %v)", attempt, got, d/2, d)
			}
		}
	}
}
