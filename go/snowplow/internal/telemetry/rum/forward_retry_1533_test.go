package rum

// forward_retry_1533_test.go — #1533: the forwarder follows the OTLP/HTTP rules for a collector that cannot take a
// batch now (a 429/502/503/504, or no answer for a reason that heals: a refused connection, a disconnect, a
// timeout; the batch is held at the head of the queue and tried again, after the OTel exporter's backoff or the
// collector's Retry-After) and for one that refuses it for good (any other answer, or a transport failure that
// recurs on every attempt: an untrusted certificate, a denied destination; one attempt, forward_failed). The queue
// and the per-attempt timeout stay the only bounds; shutdown counts what it holds as forward_failed. The waits are
// recorded, not slept (Forwarder.sleep), so no arm takes real backoff time (the timeout arm takes one real
// ForwardTimeout); the collector is a real httptest server (or a real closed port).

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
)

// collector1533 is a stand-in OTLP/HTTP collector: every POST counted, answered by `answer` (the POST's number,
// from 1).
type collector1533 struct {
	posts  atomic.Int64
	answer func(n int64, w http.ResponseWriter)
}

func (c *collector1533) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := c.posts.Add(1)
		if c.answer == nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		c.answer(n, w)
	})
}

// waits1533 records the forwarder's waits instead of sleeping them; onWait runs on each (the simulated time so far).
type waits1533 struct {
	mu     sync.Mutex
	got    []time.Duration
	total  time.Duration
	onWait func(total time.Duration)
}

func (w *waits1533) sleep(ctx context.Context, d time.Duration) bool {
	w.mu.Lock()
	w.got = append(w.got, d)
	w.total += d
	total, hook := w.total, w.onWait
	w.mu.Unlock()
	if hook != nil {
		hook(total)
	}
	return ctx.Err() == nil
}

func (w *waits1533) list() []time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]time.Duration(nil), w.got...)
}

func spansDelta(before Snapshot, key string) uint64 { return Read().Spans[key] - before.Spans[key] }

// until polls cond for up to 5 s.
func until(t *testing.T, what string, cond func() bool) {
	t.Helper()
	untilWithin(t, what, 5*time.Second, cond)
}

// untilWithin polls cond for up to d.
func untilWithin(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %v", what, d)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func startForwarder(t *testing.T, f *Forwarder) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f.Start(ctx)
	return cancel
}

// TestForward1533_AHealthyCollectorCostsOnePostPerBatch: with a collector that takes every batch, each batch is one
// POST and no wait: the retry path costs nothing when nothing is wrong.
func TestForward1533_AHealthyCollectorCostsOnePostPerBatch(t *testing.T) {
	c := &collector1533{}
	srv := httptest.NewServer(c.handler())
	defer srv.Close()
	f, w := NewForwarder(srv.URL+"/v1/traces"), &waits1533{}
	f.sleep = w.sleep
	before := Read()
	startForwarder(t, f)
	for i := 0; i < 20; i++ {
		if !f.enqueue(batch{body: []byte("spans"), spans: 2}) {
			t.Fatalf("batch %d: the queue refused it", i)
		}
	}
	until(t, "40 spans forwarded", func() bool { return spansDelta(before, "forwarded") == 40 })
	if p := c.posts.Load(); p != 20 {
		t.Fatalf("20 batches made %d POSTs, want exactly 1 each", p)
	}
	if got := w.list(); len(got) != 0 || spansDelta(before, "forward_failed") != 0 {
		t.Fatalf("a healthy collector: waits %v, forward_failed %d, want none", got, spansDelta(before, "forward_failed"))
	}
}

// TestForward1533_ACollectorRefusingConnectionsThenUpLosesNothing is the daemonset roll (#1533, 057), held long
// enough to reach the backoff's cap (#1535 review): the node-local collector's port is closed (connections refused)
// for 150 s of simulated backoff, then it serves. The five batches accepted meanwhile are all forwarded,
// forward_failed and forward_overflow 0, one POST each once it is up; the waits follow the exporter's backoff, 5 s
// ±50 %, then x1.5 (5, 7.5, 11.25, 16.875, 25.3), capped at 30 s from the 6th wait on. 150 s is at least 7 waits
// whatever the jitter (all at +50 %: 7.5+11.25+16.9+25.3+38+45 = 143.8 s < 150 s), so every run has the cap reached
// (wait 6) and held (wait 7). The outage runs 30 times: at the cap a wait is 30 s ±50 %, inside [15 s, 45 s]
// always, while an uncapped 6th wait (38 s nominal) is above 45 s about a third of the time and a 7th (57 s) more
// than two thirds, so a cap raised or dropped is caught with certainty for practical purposes (mutations: dropping
// the batch on the first failure, as before, reads forward_failed; the cap at 300 s reads a wait above 45 s).
func TestForward1533_ACollectorRefusingConnectionsThenUpLosesNothing(t *testing.T) {
	for run := 0; run < 30; run++ {
		got := refusedThenUp1533(t, 150*time.Second)
		if len(got) < 7 {
			t.Fatalf("run %d: %d waits in a 150 s outage, want at least 7 (all: %v)", run, len(got), got)
		}
		// the exporter's documented shape as literals (otlptracehttp v1.47.0 internal/retry, cenkalti/backoff
		// v5.0.3), so a change to the forwarder's constants cannot loosen this arm with them
		nominal := 5 * time.Second
		for i, d := range got {
			if d < nominal/2 || d > nominal*3/2 {
				t.Fatalf("run %d: wait %d is %v, want the exporter's backoff %v ±50 %% (all: %v)", run, i, d, nominal, got)
			}
			if nominal = time.Duration(float64(nominal) * 1.5); nominal > 30*time.Second {
				nominal = 30 * time.Second
			}
		}
	}
}

// refusedThenUp1533 holds five batches through a collector refusing connections for `outage` of simulated backoff,
// then serving; it checks nothing was lost and returns the waits.
func refusedThenUp1533(t *testing.T, outage time.Duration) []time.Duration {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // the port is closed: a connection is refused
	c := &collector1533{}
	var srv *httptest.Server
	var up sync.Once
	defer func() {
		if srv != nil {
			srv.Close()
		}
	}()
	f, w := NewForwarder("http://"+addr+"/v1/traces"), &waits1533{}
	w.onWait = func(total time.Duration) {
		if total >= outage {
			up.Do(func() {
				l, err := net.Listen("tcp", addr)
				if err != nil {
					t.Errorf("reopen %s: %v", addr, err)
					return
				}
				srv = httptest.NewUnstartedServer(c.handler())
				_ = srv.Listener.Close()
				srv.Listener = l
				srv.Start()
			})
		}
	}
	f.sleep = w.sleep
	before := Read()
	for i := 0; i < 5; i++ {
		f.enqueue(batch{body: []byte("spans"), spans: 3})
	}
	cancel := startForwarder(t, f)
	defer cancel()
	until(t, "15 spans forwarded", func() bool { return spansDelta(before, "forwarded") == 15 })
	if spansDelta(before, "forward_failed") != 0 || spansDelta(before, "forward_overflow") != 0 {
		t.Fatalf("forward_failed %d, forward_overflow %d, want 0", spansDelta(before, "forward_failed"),
			spansDelta(before, "forward_overflow"))
	}
	if p := c.posts.Load(); p != 5 {
		t.Fatalf("the collector took %d POSTs once up, want 5", p)
	}
	return w.list()
}

func scripted(t *testing.T, answer func(n int64, w http.ResponseWriter)) (*Forwarder, *collector1533, *waits1533) {
	t.Helper()
	c := &collector1533{answer: answer}
	srv := httptest.NewServer(c.handler())
	t.Cleanup(srv.Close)
	f, w := NewForwarder(srv.URL+"/v1/traces"), &waits1533{}
	f.sleep = w.sleep
	return f, c, w
}

// TestForward1533_ATransientAnswerIsRetriedUntilTaken: 503 three times, then 200: the batch is forwarded on the 4th
// POST, nothing failed (mutation: a 503 not retried reads forward_failed 4).
func TestForward1533_ATransientAnswerIsRetriedUntilTaken(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable,
		http.StatusGatewayTimeout} {
		f, c, w := scripted(t, func(n int64, rw http.ResponseWriter) {
			if n <= 3 {
				rw.WriteHeader(status)
				return
			}
			rw.WriteHeader(http.StatusOK)
		})
		before := Read()
		startForwarder(t, f)
		f.enqueue(batch{body: []byte("spans"), spans: 4})
		until(t, fmt.Sprintf("%d: 4 spans forwarded", status), func() bool { return spansDelta(before, "forwarded") == 4 })
		if c.posts.Load() != 4 || len(w.list()) != 3 || spansDelta(before, "forward_failed") != 0 {
			t.Fatalf("%d: %d POSTs, %d waits, forward_failed %d; want 4, 3, 0", status, c.posts.Load(), len(w.list()),
				spansDelta(before, "forward_failed"))
		}
	}
}

// TestForward1533_APermanentRefusalIsOneAttempt: a 400, and a 500 (the collector judging the payload itself: not in
// the OTLP/HTTP retryable set), are each one POST and forward_failed, no wait (mutation: a 400 retried).
func TestForward1533_APermanentRefusalIsOneAttempt(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusInternalServerError, http.StatusUnauthorized} {
		f, c, w := scripted(t, func(_ int64, rw http.ResponseWriter) { rw.WriteHeader(status) })
		before := Read()
		startForwarder(t, f)
		f.enqueue(batch{body: []byte("spans"), spans: 5})
		until(t, fmt.Sprintf("%d: 5 spans failed", status), func() bool { return spansDelta(before, "forward_failed") == 5 })
		time.Sleep(50 * time.Millisecond) // a retry, were there one, would have been made by now
		if c.posts.Load() != 1 || len(w.list()) != 0 {
			t.Fatalf("%d: %d POSTs, waits %v; want exactly 1 attempt and no wait", status, c.posts.Load(), w.list())
		}
	}
}

// countingListener counts the connections a server accepts.
type countingListener struct {
	net.Listener
	accepted atomic.Int64
}

func (l *countingListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
	}
	return c, err
}

// TestForward1533_AnUntrustedCertificateIsOneAttempt (#1535 review): a collector whose certificate the client does
// not trust fails the same way on every attempt, so the batch is one connection and forward_failed, no wait, and the
// worker moves on: the next batch, sent to a collector it can reach (the address is changed before that batch is
// handed to the worker), is delivered (mutation: every transport failure retried reads a wait, and the next batch
// stuck behind the first).
func TestForward1533_AnUntrustedCertificateIsOneAttempt(t *testing.T) {
	c := &collector1533{}
	srv := httptest.NewUnstartedServer(c.handler())
	cl := &countingListener{Listener: srv.Listener}
	srv.Listener = cl
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // the server's own "TLS handshake error" line
	srv.StartTLS()
	defer srv.Close()
	f, w := NewForwarder(srv.URL+"/v1/traces"), &waits1533{}
	f.sleep = w.sleep
	// the arm's site: this client's failure against this server is a certificate verification failure
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/traces", bytes.NewReader([]byte("x")))
	if resp, err := f.client.Do(req); !errors.As(err, new(*tls.CertificateVerificationError)) {
		if resp != nil {
			_ = resp.Body.Close()
		}
		t.Fatalf("the client against an untrusted certificate: %v, want a *tls.CertificateVerificationError", err)
	}
	until(t, "the probe connection counted", func() bool { return cl.accepted.Load() == 1 })
	before := Read()
	startForwarder(t, f)
	f.enqueue(batch{body: []byte("spans"), spans: 6})
	until(t, "6 spans failed", func() bool { return spansDelta(before, "forward_failed") == 6 })
	time.Sleep(50 * time.Millisecond) // a retry, were there one, would have been made by now
	if n := cl.accepted.Load() - 1; n != 1 || len(w.list()) != 0 || c.posts.Load() != 0 {
		t.Fatalf("untrusted certificate: %d connections, waits %v, %d POSTs taken; want 1, none, 0", n, w.list(),
			c.posts.Load())
	}
	good := &collector1533{}
	gsrv := httptest.NewServer(good.handler())
	defer gsrv.Close()
	f.url = gsrv.URL + "/v1/traces" // before the enqueue that hands the next batch to the worker
	f.enqueue(batch{body: []byte("spans"), spans: 2})
	until(t, "the next batch forwarded", func() bool { return spansDelta(before, "forwarded") == 2 })
	if good.posts.Load() != 1 || spansDelta(before, "forward_failed") != 6 {
		t.Fatalf("the next batch: %d POSTs, forward_failed %d; want 1, 6", good.posts.Load(),
			spansDelta(before, "forward_failed"))
	}
}

// TestForward1533_ADeniedDestinationIsOneAttempt (#1535 review): an address the dial policy denies (the metadata
// address) is denied the same way on every attempt: one attempt, forward_failed, no wait (mutation: every transport
// failure retried reads a wait).
func TestForward1533_ADeniedDestinationIsOneAttempt(t *testing.T) {
	url := "http://169.254.169.254/v1/traces"
	f, w := NewForwarder(url), &waits1533{}
	f.sleep = w.sleep
	// the arm's site: this client's failure is the destination policy's refusal, made before any packet is sent
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader([]byte("x")))
	if resp, err := f.client.Do(req); !credhttp.DestinationRefused(err) {
		if resp != nil {
			_ = resp.Body.Close()
		}
		t.Fatalf("the client to %s: %v, want the destination policy's refusal", url, err)
	}
	before := Read()
	startForwarder(t, f)
	f.enqueue(batch{body: []byte("spans"), spans: 7})
	until(t, "7 spans failed", func() bool { return spansDelta(before, "forward_failed") == 7 })
	time.Sleep(50 * time.Millisecond) // a retry, were there one, would have been made by now
	if got := w.list(); len(got) != 0 {
		t.Fatalf("a denied destination: waits %v, want none (exactly 1 attempt)", got)
	}
}

// TestForward1533_ADisconnectWithoutAnAnswerIsRetried: the collector drops the connection on the first POST without
// a whole answer, then answers 200. OTLP/HTTP: "If the server disconnects without returning a response, the client
// SHOULD retry and send the same request." Each way of going away is two POSTs, one wait, forwarded: closed
// without a byte (the client reads EOF), reset (SO_LINGER 0: ECONNRESET, a collector process killed), cut in the
// middle of the headers (an unexpected EOF) (mutations: each of those not transient reads forward_failed).
func TestForward1533_ADisconnectWithoutAnAnswerIsRetried(t *testing.T) {
	for _, tc := range []struct {
		name string
		drop func(conn net.Conn)
	}{
		{"closed", func(conn net.Conn) { _ = conn.Close() }},
		{"reset", func(conn net.Conn) {
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.SetLinger(0)
			}
			_ = conn.Close()
		}},
		{"cut mid-answer", func(conn net.Conn) {
			_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n"))
			_ = conn.Close()
		}},
	} {
		f, c, w := scripted(t, func(n int64, rw http.ResponseWriter) {
			if n == 1 {
				conn, _, err := rw.(http.Hijacker).Hijack()
				if err != nil {
					t.Errorf("%s: hijack: %v", tc.name, err)
					return
				}
				tc.drop(conn)
				return
			}
			rw.WriteHeader(http.StatusOK)
		})
		before := Read()
		cancel := startForwarder(t, f)
		f.enqueue(batch{body: []byte("spans"), spans: 8})
		until(t, tc.name+": 8 spans forwarded", func() bool { return spansDelta(before, "forwarded") == 8 })
		if c.posts.Load() != 2 || len(w.list()) != 1 || spansDelta(before, "forward_failed") != 0 {
			t.Fatalf("%s: %d POSTs, waits %v, forward_failed %d; want 2, 1 wait, 0", tc.name, c.posts.Load(), w.list(),
				spansDelta(before, "forward_failed"))
		}
		cancel()
	}
}

// TestForward1533_AStatusLineCutBeforeItsCodeIsOneAttempt pins today's limitation (transientTransport): a collector
// that disconnects in the middle of the status line ("HTTP/1.1 20") is, to Go's client, a malformed answer
// (`malformed HTTP status code`, no EOF in the error's chain), so the batch is one attempt and forward_failed, no
// wait, although the specification would retry a disconnect without a response. A change that retries it changes
// this arm on purpose.
func TestForward1533_AStatusLineCutBeforeItsCodeIsOneAttempt(t *testing.T) {
	f, c, w := scripted(t, func(_ int64, rw http.ResponseWriter) {
		conn, _, err := rw.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_, _ = conn.Write([]byte("HTTP/1.1 20"))
		_ = conn.Close()
	})
	before := Read()
	startForwarder(t, f)
	f.enqueue(batch{body: []byte("spans"), spans: 10})
	until(t, "10 spans failed", func() bool { return spansDelta(before, "forward_failed") == 10 })
	time.Sleep(50 * time.Millisecond) // a retry, were there one, would have been made by now
	if c.posts.Load() != 1 || len(w.list()) != 0 {
		t.Fatalf("a status line cut before its code: %d POSTs, waits %v; want exactly 1 attempt and no wait",
			c.posts.Load(), w.list())
	}
}

// TestForward1533_AnAttemptTimingOutIsRetried: the collector holds the first POST past ForwardTimeout (real time,
// 5 s), then answers the next at once: the timed-out attempt is retried, two POSTs, one wait, forwarded (mutation: a
// timeout not transient reads forward_failed).
func TestForward1533_AnAttemptTimingOutIsRetried(t *testing.T) {
	var posts atomic.Int64
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		if posts.Add(1) == 1 {
			select {
			case <-release: // the arm is over
			case <-time.After(3 * ForwardTimeout):
			}
			return
		}
		rw.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release) // before srv.Close, which waits for the held handler
	f, w := NewForwarder(srv.URL+"/v1/traces"), &waits1533{}
	f.sleep = w.sleep
	before := Read()
	startForwarder(t, f)
	f.enqueue(batch{body: []byte("spans"), spans: 9})
	untilWithin(t, "9 spans forwarded", 3*ForwardTimeout, func() bool { return spansDelta(before, "forwarded") == 9 })
	if posts.Load() != 2 || len(w.list()) != 1 || spansDelta(before, "forward_failed") != 0 {
		t.Fatalf("%d POSTs, waits %v, forward_failed %d; want 2, 1 wait, 0", posts.Load(), w.list(),
			spansDelta(before, "forward_failed"))
	}
}

// TestForward1533_RetryAfterIsHonoured: a 503 with Retry-After: 40 waits at least 40 s (longer than any backoff
// step: the header decides), then the batch is taken; a 503 with Retry-After: 1 waits the backoff (the longer of
// the two) (mutation: Retry-After ignored reads a ~5 s wait).
func TestForward1533_RetryAfterIsHonoured(t *testing.T) {
	for _, tc := range []struct {
		header   string
		min, max time.Duration
	}{
		{"40", 40 * time.Second, 40*time.Second + time.Millisecond},
		{"1", 2500 * time.Millisecond, 7500 * time.Millisecond}, // the exporter's 5 s ±50 %
		// #1533 pre-review: a huge value is the largest wait, never an overflow (shutdown cuts it: below); a negative
		// one, a past date and garbage are no wait of their own, so the backoff decides
		{"1000000000000", time.Duration(math.MaxInt64), time.Duration(math.MaxInt64)},
		{"-5", 2500 * time.Millisecond, 7500 * time.Millisecond},
		{"Wed, 21 Oct 2015 07:28:00 GMT", 2500 * time.Millisecond, 7500 * time.Millisecond},
		{"soon", 2500 * time.Millisecond, 7500 * time.Millisecond},
	} {
		f, c, w := scripted(t, func(n int64, rw http.ResponseWriter) {
			if n == 1 {
				rw.Header().Set("Retry-After", tc.header)
				rw.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			rw.WriteHeader(http.StatusOK)
		})
		before := Read()
		startForwarder(t, f)
		f.enqueue(batch{body: []byte("spans"), spans: 1})
		until(t, "1 span forwarded", func() bool { return spansDelta(before, "forwarded") == 1 })
		got := w.list()
		if len(got) != 1 || got[0] < tc.min || got[0] > tc.max || c.posts.Load() != 2 {
			t.Fatalf("Retry-After %s: waits %v after %d POSTs, want one wait in [%v, %v]", tc.header, got, c.posts.Load(),
				tc.min, tc.max)
		}
	}
}

// TestForward1533_TheQueueFillsDuringAnOutageAndShutdownCountsWhatItHolds: the collector refuses connections; the
// worker holds the first batch (its wait parked), the 64-batch queue fills behind it, and the next 10 portal
// requests are answered at once with their spans counted forward_overflow (never held). Shutdown mid-retry counts
// the held batch and the 64 queued ones forward_failed (mutation: the held batch not counted reads 64).
func TestForward1533_TheQueueFillsDuringAnOutageAndShutdownCountsWhatItHolds(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	f := NewForwarder("http://" + addr + "/v1/traces")
	waiting := make(chan struct{}, 1)
	f.sleep = func(ctx context.Context, _ time.Duration) bool {
		select {
		case waiting <- struct{}{}:
		default:
		}
		<-ctx.Done() // parked: the outage outlasts the test, until shutdown
		return false
	}
	in := NewIngest(f, nil)
	body := batchBody(t, 1)
	post := func(i int) (int, time.Duration) {
		req := httptest.NewRequest(http.MethodPost, Path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req = req.WithContext(xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: fmt.Sprintf("q1533-%d", i)})))
		rec := httptest.NewRecorder()
		start := time.Now()
		in.ServeHTTP(rec, req)
		return rec.Code, time.Since(start)
	}
	before := Read()
	cancel := startForwarder(t, f)
	if code, _ := post(0); code != http.StatusOK {
		t.Fatalf("the first batch: %d", code)
	}
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("the worker never began retrying the first batch")
	}
	for i := 1; i <= ForwardQueue+10; i++ {
		done := make(chan [2]any, 1)
		go func() {
			code, took := post(i)
			done <- [2]any{code, took}
		}()
		select {
		case r := <-done:
			if r[0].(int) != http.StatusOK || r[1].(time.Duration) > time.Second {
				t.Fatalf("portal request %d: %v after %v, want answered at once", i, r[0], r[1])
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("portal request %d was held: the collector's outage reached the portal", i)
		}
	}
	if o := spansDelta(before, "forward_overflow"); o != 10 {
		t.Fatalf("forward_overflow %d, want the 10 past the %d-batch queue", o, ForwardQueue)
	}
	cancel()
	until(t, "the held and queued batches counted", func() bool {
		return spansDelta(before, "forward_failed") == ForwardQueue+1
	})
	if spansDelta(before, "forwarded") != 0 {
		t.Fatalf("forwarded %d during an outage", spansDelta(before, "forwarded"))
	}
}

// TestForward1533_ShutdownCutsAHugeRetryAfter: the real wait, not a recorded one. The collector answers 503 with
// Retry-After: 1e12 seconds (the largest wait); shutdown 100 ms into it ends the wait at once and counts the held
// batch forward_failed: no Retry-After stalls delivery past shutdown.
func TestForward1533_ShutdownCutsAHugeRetryAfter(t *testing.T) {
	c := &collector1533{answer: func(_ int64, rw http.ResponseWriter) {
		rw.Header().Set("Retry-After", "1000000000000")
		rw.WriteHeader(http.StatusServiceUnavailable)
	}}
	srv := httptest.NewServer(c.handler())
	defer srv.Close()
	f := NewForwarder(srv.URL + "/v1/traces") // its own, real sleep
	before := Read()
	cancel := startForwarder(t, f)
	f.enqueue(batch{body: []byte("spans"), spans: 7})
	until(t, "the first POST", func() bool { return c.posts.Load() == 1 })
	time.Sleep(100 * time.Millisecond)
	start := time.Now()
	cancel()
	until(t, "the held batch counted at shutdown", func() bool { return spansDelta(before, "forward_failed") == 7 })
	if took := time.Since(start); took > time.Second || c.posts.Load() != 1 {
		t.Fatalf("shutdown took %v to end the wait (%d POSTs), want at once", took, c.posts.Load())
	}
}
