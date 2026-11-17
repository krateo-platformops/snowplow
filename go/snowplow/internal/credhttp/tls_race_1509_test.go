package credhttp

// tls_race_1509_test.go — #1509 (b) through TLS: a connection is up only when
// its handshake is done, so a connect that succeeds while the handshake never
// answers is raced like a connect that never answers (run28 N1: 1,249 retries
// ended at the 10 s handshake bound). The server here accepts TCP and, while it
// stalls, never sends a ServerHello: the connection is held, unread, until the
// client closes it.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/client-go/transport"
)

// stallListener holds every connection accepted while it stalls (TCP up, no
// TLS, nothing read or written) and hands the others to the server.
type stallListener struct {
	net.Listener
	mu         sync.Mutex
	stallUntil time.Time
	stallNextN int // also stall the next N accepted connections
	held       atomic.Int32
	heldClosed atomic.Int32
	accepted   atomic.Int32
	wg         sync.WaitGroup
}

func (l *stallListener) Accept() (net.Conn, error) {
	for {
		c, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		l.accepted.Add(1)
		l.mu.Lock()
		stall := time.Now().Before(l.stallUntil) || l.stallNextN > 0
		if l.stallNextN > 0 {
			l.stallNextN--
		}
		l.mu.Unlock()
		if !stall {
			return c, nil
		}
		l.held.Add(1)
		l.wg.Add(1)
		go func() {
			defer l.wg.Done()
			// Never answer; returns when the client closes the connection.
			_, _ = io.Copy(io.Discard, c)
			l.heldClosed.Add(1)
			_ = c.Close()
		}()
	}
}

func (l *stallListener) stallFor(d time.Duration) {
	l.mu.Lock()
	l.stallUntil = time.Now().Add(d)
	l.mu.Unlock()
}

func (l *stallListener) stallNext(n int) {
	l.mu.Lock()
	l.stallNextN = n
	l.mu.Unlock()
}

func stallTLSServer(t *testing.T, h http.Handler) (*httptest.Server, *stallListener) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	sl := &stallListener{Listener: ln}
	srv := httptest.NewUnstartedServer(h)
	srv.Listener = sl
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv, sl
}

// httptraceCtx counts the setup events a dial reports to its request's trace.
func httptraceCtx(connectStarts, connectDones, tlsStarts, tlsDones *atomic.Int32, tlsErr *atomic.Bool) context.Context {
	return httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		ConnectStart:      func(string, string) { connectStarts.Add(1) },
		ConnectDone:       func(string, string, error) { connectDones.Add(1) },
		TLSHandshakeStart: func() { tlsStarts.Add(1) },
		TLSHandshakeDone: func(_ tls.ConnectionState, err error) {
			tlsDones.Add(1)
			if err != nil {
				tlsErr.Store(true)
			}
		},
	})
}

func caPEMOf(srv *httptest.Server) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
}

// A connect that succeeds with a handshake that never answers is raced, and
// the race is won by a connection whose handshake is DONE — not by the first
// connect (which the hung one was).
func TestIssue1509_B_HungHandshakeIsRacedAndOnlyATLSDoneConnectionWins(t *testing.T) {
	resetConnectTimes(t)
	srv, sl := stallTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	addr := sl.Addr().String()
	seedConnectTimes(setupKey(addr, true), 20*time.Millisecond)
	sl.stallNext(1)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEMOf(srv))

	start := time.Now()
	c, err := endpointDialTLS(context.Background(), "tcp", addr, &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}})
	if err != nil {
		t.Fatal(err)
	}
	took := time.Since(start)
	tc, ok := c.(*tls.Conn)
	if !ok || !tc.ConnectionState().HandshakeComplete {
		t.Fatalf("returned %T, handshake complete %v; want a *tls.Conn past its handshake", c, ok && tc.ConnectionState().HandshakeComplete)
	}
	if tc.ConnectionState().NegotiatedProtocol != "h2" {
		t.Fatalf("negotiated %q; the raced handshake must offer the transport's NextProtos", tc.ConnectionState().NegotiatedProtocol)
	}
	if took >= time.Second {
		t.Fatalf("took %v; the hung handshake must be raced at its 20ms trigger", took)
	}
	if connClosedSignal(c) == nil {
		t.Fatal("the raced TLS connection carries no close signal (#1509 a)")
	}
	_ = c.Close()
	// The hung connection is a loser: closed, not leaked.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && sl.heldClosed.Load() < sl.held.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if h, hc := sl.held.Load(), sl.heldClosed.Load(); h != 1 || hc != 1 {
		t.Fatalf("held %d, closed %d; want the hung connection closed", h, hc)
	}
}

// Every attempt hung in TLS: the error is the handshake's, and the request's
// trace sees one setup (connect done, TLS failed), as the retry cause reads it.
func TestIssue1509_B_AllHandshakesHungReportsTheTLSPhase(t *testing.T) {
	resetConnectTimes(t)
	srv, sl := stallTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	addr := sl.Addr().String()
	seedConnectTimes(setupKey(addr, true), 30*time.Millisecond)
	// The bound is the TLS level's: its connects answer (loopback, sub-ms),
	// so the connect level is given a measured time that keeps its own race
	// out of the count (each level races on its own trigger; this arm counts
	// one level's sockets).
	seedConnectTimes(setupKey(addr, false), time.Hour)
	sl.stallFor(time.Hour)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEMOf(srv))

	var connectStarts, connectDones, tlsStarts, tlsDones atomic.Int32
	var tlsErr atomic.Bool
	ctx := httptraceCtx(&connectStarts, &connectDones, &tlsStarts, &tlsDones, &tlsErr)
	ctx, cancel := context.WithTimeout(ctx, 400*time.Millisecond)
	defer cancel()
	if _, err := raceDialTLS(ctx, "tcp", addr, &tls.Config{RootCAs: pool}); err == nil {
		t.Fatal("every handshake hung, want an error")
	}
	if connectStarts.Load() != 1 || connectDones.Load() != 1 || tlsStarts.Load() != 1 || tlsDones.Load() != 1 || !tlsErr.Load() {
		t.Fatalf("trace saw ConnectStart %d, ConnectDone %d, TLSHandshakeStart %d, TLSHandshakeDone %d (err %v); want one setup that failed in TLS",
			connectStarts.Load(), connectDones.Load(), tlsStarts.Load(), tlsDones.Load(), tlsErr.Load())
	}
	limit := int(math.Log2(float64(400*time.Millisecond)/float64(30*time.Millisecond))) + 1
	if n := int(sl.accepted.Load()); n > limit {
		t.Fatalf("%d sockets for one stuck setup; want at most %d (log2(cap/trigger)+1)", n, limit)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && sl.heldClosed.Load() < sl.held.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if h, hc := sl.held.Load(), sl.heldClosed.Load(); h != hc {
		t.Fatalf("held %d, closed %d; every hung connection must be closed", h, hc)
	}
}

// The acceptance arm (#1509, TLS): a warm HTTP/2 connection; the server leaves
// (the shutdown 429) and, for the next 2 s, accepts TCP but never answers a
// ClientHello. The customer request must complete, inside its 9 s budget, on a
// connection raced after the stall: the attempts double their waits, so the
// first one started after the stall starts before twice the stall. With the
// race limited to the connect, its one handshake waits out the 10 s handshake
// bound, past the budget.
func TestIssue1509_RideThroughAHungTLSHandshake(t *testing.T) {
	resetConnectTimes(t)
	seedReserve(t, 50*time.Millisecond)
	var leaving atomic.Bool
	srv, sl := stallTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if leaving.CompareAndSwap(true, false) {
			w.Header().Set("Connection", "close")
			w.Header().Set("Retry-After", "5")
			http.Error(w, "the apiserver is shutting down", http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	rt, err := NewEndpointTransport(&transport.Config{
		TLS:   transport.TLSConfig{CAData: caPEMOf(srv)},
		Proxy: http.ProxyFromEnvironment,
	})
	if err != nil {
		t.Fatal(err)
	}
	// The destination's windows hold enough setups for a measured trigger
	// (#1521): triggerMinSamples real setups, asserted.
	requireWarm1521(t, srv)
	cli := &http.Client{Transport: rt}
	warm, err := Send(SendSiteStage, cli, getReq(t, budgetLeft(9*time.Second), srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	_ = warm.Body.Close()
	if warm.ProtoMajor != 2 {
		t.Fatalf("warm request over %s; the arm needs HTTP/2", warm.Proto)
	}

	const stall = 2 * time.Second
	leaving.Store(true)
	start := time.Now()
	sl.stallFor(stall)
	resp, err := Send(SendSiteStage, cli, getReq(t, budgetLeft(9*time.Second), srv.URL))
	took := time.Since(start)
	if err != nil {
		t.Fatalf("after %v: %v (the hung handshake was not raced)", took, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || took > 2*stall+500*time.Millisecond {
		t.Fatalf("status %d after %v; want 200 within %v (the doubling race's first attempt after the stall)", resp.StatusCode, took, 2*stall+500*time.Millisecond)
	}
	if sl.held.Load() == 0 {
		t.Fatal("no connection was held: the arm did not exercise a hung handshake")
	}
	t.Logf("done %v after start; %d connections held in a hung handshake", took, sl.held.Load())
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && sl.heldClosed.Load() < sl.held.Load() {
		time.Sleep(10 * time.Millisecond)
	}
	if h, hc := sl.held.Load(), sl.heldClosed.Load(); h != hc {
		t.Fatalf("held %d, closed %d; every losing connection must be closed", h, hc)
	}
}

// paceListener paces the server's handshake on chosen accepted connections
// (1-based): delay[n] before its first byte; pause[n] after its first chunk
// (the client has started reading the handshake) before the rest.
type paceListener struct {
	net.Listener
	accepted atomic.Int32
	delay    map[int32]time.Duration
	pause    map[int32]time.Duration
}

const paceChunk = 64

func (l *paceListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	n := l.accepted.Add(1)
	return &pacedConn{Conn: c, delay: l.delay[n], pause: l.pause[n]}, nil
}

type pacedConn struct {
	net.Conn
	delay, pause time.Duration
	started      bool // only the server's handshake goroutine writes first
}

func (c *pacedConn) Write(b []byte) (int, error) {
	if c.started {
		return c.Conn.Write(b)
	}
	c.started = true
	time.Sleep(c.delay)
	if c.pause == 0 || len(b) <= paceChunk {
		return c.Conn.Write(b)
	}
	n, err := c.Conn.Write(b[:paceChunk])
	if err != nil {
		return n, err
	}
	time.Sleep(c.pause)
	m, err := c.Conn.Write(b[paceChunk:])
	return n + m, err
}

// Once a destination answers, a waiter's setup is its own attempt: it is not
// raced in the interval in which the destination answered (that answer is
// the fresh evidence), even if it is slower than the trigger. Two setups
// join one chain; the first comes up (answered), and the second's handshake
// starts at once but takes 1.5 trigger intervals. The server must see exactly
// two connections. With the race still armed while answering (M-i), the
// interval's timer starts another socket.
func TestIssue1509_Chain_AnsweredIntervalIsNotRaced(t *testing.T) {
	resetConnectTimes(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const trigger = 100 * time.Millisecond
	pl := &paceListener{
		Listener: ln,
		// The first setup takes a little while, so both waiters join its chain.
		delay: map[int32]time.Duration{1: 15 * time.Millisecond},
		// The second answers at once, then completes 1.5 intervals later:
		// after the answered interval's timer, before the next one.
		pause: map[int32]time.Duration{2: trigger * 3 / 2},
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Listener = pl
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	addr := ln.Addr().String()
	seedConnectTimes(setupKey(addr, true), trigger)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEMOf(srv))
	cfg := &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}}

	type result struct {
		c   net.Conn
		err error
		at  time.Duration
	}
	results := make(chan result, 2)
	start := time.Now()
	for range 2 {
		go func() {
			c, err := endpointDialTLS(context.Background(), "tcp", addr, cfg)
			results <- result{c, err, time.Since(start)}
		}()
	}
	var last time.Duration
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		defer r.c.Close()
		last = max(last, r.at)
	}
	time.Sleep(trigger) // a late raced attempt would be accepted by now
	if n := pl.accepted.Load(); n != 2 {
		t.Fatalf("the server accepted %d connections for 2 setups of a destination that answered; want 2 (the answered interval is not raced; second setup up at %v)", n, last)
	}
	if r := racesFiredAt(addr); r != 0 {
		t.Fatalf("%d races fired at the destination; want exactly 0 (accepted - requests = 0)", r)
	}
	if last < trigger {
		t.Fatalf("the second setup came up after %v; the arm needs it slower than the trigger (%v)", last, trigger)
	}
}

// trickleListener sends the handshake of its trickleNth accepted connection
// (1-based) a few bytes at a time, every gap: a handshake that keeps
// receiving bytes, slowly. The others are not paced, except delayFirst (the
// first connection's first byte, so a second setup joins its chain).
type trickleListener struct {
	net.Listener
	accepted   atomic.Int32
	trickleNth int32
	gap        time.Duration
	delayFirst time.Duration
}

const trickleChunk = 32

func (l *trickleListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	n := l.accepted.Add(1)
	tc := &trickledConn{Conn: c}
	if n == 1 {
		tc.delay = l.delayFirst
	}
	if n == l.trickleNth {
		tc.gap = l.gap
	}
	return tc, nil
}

type trickledConn struct {
	net.Conn
	delay, gap time.Duration
	started    bool // only the server's handshake goroutine writes first
}

func (c *trickledConn) Write(b []byte) (int, error) {
	if !c.started {
		c.started = true
		time.Sleep(c.delay)
	}
	if c.gap == 0 {
		return c.Conn.Write(b)
	}
	written := 0
	for len(b) > 0 {
		k := min(trickleChunk, len(b))
		n, err := c.Conn.Write(b[:k])
		written += n
		if err != nil {
			return written, err
		}
		b = b[k:]
		time.Sleep(c.gap)
	}
	return written, nil
}

// On an answering destination, a handshake that keeps receiving bytes is
// fresh evidence in every trigger interval it receives them, so it is never
// raced again (#1521 review; Diego's ruling: answering holds on an attempt
// up or TLS bytes read, per trigger interval). Two setups join one chain:
// the first comes up (answered), and the second's handshake trickles in a
// few bytes every fifth of an interval for more than 3 intervals. The server
// must see exactly 2 connections. With progress counted on a handshake's
// first read only (M-progress1), the second interval after its first bytes
// has no evidence and the chain races it.
func TestIssue1509_Chain_AHandshakeStillReceivingBytesIsNotRaced(t *testing.T) {
	resetConnectTimes(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const trigger = 50 * time.Millisecond
	tl := &trickleListener{Listener: ln, trickleNth: 2, gap: trigger / 5, delayFirst: 15 * time.Millisecond}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Listener = tl
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	addr := ln.Addr().String()
	seedConnectTimes(setupKey(addr, true), trigger)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEMOf(srv))
	cfg := &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}}

	type result struct {
		c   net.Conn
		err error
		at  time.Duration
	}
	results := make(chan result, 2)
	start := time.Now()
	for range 2 {
		go func() {
			c, err := endpointDialTLS(context.Background(), "tcp", addr, cfg)
			results <- result{c, err, time.Since(start)}
		}()
	}
	var last time.Duration
	for range 2 {
		r := <-results
		if r.err != nil {
			t.Fatal(r.err)
		}
		defer r.c.Close()
		last = max(last, r.at)
	}
	time.Sleep(trigger) // a late raced attempt would be accepted by now
	if n := tl.accepted.Load(); n != 2 {
		t.Fatalf("the server accepted %d connections; want 2: the second setup's handshake kept receiving bytes (up at %v), so it must not be raced again", n, last)
	}
	if r := racesFiredAt(addr); r != 0 {
		t.Fatalf("%d races fired at the destination; want exactly 0 (accepted - requests = 0)", r)
	}
	if last < 3*trigger {
		t.Fatalf("the trickled setup came up after %v; the arm needs its handshake to last over 3 trigger intervals (%v)", last, 3*trigger)
	}
}

// A live attempt keeps covering its waiter when the chain flips out of
// answering and back (#1521 review: dropping earlier attempts as cover on the
// flip back doubled a healthy burst's setups). A connected attempt covers
// until it ends (rule B). Trigger T = 50 ms:
//
//	0       W1, W2 join; attempt 1 (conn 1) comes up at 15 ms: W1 served,
//	        answered; attempt 2 (conn 2) starts for W2: connected, its
//	        handshake held by the server.
//	65      interval check: the success is fresh evidence; still answering.
//	90      W3 joins: attempt 3 (conn 3) starts for it: connected, held too.
//	115     interval check: nothing came up and no bytes were read: the chain
//	        races (attempt 4, conn 4, answers at once). Attempts 2 and 3 are
//	        connected: they keep covering.
//	~116    attempt 4 comes up: W2 served, answered again. W3 is covered by a
//	        live attempt: no new attempt.
//
// The server must accept exactly 4 connections, and every connection beyond
// one per waiter is the one race fired at this destination (exactly: accepted
// - 3 == racesFiredAt). Dropping earlier attempts as cover on the flip back
// (M-epoch), or every attempt at every check (M-dropall), starts another for
// W3 that replaces the race's or adds a fifth.
func TestIssue1509_Chain_ALiveAttemptStillCoversAcrossAReRace(t *testing.T) {
	resetConnectTimes(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const trigger = 50 * time.Millisecond
	pl := &paceListener{
		Listener: ln,
		delay:    map[int32]time.Duration{1: 15 * time.Millisecond, 2: 3 * trigger, 3: 3 * trigger},
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Listener = pl
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	addr := ln.Addr().String()
	seedConnectTimes(setupKey(addr, true), trigger)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEMOf(srv))
	cfg := &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}}

	errs := make(chan error, 3)
	dial := func() {
		c, err := endpointDialTLS(context.Background(), "tcp", addr, cfg)
		if err == nil {
			t.Cleanup(func() { _ = c.Close() })
		}
		errs <- err
	}
	go dial()
	go dial()
	time.Sleep(90 * time.Millisecond)
	go dial()
	for range 3 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(2 * trigger) // a late attempt would be accepted by now
	n, races := pl.accepted.Load(), racesFiredAt(addr)
	if n != 4 || uint64(n-3) != races {
		t.Fatalf("the server accepted %d connections with %d races fired; want 4 and exactly 1 (attempts 1-3, one per waiter, and the one race): a live attempt must keep covering its waiter when the chain answers again", n, races)
	}
}

// A healthy burst opens one setup per request (#1521 review; the #1088
// idle-bound subtest's shape): 25 concurrent HTTP/1.1 TLS requests through
// NewEndpointTransport, to a new destination (an empty window: the cold
// default trigger), to a warm one (a full window of a realistic mix: solo
// setups and a concurrent burst's, so the trigger is measured), and to one
// warmed by solo setups only (#1521 review: its trigger, a solo setup's time,
// is shorter than the burst's concurrent handshakes, which therefore wait on
// the server past an interval while connected and must keep covering).
func TestIssue1509_Chain_AHealthyBurstOpensOneSetupPerRequest(t *testing.T) {
	for _, warm := range []string{burstCold, burstMixWarm, burstSoloWarm} {
		t.Run(warm, func(t *testing.T) {
			if err := healthyBurst1521(t, warm, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}

const (
	burstCold     = "cold destination"
	burstMixWarm  = "warm destination"
	burstSoloWarm = "solo-warmed destination"
)

// The bound above reads the races fired AT THE BURST'S DESTINATION. Its own
// check: while another destination's chain fires races, three deliberate extra
// connects to the burst's destination must still be flagged. A count across
// destinations would let the other destination's races excuse it.
func TestIssue1509_Chain_ABurstsExtraConnectIsNotExcusedByAnotherDestinationsRaces(t *testing.T) {
	err := healthyBurst1521(t, burstMixWarm, true)
	if err == nil {
		t.Fatal("three deliberate extra connects to the burst's destination passed the bound while another destination fired races: the race accounting is not per destination")
	}
	t.Logf("flagged as it must be: %v", err)
}

// healthyBurst1521 runs the burst and returns the bound's verdict. inject:
// during the burst, another destination's chain fires races and three extra
// connects go to the burst's destination outside any chain.
func healthyBurst1521(t *testing.T, warm string, inject bool) error {
	resetConnectTimes(t) // a reused port must not bring another test's samples
	const n = 25
	var accepted, arrived atomic.Int32
	var barrier atomic.Bool
	var release chan struct{}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if barrier.Load() {
			if arrived.Add(1) == n {
				close(release)
			}
			<-release
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	srv.TLS = &tls.Config{NextProtos: []string{"http/1.1"}}
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			accepted.Add(1)
		}
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	key := setupKey(addr, true)
	newRT := func() http.RoundTripper {
		rt, err := NewEndpointTransport(&transport.Config{TLS: transport.TLSConfig{CAData: caPEMOf(srv)}, Proxy: http.ProxyFromEnvironment})
		if err != nil {
			t.Fatal(err)
		}
		return rt
	}
	burst := func(rt http.RoundTripper, during func()) {
		arrived.Store(0)
		release = make(chan struct{})
		barrier.Store(true)
		defer barrier.Store(false)
		cli := &http.Client{Transport: rt}
		var wg sync.WaitGroup
		errs := make(chan error, n)
		for range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := cli.Get(srv.URL)
				if err != nil {
					errs <- err
					return
				}
				_ = resp.Body.Close()
			}()
		}
		if during != nil {
			during()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
	}
	solo := func() {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := newRT().RoundTrip(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
	}
	switch warm {
	case burstSoloWarm:
		for range 25 {
			solo()
		}
		srv.CloseClientConnections()
		if _, measured := connectTimes.trigger(key); !measured {
			t.Fatalf("the solo-warmed destination's window holds %d setups; its trigger is not measured", connectTimes.ring(key).count())
		}
	case burstMixWarm:
		for range 5 {
			solo()
		}
		burst(newRT(), nil)
		for range 5 {
			solo()
		}
		srv.CloseClientConnections()
		if _, measured := connectTimes.trigger(key); !measured {
			t.Fatalf("the warm destination's window holds %d setups; its trigger is not measured", connectTimes.ring(key).count())
		}
	default:
		if _, measured := connectTimes.trigger(key); measured {
			t.Fatal("the new destination's trigger is measured already")
		}
	}
	trigger, _ := connectTimes.trigger(key)
	acceptedBefore := accepted.Load()

	var during func()
	var otherAddr string
	if inject {
		// Another destination whose handshakes hang, with a measured 1 ms
		// trigger: its chain races every few milliseconds while it waits.
		other, sl := stallTLSServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
		otherAddr = sl.Addr().String()
		seedConnectTimes(setupKey(otherAddr, true), time.Millisecond)
		sl.stallFor(time.Minute)
		pool := x509.NewCertPool()
		pool.AppendCertsFromPEM(caPEMOf(other))
		otherCfg := &tls.Config{RootCAs: pool}
		during = func() {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_, _ = endpointDialTLS(ctx, "tcp", otherAddr, otherCfg) // hangs, raced, gives up
			// Three connects to the burst's destination outside any chain:
			// more than a race at that destination could ever excuse (a race
			// fires in about 1 % of warm bursts, and adds at most one).
			for range 3 {
				if c, err := endpointDialFn(context.Background(), "tcp", addr); err == nil {
					_ = c.Close()
				}
			}
		}
	}

	var mu sync.Mutex
	var starts []time.Duration
	t0 := time.Now()
	prev := endpointDialFn
	endpointDialFn = func(ctx context.Context, network, a string) (net.Conn, error) {
		if a == addr { // the burst's destination only
			mu.Lock()
			starts = append(starts, time.Since(t0))
			mu.Unlock()
		}
		return prev(ctx, network, a)
	}
	defer func() { endpointDialFn = prev }()
	racesBefore := racesFiredAt(addr)
	burst(newRT(), during)
	time.Sleep(20 * time.Millisecond) // a late attempt would have started by now
	races := int(racesFiredAt(addr) - racesBefore)
	if inject && racesFiredAt(otherAddr) == 0 {
		t.Fatal("the other destination fired no race: the check below would prove nothing")
	}
	mu.Lock()
	defer mu.Unlock()
	d, a := len(starts), int(accepted.Load()-acceptedBefore)
	if warm == burstCold && (races != 0 || d != n || a != n) {
		// A new destination's trigger is the cold default (3 s), which a
		// burst of milliseconds never reaches: no race can fire, and the
		// count is exact, independent of the race accounting below.
		return fmt.Errorf("a healthy burst of %d requests to a new destination started %d connects, the server accepted %d, and %d races fired; want exactly %d, %d and 0 (trigger %v; connect starts %v)",
			n, d, a, races, n, n, trigger, starts)
	}
	// One setup per request; every connect beyond that is a race this
	// destination's chains fired (a setup ran past the measured trigger: the
	// design's ~5 %), never an extra of its own. Doubling (a waiter's attempt
	// replaced while alive) adds connects no race accounts for.
	if d < n || d-n > races || a-n > races {
		return fmt.Errorf("a healthy burst of %d requests started %d connects and the server accepted %d, with %d races fired at its destination; want %d each plus at most one per race (trigger %v; connect starts %v)",
			n, d, a, races, n, trigger, starts)
	}
	if races > 0 {
		t.Logf("%d races fired (connects %d, accepted %d)", races, d, a)
	}
	return nil
}

// A connect that was stuck past an interval stops covering its waiter, and
// covers again once it completes (rule B: a connected attempt covers until it
// ends). Trigger T = 100 ms; connects are scripted per attempt:
//
//	0       W1, W2 join; attempt 1 (connect 1, at once) comes up at ~15 ms:
//	        W1 served, answered; attempt 2 starts for W2, its connect taking
//	        250 ms.
//	~115    interval check: the success is fresh evidence; still answering.
//	180     W3 joins: attempt 3 starts for it, its connect taking 600 ms.
//	~215    interval check: nothing came up: the chain races (attempt 4,
//	        connect 4 at once, comes up). Attempt 2 is still connecting with no
//	        evidence for an interval: it stops covering; attempt 3 started in
//	        the interval: it covers. W2 is served by attempt 4.
//	~265    attempt 2's connect completes: it covers again (its handshake is
//	        held by the server).
//	300     W4 joins: W3 and W4 are covered by attempts 2 and 3: no new attempt.
//
// So exactly 4 connects have started by 330 ms. Without the regain on connect
// (M-noregain), W4's join starts a fifth.
func TestIssue1509_Chain_AStuckConnectThatCompletesCoversAgain(t *testing.T) {
	resetConnectTimes(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const trigger = 100 * time.Millisecond
	pl := &paceListener{
		Listener: ln,
		// accepts: 1 = attempt 1, 2 = the race (attempt 4), 3 = attempt 2.
		delay: map[int32]time.Duration{1: 15 * time.Millisecond, 3: 3 * trigger},
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Listener = pl
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	addr := ln.Addr().String()
	seedConnectTimes(setupKey(addr, true), trigger)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEMOf(srv))
	cfg := &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}}

	connectDelay := map[int32]time.Duration{2: 250 * time.Millisecond, 3: 600 * time.Millisecond}
	var calls atomic.Int32
	var mu sync.Mutex
	var starts []time.Duration
	t0 := time.Now()
	prev := endpointDialFn
	endpointDialFn = func(ctx context.Context, network, a string) (net.Conn, error) {
		n := calls.Add(1)
		mu.Lock()
		starts = append(starts, time.Since(t0))
		mu.Unlock()
		if d := connectDelay[n]; d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return (&net.Dialer{}).DialContext(ctx, network, a)
	}
	t.Cleanup(func() { endpointDialFn = prev })

	errs := make(chan error, 4)
	dial := func() {
		c, err := endpointDialTLS(context.Background(), "tcp", addr, cfg)
		if err == nil {
			t.Cleanup(func() { _ = c.Close() })
		}
		errs <- err
	}
	go dial()
	go dial()
	time.Sleep(180 * time.Millisecond)
	go dial()
	time.Sleep(120 * time.Millisecond) // 300 ms
	go dial()
	time.Sleep(30 * time.Millisecond) // 330 ms
	mu.Lock()
	by330 := slices.Clone(starts)
	mu.Unlock()
	for range 4 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if len(by330) != 4 {
		t.Fatalf("%d connects had started by 330 ms (%v); want 4: the stuck connect that completed must cover W3 or W4 again", len(by330), by330)
	}
}

// The hung-TLS second stall (#1521 review): a busy destination answers a
// joiner every 2 ms; for 300 ms it then accepts TCP but never answers a
// ClientHello (its held handshakes are connected, so they keep covering their
// waiters, rule B), and the last joiner arrives at its comeback. Every waiter
// that joined during the stall must be served, and served together: once an
// attempt comes up after the comeback, every waiter beyond the attempts
// covering it gets one of its own at once. A serial handout — waiters left to
// the race's own attempts — serves them at least a trigger interval apart
// (the race schedule's shortest gap between two of its attempts), so p95 −
// p50 of their serve times must stay below one trigger.
func TestIssue1509_Chain_AHungTLSStallsWaitersAreServedTogether(t *testing.T) {
	resetConnectTimes(t)
	srv, sl := stallTLSServer(t, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := sl.Addr().String()
	const trigger = 100 * time.Millisecond
	seedConnectTimes(setupKey(addr, true), trigger)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEMOf(srv))
	cfg := &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}}
	const stallAt, stall, every = 200 * time.Millisecond, 300 * time.Millisecond, 2 * time.Millisecond
	comeback := stallAt + stall
	type served struct {
		joined, got time.Duration
		err         error
	}
	var mu sync.Mutex
	var out []served
	t0 := time.Now()
	time.AfterFunc(stallAt, func() { sl.stallFor(stall) })
	var wg sync.WaitGroup
	for i := 0; time.Duration(i)*every <= comeback; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			j := time.Since(t0)
			c, err := endpointDialTLS(ctx, "tcp", addr, cfg)
			g := time.Since(t0)
			if err == nil {
				_ = c.Close()
			}
			mu.Lock()
			out = append(out, served{j, g, err})
			mu.Unlock()
		}()
		time.Sleep(every)
	}
	wg.Wait()
	var got []time.Duration
	for _, s := range out {
		if s.joined < stallAt+every || s.joined > comeback {
			continue
		}
		if s.err != nil {
			t.Fatalf("a waiter that joined %v (during the stall) failed: %v", s.joined, s.err)
		}
		got = append(got, s.got)
	}
	if len(got) < 100 {
		t.Fatalf("only %d waiters joined during the stall; the arm needs its stream", len(got))
	}
	slices.Sort(got)
	p50, p95 := got[len(got)/2], got[len(got)*95/100]
	if p95-p50 >= trigger {
		t.Fatalf("the %d waiters that joined during the stall were served from %v to %v (p50 %v, p95 %v): p95 - p50 = %v, not below one trigger (%v): a serial handout",
			len(got), got[0], got[len(got)-1], p50, p95, p95-p50, trigger)
	}
	t.Logf("%d stall-joined waiters served by p50 %v, p95 %v after start (comeback at %v); %d handshakes held", len(got), p50, p95, comeback, sl.held.Load())
}

// No waiter is ever added to a done chain (#1521 review: the chain could
// finish between dial finding it and the waiter joining it, and the waiter
// then waited out its whole budget on a chain that would never start an
// attempt for it). The chain is finished exactly in that gap (the test seam);
// the waiter must be served by a new chain, at once.
func TestIssue1521_AJoinOntoAChainThatJustFinishedStartsAFreshOne(t *testing.T) {
	resetConnectTimes(t)
	l := newRaceListener(t)
	addr := l.ln.Addr().String()
	// Waiter 1's attempt hangs, so its chain is alive when waiter 2 dials;
	// waiter 2's attempt, on a new chain, connects at once.
	countingRaceDial(t, "hang", "ok")
	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	go func() { _, _ = raceDial(ctx1, "tcp", addr) }()
	deadline := time.Now().Add(2 * time.Second)
	for {
		connectChains.mu.Lock()
		c := connectChains.chains[chainKey{network: "tcp", addr: addr}]
		connectChains.mu.Unlock()
		if c != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("waiter 1's chain never started")
		}
		time.Sleep(time.Millisecond)
	}
	joinGapForTest = func(c *setupChain) {
		c.mu.Lock()
		c.finishLocked() // an attempt's result finishing the chain, in the gap
		c.mu.Unlock()
	}
	t.Cleanup(func() { joinGapForTest = nil })
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	start := time.Now()
	conn, err := raceDial(ctx2, "tcp", addr)
	joinGapForTest = nil
	if err != nil {
		t.Fatalf("a dial whose chain finished as it joined failed after %v: %v (it joined the done chain)", time.Since(start), err)
	}
	_ = conn.Close()
}

// The same dial/join path at TLS level, under churn: 200 concurrent TLS
// setups to a healthy destination, whose chains finish and are replaced
// while setups keep joining. Every one must come up.
func TestIssue1521_ConcurrentTLSSetupsAreNeverStranded(t *testing.T) {
	resetConnectTimes(t)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEMOf(srv))
	cfg := &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}}
	const n = 200
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			c, err := endpointDialTLS(ctx, "tcp", addr, cfg)
			if err != nil {
				errs <- err
				return
			}
			_ = c.Close()
		}()
	}
	wg.Wait()
	close(errs)
	failed := 0
	var first error
	for err := range errs {
		if first == nil {
			first = err
		}
		failed++
	}
	if failed > 0 {
		t.Fatalf("%d of %d TLS setups to a healthy destination failed (first: %v): a setup was stranded", failed, n, first)
	}
}
