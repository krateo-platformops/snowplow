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
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
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
	if last < trigger {
		t.Fatalf("the second setup came up after %v; the arm needs it slower than the trigger (%v)", last, trigger)
	}
}
