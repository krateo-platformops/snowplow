package credhttp

// egress_connections_race_parity_test.go — the existing snowplow_egress_connections counts (#847: opened and handed
// over, per egress and protocol) through #1521's race, end to end through a stage client (CountConnectionsOn over
// NewEndpointTransport), for https and plain http, a single setup, a raced one, one whose connect was dropped as
// cover while stuck, and (plain http) a raced loser that came up. Every moment is sequenced (connects held on
// channels and on the requests' joins), never timed. Each scenario's cells are judged against the server's own count, an instrument outside the
// client: stage opened equals the connections the server completed (for https a handshake the server's own
// HandshakeContext finished, client's Finished read; for http an accept), handovers equal the requests, and no other
// egress moves. Equality holds since #1544 counts a raced TLS connection that completed as its chain ended. Two known
// exceptions, neither allowed for here (the failure names them): a TLS 1.3 close with the server's session tickets
// unread is a RST, which can drop the client's Finished before the server reads it (counted opened, never completed
// on the server; reviewer-1486, #1549; not observed in 45 events); and for plain http, net.Dialer turns a cancel
// that lands as the connect completes into an error and closes the socket (golang/go#16523: accepted by the server,
// never opened by the client), the connect-level twin of #1544, which this arm's connects cannot produce (a held
// connect ignores its ctx once connected).
// It uses only what main had before the per-egress race
// counters, so the same arm runs on main and on the change: it shows the counting value the race's attempts now
// carry into the address's connect chain changes no existing cell (no double count, nothing moved to or from
// "other"). The deltas are logged for the record.

import (
	"context"
	"crypto/tls"
	"encoding/json"
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

func egressCells() map[string]uint64 {
	out := map[string]uint64{}
	for _, c := range EgressConnectionsSnapshot() {
		out[c.Egress+"/"+c.Protocol+"/opened"] = c.Opened
		out[c.Egress+"/"+c.Protocol+"/handovers"] = c.Handovers
	}
	return out
}

func cellDeltas(before, after map[string]uint64) map[string]uint64 {
	out := map[string]uint64{}
	for k, v := range after {
		if d := v - before[k]; d != 0 {
			out[k] = d
		}
	}
	return out
}

type parityCase struct {
	name     string
	trigger  time.Duration      // the destination's seeded trigger (0: cold, no race)
	holds    map[int32]dialHold // what the n-th connect does; unlisted connects connect at once
	cutFrom  int32              // when set, every connect from this one on is holdUntilCut (none reaches the server)
	requests int
	want     string // "race" | "stuck" | "loser" | "" : what the race must have done (the arm proves nothing otherwise)
	httpOnly bool
}

// dialHold sequences one connect of the arm on the arm's own events, never on a timer.
type dialHold int

const (
	// holdUntilCut: never connects; returns when its attempt's ctx ends (the chain finishing cancels it), so it
	// never reaches the server
	holdUntilCut dialHold = iota + 1
	// holdUntilJoined: connects once every request has joined the chain (its waiter's ConnectStart), so all the
	// requests are one chain's waiters
	holdUntilJoined
	// connectThenHoldUntilServed: connects at once (the server accepts it), then returns the connection only after
	// every request was served and the chain finished, regardless of the ctx: a connect that completed before the
	// chain finished, its result processed after
	connectThenHoldUntilServed
)

func TestEgressConnections_UnchangedThroughTheRace(t *testing.T) {
	cases := []parityCase{
		{name: "single", requests: 1},
		// the first connect never completes, the trigger 5 ms: the race fires and its attempt (connect 2) wins;
		// every later race attempt's connect (the trigger doubles, and under -race a handshake can outlast it) and
		// the first are cut when the chain finishes: exactly one connection reaches the server
		{name: "raced", trigger: 5 * time.Millisecond, holds: map[int32]dialHold{1: holdUntilCut}, cutFrom: 3,
			requests: 1, want: "race"},
		// two requests, one chain (the first connect waits for both to join), the trigger 100 ms: the first setup
		// answers one; the other's own attempt never connects and, with no evidence for a whole interval, is
		// dropped as cover; the race attempt that interval check starts (connect 3) serves it; the stuck one and
		// any later race attempt's connect are cut: exactly two connections reach the server
		{name: "stuck", trigger: 100 * time.Millisecond, holds: map[int32]dialHold{1: holdUntilJoined, 2: holdUntilCut},
			cutFrom: 4, requests: 2, want: "stuck"},
		// the first connect completes (the server accepts it) but its result is processed after the race's
		// attempt served the request and the chain finished: a raced loser that came up, counted opened once
		// (closeLoser) and closed unused. Plain http only: an https waiter's connect-level loser is no TLS
		// connection and counted nowhere (the test below), and a TLS loser comes up only at the chain's cancel
		// instant (#1544's arm). Later race attempts' connects are cut, as in raced: one loser, one winner
		{name: "loser", trigger: 5 * time.Millisecond, holds: map[int32]dialHold{1: connectThenHoldUntilServed},
			cutFrom: 3, requests: 1, want: "loser", httpOnly: true},
	}
	for _, https := range []bool{true, false} {
		for _, tc := range cases {
			if https && tc.httpOnly {
				continue
			}
			scheme := map[bool]string{true: "https", false: "http"}[https]
			t.Run(scheme+"/"+tc.name, func(t *testing.T) { parityArm(t, https, tc) })
		}
	}
}

// completedTLSListener serves TLS itself: each accepted connection's handshake runs in its own goroutine, is counted
// when the server's HandshakeContext returns nil (the client's Finished read: the server completed it), and only then
// is the connection handed to the http.Server (whose own handshake call is then a no-op). tls.Config's
// VerifyConnection is no such count: in TLS 1.3 the server calls it before it reads the client's Finished.
type completedTLSListener struct {
	net.Listener
	cfg       *tls.Config
	completed *atomic.Int32
	once      sync.Once
	conns     chan net.Conn
	acceptErr chan error
	done      chan struct{}
	closeOnce sync.Once
}

func newCompletedTLSListener(ln net.Listener, cfg *tls.Config, completed *atomic.Int32) *completedTLSListener {
	return &completedTLSListener{Listener: ln, cfg: cfg, completed: completed, conns: make(chan net.Conn),
		acceptErr: make(chan error, 1), done: make(chan struct{})}
}

func (l *completedTLSListener) Accept() (net.Conn, error) {
	l.once.Do(func() {
		go func() {
			for {
				raw, err := l.Listener.Accept()
				if err != nil {
					l.acceptErr <- err
					return
				}
				go func(raw net.Conn) {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					tc := tls.Server(raw, l.cfg)
					if tc.HandshakeContext(ctx) != nil {
						_ = raw.Close()
						return
					}
					l.completed.Add(1)
					select {
					case l.conns <- tc:
					case <-l.done:
						_ = tc.Close()
					}
				}(raw)
			}
		}()
	})
	select {
	case c := <-l.conns:
		return c, nil
	case err := <-l.acceptErr:
		l.acceptErr <- err // every later Accept gets it too
		return nil, err
	}
}

func (l *completedTLSListener) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return l.Listener.Close()
}

func parityArm(t *testing.T, https bool, tc parityCase) {
	resetConnectTimes(t)
	var accepted, handshakes atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{}"))
	}))
	// closedUnused: connections the server saw closed without ever carrying a request (a raced loser)
	var stateMu sync.Mutex
	active := map[net.Conn]bool{}
	closedUnused := make(chan struct{}, 8)
	srv.Config.ConnState = func(c net.Conn, s http.ConnState) {
		stateMu.Lock()
		defer stateMu.Unlock()
		switch s {
		case http.StateNew:
			accepted.Add(1)
		case http.StateActive:
			active[c] = true
		case http.StateClosed, http.StateHijacked:
			if !active[c] {
				closedUnused <- struct{}{}
			}
			delete(active, c)
		}
	}
	url, caPEM := "", []byte(nil)
	if https {
		certSrv := httptest.NewUnstartedServer(http.NotFoundHandler())
		certSrv.StartTLS() // for its certificate only: srv's handshakes are served by completedTLSListener
		t.Cleanup(certSrv.Close)
		srv.Listener = newCompletedTLSListener(srv.Listener,
			&tls.Config{Certificates: certSrv.TLS.Certificates, NextProtos: []string{"http/1.1"}}, &handshakes)
		srv.Start()
		url, caPEM = "https://"+srv.Listener.Addr().String(), caPEMOf(certSrv)
	} else {
		srv.Start()
		url = srv.URL
	}
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	if tc.trigger > 0 {
		seedConnectTimes(setupKey(addr, https), tc.trigger)
		seedConnectTimes(setupKey(addr, false), tc.trigger)
	}
	var calls atomic.Int32
	// every connect the hook entered has returned (not a WaitGroup: a late race attempt's connect can enter after the
	// wait begins, and a cut one, its ctx already ended, returns without dialling)
	var returned atomic.Int32
	joined, allJoined := atomic.Int32{}, make(chan struct{})
	var racesAtJoin atomic.Uint64 // the races fired when the last request joined
	served := make(chan struct{})
	prev := endpointDialFn
	endpointDialFn = func(ctx context.Context, network, a string) (net.Conn, error) {
		n := calls.Add(1)
		defer returned.Add(1)
		h := tc.holds[n]
		if h == 0 && tc.cutFrom > 0 && n >= tc.cutFrom {
			h = holdUntilCut
		}
		switch h {
		case holdUntilCut:
			<-ctx.Done()
			return nil, ctx.Err()
		case holdUntilJoined:
			select {
			case <-allJoined:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case connectThenHoldUntilServed:
			c, err := prev(ctx, network, a)
			if err != nil {
				return nil, err
			}
			<-served
			return c, nil
		}
		return prev(ctx, network, a)
	}
	t.Cleanup(func() { endpointDialFn = prev })
	cfg := &transport.Config{}
	if https {
		cfg.TLS = transport.TLSConfig{CAData: caPEM}
	}
	rt, err := NewEndpointTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cli := CountConnectionsOn(EgressStage, &http.Client{Transport: rt})

	racesBefore, stuckBefore := racesFiredAt(addr), stuckDroppedAt(addr)
	before := egressCells()
	var wg sync.WaitGroup
	errs := make(chan error, tc.requests)
	for range tc.requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// its waiter's join reports ConnectStart (join): when every request has joined, holdUntilJoined connects
			ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
				ConnectStart: func(string, string) {
					if joined.Add(1) == int32(tc.requests) {
						racesAtJoin.Store(racesFiredAt(addr))
						close(allJoined)
					}
				},
			})
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			req.Close = true // one connection per request: every setup is the race's
			resp, err := cli.Do(req)
			if err != nil {
				errs <- err
				return
			}
			_ = resp.Body.Close()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	close(served) // every request was served: the chain has finished (connectThenHoldUntilServed returns now)
	// every connect of the arm has returned: nothing reaches the server after this (a connect entering later is a
	// cut one: its chain has finished, so it returns without dialling)
	for deadline := time.Now().Add(10 * time.Second); returned.Load() != calls.Load(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("%d of the arm's %d connects never returned (a held connect was not cut by its chain's finish)",
				calls.Load()-returned.Load(), calls.Load())
		}
	}
	if tc.want == "loser" {
		select {
		case <-closedUnused: // the loser that came up, closed unused
		case <-time.After(10 * time.Second):
			t.Fatal("the server never saw the raced loser that came up closed: it was left open")
		}
	}
	d := cellDeltas(before, egressCells())
	raw, _ := json.Marshal(d)
	t.Logf("EGRESS-CELLS %s/%s: %s (server: accepted %d, handshakes %d; races %d, stuck %d)",
		map[bool]string{true: "https", false: "http"}[https], tc.name, raw, accepted.Load(), handshakes.Load(),
		racesFiredAt(addr)-racesBefore, stuckDroppedAt(addr)-stuckBefore)
	switch tc.want {
	case "race":
		if racesFiredAt(addr) == racesBefore {
			t.Fatal("no race fired: the arm proves nothing about a raced setup")
		}
		if accepted.Load() != 1 || (https && handshakes.Load() != 1) {
			t.Fatalf("the server accepted %d and completed %d handshake(s); want exactly the race's one attempt",
				accepted.Load(), handshakes.Load())
		}
	case "stuck":
		if racesAtJoin.Load() != racesBefore {
			t.Fatal("a race fired before both requests joined the chain: they were not one chain's waiters when " +
				"the first answered, and the arm proves nothing about a stuck cover")
		}
		if stuckDroppedAt(addr) == stuckBefore {
			t.Fatal("no connect was dropped as cover while stuck: the arm proves nothing about it")
		}
		if accepted.Load() != 2 || (https && handshakes.Load() != 2) {
			t.Fatalf("the server accepted %d and completed %d handshake(s); want exactly the two that serve the "+
				"requests", accepted.Load(), handshakes.Load())
		}
	case "loser":
		if racesFiredAt(addr) == racesBefore || accepted.Load() != 2 {
			t.Fatalf("races %d, the server accepted %d: want a race and 2 accepted (its winner and the loser that "+
				"came up), or the arm proves nothing about a loser", racesFiredAt(addr)-racesBefore, accepted.Load())
		}
	}
	if extra := len(closedUnused); extra != 0 {
		t.Fatalf("the server saw %d more connection(s) closed unused: only the loser scenario has one", extra)
	}
	var opened, handovers uint64
	for k, v := range d {
		switch {
		case len(k) > 6 && k[:6] == "stage/" && k[len(k)-7:] == "/opened":
			opened += v
		case len(k) > 6 && k[:6] == "stage/" && k[len(k)-10:] == "/handovers":
			handovers += v
		default:
			t.Fatalf("cell %s moved by %d: only stage's cells may move (%s)", k, v, raw)
		}
	}
	server := uint64(accepted.Load())
	if https {
		server = uint64(handshakes.Load())
	}
	if opened != server {
		exception := ""
		switch {
		case https && opened > server:
			exception = "; a counting defect, unless it is the known exception (#1549): a TLS 1.3 close with the server's session tickets unread " +
				"is a RST that can drop the client's Finished before the server reads it (counted, not completed)"
		case !https && opened < server:
			exception = "; a counting defect, unless it is the known exception (golang/go#16523): net.Dialer turns a cancel that lands as the " +
				"connect completes into an error and closes the socket (accepted, never opened)"
		}
		t.Fatalf("stage opened %d connections, the server completed %d (for %d requests): want equal (%s)%s",
			opened, server, tc.requests, raw, exception)
	}
	if opened < uint64(tc.requests) {
		t.Fatalf("stage opened %d connections for %d requests (%s)", opened, tc.requests, raw)
	}
	if handovers != uint64(tc.requests) {
		t.Fatalf("stage handed over %d connections for %d requests (%s)", handovers, tc.requests, raw)
	}
}

// TestEgressConnections_AConnectLevelLoserOfAnHTTPSWaiterIsNotCounted: the one place the address's connect chain could
// now read a counting value it did not have before (a TLS chain's attempt carries its waiter's): a raw connection the
// connect chain brought up and closed because another attempt won. For an https waiter it is no connection anyone
// used (no TLS on it), so it is counted nowhere, with the value or without it, as on main; a plain-http waiter's
// raced connection is counted opened, as on main.
func TestEgressConnections_AConnectLevelLoserOfAnHTTPSWaiterIsNotCounted(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	raw := func() net.Conn {
		c, err := net.Dial("tcp", ln.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	httpsCtx := countConnections(httptest.NewRequest(http.MethodGet, "https://x/", nil), EgressStage)
	httpCtx := countConnections(httptest.NewRequest(http.MethodGet, "http://x/", nil), EgressStage)
	before := egressCells()
	closeLoser(httpsCtx, raw(), false)             // the connect chain's loser, the https waiter's value carried
	closeLoser(context.Background(), raw(), false) // the same without any value (main's connect chain)
	if d := cellDeltas(before, egressCells()); len(d) != 0 {
		t.Fatalf("a connect-level loser of an https waiter moved %v; it is no connection anyone used", d)
	}
	closeLoser(httpCtx, raw(), false) // a plain-http waiter's raced connection: opened, as before
	if d := cellDeltas(before, egressCells()); len(d) != 1 || d["stage/http1/opened"] != 1 {
		t.Fatalf("a plain-http waiter's raced connection moved %v; want stage/http1/opened +1", d)
	}
}
