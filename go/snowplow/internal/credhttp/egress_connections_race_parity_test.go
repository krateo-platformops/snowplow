package credhttp

// egress_connections_race_parity_test.go — the existing snowplow_egress_connections counts (#847: opened and handed
// over, per egress and protocol) through #1521's race, end to end through a stage client (CountConnectionsOn over
// NewEndpointTransport), for https and plain http, a single setup, a raced one and one whose connect was dropped as
// cover while stuck. Each scenario's cells are judged against the server's own count, an instrument outside the
// client: opened in stage equals the connections the server completed (a handshake for https, an accept for http),
// handovers equal the requests, and no other egress moves. It uses only what main had before the per-egress race
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
	trigger  time.Duration // the destination's seeded trigger (0: cold, no race)
	delays   map[int32]time.Duration
	requests int
	want     string // "race" | "stuck" | "" : what the race must have done (the arm proves nothing otherwise)
}

func TestEgressConnections_UnchangedThroughTheRace(t *testing.T) {
	cases := []parityCase{
		{name: "single", requests: 1},
		// the first connect slow, the trigger 5 ms: the race fires, its attempt wins, the first comes up later and
		// is closed (a raced connection, counted opened by closeLoser)
		{name: "raced", trigger: 5 * time.Millisecond, delays: map[int32]time.Duration{1: 60 * time.Millisecond},
			requests: 1, want: "race"},
		// two requests at once, the trigger 50 ms: the first setup answers, the second's own attempt connects for
		// 200 ms and is dropped as cover at the next interval check, then covers again when it connects
		{name: "stuck", trigger: 50 * time.Millisecond, delays: map[int32]time.Duration{2: 200 * time.Millisecond},
			requests: 2, want: "stuck"},
	}
	for _, https := range []bool{true, false} {
		for _, tc := range cases {
			scheme := map[bool]string{true: "https", false: "http"}[https]
			t.Run(scheme+"/"+tc.name, func(t *testing.T) { parityArm(t, https, tc) })
		}
	}
}

func parityArm(t *testing.T, https bool, tc parityCase) {
	resetConnectTimes(t)
	var accepted, handshakes atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{}"))
	}))
	srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			accepted.Add(1)
		}
	}
	if https {
		srv.TLS = &tls.Config{NextProtos: []string{"http/1.1"},
			VerifyConnection: func(tls.ConnectionState) error { handshakes.Add(1); return nil }}
		srv.StartTLS()
	} else {
		srv.Start()
	}
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	if tc.trigger > 0 {
		seedConnectTimes(setupKey(addr, https), tc.trigger)
		seedConnectTimes(setupKey(addr, false), tc.trigger)
	}
	var calls atomic.Int32
	prev := endpointDialFn
	endpointDialFn = func(ctx context.Context, network, a string) (net.Conn, error) {
		if d := tc.delays[calls.Add(1)]; d > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return prev(ctx, network, a)
	}
	t.Cleanup(func() { endpointDialFn = prev })
	cfg := &transport.Config{}
	if https {
		cfg.TLS = transport.TLSConfig{CAData: caPEMOf(srv)}
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
			req, _ := http.NewRequest(http.MethodGet, srv.URL, nil)
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
	time.Sleep(400 * time.Millisecond) // a raced attempt still connecting has come up and been closed by now
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
	case "stuck":
		if stuckDroppedAt(addr) == stuckBefore {
			t.Fatal("no connect was dropped as cover while stuck: the arm proves nothing about it")
		}
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
		t.Fatalf("stage opened %d connections, the server completed %d (%s)", opened, server, raw)
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
