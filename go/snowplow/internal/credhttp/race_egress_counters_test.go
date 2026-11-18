package credhttp

// race_egress_counters_test.go — #1521's race counted per egress (Diego 2026-11-18, "no #1521 fix; add counters"):
// races fired, attempts replaced and stuck connects dropped, cumulative, charged to the egress of the chain's latest
// waiter, both levels of an https setup included. The arm runs the stuck-connect scenario
// (TestIssue1509_Chain_AStuckConnectThatCompletesCoversAgain) with its waiters carrying a request's connection
// counting for one egress: every event the per-destination counters record lands in that egress, none elsewhere.

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func raceDeltas(before, after []EgressRaceCount) map[string][3]uint64 {
	out := map[string][3]uint64{}
	for i, a := range after {
		b := before[i]
		out[a.Egress] = [3]uint64{a.RacesFired - b.RacesFired, a.AttemptsReplaced - b.AttemptsReplaced,
			a.StuckDropped - b.StuckDropped}
	}
	return out
}

// TestRaceCounters_EveryEventLandsInItsWaitersEgress: the stuck-connect scenario (a race fired at the first interval,
// a connect dropped as cover while stuck, attempts at the new answering period) with every waiter carrying a stage
// request's connection counting. The stage cell moves by exactly what the destination's counters moved by, both
// levels (the TLS chain's and, through its attempts, the address's connect chain), and no other egress moves
// (mutations: an attempt that does not carry the counting into the connect chain charges the connect level to
// "other"; a counter not incremented reads 0 in stage).
func TestRaceCounters_EveryEventLandsInItsWaitersEgress(t *testing.T) {
	resetConnectTimes(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	const trigger = 100 * time.Millisecond
	pl := &paceListener{Listener: ln, delay: map[int32]time.Duration{1: 15 * time.Millisecond, 3: 3 * trigger}}
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
	prev := endpointDialFn
	endpointDialFn = func(ctx context.Context, network, a string) (net.Conn, error) {
		n := calls.Add(1)
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

	// a stage request's connection counting, as CountConnectionsOn(EgressStage, …) installs it
	req := httptest.NewRequest(http.MethodGet, "https://"+addr+"/", nil)
	ctx := countConnections(req, EgressStage)

	destBefore := [3]uint64{racesFiredAt(addr), replacedAt(addr), stuckDroppedAt(addr)}
	before := EgressRacesSnapshot()
	errs := make(chan error, 4)
	dial := func() {
		c, err := endpointDialTLS(ctx, "tcp", addr, cfg)
		if err == nil {
			t.Cleanup(func() { _ = c.Close() })
		}
		errs <- err
	}
	go dial()
	go dial()
	time.Sleep(180 * time.Millisecond)
	go dial()
	time.Sleep(120 * time.Millisecond)
	go dial()
	for range 4 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(2 * trigger) // the chain's last interval check has run
	dest := [3]uint64{racesFiredAt(addr) - destBefore[0], replacedAt(addr) - destBefore[1],
		stuckDroppedAt(addr) - destBefore[2]}
	got := raceDeltas(before, EgressRacesSnapshot())
	if dest[0] == 0 || dest[2] == 0 {
		t.Fatalf("the scenario fired %d races and dropped %d stuck connects at its destination; want at least one "+
			"of each (the arm proves nothing otherwise)", dest[0], dest[2])
	}
	if got[EgressStage] != dest {
		t.Fatalf("stage's cell moved by %v (races, replaced, stuck); want the destination's %v: every event of a "+
			"chain whose waiters are stage requests, both levels", got[EgressStage], dest)
	}
	for _, e := range Egresses {
		if e != EgressStage && got[e] != ([3]uint64{}) {
			t.Fatalf("%s's cell moved by %v; only stage's waiters dialed", e, got[e])
		}
	}
}

// TestRaceCounters_AChainWithoutCountingIsChargedToOther: waiters that carry no connection counting (a client outside
// CountConnectionsOn and Harden) leave their chain's events in "other", the closed set's catch-all, never in a named
// egress (mutation: charging them to the first egress reads stage).
func TestRaceCounters_AChainWithoutCountingIsChargedToOther(t *testing.T) {
	before := EgressRacesSnapshot()
	raceCellOf(context.Background()).racesFired.Add(1)
	raceCellOf(nil).stuckDropped.Add(1)
	got := raceDeltas(before, EgressRacesSnapshot())
	if got[EgressOther] != ([3]uint64{1, 0, 1}) || got[EgressStage] != ([3]uint64{}) {
		t.Fatalf("other moved by %v, stage by %v; want other {1, 0, 1}, stage none", got[EgressOther], got[EgressStage])
	}
	req := httptest.NewRequest(http.MethodGet, "http://x/", nil)
	raceCellOf(countConnections(req, EgressAuthn)).attemptsReplaced.Add(2)
	if d := raceDeltas(before, EgressRacesSnapshot())[EgressAuthn]; d != ([3]uint64{0, 2, 0}) {
		t.Fatalf("authn moved by %v; want {0, 2, 0}", d)
	}
}

// TestRaceCounters_AConnectLevelRaceIsChargedToTheHTTPSWaitersEgress: the race fires in the address's CONNECT chain
// (its trigger 5 ms, the first raw connect 60 ms; the TLS chain's own trigger 1 s, so it never races). The connect
// chain's waiter is the TLS chain's attempt, which carries the stage request's connection counting into it, so the
// connect-level race is charged to stage, not to "other" (mutation: the attempt not carrying the counting charges it
// to "other").
func TestRaceCounters_AConnectLevelRaceIsChargedToTheHTTPSWaitersEgress(t *testing.T) {
	resetConnectTimes(t)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)
	addr := srv.Listener.Addr().String()
	seedConnectTimes(setupKey(addr, true), time.Second)
	seedConnectTimes(setupKey(addr, false), 5*time.Millisecond)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caPEMOf(srv))
	cfg := &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}}
	var calls atomic.Int32
	prev := endpointDialFn
	endpointDialFn = func(ctx context.Context, network, a string) (net.Conn, error) {
		if calls.Add(1) == 1 {
			select {
			case <-time.After(60 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return (&net.Dialer{}).DialContext(ctx, network, a)
	}
	t.Cleanup(func() { endpointDialFn = prev })
	ctx := countConnections(httptest.NewRequest(http.MethodGet, "https://"+addr+"/", nil), EgressStage)
	connBefore := countsAt(&raceCounts, addr)
	before := EgressRacesSnapshot()
	c, err := endpointDialTLS(ctx, "tcp", addr, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	time.Sleep(150 * time.Millisecond) // the slow first connect has come up (or been cancelled) by now
	if raceCountAt(setupKey(addr, false)) == 0 {
		t.Fatal("the connect chain fired no race: the arm proves nothing about the connect level")
	}
	got := raceDeltas(before, EgressRacesSnapshot())
	if want := countsAt(&raceCounts, addr) - connBefore; got[EgressStage][0] != want || got[EgressOther] != ([3]uint64{}) {
		t.Fatalf("stage's races moved by %d, other's cell by %v; want stage = the destination's %d (both levels), "+
			"other none", got[EgressStage][0], got[EgressOther], want)
	}
}

// raceCountAt is one destination window's race count (one level).
func raceCountAt(key string) uint64 {
	if v, ok := raceCounts.Load(key); ok {
		return v.(*atomic.Uint64).Load()
	}
	return 0
}
