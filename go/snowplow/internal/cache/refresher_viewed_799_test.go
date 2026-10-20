// refresher_viewed_799_test.go — #799 gate defects B1 and B2 (architect-623,
// #799 issuecomment-6287593595), adapted from architect-623's RUN probes.
//
//   - B1 NO STRANDED WORKER: two workers return together and both probe a viewed
//     tier holding ONE key. Only one may take it; the other must go back to the
//     normal tier, never park in the viewed tier's blocking Get. The seam
//     (tierProbeHook) holds the first prober between its Len()>0 probe and its
//     Get until a second prober arrives (or 500ms), so the race is exact, not
//     timing. Then two normal keys must run CONCURRENTLY on both workers.
//     Uses the #799 seam, so it does not compile on 546c2b5b; there,
//     architect-623's timing probe stranded a worker in 9 of 120 rounds.
//   - B2 ONE RE-RESOLVE PER KEY ACROSS TIERS: K runs on the normal tier; a tab
//     arms it; a second change routes K to the viewed tier. A second worker must
//     not re-resolve K concurrently — and the second change must still be
//     re-resolved once the first ends. Compiles on 546c2b5b (stats read by name).
package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestViewed799_B1_RacingProbesNeverStrandAWorker is B1.
func TestViewed799_B1_RacingProbesNeverStrandAWorker(t *testing.T) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 2, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 5)
	n1, n2, v, f1, f2 := keys[0], keys[1], keys[2], keys[3], keys[4]

	busy := make(chan struct{})
	var inBusy atomic.Int32
	finalGate := make(chan struct{})
	var inFinal, maxFinal atomic.Int32
	var vRuns atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		switch k {
		case n1, n2:
			inBusy.Add(1)
			<-busy
		case v:
			vRuns.Add(1)
		case f1, f2:
			n := inFinal.Add(1)
			for m := maxFinal.Load(); n > m && !maxFinal.CompareAndSwap(m, n); m = maxFinal.Load() {
			}
			select {
			case <-finalGate:
			case <-time.After(5 * time.Second):
			}
			inFinal.Add(-1)
		}
		return nil
	})
	// The seam: the first viewed-tier prober waits (holding whatever the probe
	// holds) until a second prober reaches the same point, or 500ms.
	var probers atomic.Int32
	second := make(chan struct{})
	hook := func(tr refreshTier) {
		if tr != tierViewed {
			return
		}
		switch probers.Add(1) {
		case 1:
			select {
			case <-second:
			case <-time.After(500 * time.Millisecond):
			}
		case 2:
			close(second)
		}
	}
	tierProbeHook.Store(&hook)
	t.Cleanup(func() { tierProbeHook.Store(nil) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	viewed765Arm(t, v)

	terminalMark([]string{n1, n2})
	waitFor(t, 10*time.Second, "both workers busy", func() bool { return inBusy.Load() == 2 })
	terminalMark([]string{v}) // ONE viewed key while both workers are busy
	close(busy)               // both return together and probe the viewed tier
	waitFor(t, 10*time.Second, "the viewed key re-resolved", func() bool { return vRuns.Load() == 1 })
	time.Sleep(100 * time.Millisecond)

	// No more viewed work: two normal keys must run on BOTH workers at once.
	terminalMark([]string{f1, f2})
	deadline := time.Now().Add(3 * time.Second)
	for maxFinal.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	got := maxFinal.Load()
	close(finalGate)
	if got < 2 {
		t.Fatalf("B1 RED (#799): only %d of 2 workers ran normal-tier work after the viewed tier emptied — "+
			"a worker that lost the probe race is parked in the viewed tier's blocking Get", got)
	}
}

// TestViewed799_B2_OneReResolvePerKeyAcrossTiers is B2.
func TestViewed799_B2_OneReResolvePerKeyAcrossTiers(t *testing.T) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 2, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 1)
	k := keys[0]
	var inFlight, maxConc, calls atomic.Int32
	firstIn := make(chan struct{})
	gate := make(chan struct{})
	RegisterRefreshFunc("widgets", func(_ context.Context, key string, _ ResolvedKeyInputs) error {
		if key != k {
			return nil
		}
		n := inFlight.Add(1)
		defer inFlight.Add(-1)
		for m := maxConc.Load(); n > m && !maxConc.CompareAndSwap(m, n); m = maxConc.Load() {
		}
		if calls.Add(1) == 1 {
			close(firstIn)
			select {
			case <-gate:
			case <-time.After(5 * time.Second):
			}
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)

	terminalMark([]string{k}) // unviewed: the normal tier
	viewed765Wait(t, firstIn, "the first re-resolve never started")
	viewed765Arm(t, k)        // a tab opens the page while it runs
	terminalMark([]string{k}) // a second change: routed to the viewed tier
	time.Sleep(500 * time.Millisecond)
	conc := maxConc.Load()
	close(gate)
	if conc > 1 {
		t.Fatalf("B2 RED (#799): key re-resolved by %d workers at once (normal tier in flight + viewed tier) — "+
			"one re-resolve per key at a time is what the trigger set, dirty window and rate floor assume", conc)
	}
	waitFor(t, 10*time.Second, "the second change re-resolved after the first ended", func() bool { return calls.Load() == 2 })
	if got := RefresherStatsByStat()["deferred_in_flight"]; got != 1 {
		t.Fatalf("B2: deferred_in_flight = %d, want 1 (the viewed dequeue waited for the in-flight re-resolve)", got)
	}
}
