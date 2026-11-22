// refresher_floor_live_backlog_1570_test.go — #1570: a floored key's wait
// follows its side's LIVE backlog (refresher_floor_deferral.go), never the
// backlog it saw when it was floored.
//
// Both arms drive the real refresher (StartRefresher, the real dequeue, the real
// floored branch, the real terminal Put) under the production floor rule. A
// measured cell X is dequeued at the top of a backlog of fillers — floored at
// RefreshCost × K/S with K the whole backlog — and the fillers then drain.
//
//  1. THE WAIT CLOSES AS THE BACKLOG DRAINS (S = 1, a 100 ms cell, 40 and 120
//     fillers: the reviewer's probe, RUN on fc72c786 at 4.11–4.14 s and
//     12.19–12.22 s for a 44 ms / 132 ms drain — cost × (N+1)/S, frozen).
//  2. AT BURST SCALE (S = 3, 1,600 fillers, a 275 ms cell: the refresher's
//     recorded burst and the reviewer's example, whose frozen floor is ~147 s).
//
// In both, X's next refresh must start within RefreshCost × max(1, K_now/S) of
// the drain (K_now ≤ S once drained: one cost), plus scheduling slack — and not
// before its own floor (start ≥ its last Put + its cost: the coalescing kept).
// Falsifier: the fixed AddAfter(floor − elapsed) restored ⇒ X waits out the
// floor frozen at the peak (4.1 s / ~147 s) ⇒ RED. Every bound comes from the
// arm's own clock and the fake refresh's own measured cost.

package cache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// liveBacklogArm runs one arm: parallelism p, a cell X costing cost, fillers
// behind it, and the drain; it asserts X's second refresh against the drain.
func liveBacklogArm(t *testing.T, p, fillers int, cost, slack time.Duration) {
	t.Helper()
	CompleteRBACSnapshotForTest(t)
	cleanup := withCleanRefresher(t, p, 0)
	t.Cleanup(cleanup)
	t.Cleanup(SetRefresherRateFloorForTest("")) // the production rule
	resetRefresherForTest()
	c := ResolvedCache()
	s := max(p-1, 1) // the unviewed side's workers (#1069)

	mk := func(name string) string {
		in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: name}
		k := ComputeKey(in)
		putAgedEntry(c, k, &in)
		return k
	}
	x := mk("live-backlog-x")
	gates := make([]string, s)
	isGate := map[string]bool{}
	for i := range gates {
		gates[i] = mk(fmt.Sprintf("live-backlog-gate-%d", i))
		isGate[gates[i]] = true
	}
	fill := make([]string, fillers)
	for i := range fill {
		fill[i] = mk(fmt.Sprintf("live-backlog-fill-%d", i))
	}

	var (
		mu         sync.Mutex
		xRuns      []refreshRun
		xPutAt     []time.Time
		lastFillAt time.Time
		fillDone   atomic.Int64
		gateIn     atomic.Int64
	)
	gateCh := make(chan struct{})
	openGates := sync.OnceFunc(func() { close(gateCh) })
	t.Cleanup(openGates)
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, used ResolvedKeyInputs) error {
		switch {
		case k == x:
			start := time.Now()
			gen := c.CaptureGen(k)
			rctx := WithL1KeyContext(ctx, k)
			time.Sleep(cost)
			if !c.replaceIfGenRefresh(rctx, k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &used}, gen).Stored() {
				t.Errorf("terminal Put refused for X")
			}
			mu.Lock()
			xRuns = append(xRuns, refreshRun{start: start, cost: time.Since(start)})
			xPutAt = append(xPutAt, time.Now())
			mu.Unlock()
		case isGate[k]:
			gateIn.Add(1)
			<-gateCh // holds an unviewed worker until the backlog is in place
		default: // a filler: ~1 ms of work, no Put, so it is never floored itself
			time.Sleep(time.Millisecond)
			mu.Lock()
			lastFillAt = time.Now()
			mu.Unlock()
			fillDone.Add(1)
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	r := refresherSingleton()

	// X's first refresh records its cost.
	enqueueRefreshForTest(x)
	waitFor(t, 5*time.Second, "X's first refresh", func() bool { mu.Lock(); defer mu.Unlock(); return len(xRuns) == 1 })
	// Every unviewed worker held; X, then the fillers, queued behind them.
	for _, g := range gates {
		enqueueRefreshForTest(g)
	}
	waitFor(t, 5*time.Second, "every unviewed worker held by a gate", func() bool { return gateIn.Load() == int64(s) })
	enqueueRefreshForTest(x)
	for _, f := range fill {
		enqueueRefreshForTest(f)
	}
	floored0 := r.flooredTotal.Load()
	openGates()

	// SETUP: X is floored at the top of the backlog, while most fillers still
	// wait (read from the floored counter, which both the set and a frozen
	// AddAfter tick, so the falsifier passes this SETUP too).
	var doneAtFloor int64
	waitFor(t, 5*time.Second, "X floored at the top of the backlog", func() bool {
		if r.flooredTotal.Load() == floored0 {
			return false
		}
		doneAtFloor = fillDone.Load()
		return true
	})
	mu.Lock()
	first := xRuns[0]
	firstPut := xPutAt[0]
	mu.Unlock()
	if doneAtFloor > int64(fillers/4) {
		t.Fatalf("SETUP: %d of %d fillers had already run when X was floored; the arm needs the peak", doneAtFloor, fillers)
	}
	kAtFloor := fillers - int(doneAtFloor) // at least: the fillers still queued behind X
	frozen := time.Duration(int64(first.cost) * int64(kAtFloor) / int64(s))

	waitFor(t, 15*time.Second, fmt.Sprintf("X's second refresh (a floor frozen at the peak holds it ~%v)", frozen),
		func() bool { mu.Lock(); defer mu.Unlock(); return len(xRuns) == 2 })
	mu.Lock()
	second := xRuns[1]
	drained := lastFillAt
	mu.Unlock()
	if n := fillDone.Load(); n != int64(fillers) {
		t.Fatalf("SETUP: X re-ran before the drain (%d of %d fillers done): the backlog did not hold it", n, fillers)
	}
	// The floor still holds: no refresh before the cell is as old as its cost.
	if age := second.start.Sub(firstPut); age < first.cost-slack {
		t.Fatalf("X re-ran %v after its last Put, below its own cost %v: the floor did not hold", age, first.cost)
	}
	// And it follows the drain: within one cost (K_now ≤ S) of the last filler.
	bound := first.cost + slack
	if lag := second.start.Sub(drained); lag > bound {
		t.Fatalf("X re-ran %v after the backlog drained: want ≤ %v (cost %v × max(1, K_now/S) + %v slack). "+
			"Its floor was frozen at the peak (K≥%d, S=%d: %v)", lag, bound, first.cost, slack, kAtFloor, s, frozen)
	}
	t.Logf("GREEN: X (cost %v) floored at K≥%d S=%d (a frozen floor would be %v) re-ran %v after the drain (%d fillers)",
		first.cost, kAtFloor, s, frozen, second.start.Sub(drained), fillers)
}

// TestRefresher1570_FlooredWaitClosesAsTheBacklogDrains — arm 1 (the reviewer's
// probe: S = 1, a 100 ms cell, 40 and 120 keys behind it).
func TestRefresher1570_FlooredWaitClosesAsTheBacklogDrains(t *testing.T) {
	for _, n := range []int{40, 120} {
		t.Run(fmt.Sprintf("behind_%d", n), func(t *testing.T) {
			liveBacklogArm(t, 2, n, 100*time.Millisecond, 50*time.Millisecond)
		})
	}
}

// TestRefresher1570_FlooredWaitClosesAtBurstScale — arm 2: K ≈ 1,600, S = 3, a
// 275 ms cell.
func TestRefresher1570_FlooredWaitClosesAtBurstScale(t *testing.T) {
	liveBacklogArm(t, 4, 1600, 275*time.Millisecond, 100*time.Millisecond)
}
