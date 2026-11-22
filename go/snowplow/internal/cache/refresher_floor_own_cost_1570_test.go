// refresher_floor_own_cost_1570_test.go — #1570: the refresher's per-key rate
// floor is the cell's OWN last recompute time (ResolvedEntry.RefreshCost,
// stamped by the refresher terminal Put), not a fixed 2 s.
//
// The arms drive the real refresher (StartRefresher, the real dequeue, the real
// floored branch and the real terminal Put replaceIfGenRefresh) with fake
// refresh functions whose cost the test controls. No override is set: every
// arm runs the production rule (SetRefresherRateFloorForTest("")).
//
//  1. COALESCING KEPT FOR AN EXPENSIVE CELL (#321). A cell whose refresh costs
//     D is re-marked every few ms: every two consecutive refresh starts are at
//     least twice the previous run's cost apart (run D, then floored D).
//     Falsifier: the floor ignored → the starts are back to back (≈ D apart).
//  2. A CHEAP CELL IS NOT FLOORED FOR 2 s. A ~1 ms cell re-marked 100 ms after
//     its refresh re-resolves at once. Falsifier: a fixed 2 s floor.
//  3. THE FIRST REFRESH OF A CELL WITH NO RECORDED TIME RUNS AT ONCE, even when
//     the cell was Put a moment ago. Falsifier: any fixed floor for the
//     unknown case.
//  4. THE TERMINAL PUT RECORDS ITS COST; a Put that is not a timed refresh
//     records none.
//
// Every bound below comes from the test's own clock and the fake refresh's own
// measured duration, never from the refresher's counters.

package cache

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// floorArmSetup is the shared harness: one worker (so the only thing between
// two refreshes of the key is the floor), the production floor rule, a clean
// store under a complete RBAC snapshot, and an aged entry for key.
func floorArmSetup(t *testing.T, name string) (*ResolvedCacheStore, ResolvedKeyInputs, string) {
	t.Helper()
	CompleteRBACSnapshotForTest(t)
	cleanup := withCleanRefresher(t, 1, 0)
	t.Cleanup(cleanup)
	t.Cleanup(SetRefresherRateFloorForTest("")) // the production rule: the cell's own cost
	resetRefresherForTest()
	c := ResolvedCache()
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: name}
	key := ComputeKey(in)
	putAgedEntry(c, key, &in)
	return c, in, key
}

// refreshRun is one observed refresh: when the fake refresh started and how long
// it took up to its accepted terminal Put.
type refreshRun struct {
	start time.Time
	cost  time.Duration
}

// registerTimedRefresh registers a widgets refresh that sleeps cost, then writes
// through the REAL refresher terminal Put with the refresh ctx, recording each run.
func registerTimedRefresh(t *testing.T, c *ResolvedCacheStore, cost time.Duration) (runs func() []refreshRun) {
	t.Helper()
	var mu sync.Mutex
	var rs []refreshRun
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, used ResolvedKeyInputs) error {
		start := time.Now()
		gen := c.CaptureGen(k)
		// The resolve begins: its dep-gen sink (as resolveAndPopulateL1's), so the
		// Put's #375 check sees no moved dep and does not remark the key itself.
		rctx := WithL1KeyContext(ctx, k)
		time.Sleep(cost)
		if !c.replaceIfGenRefresh(rctx, k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &used}, gen).Stored() {
			t.Errorf("terminal Put refused for %s", k)
		}
		mu.Lock()
		rs = append(rs, refreshRun{start: start, cost: time.Since(start)})
		mu.Unlock()
		return nil
	})
	return func() []refreshRun {
		mu.Lock()
		defer mu.Unlock()
		return append([]refreshRun(nil), rs...)
	}
}

// TestRefresher1570_ExpensiveCellKeepsCoalescing — arm 1.
func TestRefresher1570_ExpensiveCellKeepsCoalescing(t *testing.T) {
	c, _, key := floorArmSetup(t, "expensive-1570")
	const cost = 250 * time.Millisecond
	runs := registerTimedRefresh(t, c, cost)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	// A storm of re-marks for 3 s: far more marks than refreshes can run.
	stop := time.Now().Add(3 * time.Second)
	marks := 0
	for time.Now().Before(stop) {
		enqueueRefreshForTest(key)
		marks++
		time.Sleep(5 * time.Millisecond)
	}
	// The last mark is still owed (lossless): wait for the refresh that serves it.
	lastMark := time.Now()
	waitFor(t, 3*time.Second, "the final mark re-resolves (lossless)", func() bool {
		rs := runs()
		return len(rs) > 0 && rs[len(rs)-1].start.After(lastMark.Add(-time.Millisecond))
	})

	rs := runs()
	if len(rs) < 3 {
		t.Fatalf("SETUP: %d refreshes for %d marks over 3 s; need ≥ 3 to measure gaps", len(rs), marks)
	}
	// Between two refreshes of the key, the second may start only once the cell
	// is as old as its last refresh took: start[k+1] ≥ start[k] + 2·cost[k]
	// (run, then floored). Clock tolerance: the fake's start is a few µs after
	// the cycle start the cost is measured from, and the Put's CreatedAt is
	// stamped just after the cost: 20 ms covers scheduling, never a whole run.
	const tol = 20 * time.Millisecond
	for k := 1; k < len(rs); k++ {
		gap := rs[k].start.Sub(rs[k-1].start)
		if gap < 2*rs[k-1].cost-tol {
			t.Fatalf("refresh %d started %v after refresh %d, whose run took %v: want ≥ 2×cost = %v "+
				"(the cell's own floor did not hold; %d marks, %d refreshes)", k, gap, k-1, rs[k-1].cost, 2*rs[k-1].cost, marks, len(rs))
		}
	}
	// And the collapse is real: ≤ one refresh per 2·cost of storm, plus the
	// first (unfloored) and the final (owed) one.
	if max := int(3*time.Second/(2*cost)) + 2; len(rs) > max {
		t.Fatalf("%d refreshes for a 3 s storm of a %v cell: want ≤ %d", len(rs), cost, max)
	}
	t.Logf("GREEN: %d marks → %d refreshes of a %v cell; floored_total=%d", marks, len(rs), cost, refresherSingleton().flooredTotal.Load())
}

// TestRefresher1570_CheapCellIsNotFlooredTwoSeconds — arm 2.
func TestRefresher1570_CheapCellIsNotFlooredTwoSeconds(t *testing.T) {
	c, _, key := floorArmSetup(t, "cheap-1570")
	runs := registerTimedRefresh(t, c, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	enqueueRefreshForTest(key)
	waitFor(t, 2*time.Second, "first refresh", func() bool { return len(runs()) == 1 })
	e, ok := c.GetNoTouch(key)
	if !ok || e.RefreshCost <= 0 || e.RefreshCost > 500*time.Millisecond {
		t.Fatalf("SETUP: the refreshed cell must carry its own ~1 ms cost, got ok=%v cost=%v", ok, e.RefreshCost)
	}
	// A change 100 ms after that refresh: the cell's floor (its ~1 ms cost) is
	// long past, so the change is refreshed at once, not at 2 s.
	time.Sleep(100 * time.Millisecond)
	marked := time.Now()
	enqueueRefreshForTest(key)
	waitFor(t, 3*time.Second, "second refresh", func() bool { return len(runs()) == 2 })
	if lat := runs()[1].start.Sub(marked); lat > 250*time.Millisecond {
		t.Fatalf("a ~1 ms cell re-marked 100 ms after its refresh waited %v to re-resolve: it was floored "+
			"by something other than its own cost (want ≪ 1 s)", lat)
	}
}

// TestRefresher1570_UnmeasuredCellFirstRefreshRunsAtOnce — arm 3.
func TestRefresher1570_UnmeasuredCellFirstRefreshRunsAtOnce(t *testing.T) {
	c, in, key := floorArmSetup(t, "unmeasured-1570")
	// Put JUST NOW by a write that is not a timed refresh (a customer read, the
	// seed): CreatedAt = now, RefreshCost = 0. Under the fixed floor this was
	// the floored case; now nothing is known about its cost, so nothing floors it.
	c.put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in})
	if e, _ := c.GetNoTouch(key); e == nil || e.RefreshCost != 0 {
		t.Fatalf("SETUP: a plain Put must record no refresh cost, got %+v", e)
	}
	runs := registerTimedRefresh(t, c, time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	fl0 := refresherSingleton().flooredTotal.Load()
	marked := time.Now()
	enqueueRefreshForTest(key)
	waitFor(t, 3*time.Second, "first refresh", func() bool { return len(runs()) == 1 })
	if lat := runs()[0].start.Sub(marked); lat > 250*time.Millisecond {
		t.Fatalf("the first refresh of a cell with no recorded cost waited %v: want at once (no invented floor)", lat)
	}
	if fl := refresherSingleton().flooredTotal.Load() - fl0; fl != 0 {
		t.Fatalf("floored_total moved by %d for a cell with no recorded cost", fl)
	}
}

// TestRefresher1570_TerminalPutRecordsItsCost — arm 4.
func TestRefresher1570_TerminalPutRecordsItsCost(t *testing.T) {
	c, _, key := floorArmSetup(t, "cost-1570")
	const cost = 60 * time.Millisecond
	runs := registerTimedRefresh(t, c, cost)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	enqueueRefreshForTest(key)
	waitFor(t, 2*time.Second, "refresh", func() bool { return len(runs()) == 1 })
	e, ok := c.GetNoTouch(key)
	if !ok {
		t.Fatal("refreshed cell missing")
	}
	if got := runs()[0].cost; e.RefreshCost < cost || e.RefreshCost > got+50*time.Millisecond {
		t.Fatalf("RefreshCost = %v, want within [%v, %v] (the fake slept %v; its run took %v)", e.RefreshCost, cost, got+50*time.Millisecond, cost, got)
	}
	// A direct write with no refresher timing on its ctx records none.
	var stored atomic.Bool
	in2 := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "cost-1570-direct"}
	k2 := ComputeKey(in2)
	putAgedEntry(c, k2, &in2)
	stored.Store(c.replaceIfGenRefresh(context.Background(), k2, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in2}, c.CaptureGen(k2)).Stored())
	if e2, _ := c.GetNoTouch(k2); !stored.Load() || e2 == nil || e2.RefreshCost != 0 {
		t.Fatalf("a terminal Put with no refresh cycle on its ctx must record no cost: stored=%v entry=%+v", stored.Load(), e2)
	}
}

// ── #1570 v2: the floor charges the backlog of the side that serves the cell ──

// TestRefresher1570_CheapCellsDoNotWaitBehindBackToBackExpensiveCells — v2 arm 1.
// One unviewed worker (parallelism 2: one slot per side), six expensive cells
// (100 ms each) and three cheap cells (~1 ms), all re-marked continuously. With
// the cost-only floor the expensive cells are always due and the cheap cells
// queue behind several of them (~0.5 s); with the backlog charged, the expensive
// cells are parked long enough that a cheap cell waits at most about one
// expensive refresh. The bound (2.5 × one expensive cost) comes from the arm's own
// fake costs, never from a refresher counter.
func TestRefresher1570_CheapCellsDoNotWaitBehindBackToBackExpensiveCells(t *testing.T) {
	CompleteRBACSnapshotForTest(t)
	cleanup := withCleanRefresher(t, 2, 0)
	t.Cleanup(cleanup)
	t.Cleanup(SetRefresherRateFloorForTest(""))
	resetRefresherForTest()
	c := ResolvedCache()
	const expensiveCost = 100 * time.Millisecond
	var exp, cheap []string
	for i := 0; i < 6; i++ {
		in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "exp-1570-" + string(rune('a'+i))}
		k := ComputeKey(in)
		putAgedEntry(c, k, &in)
		exp = append(exp, k)
	}
	for i := 0; i < 3; i++ {
		in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "cheap-1570-" + string(rune('a'+i))}
		k := ComputeKey(in)
		putAgedEntry(c, k, &in)
		cheap = append(cheap, k)
	}
	isExp := map[string]bool{}
	for _, k := range exp {
		isExp[k] = true
	}
	// pending[k] is the earliest mark of k not yet served by a refresh start.
	var mu sync.Mutex
	pending := map[string]time.Time{}
	var cheapWaits []time.Duration
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, used ResolvedKeyInputs) error {
		now := time.Now()
		mu.Lock()
		if t0, ok := pending[k]; ok {
			if !isExp[k] {
				cheapWaits = append(cheapWaits, now.Sub(t0))
			}
			delete(pending, k)
		}
		mu.Unlock()
		gen := c.CaptureGen(k)
		rctx := WithL1KeyContext(ctx, k)
		if isExp[k] {
			time.Sleep(expensiveCost)
		} else {
			time.Sleep(time.Millisecond)
		}
		c.replaceIfGenRefresh(rctx, k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &used}, gen)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	mark := func(k string) {
		mu.Lock()
		if _, ok := pending[k]; !ok {
			pending[k] = time.Now()
		}
		mu.Unlock()
		enqueueRefreshForTest(k)
	}
	stop := time.Now().Add(4 * time.Second)
	for time.Now().Before(stop) {
		for _, k := range exp {
			mark(k)
		}
		for _, k := range cheap {
			mark(k)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	mu.Lock()
	ws := append([]time.Duration(nil), cheapWaits...)
	mu.Unlock()
	if len(ws) < 20 {
		t.Fatalf("SETUP: only %d cheap refreshes in 4 s", len(ws))
	}
	sort.Slice(ws, func(i, j int) bool { return ws[i] < ws[j] })
	p90 := ws[len(ws)*9/10]
	if bound := expensiveCost * 5 / 2; p90 > bound {
		t.Fatalf("cheap cells waited p90 %v (median %v, n=%d) behind the expensive ones: want ≤ %v "+
			"(the expensive cells re-resolve back to back; the backlog is not charged)", p90, ws[len(ws)/2], len(ws), bound)
	}
	t.Logf("GREEN: cheap-cell wait p50 %v p90 %v (n=%d) beside 6 expensive cells on one worker", ws[len(ws)/2], p90, len(ws))
}

// TestRefresher1570_ViewedCellIsNotFlooredByTheUnviewedBacklog — v2 arm 2. A
// viewed cell whose refresh costs 40 ms is re-marked 60 ms after its refresh
// while the unviewed side carries a deep backlog: its floor is its own cost (its
// side is not backlogged), so it re-resolves at once. Counting K over both sides
// would floor it by 40 ms × (backlog / slots), hundreds of ms.
func TestRefresher1570_ViewedCellIsNotFlooredByTheUnviewedBacklog(t *testing.T) {
	CompleteRBACSnapshotForTest(t)
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 4, 0)
	t.Cleanup(cleanup)
	t.Cleanup(SetRefresherRateFloorForTest(""))
	_, keys := terminalFixtureNoReset(t, 41)
	viewed, backlog := keys[0], keys[1:]
	viewed765Arm(t, viewed)
	var mu sync.Mutex
	starts := []time.Time{}
	release := make(chan struct{})
	t.Cleanup(sync.OnceFunc(func() { close(release) }))
	c := ResolvedCache()
	RegisterRefreshFunc("widgets", func(ctx context.Context, k string, used ResolvedKeyInputs) error {
		if k != viewed {
			<-release // the unviewed backlog: its workers stay busy, its queue deep
			return nil
		}
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		gen := c.CaptureGen(k)
		rctx := WithL1KeyContext(ctx, k)
		time.Sleep(40 * time.Millisecond)
		c.replaceIfGenRefresh(rctx, k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &used}, gen)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	terminalMark(backlog)
	terminalMark([]string{viewed})
	waitFor(t, 3*time.Second, "the viewed cell's first refresh", func() bool { mu.Lock(); defer mu.Unlock(); return len(starts) == 1 })
	if k, s := refresherSingleton().backlogShare(tierNormal); k <= 3*s {
		t.Fatalf("SETUP: the unviewed side must be deeply backlogged, got k=%d s=%d", k, s)
	}
	time.Sleep(100 * time.Millisecond) // the refresh (40 ms) ended ~60 ms ago
	marked := time.Now()
	terminalMark([]string{viewed})
	waitFor(t, 3*time.Second, "the viewed cell's second refresh", func() bool { mu.Lock(); defer mu.Unlock(); return len(starts) == 2 })
	mu.Lock()
	lat := starts[1].Sub(marked)
	mu.Unlock()
	if lat > 100*time.Millisecond {
		t.Fatalf("a viewed cell re-marked after its own 40 ms floor waited %v: the unviewed side's backlog floored it", lat)
	}
}
