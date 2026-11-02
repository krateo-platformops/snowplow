// refresher_viewed_reserve_1069_test.go — #1069: one refresher worker is always
// free for viewed work (the mirror of #799 fix 2).
//
// Run 20 (paired GKE, the freshness investigation): every worker was in a
// ~0.9 s unviewed re-resolve full-time, and the viewed tier was probed only when
// one of them ended, so a viewer waited 2.4–3.2 s p95 for a change (bar 1 s).
//
//   - R1 a viewed key starts within one wake while all N−1 unviewed slots are
//     busy on long jobs (they never end during the arm), with no wake key.
//   - R2 normal + cluster_list work never exceeds N−1 concurrent, and reaches it.
//   - R3 the viewed tier still never exceeds N−1 (#799 guard), and the last
//     worker still runs unviewed work beside it.
//   - R4 no busy-spin while there is no viewed work: the reserved worker parks
//     once over an idle window; with unviewed work flowing its parks stay
//     bounded by the work, not by time. R4b: no wake key is queued when no
//     worker waits in the normal Get.
//   - R5 parallelism=1 alternates strictly; R5b (deterministic, white-box) the
//     wake key is not a turn.
//   - R6 the customer yield (#854) still parks both tiers, the reserved
//     worker's viewed job included, and releases them together.
//   - R7 the new stats count exactly (per-tier completions, the dequeue age).
//   - R8 a shutdown releases the parked reserved worker.
package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func maxInto(m *atomic.Int32, n int32) {
	for cur := m.Load(); n > cur && !m.CompareAndSwap(cur, n); cur = m.Load() {
	}
}

// TestReserve1069_R1_ViewedStartsWhileUnviewedSlotsAreBusy is R1 (and R2's
// N−1 ceiling under long jobs).
func TestReserve1069_R1_ViewedStartsWhileUnviewedSlotsAreBusy(t *testing.T) {
	const p = 4
	terminalEnv(t)
	cleanup := withCleanRefresher(t, p, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 9)
	normal, v := keys[:8], keys[8]
	gate := make(chan struct{})
	var gateOnce sync.Once
	release := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(release)
	var inNormal, maxNormal atomic.Int32
	viewedStarted := make(chan time.Time, 1)
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		if k == v {
			select {
			case viewedStarted <- time.Now():
			default:
			}
			return nil
		}
		maxInto(&maxNormal, inNormal.Add(1))
		<-gate // a long unviewed job: it does not end during the arm
		inNormal.Add(-1)
		return nil
	})
	r := refresherSingleton()
	cq := &wakeCountingQueue{TypedRateLimitingInterface: r.queue}
	r.queue = cq // before the workers start: every normal-tier Get is counted
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	viewed765Arm(t, v)

	terminalMark(normal)
	waitFor(t, 5*time.Second, "N−1 unviewed jobs in flight", func() bool { return inNormal.Load() >= p-1 })
	time.Sleep(150 * time.Millisecond) // a 4th worker, if it may, takes a 4th job now
	if got := maxNormal.Load(); got > p-1 {
		t.Fatalf("R1 RED (#1069): %d unviewed jobs ran at once with %d workers — the last worker is not kept for viewed work (want ≤ %d)", got, p, p-1)
	}
	parks0 := RefresherStatsByStat()["viewed_reserve_parks"]
	wakes0 := cq.wakes.Load()
	t0 := time.Now()
	terminalMark([]string{v})
	var started time.Time
	select {
	case started = <-viewedStarted:
	case <-time.After(3 * time.Second):
		t.Fatalf("R1 RED (#1069): with %d unviewed jobs in flight a viewed key did not start within 3s — it waits for an unviewed job to end", inNormal.Load())
	}
	if n := inNormal.Load(); n != p-1 {
		t.Fatalf("PRECONDITION: %d unviewed jobs in flight when the viewed key started, want %d (none may have ended)", n, p-1)
	}
	if w := cq.wakes.Load() - wakes0; w != 0 {
		t.Fatalf("R1 RED (#1069): the viewed key needed %d wake-key dequeues — the reserved worker must be woken directly, not through the normal queue", w)
	}
	// The reserved worker woke once, ran the key, and parked again: one park.
	waitFor(t, 2*time.Second, "the reserved worker parked again", func() bool {
		return RefresherStatsByStat()["viewed_reserve_parks"] > parks0
	})
	time.Sleep(100 * time.Millisecond)
	if d := RefresherStatsByStat()["viewed_reserve_parks"] - parks0; d != 1 {
		t.Fatalf("R1 RED (#1069): the reserved worker parked %d times around one viewed key, want 1 (one wake, one run)", d)
	}
	t.Logf("R1: viewed key started %v after its mark with %d unviewed jobs in flight", started.Sub(t0), p-1)
}

// TestReserve1069_R2_UnviewedNeverHoldsTheLastWorker is R2: normal and
// cluster_list work together, short jobs, constant supply.
func TestReserve1069_R2_UnviewedNeverHoldsTheLastWorker(t *testing.T) {
	const p = 4
	terminalEnv(t)
	cleanup := withCleanRefresher(t, p, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 24)
	clKeys, normal := keys[:12], keys[12:]
	var in, maxIn, done atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		maxInto(&maxIn, in.Add(1))
		time.Sleep(15 * time.Millisecond)
		in.Add(-1)
		done.Add(1)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	for _, k := range clKeys {
		RegisterClusterListKey(k)
	}
	terminalMark(append(append([]string{}, clKeys...), normal...))
	waitFor(t, 10*time.Second, "all 24 unviewed keys refreshed", func() bool { return done.Load() >= 24 })
	if got := maxIn.Load(); got > p-1 {
		t.Fatalf("R2 RED (#1069): %d normal/cluster_list jobs ran at once with %d workers, want ≤ %d", got, p, p-1)
	}
	if got := maxIn.Load(); got < p-1 {
		t.Fatalf("R2: only %d unviewed jobs ever ran at once, want %d — the unviewed side must keep all its N−1 workers", got, p-1)
	}
	t.Logf("R2: max %d unviewed jobs at once of %d workers", maxIn.Load(), p)
}

// TestReserve1069_R3_ViewedStillCappedAtNMinus1 is R3 (#799 fix 2 under #1069).
func TestReserve1069_R3_ViewedStillCappedAtNMinus1(t *testing.T) {
	const p = 4
	terminalEnv(t)
	cleanup := withCleanRefresher(t, p, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 9)
	viewed, n := keys[:8], keys[8]
	isViewed := map[string]bool{}
	for _, k := range viewed {
		isViewed[k] = true
	}
	gate := make(chan struct{})
	var gateOnce sync.Once
	release := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(release)
	var inViewed, maxViewed atomic.Int32
	normalRan := make(chan struct{})
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		if isViewed[k] {
			maxInto(&maxViewed, inViewed.Add(1))
			<-gate
			inViewed.Add(-1)
			return nil
		}
		close(normalRan)
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	viewed765Arm(t, viewed...)
	terminalMark(viewed)
	waitFor(t, 5*time.Second, "N−1 viewed jobs in flight", func() bool { return inViewed.Load() >= p-1 })
	time.Sleep(150 * time.Millisecond)
	if got := maxViewed.Load(); got > p-1 {
		t.Fatalf("R3 RED (#799 fix 2): %d viewed jobs ran at once with %d workers, want ≤ %d", got, p, p-1)
	}
	terminalMark([]string{n})
	select {
	case <-normalRan:
	case <-time.After(3 * time.Second):
		t.Fatalf("R3 RED (#799 fix 2): with %d viewed jobs in flight an unviewed key did not run within 3s", inViewed.Load())
	}
	release()
}

// TestReserve1069_R4_ReservedWorkerDoesNotSpin is R4.
func TestReserve1069_R4_ReservedWorkerDoesNotSpin(t *testing.T) {
	const p = 4
	terminalEnv(t)
	cleanup := withCleanRefresher(t, p, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, p-1+40)
	long, short := keys[:p-1], keys[p-1:]
	isLong := map[string]bool{}
	for _, k := range long {
		isLong[k] = true
	}
	gate := make(chan struct{})
	var gateOnce sync.Once
	release := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(release)
	var inLong, shortDone atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		if isLong[k] {
			inLong.Add(1)
			<-gate
			return nil
		}
		time.Sleep(2 * time.Millisecond)
		shortDone.Add(1)
		return nil
	})
	r := refresherSingleton()
	cq := &wakeCountingQueue{TypedRateLimitingInterface: r.queue}
	r.queue = cq
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)

	// Idle: every unviewed slot in a long job, no viewed work anywhere.
	terminalMark(long)
	waitFor(t, 5*time.Second, "N−1 long unviewed jobs in flight", func() bool { return inLong.Load() == p-1 })
	time.Sleep(200 * time.Millisecond)
	parks0, wakes0 := RefresherStatsByStat()["viewed_reserve_parks"], cq.wakes.Load()
	time.Sleep(time.Second)
	parks, wakes := RefresherStatsByStat()["viewed_reserve_parks"]-parks0, cq.wakes.Load()-wakes0
	if parks > 1 || wakes > 1 {
		t.Fatalf("R4 RED (#1069): over an idle 1s with no viewed work the reserved worker parked %d times and dequeued %d wake keys — "+
			"it spins (want ≤ 1 each)", parks, wakes)
	}
	if total := RefresherStatsByStat()["viewed_reserve_parks"]; total < 1 {
		t.Fatalf("PRECONDITION: the reserved worker never parked (viewed_reserve_parks=%d) — no worker was held back", total)
	}

	// Flowing: the long jobs end and 40 short unviewed jobs run. The parks are
	// bounded by the work (a worker parks at most once per job it ends), never
	// by elapsed time.
	release()
	terminalMark(short)
	waitFor(t, 10*time.Second, "40 short unviewed jobs done", func() bool { return shortDone.Load() >= 40 })
	time.Sleep(100 * time.Millisecond)
	st := RefresherStatsByStat()
	if parks, done := st["viewed_reserve_parks"], st["normal_completed"]; parks > done+p {
		t.Fatalf("R4 RED (#1069): %d reserved parks for %d unviewed completions — parks must be bounded by the work", parks, done)
	}
}

// TestReserve1069_R4b_NoWakeKeyWhenNoWorkerWaitsInTheNormalGet is R4's wake-key
// half: with every worker in a job (three unviewed, the reserved one in a viewed
// job), a second viewed key queues no wake key — nobody waits in the normal Get
// to take it, and one left there sits in the normal FIFO as phantom work. The
// key still runs as soon as a worker ends its job (it re-probes the viewed tier).
func TestReserve1069_R4b_NoWakeKeyWhenNoWorkerWaitsInTheNormalGet(t *testing.T) {
	const p = 4
	terminalEnv(t)
	cleanup := withCleanRefresher(t, p, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 10)
	short, long, v1, v2 := keys[:5], keys[5:8], keys[8], keys[9]
	isLong := map[string]bool{}
	for _, k := range long {
		isLong[k] = true
	}
	gate := make(chan struct{})
	var gateOnce sync.Once
	release := func() { gateOnce.Do(func() { close(gate) }) }
	t.Cleanup(release)
	var shortDone, inLong atomic.Int32
	v1In, v2Ran := make(chan struct{}), make(chan struct{})
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		switch {
		case k == v1:
			close(v1In)
			<-gate
		case k == v2:
			close(v2Ran)
		case isLong[k]:
			inLong.Add(1)
			<-gate
		default:
			shortDone.Add(1)
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	viewed765Arm(t, v1, v2)
	terminalMark(short) // unviewed dequeues first: each one's Get must end its "waiting" count
	waitFor(t, 5*time.Second, "5 short unviewed jobs done", func() bool { return shortDone.Load() == 5 })
	terminalMark(long)
	waitFor(t, 5*time.Second, "3 long unviewed jobs in flight", func() bool { return inLong.Load() == p-1 })
	terminalMark([]string{v1})
	viewed765Wait(t, v1In, "the reserved worker never started v1")
	r := refresherSingleton()
	terminalMark([]string{v2}) // every worker busy
	time.Sleep(100 * time.Millisecond)
	if n := r.queue.Len(); n != 0 {
		t.Fatalf("R4b RED (#1069): %d item(s) in the normal queue after a viewed mark with every worker in a job — "+
			"a wake key was queued with no worker in the normal Get to take it", n)
	}
	release()
	select {
	case <-v2Ran:
	case <-time.After(3 * time.Second):
		t.Fatalf("R4b: v2 did not run within 3s of the workers' jobs ending")
	}
}

// TestReserve1069_R5_SingleWorkerAlternatesStrictly is R5: parallelism=1 as
// defined — after a viewed dequeue the next one goes to the other tiers when
// they have work, and the other way round; the wake key is not a turn.
func TestReserve1069_R5_SingleWorkerAlternatesStrictly(t *testing.T) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 1, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 6)
	n0, v1, v2, n1, n2, n3 := keys[0], keys[1], keys[2], keys[3], keys[4], keys[5]
	name := map[string]string{n0: "n0", v1: "v1", v2: "v2", n1: "n1", n2: "n2", n3: "n3"}
	firstIn := make(chan struct{})
	gate := make(chan struct{})
	var mu sync.Mutex
	var order []string
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		mu.Lock()
		order = append(order, name[k])
		mu.Unlock()
		if k == n0 {
			close(firstIn)
			<-gate
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	viewed765Arm(t, v1, v2)
	terminalMark([]string{n0})
	viewed765Wait(t, firstIn, "the first unviewed job never started")
	// Viewed marks FIRST: their wake key lands in the normal queue ahead of the
	// normal keys, where a wake that counted as a turn would let v2 run right
	// after v1 while n1..n3 wait.
	terminalMark([]string{v1, v2})
	terminalMark([]string{n1, n2, n3})
	close(gate)
	waitFor(t, 5*time.Second, "all six keys refreshed", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 6
	})
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	want := []string{"n0", "v1", "n1", "v2", "n2", "n3"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("R5 RED (#1069): one worker ran %v, want %v — with both tiers holding work the tiers alternate, "+
				"and the wake key is not a turn", got, want)
		}
	}
}

// TestReserve1069_R5b_WakeKeyIsNotATurn is R5's wake-key half, deterministic:
// with one worker, after a viewed job, the worker is blocked in the normal Get
// when a viewed key and a normal key arrive together and the wake key reaches it
// first. The normal key must run next (the viewed tier just had its turn); a
// wake key that counted as the normal turn would run the viewed key first.
// White-box: the arm holds priorityGetMu to land the three arrivals in that
// order (no wake can be decided while it is held).
func TestReserve1069_R5b_WakeKeyIsNotATurn(t *testing.T) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 1, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, 3)
	v0, v1, n1 := keys[0], keys[1], keys[2]
	name := map[string]string{v0: "v0", v1: "v1", n1: "n1"}
	var mu sync.Mutex
	var order []string
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		mu.Lock()
		order = append(order, name[k])
		mu.Unlock()
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	viewed765Arm(t, v0, v1)
	r := refresherSingleton()
	terminalMark([]string{v0})
	blockedAfterViewed := func() bool {
		r.priorityGetMu.Lock()
		defer r.priorityGetMu.Unlock()
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 1 && r.normalGetBlocked == 1 && r.lastViewed.Load()
	}
	waitFor(t, 5*time.Second, "the worker ran v0 and blocked in the normal Get", blockedAfterViewed)

	r.priorityGetMu.Lock()
	r.queue.Add(viewedWakeKey) // the wake reaches the blocked worker first
	waitFor(t, 5*time.Second, "the worker took the wake key", func() bool { return r.queue.Len() == 0 })
	marked := make(chan struct{})
	go func() { terminalMark([]string{v1}); close(marked) }() // its own wake waits for the lock
	waitFor(t, 5*time.Second, "v1 queued", func() bool { return r.viewedQueue.Len() == 1 })
	terminalMark([]string{n1})
	r.priorityGetMu.Unlock()
	<-marked
	waitFor(t, 5*time.Second, "v1 and n1 refreshed", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 3
	})
	mu.Lock()
	got := append([]string(nil), order...)
	mu.Unlock()
	if got[1] != "n1" {
		t.Fatalf("R5b RED (#1069): one worker ran %v, want [v0 n1 v1] — after a viewed job the next one goes to the normal tier; "+
			"the wake key is not that turn", got)
	}
}

// TestReserve1069_R6_CustomerYieldParksBothTiers is R6 (#854 unchanged).
func TestReserve1069_R6_CustomerYieldParksBothTiers(t *testing.T) {
	const p = 4
	terminalEnv(t)
	cleanup := withCleanRefresher(t, p, 0)
	t.Cleanup(cleanup)
	_, keys := terminalFixtureNoReset(t, p)
	normal, v := keys[:p-1], keys[p-1]
	var inflight atomic.Bool
	inflight.Store(true)
	customerHookInstall(t, func() bool { return inflight.Load() })
	var ran, viewedRan atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, _ ResolvedKeyInputs) error {
		ran.Add(1)
		if k == v {
			viewedRan.Add(1)
		}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	viewed765Arm(t, v)
	terminalMark(normal)
	terminalMark([]string{v})
	waitFor(t, 5*time.Second, "every worker parked for the customer", func() bool {
		return RefresherStatsByStat()["yielded"] >= p
	})
	time.Sleep(200 * time.Millisecond)
	if n := ran.Load(); n != 0 {
		t.Fatalf("R6 RED (#854): %d refreshes ran while a customer request was in flight — both tiers must yield", n)
	}
	st := RefresherStatsByStat()
	if st["yielded"] != p || st["viewed_dequeued"] != 1 {
		t.Fatalf("R6: yielded=%d viewed_dequeued=%d, want %d parks (%d unviewed + the reserved worker's viewed job) and 1 viewed dequeue",
			st["yielded"], st["viewed_dequeued"], p, p-1)
	}
	inflight.Store(false)
	waitFor(t, 5*time.Second, "every parked refresh ran after the customer ended", func() bool { return ran.Load() == p })
	if viewedRan.Load() != 1 {
		t.Fatalf("R6: the viewed key ran %d times, want 1", viewedRan.Load())
	}
	if c := RefresherStatsByStat()["capped"]; c != 0 {
		t.Fatalf("R6: capped=%d, want 0 (the yield released on the customer's end, not on its cap)", c)
	}
}

// TestReserve1069_R8_ShutdownReleasesTheReservedWorker is R8: a parked worker
// learns of the shutdown (a worker in a queue's Get learns it from the Get).
func TestReserve1069_R8_ShutdownReleasesTheReservedWorker(t *testing.T) {
	const p = 4
	terminalEnv(t)
	cleanup := withCleanRefresher(t, p, 0)
	t.Cleanup(cleanup)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	waitFor(t, 5*time.Second, "the reserved worker parked", func() bool {
		return RefresherStatsByStat()["viewed_reserve_parks"] >= 1
	})
	r := refresherSingleton()
	cancel() // production's stop: the ctx goroutine shuts every tier down
	done := make(chan struct{})
	go func() { r.workersWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("R8 RED (#1069): after the refresher's ctx ended a worker did not exit within 3s — the parked reserved worker never learns of the shutdown")
	}
}

// TestReserve1069_R7_StatsCountExactly is R7: completions by tier and the age of
// the change a dequeue runs, by side, on the virtual dirty-window clock.
func TestReserve1069_R7_StatsCountExactly(t *testing.T) {
	terminalEnv(t)
	cleanup := withCleanRefresher(t, 2, 0)
	t.Cleanup(cleanup)
	var nowMu sync.Mutex
	now := time.Unix(1_800_000_000, 0)
	restore := SetDirtyWindowClockForTest(func() time.Time { nowMu.Lock(); defer nowMu.Unlock(); return now })
	t.Cleanup(restore)
	advance := func(d time.Duration) { nowMu.Lock(); now = now.Add(d); nowMu.Unlock() }
	_, keys := terminalFixtureNoReset(t, 5)
	normal, viewed := keys[:3], keys[3:]
	RegisterRefreshFunc("widgets", func(context.Context, string, ResolvedKeyInputs) error { return nil })
	viewed765Arm(t, viewed...)
	// Marked before the pool starts, on the frozen clock: the normal changes are
	// 700 ms old at their dequeue, the viewed ones 400 ms.
	terminalMark(normal)
	advance(300 * time.Millisecond)
	terminalMark(viewed)
	advance(400 * time.Millisecond)
	base := RefresherWorkSnapshot()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	StartRefresher(ctx)
	if _, err := WaitRefresherQuiescentForTest(base, 10*time.Second); err != nil {
		t.Fatalf("PRECONDITION: %v", err)
	}
	st := RefresherStatsByStat()
	for stat, want := range map[string]int64{
		"normal_completed":          3,
		"viewed_completed":          2,
		"completed":                 5,
		"unviewed_dirty_age_ms_max": 700,
		"unviewed_dirty_age_ms_p95": 700,
		"viewed_dirty_age_ms_max":   400,
		"viewed_dirty_age_ms_p95":   400,
	} {
		if got := st[stat]; got != want {
			t.Errorf("R7: %s = %d, want %d", stat, got, want)
		}
	}
}
