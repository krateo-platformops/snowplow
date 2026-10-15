// nested_resolve_bound_falsifier_test.go — AGGREGATE OOM bound falsifiers
// (C1-C4, docs/oom-aggregate-adaptive-bound-2026-07-02.md). Successor to the
// #78 count-cap falsifiers (peak <= floor(budget/weight)), which are FORBIDDEN
// here: a count-cap does NOT prove the BYTE bound (the 1.5.1 180× lesson, and
// the #78 single-tree probe that passed while the N-tree page OOMed). These
// arms drive the gate through the runtime-headroom TEST SEAM (injected
// GOMEMLIMIT + a live-heap sampler that TRACKS admitted weight), NOT a
// resurrected env (C4), and assert on BYTES: peak modeled live heap stays under
// GOMEMLIMIT − reserve AND all N trees complete content-correct.
//
// The model: each admitted tree "allocates" estUnit of live heap for the
// duration it holds its admission; the injected liveHeapFn returns a base plus
// the sum of currently-admitted footprints. This is the faithful hermetic proxy
// for the real decode-into-map concurrent-tree footprint the gate must bound —
// the gate samples THIS live heap at each admission, so if it admits too many
// concurrently the modeled peak breaches the ceiling exactly as the real pod
// OOMed. Hermetic / in-process ONLY — no kind, no remote, no apiserver.
package api

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// headroomModel is the shared test seam state: a fixed GOMEMLIMIT + a live-heap
// counter that tracks the aggregate footprint of currently-admitted trees. Each
// admitted tree adds perTreeFootprint on entry (via the harness, mirroring the
// gate's own inFlight accounting) and subtracts on release; liveHeap() returns
// base + admittedFootprint, and peak records the high-water mark. The gate's
// admissionCeiling() samples liveHeap() → if it over-admits, peak breaches
// limit − reserve.
type headroomModel struct {
	limit           int64
	base            int64
	perTreeFootprint int64
	mu              sync.Mutex
	admittedFootprint int64
	peakLiveHeap    int64
}

func (h *headroomModel) limitFn() int64 { return h.limit }

func (h *headroomModel) liveFn() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	v := h.base + h.admittedFootprint
	if v > h.peakLiveHeap {
		h.peakLiveHeap = v
	}
	return v
}

// occupy/vacate model a tree's real footprint arriving AFTER admission and
// leaving on release — so the live-heap the NEXT admission samples reflects the
// trees already running. Called by the harness around the held region.
func (h *headroomModel) occupy() {
	h.mu.Lock()
	h.admittedFootprint += h.perTreeFootprint
	if h.base+h.admittedFootprint > h.peakLiveHeap {
		h.peakLiveHeap = h.base + h.admittedFootprint
	}
	h.mu.Unlock()
}

func (h *headroomModel) vacate() {
	h.mu.Lock()
	h.admittedFootprint -= h.perTreeFootprint
	if h.admittedFootprint < 0 {
		h.admittedFootprint = 0
	}
	h.mu.Unlock()
}

func (h *headroomModel) peak() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.peakLiveHeap
}

const mib = int64(1024 * 1024)

// runAggregateTrees launches n goroutines, each: enter the gate (depth-0),
// occupy() its footprint, hold, vacate(), release. Returns completed count and
// the count of ctx/admission errors. estUnit is pinned via calibration seed so
// the gate's weight == the model's perTreeFootprint (deterministic).
//
// #720: the trees are BACKGROUND (refresher-marked). Since customer trees
// never wait, the byte ceiling is a background-only property; the customer
// side is covered by the N1/N2/N4 arms in nested_resolve_bound_720_test.go.
func runAggregateTrees(t *testing.T, h *headroomModel, n int, hold time.Duration) (completed, errored int32) {
	t.Helper()
	ctx := bgTreeCtx()
	var done, errs int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := enterNestedResolveUnit(ctx)
			if err != nil {
				atomic.AddInt32(&errs, 1)
				return
			}
			h.occupy()
			time.Sleep(hold)
			h.vacate()
			release()
			atomic.AddInt32(&done, 1)
		}()
	}
	wg.Wait()
	return atomic.LoadInt32(&done), atomic.LoadInt32(&errs)
}

// pinEstUnit seeds the calibration so currentNestedEstUnit()==w deterministically
// (the gate weighs each admission by w). Uses the one-shot calibrate path.
func pinEstUnit(w int64) {
	calibrateNestedEstUnit(w)
}

// TestAggC1_ByteCeilingHeld_GREEN is the LOAD-BEARING C1 arm. N=20 concurrent
// outermost trees, each footprint 256 MiB, GOMEMLIMIT 8 GiB, reserve = 1/8 =
// 1 GiB → ceiling ≈ 7 GiB. N×footprint = 5 GiB (fits the raw limit) but the gate
// must keep the SAMPLED peak live heap under limit − reserve = 7 GiB throughout.
// Assert: (1) peak modeled live heap < limit − reserve (BYTE bound, not a count
// cap); (2) all N trees complete (bounded event still happens — no dropped tree).
//
// #720 N3 (background bounded): the trees are BACKGROUND, the only kind the
// gate still bounds. Mutation "no gate for background" (admit every background
// tree at once) → peak breaches → RED.
func TestAggC1_ByteCeilingHeld_GREEN(t *testing.T) {
	resetNestedResolveBoundForTest()
	t.Cleanup(resetNestedResolveBoundForTest)

	h := &headroomModel{
		limit:            8 * 1024 * mib, // 8 GiB
		base:             512 * mib,      // non-resolve baseline
		perTreeFootprint: 512 * mib,      // N×fp = 10 GiB ≫ ceiling 7 GiB (the page shape that OOMed)
	}
	setRuntimeSeamsForTest(h.limitFn, h.liveFn)
	pinEstUnit(h.perTreeFootprint)

	const N = 20
	completed, errored := runAggregateTrees(t, h, N, 25*time.Millisecond)

	if completed != N {
		t.Fatalf("C1/C2 bounded-event: completed=%d errored=%d, want all %d trees to complete "+
			"(the bound SERIALIZES, never drops)", completed, errored, N)
	}
	reserve := h.limit / reserveFractionDen * reserveFractionNum
	ceiling := h.limit - reserve
	if h.peak() >= ceiling {
		t.Fatalf("C1 BYTE-CEILING BREACHED: peak sampled live heap = %d MiB >= ceiling %d MiB "+
			"(limit %d MiB − reserve %d MiB) — the aggregate gate admitted too many concurrent "+
			"trees; the pod would OOM", h.peak()/mib, ceiling/mib, h.limit/mib, reserve/mib)
	}
	// Sanity: the workload MUST be capable of breaching (else the arm is
	// degenerate) — N × footprint + base must exceed the ceiling if unbounded.
	if h.base+int64(N)*h.perTreeFootprint <= ceiling {
		t.Fatalf("C1 DEGENERATE: N×footprint+base (%d MiB) <= ceiling (%d MiB) — the workload "+
			"cannot breach even unbounded; increase N/footprint", (h.base+int64(N)*h.perTreeFootprint)/mib, ceiling/mib)
	}
}

// TestAggC1_ByteCeilingBreached_RED proves the gate is LOAD-BEARING: force
// admit-unconditionally (simulate the removed gate) and the SAME N-tree workload
// drives peak live heap PAST the ceiling. This is the K>1 concurrency
// discriminator the #78 single-tree probe lacked.
func TestAggC1_ByteCeilingBreached_RED(t *testing.T) {
	resetNestedResolveBoundForTest()
	t.Cleanup(resetNestedResolveBoundForTest)

	h := &headroomModel{
		limit:            8 * 1024 * mib,
		base:             512 * mib,
		perTreeFootprint: 512 * mib, // N×fp = 10 GiB ≫ ceiling 7 GiB (same shape as GREEN)
	}
	// RED: GOMEMLIMIT "unlimited" makes the gate transparent (admit all) — the
	// exact behaviour of NO aggregate bound. The workload then over-admits.
	unlimited := func() int64 { return 1<<63 - 1 } // math.MaxInt64 → transparent
	setRuntimeSeamsForTest(unlimited, h.liveFn)
	pinEstUnit(h.perTreeFootprint)

	const N = 20
	completed, _ := runAggregateTrees(t, h, N, 25*time.Millisecond)
	if completed != N {
		t.Fatalf("RED control: completed=%d want %d (workload must still run)", completed, N)
	}
	// With the gate transparent, all N occupy concurrently → peak ≈ base + N×fp.
	realLimit := int64(8) * 1024 * mib
	ceiling := realLimit - realLimit/reserveFractionDen*reserveFractionNum
	if h.peak() < ceiling {
		t.Fatalf("C1 RED INVALID: unbounded peak %d MiB did NOT breach ceiling %d MiB — the "+
			"harness is not discriminating (N/footprint too small)", h.peak()/mib, ceiling/mib)
	}
}

// TestAggC1_ProgressWhenTight admits UNCONDITIONALLY when inFlight==0 even if a
// single tree "won't fit" (headroom < estUnit) — the anti-deadlock invariant.
// Live heap pinned already above the limit; a lone tree must STILL be admitted.
// #720: driven by a BACKGROUND tree, the only kind that can wait (a customer
// tree is admitted whatever the headroom, see N1).
func TestAggC1_ProgressWhenTight(t *testing.T) {
	resetNestedResolveBoundForTest()
	t.Cleanup(resetNestedResolveBoundForTest)

	h := &headroomModel{
		limit:            2 * 1024 * mib, // 2 GiB
		base:             3 * 1024 * mib, // ALREADY over the limit — negative headroom
		perTreeFootprint: 256 * mib,
	}
	setRuntimeSeamsForTest(h.limitFn, h.liveFn)
	pinEstUnit(h.perTreeFootprint)

	done := make(chan struct{})
	go func() {
		release, err := enterNestedResolveUnit(bgTreeCtx())
		if err == nil {
			release()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("progress invariant VIOLATED: a lone tree blocked forever under negative headroom " +
			"(inFlight==0 must admit unconditionally)")
	}
}

// TestAggC3_FallThroughHonest503 is the C3 fall-through arm, rewritten for a
// BACKGROUND tree (#720): headroom pinned low + one tree ALREADY in flight (so
// inFlight>0, the unconditional-admit escape does not apply) + the second
// background tree's ctx deadline expires while blocked → it returns the ctx
// error, NOT empty/raw content, NOT a dropped resolve. The caller
// (resolve_inprocess.go) propagates this err; for a background re-resolve that
// means nothing is Put and the existing cell keeps being served. A CUSTOMER
// tree never reaches this path: it never waits and never gets this error
// (TestIssue720_N1_CustomerNeverWaits).
func TestAggC3_FallThroughHonest503(t *testing.T) {
	resetNestedResolveBoundForTest()
	t.Cleanup(resetNestedResolveBoundForTest)

	h := &headroomModel{
		limit:            1024 * mib, // 1 GiB
		base:             0,
		perTreeFootprint: 900 * mib, // one tree ~fills headroom; a second cannot fit
	}
	setRuntimeSeamsForTest(h.limitFn, h.liveFn)
	pinEstUnit(h.perTreeFootprint)

	// Tree 1: admit and HOLD (occupies 900 MiB → ceiling = 1024 − 128 = 896 MiB,
	// so a second 900 MiB tree cannot fit while tree 1 is in flight).
	rel1, err1 := enterNestedResolveUnit(bgTreeCtx())
	if err1 != nil {
		t.Fatalf("C3 setup: tree 1 admission failed: %v", err1)
	}
	h.occupy()
	defer func() { h.vacate(); rel1() }()

	// Tree 2 (BACKGROUND): short ctx deadline; inFlight==1 so it does NOT get
	// the unconditional escape → it BLOCKS on insufficient headroom → ctx
	// expires → honest error.
	ctx2, cancel := context.WithTimeout(bgTreeCtx(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	rel2, err2 := enterNestedResolveUnit(ctx2)
	elapsed := time.Since(start)

	if err2 == nil {
		rel2()
		t.Fatalf("C3 FALL-THROUGH FAIL: tree 2 was ADMITTED despite insufficient headroom + a live "+
			"tree — it should have blocked then returned an honest ctx error (503), not admitted")
	}
	if elapsed < 100*time.Millisecond {
		t.Fatalf("C3: tree 2 returned err in %v — it did NOT block on the bound (expected ~ctx deadline)", elapsed)
	}
	// The error is the ctx deadline (honest, retryable) — NOT a nil release, NOT
	// empty content. resolve_inprocess.go returns (nil,false,err) on this.
	if ctx2.Err() == nil {
		t.Fatalf("C3: expected ctx deadline exceeded, got err=%v ctxErr=%v", err2, ctx2.Err())
	}
}

// TestAggC4_DrivenBySeamNotEnv confirms the gate is driven by the injected
// runtime seam (GOMEMLIMIT/liveHeap), NOT any env — proves the env is gone and
// the arms exercise the real headroom decision. Also asserts transparent
// pass-through when GOMEMLIMIT is "unset" (unlimited).
func TestAggC4_DrivenBySeamNotEnv(t *testing.T) {
	resetNestedResolveBoundForTest()
	t.Cleanup(resetNestedResolveBoundForTest)

	// Unlimited GOMEMLIMIT → transparent: admission always granted, no blocking,
	// release a safe no-op, and NO calibration pressure changes the decision.
	setRuntimeSeamsForTest(func() int64 { return 1<<63 - 1 }, func() int64 { return 4 * 1024 * mib })
	_, unlimited := admissionCeiling()
	if !unlimited {
		t.Fatal("C4: MaxInt64 GOMEMLIMIT must be classified unlimited (transparent)")
	}
	release, err := enterNestedResolveUnit(cache.WithNestedCallDepth(context.Background(), 0))
	if err != nil {
		t.Fatalf("C4 transparent: unexpected err %v", err)
	}
	release() // safe no-op

	// A finite limit engages the ceiling math off the SEAM.
	setRuntimeSeamsForTest(func() int64 { return 8 * 1024 * mib }, func() int64 { return 1024 * mib })
	ceiling, unlimited2 := admissionCeiling()
	if unlimited2 {
		t.Fatal("C4: finite GOMEMLIMIT must NOT be unlimited")
	}
	// ceiling = (8Gi − 1Gi liveHeap) − 8Gi/8 reserve = 7Gi − 1Gi = 6 GiB.
	wantCeiling := int64(8)*1024*mib - 1024*mib - int64(8)*1024*mib/reserveFractionDen*reserveFractionNum
	if ceiling != wantCeiling {
		t.Fatalf("C4: ceiling=%d MiB want %d MiB (headroom − reserve off the seam)", ceiling/mib, wantCeiling/mib)
	}
}

// TestAggCostProportional_WarmNotGated — a warm-hit-equivalent (never reaching
// the depth-0 gate) does not touch admission. The gate is only entered at
// depth-0; depth>0 (inner/inherited) is classified not-outermost so the caller
// skips enterNestedResolveUnit entirely. Assert the depth classification.
func TestAggCostProportional_WarmNotGated(t *testing.T) {
	if !isOutermostNestedResolve(cache.WithNestedCallDepth(context.Background(), 0)) {
		t.Fatal("depth 0 must be the outermost (gated) point")
	}
	for _, d := range []int{1, 2, 8} {
		if isOutermostNestedResolve(cache.WithNestedCallDepth(context.Background(), d)) {
			t.Fatalf("depth %d must NOT be gated (inner resolves inherit the one admission)", d)
		}
	}
}

// TestAggCalibration_OneShot verifies the C46-1 one-shot calibration: the first
// tree's measured live-heap delta becomes the weight; subsequent deltas do not
// overwrite it; a delta <= 0 is ignored.
func TestAggCalibration_OneShot(t *testing.T) {
	resetNestedResolveBoundForTest()
	t.Cleanup(resetNestedResolveBoundForTest)

	if _, cal := calibratedNestedEstUnitForTest(); cal {
		t.Fatal("fresh: must not be calibrated")
	}
	if w := currentNestedEstUnit(); w != defaultNestedEstUnitBytes {
		t.Fatalf("pre-calibration weight=%d want fallback %d", w, defaultNestedEstUnitBytes)
	}
	calibrateNestedEstUnit(100 * mib)
	if w, cal := currentNestedEstUnit(), true; w != 100*mib || !cal {
		t.Fatalf("post-calibration weight=%d want 100MiB", w)
	}
	calibrateNestedEstUnit(500 * mib) // must NOT overwrite (one-shot)
	if w := currentNestedEstUnit(); w != 100*mib {
		t.Fatalf("calibration overwritten: weight=%d want 100MiB (one-shot)", w)
	}
	calibrateNestedEstUnit(0)  // ignored
	calibrateNestedEstUnit(-5) // ignored
	if w := currentNestedEstUnit(); w != 100*mib {
		t.Fatalf("non-positive delta changed weight: %d", w)
	}
}

// TestAggNoGoroutineLeak_CtxWatcher asserts the sync.Cond ctx-watcher goroutine
// (waitForReleaseOrCtx spawns one per wait) does NOT leak. The discriminating
// scenario targets EXACTLY what close(stop) guards: a waiter whose Wait is woken
// by a RELEASE broadcast (NOT its own ctx) and then ADMITS — its ctx never fires,
// so the ONLY way its watcher exits is close(stop). Repeated sequentially, this
// accumulates one such waiter per iteration.
//
// Each iteration: tree A holds all headroom; a waiter B (background ctx, never
// cancelled) blocks (inFlight>0 + can't fit) → spawns a watcher; we release A →
// B is woken by A's release-Broadcast, re-checks, now fits (inFlight==0) →
// ADMITS, returns (ctx never fired), releases. With close(stop) B's watcher
// exits; WITHOUT it, B's watcher strands on <-stop forever → NumGoroutine climbs
// by ~1 per iteration.
//
// DISCRIMINATING (RED-then-GREEN, arch-required): GREEN with close(stop) intact
// (delta ≈ 0 across ITER iterations). RED with close(stop) removed — the
// release-woken-then-admitted watchers strand (delta ≈ ITER). Captured in the
// artifact by a manual neuter run (the stop-path is prod code, not a test seam).
// The arm ACTUALLY RUNS (=== RUN + in the -run 'TestAgg' count).
func TestAggNoGoroutineLeak_CtxWatcher(t *testing.T) {
	resetNestedResolveBoundForTest()
	t.Cleanup(resetNestedResolveBoundForTest)

	// Headroom fits EXACTLY one tree: base 200 + one 700 = 900 ≤ ceiling 896?
	// ceiling = 1024 − 128(reserve) = 896; base 100 + 700 = 800 ≤ 896 (one fits),
	// base 100 + 2×700 = 1500 > 896 (a second cannot while the first is in
	// flight). So B blocks while A holds, and admits once A releases.
	h := &headroomModel{
		limit:            1024 * mib,
		base:             100 * mib,
		perTreeFootprint: 700 * mib,
	}
	setRuntimeSeamsForTest(h.limitFn, h.liveFn)
	pinEstUnit(h.perTreeFootprint)

	settle := func() {
		for i := 0; i < 40; i++ {
			runtime.GC()
			time.Sleep(5 * time.Millisecond)
		}
	}
	settle()
	base := runtime.NumGoroutine()

	const ITER = 25
	for i := 0; i < ITER; i++ {
		// A holds all headroom.
		relA, errA := enterNestedResolveUnit(cache.WithNestedCallDepth(context.Background(), 0))
		if errA != nil {
			t.Fatalf("iter %d: A admission failed: %v", i, errA)
		}
		h.occupy()

		// B blocks (inFlight==1, base+2×700 > ceiling → can't fit). Background
		// ctx → B's ctx NEVER fires; its watcher can only exit via close(stop).
		bDone := make(chan struct{})
		go func() {
			// #720: B is BACKGROUND — the only kind that waits, so the only
			// kind that spawns a ctx watcher. A stays a customer tree, which
			// counts and so holds B back.
			relB, errB := enterNestedResolveUnit(bgTreeCtx())
			if errB == nil {
				h.occupy()
				h.vacate()
				relB()
			}
			close(bDone)
		}()

		// Ensure B is blocked (in its Wait) before releasing A.
		time.Sleep(20 * time.Millisecond)

		// Release A → Broadcast wakes B → B re-checks, now inFlight==0 → ADMITS
		// (its ctx never fired). B's watcher must exit via close(stop).
		h.vacate()
		relA()

		select {
		case <-bDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("iter %d: B never admitted after A released (release→broadcast→admit path stuck)", i)
		}
	}

	settle()
	after := runtime.NumGoroutine()

	// A LEAK strands ~ITER watchers (25). Slack for scheduler/runtime.
	const slack = 8
	if after > base+slack {
		t.Fatalf("GOROUTINE LEAK: NumGoroutine base=%d after=%d (delta %d > slack %d) — "+
			"the ctx-watcher goroutines of release-woken-then-admitted waiters did NOT exit "+
			"(%d iterations). The waitForReleaseOrCtx close(stop) path leaked.", base, after, after-base, slack, ITER)
	}
}
