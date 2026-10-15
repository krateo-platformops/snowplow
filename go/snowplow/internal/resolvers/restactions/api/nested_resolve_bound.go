// nested_resolve_bound.go — (c) cold-fan-out AGGREGATE-footprint bound on the
// nested-resolve path. Successor to the #78 per-outermost-tree byte-budget
// semaphore (magic env NESTED_RESOLVE_FOOTPRINT_BUDGET_BYTES), which was
// per-tree and therefore did NOT bound the compositions-page fan-out: the page
// fires MANY concurrent outermost trees, each admitted under the per-tree
// budget, whose AGGREGATE simultaneously-live footprint blew the 8Gi cgroup
// ceiling → OOMKill(137) on 1.5.27 (regression-journal 2026-07-02). This
// replaces the fixed byte budget with a PROCESS-WIDE, self-adapting admission
// gate keyed off the runtime's ACTUAL headroom (GOMEMLIMIT − live heap),
// sampled synchronously at each admission (docs/oom-aggregate-adaptive-bound-2026-07-02.md).
//
// MECHANISM (Q3 shape A — recompute-per-admission, NOT a fixed-capacity
// semaphore):
//   - At each OUTERMOST nested resolve (depth-0), sample headroom =
//     GOMEMLIMIT(debug.SetMemoryLimit(-1)) − liveHeap(/memory/classes/heap/objects:bytes
//     via runtime/metrics — a cheap sample, no ReadMemStats stop-the-world).
//   - A CUSTOMER tree NEVER WAITS (#720, owner ruling 2026-10-15, "No one
//     waits in queue" → "Remove it too"): it adds its estUnit to
//     inFlightWeight/inFlightCount and proceeds at once, whatever the
//     headroom. It still COUNTS, so while customer trees are in flight the
//     background trees are squeezed harder. How far customer trees push the
//     in-flight weight past the ceiling is published as
//     snowplow_nested_resolve_admission_over_ceiling_bytes (+ _peak), the
//     early warning for the risk the owner accepted (see below).
//   - A BACKGROUND tree (refresher, prewarm, seed, boot walk) is admitted IFF
//     inFlightWeight + estUnit <= headroom − reserve.
//   - inFlight == 0 ⇒ ADMIT UNCONDITIONALLY (anti-deadlock: a tree already in
//     flight holds the live footprint and WILL release; blocking the only tree
//     forever is a deadlock — mirrors the #78 clamp). Guaranteed progress.
//   - Otherwise a background tree SERIALIZES: it waits (bounded by its own
//     ctx) until an in-flight tree releases and re-checks. On ctx
//     cancel/deadline it returns the ctx error; the background re-resolve fails
//     and the existing cell keeps being served. The bound BLOCKS background,
//     never DROPS: every admitted tree resolves fully + content-correct, just
//     later. No customer request can receive this admission error.
//
// ⚠ ACCEPTED RISK (#720, owner decision): customer bypass RE-EXPOSES the
// 1.5.27 OOMKill shape. A burst of cold composite pages fans out many
// concurrent outermost customer trees; none waits, so their aggregate
// footprint is no longer bounded by this gate. The owner accepted that subject
// to bench run 8 (#606), which must include a cold composite-page burst and
// report the peak heap and the peak of over_ceiling_bytes.
//
// PLACEMENT — OUTERMOST nested-resolve ONLY (depth-0), AFTER the apistage
// cache-hit serve. A warm apistage/L1 hit is served before nestedCallResolver
// is ever reached, so it NEVER enters the gate → cost-proportional: only a
// genuine COLD nested resolve pays (feedback_bounding_mechanism_discipline).
// Inner recursive resolves (depth>0) inherit the ONE admission the outermost
// entry holds — no per-node accounting, no self-deadlock. Same placement
// invariant #78 satisfied; inherited unchanged.
//
// WEIGHT (estUnit, C46-1 self-calibrating — NOT a design constant, the 1.5.1
// 180× lesson): the per-tree admission weight is calibrated from the FIRST
// tree's MEASURED live-heap delta (one-shot), reused after. Before calibration
// (and as the conservative fallback) it is defaultNestedEstUnitBytes = 256 MiB
// (≥5× the observed ~47MB/tree). This is an empirical-per-entry-cost weight
// (feedback_capacity_caps_empirical_per_entry_cost), not an operator dial.
//
// ZERO-KNOB (Diego 2026-07-02): no NESTED_RESOLVE_* env remains. reserve is a
// documented code-constant FRACTION of GOMEMLIMIT (a dimensionless
// headroom-share, not a byte budget); the est-unit fallback is a documented
// code constant. GOMEMLIMIT is read via debug.SetMemoryLimit(-1) (respects the
// env/cgroup-derived value) — no env re-parse.
//
// TRANSPARENT when GOMEMLIMIT is effectively unlimited (unset → the runtime
// default math.MaxInt64): headroom is unbounded, admission is always granted,
// release is a no-op — byte-identical to no bound (project_caching_is_provisional:
// cleanly removable). A pod SHOULD set GOMEMLIMIT (the chart does, 7GiB); with
// it unset there is no soft ceiling to protect and the gate correctly no-ops.
package api

import (
	"context"
	"math"
	"runtime/debug"
	"runtime/metrics"
	"sync"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

const (
	// defaultNestedEstUnitBytes is the CONSERVATIVE-HIGH per-tree admission
	// weight used before the one-shot empirical calibration lands (C46-1).
	// Over-reserve by design: a too-high fallback serializes a touch more
	// (safe); a too-low one admits too much aggregate (the 1.5.1 under-estimate
	// failure mode). 256 MiB ≥5× the observed ~47MB/tree; documented code
	// constant, NOT an env (zero-knob, Diego 2026-07-02).
	defaultNestedEstUnitBytes int64 = 256 * 1024 * 1024

	// reserveFractionNum/reserveFractionDen express `reserve` as a FRACTION of
	// GOMEMLIMIT held back for NON-resolve work (informer caches, HTTP buffers,
	// the L1/apistage stores, GC working set). 1/8 = 12.5% headroom-share. This
	// is a dimensionless share of the pod's own limit — it scales with pod size
	// and is defensible as "keep headroom for the rest of the process", unlike
	// an absolute byte budget. Documented code constant (Option 1a, zero-knob).
	reserveFractionNum int64 = 1
	reserveFractionDen int64 = 8
)

// runtimeLimitFn / liveHeapFn are the runtime-headroom TEST SEAMS. Production
// wires them to debug.SetMemoryLimit(-1) and the runtime/metrics live-heap
// sample; falsifiers inject deterministic GOMEMLIMIT / liveHeap WITHOUT a
// resurrected env (C4). Guarded by boundMu when swapped in tests.
var (
	runtimeLimitFn = prodRuntimeMemoryLimit
	liveHeapFn     = prodLiveHeapBytes
)

// prodRuntimeMemoryLimit reads the effective GOMEMLIMIT (soft limit) via
// debug.SetMemoryLimit(-1), which returns the current limit without changing
// it (respects the env/cgroup-derived value). The runtime default when unset is
// math.MaxInt64 (effectively unlimited).
func prodRuntimeMemoryLimit() int64 {
	return debug.SetMemoryLimit(-1)
}

// prodLiveHeapBytes samples live heap objects via runtime/metrics
// (/memory/classes/heap/objects:bytes) — a cheap read, no stop-the-world
// (unlike runtime.ReadMemStats). This is the live (in-use) heap, the quantity
// GC cannot shrink while trees are in flight — the OOM-relevant denominator.
func prodLiveHeapBytes() int64 {
	const key = "/memory/classes/heap/objects:bytes"
	sample := []metrics.Sample{{Name: key}}
	metrics.Read(sample)
	if sample[0].Value.Kind() != metrics.KindUint64 {
		return 0
	}
	v := sample[0].Value.Uint64()
	if v > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(v)
}

// aggregateBound is the process-wide admission gate. inFlightWeight is the sum
// of the estUnit weights of currently-admitted outermost trees, customer AND
// background; cond wakes background waiters on each release so they re-check
// headroom. customerWeight is the part of inFlightWeight that customer trees
// hold: it is what the over-ceiling gauge attributes the excess to (#720).
// No customer ever waits on cond, so the old waitingCustomers yield (C5) is
// gone: there is never a waiting customer to yield to.
type aggregateBound struct {
	mu             sync.Mutex
	cond           *sync.Cond
	inFlightWeight int64
	inFlightCount  int
	customerWeight int64
}

var (
	boundMu    sync.Mutex
	boundOnce  *aggregateBound

	// estUnit calibration (C46-1). Starts at the conservative-high fallback;
	// replaced ONCE by the first tree's measured live-heap delta.
	estUnitMu         sync.Mutex
	estUnitBytes      int64
	estUnitCalibrated bool
)

func theBound() *aggregateBound {
	boundMu.Lock()
	defer boundMu.Unlock()
	if boundOnce == nil {
		boundOnce = &aggregateBound{}
		boundOnce.cond = sync.NewCond(&boundOnce.mu)
	}
	return boundOnce
}

// currentNestedEstUnit returns the per-tree admission weight: the calibrated
// value once available, else the conservative-high code-constant fallback.
func currentNestedEstUnit() int64 {
	estUnitMu.Lock()
	est := estUnitBytes
	calibrated := estUnitCalibrated
	estUnitMu.Unlock()
	if !calibrated || est <= 0 {
		return defaultNestedEstUnitBytes
	}
	return est
}

// calibrateNestedEstUnit records the first tree's MEASURED live-heap delta as
// the empirical per-tree weight (C46-1, one-shot). A delta <= 0 (GC ran
// mid-tree, or a trivial tree) is ignored so we don't calibrate to an
// unrealistically small weight.
func calibrateNestedEstUnit(measuredDelta int64) {
	if measuredDelta <= 0 {
		return
	}
	estUnitMu.Lock()
	if !estUnitCalibrated {
		estUnitBytes = measuredDelta
		estUnitCalibrated = true
	}
	estUnitMu.Unlock()
}

// admissionCeiling returns (ceiling, unlimited). ceiling = headroom − reserve =
// (GOMEMLIMIT − liveHeap) − GOMEMLIMIT/reserveFraction, the max aggregate
// in-flight weight admittable right now. unlimited==true when GOMEMLIMIT is the
// runtime's unset default (math.MaxInt64) → no soft ceiling to protect, the
// gate is transparent.
func admissionCeiling() (ceiling int64, unlimited bool) {
	limit := runtimeLimitFn()
	if limit <= 0 || limit == math.MaxInt64 {
		return 0, true // GOMEMLIMIT unset/unlimited — transparent pass-through.
	}
	reserve := limit / reserveFractionDen * reserveFractionNum
	headroom := limit - liveHeapFn()
	ceiling = headroom - reserve
	if ceiling < 0 {
		ceiling = 0
	}
	return ceiling, false
}

// isBackgroundNestedTree classifies an outermost tree for the gate. BACKGROUND
// is any non-customer driver: the refresher, the prewarm engine and the cohort
// seed (WithBackgroundResolve / WithBackgroundResolveOrigin), the content
// prewarm (WithApistagePrewarm) and the phase-1 boot walk, whose only marker is
// WithPrewarmPath (#214: "this resolve is a prewarm walk, not a served
// request"). A customer /call carries none of them. The "who is resolving"
// question has ONE answer in the cache package (#563): cache.RefDenialOrigin
// returns RefDenialOriginServe only for a customer, and every other origin
// (background origin, prewarm path, unattributed background) is not one. The
// content-prewarm marker is kept beside it, as the gate read it before. The
// classification is by driver, never by resource.
func isBackgroundNestedTree(ctx context.Context) bool {
	return cache.RefDenialOrigin(ctx) != cache.RefDenialOriginServe ||
		cache.ApistagePrewarmFromContext(ctx)
}

// enterNestedResolveUnit is the aggregate cold-fan-out admission gate, applied
// as a lifecycle bracket at the OUTERMOST nested resolve. The caller MUST gate
// it on the depth-0 condition (isOutermostNestedResolve) so the whole subtree
// holds exactly ONE admission — inner recursive resolves (depth>0) do NOT
// re-enter.
//
// Admission (#720, "users never queue"):
//   - a CUSTOMER tree NEVER WAITS and never returns an error: it adds estUnit
//     to inFlightWeight/inFlightCount (and customerWeight), refreshes the
//     over-ceiling gauge, and proceeds. Its ctx is not consulted: a near-expired
//     customer ctx is the resolve's business, never an admission 503.
//   - a BACKGROUND tree admits iff unlimited GOMEMLIMIT (transparent) ||
//     inFlightCount == 0 (guaranteed progress / anti-deadlock: the only tree
//     runs ALONE even if it "won't fit") || inFlightWeight + estUnit <= ceiling.
//     Otherwise it WAITS, bounded by its OWN ctx, on a release and re-checks;
//     on ctx cancel/deadline it returns the ctx error (the background re-resolve
//     fails, nothing is Put, the existing cell keeps being served).
//
// COUNTS-WHILE-IN-FLIGHT (load-bearing): customer and background trees weigh
// against inFlightWeight/inFlightCount identically. A customer tree in flight
// is a non-empty gate that will release, so a background tree behind it waits
// for room (squeezed) without deadlocking.
//
// On admission a live-heap sample is taken; release() re-samples, calibrates
// the per-tree weight (one-shot), decrements, refreshes the over-ceiling gauge
// and broadcasts to wake background waiters.
func enterNestedResolveUnit(ctx context.Context) (release func(), err error) {
	b := theBound()
	est := currentNestedEstUnit()
	background := isBackgroundNestedTree(ctx)

	b.mu.Lock()
	if !background {
		// Customer bypass: account and go. Never waits, never errors.
		b.inFlightWeight += est
		b.inFlightCount++
		b.customerWeight += est
		b.observeOverCeilingLocked()
		b.mu.Unlock()
	} else {
		for {
			ceiling, unlimited := admissionCeiling()
			fits := b.inFlightWeight+est <= ceiling
			if unlimited || b.inFlightCount == 0 || fits {
				b.inFlightWeight += est
				b.inFlightCount++
				b.observeOverCeilingLocked()
				b.mu.Unlock()
				break
			}
			if werr := b.waitForReleaseOrCtx(ctx); werr != nil {
				b.mu.Unlock()
				return nil, werr
			}
			if cerr := ctx.Err(); cerr != nil {
				b.mu.Unlock()
				return nil, cerr
			}
			// woken (release or ctx) — loop re-checks ceiling + ctx.
		}
	}

	before := liveHeapFn()
	var released bool
	return func() {
		b.mu.Lock()
		if released {
			b.mu.Unlock()
			return
		}
		released = true
		after := liveHeapFn()
		if after > before {
			calibrateNestedEstUnit(after - before)
		}
		b.inFlightWeight -= est
		if b.inFlightWeight < 0 {
			b.inFlightWeight = 0
		}
		b.inFlightCount--
		if b.inFlightCount < 0 {
			b.inFlightCount = 0
		}
		if !background {
			b.customerWeight -= est
			if b.customerWeight < 0 {
				b.customerWeight = 0
			}
		}
		b.observeOverCeilingLocked()
		b.cond.Broadcast()
		b.mu.Unlock()
	}, nil
}

// observeOverCeilingLocked refreshes the #720 over-ceiling gauge from a fresh
// ceiling sample: the in-flight weight past the ceiling the gate admits by,
// capped at the weight customer trees hold — background trees never admit past
// the ceiling except alone (the empty-gate escape), so the excess that customer
// bypass causes is min(customerWeight, inFlightWeight − ceiling). 0 when
// GOMEMLIMIT is unlimited, when no customer tree is in flight, or when the
// customer trees fit. b.mu is held by the caller.
func (b *aggregateBound) observeOverCeilingLocked() {
	var over int64
	if b.customerWeight > 0 {
		if ceiling, unlimited := admissionCeiling(); !unlimited {
			over = b.inFlightWeight - ceiling
			if over > b.customerWeight {
				over = b.customerWeight
			}
			if over < 0 {
				over = 0
			}
		}
	}
	recordNestedOverCeiling(over)
}

// waitForReleaseOrCtx blocks on b.cond until a release Broadcasts OR ctx is
// done. b.mu is held on entry and on return. Because sync.Cond has no ctx
// integration, a watcher goroutine Broadcasts on ctx.Done() to wake the Wait;
// the caller's loop then re-checks ctx.Err(). Returns ctx.Err() only if ctx is
// already done on entry (fast path); otherwise returns nil (woken → re-check).
func (b *aggregateBound) waitForReleaseOrCtx(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Arm a one-shot ctx watcher that Broadcasts on cancellation so a blocked
	// Wait cannot miss the ctx deadline. stop closes when this wait resolves so
	// the watcher goroutine always exits (no leak).
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			b.mu.Lock()
			b.cond.Broadcast()
			b.mu.Unlock()
		case <-stop:
		}
	}()
	b.cond.Wait() // releases b.mu while blocked; re-acquires on wake
	close(stop)
	return nil
}

// isOutermostNestedResolve reports whether ctx is at the TOP of a nested
// subtree (depth 0) — the single point where enterNestedResolveUnit admits.
func isOutermostNestedResolve(ctx context.Context) bool {
	return cache.NestedCallDepthFromContext(ctx) == 0
}

// --- test seams (C4: drive the gate via injected headroom, NOT an env) -------

// resetNestedResolveBoundForTest clears the process-wide gate + calibration and
// restores the production runtime seams. Test-only.
func resetNestedResolveBoundForTest() {
	boundMu.Lock()
	boundOnce = nil
	boundMu.Unlock()
	estUnitMu.Lock()
	estUnitBytes = 0
	estUnitCalibrated = false
	estUnitMu.Unlock()
	runtimeLimitFn = prodRuntimeMemoryLimit
	liveHeapFn = prodLiveHeapBytes
	resetNestedOverCeilingForTest()
}

// setRuntimeSeamsForTest injects deterministic GOMEMLIMIT + live-heap samplers
// so a falsifier drives admission by BYTE headroom without a resurrected env
// (C4). limitFn/liveFn may close over test state (e.g. a live-heap counter that
// tracks admitted weight). Test-only.
func setRuntimeSeamsForTest(limitFn, liveFn func() int64) {
	runtimeLimitFn = limitFn
	liveHeapFn = liveFn
}

// calibratedNestedEstUnitForTest returns (estUnitBytes, calibrated). Test-only.
func calibratedNestedEstUnitForTest() (int64, bool) {
	estUnitMu.Lock()
	defer estUnitMu.Unlock()
	return estUnitBytes, estUnitCalibrated
}

// inFlightWeightForTest returns the current aggregate in-flight weight + count.
// Test-only.
func inFlightWeightForTest() (int64, int) {
	b := theBound()
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inFlightWeight, b.inFlightCount
}
