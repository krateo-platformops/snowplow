// seed_bound.go — the prewarm seed-unit ADAPTIVE memory-admission bound,
// applied at the shared seed-unit choke points (seedOneWidget / seedOneRestaction).
//
// THE #23 OOM CLASS: a prewarm seed unit (one (target, layer) resolve) can
// materialize a multi-hundred-MB UNPAGINATED full-list envelope
// (seedRAFullListForWidget). The adaptive nested-resolve bound does NOT cover
// this allocation — a RA's own data-stage LISTs are depth-0 non-CR paths that
// Gate 4 of the nested-resolve seam rejects (docs/prewarm-engine-implicit-on-cache-2026-07-03.md §3.1).
// So this seed-unit bound is the ONLY thing that bounds seedRAFullListForWidget.
//
// ADAPTIVE, ZERO-KNOB (fold 2026-07-03, §3.2; SUPERSEDES the fixed
// SEED_FOOTPRINT_BUDGET_BYTES semaphore, which defaulted 0/DISABLED — leaving
// the seed's dominant allocation UNBOUNDED once the seed flags fold on).
// Replaces the fixed byte budget with the SAME recompute-per-admission headroom
// gate as the (c) nested-resolve bound, via the SHARED cache.AdmissionCeiling()
// primitive: ceiling = (GOMEMLIMIT − liveHeap) − GOMEMLIMIT/8.
//
// C1 (deadlock-safety, NON-NEGOTIABLE): this is a SEPARATE bound instance
// (seedBound, its OWN mutex/cond/inFlightWeight/inFlightCount) from the nested
// bound's theBound(). It SHARES ONLY the pure calc (cache.AdmissionCeiling) +
// the live-heap sampler (cache.AdmissionLiveHeapSample) — NEVER the counter/cond.
// The stacked seed→nested case (a seed unit whose resolve triggers a depth-0
// nested-CR resolve) holds TWO independent admissions on the SAME goroutine in
// strict nest order (seed outer, nested inner); two independent semaphores each
// with an inFlightCount==0 unconditional escape cannot self-deadlock. The two
// weights are over-reservation against the same headroom — conservative-safe
// (admits LESS aggregate, never more; the 1.5.1 over-reserve-is-safe lesson).
//
// SERIALIZE-not-503 (the seed is background, no browser deadline): admit the
// (N+1)th seed unit iff inFlightWeight + estUnit <= ceiling; else PARK (block,
// ctx-bounded by the seed's own ctx — the boot budget / pipCohortTimeout),
// re-check on release. inFlightCount==0 ⇒ admit unconditionally (anti-deadlock:
// a lone oversized unit runs ALONE rather than parking forever). On ctx
// cancel/deadline return the ctx error (the unit is abandoned best-effort,
// log-only). NEVER a hard failure, NEVER a customer-visible error path.
//
// SEED-SCOPED (customer /call UNTOUCHED): enterSeedUnit is called ONLY from the
// shared seed primitives, AFTER the per-binding identity short-circuit. The
// customer dispatcher never routes here (feedback_bounding_mechanism_discipline:
// after the cache-hit, seed-scoped, cost-proportional).
//
// TRANSPARENT when GOMEMLIMIT is unset (math.MaxInt64 → cache.AdmissionCeiling
// returns unlimited): admit immediately, release is a no-op — byte-identical to
// no bound (project_caching_is_provisional: cleanly removable). The chart sets
// GOMEMLIMIT (7GiB); with it unset there is no soft ceiling to protect.
package dispatchers

import (
	"context"
	"sync"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

const (
	// defaultSeedEstUnitBytes is the CONSERVATIVE-HIGH per-seed-unit admission
	// weight used before the one-shot empirical calibration lands. Over-reserve
	// by design: a too-high fallback serializes a touch more (safe); a too-low
	// one admits too much aggregate (the 1.5.1 180× under-estimate failure mode).
	// 256 MiB ≈ a large unpaginated full-list unit. FOLDED 2026-07-03: was the
	// SEED_EST_UNIT_BYTES_FALLBACK env (deleted, zero-knob); now a CODE CONSTANT,
	// mirroring nested_resolve_bound.go's defaultNestedEstUnitBytes.
	defaultSeedEstUnitBytes int64 = 256 * 1024 * 1024
)

// seedAdmissionBound is the seed-unit process-wide admission gate — a SEPARATE
// instance from the nested bound's aggregateBound (C1). inFlightWeight is the
// sum of the estUnit weights of currently-admitted seed units; cond wakes
// parked units on each release so they re-check headroom.
type seedAdmissionBound struct {
	mu             sync.Mutex
	cond           *sync.Cond
	inFlightWeight int64
	inFlightCount  int
}

var (
	seedBoundMu   sync.Mutex
	seedBoundOnce *seedAdmissionBound

	// estUnit calibration (self-adapting, mirror nested_resolve_bound.go).
	// Starts at the conservative-high fallback; replaced ONCE by the first
	// unit's measured live-heap delta.
	seedEstUnitMu         sync.Mutex
	seedEstUnitBytes      int64
	seedEstUnitCalibrated bool
)

func seedBound() *seedAdmissionBound {
	seedBoundMu.Lock()
	defer seedBoundMu.Unlock()
	if seedBoundOnce == nil {
		seedBoundOnce = &seedAdmissionBound{}
		seedBoundOnce.cond = sync.NewCond(&seedBoundOnce.mu)
	}
	return seedBoundOnce
}

// currentEstUnit returns the per-seed-unit admission weight: the calibrated
// value once available, else the conservative-high code-constant fallback,
// clamped to the current ceiling so a single unit larger than the whole
// headroom runs ALONE rather than parking forever (guaranteed progress).
func currentEstUnit(ceiling int64, unlimited bool) int64 {
	seedEstUnitMu.Lock()
	est := seedEstUnitBytes
	calibrated := seedEstUnitCalibrated
	seedEstUnitMu.Unlock()
	if !calibrated || est <= 0 {
		est = defaultSeedEstUnitBytes
	}
	if !unlimited && ceiling > 0 && est > ceiling {
		est = ceiling // clamp: one unit may use up to the whole ceiling, never more
	}
	if est < 1 {
		est = 1
	}
	return est
}

// calibrateEstUnit records the first unit's MEASURED live-heap delta as the
// empirical per-unit weight (one-shot). Subsequent units reuse it. A delta <= 0
// (GC ran mid-unit, or a static no-op unit) is ignored so we don't calibrate to
// an unrealistically small weight. Mirrors calibrateNestedEstUnit.
func calibrateEstUnit(measuredDelta int64) {
	if measuredDelta <= 0 {
		return
	}
	seedEstUnitMu.Lock()
	if !seedEstUnitCalibrated {
		seedEstUnitBytes = measuredDelta
		seedEstUnitCalibrated = true
	}
	seedEstUnitMu.Unlock()
}

// calibratedEstUnitForTest returns (estUnitBytes, calibrated) so the
// granularity-discriminating falsifier can assert calibration landed in a
// per-UNIT band (not N× a whole cohort). Test-only.
func calibratedEstUnitForTest() (int64, bool) {
	seedEstUnitMu.Lock()
	defer seedEstUnitMu.Unlock()
	return seedEstUnitBytes, seedEstUnitCalibrated
}

// waitForReleaseOrCtx blocks on b.cond until a release Broadcasts OR ctx is
// done. b.mu is held on entry and on return. sync.Cond has no ctx integration,
// so a one-shot watcher Broadcasts on ctx.Done() to wake the Wait; the caller's
// loop then re-checks ctx.Err(). Mirrors nested_resolve_bound.go's helper.
func (b *seedAdmissionBound) waitForReleaseOrCtx(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
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

// enterSeedUnit is the seed-unit adaptive-admission bound, applied as a
// lifecycle bracket at the SHARED seed primitives (seedOneWidget /
// seedOneRestaction), AFTER their identity short-circuit. The engine seed path
// (seedScopeYielding) funnels through those primitives, so bracketing there
// bounds the whole seed with one insertion (feedback_no_special_cases — bound
// the shared mechanism, not each caller).
//
// Usage at each primitive, right after `if handle==nil || key=="" { return }`:
//
//	release, err := enterSeedUnit(ctx, "widget/"+ns+"/"+name)
//	if err != nil { return err }   // ctx cancelled while parked on the bound
//	defer release()
//
// label is a short seed-unit descriptor (kind + identity) for the assert log —
// never a per-name special-case (feedback_no_special_cases).
//
// SERIALIZE-not-503 + anti-deadlock (§3.3):
//   - unlimited GOMEMLIMIT ⇒ transparent: admit immediately, release is a no-op.
//   - inFlightCount == 0 ⇒ admit unconditionally (a lone oversized unit runs
//     ALONE rather than parking forever — guaranteed progress).
//   - else admit iff inFlightWeight + estUnit <= ceiling; else PARK (ctx-bounded
//     by the seed's own ctx) and re-check on release. On ctx cancel/deadline
//     return the ctx error (the unit is abandoned best-effort, log-only — the
//     seed yields; NEVER a hard failure).
//
// On admission inFlightWeight += estUnit, inFlightCount++, and a live-heap
// sample is taken; release() re-samples, calibrates the per-unit weight
// (one-shot), runs the AssertSeedUnitFootprint diagnostic, decrements, and
// broadcasts to wake parked units.
func enterSeedUnit(ctx context.Context, label string) (release func(), err error) {
	b := seedBound()
	// #262 — this unit really resolves (the skip decisions short-circuit BEFORE
	// this bracket), so its whole per-target wall cost counts toward the
	// learned-class engine bound's t_unit (measuredSeedUnit).
	markSeedUnitResolved(ctx)

	b.mu.Lock()
	var est int64
	var admitCeiling int64 // the ceiling this unit was admitted against (for the release-time assert)
	var admitUnlimited bool
	for {
		ceiling, unlimited := cache.AdmissionCeiling()
		est = currentEstUnit(ceiling, unlimited)
		fits := b.inFlightWeight+est <= ceiling
		// The seed is BACKGROUND (no browser deadline); there is no customer on
		// THIS gate (the customer /call path never routes here), so there is no
		// C5 waiting-customer priority to honour — a simpler admit rule than the
		// nested bound. Guaranteed progress via the inFlightCount==0 escape.
		if unlimited || b.inFlightCount == 0 || fits {
			admitCeiling = ceiling
			admitUnlimited = unlimited
			b.inFlightWeight += est
			b.inFlightCount++
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

	before := cache.AdmissionLiveHeapSample()
	var released bool
	return func() {
		b.mu.Lock()
		if released {
			b.mu.Unlock()
			return
		}
		released = true
		after := cache.AdmissionLiveHeapSample()
		var delta int64
		if after > before {
			delta = after - before
		}
		calibrateEstUnit(delta)
		// Diagnostic: per-unit oversize signal (re-homed from the old semaphore
		// release). An oversized unit is one whose MEASURED delta exceeded the
		// headroom it was ADMITTED against — so the budget is the ceiling
		// captured AT ADMISSION (admitCeiling), NOT a fresh sample (a fresh
		// sample would already reflect this unit's own allocation, moving the
		// goalpost). unlimited admission ⇒ budget 0 ⇒ AssertSeedUnitFootprint
		// no-ops, preserving the transparent posture.
		budget := admitCeiling
		if admitUnlimited || budget < 0 {
			budget = 0
		}
		cache.AssertSeedUnitFootprint(label, uint64(delta), uint64(budget))
		b.inFlightWeight -= est
		if b.inFlightWeight < 0 {
			b.inFlightWeight = 0
		}
		b.inFlightCount--
		if b.inFlightCount < 0 {
			b.inFlightCount = 0
		}
		b.cond.Broadcast()
		b.mu.Unlock()
	}, nil
}

// boundSeedUnit is the closure form of enterSeedUnit, kept for the falsifier
// tests (the shared-primitive call sites use enterSeedUnit's bracket form
// directly because their resolve+Put bodies are long).
func boundSeedUnit(ctx context.Context, label string, do func() error) error {
	release, err := enterSeedUnit(ctx, label)
	if err != nil {
		return err
	}
	defer release()
	return do()
}

// inFlightSeedWeightForTest returns the current aggregate in-flight seed weight
// + count. Test-only.
func inFlightSeedWeightForTest() (int64, int) {
	b := seedBound()
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.inFlightWeight, b.inFlightCount
}

// resetSeedBoundForTest clears the process-wide seed gate + calibration so a
// falsifier can drive a fresh state. It does NOT touch the shared
// cache.AdmissionCeiling seams — a test that injects headroom must do so via
// cache.SetAdmissionRuntimeSeamsForTest / ResetAdmissionRuntimeSeamsForTest.
// Test-only. Mirrors resetNestedResolveBoundForTest.
func resetSeedBoundForTest() {
	seedBoundMu.Lock()
	seedBoundOnce = nil
	seedBoundMu.Unlock()
	seedEstUnitMu.Lock()
	seedEstUnitBytes = 0
	seedEstUnitCalibrated = false
	seedEstUnitMu.Unlock()
}
