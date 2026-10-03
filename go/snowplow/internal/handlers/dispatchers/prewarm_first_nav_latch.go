// prewarm_first_nav_latch.go — #99 FIX-F: the first-nav readyz latch.
//
// THE DEFECT THIS SHIPS AGAINST (docs/seed-tail-restactions-budget-and-16mb-
// serve-trace-2026-07-04.md §F.0). Under shape A (#87) /readyz is seed-gated
// BY WIRING: engineSeed's select (phase1_walk.go) waits on <-bootDone (the
// WHOLE boot scope completing) OR <-pctx.Done() (the PHASE1_TIMEOUT/
// pipGlobalTimeout backstop). On any topology where the full seed exceeds
// PHASE1_TIMEOUT the gate degenerates to a TIME gate and the pod goes Ready
// with COLD cells (the fix3r reality — seed 388s+ >> the 180s chart budget).
//
// FIX-F re-spec §F.1 (as amended by #130 F3b-r2): /readyz should flip when
// {WaitAllInformersSynced good-enough barrier (unchanged) ∧ EVERY cohort's NAV
// WIDGETS are seeded} — NOT the full seed (the RA content tail is excluded by
// construction), NOT nothing (cells, not just substrate).
//
// MECHANISM (§F.1): seedScopeYielding closes firstNavDone the moment the LAST
// nav-widget unit (widget × cohort) completes (or provably has zero nav
// widgets). engineSeed's select waits on <-firstNavDone instead of
// <-bootDone; bootDone / scopeDone stay untouched (the boot scope keeps
// running to completion in background — S2 worker-release + engine semantics
// unchanged). The deferred MarkPhase1Done (phase1_walk.go) then fires at
// min(nav-widgets-complete, PHASE1_TIMEOUT backstop, pipGlobalTimeout child) —
// the C2 "Ready-degraded, never not-Ready-forever" backstop is preserved
// verbatim (the pctx.Done() arm of engineSeed's select is untouched).
//
// CLASS-SCOPED, NOT ROOTINDEX-SCOPED (#130 F3b-r2). The old FIX-F latch keyed
// on the RootIndex==0 (config-root/dashboard) widget SEGMENT — a frontend-page
// concept Diego rejected. F3b-r2 replaced that partition with the widget-vs-RA
// CLASS boundary: the latch waits for ALL nav widgets across ALL cohorts
// (navWidgetRemaining == len(flat) → 0), then fires BEFORE the RA content tail.
// A target either IS or ISN'T a nav widget (a structural fact), never a
// page/route/dashboard concept. Post-Fix-2 every nav widget is cheap (whale
// LISTs serve from the synced informer), so waiting for all of them costs little
// — a STRICT GENERALIZATION of the old latch. A topology with no nav widget
// (all-RA) fires ONLY through the zero-nav-widgets guard (provably-nothing-to-
// warm), never spuriously.
//
// NO STATIC LIST / NO MAGIC NUMBER: the class boundary is 100% structural. The
// latch carries no cap and no env knob — the PHASE1_TIMEOUT / pipGlobalTimeout
// backstops it composes with are the EXISTING budget config (§F.4:
// PREWARM_BOOT_BUDGET_SECONDS bounds the ENGINE boot scope only, never the
// readyz gate).

package dispatchers

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// firstNavLatch is the fire-once readiness signal for the all-nav-widget class
// (#130 F3b-r2; formerly the rank-1 RootIndex==0 first-nav segment). Closed at
// most once by seedScopeYielding on the
// boot pass; awaited by engineSeed's select. A process has exactly one
// (firstNavLatchSingleton); post-boot GVR-discovered re-walks reuse the same
// already-fired latch (close is idempotent via sync.Once), so a re-walk never
// re-arms or re-blocks readiness.
type firstNavLatch struct {
	done chan struct{}
	once sync.Once
	// backstop — #262: the readiness BACKSTOP deadline (unix nanos; 0 = none):
	// the engineSeed ctx deadline (min of PHASE1_TIMEOUT and pipGlobalTimeout).
	// A latch released by it is a FAILED boot, so pre-latch work that is not
	// required (learned classes) is admitted only within the time left.
	backstop atomic.Int64
}

// armFirstNavLatchForSeed is engineSeed's latch arming (phase1_walk.go): it
// builds the process latch and records the readiness BACKSTOP — the seed ctx's
// deadline, which inherits the PHASE1_TIMEOUT parent and pipGlobalTimeout, so it
// is their minimum. Extracted so an arm drives the production wiring.
func armFirstNavLatchForSeed(pctx context.Context) *firstNavLatch {
	l := ensureFirstNavLatch()
	if dl, ok := pctx.Deadline(); ok {
		l.setBackstopDeadline(dl)
	}
	return l
}

// setBackstopDeadline records the readiness backstop deadline.
func (l *firstNavLatch) setBackstopDeadline(t time.Time) {
	l.backstop.Store(t.UnixNano())
}

// backstopRemaining is the time left before the readiness backstop; ok=false
// when no backstop was recorded or the latch has already fired.
func (l *firstNavLatch) backstopRemaining() (time.Duration, bool) {
	if l == nil || l.fired() {
		return 0, false
	}
	n := l.backstop.Load()
	if n == 0 {
		return 0, false
	}
	return time.Until(time.Unix(0, n)), true
}

func newFirstNavLatch() *firstNavLatch {
	return &firstNavLatch{done: make(chan struct{})}
}

// fire closes the latch exactly once and logs the transition with the nav-widget
// counts + elapsed the caller measured. reason distinguishes the
// segment-complete path (all nav widgets seeded) from the zero-nav-widgets path
// (there is no nav widget to warm) — both legitimate "nav widgets warm / none to
// warm" states. #130 F3b-r2: segWidgets/segTargets are now the ALL-NAV-WIDGET
// counts — the distinct nav widgets across all cohorts + the total
// (widget × cohort) units the latch waited on (0/0 on the zero path), NOT the
// old RootIndex==0 subset. segIdentity/segRank are RETAINED in the signature for
// log-field back-compat but are no longer segment-scoped: the caller passes
// ""/-1 (the latch keys on the widget-vs-RA class boundary, not one cohort's
// config-root subtree).
func (l *firstNavLatch) fire(reason string, segWidgets, segTargets int, segIdentity string, segRank int, elapsed time.Duration) {
	l.once.Do(func() {
		if firstNavFireObserver != nil {
			// TEST-ONLY synchronous observation of the fire INSTANT (nil in
			// production). The falsifier needs the fire's position in the
			// ordered seed-event stream deterministically — an async wait()
			// watcher races the loop's next seed, so the observer records
			// synchronously at the true fire site.
			firstNavFireObserver(reason)
		}
		close(l.done)
		slog.Info("prewarm.first_nav.latch",
			slog.String("subsystem", "cache"),
			slog.String("reason", reason),
			slog.Int("first_nav_widgets", segWidgets),
			slog.Int("first_nav_targets", segTargets),
			slog.String("segment_identity", segIdentity),
			slog.Int("segment_rank", segRank),
			slog.Int64("elapsed_ms", elapsed.Milliseconds()),
			slog.String("effect", "all cohorts' nav widgets seeded (or provably no nav widget); "+
				"engineSeed unblocks → /readyz flips Ready with every cohort's nav widgets warm. The "+
				"boot scope keeps seeding the RA content tail in background (bootDone unaffected)."),
		)
	})
}

// wait returns the channel engineSeed's select reads. Closed when the latch
// has fired.
func (l *firstNavLatch) wait() <-chan struct{} {
	return l.done
}

// fired reports whether the latch has already fired (its done channel is
// closed). F5 (#131) reads this on engineSeed's backstop arms to distinguish a
// backstop-Ready flip (latch NOT fired → nav widgets unseeded → alert) from a
// benign tie where the latch fired just as bootDone closed. Race-safe: a
// non-blocking receive on a closed channel returns immediately; close happens-
// before every fired() call that observes it (the sync.Once in fire()).
func (l *firstNavLatch) fired() bool {
	select {
	case <-l.done:
		return true
	default:
		return false
	}
}

// firstNavLatchSingleton is the process latch. Built once (set-once at the
// first engineSeed invocation via ensureFirstNavLatch); the boot pass fires
// it, and engineSeed waits on it. Package-level so seedScopeYielding (invoked
// deep inside the engine worker via rePrewarmBoot) can reach the SAME latch
// engineSeed awaits without threading it through rePrewarmDeps (which is
// shared with the post-boot GVR-discovered scope, where there is no readyz
// wait).
var (
	firstNavLatchSingleton *firstNavLatch
	firstNavLatchOnce      sync.Once
)

// ensureFirstNavLatch returns the process first-nav latch, building it once.
// engineSeed calls this before enqueuing the boot scope so the latch exists
// before seedScopeYielding can reach it.
func ensureFirstNavLatch() *firstNavLatch {
	firstNavLatchOnce.Do(func() {
		firstNavLatchSingleton = newFirstNavLatch()
	})
	return firstNavLatchSingleton
}

// currentFirstNavLatch returns the process latch if it has been built, else
// nil. seedScopeYielding uses this: the latch exists on the boot path (built
// by engineSeed before the boot enqueue), and is nil under the pure-unit seed
// tests that call seedScopeYielding directly without an engineSeed wrapper —
// in which case the fire is a no-op (nil-safe), so those tests are unchanged.
func currentFirstNavLatch() *firstNavLatch {
	return firstNavLatchSingleton
}

// firstNavFireObserver is a TEST-ONLY synchronous hook invoked at the fire
// site (inside the once.Do, before close(done)) so a falsifier can record the
// latch's exact position in the ordered seed-event stream without racing an
// async wait() watcher. nil in production. Set/cleared under engineLatchTestMu
// by the falsifier.
var firstNavFireObserver func(reason string)

// resetFirstNavLatchForTest rebuilds the singleton so a test can observe a
// fresh arm→fire transition. TEST-ONLY — production's lifecycle is set-once
// at boot. Pairs with cache.ResetPhase1DoneForTest.
func resetFirstNavLatchForTest() {
	firstNavLatchOnce = sync.Once{}
	firstNavLatchSingleton = nil
}
