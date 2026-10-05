// prewarm_first_nav_latch_test.go — #130 F3b-r2: the ALL-NAV-WIDGET readyz
// latch falsifier set. SUPERSEDES the #99 FIX-F RootIndex==0 segment latch.
//
// F3b-r2: the latch keys on the widget-vs-RA CLASS boundary, NOT RootIndex. It
// fires when EVERY cohort's NAV WIDGETS have seeded (navWidgetRemaining==0),
// independent of RootIndex — then before ANY RA (the RA content tail is
// excluded from readiness). The old "fires after the reachable-cohort dashboards
// (RootIndex==0)" invariant is SUPERSEDED by "fires after ALL nav widgets across
// all cohorts." These arms discriminate the fire-POINT, not merely eventual
// firing:
//
//   ARM-GREEN (F3b-r2 all-nav-widget): a low-NavOrder widget (RootIndex 0) AND a
//     high-NavOrder widget (RootIndex 1) across 2 cohorts + a fanout restaction.
//     The latch MUST fire AFTER the LAST nav widget of ANY cohort (incl. the
//     RootIndex!=0 one) and BEFORE any RA. Asserts ORDERING.
//   ARM-RED-earlyfire (C-r2-2 RED): simulate the OLD RootIndex latch — fire the
//     latch the instant the RootIndex==0 widgets are done, leaving the
//     RootIndex!=0 nav widget still cold. Under the GREEN guard (fire AFTER the
//     last nav widget) this is RED. Proves the RootIndex latch is GONE, not
//     dormant.
//   ARM-ZERO (provably-empty): an all-RA topology (no nav widget) fires
//     "zero-nav-widgets" so the latch never hangs to the backstop.
//   ARM-BACKSTOP (C2, F-C4): engineSeed's select composition — the latch never
//     firing must fall through to the pctx.Done() backstop, so readiness is
//     never withheld forever. Driven at the select level.
//
// Hermetic, -race, seams only (no cluster / apiserver / informer). Serializes
// on engineLatchTestMu (shares the process latch singleton + seed counters +
// the customerInFlight atomic with the sibling latch tests). Does NOT touch
// ./internal/rbac/... (destructive TestMain).

package dispatchers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

// ── recorder that timestamps the latch fire relative to seed events ─────────
type latchSeedEvent struct {
	class    string // "widget" | "restaction" | "LATCH"
	label    string // ns/name, or the fire reason for LATCH
	rootIdx  int    // widget RootIndex (−1 for RA / LATCH)
	identity string
}

type latchRecorder struct {
	mu     sync.Mutex
	events []latchSeedEvent
}

func (r *latchRecorder) rec(e latchSeedEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
}

func (r *latchRecorder) snapshot() []latchSeedEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]latchSeedEvent(nil), r.events...)
}

// installLatchFireObserver records the latch-fire INSTANT synchronously at the
// fire site (firstNavFireObserver hook) so the "LATCH" event lands at its true
// position in the ordered seed-event stream. An async wait() watcher races the
// loop's next seed (the fire is synchronous inside seedScopeYielding, but a
// goroutine blocked on wait() may not be scheduled until later seeds run) — so
// we observe synchronously. Cleared on cleanup. Caller holds engineLatchTestMu.
func installLatchFireObserver(t *testing.T, rec *latchRecorder) {
	t.Helper()
	firstNavFireObserver = func(reason string) {
		rec.rec(latchSeedEvent{class: "LATCH", label: reason, rootIdx: -1})
	}
	t.Cleanup(func() { firstNavFireObserver = nil })
}

// latchWidgetGVR gives each fixture widget its own GVR so the seam enumerator
// can hand it a distinct identity set.
func latchWidgetGVR(res string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: res}
}

// harvestWidgetAtRoot stamps a widget through the REAL harvester at a chosen
// config-root index (BeginRoot advances curRoot), so RootIndex comes from the
// real stamping path (condition-3 style — not hand-assigned). rootIdx==0 is
// the first-nav (dashboard) segment.
func harvestWidgetAtRoot(h *navWidgetHarvester, name string, gvr schema.GroupVersionResource, rootIdx int) {
	for h.curRoot < rootIdx {
		h.BeginRoot()
	}
	w := &unstructured.Unstructured{}
	w.SetNamespace("krateo-system")
	w.SetName(name)
	w.SetGroupVersionKind(schema.GroupVersionKind{Group: gvr.Group, Version: gvr.Version, Kind: "W"})
	h.harvestNavWidget(w, gvr, -1, 1, -1, -1)
}

func latchRA(name string) templatesv1.ObjectReference {
	return templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: name, Namespace: "krateo-system"},
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}
}

// installLatchSeams wires the enumerator/target-GVR/seed seams for a fixture
// and returns the recorder. All seams record into rec; the widget seam records
// the entry's RootIndex so the assert can see segment vs tail.
func installLatchSeams(t *testing.T, rec *latchRecorder,
	enumerate func(schema.GroupVersionResource) []cache.PrewarmTarget,
	raTargetGVR func(templatesv1.ObjectReference) (schema.GroupVersionResource, bool)) {
	t.Helper()

	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = func(gvr schema.GroupVersionResource, _ string) []cache.PrewarmTarget {
		return enumerate(gvr)
	}
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	prevTGVR := restActionTargetGVRFn
	restActionTargetGVRFn = func(_ context.Context, ref templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
		return raTargetGVR(ref)
	}
	t.Cleanup(func() { restActionTargetGVRFn = prevTGVR })

	prevW := seedOneWidgetFn
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, _ string, _ seedScopeMode) error {
		rec.rec(latchSeedEvent{class: "widget", label: e.W.GetName(), rootIdx: e.RootIndex, identity: eIdentityLabel(ctx)})
		return nil
	}
	t.Cleanup(func() { seedOneWidgetFn = prevW })

	prevR := seedOneRestactionFn
	seedOneRestactionFn = func(ctx context.Context, _ redact.Label, ref templatesv1.ObjectReference, _ string, _ seedScopeMode) error {
		rec.rec(latchSeedEvent{class: "restaction", label: ref.Name, rootIdx: -1, identity: eIdentityLabel(ctx)})
		return nil
	}
	t.Cleanup(func() { seedOneRestactionFn = prevR })
}

func latchTargets(gvr schema.GroupVersionResource, ids ...eID) []cache.PrewarmTarget {
	return eIdentityTargets(gvr, ids...)
}

// firstIndexOf returns the index of the first event matching pred, or len(ev).
func firstIndexOf(ev []latchSeedEvent, pred func(latchSeedEvent) bool) int {
	for i, e := range ev {
		if pred(e) {
			return i
		}
	}
	return len(ev)
}

// ── ARM-GREEN + ARM-RED-earlyfire (C-r2-2) ─────────────────────────────────
// Fixture: 2 cohorts (devs collapsed=442, ops collapsed=5), a low-NavOrder
// widget dashboard-flex (RootIndex 0) AND a high-NavOrder widget estate-graph
// (RootIndex 1), each carrying BOTH cohorts; plus a fanout restaction. The
// all-nav-widget latch MUST fire AFTER the LAST nav widget of ANY cohort (incl.
// estate-graph, which the OLD RootIndex latch would have left cold) and BEFORE
// any RA.
//
// ARM-RED-earlyfire simulates the OLD RootIndex latch: it fires the latch the
// instant the RootIndex==0 (dashboard-flex) widgets are done, leaving the
// RootIndex!=0 nav widget (estate-graph) still cold. The GREEN guard "fire AFTER
// the last nav widget" flags that as RED — proving the class-boundary latch
// waits for ALL nav widgets, not just the RootIndex==0 ones.
func TestFirstNavLatch_AllNavWidgets_GreenAndEarlyFireRed(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	// simulateOldRootIndexLatch, when true, installs a fire observer that fires
	// the latch early — at the moment the RootIndex==0 dashboard-flex widget of
	// the LAST cohort seeds — mirroring the deleted RootIndex latch. This is the
	// C-r2-2 RED mutation, expressed hermetically (no source revert needed for
	// the arm to RUN; the source-revert RED artifact is captured separately to
	// /tmp/f3b-r2-falsifiers/).
	run := func(t *testing.T, simulateOldRootIndexLatch bool) []latchSeedEvent {
		resetFirstNavLatchForTest()
		latch := ensureFirstNavLatch() // build the process latch so seedScopeYielding can fire it
		t.Cleanup(func() {
			resetFirstNavLatchForTest()
			firstNavFireObserver = nil
			zeroCustomerInFlight()
		})

		devs := eID{name: "devs", group: true, collapsed: 442}
		ops := eID{name: "ops", group: true, collapsed: 5}

		h := newNavWidgetHarvester()
		dashGVR := latchWidgetGVR("flexes")
		tailGVR := latchWidgetGVR("estategraphs")
		harvestWidgetAtRoot(h, "dashboard-flex", dashGVR, 0) // NavOrder 0, RootIndex 0
		harvestWidgetAtRoot(h, "estate-graph", tailGVR, 1)   // NavOrder 1, RootIndex 1
		widgets := h.snapshot()

		fanoutRA := latchRA("fanout-ra")
		ras := []templatesv1.ObjectReference{fanoutRA}

		rec := &latchRecorder{}

		// Wire the seams. For the RED simulation we override seedOneWidgetFn to
		// fire the latch early (after the RootIndex==0 dashboard widgets, before
		// the RootIndex!=0 nav widget) — the old-RootIndex-latch behaviour.
		installLatchSeams(t, rec,
			func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
				switch gvr {
				case dashGVR, tailGVR:
					return latchTargets(gvr, devs, ops)
				case eFanoutRAGVR:
					return latchTargets(eFanoutRAGVR, devs, ops, eID{name: "filler", collapsed: 1})
				}
				return nil
			},
			func(_ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
				return eFanoutRAGVR, true
			})

		installLatchFireObserver(t, rec)

		if simulateOldRootIndexLatch {
			// Override the widget seam to fire the latch the instant the LAST
			// RootIndex==0 (dashboard-flex) widget seeds — the deleted behaviour.
			prevW := seedOneWidgetFn
			rootZeroSeeded := 0
			seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, ns string, m seedScopeMode) error {
				rec.rec(latchSeedEvent{class: "widget", label: e.W.GetName(), rootIdx: e.RootIndex, identity: eIdentityLabel(ctx)})
				if e.RootIndex == 0 {
					rootZeroSeeded++
					if rootZeroSeeded == 2 { // both cohorts' dashboard-flex done
						latch.fire("old-rootindex-latch", 1, 2, "", -1, 0)
					}
				}
				return nil
			}
			t.Cleanup(func() { seedOneWidgetFn = prevW })
		}

		if err := seedScopeYielding(context.Background(), ras, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
			t.Fatalf("seedScopeYielding returned %v; want nil", err)
		}
		return rec.snapshot()
	}

	t.Run("green_fires_after_all_nav_widgets_before_any_ra", func(t *testing.T) {
		ev := run(t, false)
		latchIdx := firstIndexOf(ev, func(e latchSeedEvent) bool { return e.class == "LATCH" })
		if latchIdx == len(ev) {
			t.Fatalf("F3b-r2: latch never fired; events=%+v", ev)
		}
		// (1) fire AFTER the LAST nav widget of ANY cohort — including the
		//     RootIndex!=0 estate-graph. Find the last widget seed index.
		lastWidgetIdx := -1
		for i, e := range ev {
			if e.class == "widget" {
				lastWidgetIdx = i
			}
		}
		if lastWidgetIdx < 0 {
			t.Fatalf("F3b-r2 setup: no widgets seeded; events=%+v", ev)
		}
		if latchIdx < lastWidgetIdx {
			t.Fatalf("F3b-r2 ARM-COLD: latch fired at idx %d BEFORE the last nav widget at idx %d — "+
				"readyz would flip with a nav widget (e.g. the RootIndex!=0 estate-graph) still cold; events=%+v",
				latchIdx, lastWidgetIdx, ev)
		}
		// (2) fire BEFORE any RA — the RA content tail is excluded from readiness.
		firstRAIdx := firstIndexOf(ev, func(e latchSeedEvent) bool { return e.class == "restaction" })
		if firstRAIdx == len(ev) {
			t.Fatalf("F3b-r2 setup: no RA seeded; events=%+v", ev)
		}
		if latchIdx > firstRAIdx {
			t.Fatalf("F3b-r2 ARM-TAIL: latch fired at idx %d AFTER the first RA at idx %d — "+
				"readyz must not wait on the RA content tail; events=%+v", latchIdx, firstRAIdx, ev)
		}
		// Both RootIndex==0 AND RootIndex!=0 nav widgets are covered: the latch
		// fired after a RootIndex==1 widget seeded (the discriminator vs the old latch).
		sawRootOneWidgetBeforeLatch := false
		for i, e := range ev {
			if e.class == "widget" && e.rootIdx == 1 && i < latchIdx {
				sawRootOneWidgetBeforeLatch = true
			}
		}
		if !sawRootOneWidgetBeforeLatch {
			t.Fatalf("F3b-r2: no RootIndex!=0 nav widget seeded before the latch fired — the class-boundary latch must wait for the RootIndex!=0 nav widgets too; events=%+v", ev)
		}
	})

	t.Run("red_old_rootindex_latch_fires_leaving_nonroot_nav_cold", func(t *testing.T) {
		ev := run(t, true)
		// The FIRST LATCH event is the simulated old-RootIndex early fire.
		latchIdx := firstIndexOf(ev, func(e latchSeedEvent) bool { return e.class == "LATCH" })
		if latchIdx == len(ev) {
			t.Fatalf("RED setup: latch never fired; events=%+v", ev)
		}
		lastWidgetIdx := -1
		for i, e := range ev {
			if e.class == "widget" {
				lastWidgetIdx = i
			}
		}
		// PROVE the RED against the GREEN property: the simulated old-RootIndex
		// latch fires BEFORE the last (RootIndex!=0) nav widget → the GREEN guard
		// "latch >= lastWidget" is violated.
		if !(latchIdx < lastWidgetIdx) {
			t.Fatalf("C-r2-2 NOT RED: the simulated old-RootIndex latch did NOT fire before the last nav widget (latch=%d lastWidget=%d) — the GREEN guard would not discriminate it; events=%+v",
				latchIdx, lastWidgetIdx, ev)
		}
		// And specifically it fired while a RootIndex!=0 nav widget was still cold.
		rootOneAfterLatch := false
		for i, e := range ev {
			if e.class == "widget" && e.rootIdx == 1 && i > latchIdx {
				rootOneAfterLatch = true
			}
		}
		if !rootOneAfterLatch {
			t.Fatalf("C-r2-2 RED shape degenerate: no RootIndex!=0 nav widget seeded AFTER the early fire; events=%+v", ev)
		}
	})
}

// ── ARM-NONROOT (F3b-r2): RootIndex!=0-only topology still fires on all-nav ──
// No RootIndex==0 widget exists at all (every nav widget is RootIndex!=0). Under
// F3b-r2 these are ORDINARY nav widgets (no special first-nav segment); the
// latch fires AFTER the LAST nav widget seeds — NOT via a provably-empty path.
// This is the exact inverse of the deleted F-C3 semantics: the old latch treated
// a RootIndex>0-only topology as "zero first-nav" and fired the provably-empty
// path; the class-boundary latch treats them as nav widgets and waits for them.
func TestFirstNavLatch_NonRootWidgetsOnly_FiresAfterAllNavWidgets(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	resetFirstNavLatchForTest()
	ensureFirstNavLatch()
	t.Cleanup(func() {
		resetFirstNavLatchForTest()
		firstNavFireObserver = nil
		zeroCustomerInFlight()
	})

	devs := eID{name: "devs", group: true, collapsed: 442}
	ops := eID{name: "ops", group: true, collapsed: 5}

	h := newNavWidgetHarvester()
	tailA := latchWidgetGVR("tailas")
	tailB := latchWidgetGVR("tailbs")
	harvestWidgetAtRoot(h, "tail-a", tailA, 1) // NavOrder 0, RootIndex 1
	harvestWidgetAtRoot(h, "tail-b", tailB, 2) // NavOrder 1, RootIndex 2
	widgets := h.snapshot()

	rec := &latchRecorder{}
	installLatchSeams(t, rec,
		func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
			switch gvr {
			case tailA, tailB:
				return latchTargets(gvr, devs, ops)
			}
			return nil
		},
		func(_ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
			return schema.GroupVersionResource{}, false
		})

	var fireReason string
	firstNavFireObserver = func(reason string) {
		fireReason = reason
		rec.rec(latchSeedEvent{class: "LATCH", label: reason, rootIdx: -1})
	}
	t.Cleanup(func() { firstNavFireObserver = nil })

	if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}
	ev := rec.snapshot()

	latchIdx := firstIndexOf(ev, func(e latchSeedEvent) bool { return e.class == "LATCH" })
	if latchIdx == len(ev) {
		t.Fatalf("F3b-r2: latch never fired for a RootIndex!=0-only topology — readyz would hang to the backstop; events=%+v", ev)
	}
	// F3b-r2: these are ORDINARY nav widgets → the latch waits for them all and
	// fires "segment-complete", NOT "zero-nav-widgets".
	if fireReason != "segment-complete" {
		t.Fatalf("F3b-r2: latch fired reason=%q; want \"segment-complete\" — RootIndex!=0 widgets are ordinary nav widgets, not a provably-empty first-nav set; events=%+v", fireReason, ev)
	}
	lastWidgetIdx := -1
	for i, e := range ev {
		if e.class == "widget" {
			lastWidgetIdx = i
		}
	}
	if lastWidgetIdx < 0 {
		t.Fatalf("F3b-r2 setup: no widgets seeded; events=%+v", ev)
	}
	if latchIdx < lastWidgetIdx {
		t.Fatalf("F3b-r2: latch fired at idx %d BEFORE the last nav widget at idx %d — the class-boundary latch must wait for ALL nav widgets (RootIndex-independent); events=%+v", latchIdx, lastWidgetIdx, ev)
	}
}

// ── ARM-ZERO (F3b-r2 provably-empty): no nav widget at all → zero-nav-widgets ─
// An all-RA topology (or a walk that reached no widget) has navWidgetRemaining==0
// at arm time → the latch fires "zero-nav-widgets" immediately so it never hangs
// to the PHASE1_TIMEOUT backstop, and fires BEFORE any RA seeds.
func TestFirstNavLatch_ZeroNavWidgets_FiresImmediatelyBeforeRA(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	resetFirstNavLatchForTest()
	ensureFirstNavLatch()
	t.Cleanup(func() {
		resetFirstNavLatchForTest()
		firstNavFireObserver = nil
		zeroCustomerInFlight()
	})

	devs := eID{name: "devs", group: true, collapsed: 442}

	fanoutRA := latchRA("fanout-ra")
	ras := []templatesv1.ObjectReference{fanoutRA}

	rec := &latchRecorder{}
	installLatchSeams(t, rec,
		func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
			if gvr == eFanoutRAGVR {
				return latchTargets(eFanoutRAGVR, devs)
			}
			return nil
		},
		func(_ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
			return eFanoutRAGVR, true
		})

	var fireReason string
	firstNavFireObserver = func(reason string) {
		fireReason = reason
		rec.rec(latchSeedEvent{class: "LATCH", label: reason, rootIdx: -1})
	}
	t.Cleanup(func() { firstNavFireObserver = nil })

	// NO widgets — only an RA.
	if err := seedScopeYielding(context.Background(), ras, nil, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}
	ev := rec.snapshot()

	latchIdx := firstIndexOf(ev, func(e latchSeedEvent) bool { return e.class == "LATCH" })
	if latchIdx == len(ev) {
		t.Fatalf("F3b-r2: latch never fired for a zero-nav-widget topology — readyz would hang to the backstop; events=%+v", ev)
	}
	if fireReason != "zero-nav-widgets" {
		t.Fatalf("F3b-r2: latch fired reason=%q; want \"zero-nav-widgets\" (no nav widget to warm); events=%+v", fireReason, ev)
	}
	// Fired BEFORE any RA (the RA tail is excluded from readiness).
	firstRAIdx := firstIndexOf(ev, func(e latchSeedEvent) bool { return e.class == "restaction" })
	if firstRAIdx < len(ev) && latchIdx > firstRAIdx {
		t.Fatalf("F3b-r2: zero-nav-widgets latch fired at idx %d AFTER an RA at idx %d — it must fire immediately at arm time; events=%+v", latchIdx, firstRAIdx, ev)
	}
}

// ── ARM-BACKSTOP (§F.2 / C2, F-C4): select composition ─────────────────────
// engineSeed's select waits on the FIRST of {firstNav, bootDone, pctx.Done()}.
// When the latch never fires (seed aborted before the segment) AND bootDone
// stays open, the pctx.Done() backstop MUST unblock readiness — never hang
// forever. This reproduces engineSeed's select shape in isolation (the real
// closure is not directly callable without the full Phase1Warmup wiring; the
// ── CONDITION 1 (arch pre-commit): cross-goroutine fire/wait -race arm ──────
// All the arms above fire AND observe the latch on ONE goroutine (the fire
// site records synchronously into the recorder), so -race never exercises the
// ACTUAL production handoff: the ENGINE BOOT goroutine closes the latch (deep
// inside seedScopeYielding → fireFirstNav("segment-complete")) while a SEPARATE
// goroutine (engineSeed's select) is parked on firstNav.wait(). This arm
// reproduces that exact concurrency: a waiter goroutine blocks on wait() BEFORE
// the seed starts, and the REAL production fire site (seedScopeYielding, not a
// direct latch.fire poke) unblocks it from the seed goroutine. Run under -race,
// this is the only arm that flags a data race on the done-channel close/read or
// on any state the fire path touches concurrently with the waiter.
func TestFirstNavLatch_ConcurrentFireWait_RealSegmentPath_Race(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	resetFirstNavLatchForTest()
	latch := ensureFirstNavLatch()

	devs := eID{name: "devs", group: true, collapsed: 442}
	h := newNavWidgetHarvester()
	dashGVR := latchWidgetGVR("flexes")
	harvestWidgetAtRoot(h, "dashboard-flex", dashGVR, 0) // RootIndex 0 — first-nav
	widgets := h.snapshot()

	rec := &latchRecorder{}
	installLatchSeams(t, rec,
		func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
			if gvr == dashGVR {
				return latchTargets(gvr, devs)
			}
			return nil
		},
		func(_ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
			return schema.GroupVersionResource{}, false
		})

	// WAITER goroutine — parked on wait() before the seed runs, mirroring
	// engineSeed's select arm. Records the observed transition on a channel so
	// the main goroutine can assert the handoff completed (and -race can see
	// the cross-goroutine close→read).
	waiterSaw := make(chan struct{})
	go func() {
		<-latch.wait()
		close(waiterSaw)
	}()

	// SEED goroutine — the REAL production fire site closes the latch.
	seedErr := make(chan error, 1)
	go func() {
		seedErr <- seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot)
	}()

	if err := <-seedErr; err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}
	select {
	case <-waiterSaw:
		// Cross-goroutine handoff completed: the seed goroutine's
		// segment-complete fire unblocked the parked waiter.
	case <-time.After(2 * time.Second):
		t.Fatal("CONDITION 1: the waiter parked on firstNav.wait() was NOT unblocked by the real segment-path fire — cross-goroutine handoff broken")
	}
}

// select is the load-bearing composition and is asserted verbatim here).
func TestFirstNavLatch_BackstopUnblocksWhenLatchNeverFires(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()

	resetFirstNavLatchForTest()
	firstNav := ensureFirstNavLatch()
	bootDone := make(chan struct{}) // stays open — boot scope still running

	// PHASE1_TIMEOUT / pipGlobalTimeout backstop, compressed for the test.
	pctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	got := make(chan string, 1)
	go func() {
		// VERBATIM the engineSeed select (phase1_walk.go). firstNav never
		// fires; bootDone never closes; pctx must win.
		select {
		case <-firstNav.wait():
			got <- "firstNav"
		case <-bootDone:
			got <- "bootDone"
		case <-pctx.Done():
			got <- "backstop"
		}
	}()

	select {
	case res := <-got:
		if res != "backstop" {
			t.Fatalf("F-C4/C2: select returned %q; want the pctx.Done() backstop when the latch never fires and boot is still running", res)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("F-C4/C2: readiness HUNG — the backstop did not unblock the select; /readyz would never flip (the not-Ready-forever landmine)")
	}
}

// ── mutation (ii): fire-before-any-nav-widget → ARM-COLD RED ────────────────
// Directly exercises the GREEN arm's ARM-COLD assert by FORCING an early fire
// (before any seed) and showing the GREEN guard `latchIdx < lastWidget` flags
// it. We fire the latch before seeding anything; the recorded LATCH lands before
// every widget → RED under the all-nav-widget guard.
func TestFirstNavLatch_MutationII_FireBeforeAnyNavWidget_IsRedUnderGreenGuard(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	resetFirstNavLatchForTest()
	latch := ensureFirstNavLatch()
	t.Cleanup(func() {
		resetFirstNavLatchForTest()
		firstNavFireObserver = nil
		zeroCustomerInFlight()
	})

	devs := eID{name: "devs", group: true, collapsed: 442}
	h := newNavWidgetHarvester()
	dashGVR := latchWidgetGVR("flexes")
	tailGVR := latchWidgetGVR("estategraphs")
	harvestWidgetAtRoot(h, "dashboard-flex", dashGVR, 0)
	harvestWidgetAtRoot(h, "estate-graph", tailGVR, 1)
	widgets := h.snapshot()

	rec := &latchRecorder{}
	installLatchSeams(t, rec,
		func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
			switch gvr {
			case dashGVR, tailGVR:
				return latchTargets(gvr, devs)
			}
			return nil
		},
		func(_ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
			return schema.GroupVersionResource{}, false
		})

	installLatchFireObserver(t, rec)
	// MUTATION (ii): fire BEFORE any nav widget seeds.
	latch.fire("mutation-ii-premature", 0, 0, "", -1, 0)
	if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}
	ev := rec.snapshot()

	latchIdx := firstIndexOf(ev, func(e latchSeedEvent) bool { return e.class == "LATCH" })
	lastWidgetIdx := -1
	for i, e := range ev {
		if e.class == "widget" {
			lastWidgetIdx = i
		}
	}
	// The GREEN arm's ARM-COLD guard is `latchIdx < lastWidget → FAIL`. Prove a
	// premature fire triggers exactly that condition.
	if !(latchIdx < lastWidgetIdx) {
		t.Fatalf("mutation (ii) NOT caught: premature fire did not land before the last nav-widget seed (latch=%d lastWidget=%d) — the GREEN ARM-COLD guard would not flag it; events=%+v", latchIdx, lastWidgetIdx, ev)
	}
}


