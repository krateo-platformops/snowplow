// prewarm_first_nav_latch_segment_test.go — #99b FIX-F segment-identity
// falsifier set (docs/fixf-rootindex-redrive-walk-trace-2026-07-06.md §5),
// updated 2026-07-07 for FIX-3 rank hygiene
// (docs/fix3-rank-hygiene-design-2026-07-07.md §7).
//
// Pre-FIX-3 defect: `ranked` was built over the UNION of widget- AND
// restaction-target identities by FIRST-SEEN CollapsedBindings. A machine SA
// present ONLY in a restaction target set (a bench-ns SA, all seeds
// empty_binding-skipped) out-ranked every login-cohort widget-floor identity
// and became ranked[0] — with ZERO RootIndex==0 widget targets. #99b's segKey
// scan then keyed the latch on the FIRST ranked identity with a RootIndex==0
// widget target (not ranked[0]) so /readyz gated on the real dashboard.
//
// FIX-3 changes WHO holds each rank: widgetMax DESC now places every
// widget-capable identity strictly above every widget-less one, so on a
// SINGLE-root topology the segment identity IS ranked[0] and segRank binds 0
// (the machine SA drops to the tail). The #99b scan is retained as a SAFETY NET
// for the MULTI-root case — an identity whose only widget targets are in a
// non-first root (RootIndex!=0) can still take ranked[0] yet have zero
// FIRST-NAV targets → segRank>0. TestFirstNavLatch_SegmentIdentity_MultiRoot_*
// below is the mandatory arm that keeps that scan discriminating post-FIX-3.
//
//   ARM-GREEN-BOOT600 (§5 GREEN, now FIX-3 premise): 2 RootIndex==0 widgets {U1
//     c=5, U2 c=2} + 1 RootIndex==1 widget; 1 restaction whose target set = {M
//     c=50}, M in NO widget set. Post-FIX-3 ranked = [U1(widgetMax 5),
//     U2(widgetMax 2), M(widgetMax 0)] — M's 50 no longer wins because it has
//     zero widget observations. The latch MUST fire reason=segment-complete,
//     first_nav_widgets=2, first_nav_targets=2 (U1's 2 RootIndex==0 pairs),
//     positioned AFTER the 2nd U1 RootIndex==0 widget seed and BEFORE the
//     RootIndex==1 widget (ARM-TAIL). segment_identity == U1, segment_rank == 0
//     (U1 is ranked[0] post-FIX-3, not rank 1).
//
//   ARM-FIX2-REDRIVE (§5 Fix-2 arm): 2 config roots; the boot-walk pass
//     harvests NOTHING (empty subtrees); a SECOND walk pass (redrive shape:
//     fresh walkers, shared harvester) harvests both subtrees. root-0's widgets
//     MUST stamp RootIndex==0 (RED without BeginWalk+BeginRoot in the re-walk:
//     they resume from the boot walk's curRoot and stamp 1). Exercised at the
//     harvester level (BeginWalk/BeginRoot are the unit under test).
//
//   ARM-KEEPWARM-SEGRANK (segRank/keepwarm interaction ruling, FIX-3 + c2
//     divergence fixture): the keepwarm sweep (seedModeKeepwarm) re-seeds the
//     WIDGET-CAPABLE PREFIX's CELLS; segRank is a latch-only concept and does NOT
//     retarget the keepwarm loop bound (the bound is the widgetMax tier). c2
//     WIDENS this from the c1 rank-1-only bound: the sweep now covers every
//     widget-capable identity. TestKeepwarmSweep_WidgetCapablePrefix_SeedsBothNotSegmentOnly
//     uses a MULTI-ROOT divergence fixture: W (widgetMax 9, but ALL its widgets
//     are RootIndex==1) is ranked[0]; the segment identity is V (a lower-ranked
//     but still widget-capable identity with a RootIndex==0 widget) at segRank>0.
//     The sweep MUST seed BOTH W and V (both widget-capable), NOT keyed on
//     segRank — proving the loop bound follows the widgetMax tier, not segKey.
//
// Hermetic, -race, seams only. Serializes on engineLatchTestMu.

package dispatchers

import (
	"context"
	"log/slog"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/redact"
)

// ── ARM-GREEN-BOOT600 (§5 GREEN, Fix 1) ─────────────────────────────────────
func TestFirstNavLatch_SegmentIdentity_RestactionOnlyRank0_FiresOnWidgetSegment(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	resetFirstNavLatchForTest()
	ensureFirstNavLatch()

	// The boot600 shape: M (collapsed 50) appears ONLY in the restaction target
	// set — no widget authorises it, so post-FIX-3 its widgetMax is 0. U1
	// (widgetMax 5) and U2 (widgetMax 2) are the login-cohort widget identities.
	// FIX-3 ranked = [U1(0), U2(1), M(2)] — widgetMax DESC drops the RA-only M
	// to the tail; U1 is ranked[0] and segRank binds 0.
	m := eID{name: "machine-sa", collapsed: 50}
	u1 := eID{name: "u1", group: true, collapsed: 5}
	u2 := eID{name: "u2", group: true, collapsed: 2}

	h := newNavWidgetHarvester()
	dashA := latchWidgetGVR("flexes")      // RootIndex 0 — first-nav, U1 target
	dashB := latchWidgetGVR("estategraphs") // RootIndex 0 — first-nav, U1 target
	tail := latchWidgetGVR("tailwidgets")   // RootIndex 1 — NON-first-nav, U2 target
	harvestWidgetAtRoot(h, "dash-a", dashA, 0)
	harvestWidgetAtRoot(h, "dash-b", dashB, 0)
	harvestWidgetAtRoot(h, "tail-w", tail, 1)
	widgets := h.snapshot()

	fanoutRA := latchRA("machine-fanout-ra")
	ras := []templatesv1.ObjectReference{fanoutRA}

	rec := &latchRecorder{}
	installLatchSeams(t, rec,
		func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
			switch gvr {
			case dashA, dashB:
				// Both RootIndex==0 dashboard widgets authorise U1 (2 targets each
				// would over-count; give ONE U1 target per widget so first_nav_
				// targets == 2 == first_nav_widgets).
				return latchTargets(gvr, u1)
			case tail:
				return latchTargets(gvr, u2)
			case eFanoutRAGVR:
				return latchTargets(eFanoutRAGVR, m) // M appears ONLY here.
			}
			return nil
		},
		func(_ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
			return eFanoutRAGVR, true
		})

	// Capture the fire reason + segment fields via a richer observer.
	var fireReason string
	firstNavFireObserver = func(reason string) {
		fireReason = reason
		rec.rec(latchSeedEvent{class: "LATCH", label: reason, rootIdx: -1})
	}
	t.Cleanup(func() { firstNavFireObserver = nil })

	if err := seedScopeYielding(context.Background(), ras, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}
	ev := rec.snapshot()

	// GREEN: fired segment-complete, NOT zero-first-nav-targets (the pre-#99b
	// RED reason). This is the load-bearing discriminator.
	if fireReason != "segment-complete" {
		t.Fatalf("§5 GREEN: latch fired reason=%q; want \"segment-complete\" — the pre-#99b code would fire \"zero-first-nav-targets\" (ranked[0]=M has no first-nav widget); events=%+v", fireReason, ev)
	}

	latchIdx := firstIndexOf(ev, func(e latchSeedEvent) bool { return e.class == "LATCH" })
	raIdx := firstIndexOf(ev, func(e latchSeedEvent) bool { return e.class == "restaction" })

	if latchIdx == len(ev) {
		t.Fatalf("§5 GREEN: latch never fired; events=%+v", ev)
	}
	// F3b-r2: the class-boundary latch fires AFTER ALL nav widgets (dash-a,
	// dash-b AND the RootIndex==1 tail-w — every nav widget is now part of the
	// nav-widget class, no RootIndex partition) and BEFORE the RA-only M tail.
	lastWidgetIdx := -1
	for i, e := range ev {
		if e.class == "widget" {
			lastWidgetIdx = i
		}
	}
	if lastWidgetIdx < 0 {
		t.Fatalf("§5 setup: no widgets seeded; events=%+v", ev)
	}
	if latchIdx < lastWidgetIdx {
		t.Fatalf("F3b-r2: latch fired at idx %d BEFORE the last nav widget at idx %d — the class-boundary latch must wait for ALL nav widgets (incl. the RootIndex==1 tail-w); events=%+v", latchIdx, lastWidgetIdx, ev)
	}
	// ARM-TAIL: the M restaction (widget-less, ranked last) seeds in the RA tail
	// AFTER the latch — readyz must not wait on the RA-only tail.
	if raIdx < len(ev) && latchIdx > raIdx {
		t.Fatalf("F3b-r2 ARM-TAIL: latch fired at idx %d AFTER the RA-only tail (M) at idx %d — readyz must not wait on it; events=%+v", latchIdx, raIdx, ev)
	}
}

// ── F3b-r2: MULTI-ROOT — latch is RootIndex-INDEPENDENT (supersedes the #99b
// segKey scan). ──
// The #99b segKey scan (walk `ranked` to the first identity with a RootIndex==0
// widget) is DELETED under F3b-r2: the latch no longer keys on any RootIndex
// partition, so a multi-root topology where ranked[0]=W has only a RootIndex==1
// widget and V has the RootIndex==0 widget needs NO scan — the class-boundary
// latch fires after BOTH nav widgets seed, regardless of which cohort/RootIndex
// they belong to. This arm re-expresses the multi-root property for F3b-r2:
//   (i) ranked[0] = W (widgetMax 9), widget RootIndex==1;
//   (ii) V (widgetMax 2) has a RootIndex==0 widget;
//   (iii) the latch fires reason=segment-complete AFTER BOTH widgets — never
//         early on W's widget alone, never via a RootIndex partition.
// RED (C-r2-2, captured to /tmp/f3b-r2-falsifiers/ by source-revert): the old
// RootIndex latch would fire when the RootIndex==0 widget (V's) seeds, leaving
// W's RootIndex==1 nav widget cold — the early-fire flagged by the last-widget guard.
func TestFirstNavLatch_MultiRoot_LatchIsRootIndexIndependent(t *testing.T) {
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

	// W (widgetMax 9) is ranked[0], widget lives in RootIndex==1 only. V
	// (widgetMax 2) has a RootIndex==0 widget.
	w := eID{name: "w", group: true, collapsed: 9}
	v := eID{name: "v", group: true, collapsed: 2}

	h := newNavWidgetHarvester()
	firstNavW := latchWidgetGVR("flexes")     // RootIndex 0 — V's widget
	nonFirstNavW := latchWidgetGVR("tailwid") // RootIndex 1 — W's widget
	harvestWidgetAtRoot(h, "first-nav", firstNavW, 0)
	harvestWidgetAtRoot(h, "non-first-nav", nonFirstNavW, 1)
	widgets := h.snapshot()

	rec := &latchRecorder{}
	installLatchSeams(t, rec,
		func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
			switch gvr {
			case firstNavW:
				return latchTargets(gvr, v) // V, RootIndex 0
			case nonFirstNavW:
				return latchTargets(gvr, w) // W, RootIndex 1 → ranked[0]
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

	// (iii) fired segment-complete (both nav widgets are ordinary nav widgets;
	// no provably-empty path).
	if fireReason != "segment-complete" {
		t.Fatalf("F3b-r2 multi-root: latch fired reason=%q; want \"segment-complete\"; events=%+v", fireReason, ev)
	}
	// The latch fired AFTER BOTH nav widgets seeded (V's RootIndex==0 AND W's
	// RootIndex==1) — RootIndex-independent, no scan.
	latchIdx := firstIndexOf(ev, func(e latchSeedEvent) bool { return e.class == "LATCH" })
	lastWidgetIdx := -1
	for i, e := range ev {
		if e.class == "widget" {
			lastWidgetIdx = i
		}
	}
	if latchIdx == len(ev) {
		t.Fatalf("F3b-r2 multi-root: latch never fired; events=%+v", ev)
	}
	if lastWidgetIdx < 0 {
		t.Fatalf("F3b-r2 multi-root setup: no widgets seeded; events=%+v", ev)
	}
	if latchIdx < lastWidgetIdx {
		t.Fatalf("F3b-r2 multi-root: latch fired at idx %d BEFORE the last nav widget at idx %d — it must wait for ALL nav widgets regardless of RootIndex/rank; events=%+v", latchIdx, lastWidgetIdx, ev)
	}
}

// ── ARM-FIX2-REDRIVE (§5 Fix-2 arm) ─────────────────────────────────────────
// Two config roots; the boot-walk pass harvests nothing; a SECOND walk pass
// (redrive shape) harvests both subtrees. Without BeginWalk()+per-root
// BeginRoot() on the re-walk, the second pass resumes from the boot walk's
// final curRoot and stamps root-0 widgets with the WRONG RootIndex. Exercised
// directly on the harvester (the unit BeginWalk/BeginRoot govern).
func TestNavWidgetHarvester_RedriveWalk_StampsRootIndexZeroForRootZero(t *testing.T) {
	h := newNavWidgetHarvester()
	r0w := latchWidgetGVR("root0widgets")
	r1w := latchWidgetGVR("root1widgets")

	// PASS 1 (boot walk): two roots, but both subtrees are empty (the boot
	// walk reached no widget — the 50K+ boot-race shape where the effective
	// harvest is the config-vars redrive). Advance curRoot per root anyway
	// (mirrors the boot walk's resolver closure) so PASS 1 ends at curRoot=1.
	h.BeginWalk()
	h.BeginRoot() // root 0
	h.BeginRoot() // root 1
	// (no harvestWidgetAtRoot — subtrees empty)

	// PASS 2 (engine re-walk / redrive): fresh walkers, SAME harvester. With
	// Fix 2, the re-walk calls BeginWalk() (curRoot→-1) then BeginRoot() per
	// root, so root-0's widget stamps RootIndex 0.
	h.BeginWalk() // Fix 2: reset to -1
	h.BeginRoot() // root 0 → curRoot 0
	harvestWidgetAtRootNoReset(h, "r0-widget", r0w)
	h.BeginRoot() // root 1 → curRoot 1
	harvestWidgetAtRootNoReset(h, "r1-widget", r1w)

	got := map[string]int{}
	for _, e := range h.snapshot() {
		got[e.W.GetName()] = e.RootIndex
	}
	if got["r0-widget"] != 0 {
		t.Fatalf("Fix-2: root-0 widget stamped RootIndex %d; want 0 (RED without the re-walk BeginWalk reset: it resumes at curRoot=1 and stamps 2). got=%+v", got["r0-widget"], got)
	}
	if got["r1-widget"] != 1 {
		t.Fatalf("Fix-2: root-1 widget stamped RootIndex %d; want 1; got=%+v", got["r1-widget"], got)
	}
}

// harvestWidgetAtRootNoReset stamps a widget through the REAL harvester WITHOUT
// advancing curRoot (the caller controls BeginRoot). It records at the
// harvester's CURRENT curRoot — so the assert sees the root index the
// BeginWalk/BeginRoot sequence produced, not a hand-assigned value.
func harvestWidgetAtRootNoReset(h *navWidgetHarvester, name string, gvr schema.GroupVersionResource) {
	eHarvestWidget(h, name, gvr)
}

// ── ARM-KEEPWARM-SEGRANK (segRank/keepwarm interaction ruling, FIX-3 + c2) ───
// keepwarm c2 WIDENS this arm (design §6 c1-arm table): the keepwarm sweep now
// bounds the seed to the WIDGET-CAPABLE PREFIX (widgetMax>=1), not ranked[0]
// only — so it seeds W AND V (both widget-capable). The #99b ruling this arm
// encodes SURVIVES the widening: the loop bound is the widgetMax TIER, still NOT
// segRank. segRank (the latch segment identity, #99b) is a LATCH-ONLY (boot-gate)
// concept and must NOT retarget the keepwarm loop bound — this arm pins that the
// sweep's coverage follows the widgetMax tier regardless of which identity is
// the first-nav segment.
//
// DIVERGENCE fixture (ranked[0] != segKey): W (widgetMax 9) is ranked[0] but ALL
// its widgets are RootIndex==1 → segRank>0 (the segment falls to V, a
// lower-ranked identity with a RootIndex==0 widget). Both are widget-capable, so
// c2 sweeps BOTH; the assert is that the sweep coverage is the widget-capable
// tier (W and V), NOT keyed on segRank (which would, wrongly, retarget the loop
// to only V or reorder around it). (The multi-root first-nav LATCH behaviour
// itself is pinned by TestFirstNavLatch_SegmentIdentity_MultiRoot_ScanFindsFirstNavWidget
// below.)
func TestKeepwarmSweep_WidgetCapablePrefix_SeedsBothNotSegmentOnly(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	// The latch has already fired at boot in production; a keepwarm sweep runs
	// with the singleton already closed. Reset+prefire so the sweep's fire
	// calls are the idempotent no-ops they are in prod.
	resetFirstNavLatchForTest()
	latch := ensureFirstNavLatch()
	latch.fire("boot-already-fired", 0, 0, "", -1, 0)

	// W (widgetMax 9) is ranked[0], but its widget is RootIndex==1 (non-first-
	// nav) → segRank>0. V (widgetMax 2) has a RootIndex==0 widget → V is the
	// segment identity, at a lower rank. ranked = [W(0), V(1)]; BOTH widget-capable.
	w := eID{name: "w", group: true, collapsed: 9}
	v := eID{name: "v", group: true, collapsed: 2}

	h := newNavWidgetHarvester()
	firstNavW := latchWidgetGVR("flexes")     // RootIndex 0 — V's first-nav widget
	nonFirstNavW := latchWidgetGVR("tailwid") // RootIndex 1 — W's only widget
	harvestWidgetAtRoot(h, "first-nav", firstNavW, 0)
	harvestWidgetAtRoot(h, "non-first-nav", nonFirstNavW, 1)
	widgets := h.snapshot()

	rec := &latchRecorder{}
	installLatchSeams(t, rec,
		func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
			switch gvr {
			case firstNavW:
				return latchTargets(gvr, v) // V, RootIndex 0 → the segment
			case nonFirstNavW:
				return latchTargets(gvr, w) // W, RootIndex 1 → ranked[0], not first-nav
			}
			return nil
		},
		func(_ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
			return schema.GroupVersionResource{}, false
		})

	// seedModeKeepwarm — the c2 keepwarm sweep (widget-capable prefix).
	if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeKeepwarm); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}
	ev := rec.snapshot()

	seededW := firstIndexOf(ev, func(e latchSeedEvent) bool { return e.class == "widget" && e.identity == "group:w" }) < len(ev)
	seededV := firstIndexOf(ev, func(e latchSeedEvent) bool { return e.class == "widget" && e.identity == "group:v" }) < len(ev)

	if !seededW {
		t.Fatalf("c2 keepwarm ruling: sweep did NOT seed ranked[0] (W, widgetMax 9) — the widget-capable prefix must include ranked[0]; events=%+v", ev)
	}
	if !seededV {
		t.Fatalf("c2 keepwarm ruling: sweep did NOT seed V (widgetMax 2, widget-capable) — c2 WIDENS coverage to the whole widget-capable prefix, not ranked[0] only; the #99b property (bound is the widgetMax tier, NOT segRank) must still seed V; events=%+v", ev)
	}
}

// countingHandler is a minimal slog.Handler that counts records whose msg matches
// a target, capturing the "identity" attr of each — for asserting the keepwarm
// per-cohort cohort_summary emission (#130 F3b-r2 cond 3).
type countingHandler struct {
	target     string
	identities *[]string
}

func (h countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h countingHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == h.target {
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "identity" {
				*h.identities = append(*h.identities, a.Value.String())
			}
			return true
		})
	}
	return nil
}
func (h countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h countingHandler) WithGroup(string) slog.Handler      { return h }

// TestF3bR2_C_r2_3_KeepwarmUnchanged_PrefixBoundAndCohortSummary is the #130
// F3b-r2 cond-3 arm (arch-required for mode-ruling B): the keepwarm sweep's TWO
// load-bearing concerns are UNCHANGED by the boot-mode NavOrder-flat refactor —
//   (1) the widgetMax==0 PREFIX BOUND: the sweep breaks at the first widgetMax==0
//       (RA-only, widget-less) cohort → that cohort is NOT swept.
//   (2) the per-cohort cohort_summary: EXACTLY ONE prewarm.keepwarm.cohort_summary
//       INFO line per SWEPT (widget-capable) cohort.
// RED = a "flatten all modes" impl (apply the NavOrder-flat pass to keepwarm too):
// it has no per-rank cohort boundary, so it would emit ZERO cohort_summary lines
// AND would not break at widgetMax==0 (it would seed the widget-less tail cohort's
// RA targets in NavOrder order with the rest). Either divergence fails the asserts.
func TestF3bR2_C_r2_3_KeepwarmUnchanged_PrefixBoundAndCohortSummary(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()

	// Capture prewarm.keepwarm.cohort_summary INFO lines (their identity attr).
	var summaries []string
	prev := slog.Default()
	slog.SetDefault(slog.New(countingHandler{target: "prewarm.keepwarm.cohort_summary", identities: &summaries}))
	t.Cleanup(func() { slog.SetDefault(prev) })

	// Latch already fired at boot (keepwarm runs post-boot).
	resetFirstNavLatchForTest()
	latch := ensureFirstNavLatch()
	latch.fire("boot-already-fired", 0, 0, "", -1, 0)
	t.Cleanup(func() { resetFirstNavLatchForTest(); zeroCustomerInFlight() })

	// Two WIDGET-CAPABLE cohorts (devs widgetMax high, ops lower) both on a widget
	// GVR, PLUS a widget-less cohort (machineSA) present ONLY on an RA target GVR
	// (widgetMax==0 → the prefix-bound tail that must NOT be swept).
	devs := eID{name: "devs", group: true, collapsed: 442}
	ops := eID{name: "ops", group: true, collapsed: 5}
	machineSA := eID{name: "machine", group: true, collapsed: 3} // widget_max 0 (RA-only)

	h := newNavWidgetHarvester()
	wGVR := latchWidgetGVR("flexes")
	harvestWidgetAtRoot(h, "w", wGVR, 0)
	widgets := h.snapshot()
	raGVR := eFanoutRAGVR
	ras := []templatesv1.ObjectReference{latchRA("machine-ra")}

	rec := &latchRecorder{}
	installLatchSeams(t, rec,
		func(gvr schema.GroupVersionResource) []cache.PrewarmTarget {
			switch gvr {
			case wGVR:
				return latchTargets(gvr, devs, ops) // widget-capable cohorts
			case raGVR:
				return latchTargets(gvr, machineSA) // widget-LESS cohort (RA only)
			}
			return nil
		},
		func(_ templatesv1.ObjectReference) (schema.GroupVersionResource, bool) {
			return raGVR, true
		})

	if err := seedScopeYielding(context.Background(), ras, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeKeepwarm); err != nil {
		t.Fatalf("keepwarm seedScopeYielding: %v", err)
	}

	// (1) PREFIX BOUND: the widget-less machineSA cohort must NOT be swept — no
	// cohort_summary for it (the break at widgetMax==0 stops before it).
	for _, id := range summaries {
		if id == redact.Group("machine") { // #453: cohorts log as their redact label
			t.Fatalf("cond-3 PREFIX BOUND broken: widget-less cohort 'machine' (widgetMax==0) was swept "+
				"(got a cohort_summary) — the keepwarm prefix-break at widgetMax==0 is gone. summaries=%v", summaries)
		}
	}
	// (2) COHORT SUMMARY: exactly one per SWEPT (widget-capable) cohort = 2 (devs, ops).
	if len(summaries) != 2 {
		t.Fatalf("cond-3 COHORT SUMMARY broken: want exactly 2 cohort_summary lines (devs, ops — the "+
			"widget-capable prefix), got %d: %v. A flatten-all-modes impl emits ZERO (no per-cohort boundary).", len(summaries), summaries)
	}
	devsSeen, opsSeen := false, false
	for _, id := range summaries {
		if id == redact.Group("devs") {
			devsSeen = true
		}
		if id == redact.Group("ops") {
			opsSeen = true
		}
	}
	if !devsSeen || !opsSeen {
		t.Fatalf("cond-3: both widget-capable cohorts must get a summary; devs=%v ops=%v (summaries=%v)", devsSeen, opsSeen, summaries)
	}
}
