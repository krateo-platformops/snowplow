// issue594_seed_order_external_last_test.go — #594: the boot seed orders
// external-backed widget units LAST, so a pipGlobalTimeout cut loses the
// population that can never publish instead of an arbitrary walk suffix.
//
// WHAT MAKES THESE ARMS LOAD-BEARING. ARM1 and ARM2 are RED against the
// pre-#594 sort: with NavOrder as the primary key, an external widget harvested
// mid-walk seeds in walk position, so asserting it seeds after a LATER-harvested
// internal widget fails. ARM3 is the guard that this stayed an ORDERING and did
// not become #403's skip (the unit SET must be untouched). ARM4 is the
// non-vacuity/failure-direction arm: with the probe answering false for every
// widget the sequence must be byte-identical to the pre-#594 order, so a
// cost-class key that fired unconditionally could not pass. ARM5 drives the
// readiness-exit instrument that makes the aborted tail readable at all.
//
// These reuse prewarm_engine_seed_order_test.go's harness (eHarvestWidget,
// eSeedRecorder, the enumerate/seedOne seams) and drive the REAL
// seedScopeYielding — not a re-implementation of its comparator
// (feedback_falsifier_must_drive_real_boundary_not_install_crossed_state).

package dispatchers

import (
	"context"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// i594Fixture harvests widgets in the given order through the REAL harvester
// (so NavOrder is the harvest sequence, not hand-assigned), installs the seams,
// runs seedScopeYielding in boot mode and returns the recorded widget sequence.
//
// external names the widgets whose apiRef RESTAction declares an EndpointRef.
func i594Run(t *testing.T, names []string, external map[string]bool) []string {
	return i594RunMode(t, names, external, seedModeBoot)
}

func i594RunMode(t *testing.T, names []string, external map[string]bool, mode seedScopeMode) []string {
	t.Helper()
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	only := eID{name: "devs", group: true, collapsed: 10}

	h := newNavWidgetHarvester()
	gvrs := map[string]schema.GroupVersionResource{}
	for i, n := range names {
		// A distinct resource per widget so the enumerator seam can answer per
		// widget; the resource string is irrelevant to the ordering under test.
		g := eWidgetGVR("res" + string(rune('a'+i)))
		gvrs[n] = g
		eHarvestWidget(h, n, g)
	}
	widgets := h.snapshot()
	if len(widgets) != len(names) {
		t.Fatalf("#594 setup: harvested %d widgets, want %d", len(widgets), len(names))
	}

	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = func(gvr schema.GroupVersionResource, _ string) []cache.PrewarmTarget {
		for _, g := range gvrs {
			if g == gvr {
				return eIdentityTargets(gvr, only)
			}
		}
		return nil
	}
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	prevExt := seedWidgetIsExternalFn
	seedWidgetIsExternalFn = func(_ context.Context, w *unstructured.Unstructured) bool {
		if w == nil {
			return false
		}
		return external[w.GetName()]
	}
	t.Cleanup(func() { seedWidgetIsExternalFn = prevExt })

	rec := &eSeedRecorder{}
	prevW := seedOneWidgetFn
	seedOneWidgetFn = func(ctx context.Context, e navWidgetEntry, _ string, _ seedScopeMode) error {
		rec.record("widget", e.W.GetName(), eIdentityLabel(ctx))
		return nil
	}
	t.Cleanup(func() { seedOneWidgetFn = prevW })

	if err := seedScopeYielding(context.Background(), nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", mode); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}

	var seq []string
	for _, e := range rec.snapshot() {
		if e.class == "widget" {
			seq = append(seq, e.label)
		}
	}
	return seq
}

func i594Equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ARM1 — an EXTERNAL widget harvested EARLY in the walk seeds AFTER an INTERNAL
// widget harvested later. RED pre-#594 (NavOrder-major would keep obs-panel
// second).
func TestI594_Arm1_ExternalWidgetSeedsAfterLaterInternalWidget(t *testing.T) {
	names := []string{"dashboard-flex", "obs-panel", "settings-card"}
	got := i594Run(t, names, map[string]bool{"obs-panel": true})
	want := []string{"dashboard-flex", "settings-card", "obs-panel"}
	if !i594Equal(got, want) {
		t.Fatalf("#594 ARM1 VIOLATED: external-backed widget did not sort to the tail.\n"+
			" got  %v\n want %v\n"+
			"obs-panel declares an EndpointRef, so its unit must seed after EVERY internal-backed unit "+
			"even though it was harvested at NavOrder 1 — a deadline cut must lose the cell that can "+
			"never publish, not the later internal one.", got, want)
	}
}

// ARM2 — first-nav walk order is preserved INSIDE each cost class. Two internal
// and two external widgets interleaved in the walk must come out as
// (internal in NavOrder) then (external in NavOrder). RED pre-#594.
func TestI594_Arm2_NavOrderPreservedWithinEachCostClass(t *testing.T) {
	names := []string{"ext-early", "int-early", "ext-late", "int-late"}
	got := i594Run(t, names, map[string]bool{"ext-early": true, "ext-late": true})
	want := []string{"int-early", "int-late", "ext-early", "ext-late"}
	if !i594Equal(got, want) {
		t.Fatalf("#594 ARM2 VIOLATED: cost class must be the PRIMARY key and NavOrder the secondary.\n"+
			" got  %v\n want %v\n"+
			"Within each class the walk-derived first-nav order must still decide (int-early before "+
			"int-late, ext-early before ext-late); only the class boundary reorders.", got, want)
	}
}

// ARM3 — the unit SET is unchanged. This is the guard that #594 stayed an
// ordering and did not become #403's pre-resolve skip, which DROPPED units and
// then became load-bearing on a gate that moved (#591).
func TestI594_Arm3_OrderingDropsNoUnit(t *testing.T) {
	names := []string{"a-int", "b-ext", "c-ext", "d-int"}
	got := i594Run(t, names, map[string]bool{"b-ext": true, "c-ext": true})
	if len(got) != len(names) {
		t.Fatalf("#594 ARM3 VIOLATED: %d widget units seeded, want %d — #594 may ONLY reorder. "+
			"A dropped external unit is #403's skip returning, which is exactly what this change "+
			"must not be. seq=%v", len(got), len(names), got)
	}
	seen := map[string]int{}
	for _, n := range got {
		seen[n]++
	}
	for _, n := range names {
		if seen[n] != 1 {
			t.Fatalf("#594 ARM3 VIOLATED: widget %q seeded %d times, want exactly 1; seq=%v", n, seen[n], got)
		}
	}
}

// ARM4 — NON-VACUITY / FAILURE DIRECTION. With the cost-class probe answering
// false for every widget (an unresolvable or informer-missed apiRef), the
// sequence must be byte-identical to the pre-#594 NavOrder order. A comparator
// whose new key fired unconditionally would fail this.
func TestI594_Arm4_AllInternalIsByteIdenticalToNavOrder(t *testing.T) {
	names := []string{"w0", "w1", "w2", "w3"}
	got := i594Run(t, names, map[string]bool{}) // probe answers false for all
	if !i594Equal(got, names) {
		t.Fatalf("#594 ARM4 VIOLATED: with no external widget the order must be unchanged from "+
			"pre-#594 NavOrder.\n got  %v\n want %v\n"+
			"A false from the cost-class probe means 'cannot prove external', and must degrade to "+
			"today's first-nav order rather than to a different one.", got, names)
	}
}

// ARM4b — the all-external degenerate case is also plain NavOrder: when every
// unit is in the same class the new key is inert. Together with ARM4 this pins
// the key as a CLASS SPLIT, not a reversal.
func TestI594_Arm4b_AllExternalIsByteIdenticalToNavOrder(t *testing.T) {
	names := []string{"w0", "w1", "w2"}
	got := i594Run(t, names, map[string]bool{"w0": true, "w1": true, "w2": true})
	if !i594Equal(got, names) {
		t.Fatalf("#594 ARM4b VIOLATED: with every unit external the order must still be NavOrder.\n"+
			" got  %v\n want %v", got, names)
	}
}

// ARM5 — the readiness-exit instrument. nav_units_external must carry the size
// of the deferred tail, otherwise "the cut lost only the least valuable units"
// is an unfalsifiable claim about an ordering nobody can see.
func TestI594_Arm5_NavUnitsExternalIsRecorded(t *testing.T) {
	bootNavProgressState.reset()
	t.Cleanup(func() { bootNavProgressState.reset() })

	bootNavProgressState.arm(10, 2, 3)
	snap := bootNavProgressState.snapshot()
	if snap.total != 10 || snap.external != 3 {
		t.Fatalf("#594 ARM5 VIOLATED: arm(total=10, cohorts=2, external=3) produced total=%d external=%d",
			snap.total, snap.external)
	}

	// A cut that lands entirely inside the external tail is the ACCEPTABLE
	// shape: aborted <= external.
	for i := 0; i < 8; i++ {
		bootNavProgressState.unitProcessed(nil)
	}
	bootNavProgressState.cut()
	snap = bootNavProgressState.snapshot()
	if snap.aborted != 2 {
		t.Fatalf("#594 ARM5 setup: aborted=%d, want 2", snap.aborted)
	}
	if snap.aborted > snap.external {
		t.Fatalf("#594 ARM5 VIOLATED: aborted=%d exceeds external=%d, so the comparison that "+
			"classifies the cut is not derivable from the snapshot", snap.aborted, snap.external)
	}

	bootNavProgressState.reset()
	if s := bootNavProgressState.snapshot(); s.external != 0 {
		t.Fatalf("#594 ARM5 VIOLATED: reset left external=%d, want 0 (a stale tail size would "+
			"misclassify the NEXT boot's cut)", s.external)
	}
}

// ARM6 — the predicate the ordering keys on, exercised through the API type.
// Guards the nil receiver and nil api-step elements the sibling predicate
// guards, and the two real shapes.
func TestI594_Arm6_HasExternalEndpointStage(t *testing.T) {
	var nilRA *templatesv1.RESTAction
	if nilRA.HasExternalEndpointStage() {
		t.Fatalf("#594 ARM6: nil receiver must answer false")
	}

	none := &templatesv1.RESTAction{}
	none.Spec.API = []*templatesv1.API{{Name: "internal", Path: "/apis/v1/things"}}
	if none.HasExternalEndpointStage() {
		t.Fatalf("#594 ARM6: an apiserver-path stage must answer false")
	}

	withNil := &templatesv1.RESTAction{}
	withNil.Spec.API = []*templatesv1.API{nil}
	if withNil.HasExternalEndpointStage() {
		t.Fatalf("#594 ARM6: a nil api-step element must answer false, not panic")
	}

	ext := &templatesv1.RESTAction{}
	ext.Spec.API = []*templatesv1.API{
		{Name: "internal", Path: "/apis/v1/things"},
		{Name: "perf", EndpointRef: &templatesv1.Reference{Name: "clickhouse-endpoint", Namespace: "krateo-system"}},
	}
	if !ext.HasExternalEndpointStage() {
		t.Fatalf("#594 ARM6: a stage carrying an EndpointRef must answer true — this is the cost "+
			"class the whole seed ordering keys on")
	}
}

// ARM7 — the KEEPWARM / learned-phase loops iterate widgetSeeds directly rather
// than the flat unit list, so they are governed by the OTHER sort. Without this
// arm the widgetSeeds cost-class key is an uncovered behaviour change: removing
// it leaves every other arm in this file green, because the boot path rebuilds
// and re-sorts its own list. Keepwarm has a budget too, and an external cell it
// re-warms has the same no-dep-edges / bounded-TTL profile.
func TestI594_Arm7_KeepwarmOrdersExternalLast(t *testing.T) {
	names := []string{"dashboard-flex", "obs-panel", "settings-card"}
	got := i594RunMode(t, names, map[string]bool{"obs-panel": true}, seedModeKeepwarm)
	want := []string{"dashboard-flex", "settings-card", "obs-panel"}
	if !i594Equal(got, want) {
		t.Fatalf("#594 ARM7 VIOLATED: the keepwarm loop iterates widgetSeeds in sorted order and "+
			"must also defer external-backed units.\n got  %v\n want %v\n"+
			"This arm exists because the boot path re-sorts its own flat list, so the widgetSeeds "+
			"comparator is invisible to every other arm here.", got, want)
	}
}

// ARM8 — CTX LIVENESS. The cost-class probe makes an objects.Get, and
// objects.Get FAIL-CLOSES on a ctx with no identity (filterGetByRBAC; see the
// "P0 — seed under the SA-credentialed rctx" comment at the seedScopeYielding
// call site, which exists because exactly that bug bit restActionTargetGVR).
// A probe handed an identity-stripped ctx would answer false for every widget:
// the ordering would go INERT IN PRODUCTION while every seam-driven arm above
// stayed green — the failure this arm exists to make impossible.
//
// What it pins: the probe is invoked with the SAME ctx value the seed pass
// received, so nothing between the two strips or replaces it. That the pass ctx
// is SA-credentialed is the caller's documented invariant (rctx), shared with
// the restActionTargetGVR objects.Get already proven in production on this path.
func TestI594_Arm8_CostClassProbeGetsThePassContext(t *testing.T) {
	engineLatchTestMu.Lock()
	defer engineLatchTestMu.Unlock()
	zeroCustomerInFlight()
	quietLoggingE(t)

	h := newNavWidgetHarvester()
	gvr := eWidgetGVR("flexes")
	eHarvestWidget(h, "w0", gvr)
	widgets := h.snapshot()

	prevEnum := enumeratePrewarmTargetsForGVRFn
	enumeratePrewarmTargetsForGVRFn = func(g schema.GroupVersionResource, _ string) []cache.PrewarmTarget {
		if g == gvr {
			return eIdentityTargets(g, eID{name: "devs", group: true, collapsed: 1})
		}
		return nil
	}
	t.Cleanup(func() { enumeratePrewarmTargetsForGVRFn = prevEnum })

	type ctxKey struct{}
	passCtx := context.WithValue(context.Background(), ctxKey{}, "sa-marker")

	var sawMarker, called bool
	prevExt := seedWidgetIsExternalFn
	seedWidgetIsExternalFn = func(ctx context.Context, _ *unstructured.Unstructured) bool {
		called = true
		if v, ok := ctx.Value(ctxKey{}).(string); ok && v == "sa-marker" {
			sawMarker = true
		}
		return false
	}
	t.Cleanup(func() { seedWidgetIsExternalFn = prevExt })

	prevW := seedOneWidgetFn
	seedOneWidgetFn = func(context.Context, navWidgetEntry, string, seedScopeMode) error { return nil }
	t.Cleanup(func() { seedOneWidgetFn = prevW })

	if err := seedScopeYielding(passCtx, nil, widgets, endpoints.Endpoint{}, nil, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("seedScopeYielding returned %v; want nil", err)
	}
	if !called {
		t.Fatalf("#594 ARM8 VIOLATED: the cost-class probe was never invoked — the ordering key is " +
			"dead and every other arm here is passing on a seam nothing calls")
	}
	if !sawMarker {
		t.Fatalf("#594 ARM8 VIOLATED: the probe did NOT receive the seed pass ctx. objects.Get " +
			"fail-closes without an identity, so a stripped ctx makes DeclaresExternalEndpoint " +
			"answer false for every widget and the ordering goes inert in production while the " +
			"seam-driven arms stay green.")
	}
}
