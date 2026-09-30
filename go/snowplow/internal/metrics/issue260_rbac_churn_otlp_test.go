// issue260_rbac_churn_otlp_test.go — #260 change-4 Arm D (OTLP-registered).
//
// The RBAC sub-gen churn attribution must reach ClickStack (#311): the
// #253/#258/#259 keying call correlates rotation drivers against cold-fills on
// one time axis, which expvar-only (manual /debug/vars, no time series) can't
// support. This drives a re-establishment and collects via the ManualReader over
// the REAL RegisterCallback, asserting the labeled points land — the new
// reestablishment {gvr} counter AND the promoted landed detectors. RED without
// the callback wiring.

package metrics

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue260_D_RBACChurnAttributionReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetRBACReestablishmentForTest()
	t.Cleanup(cache.ResetRBACReestablishmentForTest)

	// Drive two re-establishments on a known RBAC GVR through the test seam.
	gvr := cache.RBACResourceTypes[0]
	cache.RecordRBACReestablishmentForTest(gvr)
	cache.RecordRBACReestablishmentForTest(gvr)
	cache.ResetRBACWatchErrorForTest()
	t.Cleanup(cache.ResetRBACWatchErrorForTest)
	cache.RecordRBACWatchErrorForTest(gvr)
	cache.RecordRBACWatchErrorForTest(gvr)
	cache.RecordRBACWatchErrorForTest(gvr)

	all := flatten(collectViaRealCallback(t, "test"))

	// Re-establishment {gvr}: one series per RBAC GVR (the snapshot is pre-keyed),
	// our GVR at value 2 with the {gvr} attribute.
	pts := pointsFor(all, "snowplow_rbac_reflector_reestablished_total")
	if len(pts) != len(cache.RBACResourceTypes) {
		t.Fatalf("snowplow_rbac_reflector_reestablished_total has %d series, want %d (one per RBAC GVR) — "+
			"the callback must range RBACReestablishmentSnapshot with a {gvr} attribute", len(pts), len(cache.RBACResourceTypes))
	}
	found := false
	for _, p := range pts {
		if _, ok := p.attrs["gvr"]; !ok {
			t.Fatalf("a re-establishment point carries no {gvr} attribute: %+v", p)
		}
		if p.attrs["gvr"] == gvr.String() && p.value == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("no {gvr=%s, value=2} re-establishment point — the counter did not reach OTLP with the "+
			"right value/attribute. points=%v", gvr.String(), pts)
	}

	// Watch-error {gvr}: one series per RBAC GVR, our GVR at value 3.
	wpts := pointsFor(all, "snowplow_rbac_reflector_watch_errors_total")
	if len(wpts) != len(cache.RBACResourceTypes) {
		t.Fatalf("snowplow_rbac_reflector_watch_errors_total has %d series, want %d (one per RBAC GVR)",
			len(wpts), len(cache.RBACResourceTypes))
	}
	wfound := false
	for _, p := range wpts {
		if _, ok := p.attrs["gvr"]; !ok {
			t.Fatalf("a watch-error point carries no {gvr} attribute: %+v", p)
		}
		if p.attrs["gvr"] == gvr.String() && p.value == 3 {
			wfound = true
		}
	}
	if !wfound {
		t.Fatalf("no {gvr=%s, value=3} watch-error point — the counter did not reach OTLP with the "+
			"right value/attribute. points=%v", gvr.String(), wpts)
	}

	// bumps_by_source {source}: the 6-bucket family, promoted from expvar.
	bs := pointsFor(all, "snowplow_rbac_subgen_bumps_by_source_total")
	if len(bs) != 6 {
		t.Fatalf("snowplow_rbac_subgen_bumps_by_source_total has %d series, want 6 {source} — the landed "+
			"6-bucket detector was not promoted to OTLP", len(bs))
	}
	for _, p := range bs {
		if _, ok := p.attrs["source"]; !ok {
			t.Fatalf("a bumps_by_source point carries no {source} attribute: %+v", p)
		}
	}

	// The promoted scalar detectors are registered too.
	for _, name := range []string{
		"snowplow_rbac_role_noop_updates_total",
		"snowplow_rbac_role_semantic_noop_updates_total",
		"snowplow_rbac_binding_uid_changed_updates_total",
	} {
		if len(pointsFor(all, name)) == 0 {
			t.Errorf("%s not collected — the landed #260 detector was not promoted to OTLP (still expvar-only)", name)
		}
	}
}
