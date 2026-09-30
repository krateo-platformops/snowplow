// issue239_dirty_mark_otlp_test.go — #239 Arm D (OTLP-registered).
//
// The {path,cause,class} attribution, the fan-out denominator and the
// submit-source axis must reach ClickStack — i.e. be OBSERVED by the real
// RegisterCallback, not merely exposed on an accessor (#311: expvar-only never
// reaches OTLP). This drives a known dirty-mark through cache.Deps(), collects
// via the ManualReader over the real callback, and asserts the labeled point
// lands with its attributes. RED without the callback wiring in registerInstruments.

package metrics

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue239_D_DirtyMarkAttributionReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	gvr := schema.GroupVersionResource{Group: "widgets.ui.krateo.io", Version: "v1beta1", Resource: "widgets"}
	// A list-scope dependent, then an ADD for an object in its namespace: one
	// dirty-mark in (object_event, add_update, list_dep). No store needed — a
	// list_dep is classified by the dependency form, not the entry's output.
	cache.Deps().RecordList("L1_issue239_otlp", gvr, "ns")
	cache.Deps().OnAdd(gvr, "ns", "obj")

	all := flatten(collectViaRealCallback(t, "test"))

	pts := pointsFor(all, "snowplow_deps_dirty_mark_attributed_total")
	if len(pts) == 0 {
		t.Fatalf("snowplow_deps_dirty_mark_attributed_total not collected — the #239 attribution is not " +
			"registered/observed in the real callback, so it never reaches ClickStack (expvar-only, #311)")
	}
	found := false
	for _, p := range pts {
		if p.attrs["path"] == "object_event" && p.attrs["cause"] == "add_update" &&
			p.attrs["class"] == "list_dep" && p.value >= 1 {
			found = true
		}
	}
	if !found {
		t.Fatalf("no {path=object_event, cause=add_update, class=list_dep} point >=1 collected — the "+
			"labeled attribution did not reach OTLP with its attributes. points=%v", pts)
	}

	// The sibling #239 instruments are registered and observed too.
	if len(pointsFor(all, "snowplow_deps_dirty_mark_events_total")) == 0 {
		t.Error("snowplow_deps_dirty_mark_events_total not collected — the fan-out denominator is not on OTLP")
	}
	if len(pointsFor(all, "snowplow_deps_dirty_mark_submits_total")) == 0 {
		t.Error("snowplow_deps_dirty_mark_submits_total not collected — the submit-source axis is not on OTLP")
	}
	if len(pointsFor(all, "snowplow_deps_dirty_mark_unattributed_total")) == 0 {
		t.Error("snowplow_deps_dirty_mark_unattributed_total not collected — the classifier guard is not on OTLP")
	}
}
