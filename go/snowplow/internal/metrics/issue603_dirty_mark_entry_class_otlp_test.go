// issue603_dirty_mark_entry_class_otlp_test.go — #479 1b / #603 PR-0, OTLP arm.
//
// snowplow_deps_dirty_mark_by_entry_class_total{class} must be OBSERVED by the
// real RegisterCallback, not merely exposed on an accessor (#311: expvar-only
// never reaches ClickStack). RED without the hand-wire in registerInstruments.

package metrics

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue603_PR0_DirtyMarkByEntryClassReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	gvr := schema.GroupVersionResource{Group: "widgets.ui.krateo.io", Version: "v1beta1", Resource: "widgets"}
	// A list dependent with no stored cell: one mark, attributed "unknown".
	cache.Deps().RecordList(context.Background(), "L1_issue603_pr0_otlp", gvr, "ns")
	cache.Deps().OnAdd(gvr, "ns", "obj")

	all := flatten(collectViaRealCallback(t, "test"))
	pts := pointsFor(all, "snowplow_deps_dirty_mark_by_entry_class_total")
	if len(pts) == 0 {
		t.Fatal("snowplow_deps_dirty_mark_by_entry_class_total not collected — not hand-wired in the real callback (expvar-only, #311)")
	}
	classes := map[string]bool{}
	unknown := int64(0)
	for _, p := range pts {
		classes[p.attrs["class"]] = true
		if p.attrs["class"] == "unknown" {
			unknown = p.value
		}
	}
	for _, c := range append(cache.RemintClasses[:], "unknown", "unregistered") {
		if !classes[c] {
			t.Errorf("class=%q not observed — the closed label set must be pre-registered (zero included), got %v", c, classes)
		}
	}
	if unknown < 1 {
		t.Errorf("class=unknown value=%d, want >=1 (the driven mark)", unknown)
	}
}
