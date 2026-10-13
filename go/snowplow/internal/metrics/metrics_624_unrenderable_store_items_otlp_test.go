package metrics

// metrics_624_unrenderable_store_items_otlp_test.go — #624: the fail-closed
// counter for an unrenderable informer store slot reaches OTLP as
// snowplow_informer_unrenderable_store_items_total{site,gvr}, hand-wired in
// metrics.go (no expvar->OTLP bridge). Driven through the PRODUCTION record
// path (cache.RecordUnrenderableStoreItemForTest calls recordUnrenderableStoreItem)
// and observed via the REAL callback over a ManualReader. RED without the
// metrics.go wiring: no series for the driven cell.

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue624_UnrenderableStoreItemsReachOTLP(t *testing.T) {
	// #636: the cells are process-global. Without this, iteration 2 of
	// -count>1 finds iteration 1's cell and the isolation check below fails.
	cache.ResetUnrenderableStoreItemsForTest()
	t.Cleanup(cache.ResetUnrenderableStoreItemsForTest)

	gvr := schema.GroupVersionResource{Group: "otlp624.krateo.io", Version: "v1", Resource: "things"}
	find := func() (int64, bool) {
		for _, p := range pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_informer_unrenderable_store_items_total") {
			if p.attrs["site"] == "list_raw" && p.attrs["gvr"] == gvr.String() {
				return p.value, true
			}
		}
		return 0, false
	}
	if _, ok := find(); ok {
		t.Fatalf("cell exists before any record — the arm is not isolated")
	}

	cache.RecordUnrenderableStoreItemForTest("list_raw", gvr)
	cache.RecordUnrenderableStoreItemForTest("list_raw", gvr)

	v, ok := find()
	if !ok {
		t.Fatalf("no snowplow_informer_unrenderable_store_items_total{site=list_raw,gvr=%s} series — metrics.go wiring missing", gvr)
	}
	if v != 2 {
		t.Fatalf("OTLP value = %d after 2 records; want 2", v)
	}
}
