package metrics

// store_swap_bygvr_otlp_928_test.go — #928 bench review: WHICH GVR a store
// build swapped, on OTLP.

import (
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// TestStoreSwap870_SwapsByGVRMirroredOnOTLP: the per-GVR build outcomes reach
// OTLP as snowplow_store_swaps_by_gvr_total{gvr, outcome}, with gvr spelled
// GroupVersionResource.String() (the spelling snowplow_resolve_collection_lists
// uses, #991), every outcome of the closed set present for a GVR that had a
// build (a zero is a reading), and no other attribute. RED under G6 (no gvr
// attribute) and G7 (another gvr spelling).
func TestStoreSwap870_SwapsByGVRMirroredOnOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetStoreSwapForTest()
	t.Cleanup(cache.ResetStoreSwapForTest)
	gvr := schema.GroupVersionResource{Group: "otlp870.krateo.io", Version: "v1", Resource: "panels"}
	cache.NoteStoreSwapOutcomeForTest(gvr, "installed")
	cache.NoteStoreSwapOutcomeForTest(gvr, "installed")
	cache.NoteStoreSwapOutcomeForTest(gvr, "abort_refused")

	got := map[string]int64{}
	rm := collectViaRealCallback(t, "test")
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "snowplow_store_swaps_by_gvr_total" {
				continue
			}
			d, ok := m.Data.(metricdata.Sum[int64])
			if !ok || !d.IsMonotonic {
				t.Fatalf("%s is %T, want a monotonic int64 sum", m.Name, m.Data)
			}
			for _, dp := range d.DataPoints {
				g, okG := dp.Attributes.Value("gvr")
				o, okO := dp.Attributes.Value("outcome")
				if !okG || !okO || dp.Attributes.Len() != 2 {
					t.Fatalf("G6 RED: a data point carries attributes %v, want exactly {gvr, outcome}", dp.Attributes.ToSlice())
				}
				got[g.Emit()+"|"+o.Emit()] = dp.Value
			}
		}
	}
	want := map[string]int64{}
	for _, o := range cache.StoreSwapGVROutcomes {
		want[gvr.String()+"|"+o] = 0
	}
	want[gvr.String()+"|installed"] = 2
	want[gvr.String()+"|abort_refused"] = 1
	if len(got) != len(want) {
		t.Fatalf("G7 RED: OTLP series %v, want %v (gvr spelled %q)", got, want, gvr.String())
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("G7 RED: OTLP %s = %d, want %d (series %v)", k, got[k], v, got)
		}
	}
}
