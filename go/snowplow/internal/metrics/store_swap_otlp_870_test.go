package metrics

// store_swap_otlp_870_test.go — #870 K16 (3), the OTLP half: the known-stale
// family is mirrored on OTLP with the same values expvar reports, per reason
// and outcome, through the REAL registration and callback
// (collectViaRealCallback). The counters are moved by cache's production
// accounting path (RunStoreSwapAbortForTest), never set directly. RED under X23
// (not mirrored to OTLP) and X30 (no reason label).

import (
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestStoreSwap870_KnownStaleIsMirroredOnOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetStoreSwapForTest()
	t.Cleanup(cache.ResetStoreSwapForTest)
	gvr := schema.GroupVersionResource{Group: "otlp870.krateo.io", Version: "v1", Resource: "panels"}
	cache.RunStoreSwapAbortForTest(gvr, "divergence", []string{"ns/a", "ns/b", "ns/c"}, 4*time.Second)
	cache.RunStoreSwapAbortForTest(gvr, "schema", []string{"ns/a"}, 2*time.Second)

	rm := collectViaRealCallback(t, "test")
	keys := map[[2]string]int64{}
	builds := map[[2]string]int64{}
	win := map[[2]string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "snowplow_store_swap_known_stale_keys_total", "snowplow_store_swap_known_stale_builds_total":
				d, ok := m.Data.(metricdata.Sum[int64])
				if !ok {
					t.Fatalf("%s is %T, want a monotonic int64 sum", m.Name, m.Data)
				}
				for _, dp := range d.DataPoints {
					r, _ := dp.Attributes.Value("reason")
					o, _ := dp.Attributes.Value("outcome")
					k := [2]string{r.Emit(), o.Emit()}
					if m.Name == "snowplow_store_swap_known_stale_keys_total" {
						keys[k] = dp.Value
					} else {
						builds[k] = dp.Value
					}
				}
			case "snowplow_store_swap_known_stale_window_seconds_total":
				d, ok := m.Data.(metricdata.Sum[float64])
				if !ok {
					t.Fatalf("%s is %T, want a float64 sum", m.Name, m.Data)
				}
				for _, dp := range d.DataPoints {
					r, _ := dp.Attributes.Value("reason")
					o, _ := dp.Attributes.Value("outcome")
					win[[2]string{r.Emit(), o.Emit()}] = dp.Value
				}
			}
		}
	}
	div, sch := [2]string{"divergence", "abort_clean"}, [2]string{"schema", "abort_clean"}
	if keys[div] != 3 || keys[sch] != 1 {
		t.Fatalf("K16 (3) RED (X23/X30): OTLP keys_total = %v, want divergence|abort_clean=3 and schema|abort_clean=1", keys)
	}
	if builds[div] != 1 || builds[sch] != 1 {
		t.Fatalf("K16 (3) RED (X23): OTLP builds_total = %v, want 1 per reason", builds)
	}
	if win[div] != 4 || win[sch] != 2 {
		t.Fatalf("K16 (3) RED (X23): OTLP window_seconds_total = %v, want divergence=4 and schema=2", win)
	}
	// Every closed-set series exists from the first collection (a zero is a
	// reading, not an absence).
	if want := len(cache.StoreSwapReasons) * len(cache.StoreSwapOutcomes); len(keys) != want {
		t.Fatalf("OTLP keys_total carries %d series, want the closed set of %d", len(keys), want)
	}
}
