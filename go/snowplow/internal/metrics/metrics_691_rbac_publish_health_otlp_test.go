package metrics

// metrics_691_rbac_publish_health_otlp_test.go — #691: the three stuck-rebuild
// detectors reach OTLP (hand-wired in metrics.go), unlabelled, each reading its
// production accessor. RED without the wiring: no series.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// floatGauges691 collects the float64 gauge points by name (the shared flatten
// helper reads int64 data only).
func floatGauges691(rm metricdata.ResourceMetrics) map[string][]metricdata.DataPoint[float64] {
	out := map[string][]metricdata.DataPoint[float64]{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[float64]); ok {
				out[m.Name] = append(out[m.Name], g.DataPoints...)
			}
		}
	}
	return out
}

func TestIssue691_PublishHealthReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetRBACPublishHealthForTest()
	t.Cleanup(cache.ResetRBACPublishHealthForTest)

	rm := collectViaRealCallback(t, "deadbeef")
	panics := pointsFor(flatten(rm), "snowplow_rbac_rebuild_panics_total")
	if len(panics) != 1 || len(panics[0].attrs) != 0 {
		t.Fatalf("snowplow_rbac_rebuild_panics_total: %d series (attrs %v), want exactly 1 unlabelled — wiring missing", len(panics), panics)
	}
	if panics[0].value != int64(cache.RBACRebuildPanicsTotal()) {
		t.Fatalf("panics_total series %d != counter %d", panics[0].value, cache.RBACRebuildPanicsTotal())
	}
	fg := floatGauges691(rm)
	for _, name := range []string{"snowplow_rbac_unpublished_event_age_seconds", "snowplow_rbac_last_successful_publish_age_seconds"} {
		pts := fg[name]
		if len(pts) != 1 || pts[0].Attributes.Len() != 0 {
			t.Fatalf("%s: %d series, want exactly 1 unlabelled — wiring missing", name, len(pts))
		}
	}
	if v := fg["snowplow_rbac_unpublished_event_age_seconds"][0].Value; v != 0 {
		t.Fatalf("unpublished_event_age_seconds = %v with nothing pending, want 0", v)
	}
}
