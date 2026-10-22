package metrics

// metrics_external_fetch_cache_otlp_test.go — the external fetch's
// conversion-memo counters reach OTLP, hand-wired in metrics.go (no
// expvar->OTLP bridge), observed via the REAL callback over a ManualReader. RED
// without the metrics.go wiring: no series for the outcome cells.

import (
	"testing"

	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func TestExternalFetchCacheCountersReachOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	outcomes := []string{"convert_hit", "convert_miss"}
	read := func(all []point, name string, attrs map[string]string) int64 {
		v, ok := find643(all, name, attrs)
		if !ok {
			t.Fatalf("no %s%v series — metrics.go wiring missing (cells are pre-populated)", name, attrs)
		}
		return v
	}
	before := flatten(collectViaRealCallback(t, "deadbeef"))
	prior := map[string]int64{}
	for _, o := range outcomes {
		prior[o] = read(before, "snowplow_external_fetch_conversion_total", map[string]string{"outcome": o})
	}
	evBefore := read(before, "snowplow_external_fetch_conversion_evictions_total", map[string]string{})
	read(before, "snowplow_external_fetch_conversion_bytes", map[string]string{})
	read(before, "snowplow_external_fetch_conversion_entries", map[string]string{})

	restactionsapi.RecordExternalFetchCacheForTest()

	all := flatten(collectViaRealCallback(t, "deadbeef"))
	for _, o := range outcomes {
		if got := read(all, "snowplow_external_fetch_conversion_total", map[string]string{"outcome": o}) - prior[o]; got != 1 {
			t.Errorf("external_fetch_conversion_total{outcome=%s} delta = %d; want 1", o, got)
		}
	}
	if got := read(all, "snowplow_external_fetch_conversion_evictions_total", map[string]string{}) - evBefore; got != 1 {
		t.Errorf("external_fetch_conversion_evictions_total delta = %d; want 1", got)
	}
}
