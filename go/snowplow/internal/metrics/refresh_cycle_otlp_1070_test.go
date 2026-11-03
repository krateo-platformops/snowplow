package metrics

// refresh_cycle_otlp_1070_test.go — #1070, the OTLP half: the refresh cycle's
// Put counter and three histograms reach OTLP through the REAL registration
// (registerInstruments) with the same values expvar publishes, per tier and
// per class|tier|content. The samples are moved by cache's production
// recording path on injected instants (RunRefreshCycleForTest), never set.

import (
	"context"
	"encoding/json"
	"expvar"
	"strconv"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestRefreshCycle1070_OTLPAndExpvarAgree(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetRefreshCycleForTest()
	t.Cleanup(cache.ResetRefreshCycleForTest)
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(cache.ResetRefreshBroadcasterForTest)
	cache.RegisterRefreshCycleExpvarForTest()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	if err := registerInstruments(mp.Meter(meterName), "test"); err != nil {
		t.Fatalf("registerInstruments: %v", err)
	}
	t.Cleanup(func() { cache.SetRefreshCycleRecorder(nil) })

	ms := time.Millisecond
	for _, run := range []struct {
		name, tier         string
		identical          bool
		wait, dur, toFrame time.Duration
	}{
		{"otlp-a", cache.RefreshTierNormal, true, 1500 * ms, 2500 * ms, 1200 * ms},
		{"otlp-b", cache.RefreshTierViewed, false, 300 * ms, 700 * ms, 4000 * ms},
		{"otlp-c", cache.RefreshTierViewed, true, 12000 * ms, 40 * ms, 9 * ms},
	} {
		if !cache.RunRefreshCycleForTest(run.name, run.tier, run.identical, run.wait, run.dur, run.toFrame) {
			t.Fatalf("PRE: driven cycle %s did not complete", run.name)
		}
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	hists := map[string]map[string]metricdata.HistogramDataPoint[int64]{}
	puts := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch m.Name {
			case "snowplow_refresh_cycle_wait_ms", "snowplow_refresh_cycle_duration_ms", "snowplow_refresh_broadcaster_put_to_frame_ms":
				d, ok := m.Data.(metricdata.Histogram[int64])
				if !ok {
					t.Fatalf("%s is %T, want an int64 histogram", m.Name, m.Data)
				}
				hists[m.Name] = map[string]metricdata.HistogramDataPoint[int64]{}
				for _, dp := range d.DataPoints {
					tier, _ := dp.Attributes.Value("tier")
					hists[m.Name][tier.Emit()] = dp
				}
			case "snowplow_refresh_cycle_put_total":
				d, ok := m.Data.(metricdata.Sum[int64])
				if !ok || !d.IsMonotonic {
					t.Fatalf("%s is %T, want a monotonic int64 sum", m.Name, m.Data)
				}
				for _, dp := range d.DataPoints {
					c, _ := dp.Attributes.Value("class")
					tr, _ := dp.Attributes.Value("tier")
					k, _ := dp.Attributes.Value("content")
					puts[c.Emit()+"|"+tr.Emit()+"|"+k.Emit()] = dp.Value
				}
			}
		}
	}

	// The counter: OTLP == expvar, cell by cell, over the closed set.
	var ev map[string]uint64
	if err := json.Unmarshal([]byte(expvar.Get("snowplow_refresh_cycle_put_total").String()), &ev); err != nil {
		t.Fatal(err)
	}
	if len(puts) != len(ev) || len(ev) == 0 {
		t.Fatalf("RED: OTLP put_total has %d series, expvar %d", len(puts), len(ev))
	}
	for k, v := range ev {
		if puts[k] != int64(v) {
			t.Fatalf("RED: put_total %s: OTLP %d, expvar %d", k, puts[k], v)
		}
	}
	if ev["widgets|normal|identical"] != 1 || ev["widgets|viewed|changed"] != 1 || ev["widgets|viewed|identical"] != 1 {
		t.Fatalf("RED: put_total = %v, want widgets normal|identical, viewed|changed and viewed|identical at 1", ev)
	}

	// The histograms: OTLP == expvar per tier: count, sum and every bucket.
	type evHist struct {
		Buckets map[string]uint64 `json:"buckets"`
		Count   uint64            `json:"count"`
		SumMS   uint64            `json:"sum_ms"`
	}
	want := map[string]map[string]uint64{ // name → tier → sum
		"snowplow_refresh_cycle_wait_ms":               {cache.RefreshTierNormal: 1500, cache.RefreshTierViewed: 12300},
		"snowplow_refresh_cycle_duration_ms":           {cache.RefreshTierNormal: 2500, cache.RefreshTierViewed: 740},
		"snowplow_refresh_broadcaster_put_to_frame_ms": {cache.RefreshTierNormal: 1200, cache.RefreshTierViewed: 4009},
	}
	for name, sums := range want {
		var m map[string]evHist
		if err := json.Unmarshal([]byte(expvar.Get(name).String()), &m); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		for tier, sum := range sums {
			dp, ok := hists[name][tier]
			e := m[tier]
			if !ok {
				t.Fatalf("RED: OTLP %s has no tier=%s series (expvar count=%d)", name, tier, e.Count)
			}
			if e.SumMS != sum || uint64(dp.Sum) != e.SumMS || dp.Count != e.Count {
				t.Fatalf("RED: %s tier=%s: OTLP count=%d sum=%d, expvar count=%d sum=%d, want sum %d", name, tier, dp.Count, dp.Sum, e.Count, e.SumMS, sum)
			}
			if len(dp.Bounds) != len(cache.RefreshCycleBoundsMS) {
				t.Fatalf("RED: %s bounds %v, want %v", name, dp.Bounds, cache.RefreshCycleBoundsMS)
			}
			var cum uint64
			for i, b := range cache.RefreshCycleBoundsMS {
				cum += dp.BucketCounts[i]
				if got := e.Buckets["le_"+strconv.FormatInt(b, 10)]; got != cum {
					t.Fatalf("RED: %s tier=%s le_%d: expvar %d, OTLP cumulative %d", name, tier, b, got, cum)
				}
			}
		}
	}
}
