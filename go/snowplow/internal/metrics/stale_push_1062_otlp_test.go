package metrics

// stale_push_1062_otlp_test.go — #1062: the every-layer stale-push family
// reaches OTLP from the REAL registration, and the OTLP figures equal the
// /debug/vars figures for every layer: the same snapshot feeds both. The
// #710 name snowplow_apistage_stale_to_push_ms reads exactly the apistage
// samples, on both surfaces. RED without registerStalePushInstruments (every
// series absent) or with the alias fed from anything but the ledger.

import (
	"context"
	"encoding/json"
	"expvar"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func expvarJSON(t *testing.T, name string) map[string]any {
	t.Helper()
	v := expvar.Get(name)
	if v == nil {
		t.Fatalf("expvar %s is not published", name)
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(v.String()), &out); err != nil {
		t.Fatalf("expvar %s: %v", name, err)
	}
	return out
}

func inSet(set []string, v string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}

func histPoints(rm metricdata.ResourceMetrics, name string) []metricdata.HistogramDataPoint[int64] {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if h, ok := m.Data.(metricdata.Histogram[int64]); ok && m.Name == name {
				return h.DataPoints
			}
		}
	}
	return nil
}

func TestIssue1062_StalePushOTLPEqualsExpvarForEveryLayer(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("REFRESH_SSE_ENABLED", "true")
	cache.RegisterStalePushExpvarForTest()
	restactionsapi.RegisterStaleExpvarForTest()
	cache.ResetRefreshBroadcasterForTest()
	t.Cleanup(cache.ResetRefreshBroadcasterForTest)
	cache.ResetStalePushForTest()
	t.Cleanup(cache.ResetStalePushForTest)
	t.Cleanup(func() { cache.SetStaleToPushRecorder(nil) })

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	if err := registerInstruments(mp.Meter(meterName), "deadbeef"); err != nil {
		t.Fatalf("registerInstruments: %v", err)
	}

	// Distinct figures per layer, through the production paths: samples via the
	// ledger's record path, apistage serves via the hit write point, one of them
	// viewed past the 10 s max (the overdue scan).
	for _, ms := range []int64{150, 2500} {
		cache.AddStaleToPushForTest(cache.StaleLayerAPIStage, ms)
	}
	for _, ms := range []int64{40, 900, 3100, 12000} {
		cache.AddStaleToPushForTest(cache.StaleLayerL1, ms)
	}
	base := time.Unix(2_000_000, 0)
	var now atomic.Int64 // the scanner goroutine reads the clock too
	now.Store(base.UnixNano())
	t.Cleanup(cache.SetStalePushClockForTest(func() time.Time { return time.Unix(0, now.Load()) }))
	derived := &cache.ResolvedEntry{RawJSON: []byte(`{}`), StaleDerived: true}
	cache.NoteStaleHit("k1062-viewed", derived)
	cache.NoteStaleHit("k1062-unviewed", derived)
	cache.NoteStaleHit("k1062-unviewed", derived)
	st := cache.SubscribeRefreshStream(map[string]struct{}{"k1062-viewed": {}})
	defer st.Unsub()
	now.Store(base.Add(11 * time.Second).UnixNano())
	cache.ScanStalePushOverdueForTest()

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	all := flatten(rm)
	byLayer := func(name string) map[string]int64 {
		out := map[string]int64{}
		for _, p := range pointsFor(all, name) {
			if len(p.attrs) != 1 || !inSet(cache.StaleLayers, p.attrs["layer"]) {
				t.Fatalf("%s: attrs %v, want only layer, from the closed set %v", name, p.attrs, cache.StaleLayers)
			}
			out[p.attrs["layer"]] = p.value
		}
		return out
	}
	wantCounts := map[string]map[string]int64{
		"snowplow_stale_serves_total":     {"apistage": 3, "l1": 0},
		"snowplow_stale_unreplaced_total": {"apistage": 1, "l1": 0},
		"snowplow_stale_serves_open":      {"apistage": 3, "l1": 0},
	}
	for name, want := range wantCounts {
		got := byLayer(name)
		ev := expvarJSON(t, name)
		for _, layer := range cache.StaleLayers {
			if got[layer] != want[layer] {
				t.Fatalf("%s{layer=%s} OTLP = %d, want %d", name, layer, got[layer], want[layer])
			}
			if e, _ := ev[layer].(float64); int64(e) != got[layer] {
				t.Fatalf("%s{layer=%s}: expvar %v != OTLP %d — the surfaces diverged", name, layer, ev[layer], got[layer])
			}
		}
	}
	closesEV := expvarJSON(t, "snowplow_stale_serve_closes_total")
	for _, p := range pointsFor(all, "snowplow_stale_serve_closes_total") {
		if len(p.attrs) != 2 || !inSet(cache.StaleLayers, p.attrs["layer"]) || !inSet(cache.StalePushCloses, p.attrs["close"]) {
			t.Fatalf("closes: attrs %v outside the closed sets %v × %v", p.attrs, cache.StaleLayers, cache.StalePushCloses)
		}
		m, _ := closesEV[p.attrs["layer"]].(map[string]any)
		if e, _ := m[p.attrs["close"]].(float64); int64(e) != p.value {
			t.Fatalf("closes %v: expvar %v != OTLP %d", p.attrs, m[p.attrs["close"]], p.value)
		}
	}
	if n := len(pointsFor(all, "snowplow_stale_serve_closes_total")); n != len(cache.StaleLayers)*len(cache.StalePushCloses) {
		t.Fatalf("closes: %d points, want layer × close = %d", n, len(cache.StaleLayers)*len(cache.StalePushCloses))
	}

	wantHist := map[string]struct{ count, sum int64 }{"apistage": {2, 2650}, "l1": {4, 16040}}
	evHist := expvarJSON(t, "snowplow_stale_to_push_ms")
	pts := histPoints(rm, "snowplow_stale_to_push_ms")
	if len(pts) != 2 {
		t.Fatalf("snowplow_stale_to_push_ms: %d points, want one per layer", len(pts))
	}
	for _, dp := range pts {
		layer, _ := dp.Attributes.Value("layer")
		w := wantHist[layer.AsString()]
		if int64(dp.Count) != w.count || dp.Sum != w.sum {
			t.Fatalf("stale_to_push{%s} OTLP count/sum = %d/%d, want %d/%d", layer.AsString(), dp.Count, dp.Sum, w.count, w.sum)
		}
		e, _ := evHist[layer.AsString()].(map[string]any)
		if c, _ := e["count"].(float64); uint64(c) != dp.Count {
			t.Fatalf("stale_to_push{%s}: expvar count %v != OTLP %d", layer.AsString(), e["count"], dp.Count)
		}
		if s, _ := e["sum_ms"].(float64); int64(s) != dp.Sum {
			t.Fatalf("stale_to_push{%s}: expvar sum %v != OTLP %d", layer.AsString(), e["sum_ms"], dp.Sum)
		}
		have := map[float64]bool{}
		for _, b := range dp.Bounds {
			have[b] = true
		}
		for _, b := range []float64{1000, 3000, 10000} {
			if !have[b] {
				t.Fatalf("stale_to_push bounds %v lack %v — the owner bar would not be decidable", dp.Bounds, b)
			}
		}
	}
	// The #710 name: exactly the apistage samples, on both surfaces.
	alias := histPoints(rm, "snowplow_apistage_stale_to_push_ms")
	if len(alias) != 1 || alias[0].Count != 2 || alias[0].Sum != 2650 {
		t.Fatalf("snowplow_apistage_stale_to_push_ms = %+v, want the 2 apistage samples (sum 2650)", alias)
	}
	aev := expvarJSON(t, "snowplow_apistage_stale_to_push_ms")
	if c, _ := aev["count"].(float64); c != 2 {
		t.Fatalf("expvar snowplow_apistage_stale_to_push_ms count = %v, want 2", aev["count"])
	}
}
