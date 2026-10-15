package metrics

// metrics_710_content_fill_otlp_test.go — #710 PR-0: the apistage content-fill
// series reach OTLP from the REAL registration (registerInstruments over a
// ManualReader), and read the LIVE counters: each reason is bumped a distinct
// amount through the production atomics. The only attribute is the closed
// `reason`; the duration histogram carries the shared bounds and the count
// recorded through the production hook. RED without registerContentFillInstruments
// (every series absent).

import (
	"context"
	"reflect"
	"testing"

	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestIssue710_ContentFillSeriesReachOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	restactionsapi.ResetContentFillStatsForTest()
	t.Cleanup(restactionsapi.ResetContentFillStatsForTest)
	t.Cleanup(func() { restactionsapi.SetContentFillDurationRecorder(nil) })

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	if err := registerInstruments(mp.Meter(meterName), "deadbeef"); err != nil {
		t.Fatalf("registerInstruments: %v", err)
	}

	reasons := restactionsapi.ContentFillReasons()
	type want struct{ total, dup, bytes, joined, refused int64 }
	wants := map[string]want{}
	for i, r := range reasons {
		w := want{total: int64(10 + i), dup: int64(1 + i), bytes: int64(1000 * (i + 1)), joined: int64(100 + i), refused: int64(20 + i)}
		if !restactionsapi.AddContentFillForTest(r, uint64(w.total), uint64(w.dup), uint64(w.bytes)) {
			t.Fatalf("AddContentFillForTest(%q): not a reason of the set", r)
		}
		if !restactionsapi.AddContentFillJoinForTest(r, uint64(w.joined), uint64(w.refused), 0) {
			t.Fatalf("AddContentFillJoinForTest(%q): not a reason of the set", r)
		}
		wants[r] = w
	}
	restactionsapi.AddContentFillJoinForTest(reasons[0], 0, 0, 7)
	restactionsapi.AddContentFillInflightForTest(3, 4242)
	restactionsapi.RecordContentFillDurationForTest(7)
	restactionsapi.RecordContentFillDurationForTest(700)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	all := flatten(rm)

	enum := map[string]bool{}
	for _, r := range reasons {
		enum[r] = true
	}
	for _, c := range []struct {
		name string
		pick func(want) int64
	}{
		{"snowplow_apistage_content_fill_total", func(w want) int64 { return w.total }},
		{"snowplow_apistage_content_fill_duplicate_total", func(w want) int64 { return w.dup }},
		{"snowplow_apistage_content_fill_bytes_total", func(w want) int64 { return w.bytes }},
		{"snowplow_apistage_content_fill_joined_total", func(w want) int64 { return w.joined }},
		{"snowplow_apistage_content_fill_join_refused_total", func(w want) int64 { return w.refused }},
	} {
		pts := pointsFor(all, c.name)
		if len(pts) != len(reasons) {
			t.Fatalf("%s: %d series, want one per reason (%d)", c.name, len(pts), len(reasons))
		}
		for _, p := range pts {
			r := p.attrs["reason"]
			if !enum[r] || len(p.attrs) != 1 {
				t.Fatalf("%s: attributes %v, want exactly a closed-set reason", c.name, p.attrs)
			}
			if p.value != c.pick(wants[r]) {
				t.Fatalf("%s{reason=%s} = %d, want %d (the callback is not reading the live counter)", c.name, r, p.value, c.pick(wants[r]))
			}
		}
	}
	for name, w := range map[string]int64{
		"snowplow_apistage_content_fill_not_servable_total": 0,
		"snowplow_apistage_content_fill_inflight":           3,
		"snowplow_apistage_content_fill_inflight_bytes":     4242,
		"snowplow_apistage_content_fill_join_waiting":       7,
	} {
		pts := pointsFor(all, name)
		if len(pts) != 1 || pts[0].value != w || len(pts[0].attrs) != 0 {
			t.Fatalf("%s = %+v, want one attribute-free point of %d", name, pts, w)
		}
	}

	var hist *metricdata.HistogramDataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "snowplow_apistage_content_fill_duration_ms" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[int64])
			if !ok || len(h.DataPoints) != 1 {
				t.Fatalf("snowplow_apistage_content_fill_duration_ms: %T with the wrong point count", m.Data)
			}
			hist = &h.DataPoints[0]
		}
	}
	if hist == nil {
		t.Fatalf("snowplow_apistage_content_fill_duration_ms is absent")
	}
	wantBounds := make([]float64, 0, len(restactionsapi.ContentFillDurationBoundsMS))
	for _, b := range restactionsapi.ContentFillDurationBoundsMS {
		wantBounds = append(wantBounds, float64(b))
	}
	if !reflect.DeepEqual(hist.Bounds, wantBounds) {
		t.Fatalf("duration bounds %v, want %v", hist.Bounds, wantBounds)
	}
	if hist.Count != 2 || hist.Sum != 707 {
		t.Fatalf("duration count/sum = %d/%d, want 2/707", hist.Count, hist.Sum)
	}
	if d := restactionsapi.ContentFillStatsSnapshot().Duration; d.Count != hist.Count {
		t.Fatalf("expvar histogram count %d != OTLP %d — the surfaces diverged", d.Count, hist.Count)
	}
}
