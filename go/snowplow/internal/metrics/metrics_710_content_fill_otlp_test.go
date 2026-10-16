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

// TestIssue710_StaleServeSeriesReachOTLP — #710 PR-2b: the D2 family reaches
// OTLP from the real registration with live values, and stale_to_push_ms has
// bucket bounds at exactly 1000, 3000 and 10000 ms (the owner bar is p95 ≤ 3 s,
// max ≤ 10 s, target 1 s: decidable from the histogram).
func TestIssue710_StaleServeSeriesReachOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	restactionsapi.ResetStaleServeForTest()
	t.Cleanup(restactionsapi.ResetStaleServeForTest)
	t.Cleanup(func() { restactionsapi.SetStaleToPushRecorder(nil) })

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	if err := registerInstruments(mp.Meter(meterName), "deadbeef"); err != nil {
		t.Fatalf("registerInstruments: %v", err)
	}
	restactionsapi.AddStaleServeForTest(4, 1, 2500)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	all := flatten(rm)
	pts := pointsFor(all, "snowplow_apistage_stale_serve_total")
	if len(pts) != 1 || pts[0].value != 4 || pts[0].attrs["reason"] != "known_stale" || len(pts[0].attrs) != 1 {
		t.Fatalf("stale_serve_total = %+v, want one {reason=known_stale} point of 4", pts)
	}
	if pts := pointsFor(all, "snowplow_apistage_stale_overdue_total"); len(pts) != 1 || pts[0].value != 1 {
		t.Fatalf("stale_overdue_total = %+v, want 1", pts)
	}
	for _, name := range []string{"snowplow_apistage_stale_debt_open", "snowplow_apistage_stale_debt_oldest_age_ms", "snowplow_apistage_stale_debt_retries_total",
		"snowplow_apistage_stale_push_total", "snowplow_apistage_stale_push_unarmed_total", "snowplow_apistage_stale_dependent_unresolved_total",
		"snowplow_apistage_stale_owed_open", "snowplow_apistage_stale_owed_dropped_total", "snowplow_apistage_stale_owed_arm_emits_total"} {
		if pts := pointsFor(all, name); len(pts) != 1 {
			t.Fatalf("%s: %d points, want 1", name, len(pts))
		}
	}
	if pts := pointsFor(all, "snowplow_apistage_stale_serve_withdrawn_total"); len(pts) != 1 || pts[0].attrs["reason"] != "unarmable" || len(pts[0].attrs) != 1 {
		t.Fatalf("stale_serve_withdrawn_total = %+v, want one {reason=unarmable} point", pts)
	}
	var hist *metricdata.HistogramDataPoint[int64]
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if h, ok := m.Data.(metricdata.Histogram[int64]); ok && m.Name == "snowplow_apistage_stale_to_push_ms" && len(h.DataPoints) == 1 {
				hist = &h.DataPoints[0]
			}
		}
	}
	if hist == nil || hist.Count != 1 || hist.Sum != 2500 {
		t.Fatalf("stale_to_push_ms histogram = %+v, want one sample of 2500", hist)
	}
	have := map[float64]bool{}
	for _, b := range hist.Bounds {
		have[b] = true
	}
	for _, b := range []float64{1000, 3000, 10000} {
		if !have[b] {
			t.Fatalf("stale_to_push_ms bounds %v lack %v — the owner bar would not be decidable from the histogram", hist.Bounds, b)
		}
	}
}

// TestIssue710_ContentFillAdmissionSeriesReachOTLP — #710 PR-2a: the admission
// gate's series reach OTLP from the real registration with live values; the
// only attribute is the closed `class`.
func TestIssue710_ContentFillAdmissionSeriesReachOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	restactionsapi.ResetContentFillStatsForTest()
	t.Cleanup(restactionsapi.ResetContentFillStatsForTest)
	t.Cleanup(func() { restactionsapi.SetContentFillAdmissionWaitRecorder(nil) })

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	if err := registerInstruments(mp.Meter(meterName), "deadbeef"); err != nil {
		t.Fatalf("registerInstruments: %v", err)
	}
	classes := restactionsapi.ContentFillAdmissionClasses()
	for i, c := range classes {
		if !restactionsapi.AddContentFillAdmissionForTest(c, int64(3+i), uint64(5+i), 0) {
			t.Fatalf("AddContentFillAdmissionForTest(%q): not a class of the set", c)
		}
	}
	restactionsapi.AddContentFillAdmissionForTest(classes[0], 0, 0, 9999)
	restactionsapi.RecordContentFillAdmissionWaitForTest(classes[1], 42)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	all := flatten(rm)
	for name, base := range map[string]int64{
		"snowplow_apistage_content_fill_admission_queued":        3,
		"snowplow_apistage_content_fill_admission_timeout_total": 5,
	} {
		pts := pointsFor(all, name)
		if len(pts) != len(classes) {
			t.Fatalf("%s: %d series, want one per class (%d)", name, len(pts), len(classes))
		}
		for _, p := range pts {
			idx := -1
			for i, c := range classes {
				if p.attrs["class"] == c {
					idx = i
				}
			}
			if idx < 0 || len(p.attrs) != 1 {
				t.Fatalf("%s: attributes %v, want exactly a closed-set class", name, p.attrs)
			}
			if p.value != base+int64(idx) {
				t.Fatalf("%s{class=%s} = %d, want %d", name, classes[idx], p.value, base+int64(idx))
			}
		}
	}
	if pts := pointsFor(all, "snowplow_apistage_content_fill_admission_inflight_weight"); len(pts) != 1 || pts[0].value != 9999 {
		t.Fatalf("admission_inflight_weight = %+v, want one point of 9999", pts)
	}
	for _, name := range []string{"snowplow_apistage_content_fill_admission_over_ceiling_bytes", "snowplow_apistage_content_fill_admission_over_ceiling_bytes_peak"} {
		if pts := pointsFor(all, name); len(pts) != 1 || len(pts[0].attrs) != 0 {
			t.Fatalf("%s = %+v, want one attribute-free point", name, pts)
		}
	}
	var count uint64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if h, ok := m.Data.(metricdata.Histogram[int64]); ok && m.Name == "snowplow_apistage_content_fill_admission_wait_ms" && len(h.DataPoints) == 1 {
				count = h.DataPoints[0].Count
			}
		}
	}
	if count != 1 {
		t.Fatalf("admission_wait_ms histogram count = %d, want 1", count)
	}
}
