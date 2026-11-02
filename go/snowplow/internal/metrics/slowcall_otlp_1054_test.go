package metrics

// slowcall_otlp_1054_test.go — #1054: the slow-call counter and phase
// histogram are mirrored on OTLP with the values expvar reports, through the
// REAL registration and callback, moved by slowcall's production emit path.

import (
	"context"
	"encoding/json"
	"expvar"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/krateo-platformops/snowplow/internal/slowcall"
)

func TestS1054_SlowCallsAreMirroredOnOTLP(t *testing.T) {
	slowcall.ResetDefaultStatsForTest()
	t.Cleanup(slowcall.ResetDefaultStatsForTest)
	t.Cleanup(func() { slowcall.SetPhaseRecorder(nil) })
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	if err := registerInstruments(mp.Meter(meterName), "deadbeef"); err != nil {
		t.Fatalf("registerInstruments: %v", err)
	}
	slowcall.EmitSlowForTest("GET /call", slowcall.PhaseFlightWait, 4*time.Second)
	slowcall.EmitSlowForTest("GET /call", slowcall.PhaseFlightWait, 27*time.Second)
	slowcall.EmitSlowForTest("POST /call/read", slowcall.PhaseStageDial, 3500*time.Millisecond)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	all := flatten(rm)
	got := map[string]int64{}
	for _, p := range pointsFor(all, "snowplow_slow_requests_total") {
		if len(p.attrs) != 2 || p.attrs["class"] != "customer" {
			t.Fatalf("snowplow_slow_requests_total attributes %v, want exactly route and class=customer", p.attrs)
		}
		got[p.attrs["route"]] = p.value
	}
	if got["GET /call"] != 2 || got["POST /call/read"] != 1 || len(got) != 2 {
		t.Fatalf("snowplow_slow_requests_total = %v, want {GET /call: 2, POST /call/read: 1}", got)
	}
	// The expvar mirror reads the same accessor.
	var ev map[string]map[string]uint64
	if err := json.Unmarshal([]byte(expvar.Get("snowplow_slow_requests_total").String()), &ev); err != nil {
		t.Fatalf("expvar: %v", err)
	}
	if ev["GET /call"]["customer"] != 2 || ev["POST /call/read"]["customer"] != 1 {
		t.Fatalf("expvar snowplow_slow_requests_total = %v", ev)
	}
	for _, name := range []string{"snowplow_slow_request_snapshots_total", "snowplow_slow_request_snapshots_suppressed_total"} {
		if pts := pointsFor(all, name); len(pts) != 1 || pts[0].value != 0 {
			t.Fatalf("%s = %+v, want one point of 0", name, pts)
		}
	}

	counts := map[string]uint64{}
	maxMS := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "snowplow_slow_request_phase_ms" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 histogram", m.Name, m.Data)
			}
			for _, dp := range h.DataPoints {
				p, _ := dp.Attributes.Value("phase")
				counts[p.Emit()] = dp.Count
				if mx, ok := dp.Max.Value(); ok {
					maxMS[p.Emit()] = mx
				}
			}
		}
	}
	if counts["flight_wait"] != 2 || counts["stage_dial"] != 1 || len(counts) != 2 {
		t.Fatalf("snowplow_slow_request_phase_ms counts = %v, want {flight_wait: 2, stage_dial: 1}", counts)
	}
	if maxMS["flight_wait"] != 27000 {
		t.Fatalf("flight_wait max = %d ms, want 27000", maxMS["flight_wait"])
	}
}
