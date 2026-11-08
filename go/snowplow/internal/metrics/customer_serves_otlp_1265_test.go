package metrics

// customer_serves_otlp_1265_test.go — #1265 P0: the served-cold counter is
// mirrored on OTLP with the values /debug/vars reports, through the REAL
// registration and callback, moved by the dispatchers' production recorder
// (customerServe.end, via its cross-package fixture). Every cell is emitted
// pre-populated, class_warm is only "none", and the label values are the
// closed sets.

import (
	"context"
	"encoding/json"
	"expvar"
	"testing"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

func collect1265(t *testing.T) []point {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	if err := registerInstruments(mp.Meter(meterName), "deadbeef"); err != nil {
		t.Fatalf("registerInstruments: %v", err)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return flatten(rm)
}

func serves1265(pts []point) (served, cold map[string]int64) {
	served, cold = map[string]int64{}, map[string]int64{}
	for _, p := range pointsFor(pts, "snowplow_customer_serves_total") {
		served[p.attrs["handler"]+"|"+p.attrs["outcome"]+"|"+p.attrs["class_warm"]] = p.value
	}
	for _, p := range pointsFor(pts, "snowplow_customer_serves_cold_total") {
		cold[p.attrs["handler"]+"|"+p.attrs["cause"]] = p.value
	}
	return served, cold
}

func TestCustomerServes1265_MirroredOnOTLPWithTheExpvarValues(t *testing.T) {
	// Registered whatever the cache mode (with the cache off every serve is
	// cold{cache_off}): no CACHE_ENABLED here.
	served0, cold0 := serves1265(collect1265(t))
	if len(served0) != 2*4 || len(cold0) != 2*6 {
		t.Fatalf("pre-populated cells: served %d (want 8), cold %d (want 12): %v %v", len(served0), len(cold0), served0, cold0)
	}
	for k := range served0 {
		if k[len(k)-len("|none"):] != "|none" {
			t.Fatalf("class_warm other than none emitted before #1265 P3: %q", k)
		}
	}

	dispatchers.RecordCustomerServeForTest("widgets", "cold", "no_cell")
	dispatchers.RecordCustomerServeForTest("widgets", "cold", "no_cell")
	dispatchers.RecordCustomerServeForTest("restactions", "cold", "expired_ttl")
	dispatchers.RecordCustomerServeForTest("widgets", "stale", "")
	dispatchers.RecordCustomerServeForTest("restactions", "fresh", "")

	pts := collect1265(t)
	for _, p := range append(pointsFor(pts, "snowplow_customer_serves_total"), pointsFor(pts, "snowplow_customer_serves_cold_total")...) {
		for k := range p.attrs {
			switch k {
			case "handler", "outcome", "class_warm", "cause":
			default:
				t.Fatalf("%s emits attribute %q outside {handler, outcome, class_warm, cause}", p.metric, k)
			}
		}
	}
	served, cold := serves1265(pts)
	want := map[string]int64{
		"widgets|cold|none": 2, "restactions|cold|none": 1, "widgets|stale|none": 1, "restactions|fresh|none": 1,
	}
	for k, v := range served {
		if v-served0[k] != want[k] {
			t.Fatalf("snowplow_customer_serves_total{%s} moved %d, want %d", k, v-served0[k], want[k])
		}
	}
	wantCold := map[string]int64{"widgets|no_cell": 2, "restactions|expired_ttl": 1}
	for k, v := range cold {
		if v-cold0[k] != wantCold[k] {
			t.Fatalf("snowplow_customer_serves_cold_total{%s} moved %d, want %d", k, v-cold0[k], wantCold[k])
		}
	}

	// /debug/vars reads the same accessors.
	var ev map[string]map[string]map[string]uint64
	if err := json.Unmarshal([]byte(expvar.Get("snowplow_customer_serves_total").String()), &ev); err != nil {
		t.Fatalf("expvar snowplow_customer_serves_total: %v", err)
	}
	if int64(ev["widgets"]["cold"]["none"]) != served["widgets|cold|none"] {
		t.Fatalf("expvar widgets.cold.none = %d, OTLP %d", ev["widgets"]["cold"]["none"], served["widgets|cold|none"])
	}
	var evc map[string]map[string]uint64
	if err := json.Unmarshal([]byte(expvar.Get("snowplow_customer_serves_cold_total").String()), &evc); err != nil {
		t.Fatalf("expvar snowplow_customer_serves_cold_total: %v", err)
	}
	if int64(evc["restactions"]["expired_ttl"]) != cold["restactions|expired_ttl"] {
		t.Fatalf("expvar restactions.expired_ttl = %d, OTLP %d", evc["restactions"]["expired_ttl"], cold["restactions|expired_ttl"])
	}
}
