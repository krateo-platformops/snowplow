// content_fill_expansion_787_test.go — #787 step 1: the learned decode
// expansion reaches OTLP through the REAL Setup, with the closed labels, and the
// sample counter is a cumulative monotonic Sum (the gate's reading sums it per
// (pod, start time) across restarts).
package metrics

import (
	"context"
	"testing"
	"time"

	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestContentFill787_ExpansionExportsOnOTLP(t *testing.T) {
	rcv := newOTLPReceiver(t)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", rcv.srv.URL)
	t.Setenv("CACHE_ENABLED", "true")
	restactionsapi.ResetContentFillGateForTest()
	t.Cleanup(restactionsapi.ResetContentFillGateForTest)

	const gvr = "g787/v1, Resource=things"
	restactionsapi.CalibrateContentFillExpansionForTest(gvr, 14) // seeded
	restactionsapi.CalibrateContentFillExpansionForTest(gvr, 4)  // decayed → 13.5

	ctx := context.Background()
	shutdown, err := Setup(ctx, "deadbeef")
	if err != nil {
		t.Fatalf("metrics.Setup: %v", err)
	}
	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := shutdown(flushCtx); err != nil {
		t.Fatalf("flush/shutdown: %v", err)
	}

	gauges := map[string]float64{} // name{label} -> value
	var samplesSum *metricspb.Sum
	for _, e := range rcv.snapshot() {
		for _, rm := range e.req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, mt := range sm.GetMetrics() {
					switch mt.GetName() {
					case "snowplow_apistage_content_fill_decode_expansion", "snowplow_apistage_content_fill_decode_expansion_summary":
						g, ok := mt.GetData().(*metricspb.Metric_Gauge)
						if !ok {
							t.Fatalf("%s exported as %T, want a Gauge", mt.GetName(), mt.GetData())
						}
						for _, dp := range g.Gauge.GetDataPoints() {
							label := ""
							for _, kv := range dp.GetAttributes() {
								label += kv.GetKey() + "=" + kv.GetValue().GetStringValue()
							}
							gauges[mt.GetName()+"{"+label+"}"] = dp.GetAsDouble()
						}
					case "snowplow_apistage_content_fill_calibration_samples_total":
						s, ok := mt.GetData().(*metricspb.Metric_Sum)
						if !ok {
							t.Fatalf("calibration_samples exported as %T, want a Sum", mt.GetData())
						}
						samplesSum = s.Sum
					}
				}
			}
		}
	}
	want := map[string]float64{
		"snowplow_apistage_content_fill_decode_expansion{gvr=" + gvr + "}":       13.5,
		"snowplow_apistage_content_fill_decode_expansion_summary{stat=min}":      13.5,
		"snowplow_apistage_content_fill_decode_expansion_summary{stat=max}":      13.5,
		"snowplow_apistage_content_fill_decode_expansion_summary{stat=weighted}": 13.5,
	}
	for k, v := range want {
		if got, ok := gauges[k]; !ok || got != v {
			t.Fatalf("#787 RED: %s = %v (present=%v), want %v; exported gauges: %v", k, got, ok, v, gauges)
		}
	}
	if samplesSum == nil {
		t.Fatalf("#787 RED: calibration_samples_total was not exported")
	}
	if samplesSum.GetAggregationTemporality() != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE || !samplesSum.GetIsMonotonic() {
		t.Fatalf("calibration_samples_total must be a cumulative monotonic Sum")
	}
	byOutcome := map[string]int64{}
	for _, dp := range samplesSum.GetDataPoints() {
		for _, kv := range dp.GetAttributes() {
			if kv.GetKey() == "outcome" {
				byOutcome[kv.GetValue().GetStringValue()] = dp.GetAsInt()
			}
		}
	}
	if byOutcome["seeded"] != 1 || byOutcome["decayed"] != 1 || len(byOutcome) != 4 {
		t.Fatalf("#787 RED: samples by outcome %v, want seeded=1 decayed=1 and all four outcomes present", byOutcome)
	}
}
