// list_narrow_otlp_765_test.go — #603 PR-2 / #765 follow-up: the 7-day gate
// is read from OTLP across pod restarts and replicas, so every gate instrument
// must export as a CUMULATIVE, MONOTONIC Sum with a start time. A delta or a
// gauge would make "sum the last value per (pod, start time)" wrong, and a
// missing start time would make a restart's reset to zero indistinguishable
// from a counter that went down.
package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

func TestListNarrow765_GateCountersExportCumulativeMonotonic(t *testing.T) {
	rcv := newOTLPReceiver(t)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", rcv.srv.URL)
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetRefresherForTest)

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

	want := []string{
		"snowplow_deps_list_narrow_total",
		"snowplow_deps_list_narrow_audit_total",
		"snowplow_deps_list_narrow_audit_samples_total",
		"snowplow_deps_list_narrow_replay_total",
		"snowplow_deps_interest_verdict_total",
		"snowplow_deps_interest_eval_total",
	}
	seen := map[string]bool{}
	labels := map[string]bool{}
	for _, e := range rcv.snapshot() {
		for _, rm := range e.req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, mt := range sm.GetMetrics() {
					name := mt.GetName()
					found := false
					for _, w := range want {
						found = found || w == name
					}
					if !found {
						continue
					}
					seen[name] = true
					sum, ok := mt.GetData().(*metricspb.Metric_Sum)
					if !ok {
						t.Fatalf("%s exported as %T, want a Sum (a counter): the gate sums counters", name, mt.GetData())
					}
					if got := sum.Sum.GetAggregationTemporality(); got != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
						t.Fatalf("%s temporality %v, want CUMULATIVE: the gate reads the last value per series", name, got)
					}
					if !sum.Sum.GetIsMonotonic() {
						t.Fatalf("%s is not monotonic: a backend cannot tell a restart from a decrease", name)
					}
					for _, dp := range sum.Sum.GetDataPoints() {
						if dp.GetStartTimeUnixNano() == 0 {
							t.Fatalf("%s data point has no start time: a restart's new series cannot be told apart", name)
						}
						var o, r string
						for _, kv := range dp.GetAttributes() {
							switch kv.GetKey() {
							case "outcome":
								o = kv.GetValue().GetStringValue()
							case "reason":
								r = kv.GetValue().GetStringValue()
							}
						}
						labels[name+"{"+o+","+r+"}"] = true
					}
				}
			}
		}
	}
	for _, w := range want {
		if !seen[w] {
			t.Fatalf("gate instrument %s was not exported (got %v)", w, exportedNames(rcv.snapshot()))
		}
	}
	// Every closed label combination exists from boot, at zero: a series that
	// appears only on its first increment cannot be told from an unwired one,
	// and the gate's "0 would-change" must be a present zero.
	mustHave := []string{
		"snowplow_deps_list_narrow_total{avoided,unread_field}",
		"snowplow_deps_list_narrow_total{kept,read_field}",
		"snowplow_deps_list_narrow_audit_total{changed,}",
		"snowplow_deps_list_narrow_audit_total{identical,}",
		"snowplow_deps_list_narrow_audit_total{unaudited,}",
		"snowplow_deps_interest_verdict_total{narrowed,ok}",
		"snowplow_deps_interest_verdict_total{all,not_informer_served}",
		"snowplow_deps_interest_verdict_total{all,stage_error}",
	}
	for _, l := range mustHave {
		if !labels[l] {
			t.Fatalf("gate series %s is absent at boot; it must export as a present zero", l)
		}
	}
}
