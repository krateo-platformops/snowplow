// otlp_export_c4_red_probe_test.go — 1.12.6 C4 follow-up (arch N9), the
// RED probe that compiles on cc0b28e: the two C4 numbers observability.md
// designates as ALERTABLE — drop_evict_suspended ("a mass failure is in
// progress") and suppressed_skips (the #191 cure signal) — must reach the
// OTLP wire, because platform observability is ClickStack (OTLP-first) and
// an expvar-only counter cannot be alerted on. On cc0b28e both are
// expvar-only (metrics.go mirrored a fixed 12-value refresher tuple that
// predates C4) and this probe is RED behaviourally: the export carries
// snowplow_refresher without those stat labels. It uses no symbol added by
// the follow-up; the value arm (otlp_export_c4_test.go) is the GREEN twin.
package metrics

import (
	"context"
	"testing"
	"time"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// c4FindSumByStat locates a Sum metric by name and returns the value of the
// data point carrying attribute stat=<stat>. A counter keyed by a label has
// one data point per label value; findSum reads only the first.
func c4FindSumByStat(exports []capturedExport, name, stat string) (val int64, found bool) {
	for _, e := range exports {
		for _, rm := range e.req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, mt := range sm.GetMetrics() {
					if mt.GetName() != name {
						continue
					}
					sum, ok := mt.GetData().(*metricspb.Metric_Sum)
					if !ok {
						continue
					}
					for _, dp := range sum.Sum.GetDataPoints() {
						for _, kv := range dp.GetAttributes() {
							if kv.GetKey() == "stat" && kv.GetValue().GetStringValue() == stat {
								return dp.GetAsInt(), true
							}
						}
					}
				}
			}
		}
	}
	return 0, false
}

// c4StatLabels lists the stat label values exported on a Sum metric, so a
// failure says what DID arrive.
func c4StatLabels(exports []capturedExport, name string) []string {
	var out []string
	for _, e := range exports {
		for _, rm := range e.req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, mt := range sm.GetMetrics() {
					if mt.GetName() != name {
						continue
					}
					if sum, ok := mt.GetData().(*metricspb.Metric_Sum); ok {
						for _, dp := range sum.Sum.GetDataPoints() {
							for _, kv := range dp.GetAttributes() {
								if kv.GetKey() == "stat" {
									out = append(out, kv.GetValue().GetStringValue())
								}
							}
						}
					}
				}
			}
		}
	}
	return out
}

func TestIssue1126_N9_RedProbe_C4AlertCountersRideSnowplowRefresher(t *testing.T) {
	rcv := newOTLPReceiver(t)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", rcv.srv.URL)
	t.Setenv("CACHE_ENABLED", "true")

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
	exports := rcv.snapshot()
	if len(exports) == 0 {
		t.Fatal("NOTHING was exported: no OTLP request reached the receiver")
	}
	for _, stat := range []string{"drop_evict_suspended", "suppressed_skips"} {
		if _, found := c4FindSumByStat(exports, "snowplow_refresher", stat); !found {
			t.Errorf("RED: snowplow_refresher{stat=%q} is absent from the OTLP export — the C4 alert counter is "+
				"expvar-only and cannot be alerted on from ClickStack (arch N9). Exported stat labels: %v",
				stat, c4StatLabels(exports, "snowplow_refresher"))
		}
	}
}
