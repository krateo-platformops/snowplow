// otlp_export_c4_test.go — 1.12.6 C4 follow-up (arch N9), the value arm:
// every C4 terminal-semantics counter is mirrored to OTLP and the mirror
// reads the LIVE counters. Four monotonic counters ride snowplow_refresher
// under their own stat label (the expvar names minus the prefix/suffix);
// the live count of suppressed keys is a gauge of its own,
// snowplow_refresher_suppressed_keys, because it drops on Put and eviction
// and cannot be a cumulative Sum. Distinct, non-equal values so a mis-wired
// observation that reads the wrong counter cannot pass by coincidence.
package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// c4FindGauge locates a Gauge metric by name and returns its first data
// point's int value.
func c4FindGauge(exports []capturedExport, name string) (val int64, found bool) {
	for _, e := range exports {
		for _, rm := range e.req.GetResourceMetrics() {
			for _, sm := range rm.GetScopeMetrics() {
				for _, mt := range sm.GetMetrics() {
					if mt.GetName() != name {
						continue
					}
					g, ok := mt.GetData().(*metricspb.Metric_Gauge)
					if !ok || len(g.Gauge.GetDataPoints()) == 0 {
						continue
					}
					return g.Gauge.GetDataPoints()[0].GetAsInt(), true
				}
			}
		}
	}
	return 0, false
}

func TestIssue1126_N9_AllFiveC4RefresherCountersExport(t *testing.T) {
	rcv := newOTLPReceiver(t)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", rcv.srv.URL)
	t.Setenv("CACHE_ENABLED", "true")

	cache.ResetRefreshTerminalForTest()
	t.Cleanup(cache.ResetRefreshTerminalForTest)
	// drop_evict=1 suspended=2 suppressed_set=3 suppressed_skips=4 keys=5
	cache.AddRefreshTerminalCountersForTest(1, 2, 3, 4, 5)
	if ts := cache.RefreshTerminalStatsSnapshot(); ts.DropEvictTotal != 1 || ts.DropEvictSuspendedTotal != 2 ||
		ts.SuppressedSetTotal != 3 || ts.SuppressedSkipsTotal != 4 || ts.SuppressedKeys != 5 {
		t.Fatalf("setup: the counters themselves did not move (%+v) — the arm would test nothing", ts)
	}

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

	for stat, want := range map[string]int64{
		"drop_evict":           1,
		"drop_evict_suspended": 2,
		"suppressed_set":       3,
		"suppressed_skips":     4,
	} {
		got, found := c4FindSumByStat(exports, "snowplow_refresher", stat)
		if !found {
			t.Errorf("snowplow_refresher{stat=%q} absent from the export (labels seen: %v)", stat,
				c4StatLabels(exports, "snowplow_refresher"))
			continue
		}
		if got != want {
			t.Errorf("snowplow_refresher{stat=%q} = %d; want %d — the observable callback is not reading "+
				"the live C4 counter", stat, got, want)
		}
	}
	got, found := c4FindGauge(exports, "snowplow_refresher_suppressed_keys")
	if !found {
		t.Fatalf("snowplow_refresher_suppressed_keys gauge absent from the export. Exported: %v", exportedNames(exports))
	}
	if got != 5 {
		t.Errorf("snowplow_refresher_suppressed_keys = %d; want 5", got)
	}
}
