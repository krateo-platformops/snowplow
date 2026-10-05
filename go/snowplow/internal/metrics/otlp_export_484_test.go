// otlp_export_484_test.go — #484: the three live-refresh repair counters
// reach OTLP under the names an operator queries in ClickStack.
//
// TestC7_OTLP_EveryDerivedStatLeavesTheProcess already proves every tagged
// RefreshBroadcasterStats field leaves the process (the set is asked of the
// type). This arm pins the NAMES literally, so a renamed tag (which C7 would
// follow silently) breaks a dashboard query here first.
package metrics

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestOTLP484_RefreshRepairCountersLeaveTheProcess(t *testing.T) {
	rcv := newOTLPReceiver(t)
	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", rcv.srv.URL)
	t.Setenv("CACHE_ENABLED", "true")

	cache.SetRefreshBroadcasterStatsForTest(&cache.RefreshBroadcasterStats{
		TrailingEmitted:       4841,
		PendingCoalesced:      4842,
		PendingOverflowResync: 4843,
		Deferred:              4844,
	})
	t.Cleanup(func() { cache.SetRefreshBroadcasterStatsForTest(nil) })

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
	for name, want := range map[string]float64{
		"snowplow_refresh_broadcaster_trailing_emitted_total":        4841,
		"snowplow_refresh_broadcaster_pending_coalesced_total":       4842,
		"snowplow_refresh_broadcaster_pending_overflow_resync_total": 4843,
		"snowplow_refresh_broadcaster_deferred_total":                4844,
	} {
		got, found := c7Find(exports, name, "")
		if !found {
			t.Errorf("%s never left the process (exported: %v)", name, exportedNames(exports))
			continue
		}
		if got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
}
