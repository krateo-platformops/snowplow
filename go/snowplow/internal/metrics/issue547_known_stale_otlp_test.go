package metrics

// issue547_known_stale_otlp_test.go — #547: the KNOWN-STALE detector leaves the
// process on OTLP. snowplow_resolved_cache_known_stale_served_total{reason} is
// hand-wired in metrics.go (there is no expvar→OTLP bridge): one monotonic
// series per reason of the CLOSED set, zeros included, read from the LIVE
// counters — distinct values per reason, so a mis-wired observation that reads
// the wrong counter cannot pass by coincidence.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue547_KnownStaleServedLeavesTheProcessOnOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	// The known-stale counters are reset with the terminal state (one composed hook).
	cache.ResetRefreshTerminalForTest()
	t.Cleanup(cache.ResetRefreshTerminalForTest)

	want := map[string]int64{}
	for i, reason := range cache.KnownStaleReasons {
		n := uint64(11 * (i + 1))
		if !cache.AddKnownStaleServedForTest(reason, n) {
			t.Fatalf("reason %q of the closed set has no test bump", reason)
		}
		want[reason] = int64(n)
	}

	all := flatten(collectViaRealCallback(t, "deadbeef"))
	got := map[string]int64{}
	for _, p := range pointsFor(all, "snowplow_resolved_cache_known_stale_served_total") {
		if len(p.attrs) != 1 {
			t.Errorf("#547: point carries attributes %v, want exactly {reason}", p.attrs)
		}
		got[p.attrs["reason"]] = p.value
	}
	if len(got) == 0 {
		t.Fatalf("#547 RED: snowplow_resolved_cache_known_stale_served_total never left the process")
	}
	for reason, w := range want {
		if got[reason] != w {
			t.Errorf("#547: known_stale_served_total{reason=%s} = %d on OTLP, want %d (the live counter)", reason, got[reason], w)
		}
	}
	if len(got) != len(cache.KnownStaleReasons) {
		t.Errorf("#547: %d reason series exported, want the closed set of %d: %v", len(got), len(cache.KnownStaleReasons), got)
	}
}
