package metrics

// issue740_known_stale_rerun_otlp_test.go — #740: the absorbed-read rerun
// counter leaves the process on OTLP.
// snowplow_resolved_cache_known_stale_revalidation_rerun_total{slot} is
// hand-wired in metrics.go (there is no expvar→OTLP bridge): one monotonic
// series per slot of the CLOSED set, zeros included, read from the LIVE
// counters — distinct values per slot, so a mis-wired observation that reads the
// wrong counter cannot pass by coincidence.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue740_KnownStaleRerunLeavesTheProcessOnOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	// The known-stale counters are reset with the terminal state (one composed hook).
	cache.ResetRefreshTerminalForTest()
	t.Cleanup(cache.ResetRefreshTerminalForTest)

	want := map[string]int64{}
	for i, slot := range cache.KnownStaleRevalidationSlots {
		n := uint64(7 * (i + 1))
		if !cache.AddKnownStaleRevalidationRerunForTest(slot, n) {
			t.Fatalf("slot %q of the closed set has no test bump", slot)
		}
		want[slot] = int64(n)
	}

	all := flatten(collectViaRealCallback(t, "deadbeef"))
	got := map[string]int64{}
	for _, p := range pointsFor(all, "snowplow_resolved_cache_known_stale_revalidation_rerun_total") {
		if len(p.attrs) != 1 {
			t.Errorf("#740: point carries attributes %v, want exactly {slot}", p.attrs)
		}
		got[p.attrs["slot"]] = p.value
	}
	if len(got) == 0 {
		t.Fatalf("#740 RED: snowplow_resolved_cache_known_stale_revalidation_rerun_total never left the process")
	}
	for slot, w := range want {
		if got[slot] != w {
			t.Errorf("#740: known_stale_revalidation_rerun_total{slot=%s} = %d on OTLP, want %d (the live counter)", slot, got[slot], w)
		}
	}
	if len(got) != len(cache.KnownStaleRevalidationSlots) {
		t.Errorf("#740: %d slot series exported, want the closed set of %d: %v", len(got), len(cache.KnownStaleRevalidationSlots), got)
	}
}
