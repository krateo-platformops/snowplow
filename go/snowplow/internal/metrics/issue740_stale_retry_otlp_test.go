package metrics

// issue740_stale_retry_otlp_test.go — #740: the viewer-gated retry family
// leaves the process on OTLP. snowplow_stale_retry_total{kind,source} and
// snowplow_stale_retry_abandoned_total{reason} are hand-wired in metrics.go
// (there is no expvar→OTLP bridge): one series per cell of the CLOSED sets,
// zeros included, read from the LIVE counters — distinct values per cell, so a
// mis-wired observation that reads the wrong counter cannot pass by coincidence.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue740_StaleRetryLeavesTheProcessOnOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetStaleRetryForTest()
	t.Cleanup(cache.ResetStaleRetryForTest)

	want := map[[2]string]int64{}
	n := uint64(3)
	for k, kind := range cache.StaleRetryKinds {
		for src, source := range cache.StaleRetrySources {
			cache.AddStaleRetryForTest(cache.StaleRetryOutcome(k+1), cache.StaleRetrySource(src), n, 0, 0, 0)
			want[[2]string{kind, source}] = int64(n)
			n += 2
		}
	}
	cache.AddStaleRetryForTest(0, 0, 0, 11, 13, 17)

	all := flatten(collectViaRealCallback(t, "deadbeef"))
	got := map[[2]string]int64{}
	for _, p := range pointsFor(all, "snowplow_stale_retry_total") {
		if len(p.attrs) != 2 {
			t.Errorf("#740: point carries attributes %v, want exactly {kind, source}", p.attrs)
		}
		got[[2]string{p.attrs["kind"], p.attrs["source"]}] = p.value
	}
	if len(got) == 0 {
		t.Fatalf("#740 RED: snowplow_stale_retry_total never left the process")
	}
	for cell, w := range want {
		if got[cell] != w {
			t.Errorf("#740: stale_retry_total%v = %d on OTLP, want %d (the live counter)", cell, got[cell], w)
		}
	}
	if len(got) != len(want) {
		t.Errorf("#740: %d (kind, source) series exported, want the closed %d: %v", len(got), len(want), got)
	}
	ab := map[string]int64{}
	for _, p := range pointsFor(all, "snowplow_stale_retry_abandoned_total") {
		ab[p.attrs["reason"]] = p.value
	}
	if ab["no_viewer"] != 11 || ab["evicted"] != 13 || len(ab) != len(cache.StaleRetryAbandonedReasons) {
		t.Errorf("#740: stale_retry_abandoned_total = %v, want {no_viewer:11, evicted:13}", ab)
	}
	if pts := pointsFor(all, "snowplow_stale_retry_resumed_total"); len(pts) != 1 || pts[0].value != 17 {
		t.Errorf("#740: stale_retry_resumed_total = %v, want one point of 17", pts)
	}
	for _, name := range []string{"snowplow_stale_retry_chains_active", "snowplow_stale_retry_viewers", "snowplow_stale_retry_backoff_ms"} {
		if pts := pointsFor(all, name); len(pts) != 1 {
			t.Errorf("#740: %s exported %d points, want 1", name, len(pts))
		}
	}
}
