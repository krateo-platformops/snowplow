package metrics

// metrics_681_denial_only_otlp_test.go — #681 H9 / #682: the denial-only Put
// counter, the RBAC sub-gen epoch and #677's pre-dial counter reach OTLP
// (hand-wired in metrics.go), every closed-set label pre-populated, each series
// moving with its production counter. The four #677
// snowplow_refresher_regate_declined_* series are GONE.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func TestIssue681_DenialOnlyPutReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetDenialOnlyPutForTest()
	t.Cleanup(cache.ResetDenialOnlyPutForTest)
	by := func() map[string]int64 {
		out := map[string]int64{}
		for _, p := range pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_resolved_cache_denial_only_put_total") {
			out[p.attrs["site"]] = p.value
		}
		return out
	}
	before := by()
	if len(before) != len(cache.DenialOnlyPutSites) {
		t.Fatalf("%d site series, want the closed set of %d (metrics.go wiring missing?)", len(before), len(cache.DenialOnlyPutSites))
	}
	cache.RecordDenialOnlyPut(cache.DenialOnlyPutSiteSeed)
	cache.RecordDenialOnlyPut("not-a-site")
	after := by()
	if after[cache.DenialOnlyPutSiteSeed]-before[cache.DenialOnlyPutSiteSeed] != 1 {
		t.Fatal("denial_only_put_total{seed} did not move by 1")
	}
	if _, ok := after["not-a-site"]; ok {
		t.Fatal("a site outside the closed set became a series")
	}
}

func TestIssue682_EpochAndPreDialReachOTLP_MarkSeriesGone(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	pts := flatten(collectViaRealCallback(t, "deadbeef"))
	if n := len(pointsFor(pts, "snowplow_rbac_subgen_epoch")); n != 1 {
		t.Fatalf("snowplow_rbac_subgen_epoch: %d series, want 1", n)
	}
	if n := len(pointsFor(pts, "snowplow_internal_dispatch_predial_total")); n != len(restactionsapi.PreDialOutcomes) {
		t.Fatalf("snowplow_internal_dispatch_predial_total: %d series, want %d", n, len(restactionsapi.PreDialOutcomes))
	}
	for _, gone := range []string{
		"snowplow_refresher_regate_declined_marks", "snowplow_refresher_regate_declined_set_total",
		"snowplow_refresher_regate_declined_skip_total", "snowplow_refresher_regate_declined_retired_total",
	} {
		if n := len(pointsFor(pts, gone)); n != 0 {
			t.Fatalf("%s still exported (%d series) — #681 removed the mark", gone, n)
		}
	}
}
