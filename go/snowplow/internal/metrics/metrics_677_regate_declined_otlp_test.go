package metrics

// metrics_677_regate_declined_otlp_test.go — #677: the refresher's
// known-declined detectors and branch C's pre-dial outcome counter reach OTLP
// (hand-wired in metrics.go), every closed-set label pre-populated, and each
// series moves with its production counter. Observed via the REAL callback.
// RED without the wiring: no series.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func TestIssue677_RegateDeclinedReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetRegateDeclinedForTest()
	t.Cleanup(cache.ResetRegateDeclinedForTest)

	retiredBy := func() map[string]int64 {
		out := map[string]int64{}
		for _, p := range pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_refresher_regate_declined_retired_total") {
			out[p.attrs["reason"]] = p.value
		}
		return out
	}
	before := retiredBy()
	if len(before) != len(cache.RegateDeclinedRetiredReasons) {
		t.Fatalf("%d retired_total series, want the closed set of %d (metrics.go wiring missing?)", len(before), len(cache.RegateDeclinedRetiredReasons))
	}
	for _, r := range cache.RegateDeclinedRetiredReasons {
		if _, ok := before[r]; !ok {
			t.Fatalf("no retired_total{reason=%s} series — every closed cell must be pre-populated", r)
		}
	}

	m := &cache.RegateDeclinedMark{RBACPublishSeq: 1}
	cache.MarkRegateDeclined("otlp677|a", m)
	cache.MarkRegateDeclined("otlp677|b", m)
	cache.NoteRegateDeclinedSkip()
	cache.RetireRegateDeclined("otlp677|b", cache.RegateDeclinedRetiredRBACPublished)

	pts := flatten(collectViaRealCallback(t, "deadbeef"))
	one := func(name string) int64 {
		ps := pointsFor(pts, name)
		if len(ps) != 1 {
			t.Fatalf("%s: %d series, want 1 — metrics.go wiring missing", name, len(ps))
		}
		return ps[0].value
	}
	if v := one("snowplow_refresher_regate_declined_marks"); v != 1 {
		t.Fatalf("marks = %d, want 1", v)
	}
	if v := one("snowplow_refresher_regate_declined_set_total"); v != 2 {
		t.Fatalf("set_total = %d, want 2", v)
	}
	if v := one("snowplow_refresher_regate_declined_skip_total"); v != 1 {
		t.Fatalf("skip_total = %d, want 1", v)
	}
	if got := retiredBy()[cache.RegateDeclinedRetiredRBACPublished] - before[cache.RegateDeclinedRetiredRBACPublished]; got != 1 {
		t.Fatalf("retired_total{rbac_published} moved by %d, want 1", got)
	}
}

func TestIssue677_PreDialReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	by := func() map[string]int64 {
		out := map[string]int64{}
		for _, p := range pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_internal_dispatch_predial_total") {
			out[p.attrs["outcome"]] = p.value
		}
		return out
	}
	before := by()
	if len(before) != len(restactionsapi.PreDialOutcomes) {
		t.Fatalf("%d predial series, want the closed set of %d (metrics.go wiring missing?)", len(before), len(restactionsapi.PreDialOutcomes))
	}
	restactionsapi.RecordPreDialForTest("denied")
	restactionsapi.RecordPreDialForTest("not-an-outcome")
	after := by()
	if after["denied"]-before["denied"] != 1 {
		t.Fatalf("predial_total{denied} moved by %d, want 1", after["denied"]-before["denied"])
	}
	if _, ok := after["not-an-outcome"]; ok {
		t.Fatal("an outcome outside the closed set became a series")
	}
}
