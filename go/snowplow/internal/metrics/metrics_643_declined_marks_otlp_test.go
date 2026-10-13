package metrics

// metrics_643_declined_marks_otlp_test.go — #643 PR-B: the engine-wide
// declined-external set's size reaches OTLP as
// snowplow_prewarm_declined_external_marks (hand-wired in metrics.go). Driven
// through the real set and observed via the REAL callback. RED without the
// wiring: no series.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue643_DeclinedExternalMarksGaugeReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetEngineDeclinedExternalSetForTest()
	t.Cleanup(cache.ResetEngineDeclinedExternalSetForTest)
	set := cache.EngineDeclinedExternalSet()
	set.Mark("otlp643|a")
	set.Mark("otlp643|b")
	set.Mark("otlp643|c")
	set.Unmark("otlp643|c")

	pts := pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_prewarm_declined_external_marks")
	if len(pts) != 1 {
		t.Fatalf("snowplow_prewarm_declined_external_marks: %d series, want 1 — metrics.go wiring missing", len(pts))
	}
	if pts[0].value != 2 {
		t.Fatalf("snowplow_prewarm_declined_external_marks = %d, want 2 (3 marked, 1 retired)", pts[0].value)
	}
}

// TestIssue643_DeclinedExternalRetiredReachesOTLP — the retire-reason breakdown
// reaches OTLP with every closed-set cell pre-populated, and a retire moves its
// own cell. Driven through the real cache record path; RED without the wiring.
func TestIssue643_DeclinedExternalRetiredReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetDeclinedExternalRetiredForTest()
	t.Cleanup(cache.ResetDeclinedExternalRetiredForTest)
	find := func() map[string]int64 {
		out := map[string]int64{}
		for _, p := range pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_prewarm_declined_external_retired_total") {
			out[p.attrs["reason"]] = p.value
		}
		return out
	}
	before := find()
	for _, r := range cache.DeclinedExternalRetiredReasons {
		if _, ok := before[r]; !ok {
			t.Fatalf("no snowplow_prewarm_declined_external_retired_total{reason=%s} series — every closed cell must be pre-populated (metrics.go wiring missing?)", r)
		}
	}
	if len(before) != len(cache.DeclinedExternalRetiredReasons) {
		t.Fatalf("%d reason series, want exactly the closed set of %d", len(before), len(cache.DeclinedExternalRetiredReasons))
	}
	cache.RecordDeclinedExternalRetired(cache.DeclinedExternalRetiredRAUnreadable, 3)
	cache.RecordDeclinedExternalRetired("not-a-reason", 5) // dropped: the set cannot grow from data
	after := find()
	if got := after[cache.DeclinedExternalRetiredRAUnreadable] - before[cache.DeclinedExternalRetiredRAUnreadable]; got != 3 {
		t.Fatalf("retired_total{ra_unreadable} moved by %d, want 3", got)
	}
	if _, ok := after["not-a-reason"]; ok {
		t.Fatal("a reason outside the closed set became a series")
	}
}
