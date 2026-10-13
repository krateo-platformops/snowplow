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
