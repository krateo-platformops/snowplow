package metrics

// issue778_uaf_null_skip_otlp_test.go — #778: the userAccessFilter null-skip
// family reaches OTLP (hand-wired in metrics.go) through the REAL callback:
// the total moves once per skipping evaluation, the gauge counts the stages
// skipping NOW (two stages → 2; one reconciles → 1), and neither series carries
// an attribute (the by-stage map, which names RESTActions, stays expvar-only).

import (
	"testing"

	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func TestIssue778_NullSkipReachesOTLPWithoutAttributes(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	restactionsapi.ResetUAFNullSkippedForTest()
	t.Cleanup(restactionsapi.ResetUAFNullSkippedForTest)

	read := func() (total, skipping int64) {
		pts := flatten(collectViaRealCallback(t, "deadbeef"))
		for name, dst := range map[string]*int64{
			"snowplow_uaf_resources_null_skipped_total":   &total,
			"snowplow_uaf_resources_null_skipping_stages": &skipping,
		} {
			ps := pointsFor(pts, name)
			if len(ps) != 1 {
				t.Fatalf("#778: %s: %d series, want exactly 1 (metrics.go wiring missing?)", name, len(ps))
			}
			if len(ps[0].attrs) != 0 {
				t.Fatalf("#778: %s carries attributes %v — the by-stage RESTAction names must stay expvar-only", name, ps[0].attrs)
			}
			*dst = ps[0].value
		}
		return total, skipping
	}

	if total, skipping := read(); total != 0 || skipping != 0 {
		t.Fatalf("#778 PRE: total=%d skipping=%d after reset, want 0 and 0", total, skipping)
	}
	restactionsapi.RecordUAFResourcesEvaluatedForTest("krateo-system", "blueprint-formdef", "namespaces", true)
	restactionsapi.RecordUAFResourcesEvaluatedForTest("krateo-system", "blueprint-formdef", "namespaces", true)
	restactionsapi.RecordUAFResourcesEvaluatedForTest("krateo-system", "other-ra", "picker", true)
	total, skipping := read()
	if total != 3 {
		t.Fatalf("#778 RED: snowplow_uaf_resources_null_skipped_total = %d after 3 skipping evaluations, want 3", total)
	}
	if skipping != 2 {
		t.Fatalf("#778 RED: snowplow_uaf_resources_null_skipping_stages = %d with 2 stages skipping, want 2", skipping)
	}

	// One stage's input reconciles: it stops skipping, the total does not move.
	restactionsapi.RecordUAFResourcesEvaluatedForTest("krateo-system", "other-ra", "picker", false)
	total, skipping = read()
	if total != 3 || skipping != 1 {
		t.Fatalf("#778 RED: after one stage resolved, total=%d skipping=%d, want 3 and 1 — the gauge must be CURRENT state", total, skipping)
	}
}
