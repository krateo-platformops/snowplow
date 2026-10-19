package metrics

// issue778_uaf_null_skip_otlp_test.go — #778: the userAccessFilter null-skip
// counter reaches OTLP (hand-wired in metrics.go) through the REAL callback:
// the total and the distinct-stage gauge move with the production recorder,
// and neither series carries an attribute (the by-stage map, which names
// RESTActions, stays expvar-only).

import (
	"testing"

	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func TestIssue778_NullSkipReachesOTLPWithoutAttributes(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	restactionsapi.ResetUAFNullSkippedForTest()
	t.Cleanup(restactionsapi.ResetUAFNullSkippedForTest)

	read := func() (total, stages int64) {
		pts := flatten(collectViaRealCallback(t, "deadbeef"))
		for name, dst := range map[string]*int64{
			"snowplow_uaf_resources_null_skipped_total":  &total,
			"snowplow_uaf_resources_null_skipped_stages": &stages,
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
		return total, stages
	}

	if total, stages := read(); total != 0 || stages != 0 {
		t.Fatalf("#778 PRE: total=%d stages=%d after reset, want 0 and 0", total, stages)
	}
	restactionsapi.RecordUAFNullSkippedForTest("krateo-system", "blueprint-formdef", "namespaces")
	restactionsapi.RecordUAFNullSkippedForTest("krateo-system", "blueprint-formdef", "namespaces")
	restactionsapi.RecordUAFNullSkippedForTest("krateo-system", "other-ra", "picker")
	total, stages := read()
	if total != 3 {
		t.Fatalf("#778 RED: snowplow_uaf_resources_null_skipped_total = %d after 3 skips, want 3", total)
	}
	if stages != 2 {
		t.Fatalf("#778 RED: snowplow_uaf_resources_null_skipped_stages = %d after skips in 2 stages, want 2", stages)
	}
}
