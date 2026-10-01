package metrics

// metrics_311_malformed_dial_otlp_test.go — #311: the malformed-dial skip
// DETECTOR reaches OTLP per bounded reason (registered in registerInstruments +
// observed via the REAL callback over a ManualReader, collectViaRealCallback).
// A non-zero (any reason) is an upstream render defect the guard caught
// (#288/#293/#302); buried in /debug/vars it is observable-if-you-look, not
// alertable. RED without the #311 wiring (0 series). Also guards the CARDINALITY
// bound: every emitted reason is in the fixed code-defined enum (the #260
// bounded-attribute discipline — a request-derived reason would blow up OTLP
// series cardinality).

import (
	"testing"

	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func TestIssue311_MalformedDialSkippedReachOTLP(t *testing.T) {
	restactionsapi.ResetMalformedDialSkippedForTest()
	t.Cleanup(restactionsapi.ResetMalformedDialSkippedForTest)

	// Bump each reason a DISTINCT count so the assertion proves the callback
	// observes the ACTUAL per-reason value, not merely that the series exists.
	enum := restactionsapi.MalformedDialReasonEnum()
	want := map[string]int64{}
	for i, r := range enum {
		for j := 0; j <= i; j++ {
			restactionsapi.RecordMalformedDialSkipForTest(r)
		}
		want[r] = int64(i + 1)
	}

	all := flatten(collectViaRealCallback(t, "deadbeef"))
	pts := pointsFor(all, "snowplow_malformed_dial_skipped_total")

	if len(pts) != len(enum) {
		t.Fatalf("#311: snowplow_malformed_dial_skipped_total must reach OTLP as %d {reason} series "+
			"(0 = unregistered / expvar-only = the #311 alertability failure); got %d", len(enum), len(pts))
	}

	enumSet := map[string]bool{}
	for _, r := range enum {
		enumSet[r] = true
	}
	got := map[string]int64{}
	for _, p := range pts {
		reason, ok := p.attrs["reason"]
		if !ok {
			t.Fatalf("#311: a malformed-dial OTLP point carries no {reason} attribute: %+v", p)
		}
		// CARDINALITY BOUND: every emitted reason MUST be in the fixed enum.
		if !enumSet[reason] {
			t.Fatalf("#311 cardinality bound VIOLATED: reason %q is not in the code-defined enum %v "+
				"(a request-derived reason would blow up OTLP series cardinality)", reason, enum)
		}
		got[reason] = p.value
	}
	for r, w := range want {
		if got[r] != w {
			t.Fatalf("#311: reason %q OTLP value = %d, want %d — the callback is not reading the actual "+
				"MalformedDialSkippedByReasonSnapshot()[%q]", r, got[r], w, r)
		}
	}
}
