package metrics

// metrics_233_otlp_test.go — #233 check 5: the unparseable-CA delegation counter
// is OTLP-NATIVE (registered in registerInstruments + observed via the real
// callback), reaching ClickStack — NOT expvar-only. A DETECTOR (non-zero = a
// broken CA bundle) buried in /debug/vars is observable-if-you-look, not
// alertable (#311). This runs the REAL registerInstruments + callback over a
// ManualReader (collectViaRealCallback), so it proves the production wiring.

import (
	"testing"

	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func TestIssue233_UnparseableCADelegationsReachOTLP(t *testing.T) {
	restactionsapi.ResetUnparseableCADelegationForTest()
	t.Cleanup(restactionsapi.ResetUnparseableCADelegationForTest)

	// Make the accessor non-zero so the assertion proves the callback observes the
	// ACTUAL value, not merely that the instrument name is registered.
	for i := 0; i < 3; i++ {
		restactionsapi.RecordUnparseableCADelegationForTest()
	}

	all := flatten(collectViaRealCallback(t, "deadbeef"))
	pts := pointsFor(all, "snowplow_unparseable_ca_delegations_total")

	if len(pts) != 1 {
		t.Fatalf("#233: snowplow_unparseable_ca_delegations_total must be REGISTERED as exactly one OTLP "+
			"series (unlabeled scalar detector) reaching ClickStack — got %d points (0 = unregistered / "+
			"expvar-only, the #311 alertability failure)", len(pts))
	}
	if pts[0].value != 3 {
		t.Fatalf("#233: the OTLP callback must observe the ACTUAL counter value (want 3, the bumped "+
			"total); got %d — the callback is not reading restactionsapi.UnparseableCADelegationTotal()",
			pts[0].value)
	}
	if len(pts[0].attrs) != 0 {
		t.Fatalf("#233: the detector must be an UNLABELED scalar (1 series, bounded cardinality); "+
			"got attrs %v", pts[0].attrs)
	}
}
