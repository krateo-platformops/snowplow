package metrics

// metrics_397_phase1_deadline_otlp_test.go — #397: the phase-1 deadline-release
// DETECTOR reaches OTLP through the REAL callback (registerInstruments over a
// ManualReader, collectViaRealCallback). 0 on a process whose readiness was not
// deadline-released; 1 once the real recorder takes the deadline outcome. RED
// without the #397 wiring: no snowplow_phase1_deadline_released_total series.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

func TestIssue397_Phase1DeadlineReleasedReachesOTLP(t *testing.T) {
	dispatchers.ResetPhase1ReadinessExitForTest()
	t.Cleanup(dispatchers.ResetPhase1ReadinessExitForTest)

	const name = "snowplow_phase1_deadline_released_total"

	pts := pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), name)
	if len(pts) != 1 {
		t.Fatalf("#397 RED: %s must reach OTLP as exactly one series (expvar-only = unalertable); got %d", name, len(pts))
	}
	if pts[0].value != 0 {
		t.Fatalf("#397: %s = %d before any readiness exit, want 0", name, pts[0].value)
	}

	dispatchers.RecordPhase1DeadlineExitForTest()

	pts = pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), name)
	if len(pts) != 1 || pts[0].value != 1 {
		t.Fatalf("#397: after a deadline release %s must read 1 through the real callback; got %+v", name, pts)
	}
	if len(pts[0].attrs) != 0 {
		t.Fatalf("#397: %s is a bare scalar; got attributes %v", name, pts[0].attrs)
	}
}
