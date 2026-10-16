package metrics

// metrics_736_readiness_exit_otlp_test.go — #736: the releasing readiness
// outcome reaches OTLP through the REAL callback, one series per outcome of
// the closed set: all 0 before the exit, then 1 at exactly the releasing
// outcome. RED without the #736 wiring: no snowplow_phase1_readiness_exit_total.

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

func TestIssue736_ReadinessExitOutcomeReachesOTLP(t *testing.T) {
	dispatchers.ResetPhase1ReadinessExitForTest()
	t.Cleanup(dispatchers.ResetPhase1ReadinessExitForTest)

	const name = "snowplow_phase1_readiness_exit_total"
	read := func() map[string]int64 {
		pts := pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), name)
		if len(pts) != len(dispatchers.Phase1ReadinessExitOutcomes) {
			t.Fatalf("#736 RED: %s must reach OTLP as one series per outcome (%d); got %d", name, len(dispatchers.Phase1ReadinessExitOutcomes), len(pts))
		}
		m := map[string]int64{}
		for _, p := range pts {
			if len(p.attrs) != 1 {
				t.Fatalf("#736: %s carries exactly {outcome}; got %v", name, p.attrs)
			}
			m[p.attrs["outcome"]] = p.value
		}
		return m
	}
	for o, v := range read() {
		if v != 0 {
			t.Fatalf("#736: %s{outcome=%s} = %d before any readiness exit, want 0", name, o, v)
		}
	}
	dispatchers.RecordPhase1ReadinessExitForTest("boot_aborted", time.Second, -1, -1, -1, -1, -1)
	got := read()
	for _, o := range dispatchers.Phase1ReadinessExitOutcomes {
		want := int64(0)
		if o == "boot_aborted" {
			want = 1
		}
		if got[o] != want {
			t.Fatalf("#736: after a boot_aborted release %s{outcome=%s} = %d, want %d (%v)", name, o, got[o], want, got)
		}
	}
}
