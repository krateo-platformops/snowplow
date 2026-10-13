package metrics

// metrics_643_prewarm_pass_otlp_test.go — #643 PR-A: the background-pass
// detectors reach OTLP, hand-wired in metrics.go (no expvar->OTLP bridge).
// Driven through the PRODUCTION record paths (dispatchers.RecordPrewarmPassForTest
// calls recordPrewarmPass + the skip bump; restactionsapi.RecordExternalFetchForTest
// calls recordExternalFetch / recordExternalFetchAbandoned) and observed via the
// REAL callback over a ManualReader. RED without the metrics.go wiring: no
// series for the driven cells.

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

func find643(all []point, name string, attrs map[string]string) (int64, bool) {
	for _, p := range pointsFor(all, name) {
		match := len(p.attrs) == len(attrs)
		for k, v := range attrs {
			if p.attrs[k] != v {
				match = false
			}
		}
		if match {
			return p.value, true
		}
	}
	return 0, false
}

func TestIssue643_PrewarmPassDetectorsReachOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	before := flatten(collectViaRealCallback(t, "deadbeef"))
	cutBefore, ok := find643(before, "snowplow_prewarm_pass_total", map[string]string{"kind": "gvr-discovered", "outcome": "cut"})
	if !ok {
		t.Fatalf("snowplow_prewarm_pass_total{kind=gvr-discovered,outcome=cut} absent before any pass — the closed cells must be pre-populated (a 0 is a reading)")
	}
	skipBefore, _ := find643(before, "snowplow_prewarm_skip_declined_external_total", map[string]string{"kind": "gvr-discovered"})
	if _, ok := find643(before, "snowplow_prewarm_skip_resume_done_total", map[string]string{"kind": "gvr-discovered"}); !ok {
		t.Fatalf("snowplow_prewarm_skip_resume_done_total{kind=gvr-discovered} absent — the positive control must exist before the skip does")
	}

	dispatchers.RecordPrewarmPassForTest("gvr-discovered", true, 40, 25, 8*time.Minute, 481*time.Second)

	all := flatten(collectViaRealCallback(t, "deadbeef"))
	if v, _ := find643(all, "snowplow_prewarm_pass_total", map[string]string{"kind": "gvr-discovered", "outcome": "cut"}); v != cutBefore+1 {
		t.Fatalf("pass_total{gvr-discovered,cut} = %d after one cut pass; want %d", v, cutBefore+1)
	}
	want := map[string]int64{
		"snowplow_prewarm_pass_units_planned":         40,
		"snowplow_prewarm_pass_units_done":            25,
		"snowplow_prewarm_pass_budget_ms":             (8 * time.Minute).Milliseconds(),
		"snowplow_prewarm_pass_elapsed_ms":            481000,
		"snowplow_prewarm_pass_resolved_widget_units": 1,
		"snowplow_prewarm_pass_resolved_widget_us":    3000,
		"snowplow_prewarm_pass_resolved_ra_units":     1,
		"snowplow_prewarm_pass_resolved_ra_us":        5000,
	}
	for name, w := range want {
		v, ok := find643(all, name, map[string]string{"kind": "gvr-discovered"})
		if !ok {
			t.Errorf("no %s{kind=gvr-discovered} series — metrics.go wiring missing", name)
			continue
		}
		if v != w {
			t.Errorf("%s{kind=gvr-discovered} = %d; want %d", name, v, w)
		}
	}
	if v, _ := find643(all, "snowplow_prewarm_skip_declined_external_total", map[string]string{"kind": "gvr-discovered"}); v != skipBefore+1 {
		t.Errorf("skip_declined_external_total{gvr-discovered} = %d; want %d", v, skipBefore+1)
	}
}

func TestIssue643_ExternalFetchCountersReachOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	before := flatten(collectViaRealCallback(t, "deadbeef"))
	read := func(all []point, name, origin string) int64 {
		v, ok := find643(all, name, map[string]string{"origin": origin})
		if !ok {
			t.Fatalf("no %s{origin=%s} series — metrics.go wiring missing (cells are pre-populated)", name, origin)
		}
		return v
	}
	bgF, rqF := read(before, "snowplow_external_fetch_total", "background"), read(before, "snowplow_external_fetch_total", "request")
	bgA, rqA := read(before, "snowplow_external_fetch_abandoned_total", "background"), read(before, "snowplow_external_fetch_abandoned_total", "request")

	restactionsapi.RecordExternalFetchForTest(true)
	restactionsapi.RecordExternalFetchForTest(true)
	restactionsapi.RecordExternalFetchForTest(false)

	all := flatten(collectViaRealCallback(t, "deadbeef"))
	if got := read(all, "snowplow_external_fetch_total", "background") - bgF; got != 2 {
		t.Errorf("external_fetch_total{background} delta = %d; want 2", got)
	}
	if got := read(all, "snowplow_external_fetch_total", "request") - rqF; got != 1 {
		t.Errorf("external_fetch_total{request} delta = %d; want 1", got)
	}
	if got := read(all, "snowplow_external_fetch_abandoned_total", "background") - bgA; got != 2 {
		t.Errorf("external_fetch_abandoned_total{background} delta = %d; want 2", got)
	}
	if got := read(all, "snowplow_external_fetch_abandoned_total", "request") - rqA; got != 1 {
		t.Errorf("external_fetch_abandoned_total{request} delta = %d; want 1", got)
	}
}
