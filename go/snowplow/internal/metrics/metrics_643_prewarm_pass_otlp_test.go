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

	"github.com/krateo-platformops/snowplow/internal/cache"
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
	// #643 PR-B2 — each series carries {origin, driver}; the {origin} reading is
	// the sum over that origin's driver series.
	read := func(all []point, name, origin string) int64 {
		var sum int64
		n := 0
		for _, p := range pointsFor(all, name) {
			if p.attrs["origin"] == origin {
				sum += p.value
				n++
			}
		}
		if n == 0 {
			t.Fatalf("no %s{origin=%s} series — metrics.go wiring missing (cells are pre-populated)", name, origin)
		}
		return sum
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

// TestIssue643_PRB2_ExternalFetchDriverReachesOTLP — #643 PR-B2 W5 OTLP arm:
// every closed driver cell is a series before any fetch (a 0 is a reading),
// each carries the origin it rolls up to, and a fetch under one driver moves
// exactly that series on both counters.
func TestIssue643_PRB2_ExternalFetchDriverReachesOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	wantOrigin := func(d string) string {
		if d == cache.RefDenialOriginServe {
			return "request"
		}
		return "background"
	}
	before := flatten(collectViaRealCallback(t, "deadbeef"))
	for _, name := range []string{"snowplow_external_fetch_total", "snowplow_external_fetch_abandoned_total"} {
		for _, d := range cache.RefDenialOrigins {
			if _, ok := find643(before, name, map[string]string{"origin": wantOrigin(d), "driver": d}); !ok {
				t.Fatalf("no %s{origin=%s,driver=%s} series before any fetch — every closed driver cell must be pre-populated",
					name, wantOrigin(d), d)
			}
		}
		if got := len(pointsFor(before, name)); got != len(cache.RefDenialOrigins) {
			t.Fatalf("%s has %d series, want exactly %d (one per closed driver)", name, got, len(cache.RefDenialOrigins))
		}
	}
	for _, d := range cache.RefDenialOrigins {
		restactionsapi.RecordExternalFetchDriverForTest(d)
		after := flatten(collectViaRealCallback(t, "deadbeef"))
		for _, name := range []string{"snowplow_external_fetch_total", "snowplow_external_fetch_abandoned_total"} {
			for _, e := range cache.RefDenialOrigins {
				b, _ := find643(before, name, map[string]string{"origin": wantOrigin(e), "driver": e})
				a, _ := find643(after, name, map[string]string{"origin": wantOrigin(e), "driver": e})
				want := int64(0)
				if e == d {
					want = 1
				}
				if a-b != want {
					t.Errorf("a fetch under driver=%s moved %s{driver=%s} by %d, want %d", d, name, e, a-b, want)
				}
			}
		}
		before = after
	}
}

// TestIssue643_PRB2_WalkCountersReachOTLP — the walk positive control and the
// unmarkable counter are OTLP series (hand-wired; no expvar bridge).
func TestIssue643_PRB2_WalkCountersReachOTLP(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	all := flatten(collectViaRealCallback(t, "deadbeef"))
	for _, name := range []string{"snowplow_prewarm_walk_skip_declined_external_total", "snowplow_prewarm_walk_errored_unmarkable_total"} {
		v, ok := find643(all, name, map[string]string{})
		if !ok {
			t.Fatalf("no %s series — metrics.go wiring missing", name)
		}
		if want := map[string]uint64{
			"snowplow_prewarm_walk_skip_declined_external_total": dispatchers.WalkSkipDeclinedExternalTotal(),
			"snowplow_prewarm_walk_errored_unmarkable_total":     dispatchers.WalkErroredUnmarkableTotal(),
		}[name]; uint64(v) != want {
			t.Fatalf("%s = %d on OTLP, %d in process", name, v, want)
		}
	}
}
