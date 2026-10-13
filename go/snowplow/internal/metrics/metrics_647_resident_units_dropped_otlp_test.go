package metrics

// metrics_647_resident_units_dropped_otlp_test.go — #647: the counter of
// RESTAction units the resident enumeration dropped reaches OTLP as
// snowplow_prewarm_resident_units_dropped_total{reason}, hand-wired in
// metrics.go (no expvar->OTLP bridge). Driven through the PRODUCTION record path
// (dispatchers.RecordResidentUnitDroppedForTest calls recordResidentUnitDropped)
// and observed via the REAL callback over a ManualReader. RED without the
// metrics.go wiring: no series at all.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

func TestIssue647_ResidentUnitsDroppedReachOTLP(t *testing.T) {
	read := func() map[string]int64 {
		out := map[string]int64{}
		for _, p := range pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_prewarm_resident_units_dropped_total") {
			out[p.attrs["reason"]] = p.value
		}
		return out
	}
	before := read()
	for _, r := range []string{"get_failed", "decode_failed", "no_static_target"} {
		if _, ok := before[r]; !ok {
			t.Fatalf("no snowplow_prewarm_resident_units_dropped_total{reason=%s} series (the closed set is emitted, zeros included; got %v) — metrics.go wiring missing", r, before)
		}
	}

	dispatchers.RecordResidentUnitDroppedForTest("get_failed")
	dispatchers.RecordResidentUnitDroppedForTest("get_failed")
	dispatchers.RecordResidentUnitDroppedForTest("no_static_target")

	after := read()
	if d := after["get_failed"] - before["get_failed"]; d != 2 {
		t.Fatalf("OTLP get_failed delta = %d after 2 records; want 2", d)
	}
	if d := after["no_static_target"] - before["no_static_target"]; d != 1 {
		t.Fatalf("OTLP no_static_target delta = %d after 1 record; want 1", d)
	}
	if d := after["decode_failed"] - before["decode_failed"]; d != 0 {
		t.Fatalf("OTLP decode_failed delta = %d; want 0", d)
	}
}
