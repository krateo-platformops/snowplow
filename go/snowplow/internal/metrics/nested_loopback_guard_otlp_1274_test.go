package metrics

// nested_loopback_guard_otlp_1274_test.go — #1274 review: the recursion
// guard's stops are on OTLP through the REAL registration and callback, both
// kinds present at 0 before any stop, each moved by its production bump, with
// only the closed kind attribute.

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func guardStops1274(t *testing.T) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for _, p := range pointsFor(collect1265(t), "snowplow_nested_loopback_guard_stops_total") {
		if len(p.attrs) != 1 {
			t.Fatalf("attributes %v, want exactly kind", p.attrs)
		}
		out[p.attrs["kind"]] = p.value
	}
	return out
}

func TestNestedLoopbackGuard1274_StopsAreOnOTLPFromZero(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true") // CFG-1: a cache-side instrument
	before := guardStops1274(t)
	if len(before) != 2 {
		t.Fatalf("kinds %v, want exactly cycle and depth, present before any stop", before)
	}
	for _, k := range cache.NestedLoopbackGuardStopKinds {
		if _, ok := before[k]; !ok {
			t.Fatalf("kind %q absent: %v", k, before)
		}
	}
	cache.BumpHTTPEdgeCycleStop()
	cache.BumpHTTPEdgeDepthStop()
	cache.BumpHTTPEdgeDepthStop()
	after := guardStops1274(t)
	if after["cycle"]-before["cycle"] != 1 || after["depth"]-before["depth"] != 2 {
		t.Fatalf("OTLP stops moved %v → %v, want cycle +1, depth +2", before, after)
	}
	snap := cache.NestedLoopbackGuardStopsSnapshot()
	for _, k := range cache.NestedLoopbackGuardStopKinds {
		if int64(snap[k]) != after[k] {
			t.Fatalf("OTLP %s=%d, the expvar snapshot %d", k, after[k], snap[k])
		}
	}
}
