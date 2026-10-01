package metrics

// metrics_m3_customer_inflight_otlp_test.go — #386 M3 (PR2): the resolve-path
// customer-in-flight COUNT reaches OTLP as snowplow_customer_resolve_inflight,
// hand-wired in metrics.go on #311's pattern (no expvar->OTLP bridge, scalars
// only). This is a CORRELATION gauge, NOT a fault detector (0 = no customers =
// healthy), so the falsifier is "measures the right quantity under driven load":
// K concurrent resolve-path in-flight → the gauge reads K, then DRAINS to 0.
// RED without the metrics.go wiring (0 series). Observed via the REAL callback
// over a ManualReader (collectViaRealCallback), not a seam.

import (
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
)

func TestIssue386_M3_CustomerResolveInflightReachesOTLP(t *testing.T) {
	// Baseline: a single series reading 0 (idle = healthy — a correlation signal,
	// not an alarm). If this isn't 0, some other in-flight leaked — fail loudly.
	base := pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_customer_resolve_inflight")
	if len(base) != 1 || base[0].value != 0 {
		t.Fatalf("#386 M3: baseline snowplow_customer_resolve_inflight = %+v; want a single series == 0 "+
			"(0 = unregistered/expvar-only = the PR2 wiring failure, or a leaked in-flight)", base)
	}

	// Drive K CONCURRENT resolve-path /calls in flight: K goroutines each mark
	// in-flight via the SAME mechanism production uses, hold at a barrier, and we
	// collect while all K are live.
	const K = 7
	release := make(chan struct{})
	var started, finished sync.WaitGroup
	started.Add(K)
	finished.Add(K)
	for i := 0; i < K; i++ {
		go func() {
			defer finished.Done()
			dec := dispatchers.MarkCustomerResolveInFlightForTest()
			started.Done()
			<-release
			dec()
		}()
	}
	started.Wait() // all K are now in-flight

	pts := pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_customer_resolve_inflight")
	if len(pts) != 1 {
		t.Fatalf("#386 M3: want ONE snowplow_customer_resolve_inflight series, got %d (%+v)", len(pts), pts)
	}
	if pts[0].value != int64(K) {
		t.Fatalf("#386 M3: OTLP gauge = %d under %d concurrent resolve-path in-flight; want %d — "+
			"the callback is not reading dispatchers.CustomerResolveInFlightCount()", pts[0].value, K, K)
	}

	// Release all; the gauge MUST drain back to 0 (in-flight is transient).
	close(release)
	finished.Wait()
	after := pointsFor(flatten(collectViaRealCallback(t, "deadbeef")), "snowplow_customer_resolve_inflight")
	if len(after) != 1 || after[0].value != 0 {
		t.Fatalf("#386 M3: after drain gauge = %+v; want a single series == 0 — the count did not return to idle", after)
	}
}
