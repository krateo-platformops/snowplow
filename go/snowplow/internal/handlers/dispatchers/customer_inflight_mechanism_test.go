package dispatchers

// customer_inflight_mechanism_test.go — #386 M3: the in-flight COUNT mechanism.
// dec-on-ALL-exits (incl panic), clamp >= 0, and concurrent balanced inc/dec
// drains to 0. The carrier-SCOPE arms (resolve-path marks, writes/list/direct-proxy
// do NOT) live in internal/handlers (they need the Call()/List() handlers).

import (
	"sync"
	"testing"
)

func TestIssue386_M3_InflightMarkMechanism(t *testing.T) {
	zeroCustomerInFlight()
	t.Cleanup(zeroCustomerInFlight)

	if n := CustomerResolveInFlightCount(); n != 0 {
		t.Fatalf("#386 M3: baseline inflight=%d, want 0", n)
	}

	// inc / dec
	done := markCustomerInFlight()
	if n := CustomerResolveInFlightCount(); n != 1 {
		t.Fatalf("#386 M3: after mark inflight=%d, want 1", n)
	}
	done()
	if n := CustomerResolveInFlightCount(); n != 0 {
		t.Fatalf("#386 M3: after done inflight=%d, want 0", n)
	}

	// dec-on-PANIC: every ServeHTTP uses `defer markCustomerInFlight()()`, so the
	// decrement MUST run on a panic exit too — otherwise a panicking /call leaks a
	// permanent +1 that would starve #384 serveReserve forever.
	func() {
		defer func() { _ = recover() }()
		defer markCustomerInFlight()()
		panic("boom")
	}()
	if n := CustomerResolveInFlightCount(); n != 0 {
		t.Fatalf("#386 M3: after panic inflight=%d, want 0 — the deferred decrement must cover a panic exit", n)
	}

	// CLAMP >= 0: a hypothetical inc/dec imbalance must never surface a negative
	// count to #384 serveReserve (which would over-reserve P_max).
	customerInFlightCount.Add(-1)
	if n := CustomerResolveInFlightCount(); n != 0 {
		t.Fatalf("#386 M3: clamp inflight=%d, want 0 (read-side floor on a negative raw count)", n)
	}
	customerInFlightCount.Add(1) // restore balance
	if n := CustomerResolveInFlightCount(); n != 0 {
		t.Fatalf("#386 M3: after restore inflight=%d, want 0", n)
	}

	// Concurrent balanced inc/dec drains to exactly 0 (atomic; no lost updates).
	var wg sync.WaitGroup
	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer markCustomerInFlight()()
		}()
	}
	wg.Wait()
	if n := CustomerResolveInFlightCount(); n != 0 {
		t.Fatalf("#386 M3: after 256 concurrent balanced inc/dec inflight=%d, want 0", n)
	}
}
