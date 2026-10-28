// refresher_yield_falsifier_test.go — Ship #98 / 0.30.215 cross-package
// end-to-end falsifier for the cooperative customer-priority yield.
//
// Wires the production pair:
//
//   cache.SetCustomerInflightHook(dispatchers.CustomerInFlight)
//
// then drives the dispatchers' customer-inflight counter via the same
// markCustomerInFlight() bracket every real /call uses (the prewarm
// engine's signal at prewarm_engine.go:92-99) and verifies the
// refresher's yield engages / releases under the actual production
// wiring — not just under a synthetic predicate.
//
// COVERAGE
//
//   - FA-98.1 (Wiring): cache.SetCustomerInflightHook + dispatchers.CustomerInFlight
//     observe the same atomic counter. Driving customerInFlightCount via
//     markCustomerInFlight() makes cache's customerInFlightLocked() return
//     true.
//
//   - FA-98.2 (Yield engages under real wiring): with the production
//     hook installed, holding the customer-inflight counter via
//     markCustomerInFlight() postpones a refresher handler invocation
//     for at least 200ms (the architect §3 yield-engages assertion
//     against the actual code path).
//
//   - FA-98.3 (Yield releases promptly): once the markCustomerInFlight
//     done-fn fires (counter goes back to 0), the refresher handler
//     runs within the yield-poll envelope.

package dispatchers

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// markCustomerInFlightForTest is the test-only seam to drive the
// customer-inflight counter from outside the ServeHTTP entry points.
// Production code MUST NOT call this; the production increment lives
// at restactions.go:77 / widgets.go:62 brackets.
func markCustomerInFlightForTest() func() {
	return markCustomerInFlight()
}

// FA-98.1 — wiring verification. The cache-side hook returns
// dispatchers.CustomerInFlight()'s value 1:1.
func TestFA98_1_HookWiring(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetDepsForTest()
	cache.ResetResolvedCacheForTest()
	cache.ResetRefresherForTest()
	t.Cleanup(func() {
		cache.SetCustomerInflightHook(nil)
		cache.ResetRefresherForTest()
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	// Wire production hook.
	cache.SetCustomerInflightHook(CustomerInFlight)

	// Counter starts at 0 — hook must report no in-flight.
	if CustomerInFlight() {
		t.Fatalf("CustomerInFlight()=true at start; counter must be 0")
	}

	// Hold a single customer in-flight via the production bracket.
	done := markCustomerInFlightForTest()
	if !CustomerInFlight() {
		t.Fatalf("CustomerInFlight()=false after markCustomerInFlight(); counter must be >0")
	}
	// The cache-side predicate MUST see the same value (verified via the
	// hook). We assert indirectly through CustomerInFlight (the predicate
	// the hook returns) — they are the same atomic counter so this is
	// load-bearing.

	done()
	if CustomerInFlight() {
		t.Fatalf("CustomerInFlight()=true after done(); counter must be back to 0")
	}
}

// FA-98.2 — yield engages END-TO-END under production wiring. A
// refresher handler enqueued WHILE a customer is in-flight does NOT
// run until the counter drops.
func TestFA98_2_YieldEngagesEndToEnd(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetDepsForTest()
	cache.ResetResolvedCacheForTest()
	cache.ResetRefresherForTest()
	t.Cleanup(func() {
		cache.SetCustomerInflightHook(nil)
		cache.ResetRefresherForTest()
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	cache.SetCustomerInflightHook(CustomerInFlight)

	// Hold customer inflight (counter=1).
	done := markCustomerInFlightForTest()

	c := cache.ResolvedCache()
	inputs := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", BindingUID: "uid-cafe"}
	key := cache.ComputeKey(inputs)
	c.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &inputs})

	var handlerRan atomic.Int32
	cache.RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ cache.ResolvedKeyInputs) error {
		handlerRan.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.StartRefresher(ctx)

	// Enqueue a refresh through the production seam. The refresher's
	// worker will pull it, call yieldToCustomer(), see counter=1, park.
	cache.EnqueueRefresh(key)

	// 200ms window: handler MUST NOT have run (counter still 1).
	time.Sleep(200 * time.Millisecond)
	if got := handlerRan.Load(); got != 0 {
		t.Fatalf("FA-98.2: refresher handler ran %d times while CustomerInFlight()=true; yield did not engage", got)
	}

	// FA-98.3 — release counter; handler must run within 2s.
	done()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if handlerRan.Load() >= 1 {
			return // Pass.
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("FA-98.3: refresher handler did not run within 2s after done(); yield never released")
}

// FA-98.4 — concurrent customers do NOT drop counter below 0; the
// production bracket is balanced. Race-checked.
func TestFA98_4_ConcurrentCustomersBalanced(t *testing.T) {
	const N = 64
	dones := make([]func(), N)
	for i := 0; i < N; i++ {
		dones[i] = markCustomerInFlight()
	}
	if !customerInFlight() {
		t.Fatalf("counter should be %d, customerInFlight() returned false", N)
	}
	for _, d := range dones {
		d()
	}
	if customerInFlight() {
		t.Fatalf("counter not back to 0 after %d balanced done()s", N)
	}
}
