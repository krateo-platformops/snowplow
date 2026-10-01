//go:build c1amplification

// issue374_c1_work_test.go — C1 WORK gate for #374 Part B (pickup no-park).
// Measures AVOIDED WORK (counter), not wall-clock: under a customer burst, a
// non-resident no-op dequeue must never PARK a worker.
//
// NOTE on scope/value: Part C (the fan-out isSelf-Get skip) was DROPPED from
// #374 — Has() takes the same c.mu Lock as the Get-miss it would replace (no
// lock-hold cut; +1 lock for resident keys), and its missTotal-de-contamination
// benefit is delivered by #376's GetNoTouch. So #374's measured value is Part B:
// a customer-priority correctness safeguard — a no-op never parks behind a
// customer. This is PRODUCTION value (customer-driven parking); it was NOT the
// 057 symptom (057 has no customer traffic, yielded_total=0). No c.mu-contention
// or drain-throughput claim.
//
// Build-tagged (c1amplification) so normal CI does not run it. Run:
//   go test -tags c1amplification -run TestC1_374 -timeout 10m ./internal/cache/

package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestC1_374_PartB_ParkWorkCut — Part B WORK: under a sustained customer burst,
// N non-resident no-op dequeues each AVOID a yieldToCustomer park. Asserts
// pickup_noop_no_park == N and yieldedTotal unmoved (no park on the no-ops).
func TestC1_374_PartB_ParkWorkCut(t *testing.T) {
	cleanup := withCleanRefresher(t, 4, 0)
	defer cleanup()

	var inflight atomic.Bool
	inflight.Store(true) // sustained customer burst = the production shape
	customerHookInstall(t, func() bool { return inflight.Load() })

	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error { return nil })

	beforePark := PickupNoopNoParkTotal()
	beforeYield := refresherSingleton().yieldedTotal.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	const N = 2_000
	for i := 0; i < N; i++ {
		inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", BindingUID: "c1-nonresident-" + itoa(i)}
		enqueueRefreshForTest(ComputeKey(inputs)) // non-resident (never Put)
	}

	// Let the 4 workers drain the N no-ops. Each must skip the park.
	deadline := time.Now().Add(20 * time.Second)
	for PickupNoopNoParkTotal()-beforePark < uint64(N) && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}

	parkCut := PickupNoopNoParkTotal() - beforePark
	if parkCut < uint64(N) {
		t.Fatalf("Part B WORK: %d/%d non-resident no-op dequeues skipped the park; want all %d", parkCut, N, N)
	}
	if yd := refresherSingleton().yieldedTotal.Load() - beforeYield; yd != 0 {
		t.Fatalf("Part B WORK: no no-op may PARK under the customer burst; yieldedTotal moved by %d", yd)
	}
	t.Logf("#374 Part B WORK CUT: %d/%d non-resident no-op dequeues AVOIDED the yieldToCustomer park (0 parks) under a sustained customer burst (production value; no 057 effect — yielded_total=0 there)", parkCut, N)
}
