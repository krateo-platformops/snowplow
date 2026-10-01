//go:build unit || integration

// refresher_residency_partb_374_test.go — #374 Part B (pickup-cheapen) arms.
//
// Part B: in processNext, a residency probe BEFORE yieldToCustomer so a
// non-resident no-op NEVER parks a worker behind a customer burst. The dequeue
// Get stays AUTHORITATIVE (freshness Arm C): the probe gates the PARK only.
//
// Models refresher_customer_yield_test.go's harness (withCleanRefresher,
// customerHookInstall, RegisterRefreshFunc, StartRefresher, enqueueRefreshForTest).

package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestFalsifier374_PartB_NonResidentSkipsPark — the load-bearing Part B arm. A
// non-resident key with customerInFlight()=true must be processed (skipped as a
// no-op) WITHOUT parking — pickup_noop_no_park bumps, yieldedTotal does NOT, and
// the resolve handler never runs. RED pre-#374: the no-op parks (yieldedTotal>0)
// and pickup_noop_no_park stays 0.
func TestFalsifier374_PartB_NonResidentSkipsPark(t *testing.T) {
	cleanup := withCleanRefresher(t, 2, 0)
	defer cleanup()

	var inflight atomic.Bool
	inflight.Store(true) // customer burst in flight for the whole test
	customerHookInstall(t, func() bool { return inflight.Load() })

	// NON-resident key: compute a key but DO NOT Put it (no cell).
	inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", BindingUID: "uid-nonresident-374"}
	key := ComputeKey(inputs)

	handlerRan := make(chan struct{}, 1)
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		select {
		case handlerRan <- struct{}{}:
		default:
		}
		return nil
	})

	beforePark := PickupNoopNoParkTotal()
	beforeYield := refresherSingleton().yieldedTotal.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	enqueueRefreshForTest(key)

	// The non-resident no-op must dequeue + skip WITHOUT parking. Generous window.
	time.Sleep(400 * time.Millisecond)

	if d := PickupNoopNoParkTotal() - beforePark; d < 1 {
		t.Fatalf("Part B (RED pre-#374): a non-resident no-op must skip the park + bump pickup_noop_no_park; got delta %d", d)
	}
	if d := refresherSingleton().yieldedTotal.Load() - beforeYield; d != 0 {
		t.Fatalf("Part B (RED pre-#374): a non-resident no-op must NOT park while a customer is in flight (yieldedTotal unchanged); got delta %d — it parked behind the customer", d)
	}
	select {
	case <-handlerRan:
		t.Fatalf("Part B: a non-resident key must NOT run the resolve handler (processOne skips it)")
	default:
	}
}

// TestFalsifier374_PartB_ResidentStillYieldsAndRefreshes — B1 no-regression +
// the freshness Arm C (authoritative dequeue). A RESIDENT key with
// customerInFlight()=true still YIELDS (customer-priority preserved — Part B
// does NOT skip the park for real work), then after release still RE-RESOLVES
// (the dequeue Get is authoritative; a resident key is never dropped by Part B).
func TestFalsifier374_PartB_ResidentStillYieldsAndRefreshes(t *testing.T) {
	cleanup := withCleanRefresher(t, 2, 0)
	defer cleanup()

	var inflight atomic.Bool
	inflight.Store(true)
	customerHookInstall(t, func() bool { return inflight.Load() })

	// RESIDENT key: Put a cell.
	c := ResolvedCache()
	inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", BindingUID: "uid-resident-374"}
	key := ComputeKey(inputs)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"y":1}`), Inputs: &inputs})

	handlerRan := make(chan struct{}, 1)
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		select {
		case handlerRan <- struct{}{}:
		default:
		}
		return nil
	})

	beforeYield := refresherSingleton().yieldedTotal.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)
	enqueueRefreshForTest(key)

	// While inflight: a resident key must PARK (yield engaged) — handler must NOT run.
	select {
	case <-handlerRan:
		t.Fatalf("B1: a resident key ran the handler while customerInFlight()=true — Part B must NOT skip the yield for real (resident) work")
	case <-time.After(250 * time.Millisecond):
	}
	if d := refresherSingleton().yieldedTotal.Load() - beforeYield; d < 1 {
		t.Fatalf("B1 (customer-priority): a resident key must still YIELD while a customer is in flight; yieldedTotal delta %d", d)
	}

	// Release → the resident key still RE-RESOLVES (authoritative dequeue, Arm C).
	inflight.Store(false)
	select {
	case <-handlerRan:
		// Pass — resident key refreshed after release; Part B gated the park only.
	case <-time.After(2 * time.Second):
		t.Fatalf("Arm C: a resident key must still be re-resolved after the yield releases — Part B must not drop the refresh")
	}
}
