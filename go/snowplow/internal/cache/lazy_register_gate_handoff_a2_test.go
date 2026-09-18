// lazy_register_gate_handoff_a2_test.go — 1.12.8 option-4 cost falsifier for
// the A2 gate ⇄ #130 F1b confirm-prime hand-off.
//
// # The regression it pins
//
// A2 added a discovery fetch BEFORE registration (unregisterableReason) while
// the F1b confirm-prime fetches the SAME group/version's resource list AFTER
// it. Left alone that is TWO round-trips on first touch of a new group/version
// where there was one — caught, correctly, by
// TestF1b_LazyRegister_PrimesConfirm_NoFlagNoDirectCall's "exactly 1" guard
// (lazy_register_confirm_prime_f1b_test.go). That is a green cost guard on
// lines this change touches, so it was left EXACTLY as it was and the code was
// fixed instead: the gate hands its just-fetched list to the prime, which then
// issues no call of its own.
//
// # Why BOTH arms are needed (feedback_falsifier_shape_must_discriminate)
//
//	ARM A — first GVR in a gv: the gate LIVE-FETCHES and the prime reuses that
//	        list.                                        Total: 1 round-trip.
//	ARM B — a second GVR in the SAME, already-memoised gv: the gate answers
//	        from the memo and fetches NOTHING, so the prime must do its OWN
//	        live fetch.                                  Total: 1 round-trip.
//
// Arm A alone does not discriminate. An implementation that passes the memoised
// list to the prime in BOTH cases also shows 1 in Arm A — while silently
// reintroducing the residual this design exists to avoid: a later GVR
// confirming off data up to one memo TTL (30s) old, which delays a
// post-startup CRD's conjunct-4 heal by a ticker cycle (the property the #119
// group-granularity ruling protects). That implementation shows ZERO
// round-trips in Arm B and fails here.
//
// ATTRIBUTION, not inference: each arm asserts memo state via
// ResourceVerbsMemoHasGVForTest, because only the GATE writes that memo — the
// confirm path (resourceTypeServed) never does. Arm A proves the gate fetched
// (empty → populated); Arm B proves the gate could not have (already
// populated), so its single round-trip is necessarily the prime's. Without
// that, a +1 in Arm B would be equally consistent with the gate re-fetching.
//
// Harness reuse is deliberate: newF1bWatcher / newCountingDiscovery /
// ensureAndSync / waitServable and the walk.example GVRs are the EXACT ones the
// F1b cost guard uses, so the two measure the same seam the same way.

package cache_test

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestF1b_GateHandoff_OneRoundTripWhetherGateFetchedOrMemoHit(t *testing.T) {
	// The resource-verb memo is process-global and outlives individual tests
	// within its TTL, so arm A's precondition is established explicitly rather
	// than assumed from test ordering.
	cache.ResetResourceVerbsMemoForTest()
	t.Cleanup(cache.ResetResourceVerbsMemoForTest)

	rw := newF1bWatcher(t)
	disco := newCountingDiscovery(map[string]bool{"walk.example/v1": true})
	rw.SetDiscoveryClient(disco)

	const gv = "walk.example/v1"

	// --- ARM A: first GVR in the gv — the gate live-fetches. ---------------
	if cache.ResourceVerbsMemoHasGVForTest(gv) {
		t.Fatalf("ARM A precondition: the verb memo must be EMPTY for %s before the first "+
			"registration, otherwise this arm measures the memo-hit path instead", gv)
	}

	ensureAndSync(t, rw, f1bGVRa)

	if !cache.ResourceVerbsMemoHasGVForTest(gv) {
		t.Fatalf("ARM A: after the first registration the gate must have LIVE-FETCHED %s "+
			"(only the A2 gate writes the verb memo); it is still empty, so this arm is not "+
			"exercising the hand-off it claims to", gv)
	}
	if !waitServable(t, rw, f1bGVRa, 3*time.Second) {
		t.Fatalf("ARM A: the lazily-registered GVR must still become servable via the prime — "+
			"the hand-off must CONFIRM, not merely skip the fetch; snap=%+v",
			rw.ServabilitySnapshotFor(f1bGVRa))
	}

	afterA := disco.callCount(gv)
	if afterA != 1 {
		t.Fatalf("ARM A cost: first touch of a new group/version must cost EXACTLY 1 discovery "+
			"round-trip — the gate's fetch REPLACES the prime's; got %d. 2 means the gate's "+
			"list is not reaching primeConfirmAsyncLocked", afterA)
	}

	// --- ARM B: sibling GVR, SAME gv, memo now warm — the prime fetches. ---
	if !cache.ResourceVerbsMemoHasGVForTest(gv) {
		t.Fatalf("ARM B precondition: the memo must still hold %s, so the gate CANNOT live-fetch "+
			"during this registration and any round-trip is attributable to the prime", gv)
	}

	ensureAndSync(t, rw, f1bGVRb)

	if !waitServable(t, rw, f1bGVRb, 3*time.Second) {
		t.Fatalf("ARM B: the sibling GVR must become servable; snap=%+v",
			rw.ServabilitySnapshotFor(f1bGVRb))
	}

	switch got := disco.callCount(gv) - afterA; {
	case got == 0:
		t.Fatalf("ARM B: the prime issued NO discovery call for a GVR registered into an " +
			"already-memoised group/version — the gate handed its MEMOISED list through. That " +
			"reintroduces the residual option 4 exists to avoid: a confirm decided on data up " +
			"to one memo TTL (30s) old, delaying a post-startup CRD's conjunct-4 heal by a " +
			"ticker cycle. The hand-off must be gated on the gate having FRESHLY fetched")
	case got != 1:
		t.Fatalf("ARM B cost: want exactly 1 additional discovery round-trip (the prime's own "+
			"live fetch, since the gate memo-hit); got %d", got)
	}
}
