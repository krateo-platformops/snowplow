// issue242_cap_rollback_coordinates_test.go — #242: recordInternal's cap
// rollback (deps.go) rolls back the key INSIDE the forward bucket
// (ks.keys.Delete) but not the bucket it just created, leaving an empty keySet
// in the forward index forever. Because bucket-create does coordinates.Add(1)
// (deps.go:697), the coordinates gauge over-reads by exactly the number of
// cap-rollbacks that created a new bucket.
//
// Fix: on the rollback path, when WE created the bucket (!loadedBucket) and no
// concurrent Record has committed an edge into it (ks.count==0), prune it and
// gate coordinates.Add(-1) on the CompareAndDelete success — the count-guarded
// pattern RemoveL1Key already uses (deps.go:1528-1534, #239).
//
// PAIRED + CONVERGENCE falsifier (freshness-audit brief):
//   A  the leak: a NEW-coordinate Record that hits the cap must leave neither a
//      phantom bucket nor an inflated gauge. RED on main.
//   B  no over-prune: a cap-rollback on an EXISTING bucket other keys depend on
//      must NOT delete it — discriminates guard 2 (a bare CompareAndDelete
//      orphans the co-dependents).
//   C  -race convergence: under concurrent Record+rollback, after quiescence the
//      gauge is EXACT — coordinates == #{non-empty forward buckets}, no empties.
//      Isolates guard 1 (!loadedBucket) and guard 3 (decrement gated on the CAS):
//      a fix missing either drifts the gauge below the real bucket count.

package cache

import (
	"sync"
	"testing"
)

// forwardBucketCount counts every bucket in the forward index.
func forwardBucketCount(d *DepTracker) int {
	n := 0
	d.forward.Range(func(_, _ any) bool { n++; return true })
	return n
}

// forwardBucketBreakdown splits the forward index into live (>=1 committed edge)
// and empty (count==0) buckets. After quiescence a count==0 bucket is exactly a
// leaked cap-rollback phantom — no Record is in-flight between ks.keys.LoadOrStore
// and ks.count.Add(1).
func forwardBucketBreakdown(d *DepTracker) (live, empty, total int) {
	d.forward.Range(func(_, v any) bool {
		total++
		if v.(*keySet).count.Load() == 0 {
			empty++
		} else {
			live++
		}
		return true
	})
	return
}

// TestIssue242_A_CapRollbackOnNewCoordinateDoesNotLeakBucketOrCoordinate — Arm A.
func TestIssue242_A_CapRollbackOnNewCoordinateDoesNotLeakBucketOrCoordinate(t *testing.T) {
	d := newTestDepTracker(t, 2) // tiny cap
	gvr := gvrCompositions()
	d.Record("L1A", gvr, "ns", "n1") // bucket n1: coordinates=1, records=1
	d.Record("L1A", gvr, "ns", "n2") // bucket n2: coordinates=2, records=2 (AT cap)

	before := d.Stats()
	beforeBuckets := forwardBucketCount(d)
	if before.Coordinates != 2 || beforeBuckets != 2 {
		t.Fatalf("precondition: coordinates=%d buckets=%d, want 2/2", before.Coordinates, beforeBuckets)
	}

	// A NEW coordinate n3: recordInternal creates the bucket (coordinates.Add(1))
	// THEN the cap check rolls the key back. The bucket must not survive and the
	// gauge must return to its pre-attempt value.
	d.Record("L1A", gvr, "ns", "n3")

	after := d.Stats()
	afterBuckets := forwardBucketCount(d)

	// Vacuity: the cap-rollback actually fired.
	if after.RecordDroppedCap != before.RecordDroppedCap+1 {
		t.Fatalf("vacuity: RecordDroppedCap=%d want %d — the cap-rollback did not fire, the arm is vacuous",
			after.RecordDroppedCap, before.RecordDroppedCap+1)
	}
	if after.Coordinates != before.Coordinates {
		t.Fatalf("RED #242: coordinates=%d after a cap-rolled-back NEW coordinate, want %d — the gauge "+
			"over-reads by the phantom bucket the rollback left in the forward index",
			after.Coordinates, before.Coordinates)
	}
	if afterBuckets != beforeBuckets {
		t.Fatalf("RED #242: forward bucket count=%d after the cap-rollback, want %d — an empty keySet leaked",
			afterBuckets, beforeBuckets)
	}
	if _, empty, _ := forwardBucketBreakdown(d); empty != 0 {
		t.Fatalf("RED #242: %d empty forward bucket(s) leaked by the cap-rollback", empty)
	}
}

// TestIssue242_B_CapRollbackOnExistingBucketDoesNotOverPrune — Arm B.
func TestIssue242_B_CapRollbackOnExistingBucketDoesNotOverPrune(t *testing.T) {
	d := newTestDepTracker(t, 2) // tiny cap
	gvr := gvrCompositions()
	d.Record("L1A", gvr, "ns", "n1") // bucket n1 (count=1): coordinates=1, records=1
	d.Record("L1B", gvr, "ns", "n1") // SAME bucket n1 (count=2): coordinates=1, records=2 (AT cap)

	before := d.Stats()
	if before.Coordinates != 1 || forwardBucketCount(d) != 1 {
		t.Fatalf("precondition: coordinates=%d buckets=%d, want 1/1", before.Coordinates, forwardBucketCount(d))
	}

	// L1C also depends on n1 (EXISTING bucket): recordInternal loads the bucket
	// (loadedBucket=true), adds L1C to ks.keys, hits the cap, rolls the key back.
	// The bucket must SURVIVE — L1A and L1B still depend on it — and coordinates
	// must not move. A bare CompareAndDelete without the !loadedBucket/count==0
	// guards would delete n1 here and orphan L1A/L1B.
	d.Record("L1C", gvr, "ns", "n1")

	after := d.Stats()
	if after.RecordDroppedCap != before.RecordDroppedCap+1 {
		t.Fatalf("vacuity: RecordDroppedCap=%d want %d", after.RecordDroppedCap, before.RecordDroppedCap+1)
	}
	if after.Coordinates != 1 || forwardBucketCount(d) != 1 {
		t.Fatalf("OVER-PRUNE #242: coordinates=%d buckets=%d after a cap-rollback on an EXISTING bucket, "+
			"want 1/1 — the fix must never delete a bucket other keys still depend on",
			after.Coordinates, forwardBucketCount(d))
	}
	// Dependents reachable: n1 still matches L1A and L1B (not orphaned).
	if m := len(d.collectMatchesWithDep(gvr, "ns", "n1")); m != 2 {
		t.Fatalf("OVER-PRUNE #242: coordinate n1 matches %d L1 keys, want 2 (L1A,L1B) — an over-eager "+
			"prune orphaned the co-dependent edges", m)
	}
}

// TestIssue242_C_GaugeInvariantUnderConcurrentCapRollback — Arm C (run -race).
func TestIssue242_C_GaugeInvariantUnderConcurrentCapRollback(t *testing.T) {
	d := newTestDepTracker(t, 8) // small cap → forces new-bucket cap-rollbacks
	gvr := gvrCompositions()

	const goroutines = 24
	const iters = 40
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				// Contended shared coordinates (same bucket, many keys) + unique
				// coordinates (fresh buckets that mostly hit the cap → rollback).
				d.Record("L1_"+itoa(g), gvr, "ns", "shared"+itoa(i%8))
				d.Record("L1_"+itoa(g), gvr, "ns", "uniq"+itoa(g)+"_"+itoa(i))
			}
		}(g)
	}
	wg.Wait()

	// Quiesced. Vacuity: the run actually exercised cap-rollbacks.
	st := d.Stats()
	if st.RecordDroppedCap == 0 {
		t.Fatalf("vacuity: no cap-rollbacks occurred — the concurrency did not exercise the rollback path")
	}
	live, empty, total := forwardBucketBreakdown(d)
	// No leaked empties (RED on main).
	if empty != 0 {
		t.Fatalf("RED #242: %d empty forward bucket(s) leaked under concurrent cap-rollback (total=%d, live=%d)",
			empty, total, live)
	}
	// THE GAUGE INVARIANT: coordinates == live forward buckets. Over-read on main
	// (phantom empties counted); under-read under a guard-1/guard-3-missing fix
	// (over-decrement in the loadedBucket+count==0 race).
	if st.Coordinates != int64(live) {
		t.Fatalf("RED #242: coordinates gauge=%d but %d live forward buckets (total=%d) — the gauge is not "+
			"exact after concurrent cap-rollback", st.Coordinates, live, total)
	}
}
