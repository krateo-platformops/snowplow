package cache

// store_verify_race_test.go — B-6, and the #239 coordinate gauge.
//
// The decorator reads the informer's indexer from the REFLECTOR's goroutine
// while the informer's own processor goroutine is writing it, and the repair
// queue mutates shared bookkeeping from a third. Converting anything here from
// a private copy to a shared reference is a concurrency change and needs a
// concurrent arm, not a content-equivalence check
// (feedback_shared_vs_copy_is_a_concurrency_change).

import (
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestStoreVerification_ConcurrentSnapshotsAndEvents — B-6.
//
// Drives repeated real re-establishments while the authoritative set changes
// underneath, so comparisons run concurrently with Replace, with delta
// processing, and with each other. Run with -race; the assertions are
// deliberately weak because the RACE DETECTOR is the assertion — an arm that
// asserted counts here would fail on scheduling rather than on correctness,
// and would be quietly disabled the first time it flaked.
func TestStoreVerification_ConcurrentSnapshotsAndEvents(t *testing.T) {
	resetVerificationForArm(t)
	src := newFakeSource(verifyGVR,
		fakeObj{ns: "krateo", name: "a", rv: "1", uid: "u-a"},
		fakeObj{ns: "krateo", name: "b", rv: "1", uid: "u-b"},
		fakeObj{ns: "krateo", name: "c", rv: "1", uid: "u-c"},
	)
	inf, _ := verifiedInformer(t, src)

	var wg sync.WaitGroup
	// Mutator: the authoritative set moves constantly, with no events emitted.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			rv := verifyItoa(2 + i)
			src.setSilently(
				fakeObj{ns: "krateo", name: "a", rv: rv, uid: "u-a"},
				fakeObj{ns: "krateo", name: "b", rv: rv, uid: "u-b2"}, // uid churn
			)
		}
	}()
	// Breaker: forces real re-establishments, hence real comparisons.
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Paced: the reflector backs off ~800 ms between re-establishments, so
		// breaking faster than that would spin without ever producing the
		// second invocation a comparison needs.
		for i := 0; i < 5; i++ {
			time.Sleep(400 * time.Millisecond)
			src.breakWatches()
		}
	}()
	// Reader: the /debug/servable and expvar surfaces read the same state a
	// scrape would, concurrently with all of it.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 400; i++ {
			_ = StoreVerificationStatsByStat()
			_ = VerifySkippedByReasonSnapshot()
			_ = UnverifiableByReasonSnapshot()
			_, _ = verificationRowFor(verifyGVR)
			inf.GetIndexer().ListKeys()
		}
	}()
	wg.Wait()
	waitForVerify(t, "at least one comparison to complete", verifyBound, func() bool {
		return storeVerificationsTotal.Load() > 0
	})

	// The instrument must still be coherent — a torn read would show up as a
	// confirmed count above the raw candidate count, which is impossible.
	lu, ld, la, um := divergenceCounts(verifySiteSnapshot)
	if got, cand := lu+ld+la+um, storeDivergenceCandidates.Load(); got > cand {
		t.Errorf("confirmed divergences (%d) exceed raw candidates (%d) — the counters are not coherent", got, cand)
	}
	if storeVerificationsTotal.Load() == 0 {
		t.Error("no verification completed at all; the arm exercised nothing")
	}
}

// TestDepsCoordinates_CountsDistinctCoordinatesNotEdges — #239.
//
// The whole value of this gauge is that it is the number `records` is NOT. The
// scaling conclusion in #239 — that dirty-mark fan-out grows linearly with
// cohort count, because coordinates are cluster-scoped while L1 entries are
// cohort-scoped — was derived from edges divided by a measured fan-out, never
// counted. This arm asserts the two move INDEPENDENTLY, which is the property
// that makes the gauge able to confirm or kill that conclusion.
func TestDepsCoordinates_CountsDistinctCoordinatesNotEdges(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	d := Deps()

	gvr := schema.GroupVersionResource{Group: "widgets.krateo.io", Version: "v1beta1", Resource: "panels"}

	// THE COHORT AXIS. Many L1 keys — one per cohort — onto ONE coordinate.
	// This is the shape #239 describes: adding users adds entries, not objects.
	for i := 0; i < 25; i++ {
		d.Record("L1_cohort_"+verifyItoa(i), gvr, "demo", "panel-a")
	}
	if got := d.Stats().Coordinates; got != 1 {
		t.Fatalf("coordinates = %d after 25 cohorts depended on ONE object, want 1 — if this grows "+
			"with cohorts, #239's scaling conclusion collapses and the gauge is what says so", got)
	}
	if got := d.Stats().TotalRecords; got != 25 {
		t.Fatalf("records = %d, want 25 — edges DO scale with cohorts", got)
	}

	// THE CLUSTER AXIS. Distinct objects add coordinates.
	d.Record("L1_cohort_0", gvr, "demo", "panel-b")
	d.Record("L1_cohort_0", gvr, "demo", "panel-c")
	if got := d.Stats().Coordinates; got != 3 {
		t.Fatalf("coordinates = %d after two more distinct objects, want 3", got)
	}

	// Idempotence: re-recording an existing edge must move neither number.
	before := d.Stats()
	d.Record("L1_cohort_0", gvr, "demo", "panel-b")
	after := d.Stats()
	if before.Coordinates != after.Coordinates || before.TotalRecords != after.TotalRecords {
		t.Errorf("a duplicate Record moved the counters: %+v -> %+v", before, after)
	}

	// A coordinate disappears only when its LAST edge does. Dropping one of
	// the 25 cohort keys must NOT retire the coordinate they share — the
	// decrement belongs to the bucket, not to the edge.
	d.RemoveL1Key("L1_cohort_1")
	if got := d.Stats().Coordinates; got != 3 {
		t.Errorf("coordinates = %d after one of 25 cohorts left, want 3 — the gauge is counting "+
			"edges, not coordinates", got)
	}
	for i := 0; i < 25; i++ {
		d.RemoveL1Key("L1_cohort_" + verifyItoa(i))
	}
	if got := d.Stats().Coordinates; got != 0 {
		t.Errorf("coordinates = %d after every edge was removed, want 0 — a coordinate that outlives "+
			"its last edge is a leak the gauge would report as real fan-out capacity", got)
	}
	if got := d.Stats().TotalRecords; got != 0 {
		t.Errorf("records = %d after every edge was removed, want 0", got)
	}
}
