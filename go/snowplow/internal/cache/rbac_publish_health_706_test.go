package cache

// rbac_publish_health_706_test.go — #691 x #706: a sub-gen bump recorded AFTER
// a rebuild's pending-set take is not flushed by that rebuild's publish (#706
// rotates it one rebuild late). For that one rebuild the #691 gauges must
// still report an unpublished event, and aging, rather than reading 0 because
// the rebuild that just published "completed". It clears on the next publish,
// which is the one that rotates the subject.
//
// What keeps it visible: recordPendingSubGenBumps re-arms
// (scheduleRBACRebuild) AFTER inserting the bump, and that re-arm stamps a
// pending event the completing rebuild did not take.
//
// The arm parks two REAL rebuilds over real informers (i703 helpers):
//
//	R1  takes dave's bump; parked after its take, carol's binding lands and
//	    her bump is recorded; released, R1 publishes WITHOUT rotating carol.
//	R2  the rebuild whose take holds carol's bump; parked after its take,
//	    i.e. after R1's publish and before R2's. Here the gauges are read.
//	    Released, R2 publishes, rotates carol, and the gauge returns to 0.
//
// Mutation "flush everything after the Store" (the pre-#706 order): R1 rotates
// carol, no rebuild ever takes her bump, and the arm fails.

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestIssue691x706_BumpAfterTheTakeStaysUnpublishedUntilTheNextPublish(t *testing.T) {
	defer withCleanDepWatch(t)()
	_, dyn, rec := i703Setup(t, false)
	if err := WaitRBACRebuildQuiesceForTest(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	ResetRBACPublishHealthForTest()

	dave := subjectKey{Kind: "User", Name: "dave"}
	carol := subjectKey{Kind: "User", Name: i703User}
	r1Parked, r2Parked := make(chan struct{}), make(chan struct{})
	r1Release, r2Release := make(chan struct{}), make(chan struct{})
	var r1, r2 atomic.Bool
	var r1Released, r2Released atomic.Bool
	releaseR1 := func() {
		if r1Released.CompareAndSwap(false, true) {
			close(r1Release)
		}
	}
	releaseR2 := func() {
		if r2Released.CompareAndSwap(false, true) {
			close(r2Release)
		}
	}
	h := func(taken takenSubGenBumps) {
		for _, s := range taken.subjects {
			switch {
			case s == dave && r1.CompareAndSwap(false, true):
				close(r1Parked)
				<-r1Release
				return
			case s == carol && r2.CompareAndSwap(false, true):
				close(r2Parked)
				<-r2Release
				return
			}
		}
	}
	rebuildPostTakeHookForTest.Store(&h)
	t.Cleanup(func() { rebuildPostTakeHookForTest.Store(nil) })
	// Deferred after withCleanDepWatch, so these run FIRST: nothing stays
	// parked while that stop joins the writer.
	defer releaseR2()
	defer releaseR1()

	// R1: takes dave's bump and parks before its indexer read.
	i703CreateCRB(t, dyn, "grant-dave", "dave")
	select {
	case <-r1Parked:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition: no rebuild took dave's bump")
	}
	// carol's bump is recorded after R1's take.
	i703CreateCRB(t, dyn, "grant-carol", i703User)
	i703Wait(t, "carol's bump to be recorded", func() bool { return i703Pending(i703User) })
	releaseR1()

	// R2: the rebuild that takes carol's bump. By the time it parks, R1 has
	// published (dave rotated) and carol has not rotated.
	select {
	case <-r2Parked:
	case <-time.After(10 * time.Second):
		t.Fatalf("no rebuild after R1 took carol's bump (carol rotations so far: %d) — "+
			"R1's publish flushed a bump recorded after its take", len(rec.of(i703User)))
	}
	if n := len(rec.of("dave")); n != 1 {
		t.Fatalf("precondition: R1 published dave's rotation %d times, want 1", n)
	}
	if n := len(rec.of(i703User)); n != 0 {
		t.Fatalf("carol rotated %d times before the rebuild that took her bump published", n)
	}
	// THE GAUGE: an RBAC event whose rotation is not yet published, and aging.
	age1 := RBACUnpublishedEventAge()
	if age1 <= 0 {
		t.Fatalf("unpublished_event_age = %v between R1's publish and R2's: carol's bump is unpublished, "+
			"the gauge must not read 0", age1)
	}
	time.Sleep(50 * time.Millisecond)
	if age2 := RBACUnpublishedEventAge(); age2 <= age1 {
		t.Fatalf("unpublished_event_age did not age while carol's bump stayed unpublished (%v → %v)", age1, age2)
	}
	publishAge := RBACLastPublishAge()

	releaseR2()
	if err := WaitRBACRebuildQuiesceForTest(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	carols := rec.of(i703User)
	if len(carols) != 1 || !carols[0].snapGrants {
		t.Fatalf("carol must rotate exactly once, with a snapshot that binds her: %+v", carols)
	}
	if age := RBACUnpublishedEventAge(); age != 0 {
		t.Fatalf("unpublished_event_age = %v after the publish that rotated carol, want 0", age)
	}
	if after := RBACLastPublishAge(); after >= publishAge {
		t.Fatalf("last_successful_publish_age did not reset at R2's publish (%v → %v)", publishAge, after)
	}
}
