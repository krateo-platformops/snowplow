package cache

// rbac_publish_slot_1206_test.go — #1206: every RBAC publish goes through the
// writer slot, RebuildRBACSnapshotForTest included.
//
// The failure: a test's rebuild ran outside the slot, while one the informers'
// events had started was still in flight. That rebuild had read the indexers
// first and Stored last: a higher PublishSeq holding older content. The arm then
// read one identity's verdicts on the older snapshot and another's on the newer,
// and reported two identities with one binding set as divergent.
//
// The same order would undo a revoke, so the arm is written as one. No timing: a
// real rebuild reads the indexers and parks before its Store, a real revoke lands,
// and only then does the helper run. Under the slot it cannot finish while the
// parked rebuild holds the slot, so no Store can come after it with older content.
//
// Mutation → RED, every run: RebuildRBACSnapshotForTest calls rebuildRBACSnapshot
// directly (the helper finishes while the slot is held, and the parked Store then
// undoes the revoke).

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRBACPublish1206_TheHelperNeverOvertakesARebuildInFlight(t *testing.T) {
	defer withCleanDepWatch(t)()
	rw, dyn, _ := i703Setup(t, true) // carol is bound

	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var sawRevoked, undone atomic.Bool
	h := func() {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			return
		}
		// Before each later Store, the live snapshot is what the last one left.
		if !i703SnapGrants(i703User) {
			sawRevoked.Store(true)
		} else if sawRevoked.Load() {
			undone.Store(true)
		}
	}
	rebuildPreStoreHookForTest.Store(&h)
	t.Cleanup(func() { rebuildPreStoreHookForTest.Store(nil) })
	var released atomic.Bool
	releaseOnce := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	defer releaseOnce()

	// An unrelated event starts a rebuild: it reads carol as bound, and parks
	// before its Store, holding the writer slot.
	i703CreateCRB(t, dyn, "grant-dave", "dave")
	select {
	case <-entered:
	case <-time.After(30 * time.Second):
		t.Fatal("PRECONDITION: the rebuild never reached its Store")
	}
	i703DeleteCRB(t, dyn, "grant-carol") // the revoke lands in the indexer
	i703Wait(t, "the revoke's bump to be recorded", func() bool { return i703Pending(i703User) })

	done := make(chan struct{})
	go func() {
		RebuildRBACSnapshotForTest(rw)
		close(done)
	}()
	// A helper that bypasses the slot finishes in milliseconds, whatever the load:
	// this bound decides nothing on correct code, which stays parked until release.
	select {
	case <-done:
		t.Fatalf("RED (#1206): RebuildRBACSnapshotForTest returned while another rebuild held the writer slot")
	case <-time.After(2 * time.Second):
	}

	releaseOnce()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the helper never ran after the slot was released")
	}
	if err := WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		t.Fatal(err)
	}
	if undone.Load() {
		t.Fatalf("RED (#1206): a Store with older content undid a published revoke")
	}
	if i703SnapGrants(i703User) {
		t.Fatalf("RED: the revoke is not in the final snapshot")
	}
}
