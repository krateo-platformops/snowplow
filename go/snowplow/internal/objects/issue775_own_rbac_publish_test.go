package objects

// issue775_own_rbac_publish_test.go — #775: TestGet_RBACNarrowing_GroupGrant
// failed under load with a 401 "Endpoint not found in context", right after
// `cache.rbac.snapshot.initial_publish_aborted`.
//
// THE RACE. WaitForCacheSync waits on the informers' HasSynced, but the RBAC
// snapshot EvaluateRBAC reads is published by a SEPARATE goroutine
// (waitAndPublishInitialRBACSnapshot) after the RBAC sync channels close, into
// one process-global snapshot. A harness that returns after WaitForCacheSync
// can therefore GET while the global still holds nil (degrade-to-deny) or a
// previous test's snapshot: the group grant reads as denied, the informer
// branch is skipped, and the apiserver fall-through has no user endpoint — the
// 401. The aborted line is the failing test's OWN watcher: its publish had not
// run even when the test's cleanup stopped it.
//
// ARMS
//   P1  this watcher's initial publish is PARKED at the rebuild barrier (the
//       exact point between "RBAC informers synced" and "snapshot stored"),
//       and released only when the harness says it is waiting for that
//       publish. A harness that GETs without waiting runs inside the window
//       every time; one that waits cannot.
//   P2  a neighbour's late publish (a foreign snapshot that grants nobody,
//       #385's shape) lands AFTER this watcher's own initial publish has
//       returned. The harness's synchronous rebuild from its own watcher must
//       make the live snapshot its own again before the GET.

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue775_P1_GroupGrantWaitsForItsOwnRBACPublish(t *testing.T) {
	if rbacNarrowBaseline {
		t.Skip("group-grant assertion is Tag-0.30.101 only")
	}
	resetServeCounters()
	// Before this watcher publishes, the global holds whatever a previous
	// test left; nil is the snapshot-less state (degrade-to-deny).
	cache.PublishRBACSnapshotForTest(nil)

	awaiting := make(chan struct{})
	finished := make(chan struct{})
	var parked atomic.Int64
	cache.SetRebuildBarrierForTest(func() {
		parked.Add(1)
		select {
		case <-awaiting:
		case <-finished:
		}
	})
	// Release before ANY cleanup: the watcher's Stop (a later-registered, so
	// earlier-run, cleanup) waits for the parked publish goroutine. A defer
	// runs on t.Fatalf too, ahead of the cleanups.
	defer func() {
		close(finished)
		cache.SetRebuildBarrierForTest(nil)
	}()
	var once sync.Once
	ownRBACPublishAwaitHook = func() { once.Do(func() { close(awaiting) }) }
	t.Cleanup(func() { ownRBACPublishAwaitHook = nil })

	newGroupGrantWatcher(t)

	res := Get(ctxWithUser("alice", "devs"), serveTestRef(authorizedNS, "obj-authorized"))
	if res.Unstructured == nil {
		t.Fatalf("#775 RED: the harness returned and GETs before this watcher's own RBAC snapshot was published "+
			"(rebuilds parked: %d) — the group grant reads as denied and the GET falls through (Err=%v)", parked.Load(), res.Err)
	}
	if s := ObjectsGetStatsSnapshot(); s.InformerServed != 1 {
		t.Fatalf("#775: InformerServed want 1; got %d", s.InformerServed)
	}
}

func TestIssue775_P2_GroupGrantOwnsTheLiveSnapshotAfterANeighboursLatePublish(t *testing.T) {
	if rbacNarrowBaseline {
		t.Skip("group-grant assertion is Tag-0.30.101 only")
	}
	resetServeCounters()
	landed := false
	ownRBACPublishLandedHook = func() {
		foreign := &cache.RBACSnapshot{PublishSeq: 1 << 40}
		cache.RebuildSubjectIndexesForTest(foreign) // empty indexes: grants nobody
		cache.PublishRBACSnapshotForTest(foreign)
		landed = true
	}
	t.Cleanup(func() { ownRBACPublishLandedHook = nil })

	newGroupGrantWatcher(t)
	if !landed {
		t.Fatal("#775 P2 precondition: the neighbour's snapshot never landed — the harness did not reach its post-publish step")
	}

	res := Get(ctxWithUser("alice", "devs"), serveTestRef(authorizedNS, "obj-authorized"))
	if res.Unstructured == nil {
		t.Fatalf("#775 P2 RED: a neighbour's snapshot published after this watcher's own is still live at the GET — "+
			"the group grant reads as denied and the GET falls through (Err=%v)", res.Err)
	}
	if s := ObjectsGetStatsSnapshot(); s.InformerServed != 1 {
		t.Fatalf("#775 P2: InformerServed want 1; got %d", s.InformerServed)
	}
}
