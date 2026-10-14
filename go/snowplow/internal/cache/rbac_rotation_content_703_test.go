// rbac_rotation_content_703_test.go — #703 (security): a sub-gen key rotation
// is never published before a snapshot whose CONTENT includes the change that
// caused it.
//
// rebuildRBACSnapshot reads the RBAC indexers, builds, Stores, then bumps the
// pending subjects. Pre-#703 it drained the pending set at the bump, so an
// event recorded between the read and the bump was rotated against a snapshot
// built without it. On a revoke: new key + pre-revoke grant, cached until TTL.
//
// Both arms drive the real informer handlers over a fake cluster and park a
// real rebuild with a test hook, so the interleaving is certain:
//
//	(a) parked AFTER the indexer read, BEFORE the Store: the event's change is
//	    not in the snapshot being published, so its subject must NOT rotate
//	    with it. Grant and revoke. RED on the pre-#703 order.
//	(b) parked AFTER the pending set is taken, BEFORE the read: the change IS
//	    in the snapshot, but its bump was recorded after the take, so it
//	    rotates one rebuild late. It must still rotate (no lost bump), exactly
//	    once.
//
// Every assertion is read at the moment the subject's rotation is announced
// (the RBAC-shift hook, fired right after BumpSubjectSubGens on the writer
// goroutine): the snapshot live at that instant is the one the new key pairs
// with.

package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

const i703User = "carol"

var i703CRBGVR = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}

func i703CreateCRB(t *testing.T, dyn dynamic.Interface, name, user string) {
	t.Helper()
	crb := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name)},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: user}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "reader"},
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(crb)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if _, err := dyn.Resource(i703CRBGVR).Create(context.Background(), &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
}

func i703DeleteCRB(t *testing.T, dyn dynamic.Interface, name string) {
	t.Helper()
	if err := dyn.Resource(i703CRBGVR).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete %s: %v", name, err)
	}
}

// i703SnapGrants reports whether the LIVE snapshot binds user.
func i703SnapGrants(user string) bool {
	s := LiveRBACSnapshot()
	if s == nil {
		return false
	}
	for _, crb := range s.ClusterRoleBindings {
		for _, sub := range crb.Subjects {
			if sub.Kind == "User" && sub.Name == user {
				return true
			}
		}
	}
	return false
}

func i703Pending(user string) bool {
	pendingSubGenBumps.mu.Lock()
	defer pendingSubGenBumps.mu.Unlock()
	_, ok := pendingSubGenBumps.set[subjectKey{Kind: "User", Name: user}]
	return ok
}

// i703Rotation is one announced rotation of a subject.
type i703Rotation struct {
	user       string
	snapGrants bool   // does the snapshot live at the rotation bind carol?
	seq        uint64 // that snapshot's PublishSeq
}

type i703Recorder struct {
	mu   sync.Mutex
	rots []i703Rotation
}

func (r *i703Recorder) hook(rs RotatedSubjectSet) {
	s := LiveRBACSnapshot()
	var seq uint64
	if s != nil {
		seq = s.PublishSeq
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, u := range []string{i703User, "dave"} {
		if rs.Rotated(u, nil) {
			r.rots = append(r.rots, i703Rotation{user: u, snapGrants: i703SnapGrants(i703User), seq: seq})
		}
	}
}

func (r *i703Recorder) of(user string) []i703Rotation {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []i703Rotation
	for _, x := range r.rots {
		if x.user == user {
			out = append(out, x)
		}
	}
	return out
}

// i703Setup builds a real watcher (optionally with carol already bound),
// publishes, builds the bindings index (which arms the delta hooks that record
// sub-gen bumps) and installs the rotation recorder.
func i703Setup(t *testing.T, carolBound bool) (*ResourceWatcher, dynamic.Interface, *i703Recorder) {
	t.Helper()
	rw, dyn := realWatcher(t)
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })
	if err := rw.WaitRBACQuiescentForTest(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	if carolBound {
		i703CreateCRB(t, dyn, "grant-carol", i703User)
		i703Wait(t, "carol's binding in the published snapshot", func() bool {
			publishRBACSnapshotUnderSlot(rw)
			return i703SnapGrants(i703User)
		})
		if err := WaitRBACRebuildQuiesceForTest(10 * time.Second); err != nil {
			t.Fatal(err)
		}
	}
	publishRBACSnapshotUnderSlot(rw)
	if got := i703SnapGrants(i703User); got != carolBound {
		t.Fatalf("precondition: snapshot binds carol=%v, want %v", got, carolBound)
	}
	BuildBindingsByGVRIndex([]schema.GroupVersionResource{{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}})
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	ResetPendingSubGenBumpsForTest()

	rec := &i703Recorder{}
	ResetRBACShiftHooksForTest()
	t.Cleanup(ResetRBACShiftHooksForTest)
	RegisterRBACShiftHook(rec.hook)
	return rw, dyn, rec
}

// i703Park installs hook to park the FIRST rebuild that reaches it; returns
// (entered, release).
func i703Park(t *testing.T, slot *atomic.Pointer[func()]) (<-chan struct{}, func()) {
	t.Helper()
	entered := make(chan struct{})
	release := make(chan struct{})
	var first, released atomic.Bool
	h := func() {
		if first.CompareAndSwap(false, true) {
			close(entered)
			<-release
		}
	}
	slot.Store(&h)
	rel := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	t.Cleanup(func() {
		slot.Store(nil)
		rel()
	})
	return entered, rel
}

func i703Wait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestIssue703_EventBetweenReadAndFlushDoesNotRotateWithThatSnapshot — arm (a).
func TestIssue703_EventBetweenReadAndFlushDoesNotRotateWithThatSnapshot(t *testing.T) {
	for _, tc := range []struct {
		name       string
		carolBound bool // before the event
		event      func(t *testing.T, dyn dynamic.Interface)
	}{
		{"revoke", true, func(t *testing.T, dyn dynamic.Interface) { i703DeleteCRB(t, dyn, "grant-carol") }},
		{"grant", false, func(t *testing.T, dyn dynamic.Interface) { i703CreateCRB(t, dyn, "grant-carol", i703User) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer withCleanDepWatch(t)()
			_, dyn, rec := i703Setup(t, tc.carolBound)
			entered, release := i703Park(t, &rebuildPreStoreHookForTest)
			defer release() // before withCleanDepWatch's deferred stop joins the writer

			// An unrelated binding starts a rebuild; it reads the indexers
			// (carol unchanged) and parks before its Store.
			i703CreateCRB(t, dyn, "grant-dave", "dave")
			select {
			case <-entered:
			case <-time.After(10 * time.Second):
				t.Fatal("precondition: the rebuild never reached the pre-Store hook")
			}
			// carol's change lands in the indexer and her bump is recorded
			// while that snapshot, built without it, waits to be published.
			tc.event(t, dyn)
			i703Wait(t, "carol's bump to be recorded", func() bool { return i703Pending(i703User) })

			release()
			if err := WaitRBACRebuildQuiesceForTest(10 * time.Second); err != nil {
				t.Fatal(err)
			}

			want := !tc.carolBound // the post-change state
			rots := rec.of(i703User)
			if len(rots) != 1 {
				t.Fatalf("carol rotated %d times, want exactly 1 (no lost bump, no double bump): %+v", len(rots), rots)
			}
			if rots[0].snapGrants != want {
				t.Fatalf("#703 (%s): carol's key rotated with snapshot seq %d whose content still has "+
					"carol bound=%v — the new key is paired with the PRE-change verdict, and a cell cached "+
					"under it survives until TTL", tc.name, rots[0].seq, rots[0].snapGrants)
			}
			if got := i703SnapGrants(i703User); got != want {
				t.Fatalf("final snapshot binds carol=%v, want %v", got, want)
			}
		})
	}
}

// TestIssue703_BumpRecordedAfterTheTakeRotatesAtTheNextRebuild — arm (b).
func TestIssue703_BumpRecordedAfterTheTakeRotatesAtTheNextRebuild(t *testing.T) {
	defer withCleanDepWatch(t)()
	_, dyn, rec := i703Setup(t, false)
	// Park the first rebuild whose TAKE holds dave's bump (the rebuild his
	// event spawns can take before his record lands; that one passes).
	entered := make(chan struct{})
	release := make(chan struct{})
	var parked, released atomic.Bool
	releaseFn := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	dave := subjectKey{Kind: "User", Name: "dave"}
	h := func(taken takenSubGenBumps) {
		for _, s := range taken.subjects {
			if s == dave && parked.CompareAndSwap(false, true) {
				close(entered)
				<-release
				return
			}
		}
	}
	rebuildPostTakeHookForTest.Store(&h)
	t.Cleanup(func() {
		rebuildPostTakeHookForTest.Store(nil)
		releaseFn()
	})
	defer releaseFn() // before withCleanDepWatch's deferred stop joins the writer

	// dave's binding starts a rebuild that takes the pending set (dave) and
	// parks BEFORE its indexer read.
	i703CreateCRB(t, dyn, "grant-dave", "dave")
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("precondition: the rebuild never reached the post-take hook")
	}
	// carol's binding is written to the indexer (so the parked rebuild's read
	// WILL see it) and her bump is recorded after the take.
	i703CreateCRB(t, dyn, "grant-carol", i703User)
	i703Wait(t, "carol's bump to be recorded", func() bool { return i703Pending(i703User) })

	releaseFn()
	if err := WaitRBACRebuildQuiesceForTest(10 * time.Second); err != nil {
		t.Fatal(err)
	}

	daves := rec.of("dave")
	if len(daves) != 1 {
		t.Fatalf("precondition: dave rotated %d times, want 1: %+v", len(daves), daves)
	}
	carols := rec.of(i703User)
	if len(carols) != 1 {
		t.Fatalf("carol rotated %d times, want exactly 1 — a bump recorded after the take must be "+
			"flushed by the re-armed rebuild, never lost: %+v", len(carols), carols)
	}
	if !carols[0].snapGrants {
		t.Fatalf("carol rotated with snapshot seq %d that does not bind her", carols[0].seq)
	}
	if carols[0].seq <= daves[0].seq {
		t.Fatalf("carol rotated with the parked rebuild's snapshot (seq %d, dave's seq %d): her bump "+
			"was recorded after that rebuild's take, so it belongs to the NEXT rebuild", carols[0].seq, daves[0].seq)
	}
}

// TestIssue703_ARebuildThatPanicsAfterTheTakeLosesNoBump — the take moved the
// drain to the TOP of the rebuild, so a rebuild that dies before its flush
// would otherwise discard the bumps it had taken. They are put back (and dirty
// re-marked), and the retried rebuild rotates them, once, with a snapshot that
// holds the change.
func TestIssue703_ARebuildThatPanicsAfterTheTakeLosesNoBump(t *testing.T) {
	defer withCleanDepWatch(t)()
	_, dyn, rec := i703Setup(t, false)
	dave := subjectKey{Kind: "User", Name: "dave"}
	var panicked atomic.Bool
	h := func(taken takenSubGenBumps) {
		for _, s := range taken.subjects {
			if s == dave && panicked.CompareAndSwap(false, true) {
				panic("#703 arm: the rebuild dies after the take")
			}
		}
	}
	rebuildPostTakeHookForTest.Store(&h)
	t.Cleanup(func() { rebuildPostTakeHookForTest.Store(nil) })

	i703CreateCRB(t, dyn, "grant-dave", "dave")
	i703Wait(t, "the rebuild holding dave's bump to panic", panicked.Load)
	i703Wait(t, "dave's rotation after the retry", func() bool { return len(rec.of("dave")) > 0 })
	if err := WaitRBACRebuildQuiesceForTest(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	daves := rec.of("dave")
	if len(daves) != 1 {
		t.Fatalf("dave rotated %d times after a panicked rebuild, want exactly 1: %+v", len(daves), daves)
	}
	if s := LiveRBACSnapshot(); s == nil || daves[0].seq == 0 {
		t.Fatalf("dave's rotation carried no published snapshot")
	}
}
