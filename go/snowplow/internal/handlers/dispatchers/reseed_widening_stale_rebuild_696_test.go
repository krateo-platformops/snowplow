package dispatchers

// reseed_widening_stale_rebuild_696_test.go — #696: the s258 fixture must not
// let an EARLIER test's stopped watcher republish the RBAC snapshot over its
// own.
//
// THE MECHANISM. The published RBAC snapshot is process-global, and
// scheduleRBACRebuild's goroutine is detached by design (rw.Stop does not join
// it). A rebuild still in flight for a stopped watcher holds the single writer
// slot, so the live watcher's binding ADD only marks dirty; the in-flight
// goroutine then loops, rebuilds from the STOPPED watcher's indexers and
// publishes that snapshot over the live one. A subject bound only on the live
// watcher then derives an empty BindingUID. In whole-package runs this was
// TestS258_WideningSubjectAppearsInReseedSet's "live widgets key with
// non-empty BindingUID" precondition; alone it never failed.
//
// THIS ARM makes the window certain: the rebuild barrier holds the stopped
// watcher's rebuild in the writer slot and releases it at the worst moment.
// With s258BuildWatcher's quiesce, the stale rebuild runs to completion
// BEFORE the live watcher exists and the live binding survives. Without it:
// RED, carol's BindingUID reads "" after the stale publish.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

func s696CreateCRB(t *testing.T, dyn dynamic.Interface, user string) {
	t.Helper()
	crb := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "s258-" + user, UID: types.UID("uid-s258-" + user)},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: user}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "s258-reader"},
	}
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(crb)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	if _, err := dyn.Resource(s258CRBGVR).Create(context.Background(), &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create %s's binding: %v", user, err)
	}
}

// s696BindingUID derives user's widgets-key BindingUID the way the reseed
// precondition does (reseedWidgetKey), without failing on an empty one.
func s696BindingUID(user string) string {
	e := reseedWidgetEntry()
	ctx := s258IdentityCtx(user)
	_, _, inputs := dispatchCacheLookupKey(ctx, "widgets", e.GVR.Group, e.GVR.Version, e.GVR.Resource,
		h1NS, h1WName, -1, -1, effectiveKeyExtras(ctx, e.W.Object, nil))
	if inputs == nil {
		return ""
	}
	return inputs.BindingUID
}

func TestS258Fixture_AStoppedWatchersRebuildCannotRepublishOverTheLiveOne(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{})
	var held, released atomic.Bool
	releaseStale := func() {
		if released.CompareAndSwap(false, true) {
			close(release)
		}
	}
	defer cache.SetRebuildBarrierForTest(nil)
	defer releaseStale()

	// The "earlier test": a watcher whose event-driven rebuild is caught in
	// the writer slot, then stopped by its own cleanup when the subtest ends.
	t.Run("stale", func(t *testing.T) {
		staleDyn, _ := s258BuildWatcher(t)
		cache.SetRebuildBarrierForTest(func() {
			if held.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		})
		s696CreateCRB(t, staleDyn, "dave")
		select {
		case <-entered:
		case <-time.After(10 * time.Second):
			t.Fatal("precondition: the stale watcher's rebuild never reached the barrier")
		}
	})

	// Release the stale rebuild after a fixed delay. A fixture WITH the
	// quiesce is parked waiting for exactly this release, so the stale publish
	// lands before the live watcher exists. A fixture without it builds at
	// once, carol's live binding becomes visible, and the stale publish then
	// lands on top of it. (A timer, not a poll of the live state, so nothing
	// reads the fixture's globals while it is being built.)
	time.AfterFunc(2*time.Second, releaseStale)
	dyn, rw := s258BuildWatcher(t)
	stubWidgetResolve(t)
	s696CreateCRB(t, dyn, s258WideningUser)
	deadline := time.Now().Add(10 * time.Second)
	for s696BindingUID(s258WideningUser) == "" {
		if time.Now().After(deadline) {
			t.Fatal("precondition: carol never derived a BindingUID from the live watcher")
		}
		cache.RebuildRBACSnapshotForTest(rw)
		time.Sleep(20 * time.Millisecond)
	}
	for !released.Load() {
		time.Sleep(5 * time.Millisecond)
	}
	if err := cache.WaitRBACRebuildQuiesceForTest(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	if got := s696BindingUID(s258WideningUser); got == "" {
		t.Fatal("#696: a stopped watcher's in-flight rebuild republished its snapshot over the live one; " +
			"carol's binding is gone and her widgets key derives an empty BindingUID")
	}
}
