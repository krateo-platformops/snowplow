package cache

import (
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// #686 asked that the bindings index never claim to be live over nothing: an
// index marked built over a nil snapshot had an empty byRole, so every later
// role edit bumped no subject. Until #1458 the index was BUILT from the
// published snapshot and the answer was a deferral (the build waited for the
// first complete publish). Since #1458 there is no build: the event hooks
// alone write the index's membership, from the informers' first event on, and
// the index is READY once the RBAC handlers have been delivered their initial
// lists (markBindingsIndexReady, from the initial-publish goroutine). These
// arms pin #686's property in that shape.

// wait686NoRebuild waits until no RBAC snapshot rebuild is in flight: one still
// running for an earlier test's (stopped) watcher would publish its snapshot
// into this arm's window.
func wait686NoRebuild(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for rbacRebuildLock.Load() || rbacRebuildDirty.Load() {
		if time.Now().After(deadline) {
			t.Fatal("an RBAC snapshot rebuild is still in flight after 1m")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestIssue686_IndexIsNotReadyBeforeTheHandlersAreDelivered — setting the
// navigated set (the readiness settle, a prewarm pass) never makes the index
// ready, with or without a snapshot; a watcher's initial publish does, and by
// then the index holds the binding the informer delivered.
func TestIssue686_IndexIsNotReadyBeforeTheHandlersAreDelivered(t *testing.T) {
	wait686NoRebuild(t)
	prev := rbacSnap.Load()
	rbacSnap.Store(nil)
	t.Cleanup(func() { rbacSnap.Store(prev) })
	ResetBindingsByGVRIndexForTest()
	t.Cleanup(ResetBindingsByGVRIndexForTest)

	navigated := []schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "configmaps"}}
	if n := SetNavigatedGVRs(navigated); n != 0 {
		t.Fatalf("an empty index reports %d bindings", n)
	}
	if BindingsByGVRIndexBuilt() {
		t.Fatal("#686: the index claims to be READY over a nil snapshot and no delivered binding: byRole is empty, " +
			"so every later role edit would bump no subject")
	}
	rbacSnap.Store(&RBACSnapshot{})
	SetNavigatedGVRs(navigated)
	if BindingsByGVRIndexBuilt() {
		t.Fatal("#686/#1458: setting the navigated set made the index ready: only the delivery of the RBAC " +
			"handlers' initial lists may")
	}
	rbacSnap.Store(nil)

	role := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a", Name: "reader"},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list"}}},
	}
	rb := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a", Name: "reader-bind", UID: "rb-686"},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "u-686"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "reader"},
	}
	rw := newSnapshotTestWatcher(t, role, rb)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}

	if !BindingsByGVRIndexBuilt() {
		t.Fatal("#686: the watcher's initial publish returned and the index is not ready")
	}
	idx := bindingsByGVRSingleton()
	idx.mu.RLock()
	byRole := len(idx.byRole["R/tenant-a/reader"])
	bucket := len(idx.byGVR[grFromGVR(navigated[0])])
	idx.mu.RUnlock()
	if byRole != 1 {
		t.Fatalf("#686: the index is ready and holds %d bindings under the role, want the 1 the informer delivered: "+
			"the role edit would bump no one", byRole)
	}
	if bucket != 1 {
		t.Fatalf("the navigated set given before ready was lost: byGVR[configmaps]=%d, want 1", bucket)
	}
}

// TestIssue686_ReadyRotatesTheEpochExactlyOnce reconciles #686 with #685
// (#682's one-shot boot-window rotation). An RBAC edit noted before ready
// rotates the epoch exactly once: at ready, never when the navigated set is
// set, never again later.
func TestIssue686_ReadyRotatesTheEpochExactlyOnce(t *testing.T) {
	reset682(t)
	rbacSnap.Store(nil) // the window: publish seq stamped, snapshot not stored yet
	BumpRBACGenForTest()
	roleHandler682.OnUpdate(role682(noRule682), role682(cmRule682)) // an edit before ready: the note
	nav := []schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "configmaps"}}
	for i := 0; i < 3; i++ {
		SetNavigatedGVRs(nav)
		if got := RBACSubGenEpoch(); got != 0 {
			t.Fatalf("setting the navigated set (%d) rotated the epoch to %d: it is not the ready transition", i, got)
		}
	}
	rbacSnap.Store(&RBACSnapshot{})
	markBindingsIndexReady()
	if !BindingsByGVRIndexBuilt() {
		t.Fatal("markBindingsIndexReady did not make the index ready")
	}
	if got := RBACSubGenEpoch(); got != 1 {
		t.Fatalf("epoch=%d after ready, want exactly 1 (#682 boot-window rotation)", got)
	}
	SetNavigatedGVRs(nav)
	markBindingsIndexReady()
	if got := RBACSubGenEpoch(); got != 1 {
		t.Fatalf("epoch=%d after a later pass and a repeated ready, want 1: nothing rotates after ready", got)
	}
}

// TestIssue686_GVRDiscoveredBeforeReadyIsNotLost — a GVR discovered
// (AddNavigatedGVR) before the index is ready must have its bindings enrolled
// once it is. RED when the ready transition does not derive the buckets.
func TestIssue686_GVRDiscoveredBeforeReadyIsNotLost(t *testing.T) {
	wait686NoRebuild(t)
	prev := rbacSnap.Load()
	rbacSnap.Store(nil)
	t.Cleanup(func() { rbacSnap.Store(prev) })
	ResetBindingsByGVRIndexForTest()
	t.Cleanup(ResetBindingsByGVRIndexForTest)

	SetNavigatedGVRs([]schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "configmaps"}})
	panels := schema.GroupVersionResource{Group: "widgets.example.io", Version: "v1", Resource: "panels"}
	AddNavigatedGVR(panels) // discovered before ready, with no snapshot

	role := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a", Name: "panel-reader"},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"widgets.example.io"}, Resources: []string{"panels"}, Verbs: []string{"get", "list"}}},
	}
	rb := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a", Name: "panel-reader-bind", UID: "rb-686-panels"},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "u-686"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "panel-reader"},
	}
	rw := newSnapshotTestWatcher(t, role, rb)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	if !BindingsByGVRIndexBuilt() {
		t.Fatal("the watcher's initial publish returned and the index is not ready")
	}
	idx := bindingsByGVRSingleton()
	idx.mu.RLock()
	_, navigated := idx.navigated[grFromGVR(panels)]
	enrolled := len(idx.byGVR[grFromGVR(panels)])
	idx.mu.RUnlock()
	if !navigated || enrolled == 0 {
		t.Fatalf("#686: the GVR discovered before ready was lost: navigated=%v byGVR[panels]=%d", navigated, enrolled)
	}
}
