package cache

import (
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestIssue686_BindingsIndexBuildOnNilSnapshotIsDeferred — the product
// hardening for #686. BuildBindingsByGVRIndex used to set built=true over a
// nil snapshot, leaving byRole empty. The delta hooks were then ACTIVE, but
// onRoleRulesChanged found no referencing binding, so a later role-rule
// edit bumped no subject. Every warm cell kept its key across a revoke
// until TTL. Now the build is DEFERRED: the index stays unbuilt (callers
// keep their pre-build fallbacks), and the first snapshot publish builds
// it, through the real writer (rebuildRBACSnapshot).
func TestIssue686_BindingsIndexBuildOnNilSnapshotIsDeferred(t *testing.T) {
	// The snapshot is process-global: an event-driven rebuild still running
	// for an earlier test's (stopped) watcher would publish ITS snapshot into
	// this arm's nil window and run the deferred build over it. Start only
	// once no rebuild is in flight. Earlier watchers' initial publishes are
	// joined by their rw.Stop.
	deadline := time.Now().Add(time.Minute)
	for rbacRebuildLock.Load() || rbacRebuildDirty.Load() {
		if time.Now().After(deadline) {
			t.Fatal("an RBAC snapshot rebuild is still in flight after 1m")
		}
		time.Sleep(time.Millisecond)
	}
	prev := rbacSnap.Load()
	rbacSnap.Store(nil)
	t.Cleanup(func() { rbacSnap.Store(prev) })
	ResetBindingsByGVRIndexForTest()
	t.Cleanup(ResetBindingsByGVRIndexForTest)

	navigated := []schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "configmaps"}}
	before := BindingsIndexDeferredTotalForTest()
	if n := BuildBindingsByGVRIndex(navigated); n != 0 {
		t.Fatalf("a build over a nil snapshot enrolled %d bindings", n)
	}
	if BindingsByGVRIndexBuilt() {
		t.Fatal("#686: the index claims to be BUILT over a nil snapshot: byRole is empty, so every later role " +
			"edit would bump no subject while the delta hooks run as if the index were live")
	}
	if got := BindingsIndexDeferredTotalForTest() - before; got != 1 {
		t.Fatalf("deferred builds = %d, want 1", got)
	}

	// The first publish, through the real snapshot writer, runs the deferred
	// build over the published snapshot.
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
	// The deferred build runs over the first COMPLETE snapshot: the one the
	// initial publish (waitAndPublishInitialRBACSnapshot) stores. An
	// event-driven rebuild during the initial LIST can publish a partial
	// snapshot (the RoleBinding before its Role); building over that would
	// skip the binding for good, which is what the first version of this
	// arm caught under contention (byRole empty, 12-18 in 50).
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}

	if !BindingsByGVRIndexBuilt() {
		t.Fatal("#686: the first publish did not run the deferred bindings-index build")
	}
	idx := bindingsByGVRSingleton()
	idx.mu.RLock()
	byRole, pending := len(idx.byRole), idx.pendingNavigated
	idx.mu.RUnlock()
	if byRole == 0 {
		t.Fatal("#686: the deferred build ran but byRole is empty: the role edit would still bump no one")
	}
	if pending != nil {
		t.Fatalf("the deferred navigated set was not cleared after the build: %v", pending)
	}
}

// TestIssue686_DeferredFirstBuildRotatesTheEpochExactlyOnce reconciles #686
// with #685 (#682's one-shot boot-window rotation). A pre-build RBAC edit
// whose note is taken while the snapshot is still nil (the publish seq is
// stamped before rbacSnap.Store) must rotate the epoch exactly once: at the
// REAL first build (the deferred one), never at a deferral, never again
// later. Rotating at the deferral turned #685's E3 into a per-pass global
// rotation (reviewer-small, #689 gate on main 57e48a5e).
func TestIssue686_DeferredFirstBuildRotatesTheEpochExactlyOnce(t *testing.T) {
	reset682(t)
	rbacSnap.Store(nil) // the window: publish seq stamped, snapshot not stored yet
	BumpRBACGenForTest()
	roleHandler682.OnUpdate(role682(noRule682), role682(cmRule682)) // a pre-build edit: the note
	nav := []schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "configmaps"}}
	for i := 0; i < 3; i++ {
		BuildBindingsByGVRIndex(nav) // deferred: no snapshot
		if got := RBACSubGenEpoch(); got != 0 {
			t.Fatalf("deferral %d rotated the epoch to %d: a deferral is not the first build", i, got)
		}
	}
	rbacSnap.Store(&RBACSnapshot{})
	buildPendingBindingsIndex() // the first publish runs the deferred build
	if !BindingsByGVRIndexBuilt() {
		t.Fatal("the deferred build did not run at the first publish")
	}
	if got := RBACSubGenEpoch(); got != 1 {
		t.Fatalf("epoch=%d after the deferred first build, want exactly 1 (#682 boot-window rotation)", got)
	}
	BuildBindingsByGVRIndex(nav)
	buildPendingBindingsIndex()
	if got := RBACSubGenEpoch(); got != 1 {
		t.Fatalf("epoch=%d after a later rebuild, want 1: later builds never rotate", got)
	}
}

// TestIssue686_GVRDiscoveredWhileDeferredIsNotLost is reviewer-small's
// falsifier from the #689 gate. A GVR discovered (AddNavigatedGVR) while the
// index build is DEFERRED must survive the deferred build. That build resets
// navigated to its own set. Without carrying the GVR into pendingNavigated,
// no binding granting it would ever be enrolled: byGVR stays empty and its
// narrow cohorts are skipped forever. RED on 9ad0ea55, 10/10.
func TestIssue686_GVRDiscoveredWhileDeferredIsNotLost(t *testing.T) {
	deadline := time.Now().Add(time.Minute)
	for rbacRebuildLock.Load() || rbacRebuildDirty.Load() {
		if time.Now().After(deadline) {
			t.Fatal("an RBAC snapshot rebuild is still in flight after 1m")
		}
		time.Sleep(time.Millisecond)
	}
	prev := rbacSnap.Load()
	rbacSnap.Store(nil)
	t.Cleanup(func() { rbacSnap.Store(prev) })
	ResetBindingsByGVRIndexForTest()
	t.Cleanup(ResetBindingsByGVRIndexForTest)

	BuildBindingsByGVRIndex([]schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "configmaps"}})
	if BindingsByGVRIndexBuilt() {
		t.Fatal("precondition: the build over a nil snapshot must be deferred")
	}
	panels := schema.GroupVersionResource{Group: "widgets.example.io", Version: "v1", Resource: "panels"}
	AddNavigatedGVR(panels) // discovered while the build is deferred

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
		t.Fatal("the first publish did not run the deferred build")
	}
	idx := bindingsByGVRSingleton()
	idx.mu.RLock()
	_, navigated := idx.navigated[grFromGVR(panels)]
	enrolled := len(idx.byGVR[grFromGVR(panels)])
	idx.mu.RUnlock()
	if !navigated || enrolled == 0 {
		t.Fatalf("#686: the GVR discovered while the build was deferred was lost: navigated=%v byGVR[panels]=%d", navigated, enrolled)
	}
}
