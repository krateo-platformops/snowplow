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
