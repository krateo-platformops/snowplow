package cache

import (
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
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
	byRole := len(idx.membersByRole["R/tenant-a/reader"])
	idx.mu.RUnlock()
	if byRole != 1 {
		t.Fatalf("#686: the index is ready and holds %d bindings under the role, want the 1 the informer delivered: "+
			"the role edit would bump no one", byRole)
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

// TestIssue686_GVRDiscoveredBeforeReadyIsNotLost — a navigated set given
// (SetNavigatedGVRs) and a GVR discovered (AddNavigatedGVR) before the index is
// ready must have their bindings enrolled once it is. The hooks are driven
// directly, in the one order that leaves the READY TRANSITION as the only thing
// that can enrol them: the bindings are delivered while no snapshot holds their
// roles (so their ADD buckets nothing), the roles are published, and no role
// event is handled (the role hooks do nothing before ready). RED when the ready
// transition does not derive the buckets.
func TestIssue686_GVRDiscoveredBeforeReadyIsNotLost(t *testing.T) {
	wait686NoRebuild(t)
	prev := rbacSnap.Load()
	rbacSnap.Store(nil)
	t.Cleanup(func() { rbacSnap.Store(prev) })
	ResetBindingsByGVRIndexForTest()
	t.Cleanup(ResetBindingsByGVRIndexForTest)

	configmaps := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	panels := schema.GroupVersionResource{Group: "widgets.example.io", Version: "v1", Resource: "panels"}
	SetNavigatedGVRs([]schema.GroupVersionResource{configmaps}) // before ready, with no snapshot
	AddNavigatedGVR(panels)                                     // discovered before ready, with no snapshot

	role := func(name, group, resource string) *rbacv1.Role {
		return &rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a", Name: name},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{group}, Resources: []string{resource}, Verbs: []string{"get", "list"}}},
		}
	}
	binding := func(name, roleName string) *rbacv1.RoleBinding {
		return &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: "tenant-a", Name: name, UID: types.UID("uid-686-" + name)},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "u-686"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: roleName},
		}
	}
	cmBinding, panelBinding := binding("cm-bind", "cm-reader"), binding("panel-bind", "panel-reader")
	// The bindings are delivered first: no snapshot, so no rules, so no bucket.
	onBindingAdd(cmBinding)
	onBindingAdd(panelBinding)
	bucketed := func(gvr schema.GroupVersionResource) int {
		idx := bindingsByGVRSingleton()
		idx.mu.RLock()
		defer idx.mu.RUnlock()
		return len(idx.byGVR[grFromGVR(gvr)])
	}
	if bucketed(configmaps) != 0 || bucketed(panels) != 0 {
		t.Fatalf("SETUP: a binding delivered with no snapshot is already in a bucket")
	}
	// The complete snapshot is published; the roles' own events are not handled
	// before ready.
	rbacSnap.Store(&RBACSnapshot{
		Synced:           true,
		RoleBindingsByNS: map[string][]*rbacv1.RoleBinding{"tenant-a": {cmBinding, panelBinding}},
		RolesByNSName: map[string]*rbacv1.Role{
			"tenant-a/cm-reader":    role("cm-reader", "", "configmaps"),
			"tenant-a/panel-reader": role("panel-reader", "widgets.example.io", "panels"),
		},
	})
	onRoleObjectChanged(bumpSrcRoleAdd, role("cm-reader", "", "configmaps"))
	onRoleObjectChanged(bumpSrcRoleAdd, role("panel-reader", "widgets.example.io", "panels"))
	if bucketed(configmaps) != 0 || bucketed(panels) != 0 {
		t.Fatalf("SETUP: a role event handled before ready bucketed a binding")
	}

	markBindingsIndexReady()

	if !BindingsByGVRIndexBuilt() {
		t.Fatal("markBindingsIndexReady did not make the index ready")
	}
	if got := bucketed(configmaps); got != 1 {
		t.Fatalf("#686: the navigated set given before ready was lost: byGVR[configmaps]=%d, want 1", got)
	}
	if got := bucketed(panels); got != 1 {
		t.Fatalf("#686: the GVR discovered before ready was lost: byGVR[panels]=%d, want 1", got)
	}
}
