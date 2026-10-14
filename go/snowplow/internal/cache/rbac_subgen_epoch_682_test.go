package cache

// rbac_subgen_epoch_682_test.go — #682: a role-rule edit that lands between the
// first RBAC publish and the first bindings-by-GVR index build rotates every
// per-identity key exactly once, at that build.
//
//   E1  window: snapshot published, index NOT built, a role-rule edit arrives
//       through the REAL RBAC informer handler (newRBACSnapshotHandler → the
//       #257 hook), then the first build → the epoch bumps and every
//       identity-bound class key rotates. RED on main (the hook returned early
//       and nothing ever rotated).
//   E1b a non-initial Role ADD and a binding roleRef change pre-build rotate too.
//   E2  control: an edit BEFORE the first publish → no bump.
//   E2b control: an initial-list ADD delivered AFTER the first publish (informer
//       handlers lag HasSynced) → no bump. #216's harness hits exactly this.
//   E2c control: binding add / delete / subject edits pre-build → no bump: they
//       move the SubjectBindingSet digest, which already rotates the key.
//   E3  N later rebuilds, each with RBAC events landing concurrently, never
//       bump (the per-pass global rotation that would make every cell cold).
//   E4  an edit AFTER the first build rotates through the normal #257 path
//       (the subject's sub-gen) and does NOT touch the epoch.
//   E5  identity-free classes do NOT rotate on a bump.

import (
	"sync"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

// The real handler wrapper over the real hooks (no watcher needed: the hooks
// are package functions; scheduleRBACRebuild is left out).
var (
	roleHandler682 = newRBACSnapshotHandler(clientcache.ResourceEventHandlerFuncs{
		AddFunc:    func(o interface{}) { onRoleObjectChanged(bumpSrcRoleAdd, o) },
		UpdateFunc: onRoleUpdated,
		DeleteFunc: func(o interface{}) { onRoleObjectChanged(bumpSrcRoleDelete, o) },
	}, false)
	bindingHandler682 = newRBACSnapshotHandler(clientcache.ResourceEventHandlerFuncs{
		AddFunc: onBindingAdd, UpdateFunc: onBindingUpdate, DeleteFunc: onBindingDelete,
	}, true)
)

func rb682(role string, subjects ...string) *rbacv1.RoleBinding {
	var s []rbacv1.Subject
	for _, n := range subjects {
		s = append(s, rbacv1.Subject{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: n})
	}
	return &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "rb682", UID: "uid-rb682"},
		Subjects: s, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: role}}
}

func reset682(t *testing.T) {
	t.Helper()
	reset := func() {
		ResetRBACSubGenForTest()
		ResetPendingSubGenBumpsForTest()
		ResetBindingsByGVRIndexForTest()
		ResetRBACGenForTest()
	}
	reset()
	t.Cleanup(reset)
}

func role682(rules ...rbacv1.PolicyRule) *rbacv1.Role {
	return &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "cm"}, Rules: rules}
}

var (
	noRule682  = rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"secrets-none"}, Verbs: []string{"get"}}
	cmRule682  = rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"}}
	identUser  = "alice682"
	identGroup = []string{"team682", "system:authenticated"}
)

// identityKeys682 computes one key per class for the same identity, folding
// RBACSubGen exactly as dispatchCacheLookupKey does.
func identityKeys682() map[string]string {
	sub := RBACSubGenForSubject(identUser, identGroup)
	out := map[string]string{}
	for _, class := range []string{"restactions", "widgets", CacheEntryClassRAFullList, CacheEntryClassApistage, CacheEntryClassWidgetContent} {
		out[class] = ComputeKey(ResolvedKeyInputs{
			CacheEntryClass: class, Group: "templates.krateo.io", Version: "v1", Resource: "restactions",
			Namespace: "krateo-system", Name: "ra682", BindingUID: "C:uid682", SubjectBindingSet: "set682", RBACSubGen: sub,
		})
	}
	return out
}

func TestIssue682_E1_PreBuildRoleEdit_RotatesEveryIdentityBoundKeyAtFirstBuild(t *testing.T) {
	reset682(t)
	BumpRBACGenForTest() // the first snapshot is published
	before := identityKeys682()

	roleHandler682.OnUpdate(role682(noRule682), role682(cmRule682)) // a grant, pre-build
	if RBACSubGenEpoch() != 0 {
		t.Fatal("the epoch moved before the build")
	}
	BuildBindingsByGVRIndex(nil)
	if RBACSubGenEpoch() != 1 {
		t.Fatalf("E1 RED: epoch=%d after the first build following a pre-build edit, want 1 — a cell minted in the window keeps the pre-edit grant", RBACSubGenEpoch())
	}
	after := identityKeys682()
	for _, class := range []string{"restactions", "widgets", CacheEntryClassRAFullList} {
		if before[class] == after[class] {
			t.Errorf("E1 RED: the %s key did not rotate", class)
		}
	}
	// E5 — identity-free classes do not fold RBACSubGen: no rotation.
	for _, class := range []string{CacheEntryClassApistage, CacheEntryClassWidgetContent} {
		if before[class] != after[class] {
			t.Errorf("E5: the identity-free %s key rotated", class)
		}
	}
}

func TestIssue682_E2_EditBeforeFirstPublish_DoesNotRotate(t *testing.T) {
	reset682(t)
	// RBACGen()==0: before the first publish.
	roleHandler682.OnUpdate(role682(noRule682), role682(cmRule682))
	BuildBindingsByGVRIndex(nil)
	if RBACSubGenEpoch() != 0 {
		t.Fatalf("E2: epoch=%d — an event before the first publish (initial sync) rotated every key", RBACSubGenEpoch())
	}
}

func TestIssue682_E3_LaterRebuildsNeverBump(t *testing.T) {
	reset682(t)
	BumpRBACGenForTest()
	roleHandler682.OnUpdate(role682(noRule682), role682(cmRule682))
	BuildBindingsByGVRIndex(nil)
	if RBACSubGenEpoch() != 1 {
		t.Fatalf("precondition: epoch=%d after the first build", RBACSubGenEpoch())
	}
	for i := 0; i < 20; i++ {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); roleHandler682.OnUpdate(role682(cmRule682), role682(noRule682)) }()
		go func() { defer wg.Done(); BuildBindingsByGVRIndex(nil) }()
		wg.Wait()
	}
	if RBACSubGenEpoch() != 1 {
		t.Fatalf("E3 RED: epoch=%d after 20 rebuilds with concurrent RBAC events, want 1 — a per-pass global rotation", RBACSubGenEpoch())
	}
}

func TestIssue682_E4_PostBuildEdit_RotatesThroughSubjectSubGen_NotEpoch(t *testing.T) {
	reset682(t)
	BumpRBACGenForTest()
	// Seed the index with a binding of team682 to the role, so the #257 path
	// knows the role's subjects.
	snap := &RBACSnapshot{
		RoleBindingsByNS: map[string][]*rbacv1.RoleBinding{"team-b": {{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: "rb682", UID: "uid-rb682"},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "team682"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "cm"},
		}}},
		ClusterRolesByName: map[string]*rbacv1.ClusterRole{},
		RolesByNSName:      map[string]*rbacv1.Role{"team-b/cm": role682(noRule682)},
	}
	rbacSnap.Store(snap)
	t.Cleanup(func() { rbacSnap.Store(nil) })
	BuildBindingsByGVRIndex(nil)
	sub0 := RBACSubGenForSubject(identUser, identGroup)

	roleHandler682.OnUpdate(role682(noRule682), role682(cmRule682))
	flushPendingSubGenBumps()
	if RBACSubGenEpoch() != 0 {
		t.Fatalf("E4: a post-build edit moved the epoch (%d)", RBACSubGenEpoch())
	}
	if RBACSubGenForSubject(identUser, identGroup) == sub0 {
		t.Fatal("E4: a post-build role-rule edit did not rotate the subject's sub-gen (#257 path)")
	}
}

func TestIssue682_E1b_PreBuildRoleAddAndRoleRefChange_Rotate(t *testing.T) {
	t.Run("role_add_not_initial", func(t *testing.T) {
		reset682(t)
		BumpRBACGenForTest()
		roleHandler682.OnAdd(role682(cmRule682), false)
		BuildBindingsByGVRIndex(nil)
		if RBACSubGenEpoch() != 1 {
			t.Fatalf("E1b RED: a pre-build Role create (a dangling roleRef now grants) did not rotate (epoch=%d)", RBACSubGenEpoch())
		}
	})
	t.Run("binding_roleref_change", func(t *testing.T) {
		reset682(t)
		BumpRBACGenForTest()
		bindingHandler682.OnUpdate(rb682("old-role", "team682"), rb682("cm", "team682"))
		BuildBindingsByGVRIndex(nil)
		if RBACSubGenEpoch() != 1 {
			t.Fatalf("E1b RED: a pre-build binding roleRef change (digest-invisible) did not rotate (epoch=%d)", RBACSubGenEpoch())
		}
	})
}

func TestIssue682_E2b_LateInitialListAdd_DoesNotRotate(t *testing.T) {
	reset682(t)
	BumpRBACGenForTest()                           // the first publish already happened...
	roleHandler682.OnAdd(role682(cmRule682), true) // ...and an initial-list ADD is delivered late
	BuildBindingsByGVRIndex(nil)
	if RBACSubGenEpoch() != 0 {
		t.Fatalf("E2b RED: a late initial-list ADD rotated every key at the first build (epoch=%d)", RBACSubGenEpoch())
	}
}

func TestIssue682_E2c_BindingDigestPaths_DoNotRotate(t *testing.T) {
	reset682(t)
	BumpRBACGenForTest()
	bindingHandler682.OnAdd(rb682("cm", "team682"), false)
	bindingHandler682.OnUpdate(rb682("cm", "team682"), rb682("cm", "team682", "other682"))
	bindingHandler682.OnDelete(rb682("cm", "team682"))
	BuildBindingsByGVRIndex(nil)
	if RBACSubGenEpoch() != 0 {
		t.Fatalf("E2c: binding add / subject edit / delete rotated globally (epoch=%d) — the SubjectBindingSet digest already covers them", RBACSubGenEpoch())
	}
}
