package cache

// rbac_subgen_epoch_682_test.go — #682: a role-rule edit that lands between the
// first RBAC publish and the first bindings-by-GVR index build rotates every
// per-identity key exactly once, at that build.
//
//   E1  window: snapshot published, index NOT built, a role-rule edit arrives
//       (the real onRoleUpdated hook), then the first build → the epoch bumps
//       and every identity-bound class key rotates. RED on main (the hook
//       returned early and nothing ever rotated).
//   E2  control: an edit BEFORE the first publish (initial-list shape) → the
//       first build does not bump.
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
)

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

	onRoleUpdated(role682(noRule682), role682(cmRule682)) // a grant, pre-build
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
	// RBACGen()==0: the informers' initial-list shape.
	onRoleUpdated(role682(noRule682), role682(cmRule682))
	BuildBindingsByGVRIndex(nil)
	if RBACSubGenEpoch() != 0 {
		t.Fatalf("E2: epoch=%d — an event before the first publish (initial sync) rotated every key", RBACSubGenEpoch())
	}
}

func TestIssue682_E3_LaterRebuildsNeverBump(t *testing.T) {
	reset682(t)
	BumpRBACGenForTest()
	onRoleUpdated(role682(noRule682), role682(cmRule682))
	BuildBindingsByGVRIndex(nil)
	if RBACSubGenEpoch() != 1 {
		t.Fatalf("precondition: epoch=%d after the first build", RBACSubGenEpoch())
	}
	for i := 0; i < 20; i++ {
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); onRoleUpdated(role682(cmRule682), role682(noRule682)) }()
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

	onRoleUpdated(role682(noRule682), role682(cmRule682))
	flushPendingSubGenBumps()
	if RBACSubGenEpoch() != 0 {
		t.Fatalf("E4: a post-build edit moved the epoch (%d)", RBACSubGenEpoch())
	}
	if RBACSubGenForSubject(identUser, identGroup) == sub0 {
		t.Fatal("E4: a post-build role-rule edit did not rotate the subject's sub-gen (#257 path)")
	}
}
