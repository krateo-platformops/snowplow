// rbac_role_rules_skip_falsifier_257_test.go — #257 §3: the SKIP proven at the
// FULL informer-handler boundary (arch C4 condition ii). Each arm drives the
// REAL rbacSnapshotEventHandlers().UpdateFunc with a real oldObj (the object the
// UpdateFunc drops today, rbac_snapshot.go), registers a binding referencing the
// role so a bump has a subject to rotate, and asserts the referencing subject's
// sub-gen bump fires (rule change) or is SKIPPED (rules unchanged).
//
// RED on today's code: a labels-only role UPDATE bumps unconditionally
// (bumps_total +N). GREEN post-fix: rules-equal ⇒ +0; rule change ⇒ +N.
// Control matrix (return-true/return-false mutants of policyRulesEqual) lives
// alongside — a `return true` fails the grant/revoke arms, a `return false`
// fails the no-op arms.

package cache

import (
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func crbRefRole(name, rv, roleName string, subs ...rbacv1.Subject) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name), ResourceVersion: rv},
		Subjects:   subs,
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: roleName},
	}
}

func clusterRoleObj(name, rv string, rules ...rbacv1.PolicyRule) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: rv}, Rules: rules}
}

// driveRoleUpdate drives the REAL informer UpdateFunc for the ClusterRoles GVR
// with old,new, then flushes deterministically. Returns the bumps_total delta.
func driveRoleUpdate(t *testing.T, old, new *rbacv1.ClusterRole) uint64 {
	t.Helper()
	rw := &ResourceWatcher{mode: modePassthrough}
	h := rw.rbacSnapshotEventHandlers(clusterRolesTypedGVR)
	before := RBACSubGenBumpsTotal()
	h.UpdateFunc(old, new)
	flushPendingSubGenBumps()
	return RBACSubGenBumpsTotal() - before
}

func setupRoleSkipArm(t *testing.T) {
	t.Helper()
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)
	// A binding referencing ClusterRole "skip-arm-role" so onRoleRulesChanged has
	// a subject (alice) to rotate.
	onBindingAdd(crbRefRole("skip-arm-binding", "1", "skip-arm-role", noopArmUser("alice")))
	flushPendingSubGenBumps()
}

var skipArmRule = rbacv1.PolicyRule{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{"pods"}}

// TestRoleRulesSkip_LabelsOnlyUpdate_NoBump — the load-bearing RED. A role
// UPDATE that changed only a label (rules identical, new RV) must NOT rotate the
// bound subject's key. RED today (+1, oldObj dropped → unconditional bump).
func TestRoleRulesSkip_LabelsOnlyUpdate_NoBump(t *testing.T) {
	setupRoleSkipArm(t)
	d := driveRoleUpdate(t,
		clusterRoleObj("skip-arm-role", "1", skipArmRule),
		clusterRoleObj("skip-arm-role", "2", skipArmRule), // SAME rules, new RV
	)
	if d != 0 {
		t.Fatalf("#257: a labels-only role UPDATE (rules unchanged) must SKIP the sub-gen bump; got bumps delta %d — the oldObj-drop at rbac_snapshot.go:891 rotates every bound subject's key on a no-op write.", d)
	}
}

// TestRoleRulesSkip_RuleGrant_Bumps — a real rule change (grant a verb) MUST
// still rotate the key (the security direction: never suppress a real change).
func TestRoleRulesSkip_RuleGrant_Bumps(t *testing.T) {
	setupRoleSkipArm(t)
	granted := rbacv1.PolicyRule{Verbs: []string{"get", "list", "delete"}, APIGroups: []string{""}, Resources: []string{"pods"}}
	d := driveRoleUpdate(t,
		clusterRoleObj("skip-arm-role", "1", skipArmRule),
		clusterRoleObj("skip-arm-role", "2", granted),
	)
	if d == 0 {
		t.Fatalf("#257 SECURITY: a role rule change (granted 'delete') MUST rotate the bound subject's key; got bumps delta 0 — a suppressed rotation on a real grant is a stale verdict served under the old key.")
	}
}

// TestRoleRulesSkip_ReorderOnly_NoBump — a pure field-slice reorder (rules
// semantically equal) skips, like the labels-only arm.
func TestRoleRulesSkip_ReorderOnly_NoBump(t *testing.T) {
	setupRoleSkipArm(t)
	reordered := rbacv1.PolicyRule{Verbs: []string{"list", "get"}, APIGroups: []string{""}, Resources: []string{"pods"}}
	d := driveRoleUpdate(t,
		clusterRoleObj("skip-arm-role", "1", skipArmRule),
		clusterRoleObj("skip-arm-role", "2", reordered),
	)
	if d != 0 {
		t.Fatalf("#257: a pure verb reorder (semantically equal) must SKIP the bump; got %d", d)
	}
}

// MUTANT CONTROL MATRIX (arch C4): the arm SET discriminates both mutants of
// policyRulesEqual — a `return true` (always-skip) mutant fails every GRANT/REVOKE
// arm (TestRoleRulesSkip_RuleGrant_Bumps + _ResourceNamesNarrowing_Bumps, which
// require a bump), and a `return false` (never-skip) mutant fails every NO-OP arm
// (TestRoleRulesSkip_LabelsOnlyUpdate_NoBump + _ReorderOnly_NoBump, which require
// a skip). So neither trivial mutant survives the suite.

// TestRoleRulesSkip_ResourceNamesNarrowing_Bumps — the C4 leak-trap at the FULL
// skip boundary: a resourceNames NARROWING is a REVOKE; suppressing its rotation
// would serve the old (broader) grant under the stale key. It MUST bump.
func TestRoleRulesSkip_ResourceNamesNarrowing_Bumps(t *testing.T) {
	setupRoleSkipArm(t)
	wide := rbacv1.PolicyRule{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{"a", "b"}}
	narrow := rbacv1.PolicyRule{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"secrets"}, ResourceNames: []string{"a"}}
	d := driveRoleUpdate(t,
		clusterRoleObj("skip-arm-role", "1", wide),
		clusterRoleObj("skip-arm-role", "2", narrow),
	)
	if d == 0 {
		t.Fatalf("#257 C4 LEAK-TRAP (skip boundary): a resourceNames NARROWING (revoke) MUST rotate the bound subject's key; got 0 — the stale broader grant would be served under the old key.")
	}
}

// TestRoleRulesSkip_RuleGrant_KeepsRoleUpdateSourceTag — the #257×#260 stacking
// guard: a rule-change role UPDATE through the production path (UpdateFunc ->
// onRoleUpdated) must keep #260's bumpSrcRoleUpdate source tag on the FIRED bump
// (skip=false), so the role_update by_source bucket survives the skip wiring. A
// dropped tag (recordPendingSubGenBumps without the source) fails here.
func TestRoleRulesSkip_RuleGrant_KeepsRoleUpdateSourceTag(t *testing.T) {
	setupRoleSkipArm(t)
	t.Cleanup(ResetSubGenBumpsBySourceForTest)
	ResetSubGenBumpsBySourceForTest()
	before := RBACSubGenBumpsBySource(bumpSrcKeyRoleUpdate)
	granted := rbacv1.PolicyRule{Verbs: []string{"get", "delete"}, APIGroups: []string{""}, Resources: []string{"pods"}}
	rw := &ResourceWatcher{mode: modePassthrough}
	rw.rbacSnapshotEventHandlers(clusterRolesTypedGVR).UpdateFunc(
		clusterRoleObj("skip-arm-role", "1", skipArmRule),
		clusterRoleObj("skip-arm-role", "2", granted))
	flushPendingSubGenBumps()
	if d := RBACSubGenBumpsBySource(bumpSrcKeyRoleUpdate) - before; d == 0 {
		t.Fatalf("#257x#260: a rule-change role UPDATE via onRoleUpdated must keep the role_update by_source tag on the fired bump; got 0 — the skip path dropped #260's source attribution.")
	}
}

// ---- role no-op counters, proven through the FULL informer UpdateFunc ----

func driveRoleUpdateForCounters(t *testing.T, old, new *rbacv1.ClusterRole) {
	t.Helper()
	rw := &ResourceWatcher{mode: modePassthrough}
	rw.rbacSnapshotEventHandlers(clusterRolesTypedGVR).UpdateFunc(old, new)
}

func TestRoleNoop_FullHandler_SameRVRelist(t *testing.T) {
	setupRoleSkipArm(t)
	ResetRoleNoopCountersForTest()
	t.Cleanup(ResetRoleNoopCountersForTest)
	be, bn, bs := RBACRoleUpdateEventsTotal(), RBACRoleNoopUpdatesTotal(), RBACRoleSemanticNoopUpdatesTotal()
	cr := clusterRoleObj("skip-arm-role", "9", skipArmRule)
	driveRoleUpdateForCounters(t, cr, cr) // same RV relist
	if RBACRoleUpdateEventsTotal()-be != 1 {
		t.Fatalf("#257: role_update_events NOT OBSERVED through the real UpdateFunc; want +1")
	}
	if RBACRoleNoopUpdatesTotal()-bn != 1 {
		t.Fatalf("#257: same-RV relist must credit role_noop_updates +1")
	}
	if RBACRoleSemanticNoopUpdatesTotal()-bs != 1 {
		t.Fatalf("#257: same-RV relist is also semantic no-op (superset) +1")
	}
}

func TestRoleNoop_FullHandler_SemanticNoop(t *testing.T) {
	setupRoleSkipArm(t)
	ResetRoleNoopCountersForTest()
	t.Cleanup(ResetRoleNoopCountersForTest)
	bn, bs := RBACRoleNoopUpdatesTotal(), RBACRoleSemanticNoopUpdatesTotal()
	reordered := rbacv1.PolicyRule{Verbs: []string{"list", "get"}, APIGroups: []string{""}, Resources: []string{"pods"}}
	driveRoleUpdateForCounters(t,
		clusterRoleObj("skip-arm-role", "1", skipArmRule),
		clusterRoleObj("skip-arm-role", "2", reordered)) // new RV, rules equal
	if RBACRoleNoopUpdatesTotal()-bn != 0 {
		t.Fatalf("#257: a different-RV rewrite is not a byte relist; role_noop must stay 0")
	}
	if RBACRoleSemanticNoopUpdatesTotal()-bs != 1 {
		t.Fatalf("#257: rules-equal (order-insensitive) must credit role_semantic_noop +1")
	}
}

func TestRoleNoop_FullHandler_RealChange_NotNoop(t *testing.T) {
	setupRoleSkipArm(t)
	ResetRoleNoopCountersForTest()
	t.Cleanup(ResetRoleNoopCountersForTest)
	be, bn, bs := RBACRoleUpdateEventsTotal(), RBACRoleNoopUpdatesTotal(), RBACRoleSemanticNoopUpdatesTotal()
	changed := rbacv1.PolicyRule{Verbs: []string{"get", "delete"}, APIGroups: []string{""}, Resources: []string{"pods"}}
	driveRoleUpdateForCounters(t,
		clusterRoleObj("skip-arm-role", "1", skipArmRule),
		clusterRoleObj("skip-arm-role", "2", changed))
	if RBACRoleUpdateEventsTotal()-be != 1 {
		t.Fatalf("#257: role_update_events +1")
	}
	if RBACRoleNoopUpdatesTotal()-bn != 0 || RBACRoleSemanticNoopUpdatesTotal()-bs != 0 {
		t.Fatalf("#257: a real rule change must credit NEITHER role_noop nor role_semantic_noop")
	}
}
