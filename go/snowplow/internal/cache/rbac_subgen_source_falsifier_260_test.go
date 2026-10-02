// rbac_subgen_source_falsifier_260_test.go — #260 §3 per-source liveness arms.
//
// Each arm drives the REAL informer delta handler (onBindingAdd/Update/Delete),
// flushes, and asserts EXACTLY the matching {source} bucket of
// subgen_bumps_by_source_total moved — and no other bucket did (attribution).
// A source whose counter stays 0 is NOT OBSERVED (the §3 failure). RED pre-wiring
// (recordPendingSubGenBumps does not tag/count sources yet); GREEN once the
// source bitmask is threaded through recordPendingSubGenBumps → flush.
//
// Role arms (role_add/update/delete) are added with the role-kind wiring (they
// require the onRoleObjectChanged signature change) — see the GREEN follow-up.

package cache

import (
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func srcArmCRB(name, rv string, subs ...rbacv1.Subject) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name), ResourceVersion: rv},
		Subjects:   subs,
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "src-arm-role"},
	}
}

func allSubGenSourceCounts() map[string]uint64 {
	keys := []string{
		bumpSrcKeyBindingAdd, bumpSrcKeyBindingUpdate, bumpSrcKeyBindingDelete,
		bumpSrcKeyRoleAdd, bumpSrcKeyRoleUpdate, bumpSrcKeyRoleDelete,
	}
	out := make(map[string]uint64, len(keys))
	for _, k := range keys {
		out[k] = RBACSubGenBumpsBySource(k)
	}
	return out
}

// assertOnlySourceMoved fails unless EXACTLY `want` moved (delta > 0) and every
// other source is unchanged — the attribution guarantee.
func assertOnlySourceMoved(t *testing.T, before map[string]uint64, want string) {
	t.Helper()
	for k, b := range before {
		d := RBACSubGenBumpsBySource(k) - b
		if k == want {
			if d == 0 {
				t.Fatalf("#260: source %q NOT OBSERVED — its bump-by-source counter stayed 0 on the real event (unwired attribution)", k)
			}
		} else if d != 0 {
			t.Fatalf("#260: source %q moved by %d on a %q event — attribution leaked across buckets", k, d, want)
		}
	}
}

func setupSubGenSourceArm(t *testing.T) {
	t.Helper()
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)
	t.Cleanup(ResetSubGenBumpsBySourceForTest)
	ResetSubGenBumpsBySourceForTest()
	ResetPendingSubGenBumpsForTest()
}

func TestSubGenBumpsBySource_BindingAdd_Liveness(t *testing.T) {
	setupSubGenSourceArm(t)
	before := allSubGenSourceCounts()
	onBindingAdd(srcArmCRB("add-crb", "1", noopArmUser("alice")))
	flushPendingSubGenBumps()
	assertOnlySourceMoved(t, before, bumpSrcKeyBindingAdd)
}

func TestSubGenBumpsBySource_BindingUpdate_Liveness(t *testing.T) {
	setupSubGenSourceArm(t)
	before := allSubGenSourceCounts()
	// A GENUINE UPDATE (a subject added). #253: a semantically no-op UPDATE no
	// longer bumps at all, so the liveness arm must carry a real RBAC change —
	// the skip's own attribution is pinned by the arm below.
	onBindingUpdate(
		srcArmCRB("upd-crb", "1", noopArmUser("alice")),
		srcArmCRB("upd-crb", "2", noopArmUser("alice"), noopArmUser("bob")),
	)
	flushPendingSubGenBumps()
	assertOnlySourceMoved(t, before, bumpSrcKeyBindingUpdate)
}

// TestSubGenBumpsBySource_BindingUpdateSkip_MovesNoBucket — #253 x #260. A
// semantically no-op binding UPDATE (new RV, same uid/subjects/roleRef) is
// SKIPPED: it must move NO by-source bucket (binding_update included), while
// semantic_noop still counts it. This is the production reading of the fix:
// semantic_noop climbing, binding_update flat.
func TestSubGenBumpsBySource_BindingUpdateSkip_MovesNoBucket(t *testing.T) {
	setupSubGenSourceArm(t)
	t.Cleanup(ResetBindingNoopCountersForTest)
	ResetBindingNoopCountersForTest()
	before := allSubGenSourceCounts()
	beforeSem := RBACBindingSemanticNoopUpdatesTotal()
	onBindingUpdate(
		srcArmCRB("upd-skip-crb", "1", noopArmUser("alice")),
		srcArmCRB("upd-skip-crb", "2", noopArmUser("alice")),
	)
	flushPendingSubGenBumps()
	for k, b := range before {
		if d := RBACSubGenBumpsBySource(k) - b; d != 0 {
			t.Errorf("#253: a semantic no-op binding UPDATE moved by-source bucket %q by %d; want 0 — the skip must record no bump under ANY source", k, d)
		}
	}
	if d := RBACBindingSemanticNoopUpdatesTotal() - beforeSem; d != 1 {
		t.Errorf("#253: semantic_noop delta = %d; want 1 — the skipped event must still be counted", d)
	}
}

func TestSubGenBumpsBySource_BindingDelete_Liveness(t *testing.T) {
	setupSubGenSourceArm(t)
	// The binding must exist before the DELETE unrols it (and its subjects are
	// what the delete-side bump records).
	onBindingAdd(srcArmCRB("del-crb", "1", noopArmUser("alice")))
	flushPendingSubGenBumps()
	before := allSubGenSourceCounts()
	onBindingDelete(srcArmCRB("del-crb", "2", noopArmUser("alice")))
	flushPendingSubGenBumps()
	assertOnlySourceMoved(t, before, bumpSrcKeyBindingDelete)
}

func srcArmClusterRole(name string, rules ...rbacv1.PolicyRule) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules}
}

// roleSourceArm drives a role event of `source` against a role that a
// pre-registered binding references (so onRoleRulesChanged has subjects to
// bump), and asserts ONLY that role bucket moved. srcArmCRB references
// ClusterRole "src-arm-role".
func roleSourceArm(t *testing.T, source subGenBumpSource, wantKey string) {
	t.Helper()
	setupSubGenSourceArm(t)
	onBindingAdd(srcArmCRB("role-arm-binding", "1", noopArmUser("alice")))
	flushPendingSubGenBumps()
	before := allSubGenSourceCounts()
	onRoleObjectChanged(source, srcArmClusterRole("src-arm-role"))
	flushPendingSubGenBumps()
	assertOnlySourceMoved(t, before, wantKey)
}

func TestSubGenBumpsBySource_RoleAdd_Liveness(t *testing.T) {
	roleSourceArm(t, bumpSrcRoleAdd, bumpSrcKeyRoleAdd)
}

func TestSubGenBumpsBySource_RoleUpdate_Liveness(t *testing.T) {
	roleSourceArm(t, bumpSrcRoleUpdate, bumpSrcKeyRoleUpdate)
}

func TestSubGenBumpsBySource_RoleDelete_Liveness(t *testing.T) {
	roleSourceArm(t, bumpSrcRoleDelete, bumpSrcKeyRoleDelete)
}

func crbWithUID(name, uid, rv string, subs ...rbacv1.Subject) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid), ResourceVersion: rv},
		Subjects:   subs,
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "src-arm-role"},
	}
}

// TestBindingUidChanged_ExcludedFromSemanticNoop — #260 change 3 / amendment-2
// item 8. A binding deleted+recreated under the SAME name arrives (relist) as
// ONE OnUpdate with a DIFFERENT uid + new RV, same subjects/roleRef. The next
// publish DOES rotate the key through BindingUID, so semantic_noop must NOT
// credit it; binding_uid_changed_updates_total must move instead.
func TestBindingUidChanged_ExcludedFromSemanticNoop(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)
	t.Cleanup(ResetBindingNoopCountersForTest)
	ResetBindingNoopCountersForTest()

	beforeUID := RBACBindingUidChangedUpdatesTotal()
	beforeSem := RBACBindingSemanticNoopUpdatesTotal()
	ResetRBACSubGenForTest()
	ResetPendingSubGenBumpsForTest()
	onBindingUpdate(
		crbWithUID("uid-drift-crb", "old-uid", "1", noopArmUser("alice")),
		crbWithUID("uid-drift-crb", "new-uid", "2", noopArmUser("alice")),
	)
	if d := RBACBindingUidChangedUpdatesTotal() - beforeUID; d != 1 {
		t.Fatalf("#260: a uid-changed UPDATE must bump binding_uid_changed_updates_total by 1; got %d", d)
	}
	if d := RBACBindingSemanticNoopUpdatesTotal() - beforeSem; d != 0 {
		t.Fatalf("#260 amendment-2 item 8: semantic_noop must NOT credit a uid-changed (delete+recreate) update — it rotates the key through BindingUID; got delta %d", d)
	}
	// #253: the skip refuses a uid change too (the event stands in for a real
	// DELETE plus ADD, both of which bump), so the counter and the skip agree.
	ResetSubGenBumpsBySourceForTest()
	t.Cleanup(ResetSubGenBumpsBySourceForTest)
	flushPendingSubGenBumps()
	if subGenValue(subjectKey{Kind: subjectKindUser, Name: "alice"}) == 0 {
		t.Fatal("#253: a uid-changed UPDATE (same-name recreate collapsed by a relist) was SKIPPED — alice's sub-generation did not move. It must bump like the DELETE+ADD it stands in for")
	}
	if RBACSubGenBumpsBySource(bumpSrcKeyBindingUpdate) == 0 {
		t.Fatal("#253 x #260: the uid-changed UPDATE's bump must be attributed to binding_update")
	}
}

// Control: a genuine same-uid rewrite with unchanged subjects/roleRef IS a
// semantic no-op, and uid_changed does NOT fire — proving the exclusion is
// scoped to a real uid rotation, not every UPDATE.
func TestBindingSameUid_StillCreditsSemanticNoop(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)
	t.Cleanup(ResetBindingNoopCountersForTest)
	ResetBindingNoopCountersForTest()

	beforeUID := RBACBindingUidChangedUpdatesTotal()
	beforeSem := RBACBindingSemanticNoopUpdatesTotal()
	onBindingUpdate(
		crbWithUID("same-uid-crb", "u", "1", noopArmUser("alice")),
		crbWithUID("same-uid-crb", "u", "2", noopArmUser("alice")),
	)
	if d := RBACBindingUidChangedUpdatesTotal() - beforeUID; d != 0 {
		t.Fatalf("#260: a same-uid rewrite must NOT bump uid_changed; got %d", d)
	}
	if d := RBACBindingSemanticNoopUpdatesTotal() - beforeSem; d != 1 {
		t.Fatalf("#260: a same-uid rewrite with unchanged subjects/roleRef must still credit semantic_noop; got %d", d)
	}
}
