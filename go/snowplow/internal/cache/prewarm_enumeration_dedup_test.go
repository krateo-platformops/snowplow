// prewarm_enumeration_dedup_test.go — #42 seed-target dedup by representative
// identity (design docs/seed-target-dedup-representative-identity-design-2026-07-04.md).
//
// EnumeratePrewarmTargetsForGVR now returns ONE target per distinct
// representative (Username, sorted-Groups) tuple instead of one per binding.
// On the per-composition-RoleBinding topology 456 bindings that all project to
// Group/devs collapse to ONE dispatch (the live obs-by-kind-list 81→1). The
// dedup is LOSSLESS (the seeded cell set is unchanged — Path B re-derives the
// cell-key BindingUID at populate time; same tuple → same first-match binding →
// same cell) and leak-free (distinct tuples still dispatch separately).
//
// These arms exercise the enumerator directly (AC-D5 ARM-DISTINCT / ARM-COLLAPSE
// / D5e + both mutations). Pure unit — builds synthetic RBAC snapshots
// in-process via buildSnap / a local RB builder; never touches the rbac
// package's destructive TestMain (AC-D6). The dispatchers-package companion
// (prewarm_dedup_key_parity_test.go) covers ARM-KEY-PARITY through the REAL
// withCohortSeedContext → dispatchCacheLookupKey derivation.

package cache

import (
	"sort"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// subjectSetOf returns the sorted "kind:name" labels of a target slice's
// representative subjects — the Subject-SET assertion (not a bare count, per
// feedback_falsifier_shape_must_discriminate).
func subjectSetOf(ts []PrewarmTarget) []string {
	out := make([]string, 0, len(ts))
	for _, t := range ts {
		switch {
		case t.Subject.Username != "":
			out = append(out, "user:"+t.Subject.Username)
		case len(t.Subject.Groups) > 0:
			g := append([]string(nil), t.Subject.Groups...)
			sort.Strings(g)
			lbl := "group:"
			for i, gg := range g {
				if i > 0 {
					lbl += ","
				}
				lbl += gg
			}
			out = append(out, lbl)
		default:
			out = append(out, "anon")
		}
	}
	sort.Strings(out)
	return out
}

// ── ARM-DISTINCT: K>1 identities × M>1 bindings each → one target per identity ─
//
// 3× Group/devs + 2× User/alice + 1× SA all granting get/list on the SAME GVR.
// The enumerator MUST return EXACTLY 3 targets whose Subject set == {devs,
// alice, sa}. This is the UNDER-SEED guard: an over-collapse (drop
// Username+Groups) would return 1; a no-collapse (key on BindingUID) would
// return 6.
func TestPrewarmDedup_ARM_DISTINCT_OneTargetPerIdentity(t *testing.T) {
	ResetBindingsByGVRIndexForTest()

	g := gr("widgets.templates.krateo.io", "flexes")
	rules := getListRules(g.Group, g.Resource)

	var crbs []*rbacv1.ClusterRoleBinding
	var crs []*rbacv1.ClusterRole
	// 3 distinct bindings, SAME Group/devs subject (the wildcard-floor shape).
	for i, name := range []string{"devs-a", "devs-b", "devs-c"} {
		crb, cr := crbRuleUID(name, "uid-devs-"+string(rune('0'+i)),
			rbacv1.Subject{Kind: "Group", Name: "devs"}, rules)
		crbs = append(crbs, crb)
		crs = append(crs, cr)
	}
	// 2 distinct bindings, SAME User/alice subject.
	for i, name := range []string{"alice-a", "alice-b"} {
		crb, cr := crbRuleUID(name, "uid-alice-"+string(rune('0'+i)),
			rbacv1.Subject{Kind: "User", Name: "alice"}, rules)
		crbs = append(crbs, crb)
		crs = append(crs, cr)
	}
	// 1 SA binding (singleton identity).
	saCRB, saCR := crbRuleUID("sa-a", "uid-sa-0",
		rbacv1.Subject{Kind: "ServiceAccount", Name: "installer", Namespace: "krateo-system"}, rules)
	crbs = append(crbs, saCRB)
	crs = append(crs, saCR)

	PublishRBACSnapshotForTest(buildSnap(crbs, crs))
	SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{g})

	targets := EnumeratePrewarmTargetsForGVR(g, "list")
	// #130 F3 (Diego directive 2026-07-11): the SA-only binding (installer) is now
	// EXCLUDED from the seed target set — it is a machine cohort that never renders
	// the frontend. So the 6 bindings dedup to EXACTLY 2 login-cohort targets
	// (devs, alice); the SA is gone. (Pre-F3 this asserted 3 incl. the SA. The
	// dedup contract for the login cohorts is unchanged — this arm now doubles as
	// the "login cohorts survive, SA excluded" check; the dedicated exclusion arms
	// live in prewarm_sa_exclusion_f3_test.go.)
	if len(targets) != 2 {
		t.Fatalf("ARM-DISTINCT (F3): expected 2 deduped login-cohort targets (devs, alice) from 6 bindings "+
			"with the SA-only binding excluded, got %d: %+v", len(targets), targets)
	}
	got := subjectSetOf(targets)
	want := []string{"group:devs", "user:alice"}
	sort.Strings(want)
	if !equalSorted(got, want) {
		t.Fatalf("ARM-DISTINCT (F3): Subject set = %v; want %v (login cohorts survive dedup; SA-only binding excluded)", got, want)
	}
}

// ── ARM-COLLAPSE: M same-identity bindings → exactly 1 target ─────────────────
//
// 5 bindings, all Group/devs on the same GVR → 1 target. The live 81→1 shape.
func TestPrewarmDedup_ARM_COLLAPSE_SameIdentityToOne(t *testing.T) {
	ResetBindingsByGVRIndexForTest()

	g := gr("widgets.templates.krateo.io", "flexes")
	rules := getListRules(g.Group, g.Resource)

	var crbs []*rbacv1.ClusterRoleBinding
	var crs []*rbacv1.ClusterRole
	for i := 0; i < 5; i++ {
		crb, cr := crbRuleUID("devs-"+string(rune('0'+i)), "uid-devs-"+string(rune('0'+i)),
			rbacv1.Subject{Kind: "Group", Name: "devs"}, rules)
		crbs = append(crbs, crb)
		crs = append(crs, cr)
	}
	PublishRBACSnapshotForTest(buildSnap(crbs, crs))
	SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{g})

	targets := EnumeratePrewarmTargetsForGVR(g, "list")
	if len(targets) != 1 {
		t.Fatalf("ARM-COLLAPSE: expected 1 target (5 same-identity Group/devs bindings collapse), got %d: %+v", len(targets), targets)
	}
	if len(targets[0].Subject.Groups) != 1 || targets[0].Subject.Groups[0] != "devs" || targets[0].Subject.Username != "" {
		t.Fatalf("ARM-COLLAPSE: representative = %+v; want group-only [devs]", targets[0].Subject)
	}
}

// buildSnapWithRBs assembles a snapshot from RoleBindings-by-ns + their Roles
// (D5e needs RBs in different namespaces). Mirrors buildSnap but for the
// namespaced path.
func buildSnapWithRBs(rbsByNS map[string][]*rbacv1.RoleBinding, rolesByNSName map[string]*rbacv1.Role) *RBACSnapshot {
	s := &RBACSnapshot{
		ClusterRoleBindings: nil,
		RoleBindingsByNS:    rbsByNS,
		ClusterRolesByName:  map[string]*rbacv1.ClusterRole{},
		RolesByNSName:       rolesByNSName,
	}
	rebuildSubjectIndexes(s)
	return s
}

func rbRuleUID(ns, name, uid string, sub rbacv1.Subject, rules []rbacv1.PolicyRule) (*rbacv1.RoleBinding, *rbacv1.Role) {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name + "-role"},
		Rules:      rules,
	}
	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid)},
		Subjects:   []rbacv1.Subject{sub},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: name + "-role"},
	}
	return rb, role
}

// ── AC-D5e: same subject in DIFFERENT binding namespaces → exactly 1 target ───
//
// Two RoleBindings, both Group/devs, granting get/list on the SAME widget GVR,
// but in namespaces ns-a and ns-b (the per-composition-RoleBinding shape). The
// binding namespace NEVER enters the seed dispatch inputs (dispatch evaluates
// at the WIDGET's gvr/ns/name), so both collapse to ONE target. This is the
// design §1 namespace nuance — the mechanism behind the live 81→1 collapse.
func TestPrewarmDedup_D5e_SameSubjectDifferentBindingNS_CollapsesToOne(t *testing.T) {
	ResetBindingsByGVRIndexForTest()

	g := gr("widgets.templates.krateo.io", "flexes")
	rules := getListRules(g.Group, g.Resource)

	rbA, roleA := rbRuleUID("ns-a", "devs-a", "uid-rb-a",
		rbacv1.Subject{Kind: "Group", Name: "devs"}, rules)
	rbB, roleB := rbRuleUID("ns-b", "devs-b", "uid-rb-b",
		rbacv1.Subject{Kind: "Group", Name: "devs"}, rules)

	snap := buildSnapWithRBs(
		map[string][]*rbacv1.RoleBinding{"ns-a": {rbA}, "ns-b": {rbB}},
		map[string]*rbacv1.Role{"ns-a/devs-a-role": roleA, "ns-b/devs-b-role": roleB},
	)
	PublishRBACSnapshotForTest(snap)
	SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{g})

	targets := EnumeratePrewarmTargetsForGVR(g, "list")
	if len(targets) != 1 {
		t.Fatalf("D5e: expected EXACTLY 1 target (same Group/devs subject in 2 different binding namespaces must collapse — binding-ns never enters dispatch inputs), got %d: %+v", len(targets), targets)
	}
	if len(targets[0].Subject.Groups) != 1 || targets[0].Subject.Groups[0] != "devs" {
		t.Fatalf("D5e: representative = %+v; want group-only [devs]", targets[0].Subject)
	}
}
