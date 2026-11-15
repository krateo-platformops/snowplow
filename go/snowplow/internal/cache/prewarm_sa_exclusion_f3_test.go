// prewarm_sa_exclusion_f3_test.go — #130 F3 (Diego directive 2026-07-11):
// ServiceAccount-subject cohorts are EXCLUDED from the boot seed target set.
//
// EnumeratePrewarmTargetsForGVR now drops any binding whose subject set is
// entirely ServiceAccount-kind (zero User, zero Group subjects) — a machine/
// controller cohort that never renders the frontend and was starving the login
// cohorts (admins) out of the boot budget. The discriminator is subject KIND
// (subjectKey.Kind), NOT a name allowlist.
//
// ARMS (architect-mandated set — the match_subject_test.go gap that would have
// let the winning-rep-kind bug ship silently):
//
//   - ARM-ALLSA-EXCLUDED: a binding with ONLY SA subjects → target ABSENT.
//   - ARM-MIXED-KEPT (the RED-catcher): a binding with [SA-first, Group-second]
//     → target KEPT. RED = the naive "picked representative is SA-kind" gate
//     (pickRepresentativeFromSubjects is first-wins-by-slice-order, so it picks
//     the SA) would WRONGLY drop this login-cohort binding. The set-predicate
//     allSubjectsAreServiceAccountKind keeps it because a Group subject exists.
//   - ARM-GROUP-KEPT: all-Group binding, incl. `system:masters` (a GROUP subject
//     whose name looks system-ish) → KEPT. Proves the discriminator is KIND, not
//     a `system:` name-prefix (which would wrongly exclude a login cohort).
//   - ARM-USER-KEPT: a User-subject binding → KEPT.
//
// Pure unit — synthetic RBAC snapshots via buildSnap; never touches the rbac
// package's destructive TestMain.

package cache

import (
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// crbMultiSubject builds a ClusterRoleBinding carrying a MULTI-subject set (the
// mixed-binding shape crbRuleUID's single-subject signature cannot express) + a
// matching ClusterRole granting the rules. Subject ORDER is preserved so the
// [SA-first, Group-second] ordering the RED arm needs is exact.
func crbMultiSubject(name, uid string, subs []rbacv1.Subject, rules []rbacv1.PolicyRule) (*rbacv1.ClusterRoleBinding, *rbacv1.ClusterRole) {
	cr := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-role"},
		Rules:      rules,
	}
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid)},
		Subjects:   subs,
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: name + "-role"},
	}
	return crb, cr
}

// hasSubjectLabel reports whether the target set contains a representative with
// the given subjectSetOf label ("group:devs", "user:alice", "user:system:...").
func hasSubjectLabel(targets []PrewarmTarget, label string) bool {
	for _, l := range subjectSetOf(targets) {
		if l == label {
			return true
		}
	}
	return false
}

// TestF3SAExclusion_MixedSubjects is the load-bearing arm: a MIXED enumeration
// (Group + all-SA + mixed[SA,Group] + User + system:masters group) → the all-SA
// cohort is EXCLUDED, every cohort with any User/Group subject is KEPT.
//
// K>1 cohorts × M>1 subject kinds. The mixed [SA-first, Group-second] arm is the
// RED-catcher: a winning-rep-kind gate would drop it (rep=SA). system:masters
// proves KIND-not-name.
func TestF3SAExclusion_MixedSubjects(t *testing.T) {
	ResetBindingsByGVRIndexForTest()

	g := gr("widgets.templates.krateo.io", "flexes")
	rules := getListRules(g.Group, g.Resource)

	var crbs []*rbacv1.ClusterRoleBinding
	var crs []*rbacv1.ClusterRole
	add := func(crb *rbacv1.ClusterRoleBinding, cr *rbacv1.ClusterRole) {
		crbs = append(crbs, crb)
		crs = append(crs, cr)
	}

	// (1) all-Group login cohort — KEPT.
	add(crbRuleUID("devs", "uid-devs", rbacv1.Subject{Kind: "Group", Name: "devs"}, rules))
	// (2) all-SA machine cohort — EXCLUDED.
	add(crbRuleUID("cdc-sa", "uid-cdc", rbacv1.Subject{
		Kind: "ServiceAccount", Name: "portals-v1-5-1", Namespace: "krateo-system"}, rules))
	// (3) MIXED [SA-first, Group-second] — KEPT (the RED-catcher). rep picks the
	//     SA (first-wins) but a Group subject exists → not all-SA → kept.
	add(crbMultiSubject("mixed", "uid-mixed", []rbacv1.Subject{
		{Kind: "ServiceAccount", Name: "sidecar", Namespace: "krateo-system"},
		{Kind: "Group", Name: "ops"},
	}, rules))
	// (4) User login identity — KEPT.
	add(crbRuleUID("alice", "uid-alice", rbacv1.Subject{Kind: "User", Name: "alice"}, rules))
	// (5) all-Group cohort NAMED system:masters — KEPT (KIND not name-prefix).
	add(crbRuleUID("masters", "uid-masters", rbacv1.Subject{Kind: "Group", Name: "system:masters"}, rules))

	PublishRBACSnapshotForTest(buildSnap(crbs, crs))
	SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{g})

	targets := EnumeratePrewarmTargetsForGVR(g, "list")
	got := subjectSetOf(targets)

	// EXCLUDED: the all-SA cohort must be ABSENT.
	if hasSubjectLabel(targets, "user:system:serviceaccount:krateo-system:portals-v1-5-1") {
		t.Fatalf("ARM-ALLSA-EXCLUDED: the all-ServiceAccount cohort (portals-v1-5-1) MUST be excluded "+
			"from the seed targets; it is present. got=%v", got)
	}

	// KEPT: every cohort with any User/Group subject.
	// The mixed binding's rep is the SA (first-wins) — so it appears under the SA
	// username, but it MUST be present (KEPT), which is the RED-catch: a
	// winning-rep-kind gate would have dropped it.
	mustKeep := []string{
		"group:devs",
		"user:alice",
		"group:system:masters",
		"user:system:serviceaccount:krateo-system:sidecar", // the MIXED binding, kept, rep=SA
	}
	for _, m := range mustKeep {
		if !hasSubjectLabel(targets, m) {
			t.Fatalf("ARM-KEPT: cohort %q MUST survive the SA-exclusion (has a User/Group subject, "+
				"or is a mixed binding kept by the set-predicate); absent. got=%v", m, got)
		}
	}

	// Exactly 4 kept (devs, alice, system:masters, mixed) — the all-SA cohort is
	// the only exclusion.
	if len(targets) != 4 {
		t.Fatalf("ARM-COUNT: want 4 kept targets (all-SA excluded, mixed kept), got %d: %v", len(targets), got)
	}
}

// TestF3SAExclusion_AllServiceAccount_FullyExcluded is the pure ALL-SA arm: an
// enumeration of ONLY SA-subject bindings → ZERO seed targets (all excluded).
// This is the ~222-system-SA-tail-killed shape at unit scale.
func TestF3SAExclusion_AllServiceAccount_FullyExcluded(t *testing.T) {
	ResetBindingsByGVRIndexForTest()

	g := gr("widgets.templates.krateo.io", "flexes")
	rules := getListRules(g.Group, g.Resource)

	var crbs []*rbacv1.ClusterRoleBinding
	var crs []*rbacv1.ClusterRole
	for _, sa := range []struct{ name, ns string }{
		{"kube-controller-manager", "kube-system"},
		{"cloud-controller-manager", "kube-system"},
		{"portals-v1-5-1", "krateo-system"},
	} {
		crb, cr := crbRuleUID("crb-"+sa.name, "uid-"+sa.name, rbacv1.Subject{
			Kind: "ServiceAccount", Name: sa.name, Namespace: sa.ns}, rules)
		crbs = append(crbs, crb)
		crs = append(crs, cr)
	}
	PublishRBACSnapshotForTest(buildSnap(crbs, crs))
	SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{g})

	targets := EnumeratePrewarmTargetsForGVR(g, "list")
	if len(targets) != 0 {
		t.Fatalf("ARM-ALLSA-FULL: an all-ServiceAccount enumeration must produce ZERO seed targets "+
			"(the ~222-system-SA tail killed); got %d: %v", len(targets), subjectSetOf(targets))
	}
}

// TestF3SAExclusion_Predicate is the direct unit of allSubjectsAreServiceAccountKind
// — the set predicate, independent of pickRepresentative. It pins the exact
// truth table the architect's ruling requires.
func TestF3SAExclusion_Predicate(t *testing.T) {
	sa := subjectKey{Kind: rbacv1.ServiceAccountKind, Name: "x", Namespace: "ns"}
	user := subjectKey{Kind: rbacv1.UserKind, Name: "alice"}
	group := subjectKey{Kind: rbacv1.GroupKind, Name: "devs"}

	cases := []struct {
		name string
		in   []subjectKey
		want bool
	}{
		{"empty → false (nothing to exclude)", nil, false},
		{"all-SA → true (exclude)", []subjectKey{sa, sa}, true},
		{"single SA → true", []subjectKey{sa}, true},
		{"mixed SA+Group → false (KEEP — the RED-catch)", []subjectKey{sa, group}, false},
		{"mixed Group+SA → false (KEEP, order-independent)", []subjectKey{group, sa}, false},
		{"mixed SA+User → false (KEEP)", []subjectKey{sa, user}, false},
		{"all-Group → false (KEEP)", []subjectKey{group}, false},
		{"all-User → false (KEEP)", []subjectKey{user}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := allSubjectsAreServiceAccountKind(tc.in); got != tc.want {
				t.Fatalf("allSubjectsAreServiceAccountKind(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
