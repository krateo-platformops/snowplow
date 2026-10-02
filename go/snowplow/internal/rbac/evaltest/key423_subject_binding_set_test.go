// key423_subject_binding_set_test.go — #423 unit arms for
// rbac.SubjectBindingSetDigest, the identity dimension of the resolved-output
// L1 key.
//
// What each arm pins:
//   - same-set sharing: group-only members of the same groups → ONE digest
//     (cohort sharing, so the prewarm cost and cohort-count independence are
//     unchanged for the common shape);
//   - an extra binding (User-subject RB) → a DIFFERENT digest (the #423 split);
//   - User/Group symmetry: two Users named by one CRB with nothing else → ONE
//     digest; give one of them an extra binding → split, exactly like the
//     group case;
//   - ServiceAccount synthetic groups count (system:serviceaccounts[:ns]);
//   - the seed representative {Username:"", Groups:[g]} derives the SAME
//     digest as a real group-only member, INCLUDING the system:authenticated
//     bindings every authenticated request holds (seed/customer key parity);
//   - the digest is the evaluator's own subject semantics — the id set equals
//     exactly the bindings whose subjects anySubjectMatches (cross-checked by
//     brute force over every binding in the snapshot);
//   - a republish (new snapshot) is never served a stale memoised digest.
//
// BenchmarkSubjectBindingSetDigest gives the per-request cost (memo hit) and
// the per-identity-per-snapshot cold cost at a production-shaped snapshot.
package evaltest

import (
	"fmt"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func k423CR(name string) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}},
	}
}

func k423CRB(name, uid, role string, subs ...rbacv1.Subject) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid)},
		Subjects:   subs,
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role},
	}
}

func k423RB(ns, name, uid, role string, subs ...rbacv1.Subject) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID(uid)},
		Subjects:   subs,
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role},
	}
}

func k423User(n string) rbacv1.Subject {
	return rbacv1.Subject{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: n}
}
func k423Group(n string) rbacv1.Subject {
	return rbacv1.Subject{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: n}
}

// k423Fixture: a portal-wide group grant, a per-namespace group grant, the
// cluster's ever-present system:authenticated grant, a User-subject extra RB for
// alice, a CRB naming two Users, and an SA-group grant.
func k423Fixture() []runtime.Object {
	return []runtime.Object{
		k423CR("reader"),
		k423CRB("portal", "uid-crb-portal", "reader", k423Group("portal")),
		k423CRB("basic-user", "uid-crb-authn", "reader", k423Group("system:authenticated")),
		k423CRB("two-users", "uid-crb-two", "reader", k423User("u1"), k423User("u2")),
		k423CRB("all-sas", "uid-crb-sas", "reader", k423Group("system:serviceaccounts:apps")),
		k423RB("tenant-a", "portal-a", "uid-rb-portal-a", "reader", k423Group("portal")),
		k423RB("tenant-a", "alice-extra", "uid-rb-alice", "reader", k423User("alice")),
		k423RB("tenant-b", "u1-extra", "uid-rb-u1", "reader", k423User("u1")),
	}
}

func TestKey423_SubjectBindingSet_SharingAndSplit(t *testing.T) {
	newTestWatcher(t, k423Fixture()...)
	rbac.ResetSubjectBindingSetMemoForTest()

	d := rbac.SubjectBindingSetDigest
	carol := d("carol", []string{"portal"})
	dave := d("dave", []string{"portal"})
	alice := d("alice", []string{"portal"})

	if carol == "" {
		t.Fatalf("PRE: a published snapshot must yield a non-empty digest")
	}
	// Cohort sharing: group-only members of the same groups share one digest.
	if carol != dave {
		t.Fatalf("SHARING BROKEN: group-only members carol and dave hold the same binding set and must share one "+
			"digest (cohort sharing / prewarm cost); carol=%s dave=%s", carol, dave)
	}
	// The #423 split: alice holds an extra User-subject RoleBinding.
	if alice == carol {
		t.Fatalf("#423 SPLIT MISSING: alice holds an extra RoleBinding (alice-extra) yet derives the same digest as " +
			"group-only carol — a co-bound user with different step-level RBAC would share her cell")
	}
	// Group order is irrelevant.
	if d("carol", []string{"portal", "other"}) != d("carol", []string{"other", "portal"}) {
		t.Fatalf("group order must not change the digest")
	}

	// User/Group symmetry: u2 is named by ONE CRB (two-users) and nothing else;
	// a third user named the same way would share. u1 holds an extra RB → split.
	u1 := d("u1", nil)
	u2 := d("u2", nil)
	if u1 == u2 {
		t.Fatalf("USER-SUBJECT SPLIT MISSING: u1 holds an extra RB (u1-extra) — must not share u2's digest")
	}
	snap := cache.Global().Snapshot()
	ids := rbac.SubjectBindingIDs(snap, "u2", nil)
	want := []string{"C:uid-crb-authn", "C:uid-crb-two"}
	if fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Fatalf("u2's binding set: got %v want %v", ids, want)
	}

	// SA synthetic group: an SA in namespace apps matches system:serviceaccounts:apps.
	saIDs := rbac.SubjectBindingIDs(snap, "system:serviceaccount:apps:deployer", nil)
	if fmt.Sprint(saIDs) != fmt.Sprint([]string{"C:uid-crb-authn", "C:uid-crb-sas"}) {
		t.Fatalf("SA synthetic groups must be honoured exactly as EvaluateRBAC does; got %v", saIDs)
	}
}

// TestKey423_SeedRepresentativeParity — the prewarm seed's group representative
// is {Username:"", Groups:[g]} (pickRepresentativeFromSubjects). A real
// group-only member carries a username and therefore ALSO holds every
// system:authenticated binding. The digest treats every identity as
// authenticated so the two agree; without that, no customer would ever hit a
// seed-minted cell (a cold first navigation for every group-only user).
func TestKey423_SeedRepresentativeParity(t *testing.T) {
	newTestWatcher(t, k423Fixture()...)
	rbac.ResetSubjectBindingSetMemoForTest()

	rep := rbac.SubjectBindingSetDigest("", []string{"portal"})
	customer := rbac.SubjectBindingSetDigest("carol", []string{"portal"})
	if rep != customer {
		snap := cache.Global().Snapshot()
		t.Fatalf("SEED/CUSTOMER PARITY BROKEN: the seed representative and a real group-only member must derive "+
			"the same digest; rep=%v customer=%v", rbac.SubjectBindingIDs(snap, "", []string{"portal"}),
			rbac.SubjectBindingIDs(snap, "carol", []string{"portal"}))
	}
	// And the system:authenticated binding really is in both (the arm is not
	// vacuous: drop the always-authenticated rule and rep would lack it).
	ids := rbac.SubjectBindingIDs(cache.Global().Snapshot(), "", []string{"portal"})
	found := false
	for _, id := range ids {
		if id == "C:uid-crb-authn" {
			found = true
		}
	}
	if !found {
		t.Fatalf("the representative's set must include the system:authenticated CRB; got %v", ids)
	}
}

// TestKey423_SubjectBindingSet_MatchesEvaluatorSubjectSemantics brute-forces
// the id set: for every binding in the snapshot, the binding is in the set iff
// its subjects match the identity (User exact, Group membership incl.
// system:authenticated, SA exact + SA synthetic groups). Index pre-filters must
// never drop a matching binding (under-inclusion would let two different
// identities share a key).
func TestKey423_SubjectBindingSet_MatchesEvaluatorSubjectSemantics(t *testing.T) {
	newTestWatcher(t, k423Fixture()...)
	snap := cache.Global().Snapshot()
	type ident struct {
		u string
		g []string
	}
	for _, id := range []ident{
		{"alice", []string{"portal"}}, {"carol", []string{"portal"}}, {"u1", nil}, {"u2", nil},
		{"system:serviceaccount:apps:deployer", nil}, {"nobody", nil}, {"", []string{"portal"}},
	} {
		got := map[string]bool{}
		for _, s := range rbac.SubjectBindingIDs(snap, id.u, id.g) {
			got[s] = true
		}
		want := map[string]bool{}
		matches := func(subs []rbacv1.Subject) bool {
			groups := append(append([]string{}, id.g...), "system:authenticated")
			if ns, _, isSA := rbac.ParseServiceAccountUsernameForTest(id.u); isSA {
				groups = rbac.EffectiveGroupsForTest(groups, true, ns)
			}
			for _, s := range subs {
				switch s.Kind {
				case "User":
					if s.Name == id.u {
						return true
					}
				case "Group":
					for _, g := range groups {
						if s.Name == g {
							return true
						}
					}
				case "ServiceAccount":
					if ns, n, ok := rbac.ParseServiceAccountUsernameForTest(id.u); ok && s.Namespace == ns && s.Name == n {
						return true
					}
				}
			}
			return false
		}
		for _, crb := range snap.ClusterRoleBindings {
			if matches(crb.Subjects) {
				want[cache.BindingUIDFromCRB(crb)] = true
			}
		}
		for _, rbs := range snap.RoleBindingsByNS {
			for _, rb := range rbs {
				if matches(rb.Subjects) {
					want[cache.BindingUIDFromRB(rb)] = true
				}
			}
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("identity %q %v: index-derived set %v != brute-force set %v", id.u, id.g, got, want)
		}
	}
}

// TestKey423_SubjectBindingSet_NewSnapshotNotServedStaleMemo — the memo is bound
// to the snapshot pointer: after a republish that adds a binding for carol, her
// digest must change on the very next call.
func TestKey423_SubjectBindingSet_NewSnapshotNotServedStaleMemo(t *testing.T) {
	newTestWatcher(t, k423Fixture()...)
	rbac.ResetSubjectBindingSetMemoForTest()
	before := rbac.SubjectBindingSetDigest("carol", []string{"portal"})
	_ = rbac.SubjectBindingSetDigest("carol", []string{"portal"}) // memo hit
	if hits, _ := rbac.SubjectBindingSetMemoStatsForTest(); hits == 0 {
		t.Fatalf("PRE: the second identical call must be a memo hit")
	}

	old := cache.Global().Snapshot()
	next := &cache.RBACSnapshot{
		PublishSeq:          old.PublishSeq, // deliberately REUSED: the memo must key on the pointer
		ClusterRoleBindings: old.ClusterRoleBindings,
		RoleBindingsByNS:    map[string][]*rbacv1.RoleBinding{},
		ClusterRolesByName:  old.ClusterRolesByName,
		RolesByNSName:       old.RolesByNSName,
	}
	for ns, rbs := range old.RoleBindingsByNS {
		next.RoleBindingsByNS[ns] = append([]*rbacv1.RoleBinding(nil), rbs...)
	}
	next.RoleBindingsByNS["tenant-c"] = append(next.RoleBindingsByNS["tenant-c"],
		k423RB("tenant-c", "carol-extra", "uid-rb-carol", "reader", k423User("carol")))
	cache.RebuildSubjectIndexesForTest(next)
	cache.PublishRBACSnapshotForTest(next)

	after := rbac.SubjectBindingSetDigest("carol", []string{"portal"})
	if after == before {
		t.Fatalf("STALE MEMO: carol gained a binding in a new snapshot but her digest did not change")
	}
}

// BenchmarkSubjectBindingSetDigest — production-shaped snapshot: 1,000 users in
// one group, the group bound in 2,000 namespaces (per-composition RBAC), 50
// CRBs, plus 1,000 User-subject RBs.
//
//	memo-hit: the per-request cost after the first request of an identity in
//	          a snapshot generation (the steady state).
//	cold:     the per-identity-per-snapshot cost (walks only that identity's
//	          index buckets).
func BenchmarkSubjectBindingSetDigest(b *testing.B) {
	snap := &cache.RBACSnapshot{RoleBindingsByNS: map[string][]*rbacv1.RoleBinding{}}
	for i := 0; i < 50; i++ {
		snap.ClusterRoleBindings = append(snap.ClusterRoleBindings,
			k423CRB(fmt.Sprintf("crb-%d", i), fmt.Sprintf("uid-crb-%d", i), "reader", k423Group(fmt.Sprintf("g-%d", i))))
	}
	snap.ClusterRoleBindings = append(snap.ClusterRoleBindings,
		k423CRB("authn", "uid-authn", "reader", k423Group("system:authenticated")))
	for i := 0; i < 2000; i++ {
		ns := fmt.Sprintf("ns-%d", i)
		snap.RoleBindingsByNS[ns] = append(snap.RoleBindingsByNS[ns],
			k423RB(ns, "devs", fmt.Sprintf("uid-rb-devs-%d", i), "reader", k423Group("devs")))
	}
	for i := 0; i < 1000; i++ {
		ns := fmt.Sprintf("ns-%d", i)
		snap.RoleBindingsByNS[ns] = append(snap.RoleBindingsByNS[ns],
			k423RB(ns, "user", fmt.Sprintf("uid-rb-user-%d", i), "reader", k423User(fmt.Sprintf("user-%d", i))))
	}
	cache.RebuildSubjectIndexesForTest(snap)

	b.Run("memo-hit", func(b *testing.B) {
		rbac.ResetSubjectBindingSetMemoForTest()
		_ = rbac.SubjectBindingSetDigestForSnapshotForTest(snap, "user-7", []string{"devs", "g-3"})
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = rbac.SubjectBindingSetDigestForSnapshotForTest(snap, "user-7", []string{"devs", "g-3"})
		}
	})
	b.Run("cold", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rbac.ResetSubjectBindingSetMemoForTest()
			_ = rbac.SubjectBindingSetDigestForSnapshotForTest(snap, "user-7", []string{"devs", "g-3"})
		}
	})
}
