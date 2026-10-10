// issue561_binding_set_soundness_test.go — THE FALSIFIER for #423's soundness
// claim, on the carrier it was never written for.
//
// # WHAT IS ACTUALLY BEING TESTED, AND WHY IT IS NOT A UAF TEST
//
// #561 asks whether the userAccessFilter Put-decline is still necessary. The
// decline's own justification (uaf_shortttl.go) describes two users who share a
// cell: one ClusterRoleBinding granting a group they are both in, divergent
// per-namespace RoleBindings, and therefore an IDENTICAL cache key. That was
// true against the pre-#423 key.
//
// #423 then folded SubjectBindingSet — the digest of every binding whose
// subjects match the requester — into the key. If that fold is SOUND, the two
// users in that story no longer share a key and the decline is obsolete.
//
// Soundness means exactly one thing:
//
//	equal SubjectBindingSet  ⟹  equal RBAC verdict, for every question asked
//
// That is #423's own claim ("every verdict rbac.EvaluateRBAC can return is a function
// of this set"), and it is the claim NOTHING in the tree tests. #424 had to
// weaken the A-1 arms' precondition to keep them meaningful — their "these two
// users share one cell" is now asserted on every key dimension EXCEPT the #423
// fold — so the dimension that would catch this was explicitly removed from the
// only tests in the area.
//
// SO A RED HERE IS NOT "THE CHEAP #561 FIX FAILED". A red means two identities
// exist that derive the SAME cache key and deserve DIFFERENT answers, which is a
// live leak in today's key affecting EVERY identity-bound cached class — not a
// UAF story at all, and far more urgent than #561. Read a failure that way.
//
// WHY THE VERDICT IS THE RIGHT OBSERVABLE. A userAccessFilter cannot narrow on
// anything else. The CRD schema admits exactly six fields — group, resource,
// resourcesFrom, nameFrom, namespaceFrom, verb — where the two `*From` fields
// are JQ paths evaluated against each RETURNED OBJECT and the rest are static.
// There is no syntax in which a filter can reference the requester. refilter.go's
// evalSingle therefore reduces to one rbac.EvaluateRBAC call per object, which is what
// this test drives directly. Equal verdicts over the whole question grid ⟹
// byte-identical narrowed bodies, with no need to run the filter itself.
//
// WHY THE PRECONDITION IS ON SubjectBindingSet ALONE, not on the whole key.
// ComputeKey folds BindingUID *and* SubjectBindingSet (and RBACSubGen). A real
// collision needs all of them equal. Testing on the binding set alone therefore
// considers a strict SUPERSET of collision candidates: if no divergence exists
// even under this weaker precondition, none exists under the real key either.
// Conservative in the safe direction.
//
// VACUITY IS THE REAL RISK IN A TEST SHAPED LIKE THIS — "no counterexample
// found" is the pass condition, which is also what a test that examines nothing
// reports. Both guards below are t.Fatal, never t.Skip:
//
//	(i)  at least one pair of DISTINCT identities must share a binding-set
//	     digest, or the implication is never exercised;
//	(ii) at least one pair with DIFFERENT digests must DIVERGE somewhere on the
//	     grid, or the grid cannot detect divergence at all and (i) proves
//	     nothing.
//
// (ii) is the one that matters. Without it, a grid of questions nobody is
// granted would pass this test while being blind to every leak.
package evaltest

import (
	"context"
	"sort"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// ─────────────────────────────────────────────────────────────────────
// Fixture: a deliberately awkward RBAC world.
//
// Shapes chosen because each is a plausible way for the binding set and the
// verdict to come apart: group grants that several identities match, two
// bindings in DIFFERENT namespaces referencing the SAME role (so binding
// identity and granted rules diverge), a wildcard rule, a resourceNames-scoped
// rule (which the authorizer refuses under collection verbs), and a
// ServiceAccount subject whose synthetic groups are expanded by the evaluator
// rather than carried by the caller.
// ─────────────────────────────────────────────────────────────────────

const (
	nsA = "f561-tenant-a"
	nsB = "f561-tenant-b"
	nsC = "f561-tenant-c"
)

// f561World seeds the package's standard test watcher, so the snapshot is built
// by PRODUCTION code (indexes included) rather than hand-assembled here.
func f561World(t *testing.T) {
	t.Helper()
	newTestWatcher(t,
		// Cluster-wide grants. The group ones are what make equal-digest pairs
		// exist at all — several identities match the same binding.
		clusterRole("f561-view-pods", rule([]string{""}, []string{"pods"}, []string{"get", "list", "watch"})),
		clusterRoleBinding("f561-crb-platform", "f561-view-pods", groupSubject("f561-platform")),

		clusterRole("f561-view-all", rule([]string{"", "apps", "composition.krateo.io"},
			[]string{"pods", "deployments", "compositions"}, []string{"get", "list", "watch"})),
		clusterRoleBinding("f561-crb-readers", "f561-view-all", groupSubject("f561-readers")),

		// A user-specific cluster grant, so some identities genuinely differ.
		clusterRole("f561-star", rule([]string{"*"}, []string{"*"}, []string{"*"})),
		clusterRoleBinding("f561-crb-root", "f561-star", userSubject("f561-root")),

		// A ServiceAccount subject: the evaluator expands its synthetic groups
		// itself rather than the caller supplying them.
		clusterRoleBinding("f561-crb-sa", "f561-view-pods", saSubject("krateo-system", "f561-runner")),

		// SAME role name, DIFFERENT namespaces, divergent group subjects. This
		// is the shape the decline's own justification describes.
		role(nsA, "f561-ns-view", rule([]string{"composition.krateo.io"}, []string{"compositions"}, []string{"get", "list", "watch"})),
		roleBinding(nsA, "f561-rb-view", "Role", "f561-ns-view", groupSubject("f561-tenant-a-users")),
		role(nsB, "f561-ns-view", rule([]string{"composition.krateo.io"}, []string{"compositions"}, []string{"get", "list", "watch"})),
		roleBinding(nsB, "f561-rb-view", "Role", "f561-ns-view", groupSubject("f561-tenant-b-users")),
	)
}

// ─────────────────────────────────────────────────────────────────────
// The identity population and the question grid.
// ─────────────────────────────────────────────────────────────────────

type identity561 struct {
	name     string
	username string
	groups   []string
}

func f561Identities() []identity561 {
	return []identity561{
		// Should share a binding set: same groups, different usernames.
		{"alice-platform", "alice", []string{"f561-platform"}},
		{"bob-platform", "bob", []string{"f561-platform"}},
		{"carol-platform", "carol", []string{"f561-platform"}},

		// Same groups in a DIFFERENT ORDER — must not change the digest.
		{"dave-pr", "dave", []string{"f561-platform", "f561-readers"}},
		{"erin-rp", "erin", []string{"f561-readers", "f561-platform"}},

		// Tenant-divergent: the exact shape the decline's justification cites.
		{"frank-a", "frank", []string{"f561-platform", "f561-tenant-a-users"}},
		{"grace-b", "grace", []string{"f561-platform", "f561-tenant-b-users"}},

		// Identities with nothing bound.
		{"heidi-none", "heidi", nil},
		{"ivan-unbound", "ivan", []string{"f561-unbound-group"}},

		// A cluster-wide "*" grant, so the grid discriminates strongly.
		{"root", "f561-root", nil},

		// ServiceAccount identities — synthetic groups expanded by the evaluator.
		{"sa-runner", "system:serviceaccount:krateo-system:f561-runner", nil},
		{"sa-other", "system:serviceaccount:krateo-system:f561-other", nil},
	}
}

type question561 struct {
	verb, group, resource, namespace, name string
}

func f561Grid() []question561 {
	var out []question561
	verbs := []string{"get", "list", "watch", "create"}
	coords := []struct{ group, resource string }{
		{"", "pods"},
		{"apps", "deployments"},
		{"composition.krateo.io", "compositions"},
	}
	namespaces := []string{"", nsA, nsB, nsC}
	names := []string{"", "some-object"}
	for _, v := range verbs {
		for _, c := range coords {
			for _, ns := range namespaces {
				for _, n := range names {
					out = append(out, question561{v, c.group, c.resource, ns, n})
				}
			}
		}
	}
	return out
}

// verdictVector evaluates the whole grid for one identity. The vector is the
// observable: two identities with equal vectors cannot be served different
// narrowed bodies by any userAccessFilter, because the filter has nothing else
// to ask about.
func verdictVector(t *testing.T, ctx context.Context, id identity561, grid []question561) []bool {
	t.Helper()
	out := make([]bool, len(grid))
	for i, q := range grid {
		allowed, _, err := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
			Username: id.username, Groups: id.groups,
			Verb: q.verb, Group: q.group, Resource: q.resource,
			Namespace: q.namespace, Name: q.name,
		})
		if err != nil {
			t.Fatalf("rbac.EvaluateRBAC(%s, %+v) errored: %v", id.name, q, err)
		}
		out[i] = allowed
	}
	return out
}

func firstDiff(a, b []bool, grid []question561) (int, bool) {
	for i := range a {
		if a[i] != b[i] {
			return i, true
		}
	}
	return 0, false
}

// ─────────────────────────────────────────────────────────────────────
// THE FALSIFIER
// ─────────────────────────────────────────────────────────────────────

func TestIssue561_EqualBindingSetImpliesEqualVerdicts(t *testing.T) {
	ctx := context.Background()
	f561World(t)
	snap := cache.RBACSnapshotForTest()
	if snap == nil {
		t.Fatal("no published RBAC snapshot after newTestWatcher — fixture not wired")
	}
	rbac.ResetSubjectBindingSetMemoForTest()

	ids := f561Identities()
	grid := f561Grid()

	digest := map[string]string{}
	vector := map[string][]bool{}
	for _, id := range ids {
		digest[id.name] = rbac.SubjectBindingSetDigestForSnapshotForTest(snap, id.username, id.groups)
		vector[id.name] = verdictVector(t, ctx, id, grid)
		if digest[id.name] == "" {
			t.Fatalf("identity %s produced an EMPTY binding-set digest — the snapshot or the "+
				"cache-enabled gate is not wired, and every pair would collide vacuously", id.name)
		}
	}

	// ── Precondition (i): the implication must actually be exercised.
	var sharedPairs, distinctPairs [][2]identity561
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if digest[ids[i].name] == digest[ids[j].name] {
				sharedPairs = append(sharedPairs, [2]identity561{ids[i], ids[j]})
			} else {
				distinctPairs = append(distinctPairs, [2]identity561{ids[i], ids[j]})
			}
		}
	}
	if len(sharedPairs) == 0 {
		t.Fatal("VACUOUS: no two distinct identities share a binding-set digest, so " +
			"'equal digest implies equal verdicts' was never tested. The fixture must " +
			"contain identities that match the same set of bindings.")
	}

	// ── Precondition (i-b): at least one SHARED pair must hold a NON-EMPTY
	// binding set. Identities that match no binding at all share the digest of
	// the empty set and are granted nothing, so they agree on every question
	// trivially. A fixture made only of those would satisfy (i) while testing
	// nothing — this was a real defect in the first draft of this test, where 3
	// of 7 shared pairs were empty-set artifacts.
	nonEmptyShared := 0
	for _, p := range sharedPairs {
		if len(rbac.SubjectBindingIDs(snap, p[0].username, p[0].groups)) > 0 {
			nonEmptyShared++
		}
	}
	if nonEmptyShared == 0 {
		t.Fatal("VACUOUS: every equal-digest pair matches NO bindings, so they agree " +
			"only because neither is granted anything. The fixture needs identities " +
			"that share a non-empty set of bindings.")
	}

	// ── Precondition (ii): the grid must be ABLE to see a divergence.
	// Without this, a grid nobody is granted anything on passes while blind.
	sawDivergence := false
	for _, p := range distinctPairs {
		if _, differs := firstDiff(vector[p[0].name], vector[p[1].name], grid); differs {
			sawDivergence = true
			break
		}
	}
	if !sawDivergence {
		t.Fatal("VACUOUS: no pair of identities diverges anywhere on the question grid, so " +
			"the grid cannot detect a divergence and precondition (i) proves nothing. " +
			"Widen the grid or the fixture.")
	}

	t.Logf("exercised %d identities, %d grid questions, %d equal-digest pairs (%d with a NON-EMPTY binding set), %d distinct-digest pairs",
		len(ids), len(grid), len(sharedPairs), nonEmptyShared, len(distinctPairs))

	// ── THE CLAIM.
	for _, p := range sharedPairs {
		a, b := p[0], p[1]
		if idx, differs := firstDiff(vector[a.name], vector[b.name], grid); differs {
			q := grid[idx]
			t.Errorf(`LEAK: %s and %s derive the SAME binding-set digest but DIFFERENT verdicts.

  digest      %s
  question    verb=%q group=%q resource=%q namespace=%q name=%q
  %-14s allowed=%v   (user=%q groups=%v)
  %-14s allowed=%v   (user=%q groups=%v)

This is NOT a userAccessFilter finding. These two identities fold to the same
SubjectBindingSet, so #423's claim that every rbac.EvaluateRBAC verdict is a function
of that set is FALSE, and any identity-bound cached cell keyed on it can serve
one of these users the other's rows.`,
				a.name, b.name,
				digest[a.name],
				q.verb, q.group, q.resource, q.namespace, q.name,
				a.name, vector[a.name][idx], a.username, a.groups,
				b.name, vector[b.name][idx], b.username, b.groups)
		}
	}
}

// TestIssue561_DigestIsOrderInsensitive pins the one property the pair-scan
// above would silently depend on: a digest that varied with group ORDER would
// make equal-digest pairs rarer than they are in production, weakening the
// falsifier without failing it.
func TestIssue561_DigestIsOrderInsensitive(t *testing.T) {
	f561World(t)
	snap := cache.RBACSnapshotForTest()
	if snap == nil {
		t.Fatal("no published RBAC snapshot after newTestWatcher — fixture not wired")
	}
	rbac.ResetSubjectBindingSetMemoForTest()

	d1 := rbac.SubjectBindingSetDigestForSnapshotForTest(snap, "dave", []string{"f561-platform", "f561-readers"})
	d2 := rbac.SubjectBindingSetDigestForSnapshotForTest(snap, "dave", []string{"f561-readers", "f561-platform"})
	if d1 == "" {
		t.Fatal("empty digest — snapshot not wired")
	}
	if d1 != d2 {
		t.Errorf("binding-set digest is order-sensitive: %s vs %s", d1, d2)
	}
}

// TestIssue561_NamespaceSeparatesRoleBindingIdentity pins the defence that two
// RoleBindings with the same name in different namespaces do not collapse to
// one binding identity. If they did, tenant-a-users and tenant-b-users would
// share a digest while being granted in different namespaces — a leak the
// pair-scan would then report, but this names the cause directly.
func TestIssue561_NamespaceSeparatesRoleBindingIdentity(t *testing.T) {
	f561World(t)
	snap := cache.RBACSnapshotForTest()
	if snap == nil {
		t.Fatal("no published RBAC snapshot after newTestWatcher — fixture not wired")
	}
	rbac.ResetSubjectBindingSetMemoForTest()

	da := rbac.SubjectBindingSetDigestForSnapshotForTest(snap, "frank", []string{"f561-platform", "f561-tenant-a-users"})
	db := rbac.SubjectBindingSetDigestForSnapshotForTest(snap, "grace", []string{"f561-platform", "f561-tenant-b-users"})
	if da == "" || db == "" {
		t.Fatal("empty digest — snapshot not wired")
	}
	if da == db {
		ida := rbac.SubjectBindingIDs(snap, "frank", []string{"f561-platform", "f561-tenant-a-users"})
		idb := rbac.SubjectBindingIDs(snap, "grace", []string{"f561-platform", "f561-tenant-b-users"})
		sort.Strings(ida)
		sort.Strings(idb)
		t.Errorf("tenant-a and tenant-b identities share a binding-set digest %s\n  frank: %v\n  grace: %v\n%s",
			da, ida, idb,
			"Two RoleBindings in different namespaces have collapsed to one identity.")
	}
}
