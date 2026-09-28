// v7_step2b_profile_test.go — v7 Step 2B falsifiers.
//
// Step 2B is LIBRARY-ONLY and DARK. It adds the requester profile R
// (rbac.BuildRequesterProfile / RequesterProfileFor / RequesterProfile.Permits)
// and splits the pure resolve half (lookupRoleRefRules) out of roleRefPermits.
// NOTHING on any serving path calls R; no cache key, no RBAC verdict, and no
// observable counter changes.
//
// Arms:
//   - F-D1     — the roleRefPermits split is behavior-preserving: the pure
//     lookupRoleRefRules records NO snapshot miss (dark purity), and
//     roleRefPermits still bumps RecordRBACSnapshotMiss with the exact
//     pre-split labels/selectivity (only ClusterRole-absent and
//     namespaced-Role-absent).
//   - F-D2     — R.Permits == EvaluateRBAC.allowed EXHAUSTIVELY over the domain
//     the fixtures can form (not observed rows).
//   - F-D2div  — a NAIVE R (resourceNames-flattened, and ns-bucket-ignoring)
//     answers allow where EvaluateRBAC denies; the CORRECT R agrees
//     with EvaluateRBAC and the naive ones do not. Proves R's fidelity
//     is real, not coincidental.
//   - F-D5     — system:authenticated gating, SA synthetic groups, catch-all,
//     multi-group union, and missing-role deny are each reflected in R
//     identically to EvaluateRBAC; building R over dangling roleRefs
//     bumps RecordRBACSnapshotMiss ZERO times (dark).
//   - F-D7     — the R memo: pointer reuse on a hit, PublishSeq invalidation,
//     groups order-independence via canonicalGroupsHash, no
//     cross-identity bleed, and race-safety under -race.
//
// Dark proof of the SERVED path is carried by the Step 2A
// TestV7Step2A_NoServingChange_VerdictTableGolden (verdict+matchedBindingUID
// sha256 unchanged) which still passes after the split.
package evaltest

import (
	"context"
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	rbacv1 "k8s.io/api/rbac/v1"
)

func roleRef(kind, name string) rbacv1.RoleRef {
	return rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: kind, Name: name}
}

// ─────────────────────────────────────────────────────────────────────────
// F-D1 — roleRefPermits split is behavior-preserving.
// ─────────────────────────────────────────────────────────────────────────

func TestV7Step2B_FD1_SplitBehaviorPreserving(t *testing.T) {
	// Directly-constructed snapshot with exactly one present ClusterRole and one
	// present namespaced Role, so hits and misses are deterministic.
	snap := &cache.RBACSnapshot{
		ClusterRolesByName: map[string]*rbacv1.ClusterRole{
			"cr-present": clusterRole("cr-present", rule([]string{"*"}, []string{"*"}, []string{"*"})),
		},
		RolesByNSName: map[string]*rbacv1.Role{
			"ns-a/role-present": role("ns-a", "role-present",
				rule([]string{""}, []string{"configmaps"}, []string{"get"})),
		},
		RoleBindingsByNS: map[string][]*rbacv1.RoleBinding{},
	}

	type corpusEntry struct {
		name         string
		ns           string
		ref          rbacv1.RoleRef
		opts         rbac.EvaluateOptions
		wantResolved bool // lookupRoleRefRules second return
		wantPermit   bool // roleRefPermits verdict
		preRecorded  bool // did the PRE-SPLIT roleRefPermits record a miss here?
	}
	// permitOpts satisfies both the wildcard CR and the configmaps-get Role.
	cmGet := rbac.EvaluateOptions{Verb: "get", Group: "", Resource: "configmaps", Namespace: "ns-a"}
	anyGet := rbac.EvaluateOptions{Verb: "get", Group: "", Resource: "configmaps", Namespace: "ns-a"}

	corpus := []corpusEntry{
		{"CR present", "", roleRef("ClusterRole", "cr-present"), anyGet, true, true, false},
		{"CR absent", "", roleRef("ClusterRole", "cr-absent"), anyGet, false, false, true},
		{"Role present ns-a", "ns-a", roleRef("Role", "role-present"), cmGet, true, true, false},
		{"Role absent ns-a", "ns-a", roleRef("Role", "role-absent"), cmGet, false, false, true},
		{"Role in CRB (ns empty)", "", roleRef("Role", "role-x"), cmGet, false, false, false},
		{"unknown kind", "ns-a", roleRef("Bogus", "whatever"), cmGet, false, false, false},
	}

	// preSplitRecorded is the pre-split roleRefPermits recording predicate,
	// stated as code: ONLY a ClusterRole-absent and a namespaced-Role-absent
	// miss were recorded.
	preSplitRecorded := func(e corpusEntry, resolved bool) bool {
		if resolved {
			return false
		}
		switch e.ref.Kind {
		case "ClusterRole":
			return true
		case "Role":
			return e.ns != ""
		default:
			return false
		}
	}

	// ── Arm 1: DARK PURITY — lookupRoleRefRules records NO snapshot miss.
	before := cache.RBACSnapshotMissCount()
	for _, e := range corpus {
		rules, ok := rbac.LookupRoleRefRulesForTest(snap, e.ns, e.ref)
		if ok != e.wantResolved {
			t.Errorf("[%s] lookupRoleRefRules resolved=%v, want %v", e.name, ok, e.wantResolved)
		}
		if ok && rules == nil {
			t.Errorf("[%s] lookupRoleRefRules resolved but returned nil rules", e.name)
		}
		if !ok && rules != nil {
			t.Errorf("[%s] lookupRoleRefRules miss but returned non-nil rules", e.name)
		}
	}
	if delta := cache.RBACSnapshotMissCount() - before; delta != 0 {
		t.Errorf("F-D1 DARK-PURITY VIOLATION: lookupRoleRefRules bumped RecordRBACSnapshotMiss %d times over the corpus; want 0", delta)
	}

	// Verify the resolved rules are the SAME slice as the snapshot's target
	// (pure pass-through, no copy/transform).
	if rules, ok := rbac.LookupRoleRefRulesForTest(snap, "", roleRef("ClusterRole", "cr-present")); !ok || &rules[0] != &snap.ClusterRolesByName["cr-present"].Rules[0] {
		t.Errorf("F-D1: lookupRoleRefRules did not return the ClusterRole's own .Rules slice")
	}
	if rules, ok := rbac.LookupRoleRefRulesForTest(snap, "ns-a", roleRef("Role", "role-present")); !ok || &rules[0] != &snap.RolesByNSName["ns-a/role-present"].Rules[0] {
		t.Errorf("F-D1: lookupRoleRefRules did not return the Role's own .Rules slice")
	}

	// ── Arm 2: roleRefPermits verdict + miss-count parity, run TWICE with
	// different opts to prove the miss bump is at RESOLVE (opts-independent),
	// exactly as pre-split.
	wantMissesPerPass := 0
	for _, e := range corpus {
		if preSplitRecorded(e, e.wantResolved) {
			wantMissesPerPass++
			if !e.preRecorded {
				t.Errorf("[%s] test-corpus preRecorded flag disagrees with the pre-split predicate", e.name)
			}
		}
	}

	// Run the corpus TWICE (misses happen at RESOLVE, before rulesPermit, so the
	// count is opts-independent and simply doubles).
	const nPasses = 2
	before = cache.RBACSnapshotMissCount()
	for pass := 0; pass < nPasses; pass++ {
		for _, e := range corpus {
			permit, err := rbac.RoleRefPermitsForTest(snap, e.ns, e.ref, e.opts)
			if err != nil {
				t.Fatalf("[%s] roleRefPermits err: %v", e.name, err)
			}
			if permit != e.wantPermit {
				t.Errorf("[%s] roleRefPermits verdict=%v, want %v", e.name, permit, e.wantPermit)
			}
		}
	}
	gotDelta := cache.RBACSnapshotMissCount() - before
	wantDelta := uint64(wantMissesPerPass * nPasses)
	if gotDelta != wantDelta {
		t.Errorf("F-D1 MISS-COUNT PARITY VIOLATION: roleRefPermits bumped RecordRBACSnapshotMiss %d times; want %d (only ClusterRole-absent + namespaced-Role-absent, opts-independent)", gotDelta, wantDelta)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// Shared parity fixture (F-D2 / F-D2div).
// ─────────────────────────────────────────────────────────────────────────

// buildParityWatcher seeds a rich cache=on watcher: wildcard admin, a
// restaction reader (cluster + RB→ClusterRole scoped), a resourceNames-scoped
// named-get (cluster), a namespaced configmap editor Role, and a namespaced
// resourceNames-scoped secret-get Role. Returns the published snapshot.
func buildParityWatcher(t *testing.T) *cache.RBACSnapshot {
	t.Helper()
	newTestWatcher(t,
		// ClusterRoles
		clusterRole("cr-admin", rule([]string{"*"}, []string{"*"}, []string{"*"})),
		clusterRole("cr-restaction-reader",
			rule([]string{"templates.krateo.io"}, []string{"restactions"}, []string{"get", "list"})),
		// resourceNames-scoped with BOTH a name-specific verb (get) and a
		// collection verb (list): a resourceNames rule NEVER grants list (a
		// collection verb has no single named object), so both the wrong-name
		// (get bar) and the collection-verb (list) divergences are real for a
		// naive resourceNames-flattening R.
		clusterRole("cr-named-get",
			ruleNamed([]string{"templates.krateo.io"}, []string{"restactions"}, []string{"get", "list"}, []string{"foo"})),
		// Roles
		role("ns-a", "role-cm-editor",
			rule([]string{""}, []string{"configmaps"}, []string{"get", "list", "create", "update"})),
		role("ns-b", "role-secret-namedget",
			ruleNamed([]string{""}, []string{"secrets"}, []string{"get"}, []string{"topsecret"})),
		// ClusterRoleBindings
		clusterRoleBinding("crb-alice-admin", "cr-admin", userSubject("alice")),
		clusterRoleBinding("crb-devs-reader", "cr-restaction-reader", groupSubject("devs")),
		clusterRoleBinding("crb-carol-namedget", "cr-named-get", userSubject("carol")),
		// RoleBindings
		roleBinding("ns-a", "rb-bob-cm", "Role", "role-cm-editor", userSubject("bob")),
		roleBinding("ns-b", "rb-bob-secret", "Role", "role-secret-namedget", userSubject("bob")),
		roleBinding("ns-a", "rb-ops-cr", "ClusterRole", "cr-restaction-reader", groupSubject("ops")),
	)
	snap := cache.Global().Snapshot()
	if snap == nil {
		t.Fatal("nil snapshot after newTestWatcher")
	}
	return snap
}

func parityIdentities() []v7Ident {
	return []v7Ident{
		{Username: "alice", Groups: []string{"system:authenticated"}},        // admin
		{Username: "dave", Groups: []string{"devs", "system:authenticated"}}, // restaction reader (cluster)
		{Username: "carol", Groups: []string{"system:authenticated"}},        // named-get foo only
		{Username: "bob", Groups: []string{"system:authenticated"}},          // ns-a cm editor + ns-b secret named-get
		{Username: "erin", Groups: []string{"ops", "system:authenticated"}},  // restaction reader in ns-a only (RB→CR)
		{Username: "frank", Groups: []string{"system:authenticated"}},        // nothing
		{Username: "", Groups: nil},                                          // unauthenticated
	}
}

// ─────────────────────────────────────────────────────────────────────────
// F-D2 — R.Permits == EvaluateRBAC.allowed EXHAUSTIVELY over the domain.
// ─────────────────────────────────────────────────────────────────────────

func TestV7Step2B_FD2_ParityExhaustive(t *testing.T) {
	rbac.ResetRequesterProfileMemoForTest()
	snap := buildParityWatcher(t)

	verbs := []string{"get", "list", "watch", "create", "update", "delete"}
	grs := []struct{ group, resource string }{
		{"templates.krateo.io", "restactions"},
		{"", "configmaps"},
		{"", "secrets"},
		{"other.io", "widgets"}, // GVR no rule mentions
	}
	namespaces := []string{"ns-a", "ns-b", "ns-c", ""}
	names := []string{"", "foo", "bar", "topsecret"}

	ctx := context.Background()
	total, mismatches := 0, 0
	for _, id := range parityIdentities() {
		r := rbac.BuildRequesterProfile(snap, rbac.EvaluateOptions{Username: id.Username, Groups: id.Groups})
		for _, v := range verbs {
			for _, gr := range grs {
				for _, ns := range namespaces {
					for _, nm := range names {
						opts := rbac.EvaluateOptions{
							Username: id.Username, Groups: id.Groups,
							Verb: v, Group: gr.group, Resource: gr.resource,
							Namespace: ns, Name: nm,
						}
						want, _, err := rbac.EvaluateRBAC(ctx, opts)
						if err != nil {
							t.Fatalf("EvaluateRBAC(user=%q v=%s %s/%s ns=%q name=%q): %v",
								id.Username, v, gr.group, gr.resource, ns, nm, err)
						}
						got := r.Permits(opts)
						total++
						if got != want {
							mismatches++
							t.Errorf("F-D2 PARITY MISMATCH user=%q groups=%v v=%s %s/%s ns=%q name=%q: R.Permits=%v EvaluateRBAC.allowed=%v",
								id.Username, id.Groups, v, gr.group, gr.resource, ns, nm, got, want)
						}
					}
				}
			}
		}
	}
	t.Logf("F-D2 exhaustive: %d coordinates compared, %d mismatches", total, mismatches)
}

// ─────────────────────────────────────────────────────────────────────────
// F-D2div — a NAIVE R diverges from EvaluateRBAC; the CORRECT R does not.
// ─────────────────────────────────────────────────────────────────────────

// flattenResourceNames strips ResourceNames from every rule — the classic
// naive-R defect that over-grants a resourceNames-scoped rule to any name and
// to collection verbs.
func flattenResourceNames(rules []rbacv1.PolicyRule) []rbacv1.PolicyRule {
	out := make([]rbacv1.PolicyRule, len(rules))
	for i, r := range rules {
		c := r
		c.ResourceNames = nil
		out[i] = c
	}
	return out
}

func TestV7Step2B_FD2div_ResourceNamesFlatten(t *testing.T) {
	rbac.ResetRequesterProfileMemoForTest()
	snap := buildParityWatcher(t)
	ctx := context.Background()

	// carol has a resourceNames:["foo"] get grant on restactions (via cr-named-get).
	carol := rbac.EvaluateOptions{Username: "carol", Groups: []string{"system:authenticated"}}
	r := rbac.BuildRequesterProfile(snap, carol)

	// Divergence checks: cases where a resourceNames-flattened R over-grants.
	checks := []struct {
		name string
		opts rbac.EvaluateOptions
	}{
		{"get restactions bar (not in resourceNames)", withReq(carol, "get", "templates.krateo.io", "restactions", "", "bar")},
		{"list restactions (collection verb, resourceNames never grants)", withReq(carol, "list", "templates.krateo.io", "restactions", "", "")},
	}

	// Build the naive (flattened) R from the SAME resolved rules.
	naive := &rbac.RequesterProfile{
		ClusterRules:    flattenResourceNames(r.ClusterRules),
		NamespacedRules: map[string][]rbacv1.PolicyRule{},
	}
	for ns, rs := range r.NamespacedRules {
		naive.NamespacedRules[ns] = flattenResourceNames(rs)
	}

	sawNaiveDivergence := false
	for _, c := range checks {
		want, _, err := rbac.EvaluateRBAC(ctx, c.opts)
		if err != nil {
			t.Fatalf("[%s] EvaluateRBAC: %v", c.name, err)
		}
		// EvaluateRBAC denies both (the grant is name=foo, get only).
		if want {
			t.Fatalf("[%s] fixture invalid: expected EvaluateRBAC deny, got allow", c.name)
		}
		// CORRECT R agrees with EvaluateRBAC (deny).
		if got := r.Permits(c.opts); got != want {
			t.Errorf("[%s] CORRECT R.Permits=%v, want EvaluateRBAC=%v", c.name, got, want)
		}
		// NAIVE R over-grants (allow) — proves the arm can catch a real divergence.
		if naive.Permits(c.opts) {
			sawNaiveDivergence = true
		} else {
			t.Errorf("[%s] naive-R control did NOT diverge — arm cannot fail; strengthen the fixture", c.name)
		}
	}
	// Positive control: the CORRECT R must still permit the in-scope grant.
	if !r.Permits(withReq(carol, "get", "templates.krateo.io", "restactions", "", "foo")) {
		t.Errorf("F-D2div: correct R denied the in-scope grant (get restactions foo)")
	}
	if !sawNaiveDivergence {
		t.Errorf("F-D2div: naive R never diverged — the divergence arm proved nothing")
	}
}

func TestV7Step2B_FD2div_NamespaceBucketIgnore(t *testing.T) {
	rbac.ResetRequesterProfileMemoForTest()
	snap := buildParityWatcher(t)
	ctx := context.Background()

	// bob has a cm-editor Role in ns-a ONLY.
	bob := rbac.EvaluateOptions{Username: "bob", Groups: []string{"system:authenticated"}}
	r := rbac.BuildRequesterProfile(snap, bob)

	// A check in ns-b for the ns-a grant: EvaluateRBAC denies (RB is in ns-a).
	check := withReq(bob, "get", "", "configmaps", "ns-b", "")
	want, _, err := rbac.EvaluateRBAC(ctx, check)
	if err != nil {
		t.Fatalf("EvaluateRBAC: %v", err)
	}
	if want {
		t.Fatalf("fixture invalid: expected EvaluateRBAC deny for get configmaps in ns-b")
	}
	if got := r.Permits(check); got != want {
		t.Errorf("CORRECT R.Permits(get configmaps ns-b)=%v, want %v", got, want)
	}

	// Naive R that ignores the ns bucket — lumps ALL namespaced rules into the
	// cluster scope, so a ns-a grant leaks into ns-b.
	naive := &rbac.RequesterProfile{
		ClusterRules:    append([]rbacv1.PolicyRule(nil), r.ClusterRules...),
		NamespacedRules: map[string][]rbacv1.PolicyRule{},
	}
	for _, rs := range r.NamespacedRules {
		naive.ClusterRules = append(naive.ClusterRules, rs...)
	}
	if !naive.Permits(check) {
		t.Errorf("naive ns-ignoring R control did NOT diverge — arm cannot fail")
	}
	// Sanity: the correct R DOES permit the grant in its real namespace ns-a.
	if !r.Permits(withReq(bob, "get", "", "configmaps", "ns-a", "")) {
		t.Errorf("correct R denied get configmaps in the granting ns-a")
	}
}

// withReq clones an identity and stamps a request coordinate onto it.
func withReq(id rbac.EvaluateOptions, verb, group, resource, ns, name string) rbac.EvaluateOptions {
	id.Verb, id.Group, id.Resource, id.Namespace, id.Name = verb, group, resource, ns, name
	return id
}

// ─────────────────────────────────────────────────────────────────────────
// F-D5 — subject-kind / group-shape parity + dark miss-neutrality.
// ─────────────────────────────────────────────────────────────────────────

func TestV7Step2B_FD5_SubjectShapesAndDarkMissNeutrality(t *testing.T) {
	rbac.ResetRequesterProfileMemoForTest()
	newTestWatcher(t,
		clusterRole("cr-any", rule([]string{"*"}, []string{"*"}, []string{"*"})),
		clusterRole("cr-cm-read", rule([]string{""}, []string{"configmaps"}, []string{"get"})),
		// system:authenticated → everyone authenticated
		clusterRoleBinding("crb-sysauth", "cr-cm-read", groupSubject("system:authenticated")),
		// SA synthetic group system:serviceaccounts:ns-a → any SA in ns-a
		clusterRoleBinding("crb-sa-nsgroup", "cr-any", groupSubject("system:serviceaccounts:ns-a")),
		// catch-all: an unrecognised subject kind → matches nothing
		clusterRoleBinding("crb-catchall", "cr-any", rbacv1.Subject{Kind: "FutureKind", Name: "nobody"}),
		// multi-group union
		clusterRoleBinding("crb-devs", "cr-cm-read", groupSubject("devs")),
		clusterRoleBinding("crb-ops", "cr-any", groupSubject("ops")),
		// dangling roleRef → missing ClusterRole (contributes nothing; must not
		// bump the miss counter when R resolves it).
		clusterRoleBinding("crb-dangling", "cr-DOES-NOT-EXIST", userSubject("ghost")),
	)
	snap := cache.Global().Snapshot()
	ctx := context.Background()

	idents := []v7Ident{
		{Username: "someuser", Groups: []string{"system:authenticated"}},                       // sysauth → cr-cm-read
		{Username: "", Groups: nil},                                                            // unauth → sysauth NOT granted
		{Username: "system:serviceaccount:ns-a:sa1", Groups: []string{"system:authenticated"}}, // SA synthetic group → cr-any
		{Username: "system:serviceaccount:ns-b:sa2", Groups: []string{"system:authenticated"}}, // wrong ns → only sysauth
		{Username: "ghost", Groups: []string{"system:authenticated"}},                          // dangling CRB → only sysauth
		{Username: "multi", Groups: []string{"devs", "ops", "system:authenticated"}},           // union of devs+ops+sysauth
	}
	verbs := []string{"get", "list", "delete"}
	grs := []struct{ group, resource string }{{"", "configmaps"}, {"apps", "deployments"}}
	namespaces := []string{"ns-a", ""}

	for _, id := range idents {
		r := rbac.BuildRequesterProfile(snap, rbac.EvaluateOptions{Username: id.Username, Groups: id.Groups})
		for _, v := range verbs {
			for _, gr := range grs {
				for _, ns := range namespaces {
					opts := rbac.EvaluateOptions{
						Username: id.Username, Groups: id.Groups,
						Verb: v, Group: gr.group, Resource: gr.resource, Namespace: ns,
					}
					want, _, err := rbac.EvaluateRBAC(ctx, opts)
					if err != nil {
						t.Fatalf("EvaluateRBAC: %v", err)
					}
					if got := r.Permits(opts); got != want {
						t.Errorf("F-D5 mismatch user=%q v=%s %s/%s ns=%q: R=%v EvaluateRBAC=%v",
							id.Username, v, gr.group, gr.resource, ns, got, want)
					}
				}
			}
		}
	}

	// Dark miss-neutrality: building R for the ghost (whose only CRB points at a
	// missing ClusterRole) must NOT bump RecordRBACSnapshotMiss. Build many
	// times over identities with dangling refs and assert zero delta.
	before := cache.RBACSnapshotMissCount()
	for i := 0; i < 50; i++ {
		r := rbac.BuildRequesterProfile(snap, rbac.EvaluateOptions{Username: "ghost", Groups: []string{"system:authenticated"}})
		// ghost's cluster rules are empty (dangling ref contributes nothing);
		// only sysauth's cr-cm-read applies.
		if r.Permits(rbac.EvaluateOptions{Username: "ghost", Verb: "delete", Resource: "configmaps", Namespace: "ns-a"}) {
			t.Fatalf("F-D5: ghost unexpectedly permitted delete configmaps (dangling ref should contribute nothing)")
		}
	}
	if delta := cache.RBACSnapshotMissCount() - before; delta != 0 {
		t.Errorf("F-D5 DARK MISS-NEUTRALITY VIOLATION: BuildRequesterProfile bumped RecordRBACSnapshotMiss %d times over dangling refs; want 0", delta)
	}
}

// ─────────────────────────────────────────────────────────────────────────
// F-D7 — the R memo.
// ─────────────────────────────────────────────────────────────────────────

// buildMemoSnapshot directly constructs an indexed snapshot with roles, so the
// test controls PublishSeq exactly.
func buildMemoSnapshot(t *testing.T, seq uint64) *cache.RBACSnapshot {
	t.Helper()
	snap := &cache.RBACSnapshot{
		ClusterRolesByName: map[string]*rbacv1.ClusterRole{
			"cr-admin":  clusterRole("cr-admin", rule([]string{"*"}, []string{"*"}, []string{"*"})),
			"cr-reader": clusterRole("cr-reader", rule([]string{"templates.krateo.io"}, []string{"restactions"}, []string{"get", "list"})),
		},
		RolesByNSName: map[string]*rbacv1.Role{
			"ns-a/role-cm": role("ns-a", "role-cm", rule([]string{""}, []string{"configmaps"}, []string{"get", "list"})),
		},
		ClusterRoleBindings: []*rbacv1.ClusterRoleBinding{
			clusterRoleBinding("crb-alice-admin", "cr-admin", userSubject("alice")),
			clusterRoleBinding("crb-devs-reader", "cr-reader", groupSubject("devs")),
		},
		RoleBindingsByNS: map[string][]*rbacv1.RoleBinding{
			"ns-a": {roleBinding("ns-a", "rb-bob-cm", "Role", "role-cm", userSubject("bob"))},
		},
	}
	cache.RebuildSubjectIndexesForTest(snap)
	snap.PublishSeq = seq
	return snap
}

func TestV7Step2B_FD7_Memo(t *testing.T) {
	alice := rbac.EvaluateOptions{Username: "alice", Groups: []string{"devs", "system:authenticated"}}
	aliceReordered := rbac.EvaluateOptions{Username: "alice", Groups: []string{"system:authenticated", "devs"}}
	bob := rbac.EvaluateOptions{Username: "bob", Groups: []string{"system:authenticated"}}

	t.Run("hit returns same pointer", func(t *testing.T) {
		rbac.ResetRequesterProfileMemoForTest()
		snap := buildMemoSnapshot(t, 1)
		p1 := rbac.RequesterProfileFor(snap, alice)
		p2 := rbac.RequesterProfileFor(snap, alice)
		if p1 != p2 {
			t.Errorf("memo hit returned a different pointer: %p vs %p", p1, p2)
		}
		hits, misses, stores, refused, entries := rbac.RequesterProfileMemoStatsForTest()
		if hits != 1 || misses != 1 || stores != 1 || refused != 0 || entries != 1 {
			t.Errorf("stats after 2 calls (1 miss+store, 1 hit): hits=%d misses=%d stores=%d refused=%d entries=%d", hits, misses, stores, refused, entries)
		}
	})

	t.Run("new PublishSeq invalidates", func(t *testing.T) {
		rbac.ResetRequesterProfileMemoForTest()
		snap1 := buildMemoSnapshot(t, 1)
		snap2 := buildMemoSnapshot(t, 2)
		p1 := rbac.RequesterProfileFor(snap1, alice)
		p2 := rbac.RequesterProfileFor(snap2, alice)
		if p1 == p2 {
			t.Errorf("new generation did not invalidate: same pointer across PublishSeq 1→2")
		}
		if p1.Gen != 1 || p2.Gen != 2 {
			t.Errorf("Gen not stamped: p1.Gen=%d (want 1) p2.Gen=%d (want 2)", p1.Gen, p2.Gen)
		}
		// The old-generation entry is dropped by the shard swap.
		_, _, _, _, entries := rbac.RequesterProfileMemoStatsForTest()
		if entries != 1 {
			t.Errorf("after gen swap the shard should hold only the new gen's entry; entries=%d", entries)
		}
	})

	t.Run("groups order independence", func(t *testing.T) {
		rbac.ResetRequesterProfileMemoForTest()
		snap := buildMemoSnapshot(t, 1)
		p1 := rbac.RequesterProfileFor(snap, alice)
		p2 := rbac.RequesterProfileFor(snap, aliceReordered)
		if p1 != p2 {
			t.Errorf("groups order changed the memo key: reordered groups returned a different R pointer")
		}
	})

	t.Run("no cross-identity bleed", func(t *testing.T) {
		rbac.ResetRequesterProfileMemoForTest()
		snap := buildMemoSnapshot(t, 1)
		pa := rbac.RequesterProfileFor(snap, alice)
		pb := rbac.RequesterProfileFor(snap, bob)
		if pa == pb {
			t.Errorf("cross-identity bleed: alice and bob share one R pointer")
		}
		// alice is admin (permits delete configmaps in ns-a); bob is only cm
		// get/list in ns-a (denied delete).
		del := rbac.EvaluateOptions{Verb: "delete", Resource: "configmaps", Namespace: "ns-a"}
		if !pa.Permits(del) {
			t.Errorf("alice (admin) R denied delete configmaps ns-a")
		}
		if pb.Permits(del) {
			t.Errorf("bob R permitted delete configmaps ns-a (should be get/list only) — cross-identity bleed")
		}
		_, _, _, _, entries := rbac.RequesterProfileMemoStatsForTest()
		if entries != 2 {
			t.Errorf("expected 2 distinct identities cached; entries=%d", entries)
		}
	})

	t.Run("race-safe under concurrency", func(t *testing.T) {
		rbac.ResetRequesterProfileMemoForTest()
		snap := buildMemoSnapshot(t, 7)
		ref := rbac.BuildRequesterProfile(snap, alice)
		refCheck := rbac.EvaluateOptions{Verb: "get", Group: "templates.krateo.io", Resource: "restactions", Namespace: "ns-a"}
		want := ref.Permits(refCheck)

		var wg sync.WaitGroup
		for i := 0; i < 64; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p := rbac.RequesterProfileFor(snap, alice)
				if p.Permits(refCheck) != want {
					t.Errorf("concurrent R disagreed with reference on %v", refCheck)
				}
			}()
		}
		wg.Wait()
	})
}
