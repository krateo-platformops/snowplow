// refilter_namespace_memo_falsifier_test.go — perf/uaf-refilter-namespace-memo
// (v7 UAF-digest design, "Step 0").
//
// Team rule feedback_falsifier_first_before_ship: these arms are written
// BEFORE the production memo and MUST fail against the un-memoized
// refilterSlice (each records exactly which assertion is RED pre-fix in
// its doc comment).
//
// WHAT THE CHANGE IS. Within ONE refilterSlice invocation, the per-item
// EvaluateRBAC verdict is memoized keyed by the exact tuple that varies
// item-to-item — (resource, namespace, name). Username, Groups, Verb and
// Group are invocation-constant (one identity, one UAF stanza), so they
// are NOT in the key. For a collection verb (list/watch/…) evalSingle
// keeps Name=="" (rbac.IsNameSpecificVerb is false), so the key collapses
// to (resource, namespace): a LIST over N items in K namespaces makes K
// EvaluateRBAC calls, not N. For a name-specific verb (get/update/patch/
// delete) Name is in the key, so two same-(resource,namespace) items with
// DIFFERENT names never collapse — each gets its own correct verdict.
//
// The memo is a LOCAL map created per refilterSlice call, discarded at
// return — no shared state, no global, no invalidation, no staleness. A
// subsequent refilter builds a fresh memo against the live RBAC store.
//
// The four falsifier legs (task spec):
//   1. Invariance (load-bearing): memo vs no-memo produce the IDENTICAL
//      surviving item set; denied namespaces fully dropped, allowed fully
//      kept.
//   2. Call-count: the memo turns O(items) EvaluateRBAC calls into
//      O(distinct namespaces) for a collection-verb UAF.
//   3. Name-specific verb safety: a name-specific-verb UAF where two items
//      share (verb,group,resource,namespace) but differ in NAME and in
//      verdict — the memo must NOT collapse them.
//   4. Two-mutant control: a namespace-alone key, and a memo that never
//      re-evaluates across the resource OR-set, each drop a cross-resource
//      item the correct code keeps.

package api

import (
	"context"
	"reflect"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// ---- fixture -------------------------------------------------------------
//
// One RBAC store shared by every arm. narrowUser ("cyberjoker", group
// "devs") is granted, in group "example.krateo.io":
//   - list widgets  in team-a, team-b       (ClusterRole widgets-lister)
//   - list gadgets  in team-c               (ClusterRole gadgets-lister)
//   - get  widgets  in ns-named, ONLY name "alpha" (Role with resourceNames)
// and has NO grant in bench-ns-1, bench-ns-2, kube-system.
//
// EvaluateRBAC only reads the RBAC snapshot, so the target resources
// (widgets/gadgets) need no informer — they are plain strings matched
// against PolicyRules.

const memoGroup = "example.krateo.io"

func memoClusterRole(name, verb, resource string) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{memoGroup}, Resources: []string{resource}, Verbs: []string{verb}},
		},
	}
}

func memoRBToClusterRole(ns, name, clusterRole string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "devs"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: clusterRole},
	}
}

// newMemoTestWatcher seeds the shared fixture and publishes a cache=on
// watcher (via the existing newRefilterTestWatcher helper).
func newMemoTestWatcher(t *testing.T) {
	t.Helper()

	// Namespaced Role with resourceNames — the name-specific arm.
	getAlphaRole := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns-named", Name: "widgets-get-alpha"},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups:     []string{memoGroup},
				Resources:     []string{"widgets"},
				Verbs:         []string{"get"},
				ResourceNames: []string{"alpha"},
			},
		},
	}
	getAlphaBinding := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns-named", Name: "widgets-get-alpha-binding"},
		Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "devs"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "widgets-get-alpha"},
	}

	seed := []runtime.Object{
		memoClusterRole("widgets-lister", "list", "widgets"),
		memoClusterRole("gadgets-lister", "list", "gadgets"),
		memoRBToClusterRole("team-a", "widgets-lister-binding", "widgets-lister"),
		memoRBToClusterRole("team-b", "widgets-lister-binding", "widgets-lister"),
		memoRBToClusterRole("team-c", "gadgets-lister-binding", "gadgets-lister"),
		getAlphaRole,
		getAlphaBinding,
	}
	newRefilterTestWatcher(t, seed...)
}

// memoNSItem builds a namespaced-object item ({"metadata":{"namespace":ns,"name":name}})
// — the shape a list-verb UAF resolves .metadata.namespace / .metadata.name from.
func memoNSItem(ns, name string) map[string]any {
	return map[string]any{"metadata": map[string]any{"namespace": ns, "name": name}}
}

// memoUAFList is a collection-verb (list) UAF over one static resource.
func memoUAFList(resource string) *templates.UserAccessFilterSpec {
	return &templates.UserAccessFilterSpec{Verb: "list", Group: memoGroup, Resource: resource}
}

// keptNS extracts the ordered (namespace/name) identity of a kept slice.
func keptIdentities(kept []any) []string {
	out := make([]string, 0, len(kept))
	for _, it := range kept {
		m, _ := it.(map[string]any)
		meta, _ := m["metadata"].(map[string]any)
		out = append(out, meta["namespace"].(string)+"/"+meta["name"].(string))
	}
	return out
}

// -------------------------------------------------------------------------
// Leg 2 — CALL-COUNT: the perf claim as a test.
//
// A collection-verb (list) UAF over a single resource, run across
// K=5 distinct namespaces × M=4 items, must make exactly 5 EvaluateRBAC
// calls (one per namespace) — not 20.
//
// RED PRE-FIX: the un-memoized refilterSlice calls EvaluateRBAC once per
// item, so the counter reads 20, and this arm fails on `got != 5`.
func TestUAFMemo_CallCount_CollectionVerb_OnePerNamespace(t *testing.T) {
	newMemoTestWatcher(t)

	const itemsPerNS = 4
	namespaces := []string{"team-a", "team-b", "bench-ns-1", "bench-ns-2", "kube-system"}
	wantCalls := uint64(len(namespaces)) // one EvaluateRBAC per distinct namespace

	var items []any
	for _, ns := range namespaces {
		for i := 0; i < itemsPerNS; i++ {
			items = append(items, memoNSItem(ns, ns+"-"+itoaMemo(i)))
		}
	}

	ctx := ctxWithUser(narrowUser, "devs")
	uaf := memoUAFList("widgets")

	rbac.ResetEvaluateRBACCallCount()
	kept, dropped, calls := refilterSlice(ctx, discardLogger(), narrowUser, []string{"devs"}, uaf, []string{"widgets"}, items)
	got := rbac.EvaluateRBACCallCount()

	if got != wantCalls {
		t.Fatalf("call-count: %d items across %d namespaces made %d EvaluateRBAC calls; want exactly %d "+
			"(one per distinct namespace — the per-item verdict is not namespace-memoized)",
			len(items), len(namespaces), got, wantCalls)
	}
	// Byte-identical filtering result: team-a + team-b kept (8), rest dropped (12).
	if len(kept) != 2*itemsPerNS {
		t.Fatalf("call-count: kept = %d; want %d (team-a + team-b only)", len(kept), 2*itemsPerNS)
	}
	if dropped != 3*itemsPerNS {
		t.Fatalf("call-count: dropped = %d; want %d", dropped, 3*itemsPerNS)
	}
	// refilterSlice's returned `calls` field counts items evaluated (one per
	// item), unchanged by the memo — it is a diagnostic, not the EvaluateRBAC
	// tally. Guard that it stayed byte-identical to today.
	if calls != len(items) {
		t.Fatalf("call-count: res.calls (items-evaluated diagnostic) = %d; want %d (must be unchanged by the memo)", calls, len(items))
	}
}

// -------------------------------------------------------------------------
// Leg 1 — INVARIANCE (load-bearing). A table of items across K>1
// namespaces × M>1 items, a mix of allowed and denied namespaces, run
// WITH the memo (refilterSlice) and WITHOUT it (per-item evalSingle, which
// is the nil-memo path) must yield the IDENTICAL surviving set — and a
// denied namespace's items are ALL dropped, an allowed namespace's ALL
// kept.
//
// This arm is GREEN both before and after the fix (both paths agree today);
// it is the guard that the memo, once added, does not change the result.
func TestUAFMemo_Invariance_IdenticalSurvivingSet(t *testing.T) {
	newMemoTestWatcher(t)

	// K=4 namespaces (2 allowed, 2 denied) × M=3 items.
	table := []struct {
		ns      string
		allowed bool
	}{
		{"team-a", true},
		{"bench-ns-1", false},
		{"team-b", true},
		{"bench-ns-2", false},
	}
	const m = 3

	var items []any
	allowedIdents := map[string]bool{}
	for _, row := range table {
		for i := 0; i < m; i++ {
			it := memoNSItem(row.ns, row.ns+"-"+itoaMemo(i))
			items = append(items, it)
			if row.allowed {
				allowedIdents[row.ns+"/"+row.ns+"-"+itoaMemo(i)] = true
			}
		}
	}

	ctx := ctxWithUser(narrowUser, "devs")
	uaf := memoUAFList("widgets")
	resources := []string{"widgets"}

	// WITH memo — the production path.
	keptMemo, _, _ := refilterSlice(ctx, discardLogger(), narrowUser, []string{"devs"}, uaf, resources, items)

	// WITHOUT memo — evalSingle is the nil-memo path; drive it per item to
	// reproduce the pre-change behaviour exactly.
	nsExpr, nsCode := compileNamespaceFrom(uaf)
	nameExpr, nameCode := compileNameFrom(uaf)
	var keptNoMemo []any
	for _, it := range items {
		if evalSingle(ctx, discardLogger(), narrowUser, []string{"devs"}, uaf, resources, it, nsExpr, nsCode, nameExpr, nameCode) {
			keptNoMemo = append(keptNoMemo, it)
		}
	}

	// Identical surviving set, in identical order, same underlying items.
	if !reflect.DeepEqual(keptMemo, keptNoMemo) {
		t.Fatalf("invariance: memo vs no-memo surviving sets differ\n memo:   %v\n nomemo: %v",
			keptIdentities(keptMemo), keptIdentities(keptNoMemo))
	}
	// And the exact expected set: every allowed-ns item, nothing else.
	gotIdents := keptIdentities(keptMemo)
	if len(gotIdents) != len(allowedIdents) {
		t.Fatalf("invariance: kept %d items; want %d (all allowed-ns items, no denied-ns item)", len(gotIdents), len(allowedIdents))
	}
	for _, id := range gotIdents {
		if !allowedIdents[id] {
			t.Fatalf("invariance: kept an item from a DENIED namespace: %s", id)
		}
	}
}

// -------------------------------------------------------------------------
// Leg 3 — NAME-SPECIFIC VERB SAFETY. A get (name-specific) UAF where two
// same-(resource,namespace) items differ in NAME and in verdict: the memo
// must NOT collapse them. narrowUser may get widgets/alpha in ns-named
// (resourceNames-scoped Role) but NOT widgets/beta.
//
// Items: [alpha, beta, alpha]. Expected: alpha kept, beta dropped, second
// alpha kept — and exactly 2 EvaluateRBAC calls (alpha + beta; the repeat
// alpha is memoized). A memo that dropped Name from the key would give beta
// alpha's verdict (a wrong ALLOW) — the correctness bug this arm bans.
//
// RED PRE-FIX: the un-memoized code makes 3 EvaluateRBAC calls, failing the
// `== 2` assertion (the verdicts are correct pre-fix; the count is not).
func TestUAFMemo_NameSpecificVerb_NotCollapsed(t *testing.T) {
	newMemoTestWatcher(t)

	uaf := &templates.UserAccessFilterSpec{Verb: "get", Group: memoGroup, Resource: "widgets"}
	resources := []string{"widgets"}
	items := []any{
		memoNSItem("ns-named", "alpha"),
		memoNSItem("ns-named", "beta"),
		memoNSItem("ns-named", "alpha"),
	}

	ctx := ctxWithUser(narrowUser, "devs")
	rbac.ResetEvaluateRBACCallCount()
	kept, dropped, _ := refilterSlice(ctx, discardLogger(), narrowUser, []string{"devs"}, uaf, resources, items)
	got := rbac.EvaluateRBACCallCount()

	if want := uint64(2); got != want {
		t.Fatalf("name-specific: 3 items over 2 distinct names made %d EvaluateRBAC calls; want %d "+
			"(alpha + beta; the repeat alpha is memoized — Name IS part of the key)", got, want)
	}
	idents := keptIdentities(kept)
	wantKept := []string{"ns-named/alpha", "ns-named/alpha"}
	if !reflect.DeepEqual(idents, wantKept) {
		t.Fatalf("name-specific: kept = %v; want %v (beta must be dropped — the memo must NOT hand it alpha's verdict)", idents, wantKept)
	}
	if dropped != 1 {
		t.Fatalf("name-specific: dropped = %d; want 1 (beta only)", dropped)
	}
}

// -------------------------------------------------------------------------
// Leg 4 — TWO-MUTANT CONTROL. A cross-resource OR-set UAF
// (verb:list, resources=[widgets, gadgets]). narrowUser is granted gadgets
// (NOT widgets) in team-c, so the correct OR keeps team-c; widgets in
// team-a; and nothing in bench-ns-1.
//
// The REAL refilterSlice keeps team-a + team-c. Two broken memos each DROP
// team-c — this arm is what would catch either regression:
//
//	(i)  namespace-alone key (drops resource+name): widgets' deny is cached
//	     under the namespace, so gadgets reads the stale deny and team-c is
//	     dropped.
//	(ii) never re-evaluates across the resource OR-set: the first resource's
//	     (widgets) deny is treated as final, so gadgets is never tried.
func TestUAFMemo_TwoMutantControl_CrossResourceORSet(t *testing.T) {
	newMemoTestWatcher(t)

	uaf := &templates.UserAccessFilterSpec{Verb: "list", Group: memoGroup} // no static Resource; resources supplied below
	resources := []string{"widgets", "gadgets"}
	items := []any{
		memoNSItem("team-a", "a1"), memoNSItem("team-a", "a2"),
		memoNSItem("team-c", "c1"), memoNSItem("team-c", "c2"),
		memoNSItem("bench-ns-1", "b1"), memoNSItem("bench-ns-1", "b2"),
	}
	ctx := ctxWithUser(narrowUser, "devs")

	// REAL production path — correct OR: team-a (widgets) + team-c (gadgets) kept.
	keptReal, _, _ := refilterSlice(ctx, discardLogger(), narrowUser, []string{"devs"}, uaf, resources, items)
	realIdents := map[string]bool{}
	for _, id := range keptIdentities(keptReal) {
		realIdents[id] = true
	}
	if !realIdents["team-c/c1"] || !realIdents["team-c/c2"] {
		t.Fatalf("cross-resource: REAL refilterSlice dropped a team-c item it must keep (gadgets grant via OR): kept=%v", keptIdentities(keptReal))
	}
	if !realIdents["team-a/a1"] || !realIdents["team-a/a2"] {
		t.Fatalf("cross-resource: REAL refilterSlice dropped a team-a item it must keep (widgets grant): kept=%v", keptIdentities(keptReal))
	}
	if realIdents["bench-ns-1/b1"] || realIdents["bench-ns-1/b2"] {
		t.Fatalf("cross-resource: REAL refilterSlice kept a bench-ns-1 item (no grant on either resource): kept=%v", keptIdentities(keptReal))
	}
	if len(keptReal) != 4 {
		t.Fatalf("cross-resource: REAL kept = %d; want 4 (team-a + team-c)", len(keptReal))
	}

	// MUTANT (i) — namespace-alone key. MUST drop team-c (caught).
	keptI := refilterKeptMutant(ctx, uaf, resources, items, mutantNSOnly)
	iIdents := map[string]bool{}
	for _, id := range keptIdentities(keptI) {
		iIdents[id] = true
	}
	if iIdents["team-c/c1"] || iIdents["team-c/c2"] {
		t.Fatalf("mutant(i) namespace-alone key was NOT caught: it kept team-c although widgets is denied there — the cross-resource arm must discriminate it")
	}

	// MUTANT (ii) — never re-evaluates across the resource OR-set. MUST drop team-c (caught).
	keptII := refilterKeptMutant(ctx, uaf, resources, items, mutantNoReeval)
	iiIdents := map[string]bool{}
	for _, id := range keptIdentities(keptII) {
		iiIdents[id] = true
	}
	if iiIdents["team-c/c1"] || iiIdents["team-c/c2"] {
		t.Fatalf("mutant(ii) no-reeval-across-resource-set was NOT caught: it kept team-c although the first resource (widgets) denies — the cross-resource arm must discriminate it")
	}
}

// mutantMode selects the injected memo bug for the two-mutant control.
type mutantMode int

const (
	mutantNSOnly   mutantMode = iota // (i) key = namespace only (drops resource + name)
	mutantNoReeval                   // (ii) commit to the first resource's verdict; never OR the rest
)

// evalItemMemoNSOnly asks the RBAC evaluator per resource in the OR-set but
// keys the memo on the NAMESPACE ALONE (dropping resource + name) — mutant (i).
// A deny on resource[0] is cached under the namespace, so every later resource
// in the OR-set reads that stale deny and the item is dropped even when a later
// resource would grant it.
func evalItemMemoNSOnly(ctx context.Context, uaf *templates.UserAccessFilterSpec, resources []string, ns, name string, memo map[string]bool) bool {
	for _, resource := range resources {
		if allowed, hit := memo[ns]; hit { // BUG: key = namespace only
			if allowed {
				return true
			}
			continue
		}
		allowed, _, err := mutantEvaluate(ctx, uaf, resource, ns, name)
		if err != nil {
			continue
		}
		memo[ns] = allowed // BUG: per-resource verdict stored under the namespace
		if allowed {
			return true
		}
	}
	return false
}

// evalItemNoReeval decides the whole item from the FIRST resource only and
// caches that item-level verdict keyed by (namespace,name) — mutant (ii). It
// never re-evaluates across the remaining resource-set iterations, so an item
// granted only by a later resource in the OR-set is dropped.
func evalItemNoReeval(ctx context.Context, uaf *templates.UserAccessFilterSpec, resources []string, ns, name string, memo map[string]bool) bool {
	itemKey := ns + "\x00" + name
	if v, hit := memo[itemKey]; hit {
		return v
	}
	if len(resources) == 0 {
		return false
	}
	allowed, _, err := mutantEvaluate(ctx, uaf, resources[0], ns, name) // BUG: only resources[0]
	if err != nil {
		return false // fail-closed, not memoized
	}
	memo[itemKey] = allowed
	return allowed
}

// mutantEvaluate is the shared EvaluateRBAC call the mutant helpers issue.
func mutantEvaluate(ctx context.Context, uaf *templates.UserAccessFilterSpec, resource, ns, name string) (bool, string, error) {
	return rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
		Username: narrowUser, Groups: []string{"devs"},
		Verb: uaf.Verb, Group: uaf.Group, Resource: resource,
		Namespace: ns, Name: name, SkipBindingUID: true,
	})
}

// refilterKeptMutant mirrors refilterSlice's item loop but injects a broken
// memo. TEST-ONLY — it exists to prove the cross-resource arm discriminates a
// wrong memo; production code is never this function.
func refilterKeptMutant(ctx context.Context, uaf *templates.UserAccessFilterSpec, resources []string, items []any, mode mutantMode) []any {
	memo := map[string]bool{}
	_, nsCode := compileNamespaceFrom(uaf)
	_, nameCode := compileNameFrom(uaf)
	var kept []any
	for _, item := range items {
		ns, err := evalJQStringCompiled(ctx, nsCode, item)
		if err != nil {
			continue
		}
		name := ""
		if rbac.IsNameSpecificVerb(uaf.Verb) {
			name, _ = evalJQStringCompiled(ctx, nameCode, item)
		}
		var permitted bool
		switch mode {
		case mutantNSOnly:
			permitted = evalItemMemoNSOnly(ctx, uaf, resources, ns, name, memo)
		case mutantNoReeval:
			permitted = evalItemNoReeval(ctx, uaf, resources, ns, name, memo)
		}
		if permitted {
			kept = append(kept, item)
		}
	}
	return kept
}
