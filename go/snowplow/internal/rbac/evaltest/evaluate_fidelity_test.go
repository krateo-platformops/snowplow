// Package evaltest — fidelity regression tests for the 0.30.109 RBAC
// evaluator fixes. These tests are pure: they need NO cluster (the
// dynamic fake + cache.ResourceWatcher are in-memory) and never touch
// the destructive internal/rbac TestMain.
//
// Coverage:
//   - G1 (cross-user leak, P0): rule.ResourceNames is honoured.
//     A resourceNames:["foo"] rule grants `get foo`, denies `get bar`,
//     and — per Kubernetes ResourceNameMatches — never grants an
//     unselected `list` (a plain collection URL names no object); since
//     #1049 it grants `list ?fieldSelector=metadata.name=foo`, as upstream.
//   - G3/G6 (under-allow, P1): a Group subject of
//     system:serviceaccounts:<ns> grants a ServiceAccount in <ns> and
//     not one in a different namespace; system:serviceaccounts grants
//     every ServiceAccount.
package evaltest

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/rbac"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ruleNamed builds a PolicyRule that is scoped to specific named
// objects via resourceNames. The 0.30.109 G1 fix makes the evaluator
// honour this field — before the fix it was silently ignored.
func ruleNamed(apiGroups, resources, verbs, resourceNames []string) rbacv1.PolicyRule {
	return rbacv1.PolicyRule{
		APIGroups:     apiGroups,
		Resources:     resources,
		Verbs:         verbs,
		ResourceNames: resourceNames,
	}
}

// ──────────────────────────────────────────────────────────────────────
// G1 — resourceNames must scope the grant (cross-user leak fix, P0)
// ──────────────────────────────────────────────────────────────────────

// TestEvaluateRBAC_ResourceNamesScopesGet pins the core G1 contract:
// a rule scoped to resourceNames:["foo"] grants `get foo` and denies
// `get bar`. Before 0.30.109 rule.ResourceNames was never read, so the
// rule was treated as granting every object of the GVR — a cross-user
// over-exposure when filterListByRBAC ran the per-item check.
func TestEvaluateRBAC_ResourceNamesScopesGet(t *testing.T) {
	newTestWatcher(t,
		clusterRole("named-reader",
			ruleNamed([]string{"templates.krateo.io"}, []string{"restactions"},
				[]string{"get"}, []string{"foo"}),
		),
		clusterRoleBinding("named-reader-bind", "named-reader",
			userSubject("alice"),
		),
	)

	// get foo — IN the resourceNames list → permit.
	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "alice", Verb: "get",
		Group: "templates.krateo.io", Resource: "restactions",
		Namespace: "default", Name: "foo",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(get foo): %v", err)
	}
	if !ok {
		t.Fatalf("G1: alice has resourceNames:[foo] get grant — `get foo` MUST permit")
	}

	// get bar — NOT in the resourceNames list → deny. This is the
	// leak that 0.30.109 closes: before the fix this returned permit.
	ok, _, err = rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "alice", Verb: "get",
		Group: "templates.krateo.io", Resource: "restactions",
		Namespace: "default", Name: "bar",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(get bar): %v", err)
	}
	if ok {
		t.Fatalf("G1 LEAK: alice's grant is resourceNames-scoped to [foo] — `get bar` MUST deny")
	}
}

// TestEvaluateRBAC_ResourceNamesDoesNotGrantList pins the CRITICAL
// Kubernetes ResourceNameMatches semantics as snowplow builds its checks: a
// rule with a non-empty resourceNames grants a list ONLY when the request
// itself carries a listed Name. A plain collection URL names no object, so
// the rule cannot match it; since #1049 a `fieldSelector=metadata.name=foo`
// list carries Name foo and the rule matches, exactly as upstream.
//
// This is the load-bearing G1 case for filterListByRBAC: the served LIST
// branch must not let a resourceNames-scoped binding grant an unselected
// list. Before 0.30.109 it granted the entire list — a cross-user leak. The
// guard is the check's construction: CollectionCheck (no selector) and
// ObjectCheck (a collection verb) both carry no Name, whatever the items are
// called. (A hand-built `list` check WITH Name foo is allowed, as a
// SubjectAccessReview with that name is upstream — the evaluator no longer
// second-guesses the Name by verb.)
func TestEvaluateRBAC_ResourceNamesDoesNotGrantList(t *testing.T) {
	newTestWatcher(t,
		clusterRole("named-reader",
			ruleNamed([]string{"templates.krateo.io"}, []string{"restactions"},
				[]string{"get", "list"}, []string{"foo"}),
		),
		clusterRoleBinding("named-reader-bind", "named-reader",
			userSubject("alice"),
		),
	)

	// list — even though "list" is in the rule's Verbs, an unselected list
	// names no object, so the rule cannot grant it — neither the request's
	// own check nor a per-item check about the item named "foo".
	gvr := schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
	for what, opts := range map[string]rbac.EvaluateOptions{
		"CollectionCheck(no selector)":   rbac.CollectionCheck("alice", nil, gvr, "default", ""),
		"ObjectCheck(list, item foo)":    rbac.ObjectCheck("alice", nil, "list", rbac.TargetOf(gvr, "default", "foo")),
		"CollectionCheck(name!=bar)":     rbac.CollectionCheck("alice", nil, gvr, "default", "fieldSelector=metadata.name%21%3Dbar"),
		"CollectionCheck(label app=foo)": rbac.CollectionCheck("alice", nil, gvr, "default", "labelSelector=app%3Dfoo"),
	} {
		ok, _, err := rbac.EvaluateRBAC(context.Background(), opts)
		if err != nil {
			t.Fatalf("EvaluateRBAC(%s): %v", what, err)
		}
		if ok {
			t.Fatalf("G1 LEAK: a resourceNames-scoped rule MUST NOT grant an unselected `list` (%s)", what)
		}
	}

	// #1049 — a list whose field selector requires metadata.name=foo carries
	// Name foo, and Kubernetes allows it.
	ok, _, err := rbac.EvaluateRBAC(context.Background(),
		rbac.CollectionCheck("alice", nil, gvr, "default", "fieldSelector=metadata.name%3Dfoo"))
	if err != nil {
		t.Fatalf("EvaluateRBAC(list name=foo): %v", err)
	}
	if !ok {
		t.Fatalf("#1049: `list ?fieldSelector=metadata.name=foo` must be granted by resourceNames:[foo], as upstream")
	}

	// Sanity: the same rule still grants `get foo` — the resourceNames
	// scope only suppresses collection verbs, not name-specific ones.
	ok, _, err = rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "alice", Verb: "get",
		Group: "templates.krateo.io", Resource: "restactions",
		Namespace: "default", Name: "foo",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(get foo): %v", err)
	}
	if !ok {
		t.Fatalf("resourceNames rule should still grant the name-specific `get foo`")
	}
}

// TestEvaluateRBAC_ResourceNamesWildcardVerb verifies that the
// resourceNames scope is enforced even for a verb-wildcard rule. A
// rule with Verbs:["*"] + resourceNames:["foo"] still must NOT grant a
// collection verb (the "*" expands per-request-verb; for a "list"
// request the effective verb is collection-scoped).
func TestEvaluateRBAC_ResourceNamesWildcardVerb(t *testing.T) {
	newTestWatcher(t,
		clusterRole("named-star",
			ruleNamed([]string{""}, []string{"secrets"},
				[]string{"*"}, []string{"foo"}),
		),
		clusterRoleBinding("named-star-bind", "named-star",
			userSubject("alice"),
		),
	)

	// get foo — wildcard verb + name match → permit.
	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "alice", Verb: "get", Group: "", Resource: "secrets",
		Namespace: "default", Name: "foo",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(get foo): %v", err)
	}
	if !ok {
		t.Fatalf("verb-wildcard + resourceNames:[foo] should grant `get foo`")
	}

	// delete foo — name-specific verb, name match → permit.
	ok, _, err = rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "alice", Verb: "delete", Group: "", Resource: "secrets",
		Namespace: "default", Name: "foo",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(delete foo): %v", err)
	}
	if !ok {
		t.Fatalf("verb-wildcard + resourceNames:[foo] should grant `delete foo`")
	}

	// list — an unselected collection read, built as snowplow builds it
	// (no Name), must deny even with the verb wildcard.
	secrets := schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	ok, _, err = rbac.EvaluateRBAC(context.Background(), rbac.CollectionCheck("alice", nil, secrets, "default", ""))
	if err != nil {
		t.Fatalf("EvaluateRBAC(list): %v", err)
	}
	if ok {
		t.Fatalf("G1 LEAK: verb-wildcard + resourceNames MUST still deny an unselected `list`")
	}
	// A per-item collection-verb check about foo carries no Name either.
	ok, _, err = rbac.EvaluateRBAC(context.Background(), rbac.ObjectCheck("alice", nil, "list", rbac.TargetOf(secrets, "default", "foo")))
	if err != nil {
		t.Fatalf("EvaluateRBAC(ObjectCheck list foo): %v", err)
	}
	if ok {
		t.Fatalf("G1 LEAK: ObjectCheck(list, foo) must carry no Name and deny")
	}

	// get bar — name not in list → deny.
	ok, _, err = rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "alice", Verb: "get", Group: "", Resource: "secrets",
		Namespace: "default", Name: "bar",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(get bar): %v", err)
	}
	if ok {
		t.Fatalf("G1 LEAK: resourceNames:[foo] MUST deny `get bar`")
	}

	// #1073 F2 — names compare exactly: neither a name that extends a granted
	// one, nor its prefix, nor a case variant is the granted object.
	for _, n := range []string{"foobar", "fo", "Foo", "foo "} {
		ok, _, err = rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
			Username: "alice", Verb: "get", Group: "", Resource: "secrets",
			Namespace: "default", Name: n,
		})
		if err != nil {
			t.Fatalf("EvaluateRBAC(get %q): %v", n, err)
		}
		if ok {
			t.Fatalf("G1 LEAK: resourceNames:[foo] MUST deny `get %q` (names compare exactly)", n)
		}
	}
}

// TestEvaluateRBAC_EmptyResourceNamesUnchanged is the no-regression
// guard: a rule with an EMPTY resourceNames must keep its pre-0.30.109
// behaviour — it grants every object of the GVR, including `list`.
func TestEvaluateRBAC_EmptyResourceNamesUnchanged(t *testing.T) {
	newTestWatcher(t,
		clusterRole("unscoped-reader",
			// No resourceNames — the common, unscoped rule.
			rule([]string{""}, []string{"configmaps"}, []string{"get", "list"}),
		),
		clusterRoleBinding("unscoped-reader-bind", "unscoped-reader",
			userSubject("alice"),
		),
	)

	for _, c := range []struct {
		verb, name string
	}{
		{"get", "anything"},
		{"get", ""},
		{"list", ""},
		{"list", "anything"},
	} {
		ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
			Username: "alice", Verb: c.verb, Group: "", Resource: "configmaps",
			Namespace: "default", Name: c.name,
		})
		if err != nil {
			t.Fatalf("EvaluateRBAC(%s,%q): %v", c.verb, c.name, err)
		}
		if !ok {
			t.Fatalf("empty-resourceNames rule must still grant %s name=%q (no-regression)",
				c.verb, c.name)
		}
	}
}

// TestEvaluateRBAC_ResourceNamesViaRoleBinding confirms the G1 fix
// applies on the namespaced RoleBinding→Role path too (not just the
// ClusterRoleBinding path).
func TestEvaluateRBAC_ResourceNamesViaRoleBinding(t *testing.T) {
	newTestWatcher(t,
		role("demo-system", "named-role",
			ruleNamed([]string{""}, []string{"secrets"},
				[]string{"get", "update"}, []string{"app-secret"}),
		),
		roleBinding("demo-system", "named-role-bind", "Role", "named-role",
			userSubject("bob"),
		),
	)

	// get app-secret — permit.
	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "bob", Verb: "get", Group: "", Resource: "secrets",
		Namespace: "demo-system", Name: "app-secret",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(get app-secret): %v", err)
	}
	if !ok {
		t.Fatalf("bob should `get app-secret` via the resourceNames-scoped Role")
	}

	// get other-secret — deny.
	ok, _, err = rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "bob", Verb: "get", Group: "", Resource: "secrets",
		Namespace: "demo-system", Name: "other-secret",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(get other-secret): %v", err)
	}
	if ok {
		t.Fatalf("G1 LEAK: bob's Role is resourceNames-scoped to [app-secret] — `get other-secret` MUST deny")
	}

	// list — deny (collection verb).
	ok, _, err = rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "bob", Verb: "list", Group: "", Resource: "secrets",
		Namespace: "demo-system", Name: "app-secret",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(list): %v", err)
	}
	if ok {
		t.Fatalf("G1 LEAK: a resourceNames-scoped Role MUST NOT grant `list`")
	}
}

// ──────────────────────────────────────────────────────────────────────
// G3/G6 — ServiceAccount synthetic groups (under-allow fix, P1)
// ──────────────────────────────────────────────────────────────────────

// TestEvaluateRBAC_SAGroupNamespaceScoped pins the core G3/G6 contract:
// a binding granting Group subject "system:serviceaccounts:ns1" must
// authorize a ServiceAccount in ns1 and must NOT authorize one in ns2.
// Before 0.30.109 anySubjectMatches only checked opts.Groups + literal
// system:authenticated, so this binding was silently missed.
func TestEvaluateRBAC_SAGroupNamespaceScoped(t *testing.T) {
	newTestWatcher(t,
		clusterRole("ns1-sa-reader",
			rule([]string{""}, []string{"configmaps"}, []string{"get"}),
		),
		clusterRoleBinding("ns1-sa-reader-bind", "ns1-sa-reader",
			groupSubject("system:serviceaccounts:ns1"),
		),
	)

	// A ServiceAccount IN ns1 — implicitly a member of
	// system:serviceaccounts:ns1 → permit.
	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "system:serviceaccount:ns1:worker",
		Verb:     "get", Group: "", Resource: "configmaps", Namespace: "default",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(ns1 SA): %v", err)
	}
	if !ok {
		t.Fatalf("G3/G6: SA in ns1 should match Group subject system:serviceaccounts:ns1")
	}

	// A ServiceAccount in ns2 — NOT a member of
	// system:serviceaccounts:ns1 → deny.
	ok, _, err = rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "system:serviceaccount:ns2:worker",
		Verb:     "get", Group: "", Resource: "configmaps", Namespace: "default",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(ns2 SA): %v", err)
	}
	if ok {
		t.Fatalf("G3/G6: SA in ns2 must NOT match the ns1-scoped Group subject")
	}
}

// TestEvaluateRBAC_SAAllServiceAccountsGroup verifies the broad
// synthetic group system:serviceaccounts matches EVERY ServiceAccount
// regardless of namespace.
func TestEvaluateRBAC_SAAllServiceAccountsGroup(t *testing.T) {
	newTestWatcher(t,
		clusterRole("all-sa-reader",
			rule([]string{""}, []string{"pods"}, []string{"get"}),
		),
		clusterRoleBinding("all-sa-reader-bind", "all-sa-reader",
			groupSubject("system:serviceaccounts"),
		),
	)

	for _, sa := range []string{
		"system:serviceaccount:ns1:worker",
		"system:serviceaccount:ns2:builder",
		"system:serviceaccount:krateo-system:snowplow",
	} {
		ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
			Username: sa, Verb: "get", Group: "", Resource: "pods", Namespace: "default",
		})
		if err != nil {
			t.Fatalf("EvaluateRBAC(%s): %v", sa, err)
		}
		if !ok {
			t.Fatalf("G3/G6: %s should match the all-SAs group system:serviceaccounts", sa)
		}
	}

	// A NON-ServiceAccount user must NOT gain the synthetic group.
	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "alice", Verb: "get", Group: "", Resource: "pods", Namespace: "default",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(alice): %v", err)
	}
	if ok {
		t.Fatalf("a regular User must NOT be in system:serviceaccounts")
	}
}

// TestEvaluateRBAC_SASyntheticGroupViaRoleBinding confirms the
// synthetic-group match works on the namespaced RoleBinding path too.
func TestEvaluateRBAC_SASyntheticGroupViaRoleBinding(t *testing.T) {
	newTestWatcher(t,
		role("demo-system", "sa-events",
			rule([]string{""}, []string{"events"}, []string{"create"}),
		),
		roleBinding("demo-system", "sa-events-bind", "Role", "sa-events",
			groupSubject("system:serviceaccounts:ns1"),
		),
	)

	// SA in ns1, request in demo-system (the RoleBinding's namespace) → permit.
	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "system:serviceaccount:ns1:worker",
		Verb:     "create", Group: "", Resource: "events", Namespace: "demo-system",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(ns1 SA via RoleBinding): %v", err)
	}
	if !ok {
		t.Fatalf("G3/G6: ns1 SA should match system:serviceaccounts:ns1 in a RoleBinding")
	}

	// SA in ns2 → deny.
	ok, _, err = rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "system:serviceaccount:ns2:worker",
		Verb:     "create", Group: "", Resource: "events", Namespace: "demo-system",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC(ns2 SA via RoleBinding): %v", err)
	}
	if ok {
		t.Fatalf("G3/G6: ns2 SA must NOT match the ns1-scoped synthetic group")
	}
}

// TestEvaluateRBAC_SAExplicitSubjectStillWorks is a no-regression guard
// for the pre-existing explicit ServiceAccount-subject match path — the
// synthetic-group addition must not break a direct
// Kind:ServiceAccount subject.
func TestEvaluateRBAC_SAExplicitSubjectStillWorks(t *testing.T) {
	newTestWatcher(t,
		clusterRole("controller",
			rule([]string{""}, []string{"events"}, []string{"create"}),
		),
		clusterRoleBinding("controller-bind", "controller",
			saSubject("krateo-system", "snowplow"),
		),
	)

	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: "system:serviceaccount:krateo-system:snowplow",
		Verb:     "create", Group: "", Resource: "events", Namespace: "any",
	})
	if err != nil {
		t.Fatalf("EvaluateRBAC: %v", err)
	}
	if !ok {
		t.Fatalf("explicit ServiceAccount subject must still match (no-regression)")
	}
}
