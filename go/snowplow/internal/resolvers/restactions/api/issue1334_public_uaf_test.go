package api

// issue1334_public_uaf_test.go — #1334, the ruling on userAccessFilter stages
// (Must-never 3): a UAF stage lists through snowplow's ServiceAccount and the
// refilter narrows every item to what RBAC grants the REQUESTER, as for every
// identity (#615). For the public identity that must be exactly what RBAC grants
// user krateo:public and group system:unauthenticated — with #1342's effective
// group set, so nothing granted only to system:authenticated — and nothing at
// all when no grant exists.
//
// The world grants the ServiceAccount (the UAF stage's transport) read on
// everything, as the shipped ClusterRole does, so a refilter that narrowed as
// the ServiceAccount would keep every item: the arm can tell the two apart
// (the SA control below shows the instrument says "all" for that identity).
//
// The identity is the one the auth middleware installs for a token-less
// request (pinned there: TestIssue1334_TokenlessRequestCannotNameAnotherIdentity).

import (
	"sort"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	i1334UAFNS     = "krateo-public"
	i1334UAFPublic = "krateo:public"
	i1334UAFUnauth = "system:unauthenticated"
	i1334UAFSA     = "system:serviceaccount:krateo-system:snowplow"
)

func i1334UAFObjects(withPublicGrants bool) []runtime.Object {
	tm := func(k string) metav1.TypeMeta {
		return metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: k}
	}
	g := rbacv1.GroupName
	getNamed := func(name string) []rbacv1.PolicyRule {
		return []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"configmaps"}, ResourceNames: []string{name}}}
	}
	out := []runtime.Object{
		// The ServiceAccount: read on everything (the shipped ClusterRole).
		&rbacv1.ClusterRole{TypeMeta: tm("ClusterRole"), ObjectMeta: metav1.ObjectMeta{Name: "sa-read-1334"},
			Rules: []rbacv1.PolicyRule{{Verbs: []string{"get", "list", "watch"}, APIGroups: []string{"*"}, Resources: []string{"*"}}}},
		&rbacv1.ClusterRoleBinding{TypeMeta: tm("ClusterRoleBinding"), ObjectMeta: metav1.ObjectMeta{Name: "sa-read-1334"},
			Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "snowplow", Namespace: "krateo-system"}},
			RoleRef:  rbacv1.RoleRef{APIGroup: g, Kind: "ClusterRole", Name: "sa-read-1334"}},
		// Granted only to system:authenticated.
		&rbacv1.Role{TypeMeta: tm("Role"), ObjectMeta: metav1.ObjectMeta{Namespace: i1334UAFNS, Name: "authn-1334"}, Rules: getNamed("authn-only")},
		&rbacv1.RoleBinding{TypeMeta: tm("RoleBinding"), ObjectMeta: metav1.ObjectMeta{Namespace: i1334UAFNS, Name: "authn-1334"},
			Subjects: []rbacv1.Subject{{Kind: "Group", APIGroup: g, Name: "system:authenticated"}},
			RoleRef:  rbacv1.RoleRef{APIGroup: g, Kind: "Role", Name: "authn-1334"}},
	}
	if !withPublicGrants {
		return out
	}
	return append(out,
		// The page's Role, bound to the USER krateo:public.
		&rbacv1.Role{TypeMeta: tm("Role"), ObjectMeta: metav1.ObjectMeta{Namespace: i1334UAFNS, Name: "page-1334"}, Rules: getNamed("page-a")},
		&rbacv1.RoleBinding{TypeMeta: tm("RoleBinding"), ObjectMeta: metav1.ObjectMeta{Namespace: i1334UAFNS, Name: "page-1334"},
			Subjects: []rbacv1.Subject{{Kind: "User", APIGroup: g, Name: i1334UAFPublic}},
			RoleRef:  rbacv1.RoleRef{APIGroup: g, Kind: "Role", Name: "page-1334"}},
		// Granted to group system:unauthenticated (what any anonymous caller has).
		&rbacv1.ClusterRole{TypeMeta: tm("ClusterRole"), ObjectMeta: metav1.ObjectMeta{Name: "unauth-1334"}, Rules: getNamed("shared-unauth")},
		&rbacv1.ClusterRoleBinding{TypeMeta: tm("ClusterRoleBinding"), ObjectMeta: metav1.ObjectMeta{Name: "unauth-1334"},
			Subjects: []rbacv1.Subject{{Kind: "Group", APIGroup: g, Name: i1334UAFUnauth}},
			RoleRef:  rbacv1.RoleRef{APIGroup: g, Kind: "ClusterRole", Name: "unauth-1334"}},
	)
}

// i1334UAFKept runs the refilter a UAF stage applies to the ServiceAccount's
// LIST (four objects, one per grant shape plus one with none) as user+groups,
// and returns the names it keeps.
func i1334UAFKept(t *testing.T, user string, groups ...string) []string {
	t.Helper()
	apiCall := &templates.API{
		Name:             "cms",
		UserAccessFilter: &templates.UserAccessFilterSpec{Verb: "get", Group: "", Resource: "configmaps"},
	}
	dict := map[string]any{"cms": map[string]any{
		"kind": "ConfigMapList", "apiVersion": "v1",
		"items": nameAwareItems(i1334UAFNS, "page-a", "shared-unauth", "authn-only", "no-grant"),
	}}
	applyUserAccessFilter(ctxWithUser(user, groups...), dict, apiCall)
	var kept []string
	for _, it := range dict["cms"].(map[string]any)["items"].([]any) {
		kept = append(kept, it.(map[string]any)["metadata"].(map[string]any)["name"].(string))
	}
	sort.Strings(kept)
	return kept
}

func i1334Equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestIssue1334_UAFStage_PublicSeesOnlyItsGrants(t *testing.T) {
	newRefilterTestWatcher(t, i1334UAFObjects(true)...)

	if got := i1334UAFKept(t, i1334UAFSA, "system:serviceaccounts", "system:serviceaccounts:krateo-system"); len(got) != 4 {
		t.Fatalf("INSTRUMENT: narrowed as the ServiceAccount the refilter keeps all 4 items; got %v — the arm could not tell SA-scoped from narrowed", got)
	}
	if got, want := i1334UAFKept(t, i1334UAFPublic, i1334UAFUnauth), []string{"page-a", "shared-unauth"}; !i1334Equal(got, want) {
		t.Fatalf("MUST NEVER: a public request through a UAF stage kept %v; want exactly %v (the user's grant and system:unauthenticated's, never system:authenticated's)", got, want)
	}
	// Control: an authenticated user with no grant of their own sees exactly
	// the system:authenticated grant — the same refilter, the other group.
	if got, want := i1334UAFKept(t, "alice-1334"), []string{"authn-only"}; !i1334Equal(got, want) {
		t.Fatalf("CONTROL: an authenticated user kept %v; want %v", got, want)
	}
}

func TestIssue1334_UAFStage_PublicWithNoGrantSeesNothing(t *testing.T) {
	newRefilterTestWatcher(t, i1334UAFObjects(false)...)

	if got := i1334UAFKept(t, i1334UAFSA, "system:serviceaccounts", "system:serviceaccounts:krateo-system"); len(got) != 4 {
		t.Fatalf("INSTRUMENT: narrowed as the ServiceAccount the refilter keeps all 4 items; got %v", got)
	}
	if got := i1334UAFKept(t, i1334UAFPublic, i1334UAFUnauth); len(got) != 0 {
		t.Fatalf("MUST NEVER: with no grant to krateo:public or system:unauthenticated, a public request through a UAF stage kept %v; want nothing", got)
	}
}
