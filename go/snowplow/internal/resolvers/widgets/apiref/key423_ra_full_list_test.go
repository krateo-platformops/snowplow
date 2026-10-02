// key423_ra_full_list_test.go — #423 raFullList carrier arm.
//
// raFullListServe caches the apiRef'd RESTAction's FULL resolve output under a
// cell keyed by seedFullListRAKey. Pre-#423 that key folded the RA-CR's
// first-match BindingUID alone (not even RBACSubGen), so two users co-bound by
// the RA grant shared one cell even when a NON-UAF step of the RA reads data
// only one of them may see. The second user was served the first user's rows
// as a Go-slice of the shared full list.
//
// The resolve closure stands in for the RA's steps: it includes the
// sentinel-bearing row iff the REAL rbac.EvaluateRBAC lets THIS requester `get`
// configmaps x/y. Acceptance is on the rows raFullListServe returns to bob.
//
// RED on c20cc763: alice's full list is Put under the shared raKey, bob's call
// is a fast-path hit and his page contains the sentinel row. GREEN with #423:
// bob's raKey differs (SubjectBindingSet), he resolves his own rows.
package apiref

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jqutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

const k423Sentinel = "k423-sentinel-row"

func k423RAFixture() []runtime.Object {
	sub := func(kind, name string) rbacv1.Subject {
		return rbacv1.Subject{Kind: kind, APIGroup: "rbac.authorization.k8s.io", Name: name}
	}
	return []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "ra-reader-423"},
			Rules: []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{"templates.krateo.io"}, Resources: []string{"restactions"}}}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "portal-ra-423", UID: types.UID("uid-portal-ra-423")},
			Subjects: []rbacv1.Subject{sub("Group", "portal-423")},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "ra-reader-423"}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "cm-reader-423"},
			Rules: []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "alice-cm-423", UID: types.UID("uid-rb-alice-423")},
			Subjects: []rbacv1.Subject{sub("User", "alice-423")},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "cm-reader-423"}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: "z", Name: "pod-reader-423"},
			Rules: []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"pods"}}}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: "z", Name: "bob-pods-423", UID: types.UID("uid-rb-bob-423")},
			Subjects: []rbacv1.Subject{sub("User", "bob-423")},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "pod-reader-423"}},
	}
}

// k423PerUserRows resolves the RA as the requester: 12 public rows, plus the
// sentinel row iff the requester's own RBAC permits `get configmaps x/y`.
func k423PerUserRows(t *testing.T, calls *atomic.Int64) func(context.Context, int, int) (map[string]any, error) {
	return func(ctx context.Context, perPage, page int) (map[string]any, error) {
		calls.Add(1)
		ui, err := xcontext.UserInfo(ctx)
		if err != nil {
			t.Fatalf("resolve: no identity on ctx: %v", err)
		}
		items := panelDict(12)["compositionspanels"].([]any)
		ok, _, err := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
			Username: ui.Username, Groups: ui.Groups, Verb: "get", Resource: "configmaps", Namespace: "x", Name: "y",
		})
		if err != nil {
			t.Fatalf("resolve: %v", err)
		}
		if ok {
			// newest timestamp → sorts first → always on page 1.
			items = append(items, map[string]any{"metadata": map[string]any{
				"name": k423Sentinel, "creationTimestamp": "2026-12-31T00:00:00Z"}})
		}
		dict := map[string]any{"compositionspanels": items}
		if perPage > 0 && page > 0 {
			dict["slice"] = map[string]any{"perPage": float64(perPage), "page": float64(page), "offset": float64((page - 1) * perPage)}
		}
		s, err := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: raSliceJQ, Data: dict})
		if err != nil {
			return nil, err
		}
		var out map[string]any
		return out, json.Unmarshal([]byte(s), &out)
	}
}

func TestKey423_RAFullList_CrossUser(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)
	newF6Watcher(t, k423RAFixture()...)

	const ns, name = "krateo-system", "k423-ra-full-list"
	aliceCtx := f6CtxWithUser(t, "alice-423", []string{"portal-423"})
	bobCtx := f6CtxWithUser(t, "bob-423", []string{"portal-423"})

	aIn, aKey, okA := seedFullListRAKey(aliceCtx, gvr(), ns, name, nil)
	bIn, bKey, okB := seedFullListRAKey(bobCtx, gvr(), ns, name, nil)
	if !okA || !okB || aIn.BindingUID == "" || aIn.BindingUID != bIn.BindingUID {
		t.Fatalf("PRE (ii): alice and bob must be co-bound by one RA grant; a=%q b=%q", aIn.BindingUID, bIn.BindingUID)
	}
	if keyWithoutSBS423(aIn) != keyWithoutSBS423(bIn) {
		t.Fatalf("PRE (ii): without the #423 fold the two raKeys must collide (the pre-#423 shared cell)")
	}
	t.Logf("raKeys equal=%v (pre-#423: true; post-#423: false)", aKey == bKey)

	var calls atomic.Int64
	got, served, err := raFullListServe(aliceCtx, gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, k423PerUserRows(t, &calls))
	if err != nil || !served {
		t.Fatalf("SETUP: alice must be served by the raFullList path; served=%v err=%v", served, err)
	}
	if b, _ := json.Marshal(got); !strings.Contains(string(b), k423Sentinel) {
		t.Fatalf("SETUP: alice's own page must contain the sentinel row; got %s", b)
	}
	if _, hit := cache.ResolvedCache().Get(aKey); !hit {
		t.Fatalf("SETUP: alice's full list must be cached under her raKey")
	}

	got, _, err = raFullListServe(bobCtx, gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, k423PerUserRows(t, &calls))
	if err != nil {
		t.Fatalf("bob serve: %v", err)
	}
	if b, _ := json.Marshal(got); strings.Contains(string(b), k423Sentinel) {
		t.Fatalf("#423 RAFULLLIST-CARRIER LEAK: bob — denied `get configmaps x/y` — was served alice's sentinel row from "+
			"the shared raFullList cell; got %s", b)
	}
}
