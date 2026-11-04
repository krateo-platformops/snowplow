// issue1049_name_selected_list_test.go — #1049 through the serve paths: a LIST
// with `fieldSelector=metadata.name=x`, served from snowplow's own copy, is
// authorized with Name x, as the apiserver authorizes it.
//
// RED before the fix: a requester whose only grant is `list resourceNames: [x]`
// got the per-identity Forbidden from the informer pivot (dispatchViaInformer)
// and from the identity-free content cell (apistageContentServe), while the
// apiserver — the cache=off path — answers the same request 200 with x.
//
// The rbac-level parity against the vendored RequestInfo resolver, cache on
// and off, is internal/rbac/evaltest/issue1049_collection_check_parity_test.go.
// These arms prove each serve site hands the gate the request's own query.

package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const (
	i1049NSUser      = "i1049-role-list-x"        // Role in team-a: list resourceNames [x]
	i1049ClusterUser = "i1049-clusterrole-list-x" // ClusterRoleBinding: list resourceNames [x]
)

// newI1049Watcher seeds widgets team-a/{x,y} and team-b/x, and the two
// name-scoped grants. No requester holds an unscoped list.
func newI1049Watcher(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	ResetClusterListAsyncStateForTest()
	t.Cleanup(func() {
		ResetClusterListAsyncStateForTest()
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	tm := func(kind string) metav1.TypeMeta {
		return metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: kind}
	}
	scoped := []rbacv1.PolicyRule{{
		APIGroups: []string{f1WidgetsGVR.Group}, Resources: []string{f1WidgetsGVR.Resource},
		Verbs: []string{"list"}, ResourceNames: []string{"x"},
	}}
	user := func(n string) []rbacv1.Subject {
		return []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: n}}
	}
	seed := []runtime.Object{
		q618Widget("team-a", "x", nil),
		q618Widget("team-a", "y", nil),
		q618Widget("team-b", "x", nil),
		&rbacv1.Role{TypeMeta: tm("Role"), ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "list-x"}, Rules: scoped},
		&rbacv1.RoleBinding{TypeMeta: tm("RoleBinding"), ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "list-x"},
			Subjects: user(i1049NSUser), RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "list-x"}},
		&rbacv1.ClusterRole{TypeMeta: tm("ClusterRole"), ObjectMeta: metav1.ObjectMeta{Name: "list-x"}, Rules: scoped},
		&rbacv1.ClusterRoleBinding{TypeMeta: tm("ClusterRoleBinding"), ObjectMeta: metav1.ObjectMeta{Name: "list-x"},
			Subjects: user(i1049ClusterUser), RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "list-x"}},
	}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		f1WidgetsGVR: "WidgetList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	rw, err := cache.NewResourceWatcher(context.Background(), dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...))
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: rw=%v err=%v", rw, err)
	}
	t.Cleanup(rw.Stop)
	if _, syncCh := rw.EnsureResourceType(f1WidgetsGVR); syncCh != nil {
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("widgets informer did not sync within 5s")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
}

func i1049Ctx(user string) context.Context {
	ctx := xcontext.BuildContext(context.Background(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: user}))
	return cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
}

// i1049Items is the sorted "<ns>/<name>" of a LIST envelope ([]byte or a
// decoded value).
func i1049Items(t *testing.T, v any) []string {
	t.Helper()
	raw, ok := v.([]byte)
	if !ok {
		var err error
		if raw, err = json.Marshal(v); err != nil {
			t.Fatalf("marshal served value: %v", err)
		}
	}
	var env struct {
		Items []struct {
			Metadata struct{ Namespace, Name string } `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("served body is not a LIST envelope: %v", err)
	}
	out := []string{}
	for _, it := range env.Items {
		out = append(out, it.Metadata.Namespace+"/"+it.Metadata.Name)
	}
	sort.Strings(out)
	return out
}

func i1049Sel(sel string) string { return "?fieldSelector=" + url.QueryEscape(sel) }

var i1049Arms = []struct {
	name, user, path string
	want             []string // nil = the apiserver's 403
}{
	{"ns list name=x", i1049NSUser, q618NSPath("team-a") + i1049Sel("metadata.name=x"), []string{"team-a/x"}},
	{"ns list name==x", i1049NSUser, q618NSPath("team-a") + i1049Sel("metadata.name==x"), []string{"team-a/x"}},
	{"ns list namespace,name=x", i1049NSUser, q618NSPath("team-a") + i1049Sel("metadata.namespace=team-a,metadata.name=x"), []string{"team-a/x"}},
	{"ns list name=x + label", i1049NSUser, q618NSPath("team-a") + i1049Sel("metadata.name=x") + "&labelSelector=" + url.QueryEscape("!tier"), []string{"team-a/x"}},
	{"ns list name=y", i1049NSUser, q618NSPath("team-a") + i1049Sel("metadata.name=y"), nil},
	{"ns list no selector", i1049NSUser, q618NSPath("team-a"), nil},
	{"ns list name!=y", i1049NSUser, q618NSPath("team-a") + i1049Sel("metadata.name!=y"), nil},
	{"ns list namespace selector", i1049NSUser, q618NSPath("team-a") + i1049Sel("metadata.namespace=team-a"), nil},
	{"ns list label selector", i1049NSUser, q618NSPath("team-a") + "?labelSelector=" + url.QueryEscape("!tier"), nil},
	// The terms are sorted before the Name is read (fields.ParseSelector): this
	// request carries x, so it is allowed, and it selects nothing.
	{"ns list name=y,name=x (carries x, selects nothing)", i1049NSUser, q618NSPath("team-a") + i1049Sel("metadata.name=y,metadata.name=x"), []string{}},
	{"ns list name=z,name=y (carries y)", i1049NSUser, q618NSPath("team-a") + i1049Sel("metadata.name=z,metadata.name=y"), nil},
	// #1073 F2/F3: names compare exactly.
	{"ns list name=xx (no prefix match)", i1049NSUser, q618NSPath("team-a") + i1049Sel("metadata.name=xx"), nil},
	{"ns list name=X (no case folding)", i1049NSUser, q618NSPath("team-a") + i1049Sel("metadata.name=X"), nil},
	// #1073 F1: a literal '#' in the rendered path — the apiserver never
	// receives the fragment, so it authorizes y here (403 for a grant on x)…
	{"ns list name=y#,name=x (Kubernetes: y)", i1049NSUser, q618NSPath("team-a") + "?fieldSelector=metadata.name%3Dy#,metadata.name=x", nil},
	// …and x here (200 with x).
	{"ns list name=x#frag (Kubernetes: x)", i1049NSUser, q618NSPath("team-a") + "?fieldSelector=metadata.name%3Dx#frag", []string{"team-a/x"}},
	{"ns list name=x in team-b, Role only in team-a", i1049NSUser, q618NSPath("team-b") + i1049Sel("metadata.name=x"), nil},
	{"cluster list name=x, Role only", i1049NSUser, q618ClusterPath + i1049Sel("metadata.name=x"), nil},
	{"cluster list name=x, ClusterRole", i1049ClusterUser, q618ClusterPath + i1049Sel("metadata.name=x"), []string{"team-a/x", "team-b/x"}},
	{"cluster list no selector, ClusterRole", i1049ClusterUser, q618ClusterPath, nil},
	{"ns list name=x, ClusterRole", i1049ClusterUser, q618NSPath("team-b") + i1049Sel("metadata.name=x"), []string{"team-b/x"}},
}

func TestIssue1049_DispatchViaInformer_NameSelectedListCarriesTheName(t *testing.T) {
	newI1049Watcher(t)
	for _, a := range i1049Arms {
		t.Run(a.name, func(t *testing.T) {
			raw, served, _, err := dispatchViaInformer(i1049Ctx(a.user), buildCall(http.MethodGet, a.path))
			if a.want == nil {
				if served || !isRegateDenied(err) {
					t.Fatalf("%s: want the apiserver's 403 (a regate deny); served=%v err=%v", a.path, served, err)
				}
				return
			}
			if err != nil || !served {
				t.Fatalf("RED #1049 %s: Kubernetes allows this list; served=%v err=%v", a.path, served, err)
			}
			if got := i1049Items(t, raw); !eqNames(got, a.want) {
				t.Fatalf("%s: served %v, want %v", a.path, got, a.want)
			}
		})
	}
}

func TestIssue1049_ApistageContentServe_NameSelectedListCarriesTheName(t *testing.T) {
	newI1049Watcher(t)
	store := cache.ResolvedCache()
	// Two passes: the first fills each identity-free cell (MISS), the second
	// serves it (HIT); the gate runs at serve on both.
	for pass := 0; pass < 2; pass++ {
		for _, a := range i1049Arms {
			v, served, ok, _, err := apistageContentServe(i1049Ctx(a.user), store, buildCall(http.MethodGet, a.path), false)
			if a.want == nil {
				if served || !isRegateDenied(err) {
					t.Errorf("pass %d %s: want the apiserver's 403 (a regate deny); ok=%v served=%v err=%v", pass, a.name, ok, served, err)
				}
				continue
			}
			if err != nil || !ok || !served {
				t.Errorf("RED #1049 pass %d %s: Kubernetes allows this list; ok=%v served=%v err=%v", pass, a.name, ok, served, err)
				continue
			}
			if got := i1049Items(t, v); !eqNames(got, a.want) {
				t.Errorf("pass %d %s: served %v, want %v", pass, a.name, got, a.want)
			}
		}
	}
}

// TestIssue1049_GateContentEnvelope_NameSelectedList covers the RawJSON
// fallback gate (an entry stored without pre-parsed items).
func TestIssue1049_GateContentEnvelope_NameSelectedList(t *testing.T) {
	newI1049Watcher(t)
	env := []byte(`{"apiVersion":"widgets.krateo.io/v1","kind":"WidgetList","items":[` +
		`{"apiVersion":"widgets.krateo.io/v1","kind":"Widget","metadata":{"namespace":"team-a","name":"x"}}]}`)
	ctx := i1049Ctx(i1049NSUser)
	v, err := gateContentEnvelope(ctx, callPathVerb{path: q618NSPath("team-a") + i1049Sel("metadata.name=x")}, env, false)
	if err != nil {
		t.Fatalf("RED #1049: the RawJSON gate denied a list Kubernetes allows: %v", err)
	}
	if got := i1049Items(t, v); !eqNames(got, []string{"team-a/x"}) {
		t.Fatalf("served %v, want [team-a/x]", got)
	}
	if _, err := gateContentEnvelope(ctx, callPathVerb{path: q618NSPath("team-a")}, env, false); !isRegateDenied(err) {
		t.Fatalf("the unselected list must be the apiserver's 403; err=%v", err)
	}
}

// TestIssue1049_ItemBackstopChecksEachItemsName: step 2 of filterListByRBAC
// moves the request's check to each item. A name-selected request whose copy
// (wrongly) holds another name must not serve it through a resourceNames rule;
// an unselected request carries no name per item either.
func TestIssue1049_ItemBackstopChecksEachItemsName(t *testing.T) {
	newI1049Watcher(t)
	item := func(ns, name string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "widgets.krateo.io/v1", "kind": "Widget"}}
		u.SetNamespace(ns)
		u.SetName(name)
		return u
	}
	items := []*unstructured.Unstructured{item("team-a", "x"), item("team-a", "y")}
	kept, err := filterListByRBAC(i1049Ctx(i1049NSUser), f1WidgetsGVR, "team-a", "fieldSelector=metadata.name%3Dx", items)
	if err != nil {
		t.Fatalf("RED #1049: scope denied a list Kubernetes allows: %v", err)
	}
	if len(kept) != 1 || kept[0].GetName() != "x" {
		names := []string{}
		for _, k := range kept {
			names = append(names, k.GetName())
		}
		t.Fatalf("backstop kept %v, want [x]: an item the request does not name passed a resourceNames:[x] rule", names)
	}
}

// TestIssue1049_InternalDispatch_NameSelectedListCarriesTheName: branch C (the
// SA read re-gated with the requester's identity) authorizes the requester's
// own query. The fake apiserver ignores selectors, so the backstop is also on
// the path: the SA LIST returns team-a/{x,y} and only x may be served.
func TestIssue1049_InternalDispatch_NameSelectedListCarriesTheName(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	const user = "i1049-ra-list-x"
	tm := func(kind string) metav1.TypeMeta {
		return metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: kind}
	}
	newDispatchWatcher(t,
		&rbacv1.Role{TypeMeta: tm("Role"), ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "ra-list-x"},
			Rules: []rbacv1.PolicyRule{{APIGroups: []string{dispatchTestGVR.Group}, Resources: []string{dispatchTestGVR.Resource},
				Verbs: []string{"list"}, ResourceNames: []string{"x"}}}},
		&rbacv1.RoleBinding{TypeMeta: tm("RoleBinding"), ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "ra-list-x"},
			Subjects: []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: user}},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "ra-list-x"}},
	)
	rc := newFakeRestActionAPIServer(t, []fakeRAItem{{"team-a", "x"}, {"team-a", "y"}})
	ctx := ownClusterCtx(cache.WithInternalRESTConfig(ctxWithUser(user), rc))
	const path = "/apis/templates.krateo.io/v1/namespaces/team-a/restactions"

	raw, served, err := dispatchViaInternalRESTConfig(ctx, buildCall(http.MethodGet, path+i1049Sel("metadata.name=x")))
	if err != nil || !served {
		t.Fatalf("RED #1049: branch C denied a list Kubernetes allows; served=%v err=%v", served, err)
	}
	if got := servedItemNames(t, raw); len(got) != 1 || !got["x"] {
		t.Fatalf("branch C served %v, want {x}", got)
	}
	for _, q := range []string{"", i1049Sel("metadata.name=y")} {
		if raw, served, err := dispatchViaInternalRESTConfig(ctx, buildCall(http.MethodGet, path+q)); served || raw != nil || !isRegateDenied(err) {
			t.Fatalf("branch C %q: want the apiserver's 403; served=%v err=%v", q, served, err)
		}
	}
}

// TestIssue1049_ClusterListCollapse_NameScopedClusterGrant: the collapse's own
// RBAC gate (gate 5) checks the collapsed call with its query, so a requester
// whose cluster grant is `list resourceNames: [x]` takes the collapse for a
// `metadata.name=x` iterator, exactly as its serve gate allows that call.
func TestIssue1049_ClusterListCollapse_NameScopedClusterGrant(t *testing.T) {
	withClusterListCollapseEnabledForTest(t)
	newI1049Watcher(t)
	cache.ResetClusterListCellCountersForTest()
	stage := &templates.API{
		Name: "sel",
		Path: `${ "/apis/widgets.krateo.io/v1/namespaces/" + . + "/widgets?fieldSelector=metadata.name%3Dx" }`,
		Verb: ptr.To(http.MethodGet),
		DependsOn: &templates.Dependency{
			Iterator: ptr.To(`["team-a","team-b"]`),
		},
	}
	for round := 0; round < 2; round++ {
		warm0, _ := cache.ClusterListCellCounters()
		got := namesIn(q618Resolve(t, i1049Ctx(i1049ClusterUser), stage)["sel"])
		if !eqNames(got, []string{"x"}) {
			t.Fatalf("round %d: iterator got %v, want [x]", round, got)
		}
		warm1, _ := cache.ClusterListCellCounters()
		if round == 1 && warm1 == warm0 {
			t.Fatalf("RED #1049 round 1: the warm collapse was not taken — gate 5 checked the collapsed " +
				"call without the Name its metadata.name selector carries")
		}
		q618WaitAsyncPopulate(t)
	}
}

// TestIssue1049_ItemCheck_NamesAnItemOnlyWhenTheRequestDoes pins itemCheck: a
// request without a Name never gains one per item (a resourceNames rule must
// not grant an unselected list through the backstop).
func TestIssue1049_ItemCheck_NamesAnItemOnlyWhenTheRequestDoes(t *testing.T) {
	it := &unstructured.Unstructured{Object: map[string]any{}}
	it.SetNamespace("team-b")
	it.SetName("x")
	unnamed := itemCheck(rbac.CollectionCheck("u", nil, f1WidgetsGVR, "", ""), it)
	if unnamed.Name != "" || unnamed.Namespace != "team-b" || unnamed.Verb != "list" {
		t.Fatalf("unselected request, item team-b/x: check = %+v, want list team-b with no name", unnamed)
	}
	named := itemCheck(rbac.CollectionCheck("u", nil, f1WidgetsGVR, "", "fieldSelector=metadata.name%3Dy"), it)
	if named.Name != "x" || named.Namespace != "team-b" {
		t.Fatalf("request naming y, item team-b/x: check = %+v, want the item's own coordinates team-b/x", named)
	}
}
