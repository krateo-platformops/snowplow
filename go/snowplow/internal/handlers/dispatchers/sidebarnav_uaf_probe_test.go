//go:build paired854 && sidebarnav

// sidebarnav_uaf_probe_test.go — the portal's restaction.sidebar-nav (portal 1.8.154), resolved
// end to end through snowplow's production path: restActionHandler.ServeHTTP, a cache-on
// ResourceWatcher (informer-served LISTs, the in-process RBAC snapshot), the real userAccessFilter
// re-gate on the `namespaces` stage, the caller's own (informer-served, RBAC-filtered) `pages` LIST,
// and the final filter. The visitor is granted `list flexes` in N of M tenant namespaces through a
// GROUP RoleBinding, as the sandbox's snapshot identity is (username sandbox-visitor, group
// sandbox-viewers). The js/wasm build runs this same code: only the transport and the data source
// (the sandbox's fake apiserver) differ, and both are inputs here.
//
//   - WithGroups: the caller presents the visitor's groups → the namespaces stage keeps exactly the N
//     granted namespaces, and the Menu gets N refs.
//   - WithoutGroups: the SAME visitor, but the request identity carries no groups (a token minted
//     without the snapshot's visitor groups) → the UAF keeps 0, the iterator falls back to the
//     release namespace, the visitor may not list flexes there, and the answer is 200 with 0 refs.
package dispatchers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	restactionsapi "github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const (
	snRelease = "krateo-system"
	snUser    = "sandbox-visitor"
	snGroup   = "sandbox-viewers"
)

var (
	snFlexGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
	snNsGVR   = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
)

// The sidebar-nav RESTAction as portal 1.8.154 renders it with Release.Namespace krateo-system and
// no preview sandbox namespace (helm/portal/templates/restaction.sidebar-nav.yaml).
func snRA() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata": map[string]any{"name": "sidebar-nav", "namespace": snRelease,
			"annotations": map[string]any{"krateo.io/verbose": "true"}},
		"spec": map[string]any{
			"api": []any{
				map[string]any{
					"name": "namespaces", "path": "/api/v1/namespaces", "verb": "GET",
					"headers": []any{"Accept: application/json"},
					"userAccessFilter": map[string]any{
						"verb": "list", "group": "widgets.templates.krateo.io", "resource": "flexes", "namespaceFrom": ".metadata.name",
					},
					"continueOnError": true, "errorKey": "namespacesError",
					"filter": `[ .namespaces.items[]? | .metadata.name | select(. != "") ]`,
				},
				map[string]any{
					"name": "pages",
					"dependsOn": map[string]any{"name": "namespaces",
						"iterator": `[ ((.namespaces // []) | if length > 0 then . else ["krateo-system"] end)[] | { ns: . } | select(.ns != "") ]`},
					"path":            `${ "/apis/widgets.templates.krateo.io/v1beta1/namespaces/" + .ns + "/flexes" }`,
					"verb":            "GET",
					"headers":         []any{"Accept: application/json"},
					"continueOnError": true, "errorKey": "pagesError",
					"filter": `[ .pages.items[]?
          | select(((.metadata.labels // {})["krateo.io/purpose"] // "") != "preview-draft")
          | { metadata: { name: .metadata.name,
                          namespace: .metadata.namespace,
                          annotations: (.metadata.annotations // {}) } } ]`,
				},
			},
			"filter": `[ (.pages // [])[]
      | select(((.metadata.annotations // {})["krateo.io/nav-path"] // "") != "")
    ] as $roots
    | ( [ $roots[]
      | (.metadata.annotations // {}) as $a
      | (.metadata.name) as $root
      | (.metadata.name | sub("^page-"; "")) as $slug
      | (($a["krateo.io/nav-order"] // "100") | tonumber) as $ord
      | ((($a["krateo.io/nav-link-only"] // "") == "true") as $linkOnly
      | ( (if $linkOnly
             then { path: $a["krateo.io/nav-path"], page: $slug, order: $ord, linkOnly: true }
             else { path: $a["krateo.io/nav-path"], page: $slug, order: $ord, resourceRefId: $root }
           end)
          + (if $a["krateo.io/nav-label"] then { label: $a["krateo.io/nav-label"] } else {} end)
          + (if $a["krateo.io/nav-icon"]  then { icon:  $a["krateo.io/nav-icon"]  } else {} end)
          + (if $a["krateo.io/nav-group"] then { group: $a["krateo.io/nav-group"] } else {} end) ),
        ( if $linkOnly then empty
          else ( ($a["krateo.io/nav-alias"] // "") | split(",")[] | select(length > 0)
                 | { path: ., page: $slug, order: $ord, resourceRefId: $root } )
          end ))
    ] ) as $entries
    | { items: ( $entries | unique | sort_by(.order) ),
        refs:  ( [ $roots[]
                    | select((((.metadata.annotations // {})["krateo.io/nav-link-only"]) // "") != "true")
                    | { id: .metadata.name, name: .metadata.name, namespace: .metadata.namespace } ] | unique ) }`,
		},
	}}
}

func snFlex(ns, name, navPath string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "Flex",
		"metadata": map[string]any{"namespace": ns, "name": name, "uid": "uid-" + ns + "-" + name,
			"annotations": map[string]any{"krateo.io/nav-path": navPath, "krateo.io/nav-label": name}},
		"spec": map[string]any{},
	}}
}

// snAPIServer is the transport's apiserver: discovery for the three groups the resolve touches.
func snAPIServer(t *testing.T) *httptest.Server {
	t.Helper()
	w := func(rw http.ResponseWriter, v any) {
		rw.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(rw).Encode(v)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api":
			w(rw, &metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions"}, Versions: []string{"v1"}})
		case "/api/v1":
			w(rw, &metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: "v1",
				APIResources: []metav1.APIResource{{Name: "namespaces", SingularName: "namespace", Namespaced: false, Kind: "Namespace", Verbs: metav1.Verbs{"get", "list", "watch"}}}})
		case "/apis":
			var gl []metav1.APIGroup
			for _, gv := range []schema.GroupVersion{h1RAGVR.GroupVersion(), snFlexGVR.GroupVersion()} {
				d := metav1.GroupVersionForDiscovery{GroupVersion: gv.String(), Version: gv.Version}
				gl = append(gl, metav1.APIGroup{Name: gv.Group, Versions: []metav1.GroupVersionForDiscovery{d}, PreferredVersion: d})
			}
			w(rw, &metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"}, Groups: gl})
		case "/apis/" + h1RAGVR.GroupVersion().String():
			w(rw, &metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: h1RAGVR.GroupVersion().String(),
				APIResources: []metav1.APIResource{{Name: "restactions", SingularName: "restaction", Namespaced: true, Kind: "RESTAction", Verbs: metav1.Verbs{"get", "list", "watch"}}}})
		case "/apis/" + snFlexGVR.GroupVersion().String():
			w(rw, &metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: snFlexGVR.GroupVersion().String(),
				APIResources: []metav1.APIResource{{Name: "flexes", SingularName: "flex", Namespaced: true, Kind: "Flex", Verbs: metav1.Verbs{"get", "list", "watch"}}}})
		default:
			http.NotFound(rw, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

type snResult struct {
	Status struct {
		Items []map[string]any `json:"items"`
		Refs  []map[string]any `json:"refs"`
	} `json:"status"`
}

func TestSidebarNavUAFKeepsTheVisitorsNamespaces(t *testing.T) {
	granted := []string{"tenant-a", "tenant-b", "tenant-c"} // N = 3
	others := []string{"tenant-d", "tenant-e"}              // M − N
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefresherForTest()
	prevLog := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prevLog)

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	rbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	rGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
	scheme := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		h1RAGVR: "RESTActionList", snFlexGVR: "FlexList", snNsGVR: "NamespaceList",
		crbGVR: "ClusterRoleBindingList", crGVR: "ClusterRoleList", rbGVR: "RoleBindingList", rGVR: "RoleList",
	}
	subj := []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: snGroup}}
	seed := []k8sruntime.Object{
		// snowplow's ServiceAccount: lists namespaces and flexes, reads RESTActions.
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "snowplow-sa"}, Rules: []rbacv1.PolicyRule{
			{Verbs: []string{"get", "list", "watch"}, APIGroups: []string{""}, Resources: []string{"namespaces"}},
			{Verbs: []string{"get", "list", "watch"}, APIGroups: []string{h1RAGVR.Group, snFlexGVR.Group}, Resources: []string{h1RAGVR.Resource, snFlexGVR.Resource}},
		}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "snowplow-sa", UID: types.UID("uid-snowplow-sa")},
			Subjects: []rbacv1.Subject{{Kind: "ServiceAccount", Name: "snowplow", Namespace: p865AuthnNS}},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "snowplow-sa"}},
		// the visitor's group may read RESTActions cluster-wide (the dispatch gate) ...
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "visitor-restactions"}, Rules: []rbacv1.PolicyRule{
			{Verbs: []string{"get"}, APIGroups: []string{h1RAGVR.Group}, Resources: []string{h1RAGVR.Resource}},
		}},
		// bound to system:authenticated, so the dispatch gate does not depend on the visitor's groups
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "visitor-restactions", UID: types.UID("uid-visitor-ra")},
			Subjects: []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "system:authenticated"}}, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "visitor-restactions"}},
	}
	for _, ns := range append(append([]string{snRelease}, granted...), others...) {
		seed = append(seed, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, UID: types.UID("uid-ns-" + ns)}})
	}
	// ... and list flexes in exactly the N granted namespaces (a Role + a group RoleBinding in each).
	for _, ns := range granted {
		seed = append(seed,
			&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: "flex-reader", Namespace: ns}, Rules: []rbacv1.PolicyRule{
				{Verbs: []string{"get", "list", "watch"}, APIGroups: []string{snFlexGVR.Group}, Resources: []string{snFlexGVR.Resource}},
			}},
			&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "flex-reader", Namespace: ns, UID: types.UID("uid-rb-" + ns)},
				Subjects: subj, RoleRef: rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "flex-reader"}})
	}
	ra := snRA()
	seed = append(seed, ra.DeepCopy())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	// Flexes go in through their real GVR: seeding an Unstructured lets the fake tracker guess the
	// resource from the kind ("Flex" -> "flexs"), and the flexes informer would then see none.
	for _, ns := range append(append([]string{snRelease}, granted...), others...) {
		if _, err := dyn.Resource(snFlexGVR).Namespace(ns).Create(context.Background(), snFlex(ns, "page-"+ns, "/"+ns), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		t.Fatal(err)
	}
	defer rw.Stop()
	for _, gvr := range []schema.GroupVersionResource{h1RAGVR, snFlexGVR, snNsGVR} {
		_, syncCh := rw.EnsureResourceType(gvr)
		select {
		case <-syncCh:
		case <-time.After(2 * time.Minute):
			t.Fatalf("informer %s did not sync", gvr)
		}
	}
	sctx, scancel := context.WithTimeout(context.Background(), 2*time.Minute)
	if err := rw.WaitForCacheSync(sctx, 2*time.Minute); err != nil {
		t.Fatal(err)
	}
	scancel()
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	prevGlobal := cache.Global()
	cache.SetGlobal(rw)
	defer cache.SetGlobal(prevGlobal)
	for _, gvr := range []schema.GroupVersionResource{h1RAGVR, snFlexGVR, snNsGVR} {
		for end := time.Now().Add(time.Minute); !rw.IsServable(gvr); time.Sleep(10 * time.Millisecond) {
			if time.Now().After(end) {
				t.Fatalf("%s never servable", gvr)
			}
		}
	}

	srv := snAPIServer(t)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	defer env.SetTestMode(prevTM)
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		snUser + "-clientconfig": {ObjectMeta: authnMeta(p865AuthnNS, snUser+"-clientconfig", snUser),
			Data: map[string][]byte{"server-url": []byte(srv.URL), "token": []byte("tok-visitor")}},
	}})
	cache.ForceSecretsCacheReadyForTest(p865AuthnNS)
	defer func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() }()
	restoreFetch := setFetchObjectForTest(func(req *http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: ra.DeepCopy()}
	})
	defer restoreFetch()
	handler := &restActionHandler{authnNS: p865AuthnNS, saRC: &rest.Config{Host: srv.URL}}
	// snowplow's ServiceAccount endpoint, as a pod has it (the projected token file): the UAF stage dials it.
	defer restactionsapi.SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: srv.URL, Token: phase1RealSAToken}, nil
	})()

	call := func(groups []string) (int, snResult, string) {
		cache.ResetResolvedCacheForTest() // every subtest resolves; none is served from the previous one's L1 cell
		ctx := xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: snUser, Groups: groups}),
			xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-visitor"}),
		)
		u := "/call?apiVersion=" + url.QueryEscape(h1RAGVR.Group+"/"+h1RAGVR.Version) +
			"&resource=" + h1RAGVR.Resource + "&name=sidebar-nav&namespace=" + snRelease
		req := httptest.NewRequest(http.MethodGet, u, nil).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer "+in443JWT())
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		var res snResult
		_ = json.Unmarshal(rec.Body.Bytes(), &res)
		var raw struct {
			Status map[string]any `json:"status"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &raw)
		keys := []string{}
		for k := range raw.Status {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b := rec.Body.Bytes()
		if len(b) > 1500 {
			b = b[:1500]
		}
		t.Logf("status keys %v; body: %s", keys, b)
		return rec.Code, res, rec.Header().Get("X-Snowplow-Stage-Outcomes")
	}
	refNS := func(r snResult) []string {
		var out []string
		for _, ref := range r.Status.Refs {
			out = append(out, fmt.Sprint(ref["namespace"]))
		}
		sort.Strings(out)
		return out
	}

	t.Run("WithGroups", func(t *testing.T) {
		code, res, outcomes := call([]string{snGroup})
		got := refNS(res)
		t.Logf("visitor with its groups: HTTP %d, %d items, refs in %v, stage outcomes %q", code, len(res.Status.Items), got, outcomes)
		if code != http.StatusOK || fmt.Sprint(got) != fmt.Sprint(granted) {
			t.Fatalf("want 200 and refs in exactly the %d granted namespaces %v, got HTTP %d refs in %v", len(granted), granted, code, got)
		}
	})
	t.Run("SAEndpointUnavailable", func(t *testing.T) {
		// No projected SA token file (dynamic.ServiceAccountEndpoint reads
		// /var/run/secrets/kubernetes.io/serviceaccount/token): the UAF stage takes resolveStageEndpoint's
		// degraded branch.
		restore := restactionsapi.SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) {
			return nil, fmt.Errorf("dynamic.sa: read SA token: no such file or directory")
		})
		defer restore()
		code, res, outcomes := call([]string{snGroup})
		t.Logf("SA token file unavailable: HTTP %d, %d items, refs in %v, stage outcomes %q", code, len(res.Status.Items), refNS(res), outcomes)
	})
	t.Run("WithoutGroups", func(t *testing.T) {
		code, res, outcomes := call(nil)
		got := refNS(res)
		t.Logf("the same visitor, its token carrying no groups: HTTP %d, %d items, refs in %v, stage outcomes %q", code, len(res.Status.Items), got, outcomes)
		if code != http.StatusOK || len(got) != 0 {
			t.Fatalf("documented mechanism: want 200 with 0 refs (the group-bound grants do not match a groupless identity), got HTTP %d refs in %v", code, got)
		}
	})
}
