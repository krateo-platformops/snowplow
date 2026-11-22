//go:build js && wasm && sidebarnav

// The sidebar-nav probe in the browser build: snowplow's cache (informers, RBAC snapshot) runs over
// the Agentiko sandbox's own fake apiserver (internal/sandbox/fakeapiserver), serving a snapshot that
// holds N+M tenant namespaces with one nav flex each, the visitor's group granted `list flexes` in
// exactly N of them, and the sidebar-nav RESTAction as portal 1.8.154 renders it. The /call goes
// through the real restActionHandler, the UAF stage dials the fake as the ServiceAccount, the pages
// stage as the visitor (the token the fake authenticates as the snapshot's visitor identity).
package dispatchers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	"github.com/krateo-platformops/snowplow/internal/sandbox/fakeapiserver"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sdynamic "k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
)

const (
	swRelease = "krateo-system"
	swUser    = "sandbox-visitor"
	swGroup   = "sandbox-viewers"
)

var (
	swFlexGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}
	swNsGVR   = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
)

func swRAObject() map[string]any {
	return map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": "sidebar-nav", "namespace": swRelease},
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
	}
}

// swSnapshot is snapshot.json and its one object page.
func swSnapshot(granted, others []string) ([]byte, []byte) {
	uid := 0
	var items []any
	add := func(o map[string]any) {
		uid++
		md := o["metadata"].(map[string]any)
		md["uid"] = fmt.Sprintf("00000000-0000-5000-8000-%012d", uid)
		md["resourceVersion"] = fmt.Sprint(uid)
		items = append(items, o)
	}
	crd := func(group, plural, singular, kind, version string) {
		add(map[string]any{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
			"metadata": map[string]any{"name": plural + "." + group},
			"spec": map[string]any{"group": group, "scope": "Namespaced",
				"names":    map[string]any{"plural": plural, "singular": singular, "kind": kind, "listKind": kind + "List"},
				"versions": []any{map[string]any{"name": version, "served": true, "storage": true, "schema": map[string]any{"openAPIV3Schema": map[string]any{"type": "object", "x-kubernetes-preserve-unknown-fields": true}}}}}})
	}
	crd(swFlexGVR.Group, "flexes", "flex", "Flex", swFlexGVR.Version)
	crd(h1RAGVR.Group, h1RAGVR.Resource, "restaction", "RESTAction", h1RAGVR.Version)
	rbac := "rbac.authorization.k8s.io/v1"
	rule := func(groups, resources, verbs []string) map[string]any {
		return map[string]any{"apiGroups": groups, "resources": resources, "verbs": verbs}
	}
	// snowplow's ServiceAccount reads what it caches and serves.
	add(map[string]any{"apiVersion": rbac, "kind": "ClusterRole", "metadata": map[string]any{"name": "snowplow-sa"},
		"rules": []any{rule([]string{"*"}, []string{"*"}, []string{"get", "list", "watch"})}})
	add(map[string]any{"apiVersion": rbac, "kind": "ClusterRoleBinding", "metadata": map[string]any{"name": "snowplow-sa"},
		"subjects": []any{map[string]any{"kind": "ServiceAccount", "name": "snowplow", "namespace": swRelease}},
		"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "snowplow-sa"}})
	// every authenticated user may get RESTActions (the dispatch gate)
	add(map[string]any{"apiVersion": rbac, "kind": "ClusterRole", "metadata": map[string]any{"name": "visitor-restactions"},
		"rules": []any{rule([]string{h1RAGVR.Group}, []string{h1RAGVR.Resource}, []string{"get"})}})
	add(map[string]any{"apiVersion": rbac, "kind": "ClusterRoleBinding", "metadata": map[string]any{"name": "visitor-restactions"},
		"subjects": []any{map[string]any{"kind": "Group", "apiGroup": "rbac.authorization.k8s.io", "name": "system:authenticated"}},
		"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": "visitor-restactions"}})
	for _, ns := range append(append([]string{swRelease}, granted...), others...) {
		add(map[string]any{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]any{"name": ns}})
		add(map[string]any{"apiVersion": swFlexGVR.Group + "/" + swFlexGVR.Version, "kind": "Flex",
			"metadata": map[string]any{"namespace": ns, "name": "page-" + ns, "generation": 1,
				"annotations": map[string]any{"krateo.io/nav-path": "/" + ns, "krateo.io/nav-label": "page-" + ns}},
			"spec": map[string]any{}})
	}
	// the visitor's group lists flexes in exactly the N granted namespaces
	for _, ns := range granted {
		add(map[string]any{"apiVersion": rbac, "kind": "Role", "metadata": map[string]any{"name": "flex-reader", "namespace": ns},
			"rules": []any{rule([]string{swFlexGVR.Group}, []string{swFlexGVR.Resource}, []string{"get", "list", "watch"})}})
		add(map[string]any{"apiVersion": rbac, "kind": "RoleBinding", "metadata": map[string]any{"name": "flex-reader", "namespace": ns},
			"subjects": []any{map[string]any{"kind": "Group", "apiGroup": "rbac.authorization.k8s.io", "name": swGroup}},
			"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "Role", "name": "flex-reader"}})
	}
	ra := swRAObject()
	ra["metadata"].(map[string]any)["generation"] = 1
	add(ra)
	page, _ := json.Marshal(map[string]any{"items": items})

	res := func(name, singular, kind string, namespaced bool, verbs ...string) map[string]any {
		return map[string]any{"name": name, "singularName": singular, "namespaced": namespaced, "kind": kind, "verbs": verbs}
	}
	rl := func(gv string, rs ...map[string]any) map[string]any {
		return map[string]any{"kind": "APIResourceList", "groupVersion": gv, "resources": rs}
	}
	group := func(name, version string) map[string]any {
		gv := map[string]any{"groupVersion": name + "/" + version, "version": version}
		return map[string]any{"name": name, "versions": []any{gv}, "preferredVersion": gv}
	}
	snap, _ := json.Marshal(map[string]any{
		"schema": 1,
		"capturedFrom": map[string]any{"cluster": "test-fixture", "at": "2026-11-22T00:00:00Z", "server": "v1.36.4",
			"snowplow": "test-fixture", "frontend": "test-fixture", "files": map[string]any{},
			"demo": map[string]any{"seed": 1, "generator": "hand-written test fixture"}, "review": []any{}},
		"resourceVersion": "1000",
		"objects":         []any{"objects/000.json"},
		"discovery": map[string]any{
			"api": map[string]any{"kind": "APIVersions", "versions": []any{"v1"}},
			"apis": map[string]any{"kind": "APIGroupList", "apiVersion": "v1", "groups": []any{
				group("rbac.authorization.k8s.io", "v1"), group("apiextensions.k8s.io", "v1"),
				group(swFlexGVR.Group, swFlexGVR.Version), group(h1RAGVR.Group, h1RAGVR.Version)}},
			"resources": map[string]any{
				"v1": rl("v1", res("namespaces", "namespace", "Namespace", false, "get", "list", "watch"),
					res("secrets", "secret", "Secret", true, "get", "list", "watch"),
					res("configmaps", "configmap", "ConfigMap", true, "get", "list", "watch")),
				"rbac.authorization.k8s.io/v1": rl("rbac.authorization.k8s.io/v1",
					res("clusterroles", "clusterrole", "ClusterRole", false, "get", "list", "watch"),
					res("clusterrolebindings", "clusterrolebinding", "ClusterRoleBinding", false, "get", "list", "watch"),
					res("roles", "role", "Role", true, "get", "list", "watch"),
					res("rolebindings", "rolebinding", "RoleBinding", true, "get", "list", "watch")),
				"apiextensions.k8s.io/v1": rl("apiextensions.k8s.io/v1",
					res("customresourcedefinitions", "customresourcedefinition", "CustomResourceDefinition", false, "get", "list", "watch")),
				swFlexGVR.Group + "/" + swFlexGVR.Version: rl(swFlexGVR.Group+"/"+swFlexGVR.Version,
					res("flexes", "flex", "Flex", true, "get", "list", "watch")),
				h1RAGVR.Group + "/" + h1RAGVR.Version: rl(h1RAGVR.Group+"/"+h1RAGVR.Version,
					res(h1RAGVR.Resource, "restaction", "RESTAction", true, "get", "list", "watch")),
			},
		},
		"identity": map[string]any{"authnNamespace": swRelease,
			"serviceAccount": map[string]any{"namespace": swRelease, "name": "snowplow"},
			"visitor":        map[string]any{"username": swUser, "groups": []any{swGroup}, "name": "Sandbox Visitor"}},
		"scripted": map[string]any{"applyStatus": []any{}},
	})
	return snap, page
}

func TestSidebarNavUAFKeepsTheVisitorsNamespacesInTheBrowserBuild(t *testing.T) {
	granted := []string{"tenant-a", "tenant-b", "tenant-c"} // N = 3
	others := []string{"tenant-d", "tenant-e"}              // M − N
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefresherForTest()

	snap, page := swSnapshot(granted, others)
	fs, err := fakeapiserver.New(fakeapiserver.Options{
		Snapshot: snap, Pages: [][]byte{page}, SAToken: phase1RealSAToken, VisitorToken: "visitor-token",
		FrontendConfig: []byte(`{"api":{}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(fs)
	defer srv.Close()

	// snowplow's cache, as the ServiceAccount, over the fake
	dyn, err := k8sdynamic.NewForConfig(&rest.Config{Host: srv.URL, BearerToken: phase1RealSAToken})
	if err != nil {
		t.Fatal(err)
	}
	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		t.Fatal(err)
	}
	defer rw.Stop()
	for _, gvr := range []schema.GroupVersionResource{h1RAGVR, swFlexGVR, swNsGVR} {
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
	for _, gvr := range []schema.GroupVersionResource{h1RAGVR, swFlexGVR, swNsGVR} {
		for end := time.Now().Add(time.Minute); !rw.IsServable(gvr); time.Sleep(10 * time.Millisecond) {
			if time.Now().After(end) {
				t.Fatalf("%s never servable", gvr)
			}
		}
	}

	prevTM := env.TestMode()
	env.SetTestMode(true)
	defer env.SetTestMode(prevTM)
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		swUser + "-clientconfig": {ObjectMeta: authnMeta(swRelease, swUser+"-clientconfig", swUser),
			Data: map[string][]byte{"server-url": []byte(srv.URL), "token": []byte("visitor-token")}},
	}})
	cache.ForceSecretsCacheReadyForTest(swRelease)
	defer func() { cache.ResetSecretsSnapshotForTest(); cache.ResetSecretsInformerForTest() }()
	ra := &unstructured.Unstructured{Object: swRAObject()}
	restoreFetch := setFetchObjectForTest(func(req *http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: ra.DeepCopy()}
	})
	defer restoreFetch()
	handler := &restActionHandler{authnNS: swRelease, saRC: &rest.Config{Host: srv.URL, BearerToken: phase1RealSAToken}}
	// snowplow's ServiceAccount endpoint (the mounted token file the fake's SAToken stands for)
	defer restactionsapi.SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: srv.URL, Token: phase1RealSAToken}, nil
	})()

	call := func(groups []string) (int, []string, int, string) {
		cache.ResetResolvedCacheForTest()
		ctx := xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: swUser, Groups: groups}),
			xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, Token: "visitor-token"}),
		)
		u := "/call?apiVersion=" + url.QueryEscape(h1RAGVR.Group+"/"+h1RAGVR.Version) +
			"&resource=" + h1RAGVR.Resource + "&name=sidebar-nav&namespace=" + swRelease
		req := httptest.NewRequest(http.MethodGet, u, nil).WithContext(ctx)
		req.Header.Set("Authorization", "Bearer "+in443JWT())
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		var res struct {
			Status struct {
				Items []map[string]any `json:"items"`
				Refs  []map[string]any `json:"refs"`
			} `json:"status"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &res)
		var ns []string
		for _, r := range res.Status.Refs {
			ns = append(ns, fmt.Sprint(r["namespace"]))
		}
		sort.Strings(ns)
		return rec.Code, ns, len(res.Status.Items), rec.Header().Get("X-Snowplow-Stage-Outcomes")
	}

	t.Run("WithGroups", func(t *testing.T) {
		code, got, items, outcomes := call([]string{swGroup})
		t.Logf("browser build, visitor with its groups: HTTP %d, %d items, refs in %v, stage outcomes %q", code, items, got, outcomes)
		if code != http.StatusOK || fmt.Sprint(got) != fmt.Sprint(granted) {
			t.Fatalf("want 200 and refs in exactly the %d granted namespaces %v, got HTTP %d refs in %v", len(granted), granted, code, got)
		}
	})
	t.Run("WithoutGroups", func(t *testing.T) {
		code, got, items, outcomes := call(nil)
		t.Logf("browser build, the same visitor with no groups in its token: HTTP %d, %d items, refs in %v, stage outcomes %q", code, items, got, outcomes)
	})
}
