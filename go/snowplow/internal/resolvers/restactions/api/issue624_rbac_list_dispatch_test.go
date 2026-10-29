//go:build unit || integration

package api

// issue624_rbac_list_dispatch_test.go — #624 falsifier at the DISPATCH layer.
//
// The cache-layer arm (internal/cache/issue624_typed_store_items_test.go)
// proves the watcher renders typed RBAC slots. This proves what a RESTAction
// stage actually receives: dispatchViaInformer, on BOTH branches —
//
//   - decoded: an ordinary resolve (per-item RBAC filter runs; admin identity),
//   - raw (#578): an api-stage CONTENT resolve (cache.WithApistageContentResolve),
//
// for every RBAC kind, cluster-wide and per namespace, plus GET-by-name. The
// served `items` must EQUAL the apiserver's (the fake dynamic client's) items.
// Before the fix every LIST here came back {"items":[]} with served=true —
// the exact shape #624 reproduced — and every GET missed.

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"sort"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func issue624DispatchSeed() []runtime.Object {
	tm := func(kind string) metav1.TypeMeta {
		return metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: kind}
	}
	om := func(ns, name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Namespace: ns, Name: name,
			Labels: map[string]string{"krateo.io/managed-by": "portal-access"}}
	}
	rules := []rbacv1.PolicyRule{{APIGroups: []string{"composition.krateo.io"}, Resources: []string{"*"}, Verbs: []string{"get", "list"}}}
	var out []runtime.Object
	for _, ns := range []string{"ns-a", "ns-b"} {
		out = append(out,
			&rbacv1.Role{TypeMeta: tm("Role"), ObjectMeta: om(ns, "grant-"+ns), Rules: rules},
			&rbacv1.RoleBinding{TypeMeta: tm("RoleBinding"), ObjectMeta: om(ns, "grant-"+ns),
				Subjects: []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: rbacv1.GroupName, Name: "alice-" + ns}},
				RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "Role", Name: "grant-" + ns}},
		)
	}
	out = append(out,
		&rbacv1.ClusterRole{TypeMeta: tm("ClusterRole"), ObjectMeta: om("", "grant-cluster"), Rules: rules},
		&rbacv1.ClusterRoleBinding{TypeMeta: tm("ClusterRoleBinding"), ObjectMeta: om("", "grant-cluster"),
			Subjects: []rbacv1.Subject{{Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: "devs"}},
			RoleRef:  rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "grant-cluster"}},
	)
	return out
}

// newIssue624Watcher mirrors newDispatchWatcher (admin RBAC seed, synced RBAC
// informers, own RBAC publish, SetGlobal) but keeps the dynamic client so the
// test can ask the "apiserver" for the expected answer.
func newIssue624Watcher(t *testing.T) dynamic.Interface {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	seed := append(issue624DispatchSeed(), dispatchAdminRBACSeed()...)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(dispatchTestScheme(), dispatchTestListKinds(), seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: rw=%v err=%v", rw, err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return dyn
}

func issue624Canon(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return out
}

func issue624SortItems(items []any) {
	key := func(v any) string {
		m, _ := v.(map[string]any)
		md, _ := m["metadata"].(map[string]any)
		ns, _ := md["namespace"].(string)
		n, _ := md["name"].(string)
		return ns + "/" + n
	}
	sort.SliceStable(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })
}

func TestIssue624_DispatchRBACList_ServedEqualsApiserver(t *testing.T) {
	dyn := newIssue624Watcher(t)

	type listCase struct {
		gvr  schema.GroupVersionResource
		ns   string
		path string
	}
	rbacGVR := func(res string) schema.GroupVersionResource {
		return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: res}
	}
	const base = "/apis/rbac.authorization.k8s.io/v1"
	cases := []listCase{
		{rbacGVR("roles"), "", base + "/roles"},
		{rbacGVR("roles"), "ns-a", base + "/namespaces/ns-a/roles"},
		{rbacGVR("rolebindings"), "", base + "/rolebindings"},
		{rbacGVR("rolebindings"), "ns-b", base + "/namespaces/ns-b/rolebindings"},
		{rbacGVR("clusterroles"), "", base + "/clusterroles"},
		{rbacGVR("clusterrolebindings"), "", base + "/clusterrolebindings"},
	}

	branches := []struct {
		name string
		ctx  func() context.Context
	}{
		{"decoded", dispatchCtx},
		{"raw", func() context.Context { return cache.WithApistageContentResolve(dispatchCtx()) }},
	}

	for _, c := range cases {
		c := c
		var api *unstructured.UnstructuredList
		var err error
		if c.ns == "" {
			api, err = dyn.Resource(c.gvr).List(context.Background(), metav1.ListOptions{})
		} else {
			api, err = dyn.Resource(c.gvr).Namespace(c.ns).List(context.Background(), metav1.ListOptions{})
		}
		if err != nil {
			t.Fatalf("apiserver list %s: %v", c.path, err)
		}
		if len(api.Items) == 0 {
			t.Fatalf("%s: apiserver returned 0 items — non-vacuity precondition failed", c.path)
		}
		want := make([]any, 0, len(api.Items))
		for i := range api.Items {
			want = append(want, issue624Canon(t, api.Items[i].Object))
		}
		issue624SortItems(want)

		for _, b := range branches {
			b := b
			t.Run(b.name+c.path, func(t *testing.T) {
				raw, served, _ := dispatchViaInformer(b.ctx(), buildCall(http.MethodGet, c.path))
				if !served {
					t.Fatalf("served=false — a synced typed RBAC GVR must be served from the informer")
				}
				var env struct {
					Items []any `json:"items"`
				}
				if err := json.Unmarshal(raw, &env); err != nil {
					t.Fatalf("envelope: %v (%s)", err, raw)
				}
				got := env.Items
				if got == nil {
					got = []any{}
				}
				issue624SortItems(got)
				if !reflect.DeepEqual(got, want) {
					gb, _ := json.Marshal(got)
					wb, _ := json.Marshal(want)
					t.Fatalf("served items != apiserver items\n got (%d): %s\nwant (%d): %s", len(got), gb, len(want), wb)
				}
			})
		}
	}

	// GET-by-name: decoded branch (RBAC get-gate runs, admin passes) and raw.
	for _, b := range branches {
		b := b
		t.Run(b.name+"/get/rolebinding", func(t *testing.T) {
			gvr := rbacGVR("rolebindings")
			wantObj, err := dyn.Resource(gvr).Namespace("ns-a").Get(context.Background(), "grant-ns-a", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("apiserver get: %v", err)
			}
			raw, served, _ := dispatchViaInformer(b.ctx(), buildCall(http.MethodGet, base+"/namespaces/ns-a/rolebindings/grant-ns-a"))
			if !served {
				t.Fatalf("GET served=false on a stored typed object — the GET path drops typed slots")
			}
			var got any
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if w := issue624Canon(t, wantObj.Object); !reflect.DeepEqual(got, w) {
				t.Fatalf("GET served != apiserver\n got: %s\nwant: %v", raw, w)
			}
		})
	}
}
