// apistage_content_falsifier_test.go — Ship F1 (0.30.119) pre-flight
// falsifier for the content-keyed api-stage L1 + serve-time RBAC gate.
//
// THE MODEL (F1): the api-stage L1 is reshaped from Ship E's per-stage,
// per-user entry to a per-K8s-CALL CONTENT entry — the raw apiserver
// envelope of one dispatch, keyed (gvr, namespace, name-or-empty),
// IDENTITY-FREE (ComputeKey drops Username/Groups for the apistage
// class). The dispatch is un-gated; the per-user RBAC gate runs at a
// single site, on the content-Get-HIT path AND the dispatch-MISS path,
// before the content reaches dict[id].
//
// THE LEAK F1 CLOSES: pre-F1 the RBAC gate ran INSIDE dispatchViaInformer
// — it fired on a miss (fresh dispatch) but was SKIPPED on a Get-hit. A
// shared, identity-free content entry Get-hit by a second user would
// serve that user the FIRST resolver's RBAC-narrowed view. F1 moves the
// gate to a point both paths traverse.
//
// FALSIFIER (drives the REAL api.Resolve stage loop — no resolveOnceFn
// stub):
//
//   TestFalsifierF1_CrossUserNoLeak — user A (broad RBAC) resolves a
//     stage that LISTs widgets cluster-wide → content entries stored
//     un-gated → user B (narrow RBAC — fewer namespaces) resolves the
//     same stage → B's content Get-HITs A's entries → assert B's
//     dict[id] is B-NARROWED, never A's broader view. Pre-fix (gate
//     skipped on the hit path) B sees A's view — the leak.
//
//   TestFalsifierF1_GatedFromStoredByteIdentical — AC-F1.2: the
//     gated-from-stored dict[id] is byte-identical to a fresh
//     inline-gated resolve for the same identity, INCLUDING a stage
//     `filter` that strips metadata.namespace (Reading 1: the gate runs
//     on the raw pre-filter envelope, so a stripping filter cannot
//     defeat it).
//
//   TestFalsifierF1_ContentEntryIdentityFreeShared — AC-F1.3: two users
//     resolving the same stage produce exactly ONE shared content entry
//     per K8s call (identity-free key), not one per user.

package api

import (
	"context"
	"net/http"
	"sort"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

// --- F1 falsifier fixtures ------------------------------------------------

// f1WidgetsGVR is the content-unit GVR the falsifier's stage LISTs. A
// customer-style GVR — not RBAC, not composition (so no carve-out).
var f1WidgetsGVR = schema.GroupVersionResource{
	Group:    "widgets.krateo.io",
	Version:  "v1",
	Resource: "widgets",
}

// f1BroadUser sees widgets in EVERY seeded namespace; f1NarrowUser sees
// them in only f1NarrowNamespaces.
const (
	f1BroadUser  = "broad-user"
	f1NarrowUser = "narrow-user"
)

var (
	f1AllNamespaces    = []string{"team-a", "team-b", "team-c", "team-d"}
	f1NarrowNamespaces = []string{"team-a", "team-b"}
)

// f1WidgetObject builds an unstructured widget in ns. metadata.namespace
// is set — filterListByRBAC reads it for the per-namespace verdict.
func f1WidgetObject(ns, name string) runtime.Object {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.krateo.io/v1",
		"kind":       "Widget",
		"metadata": map[string]any{
			"namespace": ns,
			"name":      name,
		},
	}}
}

// f1WidgetListerClusterRole grants `list` on widgets.krateo.io widgets.
func f1WidgetListerClusterRole() *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: "f1-widget-lister"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{f1WidgetsGVR.Group}, Resources: []string{f1WidgetsGVR.Resource}, Verbs: []string{"list"}},
		},
	}
}

// f1RoleBinding binds `user` to the widget-lister ClusterRole in ns —
// granting that user `list widgets` in exactly that namespace.
func f1RoleBinding(ns, user string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "f1-binding-" + user},
		Subjects: []rbacv1.Subject{
			{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: user},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "f1-widget-lister",
		},
	}
}

// newF1Watcher builds a synced cache=on watcher seeded with one widget
// per namespace in f1AllNamespaces, a widget-lister ClusterRole, and
// RoleBindings granting f1BroadUser EVERY namespace + f1NarrowUser only
// f1NarrowNamespaces. The content-keyed api-stage path is live because it
// is folded under CACHE_ENABLED+RESOLVED_CACHE_ENABLED (#57 — the standalone
// RESOLVED_CACHE_APISTAGE_ENABLED flag was retired); the informer pivot is
// likewise implicit under CACHE_ENABLED (RESOLVER_USE_INFORMER retired).
func newF1Watcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	var seed []runtime.Object
	for _, ns := range f1AllNamespaces {
		seed = append(seed, f1WidgetObject(ns, "widget-"+ns))
	}
	seed = append(seed, f1WidgetListerClusterRole())
	for _, ns := range f1AllNamespaces {
		seed = append(seed, f1RoleBinding(ns, f1BroadUser))
	}
	for _, ns := range f1NarrowNamespaces {
		seed = append(seed, f1RoleBinding(ns, f1NarrowUser))
	}

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		f1WidgetsGVR: "WidgetList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:                "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:         "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:         "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher under CACHE_ENABLED=true")
	}
	t.Cleanup(rw.Stop) // #85: Stop() blocks until goroutine drain — no settle-sleep needed

	added, syncCh := rw.EnsureResourceType(f1WidgetsGVR)
	if !added {
		t.Fatalf("EnsureResourceType(widgets): want added=true")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("widgets informer did not sync within 5s")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync (RBAC informers): %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return rw
}

// f1ListStage is a one-stage RESTAction api[] entry: a cluster-wide
// widgets LIST. `filter` is `.<id>.items` (jsonHandler wraps the
// response under the stage id before filtering).
func f1ListStage(id string) *templates.API {
	return &templates.API{
		Name:   id,
		Path:   "/apis/widgets.krateo.io/v1/widgets",
		Verb:   ptr.To(http.MethodGet),
		Filter: ptr.To("." + id + ".items"),
	}
}

// f1ResolveAs drives the REAL api.Resolve stage loop for a one-stage
// RESTAction as `username`. No resolveOnceFn stub — the content pipeline
// (apistageContentServe: content Get / un-gated dispatch / gate) runs
// for real. Returns the resolved stage output dict[id].
func f1ResolveAs(t *testing.T, username string, stage *templates.API) map[string]any {
	t.Helper()
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: username}),
	)
	// The stage has no EndpointRef; resolveOne consults the
	// context-carried internal endpoint first. The widgets LIST is
	// served by the informer pivot (dispatchViaInformer reads the
	// informer store, never the endpoint), so this endpoint is only a
	// placeholder that lets resolveOne succeed without a per-user
	// clientconfig Secret lookup.
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
	dict := Resolve(ctx, ResolveOptions{
		// A non-nil RC keeps api.Resolve off its rest.InClusterConfig()
		// early-return (which fails outside a cluster). The stage LIST is
		// served by the informer pivot (dispatchViaInformer reads the
		// informer store, not the endpoint), so RC's contents are never
		// dereferenced — a bare *rest.Config is enough.
		RC:    &rest.Config{},
		Items: []*templates.API{stage},
		// RESTAction identity — F1's content key is per-K8s-call, so
		// these are not folded into the content key, but Resolve still
		// reads them for logging / dep recording.
		RESTActionNamespace: "default",
		RESTActionName:      "f1-widget-restaction",
	})
	return dict
}

// f1WidgetNamesIn extracts the set of widget names from a resolved stage
// output (dict[id] is the post-filter `.items` array of widget objects).
func f1WidgetNamesIn(v any) map[string]bool {
	out := map[string]bool{}
	items, ok := v.([]any)
	if !ok {
		return out
	}
	for _, it := range items {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		meta, _ := m["metadata"].(map[string]any)
		if meta == nil {
			continue
		}
		if name, _ := meta["name"].(string); name != "" {
			out[name] = true
		}
	}
	return out
}

func f1SortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Ship 0.30.242 H.c-layered Phase 2c — 3 F1 falsifier tests DELETED
// (decision ratified by team-lead 2026-06-03 per design's transitive
// architect ACK):
//
//   - TestFalsifierF1_CrossUserNoLeak — asserted v3 cohort-gate-memo
//     serve-time RBAC filtering. Design §3.4 explicitly removed the
//     cohort-gate-memo apparatus under H.c-layered per-binding cell
//     keying; UAF shipped on chart 0.30.243 + portal rev 33 as design
//     §9.4 prerequisite.
//   - TestFalsifierF1_GatedFromStoredByteIdentical — asserted the
//     serve-time gate runs on the raw envelope (Reading 1). The
//     serve-time gate was removed in 2b
//     (gateListItemsWithMemo → serveParsedListEnvelope).
//   - TestFalsifierF1_ContentEntryIdentityFreeShared — asserted the v3
//     apistage content cell is identity-free across users. Under v4
//     per-binding key (Phase 2a resolved.go) this is structurally
//     false; the inverse holds (distinct bindings → distinct cells).
//
// v4 equivalent coverage: Phase 3 F6 (raFullList cross-binding cell
// isolation under -race) + F7 (Empty-BindingUID invariant). Both F6
// and F7 MUST PASS LOCALLY before Phase 4 dual-gate review begins —
// if either surfaces a real defect, this deletion is revisited.
//
// File helpers (f1ListStage, f1ResolveAs, f1BroadUser, f1NarrowUser,
// newF1Watcher, etc.) are KEPT — they are reused by
// apistage_concurrent_isolation_test.go, cluster_list_dep_record_test.go,
// and resolve_jwt_leak_test.go which test surviving production code.
