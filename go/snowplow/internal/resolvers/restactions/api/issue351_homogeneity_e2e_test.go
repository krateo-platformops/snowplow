// issue351_homogeneity_e2e_test.go — #351 CONTENT + informer-coverage arms.
//
// The derive-level arms (issue351_homogeneity_test.go) prove the gate flips
// correctly. These prove the END-TO-END consequence the ruling requires
// (validate_content_not_just_status): a heterogeneous ns-scoped iterator served
// from a WARM collapse cell drops every kind but element-0's, and the collapse
// registers an informer for element-0's GVR only.
//
// WHY THE WARM PHASE: a customer's FIRST /call on a cold cluster-list cell
// returns deny-gate 8 and spawns an async populate — it does NOT collapse, it
// falls back per-element (Path 3.2). So to exercise the collapse SERVE (where the
// drop manifests) the cell must already be warm. warmCollapseIfDerivable mirrors
// the async populate: it derives the GVR and populates the cell — which only
// succeeds PRE-FIX (post-fix deriveTargetGVRForClusterList declines the
// heterogeneous iterator, so there is nothing to warm and Resolve falls back,
// serving every kind). This is what makes the arms able to FAIL:
//
//   A_content: warm + resolve → result must contain BOTH the widget AND the
//     gadget kind. RED pre-fix (warm cell is widgets-only → gadgets dropped).
//   C_informer: with ONLY widgets pre-registered, after warm + resolve the
//     gadgets informer must exist. RED pre-fix (the collapse's 1-element tmp
//     registers widgets only → a gadgets DELETE would never dirty-mark).

package api

import (
	"context"
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

// Two GVRs in ONE group so a single ns-scoped path template collapses by
// interpolating the resource segment (.plural) — the heterogeneous shape.
var (
	gizmoWidgetsGVR = schema.GroupVersionResource{Group: "gizmos.krateo.io", Version: "v1", Resource: "widgets"}
	gizmoGadgetsGVR = schema.GroupVersionResource{Group: "gizmos.krateo.io", Version: "v1", Resource: "gadgets"}
)

const gizmoBroadUser = "gizmo-broad-user"

func gizmoObject(apiVersion, kind, ns, name string) runtime.Object {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion, "kind": kind,
		"metadata": map[string]any{"namespace": ns, "name": name},
	}}
}

// newGizmoWatcher seeds widgets + gadgets in team-a, grants gizmoBroadUser
// cluster-wide `list` on BOTH resources, and syncs an informer for widgets. The
// gadgets informer is pre-synced ONLY when registerGadgets is true (the content
// arm needs it live to serve gadgets in the fallback); the informer arm passes
// false so that the RESOLVE's own lazy registration is what the assertion
// observes.
func newGizmoWatcher(t *testing.T, registerGadgets bool) *cache.ResourceWatcher {
	t.Helper()
	iterFailFastRetries(t) // any stray httpcall 4xx/5xx fails fast, not 5x15s
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	seed := []runtime.Object{
		gizmoObject("gizmos.krateo.io/v1", "Widget", "team-a", "widget-a"),
		gizmoObject("gizmos.krateo.io/v1", "Gadget", "team-a", "gadget-a"),
	}
	cr := &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: "gizmo-lister"},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{gizmoWidgetsGVR.Group},
			Resources: []string{gizmoWidgetsGVR.Resource, gizmoGadgetsGVR.Resource},
			Verbs:     []string{"list"},
		}},
	}
	crb := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "gizmo-broad-binding"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: gizmoBroadUser}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "gizmo-lister"},
	}
	seed = append(seed, cr, crb)

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		gizmoWidgetsGVR: "WidgetList",
		gizmoGadgetsGVR: "GadgetList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)

	toSync := []schema.GroupVersionResource{gizmoWidgetsGVR}
	if registerGadgets {
		toSync = append(toSync, gizmoGadgetsGVR)
	}
	for _, gvr := range toSync {
		added, syncCh := rw.EnsureResourceType(gvr)
		if !added {
			t.Fatalf("EnsureResourceType(%s): want added=true", gvr)
		}
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s informer did not sync within 5s", gvr)
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
	return rw
}

// heteroGizmoStage is a single iterator stage over (plural) in team-a, with an
// ns-scoped LIST path — the collapse-eligible heterogeneous shape.
func heteroGizmoStage() *templates.API {
	return &templates.API{
		Name: "gizmos",
		Path: `${ "/apis/gizmos.krateo.io/v1/namespaces/" + .ns + "/" + .plural }`,
		Verb: ptr.To("GET"),
		DependsOn: &templates.Dependency{
			Iterator: ptr.To(`[{"ns":"team-a","plural":"widgets"},{"ns":"team-a","plural":"gadgets"}]`),
		},
	}
}

func gizmoCtx() context.Context {
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: gizmoBroadUser}),
	)
	return cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
}

// warmCollapseIfDerivable mirrors the async populate goroutine: it derives the
// collapse GVR for the heterogeneous stage and, IF derivation succeeds, fills the
// cluster-scope cell so the next Resolve takes the warm collapse fast-path. Post-
// fix deriveTargetGVRForClusterList declines (heterogeneous), so this is a no-op
// and Resolve falls back per-element — which is exactly the behaviour under test.
func warmCollapseIfDerivable(t *testing.T, ctx context.Context) (warmed bool) {
	t.Helper()
	stage := heteroGizmoStage()
	log := clusterListLogger(t)
	gvr, ok := deriveTargetGVRForClusterList(ctx, log, stage, map[string]any{})
	if !ok {
		return false // post-fix: declined → nothing to warm
	}
	store := cache.ResolvedCache()
	ck := cache.ComputeKey(contentKeyInputs(gvr, "", ""))
	cc := buildClusterListCall(stage, endpoints.Endpoint{}, gvr)
	if !populateClusterListCellSync(ctx, log, stage, gvr, ck, cc, store) {
		t.Fatalf("precondition: could not warm the collapse cell for %s", gvr)
	}
	return true
}

func gizmoResolve(t *testing.T, ctx context.Context) map[string]any {
	t.Helper()
	return Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{heteroGizmoStage()},
		RESTActionNamespace: "default",
		RESTActionName:      "gizmo-hetero-restaction",
	})
}

// collectKinds recursively walks any decoded-JSON value and returns the set of
// every object "kind" string it contains — robust to whatever merge shape the
// iterator aggregation / collapse envelope produces (per-item Widget/Gadget and
// the WidgetList/GadgetList envelope kinds alike).
func collectKinds(v any) map[string]bool {
	out := map[string]bool{}
	var walk func(any)
	walk = func(x any) {
		switch xx := x.(type) {
		case map[string]any:
			if k, ok := xx["kind"].(string); ok {
				out[k] = true
			}
			for _, vv := range xx {
				walk(vv)
			}
		case []any:
			for _, e := range xx {
				walk(e)
			}
		}
	}
	walk(v)
	return out
}

func TestIssue351_A_Content_HeterogeneousIteratorServesEveryKind(t *testing.T) {
	newGizmoWatcher(t, true /*gadgets live so the fallback can serve it*/)
	ctx := gizmoCtx()
	warmCollapseIfDerivable(t, ctx) // pre-fix: warms a widgets-only cell
	dict := gizmoResolve(t, ctx)

	kinds := collectKinds(dict["gizmos"])
	if !kinds["Widget"] && !kinds["WidgetList"] {
		t.Fatalf("#351 content precondition: result is missing the widget kind entirely — got %v", f351Keys(kinds))
	}
	if !kinds["Gadget"] && !kinds["GadgetList"] {
		t.Fatalf("RED #351 CONTENT: the heterogeneous iterator's result is MISSING the gadget kind — only "+
			"element-0's kind (widgets) was served. Pre-fix the collapse derives widgets from element 0, warms a "+
			"widgets-only cluster-scope cell, and serves it — silently dropping gadgets. got kinds %v", f351Keys(kinds))
	}
}

func TestIssue351_C_Informer_ResolveRegistersEveryGVR(t *testing.T) {
	rw := newGizmoWatcher(t, false /*only widgets pre-registered*/)
	ctx := gizmoCtx()
	warmCollapseIfDerivable(t, ctx) // pre-fix: warms the widgets-only collapse cell
	_ = gizmoResolve(t, ctx)

	// After the heterogeneous resolve the gadgets informer MUST exist:
	// EnsureResourceType returns added=false for an already-registered GVR.
	// Pre-fix the warm collapse serves a 1-element tmp (widgets cluster call) so
	// lazyRegisterInnerCallPaths registered widgets only → gadgets added=true →
	// a gadgets DELETE would never dirty-mark. Post-fix the per-element fan-out
	// registers both.
	addedG, _ := rw.EnsureResourceType(gizmoGadgetsGVR)
	if addedG {
		t.Fatalf("RED #351 INFORMER COVERAGE: after the heterogeneous resolve the gadgets GVR still had NO " +
			"informer (EnsureResourceType added=true) — the collapse registered only element-0's GVR (widgets), " +
			"so a gadgets DELETE would never dirty-mark the cell. The per-element fan-out must register every GVR.")
	}
}

func f351Keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
