//go:build falsifier_268_267

// sa_serve_leak_site3_269_test.go — Arm D: the #269 site-3 SA-serve leak. A
// per-user /call resolving a widget apiRef TARGET the caller may not read fetches
// it under the SA *rest.Config (credential A) at objects.getFromAPIServer —
// credential A rides rest.InClusterConfig and is NEVER stale, so this leaks TODAY
// (unlike #268 branch-E, masked by #267).
//
// PM CONDITION 3a (mandatory): the carrier is driven through the REAL widget
// ServeHTTP attach (widgets.go:271-273) → widgets.Resolve → resolveApiRef →
// apiref.Resolve → objects.Get(apiRef target) — NOT helpers.go fetchObject (which
// reads as the user, before the attach → an arm that cannot fail), and NOT a
// hand-installed WithInternalRESTConfig seam. widgetsResolveFn is NOT swapped (the
// real widgets.Resolve runs). BOTH variants are covered: informer-servable-then-
// RBAC-denied (fall-through) AND not-informer-servable (straight to
// getFromAPIServer, no gate at all).
//
// The leak is observed on the WIRE: the fake apiserver records a GET of the denied
// apiRef target under the SA bearer (saRC.BearerToken). objects.Get's result is
// consumed internally by apiref.Resolve (converted + resolved), so the wire dial —
// "the denied target was fetched under credential A" — is the faithful boundary
// proof (the object served under the SA is exactly what #269 reports). RED against
// a6b9d348; GREEN post-Part-1 (no attach → ClientConfigFor returns the per-user
// config → dialed as the user → apiserver 403).
package dispatchers

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// targetRAPath is the apiserver GET-by-name path client-go dynamic dials for the
// denied apiRef target RESTAction.
const targetRAPath = "/apis/templates.krateo.io/v1/namespaces/ns1/restactions/target-ra"

// widgetWithApiRef builds a Panel widget CR whose spec.apiRef points at the
// (RBAC-denied) target RESTAction.
func widgetWithApiRef() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": widgetLeakGVR.Group + "/" + widgetLeakGVR.Version,
		"kind":       "Panel",
		"metadata":   map[string]any{"namespace": "ns1", "name": "leak-widget"},
		"spec": map[string]any{
			"apiRef": map[string]any{"name": "target-ra", "namespace": "ns1"},
		},
	}}
}

// targetRAServedObj is the RESTAction the SA reads at objects.getFromAPIServer.
func targetRAServedObj() servedObj {
	return servedObj{json: true, value: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RestAction",
		"metadata":   map[string]any{"namespace": "ns1", "name": "target-ra"},
		"spec": map[string]any{
			"api":    []any{},
			"filter": `{"leaked":"TARGET-SECRET-269"}`,
		},
	}}
}

// TestLeak269_ArmD_ApiRefTarget_NotServable_FetchedUnderSA — variant 1: the
// restactions GVR is NOT informer-servable (cache.Global()==nil), so objects.Get
// goes STRAIGHT to getFromAPIServer with NO gate at all → ClientConfigFor returns
// the ctx SA *rest.Config → the denied target is fetched under the SA bearer.
func TestLeak269_ArmD_ApiRefTarget_NotServable_FetchedUnderSA(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.SetGlobal(nil) // no watcher → restactions GVR not servable
	t.Cleanup(func() {
		cache.SetGlobal(nil)
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	srv := newLeakServerWithDiscovery(t, map[string]servedObj{targetRAPath: targetRAServedObj()})
	installLeakFetch(t, widgetLeakGVR, widgetWithApiRef())

	rec := httptest.NewRecorder()
	// withUserCfg=true: getFromAPIServer reads xcontext.UserConfig(ctx) before
	// ClientConfigFor; the per-user endpoint is present but the attach's SA rc
	// OVERRIDES it — that override IS the leak.
	req := httptest.NewRequest("GET", "/call", nil).WithContext(leakReqCtx("denied-user", true))
	newWidgetLeakHandler(srv.URL()).ServeHTTP(rec, req)

	wire := srv.sawBearer("GET", "/restactions/target-ra", saLeakToken)
	t.Logf("ARM-D/not-servable code=%d wire_sa_bearer=%v wire:%s", rec.Code, wire, srv.wireDump())

	// FIXED EXPECTATION: a denied apiRef target must NOT be fetched under the SA
	// credential (credential A). RED pre-fix: getFromAPIServer dials it as the SA.
	if wire {
		t.Fatalf("LEAK (#269 site-3, not-servable) — RED: the RBAC-denied apiRef target was fetched under the SA *rest.Config (credential A, never stale — leaks TODAY).\n  wire:%s", srv.wireDump())
	}
	t.Logf("ARM-D/not-servable GREEN: the denied apiRef target was not fetched under the SA identity.")
}

// TestLeak269_ArmD_ApiRefTarget_InformerServableDenied_FetchedUnderSA — variant 2:
// the restactions GVR IS informer-servable and the informer HOLDS the target, but
// the caller is RBAC-DENIED (no binding). objects.Get's informer branch
// (filterGetByRBAC) denies and FALLS THROUGH to getFromAPIServer — which the
// comment (get.go:130-135) promises will yield "the apiserver's authoritative 403
// under the user's own token". The attach breaks that promise: the fall-through
// dials the SA *rest.Config instead, and the denied target is fetched under the SA.
func TestLeak269_ArmD_ApiRefTarget_InformerServableDenied_FetchedUnderSA(t *testing.T) {
	rw := newDeniedRATargetWatcher(t)
	_ = rw

	srv := newLeakServerWithDiscovery(t, map[string]servedObj{targetRAPath: targetRAServedObj()})
	installLeakFetch(t, widgetLeakGVR, widgetWithApiRef())

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(leakReqCtx("denied-user", true))
	newWidgetLeakHandler(srv.URL()).ServeHTTP(rec, req)

	wire := srv.sawBearer("GET", "/restactions/target-ra", saLeakToken)
	t.Logf("ARM-D/informer-servable-denied code=%d wire_sa_bearer=%v wire:%s", rec.Code, wire, srv.wireDump())

	if wire {
		t.Fatalf("LEAK (#269 site-3, informer-servable-denied) — RED: the informer RBAC-deny fell through to getFromAPIServer, which dialed the SA *rest.Config; the promised authoritative per-user 403 never happened.\n  wire:%s", srv.wireDump())
	}
	t.Logf("ARM-D/informer-servable-denied GREEN: the fall-through dialed as the user, not the SA.")
}

// TestLeak269_ArmD_Control_NoAttach — the paired can-fail CONTROL for the #269
// carrier (feedback_arm_that_cannot_fail_is_not_coverage, per-carrier). IDENTICAL
// to the not-servable ArmD but the widget handler is constructed WITHOUT the attach
// (saEP/saRC nil — the post-Part-1 shape). With no SA rest.Config on the ctx,
// cache.ClientConfigFor returns the per-user config → objects.getFromAPIServer
// dials the apiRef target against the per-user endpoint (per-user.invalid), NEVER
// the SA server → no SA bearer on the wire. GREEN now, MUST STAY GREEN — proves the
// #269 arm CAN fail and pinpoints the attach as the sole cause.
func TestLeak269_ArmD_Control_NoAttach(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.SetGlobal(nil) // no watcher → restactions GVR not servable (mirrors the not-servable ArmD)
	t.Cleanup(func() {
		cache.SetGlobal(nil)
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	srv := newLeakServerWithDiscovery(t, map[string]servedObj{targetRAPath: targetRAServedObj()})
	installLeakFetch(t, widgetLeakGVR, widgetWithApiRef())

	// No attach: the handler struct leaves saEP/saRC nil (post-Part-1 shape).
	h := &widgetsHandler{authnNS: "krateo-system"}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(leakReqCtx("denied-user", true))
	h.ServeHTTP(rec, req)

	wire := srv.sawBearer("GET", "/restactions/target-ra", saLeakToken)
	t.Logf("ARM-D/control code=%d wire_sa_bearer=%v wire:%s", rec.Code, wire, srv.wireDump())
	if wire {
		t.Fatalf("ARM-D/control: with NO attach the apiRef target must NOT be fetched under the SA bearer (ClientConfigFor must return the per-user config); wire:%s", srv.wireDump())
	}
	t.Logf("ARM-D/control GREEN: no attach → the apiRef target is dialed as the user, not the SA. The #269 arm can fail (per-carrier can-fail control).")
}

// newDeniedRATargetWatcher stands up a synced cache=on watcher holding the target
// RESTAction (so the informer branch is servable + hits) with an RBAC snapshot
// that grants the caller NOTHING on restactions (so filterGetByRBAC DENIES and the
// branch falls through to getFromAPIServer). Published via
// RebuildRBACSnapshotForTest so the deny is a real published verdict (not the RC1
// unpublished fall-through).
func newDeniedRATargetWatcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	raGVRForList := schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
	target := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RestAction",
		"metadata":   map[string]any{"namespace": "ns1", "name": "target-ra"},
	}}

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		raGVRForList: "RestActionList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:                "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:         "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:         "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	// No ClusterRole/binding grants the caller `get restactions` → denied.
	seed := []runtime.Object{
		target,
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "unrelated"}, Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"}},
		}},
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	added, syncCh := rw.EnsureResourceType(raGVRForList)
	if !added {
		t.Fatalf("EnsureResourceType(restactions): want added=true")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("restactions informer did not sync")
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		cache.SetGlobal(nil)
		cache.PublishRBACSnapshotForTest(nil)
	})
	return rw
}
