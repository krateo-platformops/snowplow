// apistage_heal_retouch_falsifier_test.go — Fix #1 (1a) apistage
// content-HIT re-touch falsifier + OOM guard
// (docs/rca-stale-delete-compositiondefinitions-informer-2026-06-25.md).
//
// 1a adds, on the apistage content-HIT path for a LIST (name==""), an
// idempotent EnsureResourceType + a not-servable-gated scoped
// ConfirmResourceType so a registered-but-unconfirmed GVR served only from
// the content cache stops being permanently shielded from the
// informer-confirm path. The OOM guard (PM AC-3/AC-4): 1a acts ONLY on the
// LIST branch (name==""); the GET-by-name branch (the 1.5.1 boot-OOM
// child-resource fan-out) MUST NOT force any informer.
//
//	F-retouch-LIST — a LIST content HIT for a NOT-yet-registered GVR leaves
//	                 the GVR REGISTERED afterwards (1a's EnsureResourceType
//	                 fired). This is the heal re-touch.
//
//	F-retouch-GET (OOM guard) — a GET-by-name content HIT for the SAME
//	                 NOT-registered GVR leaves it UNREGISTERED (no informer
//	                 forced). This proves 1a is LIST-only and the GET-by-name
//	                 child fan-out is never informer-backed — the structural
//	                 guarantee against the boot-OOM regression.
//
// Per feedback_no_special_cases.md the GVR is a generic customer-style child
// GVR (services), not a compositiondefinitions/group literal. The watcher
// here is built locally (NOT newF1Watcher) so the services List-kind is
// registered — otherwise 1a's real registration would make the fake
// reflector panic on a LIST of an unregistered List-kind, masking the
// assertion.

package api

import (
	"context"
	"net/http"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// healChildGVR is a GET-by-name-style child GVR that the heal watcher does
// NOT pre-register — exactly the shape of the boot-OOM child fan-out.
var healChildGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}

// newHealRetouchWatcher builds a cache=on watcher that registers the
// services + RBAC List-kinds (so a post-1a `services` informer syncs over
// an empty fake without a reflector panic) and grants f1BroadUser get+list
// on services in team-a. It deliberately does NOT EnsureResourceType the
// services GVR — that is the precondition under test (1a must register it
// on a LIST content HIT, and must NOT on a GET-by-name HIT).
func newHealRetouchWatcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	// Grant f1BroadUser get+list on services in team-a so the content HIT
	// serves cleanly (the registration assertion is independent of the
	// serve verdict, but a clean serve keeps the test honest).
	svcRole := &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: "heal-svc-reader"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"services"}, Verbs: []string{"get", "list"}},
		},
	}
	svcBinding := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "heal-svc-binding"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: f1BroadUser}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "heal-svc-reader"},
	}

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		healChildGVR: "ServiceList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, svcRole, svcBinding)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher under CACHE_ENABLED=true")
	}
	t.Cleanup(rw.Stop)

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

// putHealContentEntry directly Puts a content entry under the apistage
// content key for (gvr, ns, name) — simulating an entry stored while the
// GVR was not-servable, the state 1a must re-touch on the next HIT.
func putHealContentEntry(store *cache.ResolvedCacheStore, gvr schema.GroupVersionResource, ns, name string) string {
	key := cache.ComputeKey(contentKeyInputs(gvr, ns, name))
	store.Put(key, &cache.ResolvedEntry{
		RawJSON: []byte(`{"apiVersion":"v1","kind":"List","items":[]}`),
	})
	return key
}

// TestFalsifierRetouchLIST_RegistersInformerOnContentHit proves 1a's heal
// re-touch: a LIST content HIT for a not-yet-registered GVR leaves it
// registered afterwards.
func TestFalsifierRetouchLIST_RegistersInformerOnContentHit(t *testing.T) {
	rw := newHealRetouchWatcher(t)
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil")
	}

	// Precondition: the child GVR is NOT registered.
	if rw.IsRegistered(healChildGVR) {
		t.Fatalf("precondition: %s must NOT be pre-registered", healChildGVR)
	}

	// Seed a LIST content entry (name==""), then HIT it.
	putHealContentEntry(store, healChildGVR, "team-a", "")
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: f1BroadUser}),
	)
	call := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: "/api/v1/namespaces/team-a/services",
			Verb: ptr.To(http.MethodGet),
		},
	}
	_, _, ok := apistageContentServe(ctx, store, call, false)
	if !ok {
		t.Fatalf("apistageContentServe ok=false on the seeded LIST content HIT")
	}

	// POST: 1a's EnsureResourceType fired on the LIST HIT → GVR registered.
	if !rw.IsRegistered(healChildGVR) {
		t.Fatalf("F-retouch-LIST: a LIST content HIT did NOT re-touch the informer — "+
			"%s is still unregistered. 1a's EnsureResourceType-on-LIST-HIT did not fire.",
			healChildGVR)
	}
}

// TestFalsifierRetouchGET_DoesNotRegisterInformer is the OOM guard: a
// GET-by-name content HIT for a not-registered GVR leaves it UNregistered.
// This proves 1a is LIST-only and the GET-by-name child-resource fan-out
// (the 1.5.1 boot-OOM path) never forces an informer.
func TestFalsifierRetouchGET_DoesNotRegisterInformer(t *testing.T) {
	rw := newHealRetouchWatcher(t)
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil")
	}

	if rw.IsRegistered(healChildGVR) {
		t.Fatalf("precondition: %s must NOT be pre-registered", healChildGVR)
	}
	// Baseline informer count — the OOM-guard quantity. The 1.5.1 boot-OOM
	// was unbounded informer population; the in-process proof is that the
	// GET-by-name HIT adds ZERO informers (count delta == 0).
	countBefore := len(rw.RegisteredGVRs())

	// Seed a GET-by-name content entry (name != ""), then HIT it.
	putHealContentEntry(store, healChildGVR, "team-a", "svc-1")
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: f1BroadUser}),
	)
	call := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: "/api/v1/namespaces/team-a/services/svc-1",
			Verb: ptr.To(http.MethodGet),
		},
	}
	_, _, ok := apistageContentServe(ctx, store, call, false)
	if !ok {
		t.Fatalf("apistageContentServe ok=false on the seeded GET-by-name content HIT")
	}

	// POST (OOM guard): the GET-by-name HIT must NOT have registered the
	// informer — no child informer forced, informer count UNCHANGED.
	if rw.IsRegistered(healChildGVR) {
		t.Fatalf("F-retouch-GET OOM GUARD FAIL: a GET-by-name content HIT REGISTERED an "+
			"informer for %s — 1a leaked onto the GET-by-name child fan-out, "+
			"reintroducing the 1.5.1 boot-OOM. 1a must act ONLY where name==\"\".",
			healChildGVR)
	}
	countAfter := len(rw.RegisteredGVRs())
	if countAfter != countBefore {
		t.Fatalf("F-retouch-GET OOM GUARD FAIL: informer count moved %d -> %d on a "+
			"GET-by-name content HIT — the child fan-out forced informer(s); 1a/1b "+
			"must add ZERO informers on the GET-by-name path.", countBefore, countAfter)
	}
}
