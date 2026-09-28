//go:build falsifier_268_267

// sa_serve_refresher_leak_268_test.go — Part 2 (step 4) RED-first arm for the
// BACKGROUND refresher SA-transport path (design §5). Part 1 (step 3) closed the
// LIVE per-user path; the refresher is the ONE remaining SA-credentialed context.
//
// The refresher (resolve_populate.go) legitimately attaches the SA endpoint +
// *rest.Config to its OWN re-resolve ctx (it has no per-user token) AND sets a
// REPRESENTATIVE identity (WithUserInfo{RepresentativeUsername/Groups}). For a
// per-user cohort that representative is a REAL end-user (non-SA) → serveUnnarrowed
// is FALSE. Post-step-2 the SA dial authenticates (fresh token), so the UNGATED
// SITE 3 (credential A) — objects.getFromAPIServer → cache.ClientConfigFor → SA
// *rest.Config → cli.Get — serves tenant data the representative may not read,
// re-populating a per-user L1 cell cross-user.
//
// RED-first: driven through the REAL refresher (resolveAndPopulateL1 →
// resolveOnceProd → objects.Get), NOT a hand-built ctx — the SA-cred +
// representative-identity crossed state arises from the refresher's own code, and
// the Part-2 guard (rbac.MustRegateSADial at getFromAPIServer) is what flips this
// GREEN. RED against the step-3 tree (e6f864c3).
package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// refresherDeniedWatcher publishes a synced cache=on RBAC snapshot that grants the
// representative NOTHING on the refreshed GVR (no binding) — so filterGetByRBAC
// denies it once the Part-2 guard consults the snapshot. Registered but holding no
// restactions, so objects.Get's informer branch misses → falls through to
// getFromAPIServer (the SA dial). Mirrors newDeniedRATargetWatcher.
func refresherDeniedWatcher(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:                "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:         "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:         "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}:               "RestActionList",
	}
	// A ClusterRole granting only configmaps (never restactions) → the
	// representative is denied the refreshed RA.
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "refresher-unrelated"}, Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"}},
		}},
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
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
}

// refresherLeakRAServed is the RESTAction CR the fake apiserver serves (as SA) for
// the refresher's dispatch-CR fetch. Its spec.filter emits a distinctive marker, so
// the SA-served body is observable in the re-Put L1 cell.
const refresherSite3Marker = "REFRESHER-SA-SITE3"

func refresherLeakRAServed() servedObj {
	return servedObj{json: true, value: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RestAction",
		"metadata":   map[string]any{"namespace": "ns1", "name": "leak-ra"},
		"spec":       map[string]any{"api": []any{}, "filter": `{"leaked":"` + refresherSite3Marker + `"}`},
	}}
}

const refresherRAPath = "/apis/templates.krateo.io/v1/namespaces/ns1/restactions/leak-ra"

// TestRefresherLeak_Site3_DispatchCR_ServedUnderSA — the refresher re-resolves a
// restactions-class cell under a representative identity DENIED the RA; the
// dispatch-CR fetch (resolveOnceProd → objects.Get) misses the informer and goes
// through objects.getFromAPIServer under the SA *rest.Config (credential A) → the
// denied RA is served as SA and its resolved body re-populates the cell.
func TestRefresherLeak_Site3_DispatchCR_ServedUnderSA(t *testing.T) {
	refresherDeniedWatcher(t)

	srv := newLeakServerWithDiscovery(t, map[string]servedObj{
		refresherRAPath: refresherLeakRAServed(),
	})

	inputs := cache.ResolvedKeyInputs{
		CacheEntryClass:        "restactions",
		Group:                  "templates.krateo.io",
		Version:                "v1",
		Resource:               "restactions",
		Namespace:              "ns1",
		Name:                   "leak-ra",
		BindingUID:             "uid-refresher",
		RepresentativeUsername: "denied-user",
		RepresentativeGroups:   []string{"tenant-a"},
	}
	key := cache.ComputeKey(inputs)
	store := cache.ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled")
	}
	store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"stale":"prior"}`), Inputs: &inputs})

	saEP := &endpoints.Endpoint{ServerURL: srv.URL(), Token: saLeakToken}
	saRC := saLeakRC(srv.URL())

	err := resolveAndPopulateL1(context.Background(), inputs, saEP, saRC)

	wire := srv.sawBearer("GET", "/restactions/leak-ra", saLeakToken)
	var served bool
	if entry, ok := store.Get(key); ok {
		served = bodyContains(entry.RawJSON, refresherSite3Marker)
	}
	t.Logf("SITE3/refresher err=%v wire_sa_bearer=%v cell_has_sa_body=%v wire:%s", err, wire, served, srv.wireDump())

	// FIXED EXPECTATION: under the refresher ctx (SA cred + representative identity),
	// a denied RA must NOT have its SA-read body re-populate the per-user cell — the
	// Part-2 guard must re-gate getFromAPIServer (filterGetByRBAC(representative) →
	// fail closed). RED pre-fix: the SA-served body lands in the cell.
	if served {
		t.Fatalf("LEAK (#269 Part 2, refresher site 3) — RED: the denied RA was fetched under the SA *rest.Config at getFromAPIServer and its body re-populated the per-user L1 cell (marker %q present).\n  wire:%s", refresherSite3Marker, srv.wireDump())
	}
	t.Logf("SITE3/refresher GREEN: the denied RA's SA-read body did not re-populate the cell (re-gated / fail-closed).")
}

// refresherGrantedRAWatcher publishes a synced watcher that HOLDS the RA (with a
// subresource api step) and GRANTS the representative `get restactions` in ns1 — so
// the refresher's dispatch-CR fetch is legitimately informer-served (isolating the
// leak to the api-step's branch-E SA dial, site 1). The subresource resource
// (pods) is NOT granted, but branch E does not gate it — that is the leak.
func refresherGrantedRAWatcher(t *testing.T, rep string) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	raGVR := schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
	ra := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RestAction",
		"metadata":   map[string]any{"namespace": "ns1", "name": "leak-ra-sub"},
		"spec": map[string]any{
			"api": []any{map[string]any{
				"name":   "podstatus",
				"path":   "/api/v1/namespaces/ns1/pods/p1/status",
				"verb":   "GET",
				"filter": ".podstatus",
			}},
			"filter": ".podstatus",
		},
	}}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		raGVR: "RestActionList",
		{Group: "", Version: "v1", Resource: "pods"}:                                          "PodList", // resolver lazy-registers the inner-call GVR for dep tracking
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:                "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:         "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:         "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	seed := []runtime.Object{
		ra,
		&rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: "ra-getter"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"templates.krateo.io"}, Resources: []string{"restactions"}, Verbs: []string{"get"}}},
		},
		&rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns1", Name: "ra-getter-binding"},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: rep}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "ra-getter"},
		},
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	added, syncCh := rw.EnsureResourceType(raGVR)
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
}

const refresherSite1Marker = "REFRESHER-SA-SITE1"

// TestRefresherLeak_Site1_SubresourceBranchE_ServedUnderSA — the refresher
// re-resolves a restactions cell whose api step reads a pod SUBRESOURCE with no
// endpointRef. The RA CR is legitimately informer-served (representative granted),
// but the subresource step is excluded from branch C (Gate 3) and dials the SA
// ENDPOINT (credential B) at branch E — UNGATED — serving the subresource the
// representative may not read into the per-user cell. RED against step-3.
func TestRefresherLeak_Site1_SubresourceBranchE_ServedUnderSA(t *testing.T) {
	refresherGrantedRAWatcher(t, "granted-rep")

	const path = "/api/v1/namespaces/ns1/pods/p1/status"
	srv := newLeakServerWithDiscovery(t, map[string]servedObj{
		path: {json: true, value: map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"namespace": "ns1", "name": "p1"},
			"status":   map[string]any{"phase": "Running", "secretField": refresherSite1Marker},
		}},
	})

	inputs := cache.ResolvedKeyInputs{
		CacheEntryClass:        "restactions",
		Group:                  "templates.krateo.io",
		Version:                "v1",
		Resource:               "restactions",
		Namespace:              "ns1",
		Name:                   "leak-ra-sub",
		BindingUID:             "uid-refresher-sub",
		RepresentativeUsername: "granted-rep",
		RepresentativeGroups:   []string{"tenant-a"},
	}
	key := cache.ComputeKey(inputs)
	store := cache.ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled")
	}
	store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"stale":"prior"}`), Inputs: &inputs})

	saEP := &endpoints.Endpoint{ServerURL: srv.URL(), Token: saLeakToken}
	saRC := saLeakRC(srv.URL())

	_ = resolveAndPopulateL1(context.Background(), inputs, saEP, saRC)

	// The RED signal for site 1 is the un-gated SA READ itself: branch E dialed the
	// SA ENDPOINT for a resource the representative may not read. (Whether the result
	// is then persisted is masked by the refresher's external-touched Put-gate — but
	// relying on that decline as the boundary is "an accident, not a control": the
	// SA still reads the tenant subresource. The Part-2 guard must re-gate branch E
	// BEFORE the dial so the SA read never happens.)
	wire := srv.sawBearer("GET", "/pods/p1/status", saLeakToken)
	t.Logf("SITE1/refresher wire_sa_bearer=%v wire:%s", wire, srv.wireDump())

	// FIXED EXPECTATION: the SA endpoint must NOT be dialed at branch E for a denied
	// resource under the refresher's representative identity — mustRegateSADial must
	// fail closed before the fetch. RED pre-fix: branch E dials the SA endpoint.
	if wire {
		t.Fatalf("LEAK (#268 Part 2, refresher site 1) — RED: a denied pod subresource was dialed under the SA ENDPOINT at branch E in the refresher's representative-identity re-resolve (un-gated SA read).\n  wire:%s", srv.wireDump())
	}
	t.Logf("SITE1/refresher GREEN: the SA endpoint was not dialed at branch E for the denied subresource (mustRegateSADial fail-closed before the fetch).")
}
