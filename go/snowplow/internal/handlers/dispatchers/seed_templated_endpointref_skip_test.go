// seed_templated_endpointref_skip_test.go — M15 BEHAVIORAL arm for the #113
// seed-skip: the REAL seedOneRestaction, driven end-to-end (fetch → convert →
// #113 templated-endpointRef skip → resolve+Put tail), must NOT reach the Put
// for a templated-endpointRef RA, and MUST reach it for a static one.
//
// WHY A BEHAVIORAL ARM (vs the pure predicate table in
// phase1_seed_templated_endpointref_test.go): the predicate table proves
// hasTemplatedEndpointRef in isolation; the §4 decline-gate arm proves the
// GTTL-1 gate can't catch this miss class. Neither proves the SKIP is actually
// wired to short-circuit the Put inside the real function. The
// seed_resolves_counter_test.go header documents that reaching seedOneRestaction's
// Put hermetically was "NOT possible" because restactions.Resolve dials the
// apiserver. Two minimal, behaviour-preserving var-seams close that gap:
//
//   - seedObjectsGetFn — the RESTAction-fetch boundary (default objects.Get).
//     Injects a templated- vs static-endpointRef RESTAction unstructured so the
//     REAL FromUnstructured conversion + REAL hasTemplatedEndpointRefFn run over
//     it — no apiserver.
//   - seedRestactionResolveAndPutFn — the resolve+encode+GTTL-gate+Put TAIL
//     (default seedRestactionResolveAndPutProd), replaced by a recorder so we
//     observe whether the seed REACHED the Put without a live resolver/encoder.
//
// The #113 skip lives in seedOneRestaction BETWEEN the two seams, so this
// exercises the real skip. A granted RBAC snapshot (dynamicfake) is published so
// the granted cohort re-derives a NON-empty BindingUID and clears the earlier
// FIX-C empty-binding guard, letting execution reach the #113 skip.
//
// RED (proven by transiently neutering the REAL predicate seam
// hasTemplatedEndpointRefFn to always-false): a templated RA then FAILS to skip
// and reaches the Put tail — the exact #113 §4 poisoning (a truncated no-extras
// body Put under the serve key). Restore → GREEN. Hermetic, -race, no cluster,
// never touches ./internal/rbac.

package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// buildGrantedRestactionWatcher publishes an RBAC snapshot granting `user`
// get/list on the RESTAction GVR so dispatchCacheLookupKey("restactions", …)
// re-derives a NON-empty first-match BindingUID (clearing the FIX-C empty-
// binding guard that precedes the #113 skip). Mirrors buildFixCWatcher but on
// restActionGVR. The RA CR itself is NOT seeded into the fake client — the fetch
// is seamed via seedObjectsGetFn, so only the grant matters here.
func buildGrantedRestactionWatcher(t *testing.T, user string) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	raGVR := restActionGVR // restactions.templates.krateo.io/v1
	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		raGVR:  "RESTActionList",
		crbGVR: "ClusterRoleBindingList",
		crGVR:  "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
	}
	raRule := []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{raGVR.Group}, Resources: []string{raGVR.Resource}}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "ra-reader"}, Rules: raRule},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "granted-ra-bind", UID: types.UID("uid-ra-granted")},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: user}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "ra-reader"},
		},
	}

	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	rw.EnsureResourceType(raGVR)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
}

// fetchedRestaction builds the objects.Result seedObjectsGetFn returns for a
// RESTAction whose sole api-step's endpointRef.name is either templated
// (${...}-shaped → hasTemplatedEndpointRef true) or a static literal. The
// unstructured is what the REAL FromUnstructured conversion in seedOneRestaction
// converts into templatesv1.RESTAction before the #113 predicate runs.
func fetchedRestaction(templated bool) objects.Result {
	epName := "spoke-a-endpoint" // static literal
	if templated {
		epName = `${ .name + "-endpoint" }` // request-extras-driven endpoint selection
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": restActionGVR.Group + "/" + restActionGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": "hub-spoke-ra", "namespace": "krateo-system"},
		"spec": map[string]any{
			"api": []any{
				map[string]any{
					"name": "s1",
					"endpointRef": map[string]any{
						"name":      epName,
						"namespace": "krateo-system",
					},
				},
			},
		},
	}}
	return objects.Result{
		GVR:          restActionGVR,
		Unstructured: u,
	}
}

// seedCohortCtx installs the granted cohort's identity so dispatchCacheLookupKey
// re-derives the non-empty BindingUID. saEP/saRC inert — the resolve+Put tail is
// seamed away, so no transport is used.
func seedCohortCtx(user string) context.Context {
	return withCohortSeedContext(context.Background(),
		seedTarget{Username: user}, endpoints.Endpoint{}, nil)
}

// TestM15_SeedOneRestaction_SkipsTemplated_PutsStatic — the M15 behavioral
// falsifier. Drives the REAL seedOneRestaction with the fetch + resolve/Put tail
// seamed; asserts the templated RA NEVER reaches the Put tail and the static RA
// DOES. RED arm neuters the real hasTemplatedEndpointRefFn predicate → the
// templated RA then reaches the Put (the #113 §4 truncated-body poisoning).
func TestM15_SeedOneRestaction_SkipsTemplated_PutsStatic(t *testing.T) {
	const user = "userGranted"
	buildGrantedRestactionWatcher(t, user)

	ref := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: "hub-spoke-ra", Namespace: "krateo-system"},
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}

	// Observe whether the resolve+Put TAIL was reached (a proxy for "the Put
	// happened"). The recorder returns nil so seedOneRestaction completes.
	var putReached bool
	origPut := seedRestactionResolveAndPutFn
	origGet := seedObjectsGetFn
	origPred := hasTemplatedEndpointRefFn
	t.Cleanup(func() {
		seedRestactionResolveAndPutFn = origPut
		seedObjectsGetFn = origGet
		hasTemplatedEndpointRefFn = origPred
	})
	seedRestactionResolveAndPutFn = func(
		_, _ context.Context, _ *templatesv1.RESTAction, _ templatesv1.ObjectReference,
		_, _ string, _ cacheHandle, _ *cache.ResolvedKeyInputs, _ objects.Result,
		_ *cache.StageErrorSink, _ *cache.ExternalTouchedSink,
	) error {
		putReached = true
		return nil
	}

	run := func(t *testing.T, templated bool) bool {
		t.Helper()
		putReached = false
		seedObjectsGetFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
			return fetchedRestaction(templated)
		}
		if err := seedOneRestaction(seedCohortCtx(user), ref, "krateo-system", seedModeBoot); err != nil {
			t.Fatalf("seedOneRestaction returned %v; want nil (skip and success paths both return nil)", err)
		}
		return putReached
	}

	// --- GREEN: templated → SKIP (Put tail NOT reached). ---
	hasTemplatedEndpointRefFn = origPred // real predicate
	if run(t, true) {
		t.Fatal("M15: seedOneRestaction REACHED the Put tail for a TEMPLATED-endpointRef RA — " +
			"the #113 skip must short-circuit before resolve+Put (else a truncated no-extras body is Put " +
			"under the serve key, the §4 poisoning).")
	}

	// --- GREEN control: static → PROCEED (Put tail reached). ---
	if !run(t, false) {
		t.Fatal("M15 control: seedOneRestaction did NOT reach the Put tail for a STATIC-endpointRef RA — " +
			"a static RA must be seeded (else the skip is over-broad and GREEN above is a no-op that never Puts anything).")
	}

	// --- RED: neuter the REAL predicate → templated RA fails to skip → Put reached. ---
	hasTemplatedEndpointRefFn = func(*templatesv1.RESTAction) bool { return false }
	if !run(t, true) {
		t.Fatal("M15 RED expected the Put tail to be REACHED once hasTemplatedEndpointRefFn is neutered to " +
			"always-false — if the templated RA STILL skipped, the skip is driven by something other than the " +
			"predicate and the GREEN arm is not discriminating.")
	}
}
