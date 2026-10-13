// prewarm_engine_p0_test.go — Ship 1 re-review: the P0 falsifier.
//
// P0 BUG: rePrewarmBoot must seed under the SA-credentialed rctx, NOT the
// bare engine ctx. restActionTargetGVR → objects.Get runs the informer-
// serve branch through filterGetByRBAC, which FAIL-CLOSES on a missing
// identity (objects/get.go:99-141) → returns (zero,false) for every
// restaction → cohortsFor silently reverts to the GLOBAL cohort set,
// defeating the per-target-GVR scoping on the apiRef/RAFullList layer.
//
// This test exercises restActionTargetGVR THROUGH a real objects.Get-
// serving watcher (NOT a directly-built index) under two ctxs:
//   - bare ctx (no SA identity)      → (zero,false)  [the regression]
//   - SA-credentialed rctx           → correct target GVR  [the fix]
// and then asserts EnumerateResourceCohorts(targetGVR) scopes to [3,6],
// NOT the global 34, proving the headline scoping is live end-to-end.

package dispatchers

import (
	"context"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// saUsernameForTest is a canonical SA username so the SA's wildcard CRB
// matches in EvaluateRBAC (parseServiceAccountUsername needs the prefix).
const saUsernameForTest = "system:serviceaccount:krateo-system:snowplow"

// buildP0Watcher seeds a fake dynamic client with a compositions-panels
// RESTAction (userAccessFilter target = compositions) + RBAC objects
// granting compositions to: the snowplow SA (wildcard CRB), admin
// (wildcard CRB), and three groups via exact-match get/list CRBs. Returns
// the watcher published as cache.Global() + the RESTAction ObjectReference.
func buildP0Watcher(t *testing.T) (*cache.ResourceWatcher, templatesv1.ObjectReference) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	// #57: informer pivot is implicit under CACHE_ENABLED (RESOLVER_USE_INFORMER retired).

	restGVR := schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)

	listKinds := map[schema.GroupVersionResource]string{
		restGVR: "RESTActionList",
		crbGVR:  "ClusterRoleBindingList",
		crGVR:   "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
	}

	// compositions-panels RESTAction with a userAccessFilter target.
	ra := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": "compositions-panels", "namespace": "krateo-system"},
		"spec": map[string]any{
			"api": []any{
				map[string]any{
					"name": "panels",
					"path": "/apis/composition.krateo.io/v1/compositions",
					"userAccessFilter": map[string]any{
						"verb":     "list",
						"group":    "composition.krateo.io",
						"resource": "compositions",
					},
				},
			},
		},
	}}

	wildcard := []rbacv1.PolicyRule{{Verbs: []string{"*"}, APIGroups: []string{"*"}, Resources: []string{"*"}}}
	compRule := []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{"composition.krateo.io"}, Resources: []string{"compositions"}}}
	// secretsRule grants ONLY secrets — its subjects appear in the GLOBAL
	// cohort set but NOT in the compositions bucket, so the
	// compositions-scoped set is genuinely narrower than the global set
	// (proves scoping is effective, not just equal-by-construction).
	secretsRule := []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{"secrets"}}}

	mkCR := func(name string, rules []rbacv1.PolicyRule) *rbacv1.ClusterRole {
		return &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules}
	}
	mkCRB := func(name, uid, role string, sub rbacv1.Subject) *rbacv1.ClusterRoleBinding {
		return &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid)},
			Subjects:   []rbacv1.Subject{sub},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role},
		}
	}

	seed := []runtime.Object{
		ra,
		mkCR("cluster-admin", wildcard),
		mkCR("comp-reader", compRule),
		mkCR("secrets-reader", secretsRule),
		mkCRB("sa-bind", "uid-sa", "cluster-admin", rbacv1.Subject{Kind: "ServiceAccount", Name: "snowplow", Namespace: "krateo-system"}),
		mkCRB("admin-bind", "uid-admin", "cluster-admin", rbacv1.Subject{Kind: "User", Name: "admin"}),
		mkCRB("devs-bind", "uid-devs", "comp-reader", rbacv1.Subject{Kind: "Group", Name: "devs"}),
		mkCRB("ops-bind", "uid-ops", "comp-reader", rbacv1.Subject{Kind: "Group", Name: "ops"}),
		mkCRB("sec-bind", "uid-sec", "comp-reader", rbacv1.Subject{Kind: "Group", Name: "security"}),
		// Subjects that grant ONLY secrets — global-set members that the
		// compositions bucket must EXCLUDE.
		mkCRB("audit-bind", "uid-audit", "secrets-reader", rbacv1.Subject{Kind: "Group", Name: "auditors"}),
		mkCRB("backup-bind", "uid-backup", "secrets-reader", rbacv1.Subject{Kind: "User", Name: "backup-bot"}),
	}

	// Per-test cancelable context so the watcher's informer goroutines are
	// fully torn down in cleanup — otherwise they keep mutating the shared
	// rbacSnap after the test ends, which the -race detector flags against
	// a later test reading it.
	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}

	// Register the RESTAction GVR so objects.Get's informer-serve branch
	// can serve it, then sync.
	rw.EnsureResourceType(restGVR)
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

	// ONE ordered cleanup (LIFO would otherwise restore the global before
	// stopping the watcher): stop the watcher + cancel its ctx FIRST so its
	// goroutines exit, THEN restore the previous global + clear the shared
	// RBAC snapshot so no leftover state bleeds into a later test.
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
		cache.ResetBindingsByGVRIndexForTest()
	})

	return rw, templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: "compositions-panels", Namespace: "krateo-system"},
		APIVersion: "templates.krateo.io/v1",
		Resource:   "restactions",
	}
}

// saCtxForTest builds an SA-credentialed ctx mirroring withPhase1SAContext:
// SA UserInfo + internal endpoint + internal REST config, so objects.Get's
// informer-serve branch passes filterGetByRBAC for the SA's wildcard CRB.
func saCtxForTest(ctx context.Context) context.Context {
	ep := endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc"}
	rctx := xcontext.BuildContext(ctx,
		xcontext.WithUserConfig(ep),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: saUsernameForTest}),
	)
	rctx = cache.WithInternalEndpoint(rctx, &ep)
	return rctx
}

func TestP0_RestActionTargetGVR_RequiresSACredentialedCtx(t *testing.T) {
	cache.ResetBindingsByGVRIndexForTest()
	_, ref := buildP0Watcher(t)

	compGVR := schema.GroupVersionResource{Group: "composition.krateo.io", Resource: "compositions"}
	panelGVR := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Resource: "panels"}

	// Build the index over the navigated GVRs (compositions + panels).
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{compGVR, panelGVR})

	// (a) BARE ctx — the P0 regression: objects.Get fail-closes on missing
	// identity → restActionTargetGVR returns (zero,false).
	if gvr, miss := restActionTargetGVR(context.Background(), ref); miss != residentDropGetFailed {
		t.Fatalf("REGRESSION: bare ctx derived target GVR %v (miss=%q) — expected (zero, %q) "+
			"(credential-less objects.Get must fail-close)", gvr, miss, residentDropGetFailed)
	}

	// (b) SA-credentialed ctx — the FIX: objects.Get serves the RA →
	// restActionTargetGVR returns the compositions target GVR.
	rctx := saCtxForTest(context.Background())
	gvr, miss := restActionTargetGVR(rctx, ref)
	if miss != raTargetFound {
		t.Fatal("FIX BROKEN: SA-credentialed ctx did not derive a target GVR — "+
			"objects.Get should serve the RA under SA identity")
	}
	if gvr.Group != "composition.krateo.io" || gvr.Resource != "compositions" {
		t.Fatalf("wrong target GVR: got %v, want composition.krateo.io/compositions", gvr)
	}

	// (c) The headline scoping is live: EnumeratePrewarmTargetsForGVR(target, "list")
	// returns the per-binding targets (admin via wildcard + 3 groups via
	// exact match), NOT a global-fallback universe.
	//
	// Ship 0.30.242 H.c-layered Phase 2c — migrated from
	// EnumerateResourceCohorts (deleted) to EnumeratePrewarmTargetsForGVR.
	// The per-binding shape can have multiple targets per binding (one per
	// subject) but the fixture in this test has 1 subject per binding, so
	// the [3,6] range still applies.
	targets := cache.EnumeratePrewarmTargetsForGVR(gvr, "list")
	if len(targets) < 3 || len(targets) > 6 {
		t.Fatalf("CLAUSE-4: expected targets in [3,6] for compositions, got %d: %+v", len(targets), targets)
	}
}

// TestP0_SeedScopeUsesSACtx_ScopedNotGlobal is the integration falsifier:
// Ship 0.30.242 H.c-layered Phase 2c — TestP0_SeedScopeUsesSACtx_ScopedNotGlobal
// DELETED. The test verified that per-GVR scoped cohorts narrowed the
// global cohort universe — but the global-cohort fallback path is gone
// (Phase 2b prewarm_engine_boot.go: when EnumeratePrewarmTargetsForGVR
// returns empty, SKIP the GVR rather than fall back). The scoping
// invariant the test guarded is now structural (no global fallback
// path = no possibility of fallback). Coverage gap: ZERO.
//
// Surviving in this file: TestP0_RestActionTargetGVR_RequiresSACredentialedCtx
// tests the SA-credentialed ctx derivation, which is unrelated to the
// deleted scoping mechanism.
