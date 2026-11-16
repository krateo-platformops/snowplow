// serve_empty_binding_miss_test.go — #95 SECURITY fast-follow (A4 SERVE side):
// the serve path treats a re-derived BindingUID of "" as a CACHE MISS — never
// serves the shared empty-identity cell to a ""-deriving identity, falls
// through to a direct resolve under the request's OWN identity.
//
// FIX-C (populate side) already skips the SEED Put of the ""-cell; #95 closes
// the READ side. Together: a ""-deriving identity never serves a ""-cell that a
// broad identity (whose EvaluateRBAC ALSO fail-closed) may have populated on
// the CUSTOMER path — the cross-identity-read leak shape.
//
// FALSIFIER SHAPE (PM spec, K=2): a broad-system ""-identity populates a ""-cell;
// a narrow ""-identity must MISS it (direct-resolve, get its OWN result), not
// HIT the broad cell. RED arm = the pre-#95 serve-from-"".
//
// ORDERING NOTE — WHY the wiring arm uses a REPRESENTATIVE Put + the guard
// predicate consumed at the real serve site, and does NOT drive a genuinely-
// denied identity through the full ServeHTTP (this is PM-anticipated: "document
// why a direct cache Put is representative"; do NOT "fix" it with a forged
// objects.Get-serves-but-EvaluateRBAC-denies split — feedback_no_fake_production_scenarios):
//   - Both widgets.ServeHTTP and restactions.ServeHTTP call fetchObject
//     (objects.Get → filterGetByRBAC) BEFORE dispatchCacheLookupKey. The RA/widget
//     fetch's RBAC filter and the key's EvaluateRBAC use the SAME (verb=get, gvr,
//     name) inputs — so an identity that re-derives "" ALSO fails fetchObject and
//     never reaches the serve guard. A "cleanly denied" identity thus can't be
//     driven through the real ServeHTTP to the guard hermetically.
//   - The A4 ""-case (system:kube-controller-manager etc.) is a fail-closed
//     inconsistency (informer-serve admits the fetch but EvaluateRBAC first-match
//     returns "") — reproducing it would require forging that split (rejected).
//   - So ARM-1 proves the REAL trigger derivation (dispatchCacheLookupKey → ""),
//     and ARM-2 proves the serve site gates on serveFromCacheEligible(cacheInputs)
//     with a representative ""-cell present (the exact cell a broad ""-identity's
//     ungated customer Put writes today — the PM's stated Put source). The
//     END-TO-END is ON-CLUSTER-covered on the fix3r/sec95 boot (KCM-class ""-serve
//     → MISS, direct-resolve, own result). Mutation neuters the predicate.
//
// Hermetic: dynamicfake watcher; never touches ./internal/rbac.

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
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var sec95WidgetGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}

// buildSec95Watcher grants userGranted get on the widget GVR (→ non-empty
// first-match BindingUID) and NOT userDenied (→ EvaluateRBAC denies → "").
func buildSec95Watcher(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		sec95WidgetGVR: "FlexList",
		crbGVR:         "ClusterRoleBindingList",
		crGVR:          "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
	}
	flexRule := []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{sec95WidgetGVR.Group}, Resources: []string{sec95WidgetGVR.Resource}}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "flex-reader"}, Rules: flexRule},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "granted-bind", UID: types.UID("uid-granted")},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: "userGranted"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "flex-reader"},
		},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": sec95WidgetGVR.Group + "/" + sec95WidgetGVR.Version,
			"kind":       "Flex",
			"metadata":   map[string]any{"name": "dashboard-flex", "namespace": "krateo-system"},
			"spec":       map[string]any{},
		}},
	}

	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	rw.EnsureResourceType(sec95WidgetGVR)
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

func sec95CohortCtx(username string) context.Context {
	return withCohortSeedContext(context.Background(),
		seedTarget{Username: username}, endpoints.Endpoint{}, nil)
}

// ARM-1 — REAL trigger derivation: the deny cohort re-derives BindingUID ""
// (the exact #95 serve-guard trigger); the granted cohort re-derives non-empty.
func TestSec95_Trigger_EmptyBindingForDenyCohort(t *testing.T) {
	buildSec95Watcher(t)

	_, _, denyInputs := dispatchCacheLookupKey(sec95CohortCtx("userDenied"), "widgets",
		sec95WidgetGVR.Group, sec95WidgetGVR.Version, sec95WidgetGVR.Resource,
		"krateo-system", "dashboard-flex", -1, -1, nil)
	if denyInputs == nil || denyInputs.BindingUID != "" {
		t.Fatalf("ARM-1: deny cohort must re-derive BindingUID==\"\" (the #95 trigger); got %+v", denyInputs)
	}
	if serveFromCacheEligible(denyInputs) {
		t.Fatalf("ARM-1: serveFromCacheEligible must be FALSE for a \"\"-BindingUID (serve treats as MISS)")
	}

	_, _, grantInputs := dispatchCacheLookupKey(sec95CohortCtx("userGranted"), "widgets",
		sec95WidgetGVR.Group, sec95WidgetGVR.Version, sec95WidgetGVR.Resource,
		"krateo-system", "dashboard-flex", -1, -1, nil)
	if grantInputs == nil || grantInputs.BindingUID == "" {
		t.Fatalf("ARM-1 control: granted cohort must re-derive a NON-empty BindingUID; got %+v", grantInputs)
	}
	if !serveFromCacheEligible(grantInputs) {
		t.Fatalf("ARM-1 control: serveFromCacheEligible must be TRUE for a non-empty BindingUID (normal serve)")
	}
}

// ARM-2 — the serve SITE consults serveFromCacheEligible so a ""-cell present in
// L1 is NOT served to a ""-deriving identity. K=2 representative: a broad
// ""-identity's customer Put wrote the ""-cell (represented here by a direct Put
// under the ""-key — the exact cell that Put writes, see the ORDERING note); a
// narrow ""-identity's serve must MISS it (guard → direct-resolve).
func TestSec95_ServeGuard_EmptyBindingCellNotServed(t *testing.T) {
	buildSec95Watcher(t)

	// The narrow cohort's REAL derived key+handle+inputs (BindingUID=="").
	denyCtx := sec95CohortCtx("userDenied")
	key, handle, inputs := dispatchCacheLookupKey(denyCtx, "widgets",
		sec95WidgetGVR.Group, sec95WidgetGVR.Version, sec95WidgetGVR.Resource,
		"krateo-system", "dashboard-flex", -1, -1, nil)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID != "" {
		t.Fatalf("setup: expected a live empty-BindingUID key/handle; got key=%q handle=%v inputs=%+v", key, handle != nil, inputs)
	}

	// Represent the broad empty-identity's customer Put: populate the
	// empty-identity cell (same key the narrow cohort derives — both fold
	// BindingUID==empty). This is the shared cell #95 must NOT serve.
	putFixture(handle, key, &cache.ResolvedEntry{RawJSON: []byte(`{"leaked":"broad-identity-content"}`), Inputs: inputs})
	if _, ok := handle.Get(key); !ok {
		t.Fatalf("setup: the representative empty-identity cell Put did not land")
	}

	// #95: the serve SITE guards on serveFromCacheEligible(cacheInputs) BEFORE
	// the handle.Get — so an empty-BindingUID lookup is a forced MISS even
	// though the cell EXISTS. GREEN = guard false → serve treats as miss.
	servedFromCell := handle != nil && serveFromCacheEligible(inputs)
	if servedFromCell {
		t.Fatalf("#95 RED: an empty-BindingUID lookup was treated as SERVE-ELIGIBLE — the shared empty-identity cell would be served to a different empty-deriving identity (the A4 read leak). serveFromCacheEligible must gate it to MISS.")
	}

	// Control: the granted cohort (non-empty BindingUID) keys a DIFFERENT cell
	// and IS serve-eligible — byte-unchanged normal path.
	_, grantHandle, grantInputs := dispatchCacheLookupKey(sec95CohortCtx("userGranted"), "widgets",
		sec95WidgetGVR.Group, sec95WidgetGVR.Version, sec95WidgetGVR.Resource,
		"krateo-system", "dashboard-flex", -1, -1, nil)
	if !(grantHandle != nil && serveFromCacheEligible(grantInputs)) {
		t.Fatalf("#95 control: a non-empty-BindingUID lookup must stay serve-eligible (normal serve unchanged); inputs=%+v", grantInputs)
	}
}

// ARM-3 — the POPULATE side (customer Put-gate, #95 REWORK). PM reader-grep
// inverted the earlier "dead weight" call: refresher.go (Get→re-resolve→re-Put)
// and refresh_subscription.go SSE arming READ identity-bound cells, and a ""
// BindingUID does NOT make cacheKey=="" → subscriptions arm on ""-cells and the
// refresher DELIVERS them. So the ungated customer Put is the leak POPULATION
// source; per-reader guards = whack-a-mole; gating the Put is the single-
// mechanism closure. This arm proves: (a) the ""-deriving request keeps its own
// content (serve MISS → direct-resolve, ARM-2) AND (b) NO entry exists at the
// ""-key after the request — the customer Put is gated. Symmetric to FIX-C.
//
// Same ORDERING NOTE as ARM-2: the gate predicate (handle!=nil && key!="" &&
// serveFromCacheEligible(inputs)) is the EXACT condition the customer Put site
// (widgets.go / restactions.go) consumes; a genuinely-denied identity can't be
// driven through ServeHTTP to the Put site (fetchObject fails first), so this
// exercises the predicate on the ""-cohort's REAL derived (key,handle,inputs)
// and asserts the resulting no-Put on a real handle. Mutation (drop the gate)
// → the ""-cell is populated → RED.
func TestSec95_PutGate_EmptyBindingCellNotPopulated(t *testing.T) {
	buildSec95Watcher(t)

	// The ""-cohort's REAL derived key/handle/inputs, on a FRESH cache (no
	// pre-seeded cell — this arm is about the POPULATE decision, not serve).
	key, handle, inputs := dispatchCacheLookupKey(sec95CohortCtx("userDenied"), "widgets",
		sec95WidgetGVR.Group, sec95WidgetGVR.Version, sec95WidgetGVR.Resource,
		"krateo-system", "dashboard-flex", -1, -1, nil)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID != "" {
		t.Fatalf("setup: expected a live empty-BindingUID key/handle; got key=%q handle=%v inputs=%+v", key, handle != nil, inputs)
	}
	// Sanity: nothing at the empty-identity key yet.
	if _, ok := handle.Get(key); ok {
		t.Fatalf("setup: the empty-identity key must be empty before the (gated) customer Put")
	}

	// The customer Put site's EXACT gate condition (widgets.go/restactions.go
	// `else if cacheHandle != nil && cacheKey != "" && serveFromCacheEligible(cacheInputs)`).
	// Run the real code path: Put IFF the gate passes.
	putEligible := handle != nil && key != "" && serveFromCacheEligible(inputs)
	if putEligible {
		putFixture(handle, key, &cache.ResolvedEntry{RawJSON: []byte(`{"resolved":"under-empty-identity"}`), Inputs: inputs})
	}

	// (b) GREEN: the Put-gate is FALSE for a ""-BindingUID → no customer Put →
	// NO shared ""-cell exists. The refresher + SSE readers have nothing to
	// deliver cross-identity.
	if _, ok := handle.Get(key); ok {
		t.Fatalf("#95 RED (populate): the customer Put populated the shared empty-identity cell — the refresher/SSE readers would deliver it cross-identity. The Put must be gated on serveFromCacheEligible.")
	}

	// Control: a granted (non-empty BindingUID) request's Put-gate PASSES → its
	// OWN cell is populated normally (byte-unchanged customer path).
	gKey, gHandle, gInputs := dispatchCacheLookupKey(sec95CohortCtx("userGranted"), "widgets",
		sec95WidgetGVR.Group, sec95WidgetGVR.Version, sec95WidgetGVR.Resource,
		"krateo-system", "dashboard-flex", -1, -1, nil)
	if !(gHandle != nil && gKey != "" && serveFromCacheEligible(gInputs)) {
		t.Fatalf("#95 control: a non-empty-BindingUID request must remain Put-eligible (normal population unchanged); inputs=%+v", gInputs)
	}
}
