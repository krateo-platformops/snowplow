// f6_cross_binding_isolation_test.go — Ship 0.30.242 H.c-layered Phase 3
// F6 (HARD GATE per the F1-deletion decision at 2c commit 74d5090).
//
// THE INVARIANT (design §1.2 + §3.3 — per-binding cell sharing)
//
// Two distinct first-match bindings produce two distinct BindingUIDs;
// the raFullList L1 cells those bindings authorise are KEY-DISTINCT
// and CONTENT-INDEPENDENT. A reader whose first-match is binding-B
// cannot, by any concurrent ordering of populate/read, observe the
// content of a cell populated under binding-A.
//
// This is the v4 STRUCTURAL replacement for the deleted v3 F1
// falsifier (TestFalsifierF1_CrossUserNoLeak) which asserted the
// serve-time cohort-gate-memo's defense-in-depth. Under H.c-layered
// the cell key itself prevents the leak — no serve-time filtering
// needed because two different bindings simply land on two different
// cells.
//
// KEY vs BYTE — what F6 tests (per Diego's 2c note #5)
//
// F6 tests KEY isolation, NOT byte-content-dedup. If the cache were to
// share underlying byte slices between cells with identical content
// (a value-dedup layer per feedback_l1_per_user_keyed_never_cohort),
// that would be CORRECT behavior — the cells would still be
// KEY-DISTINCT, just byte-shared. F6 must therefore use DIFFERENT
// CONTENT per binding so the cross-pollution test surfaces if it
// happens. The test asserts: (1) keys differ, (2) reads via binding-B
// never return binding-A's content bytes.
//
// HARNESS (per evaltest/evaluate_test.go pattern, copied to this
// package because the helper isn't exported)
//
// We build a real *cache.ResourceWatcher backed by a dynamic.fake
// client seeded with two CRBs:
//
//   crbA: grants admin "get/widgets" cluster-wide; UID="crb-a-f6-uid"
//   crbB: grants devs  "get/widgets" cluster-wide; UID="crb-b-f6-uid"
//
// Two test users:
//
//   userA: Username="admin", Groups=["system:masters"]; matches crbA
//   userB: Username="dev-1", Groups=["devs"];           matches crbB
//
// Under EvaluateRBAC's stable-order walk (design §6), each user's
// first-match is unambiguously their respective CRB. The two
// BindingUIDs are "C:crb-a-f6-uid" and "C:crb-b-f6-uid", which fold
// into distinct raFullList cell keys.
//
// CONCURRENT POPULATE + READ
//
// Spawn paired goroutines per iteration: one populates via userA's
// resolveRA (admin-content), one reads via userB's identity. The
// concurrent ordering exercises any rare cross-pollution path. Repeat
// 100× under -race. Race detector hits OR content-bleed are
// failures.
//
// HARD GATE: F6 PASS is a prerequisite for Phase 4 dual-gate review.

package apiref

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jqutil"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// f6RBACListKinds is the RBAC list-kinds registration the dynamic
// fake needs to serve the LIST verb the ResourceWatcher issues
// during initial cache sync. Mirrors evaltest/evaluate_test.go's
// rbacListKinds without exporting it.
func f6RBACListKinds() map[schema.GroupVersionResource]string {
	return map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:                "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:         "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:         "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}:  "ClusterRoleBindingList",
	}
}

// newF6Watcher constructs a real *cache.ResourceWatcher backed by a
// dynamic fake seeded with the supplied RBAC objects. Wires
// cache.SetGlobal so rbac.EvaluateRBAC reads the snapshot. t.Cleanup
// is registered for teardown so subsequent tests see a clean global.
func newF6Watcher(t *testing.T, seed ...runtime.Object) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")

	sch := runtime.NewScheme()
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1.AddToScheme: %v", err)
	}

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, f6RBACListKinds(), seed...)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher")
	}
	t.Cleanup(rw.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	// #471(b) — WaitForCacheSync returns when the informers have synced, NOT
	// when this watcher's initial RBAC snapshot has been published: that runs
	// on a detached goroutine (waitAndPublishInitialRBACSnapshot). Without the
	// await, the arm starts against rbacSnap == nil (degrade-to-deny) or has
	// the publish land mid-arm. #385 added this seam for exactly that and four
	// dispatchers harnesses plus three in restactions/api already use it; this
	// one and edge3NewWatcher did not.
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		t.Fatalf("WaitInitialRBACPublishForTest: %v", err)
	}

	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
}

// f6ContentWithMarker is the resolveRA closure factory: returns a
// closure that resolves the RA's full-list using a DISTINCTIVE panel
// name prefix so cross-pollution is content-detectable (per Diego's
// 2c note #5: F6 tests KEY isolation, not byte dedup; distinct
// content per binding lets us detect cross-pollination even if a
// byte-dedup layer existed).
func f6ContentWithMarker(t *testing.T, marker string, calls *atomic.Int64) func(context.Context, int, int) (map[string]any, error) {
	const n = 30
	items := make([]any, n)
	for i := 0; i < n; i++ {
		items[i] = map[string]any{"metadata": map[string]any{
			"name":              fmt.Sprintf("%s-panel-%03d", marker, i),
			"creationTimestamp": fmt.Sprintf("2026-01-%02dT00:00:00Z", i+1),
		}}
	}
	panels := map[string]any{"compositionspanels": items}
	return func(_ context.Context, perPage, page int) (map[string]any, error) {
		calls.Add(1)
		dict := map[string]any{}
		for k, v := range panels {
			dict[k] = v
		}
		if perPage > 0 && page > 0 {
			dict["slice"] = map[string]any{
				"perPage": float64(perPage),
				"page":    float64(page),
				"offset":  float64((page - 1) * perPage),
			}
		}
		s, err := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: raSliceJQ, Data: dict})
		if err != nil {
			return nil, err
		}
		var out map[string]any
		if err := json.Unmarshal([]byte(s), &out); err != nil {
			return nil, err
		}
		return out, nil
	}
}

// f6CtxWithUser builds a request context carrying a UserInfo. The
// production raFullListServe reads this via xcontext.UserInfo(ctx)
// → rbac.EvaluateRBAC under the user's identity.
func f6CtxWithUser(t *testing.T, username string, groups []string) context.Context {
	return xcontext.BuildContext(t.Context(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: username, Groups: groups}))
}

// f6ContainsMarker inspects the served map and returns true iff ANY
// item's metadata.name contains the marker substring. Used to detect
// content-bleed across binding cells.
func f6ContainsMarker(t *testing.T, served map[string]any, marker string) bool {
	t.Helper()
	items, ok := served["compositionspanels"].([]any)
	if !ok {
		// Some test paths may return a different shape; the slice jq
		// emits compositionspanels.
		return false
	}
	for _, it := range items {
		m, ok := it.(map[string]any)
		if !ok {
			continue
		}
		meta, ok := m["metadata"].(map[string]any)
		if !ok {
			continue
		}
		name, _ := meta["name"].(string)
		if strings.Contains(name, marker) {
			return true
		}
	}
	return false
}

// f6BuildFixture builds the test fixture: a widgets-permitting
// ClusterRole + two CRBs (A and B) with distinct UIDs binding the
// role to admin/devs respectively. The GVR is the widgets GVR per
// the design §3.3 raFullList row (the cell folds the widget's
// GET-permit BindingUID — the same widgets-layer permit the apiref
// is bounded under).
func f6BuildFixture() []runtime.Object {
	widgetsReader := &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: "widgets-reader"},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{"templates.krateo.io"},
			Resources: []string{"restactions"},
			Verbs:     []string{"get", "list"},
		}},
	}
	crbA := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "crb-A-admin", UID: types.UID("crb-a-f6-uid")},
		Subjects: []rbacv1.Subject{
			{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "admin"},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "widgets-reader",
		},
	}
	crbB := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "crb-B-devs", UID: types.UID("crb-b-f6-uid")},
		Subjects: []rbacv1.Subject{
			{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "devs"},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "widgets-reader",
		},
	}
	return []runtime.Object{widgetsReader, crbA, crbB}
}

// ──────────────────────────────────────────────────────────────────────
// F6 — sequential baseline (key isolation visible without -race)
// ──────────────────────────────────────────────────────────────────────

// TestF6_CrossBinding_KeysAreDistinct sets up two users matching two
// distinct CRBs and asserts the raFullList cell keys are KEY-distinct
// (not byte-distinct — cells happen to have different content here
// because the per-user resolveRA closures emit different markers, but
// the test's load-bearing assertion is key inequality).
func TestF6_CrossBinding_KeysAreDistinct(t *testing.T) {
	cache.ResetResolvedCacheForTest()
	newF6Watcher(t, f6BuildFixture()...)

	gvrW := gvr() // templates.krateo.io/v1/restactions
	const ns = "krateo-system"
	const name = "f6-ra"

	ctxA := f6CtxWithUser(t, "admin", []string{"system:masters"})
	ctxB := f6CtxWithUser(t, "dev-1", []string{"devs"})

	var callsA, callsB atomic.Int64
	resolveA := f6ContentWithMarker(t, "MARKER-A", &callsA)
	resolveB := f6ContentWithMarker(t, "MARKER-B", &callsB)

	// Populate cell-A under user A's identity.
	gotA, ok, err := raFullListServe(ctxA, gvrW, ns, name, ra(raSliceJQ), 5, 1, nil, resolveA)
	if err != nil || !ok {
		t.Fatalf("serve under user A: ok=%v err=%v", ok, err)
	}
	if !f6ContainsMarker(t, gotA, "MARKER-A") {
		t.Fatalf("user A's serve did not return MARKER-A content; got %+v", gotA)
	}

	// Populate cell-B under user B's identity.
	gotB, ok, err := raFullListServe(ctxB, gvrW, ns, name, ra(raSliceJQ), 5, 1, nil, resolveB)
	if err != nil || !ok {
		t.Fatalf("serve under user B: ok=%v err=%v", ok, err)
	}
	if !f6ContainsMarker(t, gotB, "MARKER-B") {
		t.Fatalf("user B's serve did not return MARKER-B content; got %+v", gotB)
	}

	// CROSS-POLLUTION assertion: user B's content MUST NOT contain
	// user A's marker. This is the F6 load-bearing claim — if it
	// passes, the per-binding cell key truly isolated the two users'
	// reads.
	if f6ContainsMarker(t, gotB, "MARKER-A") {
		t.Fatalf("F6 CROSS-BINDING LEAK: user B's serve contained MARKER-A — binding-A's cell content bled into binding-B's read")
	}
	if f6ContainsMarker(t, gotA, "MARKER-B") {
		t.Fatalf("F6 CROSS-BINDING LEAK: user A's serve contained MARKER-B — binding-B's cell content bled into binding-A's read")
	}

	// KEY-isolation assertion (the structural invariant): the two
	// users produce two DIFFERENT BindingUIDs → two DIFFERENT raKeys.
	// We compute them directly to confirm.
	keyInputsA := f6KeyInputs(gvrW.Group, gvrW.Version, gvrW.Resource,
		ns, name, "C:crb-a-f6-uid", nil)
	keyInputsB := f6KeyInputs(gvrW.Group, gvrW.Version, gvrW.Resource,
		ns, name, "C:crb-b-f6-uid", nil)
	raKeyA := cache.ComputeKey(keyInputsA)
	raKeyB := cache.ComputeKey(keyInputsB)
	if raKeyA == raKeyB {
		t.Fatalf("F6 KEY-ISOLATION BROKEN: two distinct BindingUIDs produced identical raKeys; %q == %q", raKeyA, raKeyB)
	}

	// Cells are present (both populated). The cache MUST hold both
	// entries — if cell-A's Put had over-written cell-B's, one would
	// be missing.
	if _, ok := cache.ResolvedCache().Get(raKeyA); !ok {
		t.Fatalf("cell-A was not Put under raKeyA=%s", raKeyA)
	}
	if _, ok := cache.ResolvedCache().Get(raKeyB); !ok {
		t.Fatalf("cell-B was not Put under raKeyB=%s", raKeyB)
	}
}

// ──────────────────────────────────────────────────────────────────────
// F6 — concurrent populate + read under -race (100 iterations)
// ──────────────────────────────────────────────────────────────────────

// TestF6_CrossBinding_ConcurrentRace_100Iterations is the F6 HARD
// GATE. 100 iterations of paired goroutines hammering populate-via-A
// + read-via-B concurrently. Asserts:
//   (1) NO race detector hit (the test runs under `go test -race`).
//   (2) NO content bleed in EITHER direction at ANY iteration.
//   (3) Both raKeys remain KEY-distinct after the burst.
//
// The cache is RESET between iterations so each iteration exercises
// the cold + populate + read path freshly (the race-detector signal
// is integrated across all 100 iterations).
func TestF6_CrossBinding_ConcurrentRace_100Iterations(t *testing.T) {
	cache.ResetResolvedCacheForTest()
	newF6Watcher(t, f6BuildFixture()...)

	gvrW := gvr()
	const ns = "krateo-system"
	const name = "f6-race-ra"

	ctxA := f6CtxWithUser(t, "admin", []string{"system:masters"})
	ctxB := f6CtxWithUser(t, "dev-1", []string{"devs"})

	const iterations = 100

	for iter := 0; iter < iterations; iter++ {
		// Fresh cache per iteration so the cold + populate path runs
		// each time. The race-detector signal accumulates globally.
		cache.ResetResolvedCacheForTest()

		var callsA, callsB atomic.Int64
		// Per-iteration marker so a content-bleed across iterations
		// would also be detectable (if the cache had a persistent
		// pre-existing entry from a previous iteration).
		markerA := fmt.Sprintf("ITER-%d-A", iter)
		markerB := fmt.Sprintf("ITER-%d-B", iter)
		resolveA := f6ContentWithMarker(t, markerA, &callsA)
		resolveB := f6ContentWithMarker(t, markerB, &callsB)

		// Per-iteration results, captured for post-goroutine assertion.
		var gotA, gotB map[string]any
		var errA, errB error
		var okA, okB bool

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			gotA, okA, errA = raFullListServe(ctxA, gvrW, ns, name, ra(raSliceJQ), 5, 1, nil, resolveA)
		}()
		go func() {
			defer wg.Done()
			gotB, okB, errB = raFullListServe(ctxB, gvrW, ns, name, ra(raSliceJQ), 5, 1, nil, resolveB)
		}()
		wg.Wait()

		if errA != nil || !okA {
			t.Fatalf("iter %d: user A serve failed: ok=%v err=%v", iter, okA, errA)
		}
		if errB != nil || !okB {
			t.Fatalf("iter %d: user B serve failed: ok=%v err=%v", iter, okB, errB)
		}

		// Each user MUST see their OWN marker.
		if !f6ContainsMarker(t, gotA, markerA) {
			t.Fatalf("iter %d: user A did not see %s; got %+v", iter, markerA, gotA)
		}
		if !f6ContainsMarker(t, gotB, markerB) {
			t.Fatalf("iter %d: user B did not see %s; got %+v", iter, markerB, gotB)
		}

		// CROSS-POLLUTION assertion (the F6 load-bearing claim):
		// each user's serve MUST NOT contain the other's marker. At
		// any iteration. Under any concurrent ordering. Failure here
		// = key isolation broken.
		if f6ContainsMarker(t, gotA, markerB) {
			t.Fatalf("iter %d F6 CROSS-BINDING LEAK: user A's serve contained %s — binding-B's content bled into binding-A's read", iter, markerB)
		}
		if f6ContainsMarker(t, gotB, markerA) {
			t.Fatalf("iter %d F6 CROSS-BINDING LEAK: user B's serve contained %s — binding-A's content bled into binding-B's read", iter, markerA)
		}
	}

	// After the burst, both cells should still be KEY-distinct and
	// independently addressable from the cache.
	keyInputsA := f6KeyInputs(gvrW.Group, gvrW.Version, gvrW.Resource,
		ns, name, "C:crb-a-f6-uid", nil)
	keyInputsB := f6KeyInputs(gvrW.Group, gvrW.Version, gvrW.Resource,
		ns, name, "C:crb-b-f6-uid", nil)
	if cache.ComputeKey(keyInputsA) == cache.ComputeKey(keyInputsB) {
		t.Fatalf("F6 KEY-ISOLATION BROKEN at end of %d iterations: raKeys collapsed", iterations)
	}
}

// f6Compile asserts ptr.To remains in scope as we may not use it
// elsewhere in this file. Keeps imports honest.
var _ = ptr.To[any]
var _ = templatesv1.RESTAction{}
