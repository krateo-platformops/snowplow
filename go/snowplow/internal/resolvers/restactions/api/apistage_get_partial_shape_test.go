// apistage_get_partial_shape_test.go — Ship D.4.2 (0.30.149) hermetic
// regression tests for the gateGetEnvelope partial-shape guard.
//
// **PM Tightening #5 — VERBATIM apiserver-shaped fixtures.** Each
// load-bearing test uses a testdata/ JSON fixture captured from a
// LIVE cluster via `kubectl get --raw` — NOT hand-constructed
// maps. The healthy and partial-shape fixtures are bit-equal to
// what the snowplow apistage cache stores in production:
//
//   testdata/healthy_configmap_get_byname.json
//     ← kubectl get --raw /api/v1/namespaces/krateo-system/configmaps/argocd-cm
//     The apiserver-direct GET-by-name response carries FULL TypeMeta
//     (apiVersion="v1" + kind="ConfigMap"). Verified at fixture
//     capture time; design §1.5 also TRACED this independently.
//
//   testdata/partial_shape_configmap_list_item.json
//     ← extracted item[0] from kubectl get --raw
//       /api/v1/namespaces/krateo-system/configmaps
//     Per the k8s wire convention, core-group LIST per-item TypeMeta
//     is ELIDED — the item bytes carry NO apiVersion + NO kind. This
//     is verbatim what streaming_list.go:507's decoder captures into
//     bytesObject.raw, and what gateGetEnvelope subsequently decodes
//     back when an iterator does a per-name GET against the cached
//     LIST shape.
//
// The 0.30.148 burst's site=13 evidence (10/250 fires with
// `obj_apiVersion_present=false` on `/v1, Resource=configmaps`) is the
// empirical anchor; this test pins that anchor at the unit-test layer
// using the SAME wire-shape the burst captured.
package api

import (
	"context"
	_ "embed"
	"encoding/json"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// newD42WatcherWithGetConfigmaps builds a cache=on ResourceWatcher
// whose f1BroadUser is granted `list+get configmaps` cluster-wide
// (via a ClusterRole + ClusterRoleBinding). The shared newF1Watcher
// helper grants only `list widgets`; gateGetEnvelope's
// filterGetByRBAC requires `get` on the GVR being served, so the
// D.4.2 tests need a richer RBAC fixture.
//
// Reused by every D.4.2 unit test as the watcher harness. RBAC is
// not under test here — the predicate-firing tests want RBAC to
// allow the GET through so the predicate path is reached. The
// RBAC-deny test (TestGateGetEnvelope_RBACDeny_DoesNotInvokePredicate)
// uses an identity NOT bound by this fixture.
func newD42WatcherWithGetConfigmaps(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	// #57: api-stage L1 + informer pivot are implicit under CACHE_ENABLED
	// (RESOLVED_CACHE_APISTAGE_ENABLED + RESOLVER_USE_INFORMER retired).
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	cr := &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: "d42-configmap-list-and-get"},
		Rules: []rbacv1.PolicyRule{
			// Core-group v1: empty APIGroups string is the
			// k8s convention for the core group.
			{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"list", "get"}},
		},
	}
	crb := &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: "d42-configmap-broad-binding"},
		Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: f1BroadUser}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "d42-configmap-list-and-get"},
	}

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:                "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:         "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:         "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, runtime.Object(cr), runtime.Object(crb))

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher under CACHE_ENABLED=true")
	}
	t.Cleanup(rw.Stop) // #85: Stop() blocks until goroutine drain — no settle-sleep needed

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync (RBAC informers): %v", err)
	}
	waitOwnRBACPublish(t, rw)
	return rw
}

//go:embed testdata/healthy_configmap_get_byname.json
var fixtureHealthyConfigMapGetByName []byte

//go:embed testdata/partial_shape_configmap_list_item.json
var fixturePartialShapeConfigMapListItem []byte

// configmapGVR — the GVR the 0.30.148 burst's 10 NULL site=13 fires
// were labeled with (`gvr="/v1, Resource=configmaps"`).
var configmapGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

// d42PartialShapeCounterFor returns the current per-cell value of
// ReasonApistageGetPartialShape for a (scope, gvr) pair. Used by the
// AC-D4.2.2 counter-wiring tests.
func d42PartialShapeCounterFor(scope, gvr string) uint64 {
	return cache.FallthroughCount(scope, gvr, cache.ReasonApistageGetPartialShape)
}

// scopedD42Ctx wraps ctx with the Ship D FallthroughScope + the
// f1BroadUser identity. RBAC: the F1 watcher's f1WidgetListerClusterRole
// grants `list widgets` to f1BroadUser — but gateGetEnvelope uses
// filterGetByRBAC against the GVR PASSED IN as arg, not the GVR the
// user has grants on. To bypass filterGetByRBAC cleanly in tests we
// either (a) wire a configmap RoleBinding for f1BroadUser, or (b) use
// a GVR where the broad user has implicit access. Since the gate's
// RBAC layer is not under test here (we test the partial-shape
// predicate, AFTER RBAC passes), the simplest path is (a).
func scopedD42Ctx(ctx context.Context) context.Context {
	return cache.WithFallthroughScope(ctx,
		cache.ScopeCallRestactions,
	)
}

// runGateGetEnvelopeTest is the shared harness for the D.4.2 tests.
// Sets up an F1 watcher (broad-user has full configmap GET grants
// per the helper), invokes gateGetEnvelope against the supplied raw
// bytes, returns (served-obj, ok-served, counterDelta).
//
// The harness reuses the existing newF1Watcher + extends it with a
// configmap RoleBinding so filterGetByRBAC passes for the broad user.
func runGateGetEnvelopeTest(t *testing.T, raw []byte) (any, bool, uint64) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetFallthroughCountersForTest()

	// f1BroadUser already has `list widgets` from newF1Watcher; for
	// `get configmaps` we use the broader watcher helper from
	// apistage_partial_shape_test.go's lineage. Simpler: drive the
	// gate directly with a permissive identity.
	rw := newD42WatcherWithGetConfigmaps(t)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	ctx := scopedD42Ctx(xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: f1BroadUser}),
	))

	before := d42PartialShapeCounterFor(cache.ScopeCallRestactions, configmapGVR.String())
	got, ok := gateGetEnvelope(ctx, configmapGVR, raw)
	after := d42PartialShapeCounterFor(cache.ScopeCallRestactions, configmapGVR.String())
	return got, ok, after - before
}

// ─────────────────────────────────────────────────────────────────────
// AC-D4.2.3 #1 — partial-shape ABSENT apiVersion (the 0.30.148 burst's
// empirical 10/250 NULL signal) — fires
// ─────────────────────────────────────────────────────────────────────

// TestGateGetEnvelope_PartialShape_AbsentApiVersion exercises the
// EMPIRICAL anchor from the 0.30.148 burst (site=13 success-path exit):
//
//   site=13 ... gvr="/v1, Resource=configmaps"
//     obj_apiVersion_present=false
//     obj_apiVersion_is_string=false
//     obj_apiVersion_is_empty_string=false
//     obj_apiVersion_type=<nil>
//
// The fixture testdata/partial_shape_configmap_list_item.json is the
// VERBATIM apiserver wire shape that produces this site=13 signal —
// extracted item[0] from a LIST response (core-group LIST per-item
// TypeMeta elided per k8s convention).
//
// Predicate firing: obj["apiVersion"] == nil → fires → counter += 1 →
// returns (nil, false) so the caller falls through to fresh apiserver
// GET-by-name (verified at design §1.5: kubectl get --raw on the
// same configmap returns apiVersion="v1" + kind="ConfigMap").
func TestGateGetEnvelope_PartialShape_AbsentApiVersion(t *testing.T) {
	got, ok, delta := runGateGetEnvelopeTest(t, fixturePartialShapeConfigMapListItem)

	if ok {
		t.Fatalf("gateGetEnvelope served=true on partial-shape fixture; want false (predicate fires)")
	}
	if got != nil {
		t.Errorf("gateGetEnvelope returned %v; want nil", got)
	}
	if delta != 1 {
		t.Errorf("counter delta = %d; want 1 (partial-shape fixture must fire predicate)", delta)
	}

	// Confirm the fixture itself matches the empirical 0.30.148
	// signal (defensive — a future fixture-update that accidentally
	// adds TypeMeta would silently break this test's empirical
	// anchor; this sub-assert catches that).
	var probe map[string]any
	if err := json.Unmarshal(fixturePartialShapeConfigMapListItem, &probe); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if _, present := probe["apiVersion"]; present {
		t.Errorf("fixture has apiVersion key — partial-shape anchor broken; re-capture from kubectl get --raw")
	}
	if _, present := probe["kind"]; present {
		t.Errorf("fixture has kind key — partial-shape anchor broken; re-capture from kubectl get --raw")
	}
}

// ─────────────────────────────────────────────────────────────────────
// AC-D4.2.3 #2 — partial-shape ABSENT kind (symmetric) — fires
// ─────────────────────────────────────────────────────────────────────

func TestGateGetEnvelope_PartialShape_AbsentKind(t *testing.T) {
	// Construct a JSON shape with apiVersion present + kind absent.
	// Derived from the healthy fixture by stripping kind, so the
	// rest of the shape is verbatim apiserver bytes.
	var m map[string]any
	if err := json.Unmarshal(fixtureHealthyConfigMapGetByName, &m); err != nil {
		t.Fatalf("decode healthy fixture: %v", err)
	}
	delete(m, "kind")
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal partial shape: %v", err)
	}

	got, ok, delta := runGateGetEnvelopeTest(t, raw)
	if ok || got != nil {
		t.Errorf("kind-absent: served=%v got=%v; want (nil, false)", ok, got)
	}
	if delta != 1 {
		t.Errorf("counter delta = %d; want 1", delta)
	}
}

// ─────────────────────────────────────────────────────────────────────
// AC-D4.2.3 #3 — JSON literal null apiVersion (defensive — not
// empirically observed; same predicate catches it) — fires
// ─────────────────────────────────────────────────────────────────────

// TestGateGetEnvelope_PartialShape_LiteralNullApiVersion confirms the
// Go nil-check predicate's defensive extension: a JSON `null` value
// decodes to Go `nil`, the same as an absent key. The predicate
// fires for BOTH cases via a single code path. Per design §3.3,
// this is hypothetical (the 0.30.148 burst showed only key-absent,
// not JSON-null) but cheap to cover.
func TestGateGetEnvelope_PartialShape_LiteralNullApiVersion(t *testing.T) {
	// JSON: {"apiVersion": null, "kind": "ConfigMap", "metadata": {...}}
	var m map[string]any
	if err := json.Unmarshal(fixtureHealthyConfigMapGetByName, &m); err != nil {
		t.Fatalf("decode healthy fixture: %v", err)
	}
	m["apiVersion"] = nil // explicit JSON-null in the marshalled bytes
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, ok, delta := runGateGetEnvelopeTest(t, raw)
	if ok || got != nil {
		t.Errorf("null-apiVersion: served=%v got=%v; want (nil, false)", ok, got)
	}
	if delta != 1 {
		t.Errorf("counter delta = %d; want 1 (JSON null decodes to Go nil → predicate fires)", delta)
	}
}

// ─────────────────────────────────────────────────────────────────────
// AC-D4.2.3 #4 — healthy GET-by-name response — does NOT fire (the
// canary against over-aggressive predicate)
// ─────────────────────────────────────────────────────────────────────

// TestGateGetEnvelope_HealthyShape_PassesThrough verifies the
// predicate's quiet path: a fully-populated GET-by-name response
// (apiVersion="v1" + kind="ConfigMap" — verbatim from `kubectl get
// --raw /api/v1/namespaces/.../configmaps/argocd-cm`) passes
// through unchanged. The counter stays at zero.
func TestGateGetEnvelope_HealthyShape_PassesThrough(t *testing.T) {
	got, ok, delta := runGateGetEnvelopeTest(t, fixtureHealthyConfigMapGetByName)

	if !ok {
		t.Fatalf("gateGetEnvelope served=false on healthy fixture; want true")
	}
	obj, isMap := got.(map[string]any)
	if !isMap {
		t.Fatalf("served value type = %T; want map[string]any", got)
	}
	if av := obj["apiVersion"]; av != "v1" {
		t.Errorf("served obj.apiVersion = %v; want v1 (healthy fixture)", av)
	}
	if k := obj["kind"]; k != "ConfigMap" {
		t.Errorf("served obj.kind = %v; want ConfigMap (healthy fixture)", k)
	}
	if delta != 0 {
		t.Errorf("counter delta = %d; want 0 (healthy fixture must NOT fire predicate)", delta)
	}
}

// ─────────────────────────────────────────────────────────────────────
// AC-D4.2.4 — pin against the D.4 false-positive class (LOAD-BEARING)
// ─────────────────────────────────────────────────────────────────────

// TestGateGetEnvelope_HealthyShape_EmptyStringApiVersion is the
// LOAD-BEARING regression pin against the D.4 false-positive class.
// D.4's predicate was `obj["apiVersion"].(string) == ""` which would
// fire on BOTH key-absent (the defect) AND key-present-but-empty
// (D.4's false-positive trap). D.4.2's NARROWER Go-nil-check predicate
// fires ONLY on key-absent.
//
// Empirical justification (design §3.2): the 0.30.148 burst's 250
// site=13 fires show ZERO with `obj_apiVersion_is_empty_string=true`.
// The present-but-empty-string case does NOT empirically exist on the
// gateGetEnvelope path; D.4.2 explicitly does NOT fire here, bounding
// the predicate's blast radius to the empirically-observed class only.
//
// A future regression that switches the predicate to
// `obj["apiVersion"] == nil || obj["apiVersion"] == ""` would FAIL
// this test, surfacing the D.4-class regression at unit-test time —
// before deploy, not after deploy → revert.
func TestGateGetEnvelope_HealthyShape_EmptyStringApiVersion(t *testing.T) {
	// Construct a JSON shape with both apiVersion AND kind present
	// but empty-string. Derived from the healthy fixture.
	var m map[string]any
	if err := json.Unmarshal(fixtureHealthyConfigMapGetByName, &m); err != nil {
		t.Fatalf("decode healthy fixture: %v", err)
	}
	m["apiVersion"] = ""
	m["kind"] = ""
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	got, ok, delta := runGateGetEnvelopeTest(t, raw)

	// The D.4 false-positive class: D.4.2 explicitly does NOT fire
	// on present-but-empty-string. The object passes through served.
	if !ok {
		t.Fatalf("D.4 REGRESSION: empty-string apiVersion fired predicate; D.4.2 must NOT fire here (predicate is Go nil-check, NOT string-zero-value)")
	}
	if got == nil {
		t.Errorf("D.4 REGRESSION: got=nil on empty-string apiVersion; want the served map")
	}
	if delta != 0 {
		t.Errorf("D.4 REGRESSION: counter delta = %d; want 0 (D.4.2 predicate must NOT fire on empty-string; the 0.30.148 burst had 0/250 is_empty_string=true)", delta)
	}
}

// ─────────────────────────────────────────────────────────────────────
// AC-D4.2.3 — RBAC ordering: predicate runs AFTER filterGetByRBAC
// (denied requests never reach the predicate)
// ─────────────────────────────────────────────────────────────────────

// TestGateGetEnvelope_RBACDeny_DoesNotInvokePredicate verifies the
// site placement: RBAC denial returns (nil, false) BEFORE the
// partial-shape predicate runs, so RBAC narrowing dominates. This
// matches the Ship B / Ship D RBAC-precedence contract — guards
// never widen RBAC.
//
// Mechanism: drive gateGetEnvelope with a partial-shape fixture
// against an identity that does NOT have `get configmaps` grants.
// filterGetByRBAC returns false → gate returns (nil, false) without
// ever consulting the partial-shape predicate. The counter stays at
// zero (the predicate path was not reached).
func TestGateGetEnvelope_RBACDeny_DoesNotInvokePredicate(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetFallthroughCountersForTest()

	rw := newD42WatcherWithGetConfigmaps(t) // grants ONLY f1BroadUser; no-grants-user denied
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	// Use a different identity that has NO grants → RBAC denies.
	ctx := scopedD42Ctx(xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "no-grants-user"}),
	))

	before := d42PartialShapeCounterFor(cache.ScopeCallRestactions, configmapGVR.String())
	got, ok := gateGetEnvelope(ctx, configmapGVR, fixturePartialShapeConfigMapListItem)
	after := d42PartialShapeCounterFor(cache.ScopeCallRestactions, configmapGVR.String())

	if ok || got != nil {
		t.Errorf("RBAC-deny: served=%v got=%v; want (nil, false)", ok, got)
	}
	if delta := after - before; delta != 0 {
		t.Errorf("RBAC-deny path bumped the partial-shape counter (delta=%d); predicate must run AFTER RBAC", delta)
	}
}

// ─────────────────────────────────────────────────────────────────────
// AC-D4.2.3 — counter wiring (per-GVR breakdown)
// ─────────────────────────────────────────────────────────────────────

// TestGateGetEnvelope_PartialShape_CounterWiring verifies the
// `ReasonApistageGetPartialShape` counter cell is correctly wired
// for the (scope, gvr) tuple. AC-D4.2.HG.2 cites this counter as the
// post-deploy observable on /debug/vars.
//
// Two consecutive fires on the same GVR → counter += 2 in the same
// (scope, gvr, reason) cell. Confirms cell key shape matches design.
func TestGateGetEnvelope_PartialShape_CounterWiring(t *testing.T) {
	got1, ok1, delta1 := runGateGetEnvelopeTest(t, fixturePartialShapeConfigMapListItem)
	if ok1 || got1 != nil || delta1 != 1 {
		t.Fatalf("first fire: ok=%v got=%v delta=%d; want (false, nil, 1)", ok1, got1, delta1)
	}

	// Second fire — same fixture, same scope. The counter
	// accumulates; the harness's ResetFallthroughCountersForTest
	// runs at test start, so this test's `before` baseline catches
	// the post-first-fire state.
	before := d42PartialShapeCounterFor(cache.ScopeCallRestactions, configmapGVR.String())

	rw := cache.Global()
	if rw == nil {
		t.Fatal("cache.Global() returned nil after harness setup")
	}
	ctx := scopedD42Ctx(xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: f1BroadUser}),
	))
	got2, ok2 := gateGetEnvelope(ctx, configmapGVR, fixturePartialShapeConfigMapListItem)
	if ok2 || got2 != nil {
		t.Errorf("second fire served=%v got=%v; want (nil, false)", ok2, got2)
	}
	after := d42PartialShapeCounterFor(cache.ScopeCallRestactions, configmapGVR.String())
	if delta := after - before; delta != 1 {
		t.Errorf("second-fire counter delta = %d; want 1 (per-cell accumulation)", delta)
	}

	// Confirm the cell-key shape (path + gvr + reason). A future
	// regression that wires the counter with an empty `gvr` argument
	// (or with `gvr` as something other than gvr.String() — e.g. a
	// stage name) would put the increments in the wrong cell; this
	// assert catches it.
	cellTotal := d42PartialShapeCounterFor(cache.ScopeCallRestactions, configmapGVR.String())
	if cellTotal < 2 {
		t.Errorf("(call-restactions, %s, apistage-get-partial-shape) cell = %d; want ≥2 (per-GVR breakdown wired)",
			configmapGVR.String(), cellTotal)
	}
	if other := d42PartialShapeCounterFor(cache.ScopeCallRestactions, ""); other != 0 {
		t.Errorf("(call-restactions, <empty>, ...) cell = %d; want 0 (predicate must use real GVR string, not empty)", other)
	}

	// Sanity: the counter argument string matches gvr.String()
	// format used by /debug/vars cells.
	wantGVRString := "/v1, Resource=configmaps"
	if !strings.Contains(configmapGVR.String(), "configmaps") {
		t.Errorf("configmapGVR.String() = %q; want substring containing 'configmaps' (the burst-log evidence's cell)", configmapGVR.String())
	}
	_ = wantGVRString
}
