// servehttp_orchestration_test.go — H1 (coverage-audit) SECURITY: hermetic
// httptest drive of the RESTAction AND Widget ServeHTTP ORCHESTRATION.
//
// WHY A SEAMED FULL-SERVE DRIVE (and not the sub-block mirror the repo uses for
// error-encoding): the H1 gaps are ORDERING + WHOLE-PATH properties that are
// invisible at sub-block granularity —
//   (a) the RBAC gate runs BEFORE the cache Get (a cache HIT must never
//       short-circuit the permission check → serve-before-RBAC leak);
//   (c) a cold miss Puts EXACTLY ONE entry under the DERIVED key + records a dep;
//   (d) the warm-hit bytes == the cache-off resolve bytes (cache ≡ no-cache);
//   (e/f) an RBAC-sensitive widget SKIPS the shared identity-free content cell;
//   (h/i/j) the stage-error / external sinks gate the Put (serve 200, no Put).
// None of these can be asserted by mirroring one `if` block. So this file swaps
// the four ServeHTTP collaborators (dispatch_seams.go: fetchObjectFn,
// checkDispatchRBACFn, restactionsResolveFn, widgetsResolveFn) for hermetic
// fakes and drives the REAL handler. The cache-key derivation
// (dispatchCacheLookupKey → real rbac.EvaluateRBAC over a wired dynamicfake
// watcher), the real *resolvedCache handle, setRefreshKeyHeader, the encode +
// the Put-gate control flow all stay PRODUCTION — only the object fetch, the
// coarse gate verdict, and the resolve output are faked.
//
// Never touches ./internal/rbac (the RBAC snapshot is built via the cache
// package's RebuildRBACSnapshotForTest, exactly like serve_empty_binding_miss_test.go).

package dispatchers

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

var (
	h1RAGVR     = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
	h1WidgetGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}
)

const (
	h1NS     = "krateo-system"
	h1RAName = "demo-ra"
	h1WName  = "demo-widget"
	h1User   = "alice"
)

// h1BuildWatcher wires a cache=on ResourceWatcher whose RBAC snapshot grants
// h1User get/list on BOTH the RA and widget GVRs, so dispatchCacheLookupKey
// re-derives a NON-empty first-match BindingUID (serveFromCacheEligible==true)
// and the L1 Put/Get paths are actually reachable. Mirrors buildSec95Watcher.
func h1BuildWatcher(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	// The DepTracker is a process-wide singleton — reset it so an earlier arm's
	// cold-Put dep edge (same reqCtx → same derived key) does not leak into a
	// later arm's dep assertions (arm (c) records; arm (i) asserts none).
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		h1RAGVR:     "RESTActionList",
		h1WidgetGVR: "PanelList",
		crbGVR:      "ClusterRoleBindingList",
		crGVR:       "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
	}
	rule := []rbacv1.PolicyRule{{
		Verbs:     []string{"get", "list"},
		APIGroups: []string{h1RAGVR.Group, h1WidgetGVR.Group},
		Resources: []string{h1RAGVR.Resource, h1WidgetGVR.Resource},
	}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "h1-reader"}, Rules: rule},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "h1-bind", UID: types.UID("uid-h1")},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: h1User}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "h1-reader"},
		},
	}

	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
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

// h1ReqCtx is a per-request context carrying ONLY the JWT identity (the real
// /call path). Deliberately carries NO WithInternalEndpoint / WithInternalRESTConfig
// (out-of-cluster handler → saEP/saRC nil → no attach) — M12 reads this back.
func h1ReqCtx(user string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: []string{"devs"}}))
}

func h1RAUnstructured() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": h1RAName, "namespace": h1NS},
		"spec":       map[string]any{"api": []any{}},
	}}
}

func h1WidgetUnstructured(spec map[string]any) *unstructured.Unstructured {
	if spec == nil {
		spec = map[string]any{}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1WidgetGVR.Group + "/" + h1WidgetGVR.Version,
		"kind":       "Panel",
		"metadata":   map[string]any{"name": h1WName, "namespace": h1NS},
		"spec":       spec,
	}}
}

// installRAFakes swaps the three RESTAction ServeHTTP collaborators for the
// duration of a test: fetchObject (returns the given CR), the RBAC gate verdict
// (allow/deny via allowFn), and the resolver. Returns ONE restore closure.
func installRAFakes(t *testing.T, cr *unstructured.Unstructured, allowFn func() bool,
	resolve func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error)) func() {
	t.Helper()
	r1 := setFetchObjectForTest(func(req *http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: cr}
	})
	r2 := setCheckDispatchRBACForTest(func(ctx context.Context, gvr schema.GroupVersionResource, ns string) bool {
		return allowFn()
	})
	r3 := setRestactionsResolveForTest(resolve)
	return func() { r3(); r2(); r1() }
}

// installWidgetFakes is the widget twin of installRAFakes.
func installWidgetFakes(t *testing.T, cr *unstructured.Unstructured, allowFn func() bool,
	resolve func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error)) func() {
	t.Helper()
	r1 := setFetchObjectForTest(func(req *http.Request) objects.Result {
		return objects.Result{GVR: h1WidgetGVR, Unstructured: cr}
	})
	r2 := setCheckDispatchRBACForTest(func(ctx context.Context, gvr schema.GroupVersionResource, ns string) bool {
		return allowFn()
	})
	r3 := setWidgetsResolveForTest(resolve)
	return func() { r3(); r2(); r1() }
}

// depRecordedFor reports whether the L1 dep tracker holds a forward edge from
// the given object tuple to l1Key (a DELETE/UPDATE of the object would touch the
// key). CollectMatchesForTest is the same reverse-index the dispatcher's Record
// call writes to (cache.Deps().Record at the cold-Put site).
func depRecordedFor(l1Key string, gvr schema.GroupVersionResource, ns, name string) bool {
	_, ok := cache.Deps().CollectMatchesForTest(gvr, ns, name)[l1Key]
	return ok
}

// ---------------------------------------------------------------------------
// RESTAction ServeHTTP arms
// ---------------------------------------------------------------------------

// TestH1_RA_RBACDenyIsBeforeCacheGet — arm (a) SECURITY. A cache=on dispatch
// denied by the RBAC gate returns 403 AND never consults the cache handle. RED
// (serve-before-RBAC): if the handler read the cache first, a HIT would 200 a
// forbidden request. We prove the Get is never called by pre-seeding a HIT cell
// under the request's real key and asserting the response is 403, not the cell.
func TestH1_RA_RBACDenyIsBeforeCacheGet(t *testing.T) {
	h1BuildWatcher(t)

	// Pre-seed a HIT cell under alice's REAL derived key so that a
	// serve-before-RBAC bug would return it.
	reqCtx := h1ReqCtx(h1User)
	key, handle, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("setup: expected a live non-empty-BindingUID key; key=%q handle=%v inputs=%+v", key, handle != nil, inputs)
	}
	handle.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"leaked":"cache-hit-body"}`), Inputs: inputs})

	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return false /* DENY */ },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			t.Fatalf("arm (a): resolver must NOT run on an RBAC-denied dispatch")
			return nil, nil
		})
	defer restore()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	RESTAction().ServeHTTP(rec, req)

	if rec.Code != 403 {
		t.Fatalf("arm (a): RBAC-denied dispatch must be 403; got %d body=%s", rec.Code, rec.Body.String())
	}
	if bytes.Contains(rec.Body.Bytes(), []byte("cache-hit-body")) {
		t.Fatalf("arm (a) RED (serve-before-RBAC): a denied request was served the pre-seeded cache cell — the RBAC gate must run BEFORE the cache Get. body=%s", rec.Body.String())
	}
}

// TestH1_RA_L1Hit_ServesCellAndRefreshHeader_NoResolve — arm (b) + (k). An
// allowed request with a warm cell serves entry.RawJSON verbatim + stamps the
// refresh-key/class headers, and NEVER calls the resolver. RED: resolver runs
// on a hit (wasted resolve + potential body divergence).
func TestH1_RA_L1Hit_ServesCellAndRefreshHeader_NoResolve(t *testing.T) {
	h1BuildWatcher(t)
	reqCtx := h1ReqCtx(h1User)
	key, handle, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("setup: expected a live non-empty-BindingUID key; got %q %v %+v", key, handle != nil, inputs)
	}
	warm := []byte(`{"warm":"ra-cell"}` + "\n")
	handle.Put(key, &cache.ResolvedEntry{RawJSON: warm, Inputs: inputs})

	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			t.Fatalf("arm (b) RED: resolver ran on an L1 HIT")
			return nil, nil
		})
	defer restore()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	RESTAction().ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("arm (b): expected 200 on a hit; got %d", rec.Code)
	}
	if !bytes.Equal(rec.Body.Bytes(), warm) {
		t.Fatalf("arm (b): hit must serve entry.RawJSON verbatim; got %q want %q", rec.Body.Bytes(), warm)
	}
	if got := rec.Header().Get(refreshKeyHeader); got != key {
		t.Fatalf("arm (k): hit with a non-empty key must stamp %s=%q; got %q", refreshKeyHeader, key, got)
	}
	if got := rec.Header().Get(refreshClassHeader); got != "restactions" {
		t.Fatalf("arm (b): hit must stamp refresh-class=restactions; got %q", got)
	}
}

// TestH1_RA_Miss_PutsOnceUnderDerivedKey_AndDep — arm (c). A cold miss resolves,
// serves, and Puts EXACTLY ONE entry under the request's DERIVED key + records a
// dep edge. RED (Put-under-wrong-key / no-Put): the served warm cell would not
// be retrievable under the same key a subsequent identical request derives.
func TestH1_RA_Miss_PutsOnceUnderDerivedKey_AndDep(t *testing.T) {
	h1BuildWatcher(t)
	reqCtx := h1ReqCtx(h1User)
	key, handle, _ := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" {
		t.Fatalf("setup: expected a live key/handle")
	}
	if _, ok := handle.Get(key); ok {
		t.Fatalf("setup: the derived key must be cold before the miss")
	}

	resolved := &templatesv1.RESTAction{}
	resolved.SetName(h1RAName)
	resolved.SetNamespace(h1NS)
	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			return resolved, nil
		})
	defer restore()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	RESTAction().ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("arm (c): miss must serve 200; got %d body=%s", rec.Code, rec.Body.String())
	}
	entry, ok := handle.Get(key)
	if !ok {
		t.Fatalf("arm (c) RED: after a cold miss NOTHING was Put under the request's DERIVED key %q — a subsequent identical request would never hit", key)
	}
	// The Put bytes are exactly the served bytes (encode-once, write-once).
	if !bytes.Equal(entry.RawJSON, rec.Body.Bytes()) {
		t.Fatalf("arm (c): the Put'd bytes must equal the served bytes; put=%q served=%q", entry.RawJSON, rec.Body.Bytes())
	}
	// Dep edge recorded: the RA GVR/ns/name self-dep exists for the key.
	if !depRecordedFor(key, h1RAGVR, h1NS, h1RAName) {
		t.Fatalf("arm (c): a cold Put must record the self-dep (DELETE→evict / UPDATE→re-resolve) for key %q", key)
	}
}

// TestH1_RA_WarmEqualsCacheOff — arm (d) cache≡no-cache. The bytes served on a
// warm HIT are byte-identical to the bytes a cache-off resolve-and-encode
// produces from the SAME resolver output. RED: an encode-path divergence
// between the hit-serve and the miss-encode.
func TestH1_RA_WarmEqualsCacheOff(t *testing.T) {
	h1BuildWatcher(t)
	reqCtx := h1ReqCtx(h1User)

	resolved := &templatesv1.RESTAction{}
	resolved.SetName(h1RAName)
	resolved.SetNamespace(h1NS)

	// (1) MISS path: drive ServeHTTP cold → capture the served (and Put) bytes.
	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			return resolved, nil
		})
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	RESTAction().ServeHTTP(rec1, req1)
	restore()
	if rec1.Code != 200 {
		t.Fatalf("arm (d): miss serve must be 200; got %d", rec1.Code)
	}
	missBytes := append([]byte(nil), rec1.Body.Bytes()...)

	// (2) HIT path: a SECOND identical request now hits the cell the miss Put.
	restore2 := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			t.Fatalf("arm (d): the second request must be a HIT (no resolve)")
			return nil, nil
		})
	defer restore2()
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	RESTAction().ServeHTTP(rec2, req2)
	if rec2.Code != 200 {
		t.Fatalf("arm (d): hit serve must be 200; got %d", rec2.Code)
	}
	hitBytes := rec2.Body.Bytes()

	// (3) cache-off reference: encode the SAME resolver output directly.
	ref, err := encodeResolvedJSON(resolved)
	if err != nil {
		t.Fatalf("arm (d): reference encode: %v", err)
	}
	if !bytes.Equal(missBytes, ref) || !bytes.Equal(hitBytes, ref) {
		t.Fatalf("arm (d) RED (cache != no-cache): warm/miss bytes diverge from the cache-off resolve-encode.\nmiss=%q\nhit=%q\nref=%q", missBytes, hitBytes, ref)
	}
}

// TestH1_RA_StageErrorSink_ServesButDoesNotPut — arm (h). The resolver bumps the
// ctx StageErrorSink (a per-item hard error); the body is SERVED 200 but the
// full-TTL Put is DECLINED. RED (Put-regardless-of-sink): the partial cell would
// be pinned for the TTL.
func TestH1_RA_StageErrorSink_ServesButDoesNotPut(t *testing.T) {
	h1BuildWatcher(t)
	reqCtx := h1ReqCtx(h1User)
	key, handle, _ := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" {
		t.Fatalf("setup: live key/handle expected")
	}

	resolved := &templatesv1.RESTAction{}
	resolved.SetName(h1RAName)
	resolved.SetNamespace(h1NS)
	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			cache.StageErrorSinkFromContext(ctx).Bump("apistage", "per-item boom")
			return resolved, nil
		})
	defer restore()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	RESTAction().ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("arm (h): a per-item stage error must still SERVE 200; got %d", rec.Code)
	}
	if _, ok := handle.Get(key); ok {
		t.Fatalf("arm (h) RED (Put-regardless-of-sink): a stage-error result was PERSISTED — the full-TTL Put must be gated on StageErrorSink.Count()==0")
	}
}

// TestH1_RA_ExternalSink_ServesNoPutNoDep_BumpsCounter — arm (i). The resolver
// bumps the ExternalTouchedSink; the body is served 200, the Put + dep-Record
// are declined, and the process-wide external-skipped-put counter bumps. RED:
// caching an external-touched result (no dep edge can ever invalidate it).
func TestH1_RA_ExternalSink_ServesNoPutNoDep_BumpsCounter(t *testing.T) {
	h1BuildWatcher(t)
	reqCtx := h1ReqCtx(h1User)
	key, handle, _ := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" {
		t.Fatalf("setup: live key/handle expected")
	}

	resolved := &templatesv1.RESTAction{}
	resolved.SetName(h1RAName)
	resolved.SetNamespace(h1NS)
	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			cache.ExternalTouchedSinkFromContext(ctx).Bump()
			return resolved, nil
		})
	defer restore()

	before := cache.ExternalSkippedPut()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	RESTAction().ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("arm (i): external-touched must still SERVE 200; got %d", rec.Code)
	}
	if _, ok := handle.Get(key); ok {
		t.Fatalf("arm (i) RED: an external-touched result was cached — external data has no dep edge to invalidate it, the Put must be declined")
	}
	if depRecordedFor(key, h1RAGVR, h1NS, h1RAName) {
		t.Fatalf("arm (i): an external-touched result must NOT record a dep edge")
	}
	if got := cache.ExternalSkippedPut(); got != before+1 {
		t.Fatalf("arm (i): the external-skipped-put counter must bump exactly once; got %d want %d", got, before+1)
	}
}

// TestH1_RA_BothSinks_StageErrorBranchFirst — arm (j). When BOTH sinks are
// bumped, the stage-error branch is taken FIRST (decline-partial), so the
// external-skipped-put counter does NOT bump (the external else-if is not
// reached). RED: an ordering flip would bump the external counter.
func TestH1_RA_BothSinks_StageErrorBranchFirst(t *testing.T) {
	h1BuildWatcher(t)
	reqCtx := h1ReqCtx(h1User)
	key, handle, _ := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" {
		t.Fatalf("setup: live key/handle expected")
	}

	resolved := &templatesv1.RESTAction{}
	resolved.SetName(h1RAName)
	resolved.SetNamespace(h1NS)
	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			cache.StageErrorSinkFromContext(ctx).Bump("apistage", "boom")
			cache.ExternalTouchedSinkFromContext(ctx).Bump()
			return resolved, nil
		})
	defer restore()

	before := cache.ExternalSkippedPut()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	RESTAction().ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("arm (j): both-sinks must still serve 200; got %d", rec.Code)
	}
	if _, ok := handle.Get(key); ok {
		t.Fatalf("arm (j): both-sinks must decline the Put (stage-error branch)")
	}
	if got := cache.ExternalSkippedPut(); got != before {
		t.Fatalf("arm (j) RED: the stage-error branch must be taken FIRST — the external-skipped-put counter must NOT bump when both sinks fire; got %d want %d", got, before)
	}
}

// ---------------------------------------------------------------------------
// Widget ServeHTTP arms
// ---------------------------------------------------------------------------

// TestH1_Widget_ContentHit_GatedBody_ClassWidgetContent — arm (e). A non-RBAC-
// sensitive widget with a warm identity-free content cell serves the GATED body
// under class=widgetContent. RED: serving the ungated shell, or the wrong class.
func TestH1_Widget_ContentHit_GatedBody_ClassWidgetContent(t *testing.T) {
	h1BuildWatcher(t)
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	reqCtx := h1ReqCtx(h1User)

	// A plain (non-RBAC-sensitive) widget: no apiRef, no inline GET ref.
	cr := h1WidgetUnstructured(map[string]any{})
	keyExtras := effectiveKeyExtras(reqCtx, cr.Object, nil)
	contentKey, contentHandle, _ := dispatchWidgetContentKey(reqCtx,
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource, h1NS, h1WName, -1, -1, keyExtras)
	if contentHandle == nil || contentKey == "" {
		t.Fatalf("setup: expected a live widgetContent handle+key")
	}
	// A minimal well-formed envelope gateWidgetEnvelope can serve (status shell
	// with an empty resourcesRefs items list — nothing to narrow, served as-is).
	shell := []byte(`{"status":{"widgetData":{"x":1},"resourcesRefs":{"items":[]}}}`)
	contentHandle.Put(contentKey, &cache.ResolvedEntry{RawJSON: shell, Inputs: nil})

	restore := installWidgetFakes(t, cr, func() bool { return true },
		func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
			t.Fatalf("arm (e) RED: resolver ran despite a widgetContent HIT")
			return nil, nil
		})
	defer restore()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	Widgets().ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("arm (e): content-hit must serve 200; got %d body=%s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(refreshClassHeader); got != "widgetContent" {
		t.Fatalf("arm (e): a content-hit must stamp refresh-class=widgetContent; got %q", got)
	}
	if got := rec.Header().Get(refreshKeyHeader); got != contentKey {
		t.Fatalf("arm (e): a content-hit must stamp the content key; got %q want %q", got, contentKey)
	}
}

// TestH1_Widget_RBACSensitive_SkipsContentLookup — arm (f) SECURITY. An
// RBAC-sensitive widget (apiRef + widgetDataTemplate) must SKIP the shared
// identity-free content cell entirely and fall through to the per-user resolve —
// even when a content cell EXISTS under its content key. RED (read shared
// content cell for RBAC-sensitive widget): the SA-maximal shell would leak.
func TestH1_Widget_RBACSensitive_SkipsContentLookup(t *testing.T) {
	h1BuildWatcher(t)
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	reqCtx := h1ReqCtx(h1User)

	// RBAC-sensitive: apiRef + widgetDataTemplate → isRBACSensitiveApiRefWidget==true.
	cr := h1WidgetUnstructured(map[string]any{
		"apiRef":             map[string]any{"name": "agg-ra", "namespace": h1NS},
		"widgetDataTemplate": []any{map[string]any{"forPath": ".x", "expression": "${ .a }"}},
	})
	if !isRBACSensitiveApiRefWidget(cr.Object) {
		t.Fatalf("setup: the CR must classify RBAC-sensitive")
	}
	// Plant a POISON content cell under its content key — a skip-bug would serve it.
	keyExtras := effectiveKeyExtras(reqCtx, cr.Object, nil)
	contentKey, contentHandle, _ := dispatchWidgetContentKey(reqCtx,
		h1WidgetGVR.Group, h1WidgetGVR.Version, h1WidgetGVR.Resource, h1NS, h1WName, -1, -1, keyExtras)
	if contentHandle == nil || contentKey == "" {
		t.Fatalf("setup: expected a live widgetContent handle+key")
	}
	contentHandle.Put(contentKey, &cache.ResolvedEntry{
		RawJSON: []byte(`{"status":{"widgetData":{"SA_MAXIMAL":"leak"},"resourcesRefs":{"items":[]}}}`),
		Inputs:  nil,
	})

	resolved := h1WidgetUnstructured(map[string]any{})
	_ = unstructured.SetNestedField(resolved.Object, "per-user", "status", "widgetData", "own")
	resolverRan := false
	restore := installWidgetFakes(t, cr, func() bool { return true },
		func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
			resolverRan = true
			return resolved, nil
		})
	defer restore()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	Widgets().ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("arm (f): expected 200; got %d body=%s", rec.Code, rec.Body.String())
	}
	if bytes.Contains(rec.Body.Bytes(), []byte("SA_MAXIMAL")) {
		t.Fatalf("arm (f) RED (read shared content cell for RBAC-sensitive widget): the SA-maximal poison shell leaked — an RBAC-sensitive widget must SKIP the identity-free content lookup. body=%s", rec.Body.String())
	}
	if !resolverRan {
		t.Fatalf("arm (f): an RBAC-sensitive widget must fall through to the per-user resolve (content lookup skipped)")
	}
	// It served under the per-cohort widgets class, NOT widgetContent.
	if got := rec.Header().Get(refreshClassHeader); got == "widgetContent" {
		t.Fatalf("arm (f): an RBAC-sensitive widget must NOT serve under the widgetContent class; got %q", got)
	}
}

// TestH1_Widget_StatusError_ItsCodeNot500 — arm (g). widgets.Resolve returning a
// StatusError-wrapped error emits its Code (403), not a generic 500. RED: the
// dispatcher swallows the StatusError and 500s.
func TestH1_Widget_StatusError_ItsCodeNot500(t *testing.T) {
	h1BuildWatcher(t)
	reqCtx := h1ReqCtx(h1User)
	cr := h1WidgetUnstructured(map[string]any{})

	restore := installWidgetFakes(t, cr, func() bool { return true },
		func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
			return nil, h1ForbiddenStatusError()
		})
	defer restore()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	Widgets().ServeHTTP(rec, req)

	if rec.Code != 403 {
		t.Fatalf("arm (g) RED: a StatusError(403) from widgets.Resolve must emit 403, not a generic 500; got %d body=%s", rec.Code, rec.Body.String())
	}
}

// h1ForbiddenStatusError builds the exact *apierrors.StatusError shape
// widgets.Resolve surfaces on an apiRef 403 — the shape widgets.go's
// errors.As(err, *apierrors.StatusError) must recover Code=403 from.
func h1ForbiddenStatusError() error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Message: "forbidden: cannot get restactions",
		Reason:  metav1.StatusReasonForbidden,
		Code:    403,
	}}
}

// ---------------------------------------------------------------------------
// M12 — SA transport capture: out-of-cluster ⇒ ctx carries NO internal endpoint
// ---------------------------------------------------------------------------

// TestM12_RA_NoInternalTransportOnResolveCtx — Part 1 (#268/#269) permanent guard.
// The RESTAction dispatcher must NEVER put the SA endpoint/rest.Config on the
// per-user resolve ctx (that attach was the SA-serve leak vector; Part 1 removed
// it). The ctx handed to the resolver carries NO WithInternalEndpoint /
// WithInternalRESTConfig. RED: re-adding the attach would put a value on the ctx.
func TestM12_RA_NoInternalTransportOnResolveCtx(t *testing.T) {
	h1BuildWatcher(t)
	reqCtx := h1ReqCtx(h1User)

	// Out-of-cluster the surviving saRC field is nil (snowplowSARC→nil); there is no
	// saEP field any more (Part 1 dropped it). The invariant below holds regardless
	// of saRC — the SA rc is passed as SArc, never onto the ctx.
	h := RESTAction().(*restActionHandler)
	if h.saRC != nil {
		t.Fatalf("M12: out-of-cluster RESTAction() must capture nil saRC; got %v", h.saRC)
	}

	var sawEP, sawRC bool
	resolved := &templatesv1.RESTAction{}
	resolved.SetName(h1RAName)
	resolved.SetNamespace(h1NS)
	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			_, sawEP = cache.InternalEndpointFromContext(ctx)
			_, sawRC = cache.InternalRESTConfigFromContext(ctx)
			return resolved, nil
		})
	defer restore()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	RESTAction().ServeHTTP(rec, req)

	if sawEP || sawRC {
		t.Fatalf("M12 RED (#268/#269): the resolve ctx carried an internal SA endpoint/restconfig (sawEP=%v sawRC=%v) — the attach was removed in Part 1 and must NOT return; the SA rc rides ResolveOptions.SArc only, never the ctx", sawEP, sawRC)
	}
}

// TestM12_Widget_NoInternalTransportOnResolveCtx — the widget twin.
func TestM12_Widget_NoInternalTransportOnResolveCtx(t *testing.T) {
	h1BuildWatcher(t)
	reqCtx := h1ReqCtx(h1User)

	h := Widgets().(*widgetsHandler)
	if h.saRC != nil {
		t.Fatalf("M12: out-of-cluster Widgets() must capture nil saRC; got %v", h.saRC)
	}

	var sawEP, sawRC bool
	restore := installWidgetFakes(t, h1WidgetUnstructured(map[string]any{}), func() bool { return true },
		func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
			_, sawEP = cache.InternalEndpointFromContext(ctx)
			_, sawRC = cache.InternalRESTConfigFromContext(ctx)
			return h1WidgetUnstructured(map[string]any{}), nil
		})
	defer restore()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	Widgets().ServeHTTP(rec, req)

	if sawEP || sawRC {
		t.Fatalf("M12 RED (#268/#269 widget): the resolve ctx carried an internal SA endpoint/restconfig (sawEP=%v sawRC=%v) — the attach must stay removed", sawEP, sawRC)
	}
}

// TestM12_RA_InClusterSaRCSet_StillNoInternalTransportOnResolveCtx — the
// architect's strengthening arm (#268/#269 Part 1). The out-of-cluster M12 tests
// above have saRC==nil, so a nil-guarded endpoint-ONLY re-add
// (`if r.saEP != nil { WithInternalEndpoint }`) would slip past them. This arm
// simulates IN-CLUSTER by constructing the handler with saRC NON-nil (as
// snowplowSARC returns in a pod) and asserts the resolve ctx STILL carries no
// internal transport — because Part 1 deleted the attach entirely, saRC rides
// ResolveOptions.SArc only, never the ctx. RED against any regression that
// re-attaches the SA endpoint/rc to the per-user ctx.
func TestM12_RA_InClusterSaRCSet_StillNoInternalTransportOnResolveCtx(t *testing.T) {
	h1BuildWatcher(t)
	reqCtx := h1ReqCtx(h1User)

	h := &restActionHandler{authnNS: h1NS, saRC: &rest.Config{Host: "https://sa.invalid"}}

	var sawEP, sawRC bool
	resolved := &templatesv1.RESTAction{}
	resolved.SetName(h1RAName)
	resolved.SetNamespace(h1NS)
	restore := installRAFakes(t, h1RAUnstructured(), func() bool { return true },
		func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			_, sawEP = cache.InternalEndpointFromContext(ctx)
			_, sawRC = cache.InternalRESTConfigFromContext(ctx)
			return resolved, nil
		})
	defer restore()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	h.ServeHTTP(rec, req)

	if sawEP || sawRC {
		t.Fatalf("M12 in-cluster RED (#268/#269): saRC set on the handler (in-cluster) must NOT reach the resolve ctx as WithInternalEndpoint/WithInternalRESTConfig — the attach was removed in Part 1 and must NOT return (sawEP=%v sawRC=%v). saRC rides ResolveOptions.SArc only.", sawEP, sawRC)
	}
}

// TestM12_Widget_InClusterSaRCSet_StillNoInternalTransportOnResolveCtx — the widget twin.
func TestM12_Widget_InClusterSaRCSet_StillNoInternalTransportOnResolveCtx(t *testing.T) {
	h1BuildWatcher(t)
	reqCtx := h1ReqCtx(h1User)

	h := &widgetsHandler{authnNS: h1NS, saRC: &rest.Config{Host: "https://sa.invalid"}}

	var sawEP, sawRC bool
	restore := installWidgetFakes(t, h1WidgetUnstructured(map[string]any{}), func() bool { return true },
		func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error) {
			_, sawEP = cache.InternalEndpointFromContext(ctx)
			_, sawRC = cache.InternalRESTConfigFromContext(ctx)
			return h1WidgetUnstructured(map[string]any{}), nil
		})
	defer restore()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
	h.ServeHTTP(rec, req)

	if sawEP || sawRC {
		t.Fatalf("M12 in-cluster RED (#268/#269 widget): saRC set on the handler must NOT reach the resolve ctx (sawEP=%v sawRC=%v) — the attach must stay removed.", sawEP, sawRC)
	}
}
