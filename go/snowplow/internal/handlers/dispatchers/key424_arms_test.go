// key424_arms_test.go — #424 rework arms (reviewer-424 R1 + the Put-path TOCTOU +
// the seed representative's under-serve).
//
//   TestKey424_RefresherRepresentativeDrift — reviewer-424's probe, adopted. The
//       first writer (carol) of a group-only cell later gains a User-subject
//       binding; dave still derives the cell's key. A refresher pass on the
//       cell must NOT re-Put carol's widened view for dave.
//   TestKey424_RestactionsPut_TOCTOU_GrantMidResolve
//   TestKey424_WidgetsPut_TOCTOU_GrantMidResolve — a grant to the requester
//       lands DURING the resolve; the key was minted for the pre-grant class
//       (shared with dave). The Put must be declined; the requester is still
//       served their own (post-grant) body.
//   TestKey424_SeedTerminalPut_DeclinesOnClassDrift — the single seed terminal
//       write refuses a body whose cohort's class moved after the key was minted.
//   TestKey424_SeedRepresentativeBodyParity — a grant reachable only through
//       system:authenticated: the seed's group representative (Username=="")
//       must seed the SAME body a real member resolves fresh under the same key.
//
// The resolver seam is used only where the boundary under test is the Put path
// and the arm needs a hook INSIDE the resolve (the TOCTOU arms); the bodies are
// still derived from the real evaluator for the requesting identity.

package dispatchers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

var k424RBGVR = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}

// k424Grant gives user a User-subject RoleBinding on target-reader in ns x and
// waits until the published snapshot reflects it.
func k424Grant(t *testing.T, dyn *dynamicfake.FakeDynamicClient, a psArm, user string) {
	t.Helper()
	rb := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: psTargetNS, Name: user + "-target", UID: types.UID("uid-rb-k424-" + user)},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: user}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "target-reader"},
	}
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if _, err := dyn.Resource(k424RBGVR).Namespace(psTargetNS).Create(context.Background(),
		&unstructured.Unstructured{Object: m}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("grant: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if k424Can(a, user, "get") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("grant to %s did not reach the published snapshot within 5s", user)
}

func k424Can(a psArm, user, verb string) bool {
	ok, _, _ := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: user, Groups: psGroups(a), Verb: verb,
		Resource: a.target.Resource, Namespace: psTargetNS, Name: psTargetObj,
	})
	return ok
}

// --- R1: refresher representative drift (reviewer-424's probe) --------------

func TestKey424_RefresherRepresentativeDrift(t *testing.T) {
	k423Env(t)
	a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a)
	counters := k423Counters()
	srv := psFakeAPIServer(t, a, counters)
	psSeedClientconfigs(t, srv.URL)
	carolCtx, daveCtx := psUserCtx(a, psCarol), psUserCtx(a, psDave)

	canList := func(user string) bool {
		ok, _, _ := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
			Username: user, Groups: psGroups(a), Verb: "list", Resource: a.target.Resource, Namespace: psTargetNS,
		})
		return ok
	}
	if canList(psCarol) || canList(psDave) {
		t.Fatalf("PRE: carol and dave must both be denied list")
	}
	cKey, _ := k423RAKey(t, carolCtx)
	if dKey, _ := k423RAKey(t, daveCtx); cKey != dKey {
		t.Fatalf("PRE: carol and dave must share one cell")
	}
	cr := k423ListCR(a)
	if rec := psServeCR(t, cr, srv.URL, carolCtx); psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("PRE: carol's own body must not carry the sentinel")
	}
	e, ok := cache.ResolvedCache().Get(cKey)
	if !ok || e.Inputs == nil || e.Inputs.RepresentativeUsername != psCarol {
		t.Fatalf("SETUP: carol's empty LIST body must be cached under K with carol as representative")
	}
	stored := *e.Inputs

	k424Grant(t, dyn, a, psCarol) // carol's set moves; dave stays in K's class
	for i := 0; i < 250 && !canList(psCarol); i++ {
		time.Sleep(20 * time.Millisecond)
	}
	if !canList(psCarol) || canList(psDave) {
		t.Fatalf("PRE: after the grant carol must be allowed and dave denied")
	}
	if dKey2, _ := k423RAKey(t, daveCtx); dKey2 != cKey {
		t.Fatalf("PRE: dave must still derive K")
	}
	if ctl := psServeCR(t, cr, srv.URL, carolCtx); !psHasSentinel(ctl.Body.Bytes()) {
		t.Fatalf("CONTROL: carol's own post-grant resolve must carry the sentinel, else the arm cannot fail")
	}

	// A refresher pass on K: the real resolveAndPopulateL1 + the real restactions
	// resolver; only the CR fetch is supplied.
	restore := setResolveOnceForTest(func(ctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		ctx = cache.WithBackgroundResolve(ctx)
		return resolveRestActionForRefresh(ctx, objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}, in, psAuthnNS)
	})
	defer restore()
	saSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") != "tok-sa" {
			w.WriteHeader(401)
			return
		}
		obj := map[string]any{"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{"name": psTargetObj, "namespace": psTargetNS},
			"data":     map[string]any{"password": psSentinel}}
		b, _ := json.Marshal(map[string]any{"apiVersion": "v1", "kind": "ConfigMapList", "metadata": map[string]any{}, "items": []any{obj}})
		_, _ = w.Write(b)
	}))
	defer saSrv.Close()
	saEP := &endpoints.Endpoint{ServerURL: saSrv.URL, Token: "tok-sa"}
	saRC := &rest.Config{Host: saSrv.URL, BearerToken: "tok-sa"}
	before := driftDeclined424("refresher")
	if err := resolveAndPopulateL1(context.Background(), stored, saEP, saRC); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if e2, _ := cache.ResolvedCache().Get(cKey); e2 != nil && psHasSentinel(e2.RawJSON) {
		t.Fatalf("REPRESENTATIVE-DRIFT: the refresher re-Put carol's post-grant view under K")
	}
	dials := counters[psDave].Load()
	if rec := psServeCR(t, cr, srv.URL, daveCtx); psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("REPRESENTATIVE-DRIFT LEAK: dave (denied list) was served carol's post-grant data from K "+
			"(dials as dave=%d); body=%s", counters[psDave].Load()-dials, psTrunc(rec.Body.String(), 300))
	}
	expectDriftCounted424(t, "refresher", before)
}

// --- TOCTOU on the customer Put paths ----------------------------------------

// k424GrantingSeam is a resolver seam that, the FIRST time it runs for
// `grantee`, grants them the target mid-resolve, then derives the body from the
// REAL evaluator for the requesting identity.
func k424Body(t *testing.T, ctx context.Context, a psArm) string {
	ui, err := xcontext.UserInfo(ctx)
	if err != nil {
		t.Fatalf("seam: no identity: %v", err)
	}
	ok, _, err := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
		Username: ui.Username, Groups: ui.Groups, Verb: "get",
		Resource: a.target.Resource, Namespace: psTargetNS, Name: psTargetObj,
	})
	if err != nil {
		t.Fatalf("seam: %v", err)
	}
	if ok {
		return psSentinel
	}
	return "denied"
}

func TestKey424_RestactionsPut_TOCTOU_GrantMidResolve(t *testing.T) {
	a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a)
	carolCtx, daveCtx := psUserCtx(a, psCarol), psUserCtx(a, psDave)
	cKey, _ := k423RAKey(t, carolCtx)
	if dKey, _ := k423RAKey(t, daveCtx); cKey != dKey {
		t.Fatalf("PRE: carol and dave must share one cell before the grant")
	}
	var granted atomic.Bool
	seam := func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
		if ui, _ := xcontext.UserInfo(ctx); ui.Username == psCarol && granted.CompareAndSwap(false, true) {
			k424Grant(t, dyn, a, psCarol) // lands DURING carol's resolve
		}
		raw, _ := json.Marshal(map[string]any{"password": k424Body(t, ctx, a)})
		out := opts.In.DeepCopy()
		out.Status = &runtime.RawExtension{Raw: raw}
		return out, nil
	}
	serve := func(ctx context.Context) *httptest.ResponseRecorder {
		cr := psRACR(a)
		r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
			return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
		})
		r2 := setRestactionsResolveForTest(seam)
		defer func() { r2(); r1() }()
		rec := httptest.NewRecorder()
		h := &restActionHandler{authnNS: psAuthnNS, saRC: &rest.Config{}}
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(ctx))
		return rec
	}
	before := driftDeclined424("restactions")
	if !psHasSentinel(serve(carolCtx).Body.Bytes()) {
		t.Fatalf("SETUP: carol's own (post-grant) body must carry the sentinel")
	}
	if rec := serve(daveCtx); psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("TOCTOU LEAK (restactions): a grant landing during carol's resolve put her post-grant body "+
			"under the pre-grant key K that dave still derives; dave's body=%s", psTrunc(rec.Body.String(), 300))
	}
	expectDriftCounted424(t, "restactions", before)
}

func TestKey424_WidgetsPut_TOCTOU_GrantMidResolve(t *testing.T) {
	a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a, k423WidgetExtras(a)...)
	carolCtx, daveCtx := psUserCtx(a, psCarol), psUserCtx(a, psDave)
	cr := h1WidgetUnstructured(map[string]any{})
	wKey := func(ctx context.Context) string {
		k, _, _ := dispatchCacheLookupKey(ctx, "widgets", h1WidgetGVR.Group, h1WidgetGVR.Version,
			h1WidgetGVR.Resource, h1NS, h1WName, -1, -1, effectiveKeyExtras(ctx, cr.Object, nil))
		return k
	}
	if wKey(carolCtx) != wKey(daveCtx) {
		t.Fatalf("PRE: carol and dave must share one widgets cell before the grant")
	}
	var granted atomic.Bool
	seam := func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		if ui, _ := xcontext.UserInfo(ctx); ui.Username == psCarol && granted.CompareAndSwap(false, true) {
			k424Grant(t, dyn, a, psCarol)
		}
		out := h1WidgetUnstructured(map[string]any{})
		_ = unstructured.SetNestedField(out.Object, k424Body(t, ctx, a), "status", "widgetData", "password")
		return out, nil
	}
	serve := func(ctx context.Context) *httptest.ResponseRecorder {
		r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
			return objects.Result{GVR: h1WidgetGVR, Unstructured: cr.DeepCopy()}
		})
		r2 := setWidgetsResolveForTest(seam)
		defer func() { r2(); r1() }()
		rec := httptest.NewRecorder()
		h := &widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{}}
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(ctx))
		return rec
	}
	before := driftDeclined424("widgets")
	if !psHasSentinel(serve(carolCtx).Body.Bytes()) {
		t.Fatalf("SETUP: carol's own (post-grant) widget body must carry the sentinel")
	}
	if rec := serve(daveCtx); psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("TOCTOU LEAK (widgets): carol's post-grant widget body was written under the pre-grant key; "+
			"dave's body=%s", psTrunc(rec.Body.String(), 300))
	}
	expectDriftCounted424(t, "widgets", before)
}

// --- seed: the single terminal write -----------------------------------------

func TestKey424_SeedTerminalPut_DeclinesOnClassDrift(t *testing.T) {
	a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a)
	// The seed ctx for a concrete-identity cohort (a User-subject representative).
	cohort := withCohortSeedContext(context.Background(), seedTarget{Username: psCarol, Groups: []string{psGroup}},
		endpoints.Endpoint{}, nil)
	key, handle, inputs := k424SeedKey(t, cohort)
	k424Grant(t, dyn, a, psCarol) // the class moves after the mint, before the terminal write
	ok := seedTerminalPut(cohort, handle, key, &cache.ResolvedEntry{RawJSON: []byte(`{"seeded":true}`), Inputs: inputs},
		seedTerminalGuardFor(seedModeBoot, handle, key))
	if ok {
		t.Fatalf("SEED TOCTOU: seedTerminalPut wrote a body under a key minted for the cohort's PRE-grant class")
	}
	if _, hit := handle.Get(key); hit {
		t.Fatalf("SEED TOCTOU: a cell exists under the stale key")
	}
	// Control: with no class change the same write lands.
	key2, handle2, inputs2 := k424SeedKey(t, cohort)
	if !seedTerminalPut(cohort, handle2, key2, &cache.ResolvedEntry{RawJSON: []byte(`{"seeded":true}`), Inputs: inputs2},
		seedTerminalGuardFor(seedModeBoot, handle2, key2)) {
		t.Fatalf("CONTROL: an unchanged class must still be written (the guard is over-broad)")
	}
}

func k424SeedKey(t *testing.T, ctx context.Context) (string, cacheHandle, *cache.ResolvedKeyInputs) {
	t.Helper()
	k, h, in := dispatchCacheLookupKey(ctx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, psRAName, -1, -1, nil)
	if h == nil || in == nil || in.BindingUID == "" {
		t.Fatalf("PRE: seed key needs a live handle and a BindingUID")
	}
	return k, h, in
}

// --- seed representative body parity ----------------------------------------

// k424AuthenticatedReaders: the target is readable ONLY through
// system:authenticated (a cluster-wide grant every authenticated user holds).
func k424AuthenticatedReaders(a psArm) []runtime.Object {
	return []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "authn-target-reader"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{a.target.Resource}}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "authn-target", UID: "uid-crb-authn-target"},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "system:authenticated"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "authn-target-reader"},
		},
	}
}

// Two step shapes: a GET (a denied GET is a stage error → the pre-fix seed
// declined its Put → a COLD cell) and a LIST (a denied LIST item is filtered,
// not an error → the pre-fix seed Put a NARROWER body under the member's key —
// wrong content in a warm cell).
func TestKey424_SeedRepresentativeBodyParity(t *testing.T) {
	for _, step := range []string{"get", "list"} {
		step := step
		t.Run(step, func(t *testing.T) { k424SeedParity(t, step) })
	}
}

func k424SeedParity(t *testing.T, step string) {
	k423Env(t)
	a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
	psBuildWatcher(t, a, k424AuthenticatedReaders(a)...)
	counters := k423Counters()
	srv := psFakeAPIServer(t, a, counters)
	psSeedClientconfigs(t, srv.URL)
	carolCtx := psUserCtx(a, psCarol)
	if !k424Can(a, psCarol, "get") {
		t.Fatalf("PRE: carol (authenticated) must be allowed through system:authenticated")
	}

	// The seed: the group representative {Username:"", Groups:[portal]} through the
	// production seed context + the REAL seed primitive (real resolver, real Put).
	origGet := seedObjectsGetFn
	t.Cleanup(func() { seedObjectsGetFn = origGet })
	cr := psRACR(a)
	if step == "list" {
		cr = k423ListCR(a)
	}
	seedObjectsGetFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
	}
	cohort := withCohortSeedContext(context.Background(), seedTarget{Username: "", Groups: []string{psGroup}},
		endpoints.Endpoint{ServerURL: srv.URL}, &rest.Config{Host: srv.URL})
	ref := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: psRAName, Namespace: h1NS},
		APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version, Resource: h1RAGVR.Resource,
	}
	if err := seedOneRestaction(cohort, "cohort-k424", ref, psAuthnNS, seedModeBoot); err != nil {
		t.Fatalf("seedOneRestaction: %v", err)
	}
	key, _ := k423RAKey(t, carolCtx)
	seeded, ok := cache.ResolvedCache().Get(key)
	if !ok {
		t.Fatalf("SEED BODY PARITY: the seed did not populate the cell carol derives (cold first navigation)")
	}
	if !psHasSentinel(seeded.RawJSON) {
		t.Fatalf("SEED UNDER-SERVE: the seeded body lacks a grant every member holds through system:authenticated "+
			"(the representative resolved as unauthenticated); seeded=%s", psTrunc(string(seeded.RawJSON), 300))
	}
	seededBody := append([]byte(nil), seeded.RawJSON...)

	// A member's FRESH body for the same class must be byte-identical.
	cache.ResetResolvedCacheForTest()
	fresh := psServeCR(t, cr, srv.URL, carolCtx)
	if !bytes.Equal(bytes.TrimSpace(fresh.Body.Bytes()), bytes.TrimSpace(seededBody)) {
		t.Fatalf("SEED BODY PARITY: seeded body != member's fresh body under the same key\n seeded=%s\n fresh =%s",
			psTrunc(string(seededBody), 300), psTrunc(fresh.Body.String(), 300))
	}
}

// --- RBACSubGen-only drift: the binding set is UNCHANGED at Put time ----------

// k424Revoke deletes the grant k424Grant made and waits until the snapshot no
// longer reflects it.
func k424Revoke(t *testing.T, dyn *dynamicfake.FakeDynamicClient, a psArm, user string) {
	t.Helper()
	if err := dyn.Resource(k424RBGVR).Namespace(psTargetNS).Delete(context.Background(), user+"-target", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !k424Can(a, user, "get") {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("revoke of %s did not reach the published snapshot within 5s", user)
}

// k424SubGenSettled waits until user's RBACSubGen has moved off `from` (the
// deferred bumps are flushed on the snapshot publish that follows the event).
func k424SubGenSettled(t *testing.T, user string, groups []string, from uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cache.RBACSubGenForSubject(user, rbac.WithAuthenticatedGroup(groups)) != from {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("RBACSubGen for %s never moved off %d", user, from)
}

// TestKey424_RestactionsPut_SubGenOnlyDrift_GrantThenRevokeMidResolve — the ABA
// case the binding-set limb CANNOT see. During carol's resolve she is granted
// the target, the step reads it (sentinel), and the grant is revoked again
// before the Put. At Put time her binding set equals the one the key was minted
// for (the digest limb says "unchanged"), but her body carries data from the
// transient grant. Only the monotone RBACSubGen limb sees that her RBAC moved.
// dave — same class, never granted — must not be served that body.
//
// Mutation check: dropping the rbac_subgen limb from identityClassDrift makes
// this arm RED (dave is served the sentinel).
func TestKey424_RestactionsPut_SubGenOnlyDrift_GrantThenRevokeMidResolve(t *testing.T) {
	a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a)
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR, a.target}) // activates the sub-gen deltas
	carolCtx, daveCtx := psUserCtx(a, psCarol), psUserCtx(a, psDave)
	cKey, cIn := k423RAKey(t, carolCtx)
	if dKey, _ := k423RAKey(t, daveCtx); cKey != dKey {
		t.Fatalf("PRE: carol and dave must share one cell")
	}
	var armed atomic.Bool
	seam := func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
		body := k424Body(t, ctx, a)
		if ui, _ := xcontext.UserInfo(ctx); ui.Username == psCarol && armed.CompareAndSwap(false, true) {
			sg0 := cache.RBACSubGenForSubject(psCarol, rbac.WithAuthenticatedGroup(psGroups(a)))
			k424Grant(t, dyn, a, psCarol)
			body = k424Body(t, ctx, a) // read under the transient grant
			k424Revoke(t, dyn, a, psCarol)
			k424SubGenSettled(t, psCarol, psGroups(a), sg0)
		}
		raw, _ := json.Marshal(map[string]any{"password": body})
		out := opts.In.DeepCopy()
		out.Status = &runtime.RawExtension{Raw: raw}
		return out, nil
	}
	serve := func(ctx context.Context) *httptest.ResponseRecorder {
		cr := psRACR(a)
		r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
			return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
		})
		r2 := setRestactionsResolveForTest(seam)
		defer func() { r2(); r1() }()
		rec := httptest.NewRecorder()
		h := &restActionHandler{authnNS: psAuthnNS, saRC: &rest.Config{}}
		h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(ctx))
		return rec
	}
	before := identityClassDriftDeclinedForTest("restactions", "rbac_subgen")
	if !psHasSentinel(serve(carolCtx).Body.Bytes()) {
		t.Fatalf("SETUP: carol's body must carry the transient grant's sentinel")
	}
	// PRE (the arm's discriminating shape): after the grant+revoke carol's binding
	// set is back to the minted one — so ONLY the sub-gen limb can decline.
	if got := rbac.SubjectBindingSetDigest(psCarol, psGroups(a)); got != cIn.SubjectBindingSet {
		t.Fatalf("PRE: carol's binding set must be unchanged at Put time (digest moved), else the arm is not sub-gen-only")
	}
	if rec := serve(daveCtx); psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("SUBGEN-ONLY TOCTOU LEAK: a grant+revoke inside carol's resolve left her binding set unchanged, and "+
			"her transient-grant body was written under K that dave derives; dave's body=%s", psTrunc(rec.Body.String(), 300))
	}
	if identityClassDriftDeclinedForTest("restactions", "rbac_subgen") <= before {
		t.Fatalf("the decline must have come from the rbac_subgen limb")
	}
}

// TestKey424_RefresherSubGenOnlyDrift_RoleEditMidResolve — a ROLE edit (no
// binding changes at all) referenced by the representative's binding lands
// during the refresher's re-resolve. The binding set is unchanged; RBACSubGen
// moves. The re-Put under the carried key must be declined by the sub-gen limb
// (the post-edit body is not the class the carried key names).
//
// Mutation check: dropping the rbac_subgen limb makes this arm RED (the
// post-edit body is re-Put under K).
func TestKey424_RefresherSubGenOnlyDrift_RoleEditMidResolve(t *testing.T) {
	a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a, k423PortalReaders(a)...) // Group:portal → target-reader in ns x
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR, a.target})
	carolCtx := psUserCtx(a, psCarol)
	key, in := k423RAKey(t, carolCtx)
	in.RepresentativeUsername, in.RepresentativeGroups = psCarol, psGroups(a)
	cache.ResolvedCache().Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"password":"pre-edit"}`), Inputs: in})

	rGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
	var armed atomic.Bool
	restore := setResolveOnceForTest(func(ctx context.Context, _ cache.ResolvedKeyInputs) ([]byte, error) {
		if armed.CompareAndSwap(false, true) {
			sg0 := cache.RBACSubGenForSubject(psCarol, rbac.WithAuthenticatedGroup(psGroups(a)))
			u, err := dyn.Resource(rGVR).Namespace(psTargetNS).Get(context.Background(), "target-reader", metav1.GetOptions{})
			if err != nil {
				t.Fatalf("get role: %v", err)
			}
			_ = unstructured.SetNestedSlice(u.Object, []any{map[string]any{
				"verbs": []any{"get"}, "apiGroups": []any{""}, "resources": []any{"pods"}, // target REVOKED via the role
			}}, "rules")
			if _, err := dyn.Resource(rGVR).Namespace(psTargetNS).Update(context.Background(), u, metav1.UpdateOptions{}); err != nil {
				t.Fatalf("update role: %v", err)
			}
			k424SubGenSettled(t, psCarol, psGroups(a), sg0)
		}
		return []byte(`{"password":"post-edit"}`), nil
	})
	defer restore()

	if got := rbac.SubjectBindingSetDigest(psCarol, psGroups(a)); got != in.SubjectBindingSet {
		t.Fatalf("PRE: a role edit must not change the binding set")
	}
	before := identityClassDriftDeclinedForTest("refresher", "rbac_subgen")
	if err := resolveAndPopulateL1(context.Background(), *in, nil, nil); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if rbac.SubjectBindingSetDigest(psCarol, psGroups(a)) != in.SubjectBindingSet {
		t.Fatalf("PRE: the binding set must still be unchanged after the edit (sub-gen-only drift)")
	}
	if e, ok := cache.ResolvedCache().Get(key); ok && strings.Contains(string(e.RawJSON), "post-edit") {
		t.Fatalf("SUBGEN-ONLY DRIFT: the refresher re-Put a post-role-edit body under the pre-edit key")
	}
	if identityClassDriftDeclinedForTest("refresher", "rbac_subgen") <= before {
		t.Fatalf("the decline must have come from the rbac_subgen limb")
	}
}

var _ = jwtutil.UserInfo{}
