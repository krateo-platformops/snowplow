// issue375_dep_gen_guard_test.go — #375 dep-generation guard: the dispatcher-level
// REAL-PATH arms.
//
// THE REAL-PATHS NO-NIL-SINK ARM is the complement of arm-12. arm-12 proves that a nil
// sink FIRES the detector. This arm proves no production resolve path PRODUCES one.
// It drives the real entry points (content prewarm at boot, a keepwarm re-cycle, the
// refresher's resolveAndPopulateL1, and a customer /call through RESTAction().ServeHTTP)
// with the test-build unguarded hook installed as a failure. Every accepted
// gen-guarded Put on those paths must carry a dep-gen sink, so unguarded_put_total
// stays 0. A drift at high frequency becomes a CI failure, not a production alarm.
//
// arm-7 (boot seed): the seed resolve installs the sink (WithL1KeyContext at
// phase1_pip_seed.go seedOneRestaction / seedOneWidget). The gen-guarded Puts nested in
// the seed resolve are therefore guarded, and a dep churn inside it remarks. The seed's
// own TERMINAL Put stays a plain Put for seedModeBoot (pre-readyz, #189/#323-exempt).
// Since #394 (PR #404) the post-readyz modes (keepwarm, gvr-discovered) write it with
// PutIfGen under resCtx, so it is #375-guarded too; that composition is pinned by
// TestIssue375x394_KeepwarmSeedTerminalPut_ComposesWithDepGenGuard.

package dispatchers

import (
	"context"
	"net/http/httptest"
	"sync"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// failOnUnguarded installs the #375 test-build enforcement: any accepted gen-guarded
// Put on a nil sink fails the test, naming the key.
func failOnUnguarded(t *testing.T) *[]string {
	t.Helper()
	var mu sync.Mutex
	var keys []string
	cache.ResetUnguardedPutTotalForTest()
	t.Cleanup(cache.SetUnguardedPutHookForTest(func(k string) {
		mu.Lock()
		keys = append(keys, k)
		mu.Unlock()
		t.Errorf("#375 REAL-PATH RED: an accepted gen-guarded Put of %q carried NO dep-gen sink — a production "+
			"resolve entry outside WithL1KeyContext / WithDepGenSink (unguarded_put_total drift)", k)
	}))
	return &keys
}

func prewarmCtx375(rw *cache.ResourceWatcher) (context.Context, endpoints.Endpoint, *rest.Config) {
	rc := &rest.Config{Host: "http://127.0.0.1:1"}
	saEP := endpoints.Endpoint{ServerURL: "http://127.0.0.1:1"}
	rctx := cache.WithApistagePrewarm(xcontext.BuildContext(context.Background(),
		xcontext.WithUserConfig(saEP),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: w250SAUser})))
	rctx = cache.WithServeWatcher(rctx, rw)
	rctx = cache.WithInternalEndpoint(rctx, &saEP)
	rctx = cache.WithInternalRESTConfig(rctx, rc)
	return rctx, saEP, rc
}

// Content prewarm (boot) → keepwarm re-cycle → refresher re-resolve of every warmed
// cell: zero nil-sink Puts. Also re-checks #250 after the standalone prewarm sink was
// added. The sink records deps but anchors NO edge, so no phantom anchor appears.
// NEUTER: remove both the (A) content child sink and the standalone prewarm sink → the
// content Puts run on the bare prewarm ctx (nil sink) → RED.
func TestIssue375_RealPaths_NoNilSink_PrewarmKeepwarmRefresher(t *testing.T) {
	rw := new250Watcher(t)
	unguarded := failOnUnguarded(t)
	store := cache.ResolvedCache()
	rctx, saEP, rc := prewarmCtx375(rw)
	ref := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Namespace: w250NS, Name: w250RAName},
		Resource:   "restactions",
		APIVersion: "templates.krateo.io/v1",
	}

	// Boot content prewarm.
	if _, err := prewarmOneRESTAction(rctx, ref, w250NS); err != nil {
		t.Fatalf("prewarmOneRESTAction: %v", err)
	}
	puts := put250Keys(store)
	if len(puts) == 0 {
		t.Fatalf("setup: the boot prewarm warmed nothing — the arm would be vacuous")
	}
	if bad := phantomAnchors250(store); len(bad) != 0 {
		t.Fatalf("#375×#250: the standalone prewarm sink must not anchor a phantom edge; anchors=%v", bad)
	}

	// Keepwarm cycle: the cells aged out and are re-warmed through the SAME entry.
	for k := range puts {
		store.DeleteForTest(k)
	}
	if _, err := prewarmOneRESTAction(rctx, ref, w250NS); err != nil {
		t.Fatalf("keepwarm prewarmOneRESTAction: %v", err)
	}
	if len(put250Keys(store)) == 0 {
		t.Fatalf("setup: the keepwarm cycle re-warmed nothing")
	}

	// Refresher re-resolve of every warmed cell (the real resolveAndPopulateL1).
	refreshed := 0
	for k := range put250Keys(store) {
		e, ok := store.Get(k)
		if !ok || e.Inputs == nil {
			continue
		}
		if err := resolveAndPopulateL1(context.Background(), *e.Inputs, &saEP, rc); err != nil {
			t.Fatalf("resolveAndPopulateL1(%s): %v", k, err)
		}
		refreshed++
	}
	if refreshed == 0 {
		t.Fatalf("setup: no warmed cell carried Inputs — the refresher leg was not exercised")
	}
	if n := cache.UnguardedPutTotal(); n != 0 || len(*unguarded) != 0 {
		t.Fatalf("#375 REAL-PATH RED: unguarded_put_total=%d (keys %v) across boot prewarm + keepwarm + refresher", n, *unguarded)
	}
}

// A customer /call (the real restActionHandler) Puts its cell under a sink. NEUTER:
// drop WithDepGenSink from WithL1KeyContext → nil sink at restactions.go's PutIfGen → RED.
func TestIssue375_RealPaths_NoNilSink_CustomerDispatch(t *testing.T) {
	h1BuildWatcher(t)
	unguarded := failOnUnguarded(t)
	reqCtx := h1ReqCtx(h1User)
	key, handle, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: a live cacheable key is required; key=%q inputs=%+v", key, inputs)
	}
	resolved := &templatesv1.RESTAction{}
	resolved.SetName(h1RAName)
	resolved.SetNamespace(h1NS)
	var resolveCtx context.Context
	restore := installRAFakes(t, a1RAUnstructured(false), func() bool { return true },
		func(ctx context.Context, _ restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
			resolveCtx = ctx
			return resolved, nil
		})
	defer restore()
	rec := httptest.NewRecorder()
	RESTAction().ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx))
	if rec.Code != 200 {
		t.Fatalf("dispatch: want 200, got %d %s", rec.Code, rec.Body.String())
	}
	if _, ok := handle.Get(key); !ok {
		t.Fatalf("setup: the customer dispatch did not Put its cell — the arm would be vacuous")
	}
	if _, _, ok := cache.DepGenSinkForTest(resolveCtx); !ok {
		t.Fatalf("#375 REAL-PATH RED: the customer resolve ctx carries no dep-gen sink")
	}
	if n := cache.UnguardedPutTotal(); n != 0 || len(*unguarded) != 0 {
		t.Fatalf("#375 REAL-PATH RED: customer dispatch produced unguarded_put_total=%d (keys %v)", n, *unguarded)
	}
}

// arm-7 — the BOOT SEED resolve is guarded: seedOneRestaction (real fetch → convert →
// gates → resolve tail) hands its resolve ctx a dep-gen sink whose startSeq precedes the
// resolve. A dep that churns inside the seed resolve PUT-THEN-REMARKs the gen-guarded Put
// nested in it ("moved", never nil_sink). NEUTER: drop WithDepGenSink from
// WithL1KeyContext → nil sink → RED.
func TestIssue375_Arm7_BootSeedResolve_IsGuarded(t *testing.T) {
	const user = "userGranted"
	buildGrantedRestactionWatcher(t, user)
	unguarded := failOnUnguarded(t)
	reasons := map[string]int{}
	var mu sync.Mutex
	t.Cleanup(cache.SetDepGenRemarkObserverForTest(func(_ string, r string) {
		mu.Lock()
		reasons[r]++
		mu.Unlock()
	}))
	ref := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: "uaf-seed-ra", Namespace: "krateo-system"},
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}
	origPut, origGet := seedRestactionResolveAndPutFn, seedObjectsGetFn
	t.Cleanup(func() { seedRestactionResolveAndPutFn, seedObjectsGetFn = origPut, origGet })
	seedObjectsGetFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
		return a1SeedFetchedRestaction(false)
	}
	dep := schema.GroupVersionResource{Group: "seed.example.io", Version: "v1", Resource: "things"}
	var sawSink bool
	seedRestactionResolveAndPutFn = func(
		_, resCtx context.Context, _ *templatesv1.RESTAction, _ templatesv1.ObjectReference,
		_, key string, _ cacheHandle, _ *cache.ResolvedKeyInputs, _ objects.Result,
		_ *cache.StageErrorSink, _ *cache.ExternalTouchedSink,
	) error {
		start, _, ok := cache.DepGenSinkForTest(resCtx)
		sawSink = ok && start <= cache.DepEventSeqForTest()
		// A gen-guarded Put NESTED in the seed resolve (a content cell), with a dep read
		// and churned inside the resolve.
		nested := "seed-nested-content"
		gen0 := cache.ResolvedCache().CaptureGen(nested)
		cache.Deps().Record(resCtx, key, dep, "ns", "x")
		cache.Deps().OnUpdate(dep, "ns", "x")
		cache.ResolvedCache().PutIfGen(resCtx, nested, &cache.ResolvedEntry{RawJSON: []byte(`{}`)}, gen0)
		return nil
	}
	if err := seedOneRestaction(seedCohortCtx(user), "cohort-granted", ref, "krateo-system", seedModeBoot); err != nil {
		t.Fatalf("seedOneRestaction: %v", err)
	}
	if !sawSink {
		t.Fatalf("#375 arm-7 RED: the boot seed resolve ctx carries no dep-gen sink (or its startSeq is not at entry)")
	}
	if reasons["moved"] != 1 || reasons["nil_sink"] != 0 || len(*unguarded) != 0 {
		t.Fatalf("#375 arm-7 RED: a dep churned inside the seed resolve must remark its nested gen-guarded Put once "+
			"as 'moved' — got reasons=%v unguarded=%v", reasons, *unguarded)
	}
}
