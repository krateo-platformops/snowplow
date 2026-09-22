// debug_store_falsifier_test.go — snowplow#237 deliverable A.
//
// WHAT THIS FILE IS FOR
//
// #237 spent two sessions failing to answer one question: is the INFORMER
// STORE stale, or is the L1 cell stale? Both produce a byte-identical wrong
// widget through /call, and every debug surface snowplow had was DOWNSTREAM of
// the store (/debug/apistage reports L1 entry metadata; /debug/reconcile probes
// L1 against the informer's own indexer, which is the layer under suspicion).
//
// The four arms below are one fixture in two states that differ ONLY in what
// the informer store holds:
//
//	arm 1 — store holds the widget at resourceVersion 1; L1 holds the rv=1
//	        rendering. (store stale, L1 agrees with it)
//	arm 2 — store holds the widget at resourceVersion 2; L1 holds the SAME
//	        rv=1 rendering. (store fresh, L1 stale)
//
// L1 IS HELD CONSTANT ACROSS THE ARMS — same key, same body, same entry count,
// same class, same Inputs. That is load-bearing and it is the PM gate's
// condition A-1: an earlier draft of this falsifier varied L1 occupancy between
// the arms (arm 1 empty, arm 2 with one entry), which made /debug/apistage and
// /debug/reconcile differ on ENTRY COUNT before staleness was ever asked about.
// The blindness arm would then have been red on a fixture artefact rather than
// on the blindness it claims to prove.
//
//	arm 3 — GREEN TODAY AND MUST STAY GREEN. /debug/apistage and
//	        /debug/reconcile are byte-equal across arms 1 and 2. This is the
//	        standing, executable record that no pre-existing surface can tell a
//	        stale store from a stale cell.
//	arm 4 — the discriminator. /debug/store reports resourceVersion "1" in arm
//	        1 and "2" in arm 2. RED today (the route does not exist); green
//	        after A — and green WHILE ARM 3 STAYS GREEN, which is what makes
//	        the RED attributable to the missing surface rather than to a
//	        fixture difference.
//
// The store side is driven for real: a real ResourceWatcher over a real
// SharedIndexInformer whose reflector LISTs a fake apiserver seeded at the
// arm's resourceVersion. Nothing hand-installs a crossed indexer. The L1 cell
// is a held-constant prop (its key derivation is not what these arms
// discriminate), which is why it is written with a literal key rather than a
// derived one — the two arms must present the SAME key.

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const (
	storeArmNS   = "krateo-system"
	storeArmName = "portal-builder-page-header"
	// storeArmL1Key is a literal because the two arms MUST present the same
	// L1 key: what they discriminate is the store, and an identical cell on
	// both sides is the precondition that makes arm 3 meaningful.
	storeArmL1Key = "store-falsifier-fixed-key"
	// storeArmL1Body is the rv=1 RENDERING, held in L1 by BOTH arms. In arm 2
	// it is a genuinely stale cell over a correct store.
	storeArmL1Body = `{"kind":"PageHeader","widgetData":{"title":"Portal builder"}}`
)

// storeArmGVR is the #237 coordinate class (pageheaders), not a synthetic one.
var storeArmGVR = schema.GroupVersionResource{
	Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "pageheaders",
}

// seedStoreArm builds one arm: a cache-on ResourceWatcher whose informer has
// synced the pageheader at storeRV from a fake apiserver, plus the SAME single
// L1 cell in every arm. Everything is torn down by t.Cleanup, so two arms run
// back to back in one process without leaking state into each other.
func seedStoreArm(t *testing.T, storeRV string) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		storeArmGVR: "PageHeaderList",
	}
	seed := []runtime.Object{&unstructured.Unstructured{Object: map[string]any{
		"apiVersion": storeArmGVR.Group + "/" + storeArmGVR.Version,
		"kind":       "PageHeader",
		"metadata": map[string]any{
			"name":              storeArmName,
			"namespace":         storeArmNS,
			"uid":               "11111111-2222-3333-4444-555555555555",
			"resourceVersion":   storeRV,
			"generation":        int64(1),
			"creationTimestamp": "2026-09-22T10:00:00Z",
		},
		"spec": map[string]any{"widgetData": map[string]any{"title": "Portal builder"}},
	}}}

	// Reset the dep tracker BEFORE the watcher is built: the informer handlers
	// bind the dep-watch singleton at registration and a reset afterwards
	// orphans a worker goroutine that keeps reading Deps().
	cache.ResetDepsForTest()
	cache.ResetResolvedCacheForTest()
	t.Cleanup(func() {
		cache.ResetDepsForTest()
		cache.ResetResolvedCacheForTest()
	})

	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer syncCancel()
	if err := rw.WaitForCacheSync(syncCtx, 10*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	_, syncCh := rw.EnsureResourceType(storeArmGVR)
	select {
	case <-syncCh:
	case <-time.After(10 * time.Second):
		rw.Stop()
		wcancel()
		t.Fatalf("informer for %s never synced", storeArmGVR)
	}

	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
	})

	// The L1 cell — IDENTICAL in every arm.
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("ResolvedCache() nil — RESOLVED_CACHE_ENABLED not honoured")
	}
	cache.Deps().SetStore(store)
	store.Put(storeArmL1Key, &cache.ResolvedEntry{
		RawJSON: []byte(storeArmL1Body),
		Inputs: &cache.ResolvedKeyInputs{
			CacheEntryClass: cache.CacheEntryClassWidgetContent,
			Group:           storeArmGVR.Group,
			Version:         storeArmGVR.Version,
			Resource:        storeArmGVR.Resource,
			Namespace:       storeArmNS,
			Name:            storeArmName,
		},
	})
	// The self edge, so /debug/reconcile PROBES this coordinate rather than
	// counting it as skippedNoEdge — arm 3 asserts on probed, so the edge has
	// to be there or the arm compares two zeros.
	cache.Deps().Record(storeArmL1Key, storeArmGVR, storeArmNS, storeArmName)
	t.Cleanup(func() { store.DeleteForTest(storeArmL1Key) })
}

// storeArmIndexerRV reads the resourceVersion the informer store actually
// holds, straight off the public list accessor. It is the fixture's own
// precondition check: an arm that did not get the object it seeded into the
// store would make every later assertion meaningless.
func storeArmIndexerRV(t *testing.T) string {
	t.Helper()
	objs := cache.Global().ListObjects(storeArmGVR, storeArmNS)
	for _, o := range objs {
		if o.GetName() == storeArmName {
			return o.GetResourceVersion()
		}
	}
	t.Fatalf("fixture: the informer store does not hold %s/%s at all (%d objects) — "+
		"the arm never established its precondition", storeArmNS, storeArmName, len(objs))
	return ""
}

// normaliseDebugBody decodes a debug body and zeroes the named numeric fields
// at the top level and inside every element of "entries", then re-marshals.
// json.Marshal emits map keys in sorted order, so the result is a canonical
// string safe to compare byte-for-byte.
//
// THE ZEROED SET IS DELIBERATELY EXPLICIT AND MUST NOT GROW. A normalisation
// set that grows is how a comparison arm quietly stops discriminating. Only
// two classes are zeroed:
//
//   - wall-CLOCK readings of the same fixture (ageSeconds, ttlRemainingSeconds,
//     lifetimeSeconds) — they measure when the arm ran, not what it holds;
//   - measured mutex-hold DURATIONS (snapshotHoldMicros, maxBatchHoldMicros).
//     These are not timestamps, which is why "normalise timestamps" was not a
//     sufficient instruction: they are lock-hold jitter and would flake the arm.
func normaliseDebugBody(t *testing.T, raw []byte, zero ...string) string {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("normalise: decode %s: %v", raw, err)
	}
	strip := func(m map[string]any) {
		for _, f := range zero {
			if _, ok := m[f]; ok {
				m[f] = 0
			}
		}
	}
	strip(body)
	if entries, ok := body["entries"].([]any); ok {
		for _, e := range entries {
			if em, ok := e.(map[string]any); ok {
				strip(em)
			}
		}
	}
	out, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("normalise: re-marshal: %v", err)
	}
	return string(out)
}

// callDebug drives one debug handler and returns its raw body.
func callDebug(t *testing.T, h http.HandlerFunc, target string) []byte {
	t.Helper()
	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, target, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("%s returned %d; want 200", target, rec.Code)
	}
	return rec.Body.Bytes()
}

// armBodies is what one arm contributes to the comparison.
type armBodies struct {
	indexerRV string
	apistage  string
	reconcile string
}

// runStoreArm builds the arm at storeRV and captures the two EXISTING debug
// surfaces.
//
// /debug/reconcile is called EXACTLY ONCE per arm and asserted on that first
// response. It is NOT a dry run (debug_reconcile.go: divergent entries are
// evicted by the dep-event worker after it returns), so a second call would
// observe a surface its own first call had changed. Do not add one.
func runStoreArm(t *testing.T, storeRV string) armBodies {
	t.Helper()
	seedStoreArm(t, storeRV)

	got := armBodies{indexerRV: storeArmIndexerRV(t)}
	if got.indexerRV != storeRV {
		t.Fatalf("fixture: informer store holds %s at resourceVersion %q, want %q — "+
			"the arm's precondition (the reflector LISTed the seeded object) does not hold",
			storeArmName, got.indexerRV, storeRV)
	}

	got.apistage = normaliseDebugBody(t,
		callDebug(t, DebugApistage(), "/debug/apistage"),
		"ageSeconds", "ttlRemainingSeconds", "lifetimeSeconds")
	got.reconcile = normaliseDebugBody(t,
		callDebug(t, DebugReconcile(), "/debug/reconcile"),
		"snapshotHoldMicros", "maxBatchHoldMicros")
	return got
}

// TestDebugStore_ExistingSurfacesAreBlindToAStaleStore is ARM 3.
//
// GREEN TODAY AND MUST STAY GREEN. It is the executable record of the #237
// blindness: with L1 held constant, a store at rv=1 and a store at rv=2 render
// BYTE-IDENTICALLY on /debug/apistage and /debug/reconcile. /debug/reconcile
// reports divergent=0 in both arms because probeObjectState (deps_watch.go)
// answers objExists for a present object whatever its resourceVersion — the
// present-but-stale class is outside its reach by construction
// (deps_reconcile.go: `if st != objAbsent { continue }`).
//
// If this arm ever goes red, the pair has stopped being a controlled
// comparison and arm 4's RED stops being attributable — fix the fixture, do
// NOT widen the normalisation set.
func TestDebugStore_ExistingSurfacesAreBlindToAStaleStore(t *testing.T) {
	var stale, fresh armBodies
	t.Run("arm1_store_stale_rv1", func(t *testing.T) { stale = runStoreArm(t, "1") })
	t.Run("arm2_store_fresh_rv2", func(t *testing.T) { fresh = runStoreArm(t, "2") })

	if stale.indexerRV == fresh.indexerRV {
		t.Fatalf("the two arms put the SAME resourceVersion (%q) in the store — "+
			"there is nothing to discriminate and every assertion below is vacuous",
			stale.indexerRV)
	}

	if stale.apistage != fresh.apistage {
		t.Fatalf("/debug/apistage DIFFERS across a stale and a fresh store:\n"+
			" store rv=1: %s\n store rv=2: %s\n"+
			"L1 was supposed to be held constant across the arms; a difference here means "+
			"the fixture varies something other than the store, and arm 4's RED would not be "+
			"attributable to the missing surface.", stale.apistage, fresh.apistage)
	}
	if stale.reconcile != fresh.reconcile {
		t.Fatalf("/debug/reconcile DIFFERS across a stale and a fresh store:\n"+
			" store rv=1: %s\n store rv=2: %s\n"+
			"same reasoning as above.", stale.reconcile, fresh.reconcile)
	}

	// Positive scope for the negative claim: the surfaces were non-empty and
	// the reconcile audit actually probed the coordinate. Two empty bodies are
	// also "equal", and that equality would say nothing at all.
	var ap struct {
		Count   int `json:"count"`
		Entries []struct {
			Name string `json:"name"`
		} `json:"entries"`
	}
	if err := json.Unmarshal([]byte(stale.apistage), &ap); err != nil {
		t.Fatalf("decode apistage: %v", err)
	}
	if ap.Count != 1 || len(ap.Entries) != 1 || ap.Entries[0].Name != storeArmName {
		t.Fatalf("scope: /debug/apistage must report exactly the one held-constant cell for %s; got %s",
			storeArmName, stale.apistage)
	}
	var rc struct {
		Probed    int `json:"probed"`
		Divergent int `json:"divergent"`
	}
	if err := json.Unmarshal([]byte(stale.reconcile), &rc); err != nil {
		t.Fatalf("decode reconcile: %v", err)
	}
	if rc.Probed != 1 {
		t.Fatalf("scope: /debug/reconcile probed %d coordinates, want 1 — an audit that probed "+
			"nothing is equal across the arms for the wrong reason; body=%s", rc.Probed, stale.reconcile)
	}
	if rc.Divergent != 0 {
		t.Fatalf("/debug/reconcile reported divergent=%d; want 0. The audit acts only on "+
			"objAbsent, so a present-but-stale object must read as healthy here — that IS the "+
			"blindness #237 describes; body=%s", rc.Divergent, stale.reconcile)
	}
}

// storeTarget is the /debug/store query for the fixture's coordinate.
func storeTarget() string {
	return "/debug/store?gvr=" + storeArmGVR.Group + "/" + storeArmGVR.Version + "/" +
		storeArmGVR.Resource + "&namespace=" + storeArmNS + "&name=" + storeArmName
}

// debugStoreRV drives /debug/store for the fixture coordinate and returns the
// resourceVersion it reports, plus the whole body for failure messages.
func debugStoreRV(t *testing.T) (string, string) {
	t.Helper()
	raw := callDebug(t, DebugStore(), storeTarget())
	var body struct {
		Registered      bool   `json:"registered"`
		Found           bool   `json:"found"`
		ResourceVersion string `json:"resourceVersion"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("/debug/store: decode %s: %v", raw, err)
	}
	if !body.Registered || !body.Found {
		t.Fatalf("/debug/store reports registered=%v found=%v for a coordinate the informer "+
			"store demonstrably holds (the fixture asserted it): %s",
			body.Registered, body.Found, raw)
	}
	return body.ResourceVersion, string(raw)
}

// TestDebugStore_ReportsTheStoresResourceVersion is ARM 4 — the discriminator.
//
// RED TODAY: DebugStore does not exist. The RED is attributable because arm 3
// proves, on the SAME fixture pair, that every pre-existing surface is
// byte-identical across the two states. So the only thing this pair
// discriminates is the new route, which is exactly the claim A makes.
//
// It also asserts arm 3's equality has NOT been bought by making the arms
// identical: the two /debug/store bodies must differ, and differ in
// resourceVersion specifically.
func TestDebugStore_ReportsTheStoresResourceVersion(t *testing.T) {
	var staleRV, freshRV, staleBody, freshBody string
	t.Run("arm1_store_stale_rv1", func(t *testing.T) {
		seedStoreArm(t, "1")
		if got := storeArmIndexerRV(t); got != "1" {
			t.Fatalf("fixture: store holds rv=%q, want 1", got)
		}
		staleRV, staleBody = debugStoreRV(t)
	})
	t.Run("arm2_store_fresh_rv2", func(t *testing.T) {
		seedStoreArm(t, "2")
		if got := storeArmIndexerRV(t); got != "2" {
			t.Fatalf("fixture: store holds rv=%q, want 2", got)
		}
		freshRV, freshBody = debugStoreRV(t)
	})

	if staleRV != "1" {
		t.Fatalf("/debug/store reported resourceVersion %q for a store holding rv=1: %s", staleRV, staleBody)
	}
	if freshRV != "2" {
		t.Fatalf("/debug/store reported resourceVersion %q for a store holding rv=2: %s", freshRV, freshBody)
	}
	if staleRV == freshRV {
		t.Fatalf("/debug/store reports the same resourceVersion (%q) for a stale and a fresh "+
			"store — it discriminates nothing, which is the state of every OTHER surface "+
			"(arm 3) and the whole reason this route exists", staleRV)
	}
}

// TestDebugStore_NeverReturnsABody is the structural leak guard. The route is
// metadata + hashes only: an operator reading it must not be able to recover
// object content, because the same JWT gate admits any valid Krateo token
// while the object may be one that caller's own RBAC forbids.
//
// The guard asserted here is on the WIRE, and the type-level guard is that
// StoreObjectState carries no []byte, no map and no decoded tree — the same
// shape ResolvedEntryMeta uses for /debug/apistage.
func TestDebugStore_NeverReturnsABody(t *testing.T) {
	seedStoreArm(t, "1")
	raw := callDebug(t, DebugStore(), storeTarget())
	for _, leak := range []string{"widgetData", "Portal builder", "spec", "raw", storeArmL1Body} {
		if bytes.Contains(raw, []byte(leak)) {
			t.Fatalf("/debug/store body contains %q — the route must return METADATA AND "+
				"HASHES ONLY; body=%s", leak, raw)
		}
	}
	var body struct {
		BodySHA256 string `json:"bodySHA256"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.BodySHA256) != 64 {
		t.Fatalf("bodySHA256 = %q, want a 64-hex-char sha256 of the STORED bytes — the hash is "+
			"what replaces the body, so an empty one makes the route unable to answer "+
			"\"is this the same object\"", body.BodySHA256)
	}
}

// TestDebugStore_StatusCodesDistinguishAbsentRouteFromAbsentObject pins the
// contract arm 4's RED attribution depends on: a coordinate the store does not
// hold is 200 with found=false, NEVER 404, because 404 is what "the route does
// not exist" looks like. Malformed input is 400.
func TestDebugStore_StatusCodesDistinguishAbsentRouteFromAbsentObject(t *testing.T) {
	seedStoreArm(t, "1")

	rec := httptest.NewRecorder()
	DebugStore()(rec, httptest.NewRequest(http.MethodGet,
		"/debug/store?gvr="+storeArmGVR.Group+"/"+storeArmGVR.Version+"/"+storeArmGVR.Resource+
			"&namespace="+storeArmNS+"&name=no-such-widget", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("absent OBJECT returned %d; want 200 with found=false. A 404 here is "+
			"indistinguishable from an unregistered route, which is precisely the RED arm 4 "+
			"has today", rec.Code)
	}
	var body struct {
		Registered bool `json:"registered"`
		Found      bool `json:"found"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !body.Registered || body.Found {
		t.Fatalf("absent object: want registered=true found=false; got %+v", body)
	}

	for _, bad := range []string{
		"/debug/store",
		"/debug/store?gvr=widgets.templates.krateo.io/v1beta1/pageheaders",
		"/debug/store?gvr=nonsense&name=x",
		"/debug/store?gvr=a/b/c/d/e&name=x",
	} {
		rec := httptest.NewRecorder()
		DebugStore()(rec, httptest.NewRequest(http.MethodGet, bad, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s returned %d; want 400 — malformed input must not be reported as a "+
				"state of the store", bad, rec.Code)
		}
	}
}
