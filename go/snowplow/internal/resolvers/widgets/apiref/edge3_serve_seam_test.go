// edge3_serve_seam_test.go — edge-3 serve-seam staleness class falsifiers
// (issue #277 + the C2 carrier). RED-first.
//
// THE DEFECT. When a widget is served through the Ship-4a RAFullList fast
// path (ra_full_list.go), the widget resolve is a cheap Go-slice over the
// shared raKey cell — the inner LIST that would record the backing-GVR edge
// under the WIDGET's own L1 key runs under fullCtx (raKey), never the widget
// ctx. So the widget cell carries NO edge to the backing GVR ("edge-3"). A
// later mutation to a backing object dirty-marks the raKey cell (edge-2) but
// NOT the widget cell — the widget cell is served directly by the dispatcher
// on a warm hit, bypassing the raKey cell entirely, and goes stale.
//
// HARNESS. The established apiref serve-path idiom (f6_cross_binding_isolation
// _test.go): a real *cache.ResourceWatcher over a dynamic.fake seeded with the
// RBAC grant (newF6Watcher) so raFullListServe re-derives a real BindingUID,
// the real DepTracker + real ResolvedCacheStore, and a STUB resolveRA that —
// exactly like the production restactions resolver — records the backing-GVR
// LIST edge under L1KeyFromContext(rctx). The backing mutation is driven
// through cache.Deps().OnUpdate → OnObjectEvent(objExists) — the SAME decision
// site the informer bridge's worker calls; for a LIST-dep (bucket 2) the
// verdict is state-independent (always dirty-mark), so the exported shim is a
// faithful driver (the informer state-probe only discriminates a
// self-representation DELETE, which edge-3 is not).

package apiref

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jqutil"
	pmaps "github.com/krateo-platformops/plumbing/maps"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// edge3NewWatcher mirrors newF6Watcher but ALSO registers the list-kinds for
// the RA-CR GVR (restactions) and the backing GVR (compositions), so the
// serve-seam replay's ensureInformer → EnsureResourceType can start those
// informers on the fake client without the "register resource to list kind"
// panic. In production Global() handles any GVR; the fake needs each LISTable
// GVR registered up front. No RA/backing objects are seeded — the stub
// resolveRA supplies the data; the informers exist only so a replayed edge has
// a live informer to fire from.
func edge3NewWatcher(t *testing.T, seed ...runtime.Object) *dynamicfake.FakeDynamicClient {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")

	sch := runtime.NewScheme()
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1.AddToScheme: %v", err)
	}
	listKinds := f6RBACListKinds()
	listKinds[gvr()] = "RESTActionList"              // templates.krateo.io/v1 restactions
	listKinds[edge3BackingGVR()] = "CompositionList" // apps.krateo.io/v1 compositions

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return dyn
}

// edge3BackingGVR is the GVR of the objects the shared RESTAction LISTs —
// DISTINCT from the RESTAction-CR coordinate (gvr()) so the backing LIST edge
// and the RA-CR self-dep never collide.
func edge3BackingGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "apps.krateo.io", Version: "v1", Resource: "compositions"}
}

const edge3BackingNS = "krateo-system"
const edge3BackingName = "comp-backing-0"

// edge3StubResolveRA returns a resolveRA closure backed by an in-memory panel
// set whose item names carry *markerPtr (mutable — flip it to model a backing
// mutation), running the REAL slice jq. Like the production resolver, it
// records a backing-GVR LIST edge under L1KeyFromContext(rctx): on the
// unpaginated first-sight (rctx=fullCtx→raKey) that is edge-2; on the
// page-keyed reference resolve (rctx=widget ctx→widgetKey) that is edge-3.
func edge3StubResolveRA(t *testing.T, markerPtr *string, backingGVR schema.GroupVersionResource) func(context.Context, int, int) (map[string]any, error) {
	const n = 30
	return func(rctx context.Context, perPage, page int) (map[string]any, error) {
		// Model the inner-call dep recording the real resolver performs: the
		// backing LIST edge attaches to whatever L1 key the ctx carries.
		if l1 := cache.L1KeyFromContext(rctx); l1 != "" {
			cache.Deps().RecordList(context.Background(), l1, backingGVR, edge3BackingNS)
		}
		items := make([]any, n)
		for i := 0; i < n; i++ {
			items[i] = map[string]any{"metadata": map[string]any{
				"name":              *markerPtr + "-panel-" + itoa3(i),
				"creationTimestamp": tsName(i),
			}}
		}
		dict := map[string]any{"compositionspanels": items}
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

func edge3MustJSON(t *testing.T, m map[string]any) []byte {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func edge3WidgetInputs(name string) *cache.ResolvedKeyInputs {
	return &cache.ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           "widgets.templates.krateo.io",
		Version:         "v1beta1",
		Resource:        "widgets",
		Namespace:       edge3BackingNS,
		Name:            name,
	}
}

// edge3ServedContainsMarker reports whether the widget cell stored under key
// has a body whose items carry marker — the SERVED BODY freshness check
// (store.Get → decode), never a counter.
func edge3ServedContainsMarker(t *testing.T, key, marker string) bool {
	t.Helper()
	entry, ok := cache.ResolvedCache().Get(key)
	if !ok {
		t.Fatalf("no cell stored under %q", key)
	}
	var body map[string]any
	if err := json.Unmarshal(entry.RawJSON, &body); err != nil {
		t.Fatalf("decode cell %q: %v", key, err)
	}
	return f6ContainsMarker(t, body, marker)
}

// TestEdge3_FC2_FastPathServeGoesStale — F-C2 (STEP 0). Two DIFFERENT widget
// CRs (w1, w2) share one cleanly-sliceable RAFullList. w1's resolve is
// first-sight (records the verdict + Puts raKey; its page-keyed reference
// resolve records edge-3 under w1Key). w2's resolve takes the REAL 4a fast
// path (RAFullListServeHit — proven by the counter delta). A backing object
// is then mutated. Both w1 and w2 served bodies MUST reflect it.
//
// RED on unmodified main: w2's cell carries no backing edge, so the mutation
// never reaches it → w2 stays stale. GREEN once C2 replays the raKey cell's
// backing edges onto the widget key inside raFullListServe.
func TestEdge3_FC2_FastPathServeGoesStale(t *testing.T) {
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	edge3NewWatcher(t, f6BuildFixture()...)

	const raName = "edge3-fc2-ra"
	const w1Key = "L1_edge3_fc2_w1"
	const w2Key = "L1_edge3_fc2_w2"
	gRA := gvr() // templates.krateo.io/v1/restactions — the RA-CR coordinate
	backing := edge3BackingGVR()

	store := cache.ResolvedCache()
	d := cache.Deps()
	d.SetStore(store)

	marker := "OLD"
	resolve := edge3StubResolveRA(t, &marker, backing)

	base := f6CtxWithUser(t, "admin", []string{"system:masters"})
	ctx1 := cache.WithL1KeyContext(base, w1Key)
	ctx2 := cache.WithL1KeyContext(base, w2Key)

	// --- w1: first-sight. Records verdict=sliceable, Puts raKey, and records
	// edge-3 under w1Key via the page-keyed reference resolve. ---
	got1, ok, err := raFullListServe(ctx1, gRA, edge3BackingNS, raName, ra(raSliceJQ), 5, 1, nil, resolve)
	if err != nil || !ok {
		t.Fatalf("w1 first-sight serve failed: ok=%v err=%v", ok, err)
	}
	store.Put(w1Key, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, got1), Inputs: edge3WidgetInputs("w1")})

	// --- w2: MUST take the REAL 4a fast path (RAFullListServeHit). ---
	before := cache.RAFullListServeSnapshot()
	got2, ok, err := raFullListServe(ctx2, gRA, edge3BackingNS, raName, ra(raSliceJQ), 5, 2, nil, resolve)
	if err != nil || !ok {
		t.Fatalf("w2 fast-path serve failed: ok=%v err=%v", ok, err)
	}
	after := cache.RAFullListServeSnapshot()
	if after.Hit-before.Hit != 1 {
		t.Fatalf("[C-prove-path] w2 serve must be exactly one RAFullListServeHit, got delta=%d", after.Hit-before.Hit)
	}
	if after.VerifiedSlice-before.VerifiedSlice != 0 || after.Repopulate-before.Repopulate != 0 || after.Fallback-before.Fallback != 0 {
		t.Fatalf("[C-prove-path] w2 serve took a NON-hit path (verified/repopulate/fallback moved): before=%+v after=%+v — the arm does not prove the REAL 4a path", before, after)
	}
	store.Put(w2Key, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, got2), Inputs: edge3WidgetInputs("w2")})

	// Both cells currently hold OLD content.
	if !edge3ServedContainsMarker(t, w1Key, "OLD") || !edge3ServedContainsMarker(t, w2Key, "OLD") {
		t.Fatalf("setup: both widget cells should hold OLD content before the mutation")
	}

	// --- Mutate one backing object. ---
	// Flip the backing data to NEW and refresh the shared raKey cell — this
	// models edge-2 (the raKey cell's backing LIST edge, recorded on the
	// unpaginated first-sight), which keeps the raKey cell fresh on main and
	// is NOT the subject of this falsifier. raKey must be fresh BEFORE the
	// widget re-serve so a widget that DOES get dirty-marked re-slices NEW.
	marker = "NEW"
	raKeyInputs := f6KeyInputs(gRA.Group, gRA.Version, gRA.Resource,
		edge3BackingNS, raName, "C:crb-a-f6-uid", nil)
	raKey := cache.ComputeKey(raKeyInputs)
	freshFull, err := resolve(cache.WithL1KeyContext(base, raKey), 0, 0)
	if err != nil {
		t.Fatalf("raKey refresh resolve failed: %v", err)
	}
	store.PutRAFullList(raKey, raKeyInputs, freshFull)

	// The refresher: a widget key handed to it re-serves via the 4a fast path
	// (now over the FRESH raKey cell) and re-Puts the widget cell. This is the
	// stale-while-revalidate that a dirty-mark drives. It fires ONLY for a
	// widget cell that was actually dirty-marked — i.e. that carries edge-3.
	widgetCtx := map[string]context.Context{w1Key: ctx1, w2Key: ctx2}
	widgetPage := map[string]int{w1Key: 1, w2Key: 2}
	widgetName := map[string]string{w1Key: "w1", w2Key: "w2"}
	var mu sync.Mutex
	marked := map[string]bool{} // every key the REAL dep-event decision site dirty-marked
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
		mu.Lock()
		defer mu.Unlock()
		marked[k] = true
		wc, isWidget := widgetCtx[k]
		if !isWidget {
			return // raKey (or any non-widget) — its refresh is modelled above.
		}
		got, ok, err := raFullListServe(wc, gRA, edge3BackingNS, raName, ra(raSliceJQ), 5, widgetPage[k], nil, resolve)
		if err != nil || !ok {
			t.Errorf("refresh re-serve of %q failed: ok=%v err=%v", k, ok, err)
			return
		}
		store.Put(k, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, got), Inputs: edge3WidgetInputs(widgetName[k])})
	})

	// Drive the backing-object mutation through the dep tracker's REAL decision
	// site (an UPDATE → dirty-mark every dependent). n is the exact number of
	// L1 keys OnObjectEvent matched.
	n := d.OnUpdate(backing, edge3BackingNS, edge3BackingName)

	// --- The GREEN assertion is the REAL invalidation path reaching w2, not a
	// stub artifact: w2Key must ENTER the dirty set alongside w1Key + raKey
	// (l1_keys == 3), proving the replayed edge-3 landed on the real reverse
	// index and the real OnObjectEvent matched it. ---
	mu.Lock()
	mw1, mw2, mRA := marked[w1Key], marked[w2Key], marked[raKey]
	mu.Unlock()
	if !mRA || !mw1 {
		t.Fatalf("edge-3 FC2 setup: expected raKey + w1Key dirty-marked; marked=%v", marked)
	}
	if !mw2 {
		t.Fatalf("edge-3 FC2 RED: w2Key did NOT enter the dirty-mark set — the backing UPDATE matched only {w1Key, raKey}, not w2 (n=%d). w2 was served via the 4a fast path and carries no edge-3.", n)
	}
	if n != 3 {
		t.Fatalf("edge-3 FC2: the backing UPDATE must match exactly 3 L1 keys {w1Key, w2Key, raKey}, got n=%d (marked=%v)", n, marked)
	}

	// --- And both served bodies MUST reflect the mutation (the dirty-mark
	// drove a real re-serve that re-Put fresh content). ---
	if !edge3ServedContainsMarker(t, w1Key, "NEW") {
		t.Fatalf("edge-3 FC2: w1 served body did not reflect the backing mutation (still OLD) — its edge-3 was not recorded")
	}
	if !edge3ServedContainsMarker(t, w2Key, "NEW") {
		t.Fatalf("edge-3 FC2 RED: w2 was served through the 4a fast path and its widget cell carries NO backing edge (edge-3), so the backing mutation never dirty-marked it — w2 is STALE (still OLD). C2 must replay the raKey cell's backing edges onto the widget key.")
	}
}

// edge3Widget is one served widget cell for the convergence helper.
type edge3Widget struct {
	key  string
	ctx  context.Context
	page int
	name string
}

// edge3EdgesContainBacking reports whether edges hold the backing-GVR LIST edge
// (edge-3): {backing, edge3BackingNS, "*"}.
func edge3EdgesContainBacking(edges []cache.DepKey, backing schema.GroupVersionResource) bool {
	for _, e := range edges {
		if e.GVR == backing && e.Namespace == edge3BackingNS && e.Name == "*" {
			return true
		}
	}
	return false
}

// edge3MutateAndAssertConverge flips the backing marker to "NEW", refreshes the
// shared raKey cell (models edge-2 — the raKey cell's backing edge, NOT under
// test), installs a refresher that re-serves any dirty-marked widget via the 4a
// fast path over the fresh raKey cell, drives ONE backing UPDATE through the dep
// tracker's decision site, and asserts EVERY widget's served cell reflects NEW.
// A widget with no edge-3 is never dirty-marked → its cell stays OLD → RED.
func edge3MutateAndAssertConverge(t *testing.T, base context.Context, gRA, backing schema.GroupVersionResource,
	raName string, markerPtr *string, resolve func(context.Context, int, int) (map[string]any, error), widgets []edge3Widget) {
	t.Helper()
	store := cache.ResolvedCache()
	d := cache.Deps()

	*markerPtr = "NEW"
	raKeyInputs := f6KeyInputs(gRA.Group, gRA.Version, gRA.Resource,
		edge3BackingNS, raName, "C:crb-a-f6-uid", nil)
	raKey := cache.ComputeKey(raKeyInputs)
	freshFull, err := resolve(cache.WithL1KeyContext(base, raKey), 0, 0)
	if err != nil {
		t.Fatalf("raKey refresh resolve failed: %v", err)
	}
	store.PutRAFullList(raKey, raKeyInputs, freshFull)

	byKey := map[string]edge3Widget{}
	for _, w := range widgets {
		byKey[w.key] = w
	}
	var mu sync.Mutex
	marked := map[string]bool{} // every key the REAL dep-event decision site dirty-marked
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
		mu.Lock()
		defer mu.Unlock()
		marked[k] = true
		w, ok := byKey[k]
		if !ok {
			return // raKey / any non-widget — its refresh is modelled above.
		}
		got, ok, err := raFullListServe(w.ctx, gRA, edge3BackingNS, raName, ra(raSliceJQ), 5, w.page, nil, resolve)
		if err != nil || !ok {
			t.Errorf("refresh re-serve of %q failed: ok=%v err=%v", k, ok, err)
			return
		}
		store.Put(k, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, got), Inputs: edge3WidgetInputs(w.name)})
	})
	d.OnUpdate(backing, edge3BackingNS, edge3BackingName)
	for _, w := range widgets {
		// The real invalidation path must reach each widget cell: its key must
		// ENTER the dirty-mark set (belt), AND its served body must converge
		// (suspenders — proves the dirty-mark drove a real re-serve).
		mu.Lock()
		hit := marked[w.key]
		mu.Unlock()
		if !hit {
			t.Fatalf("edge-3 RED: widget %q (%s) did NOT enter the dirty-mark set — its cell carries no edge-3, so the backing UPDATE never matched it", w.key, w.name)
		}
		if !edge3ServedContainsMarker(t, w.key, "NEW") {
			t.Fatalf("edge-3 RED: widget %q (%s) served body did not reflect the backing mutation — its cell carries no edge-3", w.key, w.name)
		}
	}
}

// TestEdge3_FPIN_FirstSightRecordsEdge3UnderWidgetCtx — F-PIN. The 4a
// first-sight page-keyed reference resolve (ra_full_list.go:381) runs under the
// ORIGINAL widget ctx, so it records edge-3 DIRECTLY under the widget key. Pins
// that against a regression that swaps ctx→fullCtx there (which would move
// edge-3 onto the raKey cell and re-open the staleness class for first-sight).
func TestEdge3_FPIN_FirstSightRecordsEdge3UnderWidgetCtx(t *testing.T) {
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	edge3NewWatcher(t, f6BuildFixture()...)

	const raName = "edge3-fpin-ra"
	const wKey = "L1_edge3_fpin_w"
	gRA := gvr()
	backing := edge3BackingGVR()
	cache.Deps().SetStore(cache.ResolvedCache())

	marker := "OLD"
	resolve := edge3StubResolveRA(t, &marker, backing)
	ctx := cache.WithL1KeyContext(f6CtxWithUser(t, "admin", []string{"system:masters"}), wKey)

	_, ok, err := raFullListServe(ctx, gRA, edge3BackingNS, raName, ra(raSliceJQ), 5, 1, nil, resolve)
	if err != nil || !ok {
		t.Fatalf("first-sight serve failed: ok=%v err=%v", ok, err)
	}
	if !edge3EdgesContainBacking(cache.Deps().EdgesUnder(wKey), backing) {
		t.Fatalf("F-PIN RED: first-sight did NOT record edge-3 (the backing LIST edge) under the widget key — the page-keyed reference resolve must run under the ORIGINAL ctx, not fullCtx. EdgesUnder(%q)=%v", wKey, cache.Deps().EdgesUnder(wKey))
	}
}

// TestEdge3_FC1b_MemoMissBodyFromFastPathCapturesNonEmptyDeps — F-C1b, THE
// blocking arm. raKey is PRE-WARMED (verdict known-sliceable + resident cell +
// edge-2), so w1's memo-MISS body is produced BY the 4a fast path — NO real
// resolveRA runs under w1's key. A naive C1 capture would therefore close
// EMPTY. It must instead close NON-EMPTY because C2's ReplayEdges runs INSIDE
// the capture window (the blocking-finding composition). w2 (memo hit) replays
// those deps and converges on a backing mutation. RED if the capture/replay
// composition breaks. Mirrors resolve.go's B2 (capture) + B3 (replay) wiring.
func TestEdge3_FC1b_MemoMissBodyFromFastPathCapturesNonEmptyDeps(t *testing.T) {
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	edge3NewWatcher(t, f6BuildFixture()...)

	const raName = "edge3-fc1b-ra"
	const w1Key = "L1_edge3_fc1b_w1"
	const w2Key = "L1_edge3_fc1b_w2"
	gRA := gvr()
	backing := edge3BackingGVR()
	store := cache.ResolvedCache()
	d := cache.Deps()
	d.SetStore(store)

	marker := "OLD"
	resolve := edge3StubResolveRA(t, &marker, backing)
	base := f6CtxWithUser(t, "admin", []string{"system:masters"})

	// PRE-WARM raKey: resident cell + known-sliceable verdict + edge-2 + RA-CR
	// self-dep (the state a prior first-sight leaves), so w1's serve is a pure
	// 4a fast-path HIT with no real resolveRA.
	raKeyInputs := f6KeyInputs(gRA.Group, gRA.Version, gRA.Resource,
		edge3BackingNS, raName, "C:crb-a-f6-uid", nil)
	raKey := cache.ComputeKey(raKeyInputs)
	shape := seedFullListShape(gRA, edge3BackingNS, raName, ra(raSliceJQ))
	fullDict, err := resolve(cache.WithL1KeyContext(base, raKey), 0, 0) // records edge-2 under raKey
	if err != nil {
		t.Fatalf("pre-warm resolve failed: %v", err)
	}
	store.PutRAFullList(raKey, raKeyInputs, fullDict)
	cache.RecordSliceability(raKey, shape, true)
	d.Record(context.Background(), raKey, gRA, edge3BackingNS, raName) // RA-CR self-dep (mirror ra_full_list.go:469)

	memo := cache.NewSeedResolveMemo(pmaps.DeepCopyJSON)

	// w1: memo MISS whose body comes from the 4a fast path. Capture (B2).
	ctx1 := cache.WithL1KeyContext(base, w1Key)
	capBuf := d.BeginCapture(w1Key)
	before := cache.RAFullListServeSnapshot()
	served1, ok, err := raFullListServe(ctx1, gRA, edge3BackingNS, raName, ra(raSliceJQ), 5, 1, nil, resolve)
	if err != nil || !ok {
		t.Fatalf("w1 fast-path serve failed: ok=%v err=%v", ok, err)
	}
	after := cache.RAFullListServeSnapshot()
	if after.Hit-before.Hit != 1 || after.VerifiedSlice-before.VerifiedSlice != 0 {
		t.Fatalf("[C-prove-path] w1's memo-miss body must be produced by the 4a fast path (one Hit, no verify): before=%+v after=%+v", before, after)
	}
	deps := d.EndCapture(w1Key, capBuf)
	if len(deps) == 0 {
		t.Fatalf("F-C1b RED (THE blocking arm): the memo-miss body was produced by the 4a fast path, so a naive capture closes EMPTY — the captured deps MUST be non-empty via C2's ReplayEdges running INSIDE the capture window. deps=%v", deps)
	}
	if !edge3EdgesContainBacking(deps, backing) {
		t.Fatalf("F-C1b RED: captured deps do not contain the backing edge (edge-3): %v", deps)
	}
	memoKey := memo.Key(edge3BackingNS, raName, "admin", []string{"system:masters"}, "", cache.HashExtras(nil), 5, 1)
	memo.Store(memoKey, pmaps.DeepCopyJSON(served1), deps, cache.DepGenEpochNow())
	store.Put(w1Key, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, served1), Inputs: edge3WidgetInputs("w1")})

	// w2: memo HIT → replay the captured deps under w2's key (B3).
	ctx2 := cache.WithL1KeyContext(base, w2Key)
	body2, deps2, _, ok := memo.Load(memoKey)
	if !ok {
		t.Fatalf("harness broken: w2 memo load MISS under key %q", memoKey)
	}
	d.ReplayEdges(context.Background(), w2Key, deps2)
	store.Put(w2Key, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, body2), Inputs: edge3WidgetInputs("w2")})
	if !edge3EdgesContainBacking(d.EdgesUnder(w2Key), backing) {
		t.Fatalf("F-C1b RED: w2 (memo hit) cell carries no edge-3 after replay: %v", d.EdgesUnder(w2Key))
	}

	// Both converge on a backing mutation.
	edge3MutateAndAssertConverge(t, base, gRA, backing, raName, &marker, resolve,
		[]edge3Widget{{w1Key, ctx1, 1, "w1"}, {w2Key, ctx2, 1, "w2"}})
}

// TestEdge3_FGENERAL_ThreeCarriersConverge — F-GENERAL (class invariant). Three
// widgets share one RA, each served by a DIFFERENT edge-3 carrier — first-sight
// (edge-3 via the page-keyed reference resolve), 4a fast-path hit (edge-3 via
// C2's replay), memo hit (edge-3 via B3's memo replay). ONE backing mutation;
// all three served bodies MUST converge.
func TestEdge3_FGENERAL_ThreeCarriersConverge(t *testing.T) {
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	edge3NewWatcher(t, f6BuildFixture()...)

	const raName = "edge3-fgeneral-ra"
	const wfsKey = "L1_edge3_fg_fs"
	const w4aKey = "L1_edge3_fg_4a"
	const wmemoKey = "L1_edge3_fg_memo"
	gRA := gvr()
	backing := edge3BackingGVR()
	store := cache.ResolvedCache()
	d := cache.Deps()
	d.SetStore(store)

	marker := "OLD"
	resolve := edge3StubResolveRA(t, &marker, backing)
	base := f6CtxWithUser(t, "admin", []string{"system:masters"})
	memo := cache.NewSeedResolveMemo(pmaps.DeepCopyJSON)

	// Carrier 1 — FIRST-SIGHT (page 1). It is also the memo PRODUCER: wrap its
	// serve in a capture so the memo entry carries the deps (as the seed pass's
	// first widget does).
	ctxFS := cache.WithL1KeyContext(base, wfsKey)
	capBuf := d.BeginCapture(wfsKey)
	gotFS, ok, err := raFullListServe(ctxFS, gRA, edge3BackingNS, raName, ra(raSliceJQ), 5, 1, nil, resolve)
	if err != nil || !ok {
		t.Fatalf("first-sight serve failed: ok=%v err=%v", ok, err)
	}
	depsFS := d.EndCapture(wfsKey, capBuf)
	store.Put(wfsKey, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, gotFS), Inputs: edge3WidgetInputs("fs")})
	memoKey := memo.Key(edge3BackingNS, raName, "admin", []string{"system:masters"}, "", cache.HashExtras(nil), 5, 1)
	memo.Store(memoKey, pmaps.DeepCopyJSON(gotFS), depsFS, cache.DepGenEpochNow())

	// Carrier 2 — 4a FAST-PATH HIT (page 2). edge-3 via C2's replay in the hit
	// branch.
	ctx4A := cache.WithL1KeyContext(base, w4aKey)
	before := cache.RAFullListServeSnapshot()
	got4A, ok, err := raFullListServe(ctx4A, gRA, edge3BackingNS, raName, ra(raSliceJQ), 5, 2, nil, resolve)
	if err != nil || !ok {
		t.Fatalf("4a fast-path serve failed: ok=%v err=%v", ok, err)
	}
	if cache.RAFullListServeSnapshot().Hit-before.Hit != 1 {
		t.Fatalf("[C-prove-path] the 4a-slice widget must take the fast-path hit")
	}
	store.Put(w4aKey, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, got4A), Inputs: edge3WidgetInputs("4a")})

	// Carrier 3 — MEMO HIT (page 1). edge-3 via B3's memo replay.
	ctxMemo := cache.WithL1KeyContext(base, wmemoKey)
	bodyMemo, depsMemo, _, ok := memo.Load(memoKey)
	if !ok {
		t.Fatalf("harness broken: memo widget load MISS")
	}
	d.ReplayEdges(context.Background(), wmemoKey, depsMemo)
	store.Put(wmemoKey, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, bodyMemo), Inputs: edge3WidgetInputs("memo")})

	// Each carrier's cell must carry edge-3.
	for _, w := range []struct {
		key string
		nm  string
	}{{wfsKey, "first-sight"}, {w4aKey, "4a-slice"}, {wmemoKey, "memo-hit"}} {
		if !edge3EdgesContainBacking(d.EdgesUnder(w.key), backing) {
			t.Fatalf("F-GENERAL RED: %s carrier cell %q carries no edge-3: %v", w.nm, w.key, d.EdgesUnder(w.key))
		}
	}

	// ONE backing mutation; all three converge.
	edge3MutateAndAssertConverge(t, base, gRA, backing, raName, &marker, resolve,
		[]edge3Widget{{wfsKey, ctxFS, 1, "fs"}, {w4aKey, ctx4A, 2, "4a"}, {wmemoKey, ctxMemo, 1, "memo"}})
}

// edge3RAUnstructured builds a minimal RESTAction CR that objects.Get can serve
// from the informer and convertToRESTAction can parse (spec.filter = the slice
// jq → ra.Spec.Filter, so seedFullListShape matches the pre-warmed verdict). No
// userAccessFilter stage, so raFullListServe does not take the UAF bypass.
func edge3RAUnstructured(ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec":       map[string]any{"filter": raSliceJQ},
	}}
}

// edge3WaitServable polls IsServable until true or timeout (the confirm-prime is
// async; offline over the fake client it degrades to servable-true).
func edge3WaitServable(t *testing.T, g schema.GroupVersionResource, timeout time.Duration) bool {
	t.Helper()
	rw := cache.Global()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if rw.IsServable(g) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return rw.IsServable(g)
}

// TestEdge3_E2E_ResolveB2B3WiringThroughApirefResolve — the B2/B3 seam-gap
// closer (architect flag 2). It drives the ACTUAL resolve.go inline wiring
// (B2 capture around the producing block + EndCapture/Store in storeMemo; B3
// Load→ReplayEdges on hit) end-to-end through apiref.Resolve, NOT the primitives
// directly. raKey is PRE-WARMED (known-sliceable + resident) so raFullListServe
// takes the fast-path HIT and the real restactions.Resolve is never invoked; the
// RESTAction CR is seeded into the watcher so objects.Get serves it. w1 (memo
// MISS, body from the 4a fast path) captures NON-EMPTY deps into the memo; w2
// (memo HIT) replays them under its own key and converges on a backing mutation.
func TestEdge3_E2E_ResolveB2B3WiringThroughApirefResolve(t *testing.T) {
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()

	const raName = "edge3-e2e-ra"
	const ns = edge3BackingNS
	const w1Key = "L1_edge3_e2e_w1"
	const w2Key = "L1_edge3_e2e_w2"
	gRA := gvr() // templates.krateo.io/v1 restactions
	backing := edge3BackingGVR()

	// Watcher seeded with the RBAC grant AND the RESTAction CR objects.Get serves.
	seed := append(f6BuildFixture(), edge3RAUnstructured(ns, raName))
	edge3NewWatcher(t, seed...)

	// Make the RESTAction GVR servable so objects.Get serves it from the
	// informer (offline-degraded-true over the fake client).
	added, syncCh := cache.Global().EnsureResourceType(gRA)
	if added {
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("restactions informer did not sync")
		}
	}
	if !edge3WaitServable(t, gRA, 3*time.Second) {
		t.Fatalf("restactions GVR never became servable — objects.Get would fall through to the apiserver")
	}

	store := cache.ResolvedCache()
	d := cache.Deps()
	d.SetStore(store)

	marker := "OLD"
	resolve := edge3StubResolveRA(t, &marker, backing)
	base := f6CtxWithUser(t, "admin", []string{"system:masters"})

	// PRE-WARM raKey: resident cell + known-sliceable verdict + edge-2 + RA-CR
	// self-dep, so raFullListServe HITs the fast path (no real resolve).
	raKeyInputs := f6KeyInputs(gRA.Group, gRA.Version, gRA.Resource,
		ns, raName, "C:crb-a-f6-uid", nil)
	raKey := cache.ComputeKey(raKeyInputs)
	shape := seedFullListShape(gRA, ns, raName, ra(raSliceJQ))
	fullDict, err := resolve(cache.WithL1KeyContext(base, raKey), 0, 0) // records edge-2 under raKey
	if err != nil {
		t.Fatalf("pre-warm resolve failed: %v", err)
	}
	store.PutRAFullList(raKey, raKeyInputs, fullDict)
	cache.RecordSliceability(raKey, shape, true)
	d.Record(context.Background(), raKey, gRA, ns, raName) // RA-CR self-dep

	memo := cache.NewSeedResolveMemo(pmaps.DeepCopyJSON)
	apiRef := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: raName, Namespace: ns},
		Resource:   "restactions",
		APIVersion: "templates.krateo.io/v1",
	}
	opts := ResolveOptions{ApiRef: apiRef, PerPage: 5, Page: 1}

	// --- w1: memo MISS through apiref.Resolve. B2 capture wraps the producing
	// 4a fast-path serve; storeMemo stores the captured deps. ---
	ctx1 := cache.WithSeedResolveMemo(cache.WithL1KeyContext(base, w1Key), memo)
	before := cache.RAFullListServeSnapshot()
	body1, err := Resolve(ctx1, opts)
	if err != nil {
		t.Fatalf("w1 apiref.Resolve failed: %v", err)
	}
	if cache.RAFullListServeSnapshot().Hit-before.Hit != 1 {
		t.Fatalf("[C-prove-path] w1 must be served by the 4a fast-path HIT through apiref.Resolve")
	}
	store.Put(w1Key, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, body1), Inputs: edge3WidgetInputs("w1")})

	// --- w2: memo HIT through apiref.Resolve. B3 replays the captured deps
	// under w2's key. Assert it took the memo path (NO fast-path serve — Hit
	// delta 0), so w2's edge-3 provably comes from B3's replay, not a second
	// raFullListServe (that would be C2, not the wiring under test). ---
	beforeW2 := cache.RAFullListServeSnapshot()
	ctx2 := cache.WithSeedResolveMemo(cache.WithL1KeyContext(base, w2Key), memo)
	body2, err := Resolve(ctx2, opts)
	if err != nil {
		t.Fatalf("w2 apiref.Resolve failed: %v", err)
	}
	afterW2 := cache.RAFullListServeSnapshot()
	if afterW2.Hit != beforeW2.Hit || afterW2.VerifiedSlice != beforeW2.VerifiedSlice || afterW2.Repopulate != beforeW2.Repopulate {
		t.Fatalf("E2E: w2 must be served by the B3 memo HIT (no raFullListServe), but a serve counter moved: before=%+v after=%+v", beforeW2, afterW2)
	}
	store.Put(w2Key, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, body2), Inputs: edge3WidgetInputs("w2")})

	// The B2/B3 wiring must have landed edge-3 on w2's cell.
	if !edge3EdgesContainBacking(d.EdgesUnder(w2Key), backing) {
		t.Fatalf("E2E RED: apiref.Resolve's B3 memo-hit replay did NOT record edge-3 under w2Key — the inline resolve.go capture/replay wiring is broken. EdgesUnder(w2Key)=%v", d.EdgesUnder(w2Key))
	}
	if !edge3EdgesContainBacking(d.EdgesUnder(w1Key), backing) {
		t.Fatalf("E2E RED: w1's B2 capture/replay did not land edge-3 under w1Key: %v", d.EdgesUnder(w1Key))
	}

	// Both converge on a backing mutation (and both enter the dirty set).
	edge3MutateAndAssertConverge(t, base, gRA, backing, raName, &marker, resolve,
		[]edge3Widget{{w1Key, ctx1, 1, "w1"}, {w2Key, ctx2, 1, "w2"}})
}
