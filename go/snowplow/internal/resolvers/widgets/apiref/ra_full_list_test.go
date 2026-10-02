// ra_full_list_test.go — Ship 4a (0.30.198) falsifiers for the apiRef-
// chokepoint page-independent serve path.
//
// These exercise raFullListServe with a STUBBED resolve closure (no live
// cluster) so the hit / first-sight-verify / fallback / repopulate paths +
// the cache-off byte-identity are provable in isolation. The pure slice/key/
// memo logic is covered in internal/cache/ra_full_list_slice_test.go.

package apiref

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jqutil"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// raSliceJQ — the compositions-panels RESTAction's top-level slice filter.
const raSliceJQ = `
{
  compositionspanels: (
    (.compositionspanels // []) as $items
    | ($items | sort_by(.metadata.creationTimestamp // "") | reverse) as $sorted
    | (.slice.offset  // 0)                 as $offset
    | (.slice.perPage // ($sorted | length)) as $perPage
    | [ $sorted | length as $len | range($offset; $offset + $perPage) | select(. < $len) | $sorted[.] ]
  )
}
`

func panelDict(n int) map[string]any {
	items := make([]any, n)
	for i := 0; i < n; i++ {
		items[i] = map[string]any{"metadata": map[string]any{
			"name":              padName(i),
			"creationTimestamp": tsName(i),
		}}
	}
	return map[string]any{"compositionspanels": items}
}
func padName(i int) string { return "panel-" + itoa3(i) }
func tsName(i int) string  { return "2026-01-" + itoa2(i+1) + "T00:00:00Z" }
func itoa2(i int) string {
	if i < 10 {
		return "0" + string(rune('0'+i))
	}
	return string(rune('0'+i/10)) + string(rune('0'+i%10))
}
func itoa3(i int) string {
	return string(rune('0'+(i/100)%10)) + string(rune('0'+(i/10)%10)) + string(rune('0'+i%10))
}

// stubResolveRA returns a resolveRA closure backed by an in-memory panel set,
// running the REAL RA slice jq. It counts invocations so a test can assert
// the cheap hit path does NO resolve.
func stubResolveRA(t *testing.T, panels map[string]any, calls *atomic.Int64) func(context.Context, int, int) (map[string]any, error) {
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

func ctxWithUser(t *testing.T) context.Context {
	return xcontext.BuildContext(t.Context(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "admin", Groups: []string{"system:masters"}}))
}

func ra(jq string) *templatesv1.RESTAction {
	return &templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{Filter: ptr.To(jq)}}
}

func gvr() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}
}

// HG-4a.serve.1 — first-sight VERIFY → Put + serve Go-slice; second /call at a
// DIFFERENT page is a cheap HIT (no resolve); the served bytes match the RA
// jq slice exactly.
func TestRAServe_VerifyThenHit(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	// #95 arch C-1: raFullListServe now declines (page-keyed fallback) when the
	// re-derived BindingUID is "" (the shared empty-identity cell must never be
	// served/populated). Publish the f6 RBAC grant so admin re-derives a REAL
	// non-empty BindingUID (C:crb-a-f6-uid) on the restactions GVR — the
	// production-representative servable state this test exercises. (Pre-C-1
	// this test rode the no-snapshot→"" path, which was never a legitimate
	// servable scenario.)
	newF6Watcher(t, f6BuildFixture()...)

	panels := panelDict(40)
	var calls atomic.Int64
	resolve := stubResolveRA(t, panels, &calls)
	ctx := ctxWithUser(t)

	// First /call page 1 perPage 10 — first sight: 2 resolves (unpaginated +
	// page-keyed reference for the byte-verify).
	got1, ok, err := raFullListServe(ctx, gvr(), "krateo-system", "compositions-panels",
		ra(raSliceJQ), 10, 1, nil, resolve)
	if err != nil || !ok {
		t.Fatalf("first serve failed: ok=%v err=%v", ok, err)
	}
	firstSightCalls := calls.Load()
	if firstSightCalls != 2 {
		t.Fatalf("first sight should resolve exactly twice (unpaginated + page-keyed verify), got %d", firstSightCalls)
	}

	// Verify served page 1 == RA jq page 1.
	ref1, _ := resolve(ctx, 10, 1)
	assertCanonEqual(t, got1, ref1, "page1")

	// Second /call page 3 perPage 10 — cheap HIT, NO new resolve.
	callsBefore := calls.Load()
	got3, ok, err := raFullListServe(ctx, gvr(), "krateo-system", "compositions-panels",
		ra(raSliceJQ), 10, 3, nil, resolve)
	if err != nil || !ok {
		t.Fatalf("second serve failed: ok=%v err=%v", ok, err)
	}
	if calls.Load() != callsBefore {
		t.Fatalf("page-3 hit should do ZERO resolve (Go-slice over cached full), but %d resolves ran", calls.Load()-callsBefore)
	}
	ref3, _ := resolve(ctx, 10, 3)
	assertCanonEqual(t, got3, ref3, "page3")

	// Serve-outcome metrics: one verified-slice + one hit.
	s := cache.RAFullListServeSnapshot()
	if s.VerifiedSlice < 1 || s.Hit < 1 {
		t.Fatalf("serve outcomes wrong: %+v", s)
	}
}

// HG-4a.serve.2 — a NON-sliceable RA (per-page aggregation) byte-verify FAILS
// → memo NOT-sliceable → every /call falls back to the page-keyed resolve
// (correct result, never a wrong one). The returned bytes equal the page-keyed
// resolve.
func TestRAServe_NonSliceableFallsBack(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	// #95 arch C-1: publish the f6 grant so admin re-derives a REAL non-empty
	// BindingUID (was riding the no-snapshot→"" path, now declined by C-1).
	newF6Watcher(t, f6BuildFixture()...)

	// Aggregation RA: output depends on the slice (returns a count), so a
	// Go-slice over the unpaginated full can never reproduce a paginated
	// resolve → byte-verify fails.
	aggJQ := `{ count: ((.compositionspanels // []) | length), pp: (.slice.perPage // 0) }`
	panels := panelDict(30)
	var calls atomic.Int64
	resolve := func(_ context.Context, perPage, page int) (map[string]any, error) {
		calls.Add(1)
		dict := map[string]any{"compositionspanels": panels["compositionspanels"]}
		if perPage > 0 && page > 0 {
			dict["slice"] = map[string]any{"perPage": float64(perPage), "offset": float64((page - 1) * perPage)}
		}
		s, _ := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: aggJQ, Data: dict})
		var out map[string]any
		_ = json.Unmarshal([]byte(s), &out)
		return out, nil
	}
	ctx := ctxWithUser(t)

	got, ok, err := raFullListServe(ctx, gvr(), "krateo-system", "agg-ra",
		ra(aggJQ), 10, 1, nil, resolve)
	if err != nil || !ok {
		t.Fatalf("agg serve failed: ok=%v err=%v", ok, err)
	}
	// Served == the page-keyed resolve (the fall-back reference).
	ref, _ := resolve(ctx, 10, 1)
	assertCanonEqual(t, got, ref, "agg-page1")

	// A second /call: the NOT-sliceable verdict is memoised → fall back
	// (served=false so the caller resolves page-keyed; raFullListServe returns
	// ok=false).
	_, ok2, _ := raFullListServe(ctx, gvr(), "krateo-system", "agg-ra",
		ra(aggJQ), 10, 2, nil, resolve)
	if ok2 {
		t.Fatalf("a known NOT-sliceable shape must return served=false (caller falls back)")
	}
	s := cache.RAFullListServeSnapshot()
	if s.Fallback < 1 {
		t.Fatalf("expected at least one fallback serve outcome: %+v", s)
	}
}

// HG-4a.serve.3 — no identity on ctx → serve declines (fall back). NEVER serve
// a per-cohort cell without an identity.
func TestRAServe_NoIdentityDeclines(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	var calls atomic.Int64
	resolve := stubResolveRA(t, panelDict(10), &calls)

	// ctx with NO UserInfo.
	_, ok, err := raFullListServe(t.Context(), gvr(), "krateo-system", "compositions-panels",
		ra(raSliceJQ), 10, 1, nil, resolve)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Fatalf("serve without identity MUST decline (ok=false), never serve a cohort cell")
	}
	if calls.Load() != 0 {
		t.Fatalf("no-identity decline should not resolve, got %d resolves", calls.Load())
	}
}

// HG-4a.serve.4 — cache OFF → raFullListServe declines immediately (the
// Resolve caller then takes the byte-identical pre-4a page-keyed path). This
// is the flag-off byte-identity guard at the serve layer.
func TestRAServe_CacheOffDeclines(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	cache.ResetResolvedCacheForTest()
	var calls atomic.Int64
	resolve := stubResolveRA(t, panelDict(10), &calls)

	_, ok, err := raFullListServe(ctxWithUser(t), gvr(), "krateo-system", "compositions-panels",
		ra(raSliceJQ), 10, 1, nil, resolve)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Fatalf("cache-off serve MUST decline (ok=false) so the pre-4a path runs byte-identically")
	}
	if calls.Load() != 0 {
		t.Fatalf("cache-off decline should not resolve via the 4a path, got %d", calls.Load())
	}
}

// HG-4a.serve.5 — EMPTY-FULL self-healing guard (0.30.208 defect fix).
//
// Models the panels-not-synced-at-boot state: the RA resolves to an EMPTY
// full ({compositionspanels: []}). The byte-verify would otherwise see
// empty-Go-slice == empty-page-keyed → record verdict=sliceable + Put the
// empty cell → freeze the fast path on empty FOREVER. The guard MUST instead:
//
//	(a) record NO sliceable verdict (verdict stays UNKNOWN / re-verifiable),
//	(b) Put NO cell (no empty cell cached),
//	(c) serve the correct (empty) page-keyed result this /call.
//
// Then, after the informer syncs (the stub starts returning real panels), the
// NEXT /call must re-run first-sight, record a real verdict, and serve
// non-empty data — proving self-healing.
func TestRAServe_EmptyFullDoesNotFreeze(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	// #95 arch C-1: publish the f6 grant so admin re-derives a REAL non-empty
	// BindingUID (C:crb-a-f6-uid); the empty-full self-heal path is exercised
	// under a real identity, not the now-declined ""-path.
	newF6Watcher(t, f6BuildFixture()...)

	// `synced` flips the stub from the not-synced (empty) state to the
	// synced (real panels) state, modelling the informer catching up.
	var synced atomic.Bool
	full := panelDict(40)
	var calls atomic.Int64
	resolve := func(ctx context.Context, perPage, page int) (map[string]any, error) {
		calls.Add(1)
		if !synced.Load() {
			// Not-synced / continueOnError-degraded: empty list at EVERY
			// pagination (both the unpaginated full and the page-keyed ref).
			return map[string]any{"compositionspanels": []any{}}, nil
		}
		dict := map[string]any{}
		for k, v := range full {
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
	ctx := ctxWithUser(t)

	// Unique RA name so this test is self-isolating from the PROCESS-GLOBAL
	// sliceability memo (not reset by ResetResolvedCacheForTest) — a verdict
	// recorded by another test for "compositions-panels" must never leak here.
	const raName = "compositions-panels-empty-guard"

	// --- Not-synced: first /call resolves EMPTY. ---
	got1, ok, err := raFullListServe(ctx, gvr(), "krateo-system", raName,
		ra(raSliceJQ), 5, 1, nil, resolve)
	if err != nil || !ok {
		t.Fatalf("empty-state serve failed: ok=%v err=%v", ok, err)
	}
	// Served the (empty) page-keyed result — correct for the moment.
	if items, _ := got1["compositionspanels"].([]any); len(items) != 0 {
		t.Fatalf("not-synced serve should be empty, got %d items", len(items))
	}

	// (a) verdict MUST stay UNKNOWN — NOT recorded as sliceable.
	shape := cache.SliceShapeHash(raFullListCallerClass, gvr().Group, gvr().Version,
		gvr().Resource, "krateo-system", raName, raSliceJQ)
	// Ship 0.30.242 H.c-layered Phase 2c — the production code path
	// (ra_full_list.go) derives BindingUID via direct rbac.EvaluateRBAC.
	// #95 arch C-1: this test now publishes the f6 grant (newF6Watcher above),
	// so admin re-derives the REAL first-match BindingUID "C:crb-a-f6-uid" (the
	// f6 crb-A-admin UID). The test's raKey computation MUST match that — NOT
	// "" (the pre-C-1 no-snapshot path, which C-1 now declines before this
	// point).
	keyInputs := f6KeyInputs(gvr().Group, gvr().Version, gvr().Resource,
		"krateo-system", raName,
		"C:crb-a-f6-uid", nil)
	raKey := cache.ComputeKey(keyInputs)
	if _, known := cache.SliceabilityLookup(raKey, shape); known {
		t.Fatalf("empty full MUST NOT record a sliceability verdict (must stay UNKNOWN/re-verifiable)")
	}

	// (b) NO empty cell cached.
	if _, ok := cache.ResolvedCache().Get(raKey); ok {
		t.Fatalf("empty full MUST NOT Put a cell (no empty cell cached)")
	}

	// A second not-synced /call must AGAIN re-run first-sight (still empty,
	// still no freeze) — it must not have been short-circuited by a recorded
	// verdict. (2 resolves per first-sight = unpaginated + page-keyed.)
	callsBefore := calls.Load()
	_, ok, err = raFullListServe(ctx, gvr(), "krateo-system", raName,
		ra(raSliceJQ), 5, 1, nil, resolve)
	if err != nil || !ok {
		t.Fatalf("second empty-state serve failed: ok=%v err=%v", ok, err)
	}
	if calls.Load()-callsBefore != 2 {
		t.Fatalf("not-synced /call must re-run first-sight (2 resolves), got %d", calls.Load()-callsBefore)
	}
	if _, known := cache.SliceabilityLookup(raKey, shape); known {
		t.Fatalf("still empty: verdict MUST remain UNKNOWN")
	}

	// --- Informer syncs: panels now present. Self-heal on next /call. ---
	synced.Store(true)
	got2, ok, err := raFullListServe(ctx, gvr(), "krateo-system", raName,
		ra(raSliceJQ), 5, 1, nil, resolve)
	if err != nil || !ok {
		t.Fatalf("post-sync serve failed: ok=%v err=%v", ok, err)
	}
	// Non-empty now: 5 items (perPage=5, page=1).
	items, _ := got2["compositionspanels"].([]any)
	if len(items) != 5 {
		t.Fatalf("post-sync serve must return 5 items (perPage=5), got %d", len(items))
	}
	// Self-healed: verdict now recorded (sliceable) + cell cached.
	if sliceable, known := cache.SliceabilityLookup(raKey, shape); !known || !sliceable {
		t.Fatalf("post-sync first-sight MUST record sliceable verdict: known=%v sliceable=%v", known, sliceable)
	}
	if _, ok := cache.ResolvedCache().Get(raKey); !ok {
		t.Fatalf("post-sync non-empty full MUST Put the cell")
	}
	// Served bytes match the RA jq page-1 reference exactly (no regression).
	synced.Store(true)
	ref, _ := resolve(ctx, 5, 1)
	assertCanonEqual(t, got2, ref, "post-sync-page1")
}

// TestRAServe_EmptyBindingDeclines — #95 arch C-1: raFullListServe treats a
// re-derived BindingUID of "" as a decline (page-keyed fallback under the
// request's own identity), NEVER serving the shared ""-keyed RAFullList cell.
// This is the read-side leak one layer below the dispatcher guards: reachable
// via the dispatcher's own #95 fall-through (top-level "" MISS → direct resolve
// → resolveApiRef → raFullListServe re-derives "").
//
// Setup: NO RBAC snapshot published (ctxWithUser has identity but EvaluateRBAC
// finds no binding → bindingUID==""), then PRE-POPULATE a known-sliceable
// ""-keyed cell (representing what a broad ""-identity would have populated
// pre-fix). With C-1 the serve MUST still decline (served=false) despite the
// hit — proving the ""-guard fires BEFORE the known-sliceable hit path. RED arm
// = remove the `if bindingUID == "" { return }` guard → it serves the ""-cell.
func TestRAServe_EmptyBindingDeclines(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	// NOTE: NO newF6Watcher / no snapshot → EvaluateRBAC returns "" (the trigger).
	// Unique raName isolates this test from the process-global sliceability memo.

	const raName = "empty-binding-decline-ra"
	// The ""-keyed cell + a known-sliceable verdict a broad ""-identity would
	// have left behind (the shared cross-identity cell C-1 must not serve).
	shape := cache.SliceShapeHash(raFullListCallerClass, gvr().Group, gvr().Version,
		gvr().Resource, "krateo-system", raName, raSliceJQ)
	keyInputs := f6KeyInputs(gvr().Group, gvr().Version, gvr().Resource,
		"krateo-system", raName, "" /* the empty-identity key */, nil)
	raKey := cache.ComputeKey(keyInputs)
	cache.ResolvedCache().PutRAFullList(raKey, keyInputs, panelDict(40))
	cache.RecordSliceability(raKey, shape, true) // known-sliceable → would HIT
	if _, ok := cache.ResolvedCache().Get(raKey); !ok {
		t.Fatalf("setup: the representative empty-identity cell must be present")
	}

	var calls atomic.Int64
	resolve := stubResolveRA(t, panelDict(40), &calls)

	// C-1: raFullListServe re-derives bindingUID=="" → declines (served=false)
	// BEFORE the known-sliceable hit → the shared ""-cell is NEVER served.
	_, ok, err := raFullListServe(ctxWithUser(t), gvr(), "krateo-system", raName,
		ra(raSliceJQ), 10, 1, nil, resolve)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ok {
		t.Fatalf("#95 C-1 RED: raFullListServe SERVED an empty-BindingUID cell (ok=true) — the shared empty-identity RAFullList slice leaks cross-identity. The bindingUID-empty early-return must decline (served=false).")
	}
}

func assertCanonEqual(t *testing.T, a, b map[string]any, label string) {
	t.Helper()
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	if string(ab) != string(bb) {
		t.Fatalf("%s: served bytes != RA jq bytes\n  got: %s\n  ref: %s", label, ab, bb)
	}
}
