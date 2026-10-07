// ra_full_list_external_miss_test.go — M6 [SEC stale-forever] falsifier for
// the RAFullList known-sliceable CELL-MISS re-resolve path (ra_full_list.go
// lines ~316-342), the external-no-cache surface #4 on the repopulate branch.
//
// THE GAP the existing ra_full_list_test.go leaves open: the first-sight
// external-touched surface (lines ~359-370) IS covered by
// TestRAServe_NonSliceableFallsBack's sibling assertions, but the SECOND
// external surface — the one on the KNOWN-SLICEABLE cell-MISS repopulate path
// — is not. That path resolves the full list UNPAGINATED to repopulate an
// evicted cell; if that unpaginated re-resolve touches a genuine external
// endpoint, the code MUST serve the (correct, fresh) Go-slice but DECLINE the
// re-Put + the dep Record + record RAFullListServeFallback + BumpExternalSkippedPut
// (external data has no informer edge → caching it serves it stale forever).
//
// RED arm (howVerified): neuter the `extSink.Count() > 0` check on the
// repopulate branch → the external aggregate is Put into L1 (a dep edge is
// recorded, no BumpExternalSkippedPut) and a subsequent page HIT serves the
// stale cached slice. Proven by transiently editing ra_full_list.go to make
// the repopulate-branch external check always-false, observing this test RED
// (cell Put + dep edge present + ExternalSkippedPut delta 0), then restoring.

package apiref

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// TestRAServe_KnownSliceableCellMiss_ExternalTouched_DeclinesPut is the M6
// falsifier. It drives the KNOWN-SLICEABLE + cell-MISS repopulate branch with
// an unpaginated re-resolve that bumps the ExternalTouchedSink, and asserts
// the five load-bearing surface-#4 properties on that branch:
//
//	(1) served=true with the CORRECT page slice (fresh, page-keyed-equivalent),
//	(2) PutRAFullList NOT called (the cell stays absent after the serve),
//	(3) NO dep edge recorded for the raKey (external RA never enters DepTracker),
//	(4) RAFullListServeFallback serve-outcome fired (not RepopulateSlice),
//	(5) BumpExternalSkippedPut fired (ExternalSkippedPut delta == 1).
func TestRAServe_KnownSliceableCellMiss_ExternalTouched_DeclinesPut(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	// #95 arch C-1: publish the f6 grant so admin re-derives a REAL non-empty
	// BindingUID (C:crb-a-f6-uid) on the restactions GVR — the servable state
	// this branch requires (a "" binding declines before ever reaching here).
	newF6Watcher(t, f6BuildFixture()...)

	// Unique RA name so this test is self-isolating in the PROCESS-GLOBAL
	// sliceability memo — a verdict recorded by another test must never leak
	// here, and ours must not leak out. Since #471 ResetResolvedCacheForTest
	// clears the memo too (it is a derived index over the store that call
	// drops), so this is belt-and-braces, not the only mitigation.
	const raName = "compositions-panels-extmiss"

	// The raKey the serve path derives (seedFullListRAKey → RAFullListKeyInputs
	// → ComputeKey), keyed on admin's f6 first-match BindingUID on restactions.
	keyInputs := f6KeyInputs(gvr().Group, gvr().Version, gvr().Resource,
		"krateo-system", raName, "C:crb-a-f6-uid", nil)
	raKey := cache.ComputeKey(keyInputs)
	shape := cache.SliceShapeHash(raFullListCallerClass, gvr().Group, gvr().Version,
		gvr().Resource, "krateo-system", raName, raSliceJQ)

	// Pre-seed a KNOWN-SLICEABLE verdict but NO cell — so raFullListServe enters
	// the `known && sliceable` branch and takes the cell-MISS repopulate path.
	// (This is the steady-state-after-eviction posture: the verdict outlives the
	// cell because the sliceability memo is process-global while the L1 cell is
	// LRU/TTL-bounded.)
	cache.RecordSliceability(raKey, shape, true)
	if _, ok := cache.ResolvedCache().Get(raKey); ok {
		t.Fatalf("setup: no cell must be present (cell-miss repopulate path requires a MISS)")
	}
	if sliceable, known := cache.SliceabilityLookup(raKey, shape); !known || !sliceable {
		t.Fatalf("setup: verdict must be known-sliceable; known=%v sliceable=%v", known, sliceable)
	}

	// Snapshot the process-global external-skipped-Put + serve-outcome counters
	// so we assert DELTAS (both are process-wide atomics).
	skippedBefore := cache.ExternalSkippedPut()
	serveBefore := cache.RAFullListServeSnapshot()

	// The resolve stub bumps the ExternalTouchedSink attached to the ctx it is
	// called with (raFullListServe threads it down as fullCtx via WithL1KeyContext).
	// It bumps ONLY on the unpaginated (perPage==0 && page==0) re-resolve — the
	// exact call the repopulate branch makes — so we prove the external touch is
	// the trigger, not an unconditional bump.
	var calls atomic.Int64
	var unpaginatedResolves atomic.Int64
	base := stubResolveRA(t, panelDict(40), &calls)
	resolve := func(ctx context.Context, perPage, page int) (map[string]any, error) {
		if perPage == 0 && page == 0 {
			unpaginatedResolves.Add(1)
			cache.ExternalTouchedSinkFromContext(ctx).Bump() // nil-safe; fires on the fullCtx sink
		}
		return base(ctx, perPage, page)
	}

	// Install the external-touched sink on the request ctx (the request path is
	// the first ExternalTouchedSink consumer; raFullListServe inherits it via
	// fullCtx = WithL1KeyContext(ctx, raKey)).
	ctx, sink := cache.WithExternalTouchedSink(ctxWithUser(t))

	got, ok, err := raFullListServe(ctx, gvr(), "krateo-system", raName,
		ra(raSliceJQ), 10, 2, nil, resolve)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// (1) served=true with the correct slice.
	if !ok {
		t.Fatalf("external-touched repopulate MUST still SERVE the fresh slice (ok=true), not decline")
	}
	// The unpaginated re-resolve ran exactly once and touched the external sink.
	if n := unpaginatedResolves.Load(); n != 1 {
		t.Fatalf("repopulate path must resolve UNPAGINATED exactly once, got %d", n)
	}
	if sink.Count() == 0 {
		t.Fatalf("setup sanity: the unpaginated re-resolve must have bumped the external sink")
	}
	// The served bytes equal a fresh page-2 page-keyed resolve (the correct slice).
	ref, _ := base(ctx, 10, 2)
	assertCanonEqual(t, got, ref, "extmiss-page2")

	// (2) PutRAFullList NOT called — the cell must STILL be absent.
	if _, ok := cache.ResolvedCache().Get(raKey); ok {
		t.Fatalf("M6 RED: external-touched repopulate PUT the cell — an external aggregate with no informer edge is now cached and will serve STALE across pages")
	}

	// (3) NO dep edge for the raKey (external RA never enters the DepTracker).
	// The serve path records its self-dep on (gvr, ns, name); an external decline
	// must skip that Record so no informer event dirty-marks a non-existent cell.
	deps := cache.Deps().CollectMatchesForTest(gvr(), "krateo-system", raName)
	if _, present := deps[raKey]; present {
		t.Fatalf("M6 RED: a dep edge was recorded for the external-touched raKey — the external RA entered the DepTracker (no informer edge can invalidate it; the Record must be declined alongside the Put)")
	}

	// (4) RAFullListServeFallback fired (NOT RepopulateSlice — the external gate
	// is checked BEFORE the normal repopulate Put/serve).
	serveAfter := cache.RAFullListServeSnapshot()
	if serveAfter.Fallback-serveBefore.Fallback != 1 {
		t.Fatalf("expected exactly one RAFullListServeFallback outcome, got delta %d (before=%+v after=%+v)",
			serveAfter.Fallback-serveBefore.Fallback, serveBefore, serveAfter)
	}
	if serveAfter.Repopulate-serveBefore.Repopulate != 0 {
		t.Fatalf("M6 RED: RAFullListServeRepopulateSlice fired (delta %d) — the external gate did NOT fire, the code took the normal repopulate-Put path",
			serveAfter.Repopulate-serveBefore.Repopulate)
	}

	// (5) BumpExternalSkippedPut fired exactly once.
	if d := cache.ExternalSkippedPut() - skippedBefore; d != 1 {
		t.Fatalf("M6 RED: ExternalSkippedPut delta = %d, want 1 — the external-touched Put-decline metric did NOT fire on the repopulate branch", d)
	}
}
