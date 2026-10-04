// inline_extras_seed_parity_test.go — falsifier #6 for inline-extras design P
// (PM MUST-FIX #1). The biggest risk under P is the #317 seed/serve key-
// mismatch class: the PIP seed historically keyed widget + RAFullList cells on
// extras=nil at TWO surfaces, while the live dispatcher — after P folds the
// inline maps into its key-union (§1) — keys on the NON-EMPTY union. If the
// seed is not taught the same fold, every widget declaring inline extras MISSES
// its prewarmed cell and resolves cold on first paint.
//
// These tests assert seed-key == serve-key at BOTH cells by driving the EXACT
// production key computations:
//   - per-cohort widget cell: dispatchCacheLookupKey with the seed's
//     unionForKey(apiRefInline, rrtInline, nil) vs the dispatcher's
//     unionForKey(apiRefInline, rrtInline, <empty request>), then a real
//     handle.Put under the seed key + handle.Get under the serve key → HIT.
//   - RAFullList sub-cell: RAFullListKeyInputs with the seed's apiRefInline
//     (no request) vs the dispatcher's apiRef-effective map (apiRefInline under
//     an empty request) → byte-identical key → HIT.
//
// A miss at either cell proves divergence (the bug). The seed BODY parity is
// automatic (widgets.Resolve reads the inline maps off the seeded CR itself);
// only the two seed KEY args are computed outside Resolve, which is exactly
// what these tests cover.
package dispatchers

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
)

// seedKeyExtrasFor reproduces the seed's per-cohort key-extras fold
// (phase1_pip_seed.go: unionForKey(GetApiRefExtras, GetResourcesRefsExtras,
// nil) — the seed has NO request extras).
func seedKeyExtrasFor(crObj map[string]any) map[string]any {
	return unionForKey(
		widgets.GetApiRefExtras(crObj),
		widgets.GetResourcesRefsExtras(crObj),
		nil,
	)
}

// TestInlineExtras_6_SeedParity_PerCohortWidgetCell — falsifier #6, cell 1.
// Seed a widget carrying BOTH inline maps under the SEED's per-cohort key, then
// look up under the DISPATCHER's serve-time key (no ?extras=) → MUST HIT.
func TestInlineExtras_6_SeedParity_PerCohortWidgetCell(t *testing.T) {
	enableWidgetContentL1(t)
	ctx := ctxWithIdentity()
	const (
		g, v, r, ns, name = "widgets.templates.krateo.io", "v1beta1", "buttons", "demo-system", "btn-seed"
		perPage, page     = 10, 1
	)
	cr := widgetCRWithExtras(t, `{"tenant":"acme"}`, `{"targetNs":"team-a"}`)

	// SEED key: union with NIL request (the seed has no request extras).
	seedKey, seedHandle, seedInputs := dispatchCacheLookupKey(ctx, "widgets",
		g, v, r, ns, name, perPage, page, seedKeyExtrasFor(cr))
	if seedHandle == nil || seedKey == "" {
		t.Fatal("expected a live per-cohort cache handle + key under the seed ctx")
	}
	body := []byte(`{"status":{"widgetData":{"label":"acme"}}}`)
	seedHandle.Put(seedKey, &cache.ResolvedEntry{RawJSON: body, Inputs: seedInputs})

	// SERVE key: union with the EMPTY request extras (no ?extras= on the /call).
	serveKey, serveHandle, _ := dispatchCacheLookupKey(ctx, "widgets",
		g, v, r, ns, name, perPage, page, keyExtrasFor(cr, map[string]any{}))
	if serveHandle == nil || serveKey == "" {
		t.Fatal("expected a live per-cohort cache handle + key under the serve ctx")
	}
	if seedKey != serveKey {
		t.Fatalf("FALSIFIER #6 FAILED (per-cohort cell): seed key %q != serve key %q — "+
			"a widget declaring inline extras would MISS its prewarmed cell and resolve cold (the #317 mismatch class)", seedKey, serveKey)
	}
	got, hit := serveHandle.Get(serveKey)
	if !hit {
		t.Fatal("FALSIFIER #6 FAILED (per-cohort cell): serve-key lookup MISSED the seeded cell — seed/serve key divergence")
	}
	if string(got.RawJSON) != string(body) {
		t.Fatalf("per-cohort cell served the wrong body: got %q want %q", got.RawJSON, body)
	}
}

// TestInlineExtras_6_SeedParity_RAFullListSubCell — falsifier #6, cell 2.
// The RAFullList sub-cell key the seed folds (apiRefInline, no request) MUST
// byte-match the dispatcher's apiRef-effective key (apiRefInline under an empty
// request), so the seeded full-list cell is HIT on the first paginated /call.
// The resourcesRefsTemplateExtras map MUST NOT perturb this cell (§1).
func TestInlineExtras_6_SeedParity_RAFullListSubCell(t *testing.T) {
	enableWidgetContentL1(t)
	const (
		rg, rv, rr, rns, rname = "templates.krateo.io", "v1", "restactions", "demo-system", "some-ra"
		bUID                   = "binding-xyz"
	)
	// A widget with apiRef.extras AND an rrt block — the rrt block must be
	// invisible to the RAFullList key.
	cr := widgetCRWithExtras(t, `{"tenant":"acme"}`, `{"targetNs":"team-a"}`)

	// SEED RAFullList key: seedRAFullListForWidget threads GetApiRefExtras(w)
	// (no request) as apiref.Resolve Extras → RAFullListKeyInputsForTest(..., apiRefInline).
	seedRAKey := cache.ComputeKey(cache.RAFullListKeyInputsForTest(rg, rv, rr, rns, rname, bUID,
		widgets.GetApiRefExtras(cr)))

	// SERVE RAFullList key: the dispatcher's apiRef path threads
	// merge(apiRefInline, request) — with no ?extras= the request is empty, so
	// the effective map IS apiRefInline.
	serveRAKey := cache.ComputeKey(cache.RAFullListKeyInputsForTest(rg, rv, rr, rns, rname, bUID,
		keyExtrasForApiRefEffective(t, cr, map[string]any{})))

	if seedRAKey != serveRAKey {
		t.Fatalf("FALSIFIER #6 FAILED (RAFullList sub-cell): seed key %q != serve key %q — "+
			"the prewarmed RAFullList cell would be MISSED on the first paginated /call", seedRAKey, serveRAKey)
	}

	// Sanity: a real Put-under-seed / Get-under-serve hits (drives the real
	// handle, not just key equality).
	c := cache.ResolvedCache()
	if c == nil {
		t.Fatal("expected a live resolved cache")
	}
	full := map[string]any{"items": []any{map[string]any{"x": float64(1)}}}
	c.PutRAFullList(seedRAKey, cache.RAFullListKeyInputsForTest(rg, rv, rr, rns, rname, bUID,
		widgets.GetApiRefExtras(cr)), full)
	if _, hit := c.Get(serveRAKey); !hit {
		t.Fatal("FALSIFIER #6 FAILED (RAFullList sub-cell): serve-key lookup MISSED the seeded full-list cell")
	}
}
