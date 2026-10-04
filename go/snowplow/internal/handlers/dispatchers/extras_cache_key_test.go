// extras_cache_key_test.go — load-bearing cache falsifier for the
// extras→widgets parity feature.
//
// The feature makes a widget's resolved output depend on the per-request
// `extras`. That output is cached in the widget L1 layers. If the cache key
// did NOT already fold `extras`, two requests for the same widget with
// DIFFERENT extras would collide on one cell and the second requester would
// be served the first requester's body — a cross-request correctness defect.
//
// The plan's verify-don't-assume gate asserts both widget L1 keys already
// include extras (dispatchWidgetContentKey → ComputeKey, and
// dispatchCacheLookupKey → ComputeKey). These tests PROVE it through the
// widget dispatcher's own key-builder (not just the raw ResolvedKeyInputs)
// and guard against regression. We target the IDENTITY-FREE widgetContent key
// because that is the SHARED cell where the cross-request collision risk
// actually lives (the per-cohort key additionally folds BindingUID; the
// content key omits identity entirely, so extras is the ONLY per-request
// discriminant beyond the gvr/ns/name/pagination tuple).
package dispatchers

import (
	"context"
	"encoding/json"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
)

// enableWidgetContentL1 turns the whole resolved-output + widgetContent L1
// stack on for one test and resets the singleton around it. Mirrors the
// setup in widget_content_empty_shell_falsifier_test.go.
func enableWidgetContentL1(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("WIDGET_CONTENT_L1_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
}

// parseExtras mirrors internal/handlers/util.ParseExtras — the production
// extras map is json.Unmarshal'd from the ?extras= query param, so values are
// JSON-native types. Building fixtures this way keeps the key inputs
// byte-identical to the real dispatch path.
func parseExtras(t *testing.T, jsonExtras string) map[string]any {
	t.Helper()
	out := map[string]any{}
	if jsonExtras == "" {
		return out
	}
	if err := json.Unmarshal([]byte(jsonExtras), &out); err != nil {
		t.Fatalf("parseExtras: %v", err)
	}
	return out
}

// TestExtras_WidgetContentKey_DiffersByExtras is THE falsifier: the same
// widget tuple (gvr/ns/name/pagination) with DIFFERENT extras MUST produce
// DIFFERENT identity-free content keys — otherwise the two requests collide
// on one shared cell and leak each other's body cross-request.
func TestExtras_WidgetContentKey_DiffersByExtras(t *testing.T) {
	enableWidgetContentL1(t)

	ctx := context.Background()
	const (
		g, v, r, ns, name = "widgets.templates.krateo.io", "v1beta1", "buttons", "demo-system", "btn-1"
		perPage, page     = 10, 1
	)

	keyA, handleA, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page,
		parseExtras(t, `{"tenant":"acme"}`))
	keyB, handleB, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page,
		parseExtras(t, `{"tenant":"globex"}`))

	if handleA == nil || handleB == nil {
		t.Fatalf("widgetContent L1 must be enabled (got handles A=%v B=%v)", handleA, handleB)
	}
	if keyA == "" || keyB == "" {
		t.Fatalf("expected non-empty content keys, got A=%q B=%q", keyA, keyB)
	}
	if keyA == keyB {
		t.Fatalf("FALSIFIER FAILED: same widget + DIFFERENT extras produced the SAME content key %q — "+
			"two requests would collide on one identity-free cell and serve each other's body", keyA)
	}
}

// TestExtras_WidgetContentKey_StableForSameExtras — caching requires the key
// to be DETERMINISTIC for identical inputs (incl. extras), and INSENSITIVE to
// extras-map iteration/JSON key order (ComputeKey canonicalises via
// sorted-key JSON). Same extras (order-shuffled) ⇒ same key ⇒ a real hit.
func TestExtras_WidgetContentKey_StableForSameExtras(t *testing.T) {
	enableWidgetContentL1(t)
	ctx := context.Background()
	const (
		g, v, r, ns, name = "widgets.templates.krateo.io", "v1beta1", "buttons", "demo-system", "btn-1"
		perPage, page     = 10, 1
	)

	key1, _, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page,
		parseExtras(t, `{"tenant":"acme","region":"eu"}`))
	// Same content, different JSON key order — must canonicalise identically.
	key2, _, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page,
		parseExtras(t, `{"region":"eu","tenant":"acme"}`))

	if key1 != key2 {
		t.Fatalf("same extras (key order shuffled) must produce the SAME content key, got %q vs %q", key1, key2)
	}
}

// TestExtras_WidgetContentKey_EmptyEqualsNil — backward-compat: a request with
// no extras param (nil) and one with the non-nil empty map ParseExtras returns
// for absent ?extras must key IDENTICALLY, and identically to today's
// no-extras key. ComputeKey folds extras only when len>0, so the empty/nil
// fold writes nothing — proving the no-extras path is byte-identical to
// pre-feature.
func TestExtras_WidgetContentKey_EmptyEqualsNil(t *testing.T) {
	enableWidgetContentL1(t)
	ctx := context.Background()
	const (
		g, v, r, ns, name = "widgets.templates.krateo.io", "v1beta1", "buttons", "demo-system", "btn-1"
		perPage, page     = 10, 1
	)

	keyNil, _, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page, nil)
	keyEmpty, _, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page, map[string]any{})

	if keyNil != keyEmpty {
		t.Fatalf("nil extras and empty-map extras must key identically (backward-compat), got %q vs %q", keyNil, keyEmpty)
	}
}

// TestExtras_WidgetContentCell_NoCrossCollision is the end-to-end serve proof:
// store a body under extras-A's key, then look up extras-B's key — it MUST
// miss (so request B never serves A's cached body), and a second lookup of
// extras-A's key MUST hit (so caching still works). This exercises the real
// cache handle Put/Get, not just key equality.
func TestExtras_WidgetContentCell_NoCrossCollision(t *testing.T) {
	enableWidgetContentL1(t)
	ctx := context.Background()
	const (
		g, v, r, ns, name = "widgets.templates.krateo.io", "v1beta1", "buttons", "demo-system", "btn-1"
		perPage, page     = 10, 1
	)

	keyA, handle, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page,
		parseExtras(t, `{"tenant":"acme"}`))
	keyB, _, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page,
		parseExtras(t, `{"tenant":"globex"}`))
	if handle == nil {
		t.Fatal("expected a live widgetContent cache handle")
	}

	bodyA := []byte(`{"status":{"widgetData":{"tenant":"acme"}}}`)
	handle.Put(keyA, &cache.ResolvedEntry{RawJSON: bodyA})

	// Request B (different extras) must NOT hit A's cell.
	if _, hit := handle.Get(keyB); hit {
		t.Fatalf("FALSIFIER FAILED: extras-B lookup hit a cell stored under extras-A — cross-request L1 collision")
	}
	// Request A (same extras) must hit, and serve exactly A's body.
	got, hit := handle.Get(keyA)
	if !hit {
		t.Fatal("extras-A lookup must hit the cell it stored")
	}
	if string(got.RawJSON) != string(bodyA) {
		t.Fatalf("extras-A cell served the wrong body: got %q want %q", got.RawJSON, bodyA)
	}
}

// ===========================================================================
// inline-extras design P — cache-key crux falsifiers (#4a / #4b / #8 / #9).
//
// These extend the request-extras analogues ABOVE to the per-surface inline
// maps. Per PM MUST-FIX #2 they drive the REAL dispatcher key path — the
// production unionForKey builder + dispatchWidgetContentKey /
// dispatchCacheLookupKey + RAFullListKeyInputs + real handle.Put/Get — NOT a
// hand-assembled ResolvedKeyInputs. The widget cell folds the UNION of both
// inline maps + request; the apiRef sub-cell folds ONLY the apiRef-effective
// map (apiRef-inline under request) — never rrt-inline (design §1).
//
// Per Caveat A (the dominant apiRef+template widget is RBAC-sensitive so its
// identity-free content cell is SKIPPED): #4a frames distinctness around the
// per-cohort + RAFullList cells (the real surface for an apiRef widget), and
// the content-cell distinctness is exercised by an apiRef-LESS widget carrying
// resourcesRefsTemplateExtras (#4b) — the shape that actually routes through
// the content cell.
// ===========================================================================

// widgetCRWithExtras builds the unstructured widget-CR Object shape the
// dispatcher reads the two inline maps off (spec.apiRef.extras +
// spec.resourcesRefsTemplateExtras). Either arg may be nil/"" to omit that
// block — exactly the absence the accessors return {} for.
func widgetCRWithExtras(t *testing.T, apiRefExtrasJSON, rrtExtrasJSON string) map[string]any {
	t.Helper()
	spec := map[string]any{}
	if apiRefExtrasJSON != "" {
		spec["apiRef"] = map[string]any{
			"name":      "some-ra",
			"namespace": "demo-system",
			"extras":    parseExtras(t, apiRefExtrasJSON),
		}
	}
	if rrtExtrasJSON != "" {
		spec["resourcesRefsTemplateExtras"] = parseExtras(t, rrtExtrasJSON)
	}
	return map[string]any{"spec": spec}
}

// keyExtrasFor mirrors the dispatcher's production fold (widgets.go): read both
// inline maps off the CR via the REAL accessors and union them with the request
// extras (request wins). This is the exact keyExtras the dispatcher passes to
// the widget key sites.
func keyExtrasFor(crObj map[string]any, request map[string]any) map[string]any {
	return unionForKey(
		widgets.GetApiRefExtras(crObj),
		widgets.GetResourcesRefsExtras(crObj),
		request,
	)
}

// ctxWithIdentity returns a context carrying a UserInfo so the per-cohort
// dispatchCacheLookupKey path (which reads xcontext.UserInfo and derives a
// BindingUID via rbac.EvaluateRBAC) returns a live handle + key instead of
// bailing on a missing identity. With no ResourceWatcher wired in these unit
// tests EvaluateRBAC degrades to bindingUID="" (fail-closed) — identical for
// every call here, so the keys differ ONLY by the extras union, which is
// exactly the discriminant these falsifiers probe.
func ctxWithIdentity() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "cyberjoker", Groups: []string{"devs"}}))
}

// TestInlineExtras_4a_ApiRefExtras_DistinctWidgetKeys — falsifier #4a. Two
// widgets identical except spec.apiRef.extras MUST produce DISTINCT per-cohort
// widget keys (the apiRef+template widget is RBAC-sensitive so we frame on the
// per-cohort cell, Caveat A) AND DISTINCT RAFullList sub-cell keys; identical
// apiRef.extras ⇒ identical keys (a real hit). A collision here would serve
// one widget's apiRef-parametrised body to the other.
func TestInlineExtras_4a_ApiRefExtras_DistinctWidgetKeys(t *testing.T) {
	enableWidgetContentL1(t)
	ctx := ctxWithIdentity()
	const (
		g, v, r, ns, name = "widgets.templates.krateo.io", "v1beta1", "buttons", "demo-system", "btn-apiref"
		perPage, page     = 10, 1
	)
	// No request extras — the inline map is the sole discriminant.
	req := map[string]any{}

	crA := widgetCRWithExtras(t, `{"tenant":"acme"}`, "")
	crB := widgetCRWithExtras(t, `{"tenant":"globex"}`, "")
	crA2 := widgetCRWithExtras(t, `{"tenant":"acme"}`, "") // same as A

	// --- per-cohort widget cell (the real surface for an apiRef widget) ---
	pcA, _, _ := dispatchCacheLookupKey(ctx, "widgets", g, v, r, ns, name, perPage, page, keyExtrasFor(crA, req))
	pcB, _, _ := dispatchCacheLookupKey(ctx, "widgets", g, v, r, ns, name, perPage, page, keyExtrasFor(crB, req))
	pcA2, _, _ := dispatchCacheLookupKey(ctx, "widgets", g, v, r, ns, name, perPage, page, keyExtrasFor(crA2, req))
	if pcA == "" || pcB == "" {
		t.Fatalf("expected non-empty per-cohort keys (got A=%q B=%q)", pcA, pcB)
	}
	if pcA == pcB {
		t.Fatalf("FALSIFIER #4a FAILED: differing apiRef.extras produced the SAME per-cohort widget key %q — cross-widget collision", pcA)
	}
	if pcA != pcA2 {
		t.Fatalf("#4a: identical apiRef.extras MUST produce identical per-cohort keys, got %q vs %q", pcA, pcA2)
	}

	// --- RAFullList sub-cell (apiRef-effective map = apiRef-inline ∪ request) ---
	// The dispatcher's apiRef path threads merge(apiRefInline, request) into
	// apiref.Resolve → RAFullListKeyInputs. Here request is empty, so the
	// effective map IS the apiRef-inline map.
	const rg, rv, rr, rns, rname = "templates.krateo.io", "v1", "restactions", "demo-system", "some-ra"
	const bUID = "binding-xyz"
	raA := cache.ComputeKey(cache.RAFullListKeyInputsForTest(rg, rv, rr, rns, rname, bUID,
		keyExtrasForApiRefEffective(t, crA, req)))
	raB := cache.ComputeKey(cache.RAFullListKeyInputsForTest(rg, rv, rr, rns, rname, bUID,
		keyExtrasForApiRefEffective(t, crB, req)))
	raA2 := cache.ComputeKey(cache.RAFullListKeyInputsForTest(rg, rv, rr, rns, rname, bUID,
		keyExtrasForApiRefEffective(t, crA2, req)))
	if raA == raB {
		t.Fatalf("FALSIFIER #4a FAILED: differing apiRef.extras produced the SAME RAFullList sub-cell key %q — apiRef fetch collision", raA)
	}
	if raA != raA2 {
		t.Fatalf("#4a: identical apiRef.extras MUST produce identical RAFullList keys, got %q vs %q", raA, raA2)
	}
}

// keyExtrasForApiRefEffective models the apiRef-EFFECTIVE map the dispatcher
// threads into the apiRef sub-cell: the apiRef-inline map folded UNDER the
// request (request wins) — and crucially WITHOUT the rrt-inline map. It is the
// test-side mirror of resolveApiRef's merge(apiRefInline, request).
func keyExtrasForApiRefEffective(t *testing.T, crObj map[string]any, request map[string]any) map[string]any {
	t.Helper()
	apiRefInline := widgets.GetApiRefExtras(crObj)
	out := map[string]any{}
	for k, v := range apiRefInline {
		out[k] = v
	}
	for k, v := range request { // request wins
		out[k] = v
	}
	return out
}

// TestInlineExtras_4b_RrtExtras_DistinctContentKeys_RAFullListUnchanged —
// falsifier #4b. An apiRef-LESS widget (routes through the identity-free
// content cell, Caveat A) carrying resourcesRefsTemplateExtras: two such
// widgets differing ONLY in the rrt block MUST produce DISTINCT content +
// per-cohort widget keys (the union folds rrt-inline), while the RAFullList
// sub-cell key MUST be UNCHANGED (rrt-inline does NOT affect the apiRef cell).
func TestInlineExtras_4b_RrtExtras_DistinctContentKeys_RAFullListUnchanged(t *testing.T) {
	enableWidgetContentL1(t)
	ctx := ctxWithIdentity()
	const (
		g, v, r, ns, name = "widgets.templates.krateo.io", "v1beta1", "buttons", "demo-system", "btn-static"
		perPage, page     = 10, 1
	)
	req := map[string]any{}

	crA := widgetCRWithExtras(t, "", `{"targetNs":"team-a"}`)
	crB := widgetCRWithExtras(t, "", `{"targetNs":"team-b"}`)
	crA2 := widgetCRWithExtras(t, "", `{"targetNs":"team-a"}`)

	// --- identity-free content cell (apiRef-less widget routes here) ---
	cA, hA, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page, keyExtrasFor(crA, req))
	cB, _, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page, keyExtrasFor(crB, req))
	cA2, _, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page, keyExtrasFor(crA2, req))
	if hA == nil {
		t.Fatal("widgetContent L1 must be enabled")
	}
	if cA == cB {
		t.Fatalf("FALSIFIER #4b FAILED: differing resourcesRefsTemplateExtras produced the SAME content key %q — cross-widget collision on the identity-free cell", cA)
	}
	if cA != cA2 {
		t.Fatalf("#4b: identical rrt extras MUST produce identical content keys, got %q vs %q", cA, cA2)
	}

	// per-cohort cell must ALSO distinguish (the union folds rrt-inline there too).
	pcA, _, _ := dispatchCacheLookupKey(ctx, "widgets", g, v, r, ns, name, perPage, page, keyExtrasFor(crA, req))
	pcB, _, _ := dispatchCacheLookupKey(ctx, "widgets", g, v, r, ns, name, perPage, page, keyExtrasFor(crB, req))
	if pcA == pcB {
		t.Fatalf("FALSIFIER #4b FAILED: differing rrt extras produced the SAME per-cohort key %q", pcA)
	}

	// --- RAFullList sub-cell MUST be UNCHANGED across differing rrt extras ---
	// rrt-inline does not reach the apiRef-effective map (design §1), so the
	// apiRef sub-cell key is invariant to it.
	const rg, rv, rr, rns, rname = "templates.krateo.io", "v1", "restactions", "demo-system", "some-ra"
	const bUID = "binding-xyz"
	raA := cache.ComputeKey(cache.RAFullListKeyInputsForTest(rg, rv, rr, rns, rname, bUID,
		keyExtrasForApiRefEffective(t, crA, req)))
	raB := cache.ComputeKey(cache.RAFullListKeyInputsForTest(rg, rv, rr, rns, rname, bUID,
		keyExtrasForApiRefEffective(t, crB, req)))
	if raA != raB {
		t.Fatalf("FALSIFIER #4b FAILED: differing resourcesRefsTemplateExtras perturbed the RAFullList sub-cell key (%q vs %q) — rrt-inline MUST NOT key the apiRef cell", raA, raB)
	}
	// Caveat B (one line): RAFullListKeyInputs strips {slice,page,perPage,offset}
	// via extrasMinusSlice, so an author declaring one of those four RESERVED
	// pagination names in apiRef.extras won't perturb the RAFullList key.
	// Negligible (reserved names), pre-existing for request-extras — noted only.
}

// TestInlineExtras_8_BackwardCompat_ByteIdenticalKeys — falsifier #8 (key
// half). A widget CR with NEITHER inline block ⇒ keyExtras == the request
// extras ⇒ the widget content + per-cohort keys are BYTE-IDENTICAL to a
// pre-inline-extras request that passed the bare request extras. With no
// request extras either, both are byte-identical to the no-extras key.
func TestInlineExtras_8_BackwardCompat_ByteIdenticalKeys(t *testing.T) {
	enableWidgetContentL1(t)
	ctx := ctxWithIdentity()
	const (
		g, v, r, ns, name = "widgets.templates.krateo.io", "v1beta1", "buttons", "demo-system", "btn-bc"
		perPage, page     = 10, 1
	)

	bare := map[string]any{} // a CR with no inline blocks
	noInlineCR := widgetCRWithExtras(t, "", "")

	// (a) no inline blocks + no request extras == today's no-extras key.
	keyUnion, _, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page, keyExtrasFor(noInlineCR, bare))
	keyLegacy, _, _ := dispatchWidgetContentKey(ctx, g, v, r, ns, name, perPage, page, bare)
	if keyUnion != keyLegacy {
		t.Fatalf("FALSIFIER #8 FAILED: absent-inline-blocks content key %q != pre-feature bare-extras key %q", keyUnion, keyLegacy)
	}

	// (b) no inline blocks + a REQUEST extras == the legacy request-extras key
	// (the union degenerates to exactly the request map).
	reqX := parseExtras(t, `{"tenant":"acme"}`)
	pcUnion, _, _ := dispatchCacheLookupKey(ctx, "widgets", g, v, r, ns, name, perPage, page, keyExtrasFor(noInlineCR, reqX))
	pcLegacy, _, _ := dispatchCacheLookupKey(ctx, "widgets", g, v, r, ns, name, perPage, page, reqX)
	if pcUnion != pcLegacy {
		t.Fatalf("FALSIFIER #8 FAILED: absent-inline-blocks + request-extras per-cohort key %q != legacy request-extras key %q", pcUnion, pcLegacy)
	}
}

// TestInlineExtras_9_UnionForKey_Race — falsifier #9 (key builder half). The
// dispatcher builds keyExtras concurrently across N in-flight /calls. Run
// unionForKey + the accessors + the real key builders concurrently under
// `-race`; the shared widget CR map (read by the accessors) must never be
// aliased or mutated, and concurrent builds for the SAME inputs must produce
// the SAME key (deterministic). A data race here = the shared-vs-copy hazard.
func TestInlineExtras_9_UnionForKey_Race(t *testing.T) {
	enableWidgetContentL1(t)
	ctx := ctxWithIdentity()
	const (
		g, v, r, ns, name = "widgets.templates.krateo.io", "v1beta1", "buttons", "demo-system", "btn-race"
		perPage, page     = 10, 1
	)
	// One SHARED CR object — concurrently read by every goroutine's accessor.
	sharedCR := widgetCRWithExtras(t, `{"tenant":"acme","nested":{"k":"v"}}`, `{"targetNs":"team-a"}`)
	req := parseExtras(t, `{"region":"eu"}`)

	want, _, _ := dispatchCacheLookupKey(ctx, "widgets", g, v, r, ns, name, perPage, page, keyExtrasFor(sharedCR, req))

	const goroutines = 32
	errs := make(chan string, goroutines)
	done := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			ke := keyExtrasFor(sharedCR, req)
			// Mutate THIS goroutine's union — must not bleed into the shared CR
			// (the accessors return deep copies; unionForKey returns a fresh map).
			ke["scratch"] = "mutated"
			got, _, _ := dispatchCacheLookupKey(ctx, "widgets", g, v, r, ns, name, perPage, page, keyExtrasFor(sharedCR, req))
			if got != want {
				errs <- got
			}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}
	close(errs)
	if bad, ok := <-errs; ok {
		t.Fatalf("FALSIFIER #9 FAILED: concurrent key build for identical inputs diverged (got %q want %q) — shared CR aliased/mutated", bad, want)
	}
	// The shared CR's apiRef.extras must be untouched after all the concurrent
	// unions + scratch mutations.
	if got := widgets.GetApiRefExtras(sharedCR)["tenant"]; got != "acme" {
		t.Fatalf("FALSIFIER #9 FAILED: shared CR apiRef.extras was mutated by a concurrent union (tenant=%v)", got)
	}
}
