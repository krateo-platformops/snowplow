// ra_full_list.go — Ship 4a (0.30.198): the page-independent RAFullList
// serve path at the apiRef chokepoint.
//
// THE CHOKEPOINT. Every widget→RESTAction read funnels through
// apiref.Resolve (resolve.go), which resolves the RA at the request's
// PerPage/Page and returns the RA's Status map. Today each page re-resolves
// the whole RA (62-namespace fan-out + 49K sort + deep-copy + gojq
// recompile) because nothing caches the RA result page-independently. Ship
// 4a caches the RA's FULL result ONCE per (RA × cohort × non-slice extras)
// resolved UNPAGINATED, and serves each page as a cheap Go-slice over that
// one cached array.
//
// PER-/CALL SLICEABILITY VERDICT (design §3). On first sight of a
// (RAFullList-key × sliceShape) we byte-VERIFY: the Go-slice over the
// freshly-resolved full F must byte-match a fresh page-keyed resolve, AND
// the full F itself must byte-match a fresh unpaginated resolve. Match →
// memo sliceable, serve the Go-slice. Differ → memo NOT-sliceable for THIS
// shape → fall back to the page-keyed resolve forever (never a wrong
// result). The verdict is process-local; re-evaluated per new shape + on
// pod restart.
//
// REMOVABILITY (project_caching_is_provisional): this file + the single
// branch in resolve.go + the cache-package resident region are the entire
// 4a surface — wholesale-deletable. Gated under cache.ResolvedCacheEnabled()
// (CACHE_ENABLED); flag-off it is never entered.

package apiref

import (
	"context"
	"encoding/json"
	"slices"
	"sync/atomic"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/ptr"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// raFullListCallerClass is the sliceShape caller-class marker for the
// apiRef-chokepoint serve path. It keeps the apiRef path's sliceability
// verdict SEPARATE from the direct-RA dispatcher path (which uses
// "restactions") so a verdict verified on one path never auto-applies to
// the other — the two paths inject the slice at different layers, so their
// byte-verify must be independent (design §3 "different sliceShape").
const raFullListCallerClass = "apiref"

// SeedFullListShapeKnownNonSliceable is the #42 FIX-B seed pre-check. It
// reports whether the apiRef→RESTAction sliceShape has ALREADY been proven
// structurally non-sliceable (Class C, false+permanent) under ANY identity —
// via the identity-free shape-level negative set (cache FIX-A). When true, the
// seed's fallback resolve in seedRAFullListForWidget is pure waste (its result
// is discarded — raFullListServe would return served=false and apiref.Resolve
// would fall through to a page-keyed resolve whose output the seed throws
// away), so the caller SKIPS the whole resolve.
//
// INVARIANT (load-bearing — must NEVER diverge from raFullListServe): this
// derives the sliceShape with the SAME const (raFullListCallerClass) and the
// SAME inputs (gvr.{G,V,R}, namespace, name, ptr.Deref(ra.Spec.Filter)) that
// raFullListServe uses at line ~121. Co-located in THIS file with the const +
// the serve path precisely so the two cannot drift. If they ever diverge the
// pre-check either OVER-skips (a genuinely-seedable shape is skipped → a
// missed seed) or UNDER-skips (a non-sliceable shape still runs the wasted
// resolve #3). The FIX-B falsifier exercises this THROUGH seedRAFullListForWidget
// (the real path), NOT by calling this helper directly, so a drift surfaces as
// a test failure rather than only a comment violation.
//
// gvr/namespace/name identify the RESTAction CR (res.GVR + the apiRef); ra is
// the typed RESTAction (Spec.Filter is the slice-jq). Mirrors raFullListServe's
// shape derivation exactly.
func SeedFullListShapeKnownNonSliceable(gvr schema.GroupVersionResource,
	namespace, name string, ra *templatesv1.RESTAction) bool {
	if ra == nil {
		return false
	}
	return cache.SliceabilityShapeKnownNegative(seedFullListShape(gvr, namespace, name, ra))
}

// seedFullListShape is the ONE shape-derivation both the pre-check and the
// serve path (raFullListServe) express — the SINGLE SOURCE OF TRUTH for the
// sliceShape (raFullListCallerClass + gvr + ns/name + slice-jq). Keeping it a
// function (not two inline expressions) makes the invariant enforceable: the
// FIX-B falsifier records the negative through this SAME derivation, so a drift
// would surface as the skip not firing.
func seedFullListShape(gvr schema.GroupVersionResource, namespace, name string, ra *templatesv1.RESTAction) string {
	sliceJQ := ptr.Deref(ra.Spec.Filter, "")
	return cache.SliceShapeHash(raFullListCallerClass, gvr.Group, gvr.Version,
		gvr.Resource, namespace, name, sliceJQ)
}

// SeedFullListPerKeyKnownNonSliceable is the #42 FIX-G seed pre-check — the
// PER-KEY (caller's-own-identity) sibling of the FIX-B SHAPE-level pre-check.
// It reports whether THIS identity's (raKey × sliceShape) verdict is already
// known-and-NOT-sliceable — PERMANENT OR NOT. The caller's widgets.Resolve
// first-sight (which ran moments earlier in the SAME seedOneWidget call, BEFORE
// seedRAFullListForWidget) records that per-key verdict synchronously; so at
// this point THIS identity's own verdict is available and, if it's a plain
// (non-permanent) false, FIX-B's shape-negative set does NOT cover it (FIX-B
// keys off permanent-only per the C-1 safety ruling). Consulting the per-key
// verdict skips the discarded resolve #3 for those non-permanent negatives too
// — the ~140s/rank the trace attributed to un-skipped heavy list-shaped RAs.
//
// SAFE: a known&&!sliceable verdict (permanent or not) means raFullListServe
// would return served=false and apiref.Resolve would fall through to a
// page-keyed resolve whose result seedRAFullListForWidget DISCARDS — so
// skipping it changes NOTHING except eliminating the waste (same discarded-
// result argument as FIX-B; TRACED side-effect-free). Lever-A per-key retry is
// UNTOUCHED — the verdict entry exists regardless; we only read it.
//
// INVARIANT (must NEVER drift from raFullListServe): the raKey is derived by
// the SHARED seedFullListRAKey — the SINGLE derivation over the FULL INPUT
// TUPLE (EvaluateRBAC on the RA-CR coordinates → first-match bindingUID →
// RAFullListKeyInputs → ComputeKey) that raFullListServe itself calls. G2-A
// (the eg2 defect fix): the earlier eg1 impl folded a bindingUID threaded out
// of seedOneWidget — derived by EvaluateRBAC on the WIDGET's coordinates, a
// DIFFERENT candidate set → likely a different first-match → lookup miss → skip
// never fired → G inert (the feedback_key_parity_golden_real_inputs_prehash_diff
// class). The fix is single-source over the WHOLE derivation, not just the hash
// function: pre-check + serve compute the raKey the SAME way, off the RA-CR
// coordinates, so a divergence is structurally impossible. extras is the
// apiRef-inline effective map (same the seed passes to apiref.Resolve). "" from
// the fail-closed EvaluateRBAC disables the per-key check (raKey would be the
// shared empty-identity cell — never consult it, #95 semantics). The FIX-G G2-B
// falsifier drives BOTH the record and this pre-check through the REAL RA-CR
// derivation with a DIVERGENT widget-vs-RA first-match, so the eg1 widget-keyed
// variant goes RED (skip doesn't fire) while eg2 goes GREEN.
func SeedFullListPerKeyKnownNonSliceable(ctx context.Context, gvr schema.GroupVersionResource,
	namespace, name string, ra *templatesv1.RESTAction, extras map[string]any) bool {
	if ra == nil {
		return false
	}
	_, raKey, ok := seedFullListRAKey(ctx, gvr, namespace, name, extras)
	if !ok {
		return false // no identity / fail-closed "" → don't consult the shared cell
	}
	shape := seedFullListShape(gvr, namespace, name, ra)
	sliceable, known := cache.SliceabilityLookupCtx(ctx, raKey, shape)
	return known && !sliceable
}

// seedFullListRAKey is the SINGLE SOURCE OF TRUTH for the RAFullList cell key
// (G2-A). It runs EvaluateRBAC on the RA-CR COORDINATES (gvr, ns, name — the
// resource the RAFullList cell is keyed on), folds the first-match bindingUID +
// extras into RAFullListKeyInputs, and returns BOTH the keyInputs and the
// ComputeKey'd raKey. raFullListServe (the producer — records the verdict + Puts
// the cell under this key) AND the FIX-G pre-check (the consumer — looks the
// verdict up) both call THIS, so producer/consumer can never key-diverge (the
// eg1 defect was the pre-check keying off a WIDGET-coordinate bindingUID). ok=false
// when there is no identity on ctx OR EvaluateRBAC fail-closes to "" (the shared
// empty-identity cell — #95: never serve/populate/consult it).
func seedFullListRAKey(ctx context.Context, gvr schema.GroupVersionResource,
	namespace, name string, extras map[string]any) (cache.ResolvedKeyInputs, string, bool) {
	ui, err := xcontext.UserInfo(ctx)
	if err != nil {
		return cache.ResolvedKeyInputs{}, "", false
	}
	_, bindingUID, _ := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
		Username:  ui.Username,
		Groups:    ui.Groups,
		Verb:      "get",
		Group:     gvr.Group,
		Resource:  gvr.Resource,
		Namespace: namespace,
		Name:      name,
	})
	if bindingUID == "" {
		return cache.ResolvedKeyInputs{}, "", false
	}
	// The requester's class (rbac.IdentityClassOf, the same derivation as
	// dispatchCacheLookupKey): #423's full binding set keeps a co-bound requester
	// with different step-level RBAC out of this cell; #435's effective sub-gen
	// rotates the key on a Role-rules edit (which keeps every binding) and lets
	// raKeyClassCurrent see a grant-then-revoke inside the resolve.
	keyInputs := cache.RAFullListKeyInputs(gvr.Group, gvr.Version, gvr.Resource,
		namespace, name, bindingUID, rbac.IdentityClassOf(ui), extras)
	// #431 — the refresher re-resolves the cell under this representative (same
	// pattern as dispatchCacheLookupKey). It is the identity the key was just
	// minted from, so it is a member of the key's class by construction; the #424
	// guard re-checks that before every refresh. Not key material (ComputeKey
	// skips it). Without it the refresher resolved ("", nil) — an empty body
	// pre-#424, a permanent binding_set decline after it.
	keyInputs.RepresentativeUsername = ui.Username
	keyInputs.RepresentativeGroups = slices.Clone(ui.Groups)
	return keyInputs, cache.ComputeKey(keyInputs), true
}

// RecordSeedFullListShapeNegativeForTest records the given RA's sliceShape as
// structurally non-sliceable (Class C false+permanent) — TEST-ONLY, mirroring
// the established cache.RecordSliceability...ForTest / PublishRBACSnapshotForTest
// convention. It exists so the cross-package FIX-B falsifier (in the
// dispatchers package, which cannot reach the unexported seedFullListShape) can
// prime the shape-negative set through the SAME derivation the serve path uses
// — driving the negative-record and the seed-side pre-check through ONE
// function, so a shape-derivation drift surfaces as the falsifier's skip not
// firing. This is exactly what raFullListServe records on a first-sight
// byte-verify of a structurally-non-sliceable shape; the helper avoids standing
// up the full SA-transport resolve just to observe that record.
func RecordSeedFullListShapeNegativeForTest(gvr schema.GroupVersionResource,
	namespace, name string, ra *templatesv1.RESTAction) {
	shape := seedFullListShape(gvr, namespace, name, ra)
	// raKey is irrelevant to the SHAPE-negative set (FIX-A keys it on the
	// identity-free shape alone); a stable placeholder keeps the per-key entry
	// well-formed. permanent=true = Class C, the promotion condition.
	cache.RecordSliceabilityClassified("seed-test-rakey/"+shape, shape, false, true, cache.SliceabilityLabels{})
}

// RecordSeedFullListPerKeyNonPermanentNegativeForTest records a NON-permanent
// (permanent=false) false per-key verdict for the RA under ctx's identity — the
// FIX-G trigger. G2-B: the raKey is derived via the SHARED seedFullListRAKey
// (RA-CR coordinates → real EvaluateRBAC first-match), the SAME derivation
// raFullListServe (producer) and SeedFullListPerKeyKnownNonSliceable (consumer)
// use — NO hand-fed bindingUID constant. So the record lands under the raKey
// the RA-CR first-match produces; if the pre-check keyed off a DIFFERENT
// (widget-coordinate) first-match — the eg1 defect — the lookup would MISS this
// record and the skip would not fire (the falsifier's divergent-binding arm
// goes RED on the eg1 variant). permanent=false → FIX-A's shape-negative set
// does NOT get it (FIX-A is permanent-only) → FIX-B alone would NOT skip; only
// FIX-G's per-key consult catches it (the G-vs-B discriminator). TEST-ONLY.
// Returns ok=false if there's no RA-CR first-match under ctx (setup error).
func RecordSeedFullListPerKeyNonPermanentNegativeForTest(ctx context.Context, gvr schema.GroupVersionResource,
	namespace, name string, ra *templatesv1.RESTAction, extras map[string]any) bool {
	_, raKey, ok := seedFullListRAKey(ctx, gvr, namespace, name, extras)
	if !ok {
		return false
	}
	shape := seedFullListShape(gvr, namespace, name, ra)
	cache.RecordSliceabilityClassified(raKey, shape, false /*sliceable*/, false /*permanent — the FIX-G discriminator*/, cache.SliceabilityLabels{})
	return true
}

// SeedFullListRAKeyInputsForTest exposes the PRE-HASH ResolvedKeyInputs that the
// SHARED seedFullListRAKey derives for the given RA-CR coordinates under ctx —
// so the G2-B falsifier can assert PRE-HASH INPUT FIELD-EQUALITY (not just the
// ComputeKey digest) between the record derivation and the pre-check derivation,
// per feedback_key_parity_golden_real_inputs_prehash_diff. Both the record
// helper and the pre-check call the SAME seedFullListRAKey, so this returns the
// exact keyInputs both sides fold into their raKey. ok=false when no RA-CR
// first-match under ctx. TEST-ONLY.
func SeedFullListRAKeyInputsForTest(ctx context.Context, gvr schema.GroupVersionResource,
	namespace, name string, extras map[string]any) (cache.ResolvedKeyInputs, bool) {
	ki, _, ok := seedFullListRAKey(ctx, gvr, namespace, name, extras)
	return ki, ok
}

// raFullListServe implements the Ship 4a page-independent serve at the
// apiRef chokepoint. It is called ONLY when cache is on AND the request is
// paginated (perPage>0 && page>0) — the unpaginated path keeps today's
// behaviour byte-identically (a paginated cell is never built for an
// unpaginated /call).
//
// Returns (result, true, nil) when it served a Go-slice over the cached
// full list; (nil, false, nil) when the caller MUST fall back to today's
// page-keyed resolve (no identity, not sliceable, cache miss path declined,
// or a defensive bail); (nil, false, err) on a hard resolve error.
//
// gvr/ns/name identify the RESTAction CR (res.GVR + opts.ApiRef). ra is the
// typed RESTAction (its Spec.Filter is the slice-jq for the sliceShape).
// resolveRA(perPage, page) is the page-keyed resolve seam — a closure over
// restactions.Resolve so this file does not duplicate the apiref Resolve
// plumbing; it returns the RA Status map for the given pagination.
func raFullListServe(
	ctx context.Context,
	gvr schema.GroupVersionResource,
	namespace, name string,
	ra *templatesv1.RESTAction,
	perPage, page int,
	extras map[string]any,
	resolveRA func(ctx context.Context, perPage, page int) (map[string]any, error),
) (map[string]any, bool, error) {
	c := cache.ResolvedCache()
	if c == nil {
		return nil, false, nil // cache off — fall back
	}

	// #592 — THE 1.12.3 A-1 BYPASS WAS REMOVED HERE. It used to refuse this whole
	// layer (served=false) for a RESTAction declaring a userAccessFilter, before
	// deriving a key, on this reasoning: the cell holds the RA's full resolve
	// output with the UAF refilter INCLUDED, under a key folding "the RA-CR's
	// first-match BindingUID ALONE", so two co-bound users with divergent
	// per-object narrowings keyed onto ONE cell and the second was served the
	// first one's rows.
	//
	// That stopped being the key. seedFullListRAKey below builds the inputs via
	// RAFullListKeyInputs, which calls SetIdentity with the FULL IdentityClass —
	// the #423 SubjectBindingSet digest and the #435 sub-generation, not the
	// BindingUID alone — and CacheEntryClassRAFullList is not in
	// identityFreeClasses, so ComputeKey writes that identity segment. raFullList
	// folds exactly the material restactions and widgets fold, and #561 already
	// shipped and measured this same argument on those two classes (see
	// dispatchers/uaf_shortttl.go for the full statement, and #588 for the
	// falsifier: a userAccessFilter cannot narrow on the REQUESTER — the CRD
	// admits only group/resource/resourcesFrom/nameFrom/namespaceFrom/verb and
	// the two *From fields are JQ paths over each RETURNED object — so equal
	// binding sets give equal verdicts and byte-identical bodies).
	//
	// #561 named raFullList identity-bound and no-longer-declined, but it fixed
	// the shared Put-decline GATE; this was a SECOND, EARLIER mechanism reading
	// the declaration predicate directly, and it outlived the gate by one release.
	//
	// WHAT REPLACES IT is not nothing: the cell cannot leak (two requesters
	// deserving different rows hold different keys) but it CAN go stale, because
	// the key folds the requester's bindings and not the per-object verdicts the
	// refilter re-evaluates. That is what the #118 (d) short cap bounds, so
	// HasUAF is stamped below and both tail Puts derive UAFTTLOverrideForEntry
	// from it — the identical treatment every other identity-bound carrier gets.

	// Ship 0.30.242 H.c-layered Phase 2b (§3.3 raFullList row) + #42 G2-A: the
	// RAFullList cell keys on the RA-CR's GET-permit first-match BindingUID,
	// derived via seedFullListRAKey — the SINGLE SOURCE OF TRUTH shared with the
	// FIX-G pre-check (SeedFullListPerKeyKnownNonSliceable), so the verdict this
	// path RECORDS is looked up under the IDENTICAL raKey the seed pre-check
	// computes (the eg1 defect was the pre-check keying off a widget-coordinate
	// bindingUID → miss → G inert).
	//
	// ok=false covers BOTH the no-identity case (never serve a binding-keyed
	// cell without an identity) AND #95 arch C-1: a fail-closed EvaluateRBAC
	// "" collapses to the shared empty-identity cell — serving/populating it
	// cross-identity is the A4 leak (reachable via #95's own dispatcher
	// fall-through), so fall back to the page-keyed resolve under the request's
	// OWN identity (also kills the ""-key PutRAFullList + sliceability records).
	keyInputs, raKey, ok := seedFullListRAKey(ctx, gvr, namespace, name, extras)
	if !ok {
		return nil, false, nil
	}

	// #592 / #118 (d) — the DECLARED limb of the UAF stamp. Set here, on the
	// inputs BOTH tail Puts write, so the cell this path populates carries the
	// short cap. HasUAF is deliberately NOT folded into ComputeKey (resolved.go),
	// so stamping it after seedFullListRAKey leaves raKey byte-identical to what
	// the FIX-G seed pre-check and raKeyClassCurrent derive from the same
	// function — the key-parity those three share is load-bearing and this must
	// not perturb it.
	//
	// The OBSERVED limb is stamped per-Put below, from the UAFTouchedSink delta
	// across the unpaginated resolve: an RA that declares no userAccessFilter of
	// its own but NESTS one produces a narrowed body this declaration cannot see
	// (R-1: two signals, because one was not enough). A DELTA rather than a
	// Count() > 0 test because this ctx's sink may be INHERITED from the widget
	// resolve above and already carry that resolve's own refilters — reading the
	// absolute count would stamp cells this path's resolve never narrowed.
	keyInputs.HasUAF = ra.HasUserAccessFilterStage()
	uafSink := cache.UAFTouchedSinkFromContext(ctx)
	uafBefore := int64(0)
	if uafSink != nil {
		uafBefore = uafSink.Count()
	}
	// stampObservedUAF folds the observed limb into keyInputs immediately before a
	// Put. Idempotent and monotonic (it only ever sets the flag), so the two Put
	// sites cannot disagree about the cell they write.
	stampObservedUAF := func() {
		if uafSink != nil && uafSink.Count() > uafBefore {
			keyInputs.HasUAF = true
		}
	}

	// #323 — capture the raKey cell's generation ONCE here, BEFORE either
	// unpaginated re-resolve below (resolveRA at the repopulate + first-sight
	// branches), so both tail PutRAFullListIfGen writes refuse (rather than
	// resurrecting a pre-delete full body) if the RA-CR / its backing objects are
	// DELETE-evicted during the resolve. PutIfGen (insert-or-replace): this is the
	// SERVE path and first-populates the raKey cell on a cold /call.
	raGen0 := c.CaptureGen(raKey)

	// widgetL1Key is the ORIGINAL widget cell's key (the request ctx's L1
	// key), NEVER fullCtx/raKey. #277 / edge-3: the serve seams replay the
	// raKey cell's backing edges onto THIS key so a backing-object mutation
	// invalidates the widget cell that was served a Go-slice — otherwise the
	// widget cell (served directly by the dispatcher on a warm hit) carries no
	// backing edge and goes stale. Empty on paths with no widget L1 key on ctx
	// (e.g. the direct-serve unit tests); ReplayEdges is nil-safe there.
	widgetL1Key := cache.L1KeyFromContext(ctx)

	// fullCtx scopes the UNPAGINATED resolves' inner-call dep edges to the
	// RAFullList key, so an informer event on any object the RA reads
	// dirty-marks THIS cell and the refresher re-resolves + re-pins it
	// (stale-while-revalidate). Idempotent dep recording lets this coexist
	// with the widget cell's own deps recorded under the request ctx.
	//
	// #375 C2 — the RESTAction itself was fetched (apiref resolve.go) under the
	// WIDGET ctx, BEFORE fullCtx exists, and its self-dep is Recorded on raKey only
	// AFTER the PutRAFullListIfGen below. So fullCtx's dep-gen sink starts at the
	// enclosing widget sink's startSeq (which precedes that read) and pre-declares
	// the RA's own coordinate: an edit of the RA during a cold raKey resolve then
	// remarks raKey instead of leaving it stale (the widget's re-resolve would
	// fast-path and slice the stale cell). With no enclosing sink (a nil-sink drift,
	// already detected at the Put) fall back to an epoch taken now.
	// #684 — the full list's OWN Put-gate sinks (stage error, per-identity
	// forbidden, external touch), installed on ctx so BOTH resolves below (the
	// unpaginated full under fullCtx and the first-sight page under ctx) bump
	// them, and every bump still reaches the enclosing widget's sinks. The cell
	// is gated by the SAME #681 predicates every other Put site uses
	// (cache.DenialOnly, cache.GenuineExternal) over what THIS path resolved —
	// never over counts the enclosing /call recorded before it got here.
	ctx, gate := cache.WithChildPutGateSinks(ctx)

	raEpoch, haveEpoch := cache.DepGenStartSeqFromContext(ctx)
	if !haveEpoch {
		raEpoch = cache.DepGenEpochNow()
	}
	fullCtx := cache.WithL1KeyContextFromEpoch(ctx, raKey, raEpoch,
		cache.DepKey{GVR: gvr, Namespace: namespace, Name: name})
	// #890/#893 — an unpaginated resolve under fullCtx records edges under raKey;
	// if its full list is not stored they are released when this serve returns.
	// The fast-path hit records nothing under raKey, so it releases nothing.
	resolvedFull := false
	defer func() {
		if resolvedFull {
			cache.Deps().ReleaseUnstored(fullCtx, raKey)
		}
	}()

	// Single source of truth for the sliceShape — the SAME derivation the #42
	// FIX-B pre-check (SeedFullListShapeKnownNonSliceable) uses, so the seed
	// skip can never diverge from what this serve path records.
	shape := seedFullListShape(gvr, namespace, name, ra)

	offset := (page - 1) * perPage

	// --- Fast path: known-sliceable verdict + cell hit -> Go-slice -------
	sliceable, known := cache.SliceabilityLookupCtx(ctx, raKey, shape)
	if known && !sliceable {
		// This (RA × shape) was proven NOT cleanly sliceable. Always fall
		// back — never a wrong result.
		return nil, false, nil
	}
	if known && sliceable {
		// #443 (e) — an inert (dry-run) resolve reads the cell without
		// stamping it warm and without joining its representative pool.
		//
		// #552 — so does every OTHER non-customer driver, and until now only the
		// dry-run was recognised here. This cell is read on the BACKGROUND
		// paginated path (the refresher's re-resolve is NOT excluded from the 4a
		// serve — boot_walk_skip_rafulllist_test.go pins that the exclusion keys
		// on ScopeBootPrewarmWalk ONLY), so snowplow's own read was taking
		// c.Get: lastRead stamped, LRU front moved, hit_total bumped,
		// noteServeWhileDirty tripped → stale_served_total. Identical defect to
		// the apistage content read, one function away.
		//
		// TWO LINES, NOT ONE, and deliberately so: NoteHitter is NOT reached
		// inside Get — it is a separate call here, which is why fixing the read
		// primitive alone would NOT have stopped a background driver from
		// joining the cell's representative pool with the representative
		// identity it is resolving under (#444). Both lines ask the same
		// question, so they now consult the same predicate instead of two.
		internalRead := cache.BackgroundResolveFromContext(ctx) || cache.Inert(ctx)
		// #643 — through cache.ServeCell: the served raKey cell, and the backing
		// edges it stands for, are noted on the resolve's sink.
		readRA := func(k string) (*cache.ResolvedEntry, bool) {
			return cache.ServeCell(ctx, c, k, !internalRead, cache.Deps().EdgesUnder(k)...)
		}
		// #547 — a KNOWN-STALE full list is a resolve INPUT: read it through
		// (a MISS here; the fall-through below resolves the RA live), never
		// Go-slice it into a fresh-looking widget cell. Checked BEFORE the read
		// so the cell is not counted as served.
		if cache.KnownStaleReadThrough(raKey) {
			readRA = func(string) (*cache.ResolvedEntry, bool) { return nil, false }
		}
		if entry, ok := readRA(raKey); ok {
			// #444 — the requester joins the cell's representative pool. A
			// background driver is not a requester.
			if !internalRead {
				entry.NoteHitter(keyInputs.RepresentativeUsername, keyInputs.RepresentativeGroups)
			}
			full, derr := decodeRAFullList(entry.RawJSON)
			if derr == nil {
				if sliced, sok := cache.GoSliceFullList(full, offset, perPage); sok {
					// #277 / edge-3 [C2 + DECISION 2, C2-TOCTOU]: replay the
					// raKey cell's backing edges onto the widget key so a
					// backing mutation invalidates this widget's cell too. An
					// EMPTY edge set means the raKey cell was evicted between
					// c.Get above and here (TOCTOU) — DECLINE the fast path
					// (served=false, no error), exactly like the decode-mismatch
					// decline below, so apiref.Resolve falls through to the
					// full page-keyed resolve which records edge-3 under the
					// widget key. NEVER serve a cell with no backing edge.
					edges := cache.Deps().EdgesUnder(raKey)
					if len(edges) == 0 {
						return nil, false, nil
					}
					// #765 B: carry the raKey edges' LIVE interest states. Sound
					// because the raKey resolve's commit runs inside it (deferred in
					// restactions.Resolve), before any raKey Put: the live state
					// covers every version already stored (architect-623, #765).
					cache.Deps().ReplayEdgesWithStates(ctx, widgetL1Key, edges,
						cache.Deps().EdgeNarrowStates(raKey, edges), cache.ReplaySiteFullList)
					// #406 — this body is DERIVED from the raKey cell, which may be
					// dirty-marked and not yet refreshed (an RA edit marks both; the
					// refresher may dequeue the widget first). Note which raKey version
					// was sliced; it travels with this resolve to its Put, which remarks
					// the widget if raKey is by then absent or at another version, and a
					// later raKey commit at a new version remarks the stored widget.
					c.NoteRAFullListSlice(ctx, raKey, entry)
					cache.RecordRAFullListServe(cache.RAFullListServeHit)
					return sliced, true, nil
				}
			}
			// Decode / shape mismatch on a cached cell (corrupt / shape
			// drift). Fall back this /call; do NOT poison the verdict.
			return nil, false, nil
		}
		// Cell miss under a known-sliceable verdict: resolve unpaginated,
		// re-Put the cell, then Go-slice. No re-verify needed (the verdict
		// is already established for this shape).
		//
		// #406 — this widget's body now comes from the fresh resolve, not from a
		// cached raKey version: it is no longer a consumer of an older one, so the
		// raKey Put below must not remark it.
		if !cache.Inert(ctx) { // #443 — an inert (dry-run) resolve leaves no trace
			c.ForgetRAFullListConsumer(raKey, widgetL1Key)
		}
		resolvedFull = true
		full, rerr := resolveRA(fullCtx, 0, 0)
		if rerr != nil {
			return nil, false, rerr
		}
		sliced, sok := cache.GoSliceFullList(full, offset, perPage)
		if !sok {
			// The full result no longer slices cleanly (shape drift). Fall
			// back this /call without poisoning the established verdict.
			return nil, false, nil
		}
		// External-no-cache (proposal 2026-06-22) — if the unpaginated
		// re-resolve touched a genuine external endpoint, SERVE the slice but
		// DECLINE the re-Put + the dep Record (external data has no informer
		// edge to invalidate the cell). Load-bearing surface #4 — without this
		// the external aggregate would be cached + served stale across pages.
		// #684 — GenuineExternal (#681 C2): the requester's own-credential
		// apiserver 403 is not an external touch.
		if cache.GenuineExternal(gate.External, gate.Forbidden) > 0 {
			if !cache.Inert(ctx) { // #443 — an inert (dry-run) resolve leaves no trace
				cache.BumpExternalSkippedPut()
			}
			cache.RecordRAFullListServe(cache.RAFullListServeFallback)
			return sliced, true, nil
		}
		// #323 — generation-guarded write: on accept, record the self-dep + replay
		// the backing edges; on REFUSE (a DELETE-eviction bumped the generation
		// during the unpaginated resolve) skip the cache-side wiring (no cell was
		// stored — do not resurrect) and serve the fresh slice `sliced` directly.
		// #375 — Put under fullCtx: its dep-gen sink (WithL1KeyContext(raKey)) holds the
		// UNPAGINATED resolve's deps, which is what this cell's body was built from. The
		// request ctx's sink is the WIDGET resolve's (it receives these deps by chaining).
		// #424 — the requester's RBAC class must still be the one raKey was
		// minted for; a change mid-resolve makes `full` another class's body.
		// Serve the slice (correct for this requester), never write it.
		// #684 — a stage error that is not a per-identity forbidden declines.
		if cache.DeclineSensitivePut(fullCtx) || raFullListStageDecline(ctx, gate) ||
			!raKeyClassCurrent(ctx, gvr, namespace, name, extras, raKey) {
			cache.RecordRAFullListServe(cache.RAFullListServeFallback)
			return sliced, true, nil
		}
		// #592 — observed limb, immediately before the write (the unpaginated
		// resolve that built `full` has run by now, so a nested refilter has
		// bumped the sink).
		stampObservedUAF()
		if c.PutRAFullListIfGen(fullCtx, raKey, keyInputs, full, raGen0) {
			noteRAFullListDenialOnlyPut(gate)
			// #375 C3 — under fullCtx (the Put's sink; it chains to the widget sink, which
			// still receives the dep), so an RA edit in [Put-check, Record) is re-checked.
			cache.Deps().Record(fullCtx, raKey, gvr, namespace, name)
			// #277 / edge-3: the unpaginated resolve above recorded the backing
			// edges under raKey (fullCtx); replay them onto the widget key so this
			// widget's cell is invalidated by a backing mutation too. Same
			// defensive TOCTOU decline as the hit branch — an empty edge set means
			// the cell we just Put carries no backing edge, so fall through.
			edges := cache.Deps().EdgesUnder(raKey)
			if len(edges) == 0 {
				return nil, false, nil
			}
			// #765 B: the raKey edges' live states (see the hit branch).
			cache.Deps().ReplayEdgesWithStates(ctx, widgetL1Key, edges,
				cache.Deps().EdgeNarrowStates(raKey, edges), cache.ReplaySiteFullList)
		}
		cache.RecordRAFullListServe(cache.RAFullListServeRepopulateSlice)
		return sliced, true, nil
	}

	// --- First sight of (RA × shape): byte-VERIFY, then memoise ---------
	// 1. Resolve UNPAGINATED -> full F (deps scoped to the RAFullList key).
	// #406 — as on the repopulate branch: a fresh-resolve body is no consumer.
	if !cache.Inert(ctx) { // #443 — an inert (dry-run) resolve leaves no trace
		c.ForgetRAFullListConsumer(raKey, widgetL1Key)
	}
	resolvedFull = true
	full, err := resolveRA(fullCtx, 0, 0)
	if err != nil {
		return nil, false, err
	}
	// 2. Resolve the OLD page-keyed way -> S_ra (the fall-back reference).
	//    Under the ORIGINAL ctx so its deps belong to the widget cell,
	//    unchanged from today.
	//
	// #277 / edge-3 [C3 + C-branch-precision] PIN: this first-sight page-keyed
	// resolve runs under the ORIGINAL ctx (widget L1 key), so its inner LIST/
	// GET dispatch records the backing-GVR edge ("edge-3") DIRECTLY under the
	// widget key — the fast-path hit + repopulate branches replay it instead
	// (they never run this resolve). NO ReplayEdges here: adding one would
	// double-record (harmless but misleading). F-PIN asserts this resolve
	// records edge-3 under ctx. Do NOT swap ctx for fullCtx here.
	sRA, err := resolveRA(ctx, perPage, page)
	if err != nil {
		return nil, false, err
	}

	// External-no-cache (proposal 2026-06-22) — load-bearing surface #4. If
	// either first-sight resolve touched a genuine external endpoint, SERVE
	// the (correct, fresh) page-keyed S_ra but record NO sliceability verdict,
	// NO dep edge, and Put NO cell — external data has no informer edge to
	// invalidate it, so caching it would serve it stale across pages/widgets
	// (the exact defect this proposal kills). Checked AFTER both resolves so
	// the sink reflects either one touching the external fetch (#684: both
	// resolve under the gate sinks; GenuineExternal per #681 C2).
	if cache.GenuineExternal(gate.External, gate.Forbidden) > 0 {
		if !cache.Inert(ctx) { // #443 — an inert (dry-run) resolve leaves no trace
			cache.BumpExternalSkippedPut()
		}
		cache.RecordRAFullListServe(cache.RAFullListServeFallback)
		return sRA, true, nil
	}

	// EMPTY-FULL GUARD (0.30.208) — self-healing refusal to freeze on empty.
	//
	// When the freshly-resolved full list's single array-valued key has
	// length 0, an empty Go-slice byte-equals an empty page-keyed S_ra, so
	// the byte-verify below would (wrongly) record verdict=sliceable and Put
	// the EMPTY cell — freezing the fast path on an empty result FOREVER (it
	// never re-verifies). But an empty full is INDISTINGUISHABLE from a
	// not-yet-synced / continueOnError-degraded resolve (panels informer not
	// synced at boot → panel LISTs degrade to []), so it is NOT an
	// authoritative sliceable verdict. Refuse to record OR Put: leave the
	// verdict UNKNOWN so the NEXT request re-runs first-sight once the
	// informer is synced (self-healing), and serve the (correct, empty)
	// page-keyed S_ra meanwhile.
	//
	// PERF: this guard adds ZERO resolves — `full` is ALREADY resolved on
	// this first-sight path for the byte-verify, so the emptiness check is
	// free. We never RecordSliceability on empty, so the cheap fast path
	// (known&&sliceable → Get → GoSlice) is never entered for an empty cell;
	// every request stays on first-sight only while the full resolves EMPTY
	// (cheap — a not-synced fan-out yields []), NOT a 48s/163MB full. The
	// one-time expensive full resolve happens exactly once, AFTER sync, when
	// the full is non-empty and the verdict is correctly recorded → cheap
	// hits thereafter. Mechanism-uniform: keyed off "the full is empty"
	// (FullListIsEmpty), NO resource/name/GVR literal.
	if cache.FullListIsEmpty(full) {
		cache.RecordRAFullListServe(cache.RAFullListServeFallback)
		return sRA, true, nil
	}
	// 3. Go-slice the full F -> S_go.
	sGo, sok := cache.GoSliceFullList(full, offset, perPage)

	// 4. Byte-compare: full F must equal a fresh unpaginated resolve (it IS
	//    `full`, so identity holds by construction — re-encode both to
	//    canonical JSON to be safe) AND S_go must byte-equal S_ra.
	verdict := sok && canonicalJSONEqual(sGo, sRA)
	// #874 — a source that DECLARES its own pagination (slice.continue /
	// slice.cursor in its output) is page-dependent by construction: a page
	// Go-sliced from the unpaginated full list would carry the full list's
	// declaration, not this page's. Never sliceable, permanently — even on the
	// one page where the two happen to byte-match (e.g. a last page whose
	// continue is false in both).
	_, _, declaresPagination := cache.SourcePagination(sRA)
	if declaresPagination {
		verdict = false
	}

	// Ship #91 / 0.30.211 — Class C heuristic. When the byte-verify FAILED
	// AND `full` is shape-identical to S_ra modulo length-vs-perPage, this
	// is an aggregation RA (or otherwise structurally non-sliceable RA)
	// whose verdict will never flip. Mark it permanent so the retry cap
	// drops from 3 to 1. Mechanism-uniform: derived from (full, sRA, perPage),
	// no resource/name/GVR literal (feedback_no_special_cases).
	permanent := declaresPagination
	if !verdict && !permanent {
		permanent = cache.IsStructurallyNonSliceable(full, sRA, perPage)
	}

	// Record the verdict + the caller-CR identity labels so the
	// snowplow_ra_full_list_memo expvar can describe the entry by its caller
	// (e.g. compositions-page-datagrid) instead of by the raKey/sliceShape
	// sha256 hashes alone. The labels are READ-SIDE ONLY (they do not change
	// the memo key) — see RecordSliceabilityWithLabels.
	cache.RecordSliceabilityClassifiedCtx(ctx, raKey, shape, verdict, permanent, cache.SliceabilityLabels{
		CallerClass:     raFullListCallerClass,
		CallerGroup:     gvr.Group,
		CallerVersion:   gvr.Version,
		CallerResource:  gvr.Resource,
		CallerNamespace: namespace,
		CallerName:      name,
	})

	// Ship #91 / 0.30.211 — Lever C symmetric dep-record at BOTH verdict
	// branches. Pre-Ship-#91 the RA-CR self-dep was recorded ONLY on the
	// verdict=true branch (line 217), so a verdict=false entry got no dep
	// edge — meaning a panels-informer event on the underlying objects
	// could not invalidate a stuck-false memo entry via the refresher hook.
	// Symmetric record() at this site lets the refresher's RAFullList
	// class-prefix hook (Lever C) call InvalidateSliceabilityForKey(raKey)
	// when the dep-tuple fires, clearing the memo and letting the next
	// /call re-enter first-sight. THIS IS THE WIRING THE ARCHITECT GUARDS.
	cache.Deps().Record(ctx, raKey, gvr, namespace, name)

	if !verdict {
		// NOT cleanly sliceable for this shape — serve the page-keyed S_ra
		// (already resolved) and never try the Go-slice for this shape until
		// Lever A's TTL expires + retryCount permits OR Lever C invalidates.
		cache.RecordRAFullListServe(cache.RAFullListServeFallback)
		return sRA, true, nil
	}

	// Sliceable — Put the full cell (possibly pinned by cost predicate),
	// then serve the verified Go-slice. The RA-CR self-dep was already
	// recorded above (symmetric with the false branch — Ship #91).
	// #323 — generation-guarded write: a REFUSE (DELETE-eviction during the
	// resolve bumped the generation) declines the cache fill without resurrecting
	// the pre-delete body; the verified slice sGo is served regardless, and the
	// self-dep Record above (the Lever-C memo wiring) stands either way.
	// #424 — same identity-class re-check as the repopulate branch.
	// #398 — a full list built from a sensitive read is never cached.
	// #684 — nor one with a stage error that is not a per-identity forbidden.
	if cache.DeclineSensitivePut(fullCtx) || raFullListStageDecline(ctx, gate) ||
		!raKeyClassCurrent(ctx, gvr, namespace, name, extras, raKey) {
		cache.RecordRAFullListServe(cache.RAFullListServeFallback)
		return sGo, true, nil
	}
	// #592 — observed limb, twin of the repopulate branch above.
	stampObservedUAF()
	if c.PutRAFullListIfGen(fullCtx, raKey, keyInputs, full, raGen0) {
		noteRAFullListDenialOnlyPut(gate)
	}
	cache.RecordRAFullListServe(cache.RAFullListServeVerifiedSlice)
	return sGo, true, nil
}

// raFullListStageErrorDeclined counts raFullList cell writes declined by the
// #681 stage-error gate (#684): a stage error that is not a per-identity
// forbidden (cache.DenialOnly false).
var raFullListStageErrorDeclined atomic.Uint64

// RAFullListStageErrorDeclinedForTest reads the counter.
func RAFullListStageErrorDeclinedForTest() uint64 { return raFullListStageErrorDeclined.Load() }

// raFullListStageDecline is the #681 stage-error gate on the full-list cell,
// over the full list's own sinks (#684): any stage error declines unless
// EVERY one was a per-identity forbidden (cache.DenialOnly — the one predicate
// every Put site shares). The denial-only body is already identity-free: the
// resolver records F1 and F2 as the canonical forbidden body.
func raFullListStageDecline(ctx context.Context, g cache.PutGateSinks) bool {
	if g.Stage.Count() == 0 || cache.DenialOnly(g.Stage, g.Forbidden, g.External, cache.CacheEntryClassRAFullList) {
		return false
	}
	if !cache.Inert(ctx) { // #443 — dry-run traffic is not a decline
		raFullListStageErrorDeclined.Add(1)
	}
	return true
}

// noteRAFullListDenialOnlyPut counts a STORED full-list cell that carried
// stage errors (the gate admitted it, so they were all per-identity forbidden).
func noteRAFullListDenialOnlyPut(g cache.PutGateSinks) {
	if g.Stage.Count() > 0 {
		cache.RecordDenialOnlyPut(cache.DenialOnlyPutSiteRAFullList)
	}
}

// raKeyClassDriftDeclined counts raFullList cell writes declined because the
// requester's RBAC class moved between mint and Put (#424).
var raKeyClassDriftDeclined atomic.Uint64

// RAKeyClassDriftDeclinedForTest reads the counter.
func RAKeyClassDriftDeclinedForTest() uint64 { return raKeyClassDriftDeclined.Load() }

// raKeyClassCurrent re-derives the raFullList key for ctx's identity through the
// SAME single-source helper the producer minted with (seedFullListRAKey: real
// EvaluateRBAC first-match + the binding-set digest + the RBAC sub-gen) and
// reports whether it still equals raKey. A grant/revoke that landed during the
// resolve moves it (#424) — including a grant-then-revoke or a Role edit that
// leaves the binding set unchanged, via the monotone sub-gen (#435).
func raKeyClassCurrent(ctx context.Context, gvr schema.GroupVersionResource,
	namespace, name string, extras map[string]any, raKey string) bool {
	_, now, ok := seedFullListRAKey(ctx, gvr, namespace, name, extras)
	if ok && now == raKey {
		return true
	}
	raKeyClassDriftDeclined.Add(1)
	return false
}

// decodeRAFullList unmarshals a cached RAFullList envelope back to a map.
func decodeRAFullList(raw []byte) (map[string]any, error) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// canonicalJSONEqual reports whether two maps marshal to byte-identical
// canonical JSON. encoding/json sorts map keys, so this is order-stable.
func canonicalJSONEqual(a, b map[string]any) bool {
	ab, err1 := json.Marshal(a)
	bb, err2 := json.Marshal(b)
	if err1 != nil || err2 != nil {
		return false
	}
	return string(ab) == string(bb)
}
