// a1_uaf_ra_full_list_bypass_test.go — the raFullList userAccessFilter arms.
//
// #592 RE-POINTED THESE, IT DID NOT INVERT THEM. They were written for the
// 1.12.3 A-1 BYPASS: a UAF-bearing RESTAction returned served=false before the
// key was derived, so the arms asserted "nothing is served, nothing is stored,
// no verdict is recorded, the bypass counter ticks".
//
// The bypass is gone (ra_full_list.go), so the literal assertions are obsolete —
// but THE PROPERTY THEY EXISTED TO PROTECT IS NOT, and it is the same property
// #561's A-1 arms kept when the Put-decline was scoped out from under them:
//
//	"this arm must still pass — then proving the KEY separates them rather
//	 than that nothing is cached."
//
// So the load-bearing claim is unchanged — a requester must never be served
// another requester's userAccessFilter-narrowed rows — and only the MECHANISM
// under test moved, from "the layer refuses" to "the two requesters occupy
// different cells". Inverting these to "now it serves" would have deleted the
// safety coverage and kept only the performance claim; that is the mistake R-1
// recorded, and it is why the cross-user pair below is the centre of the file.
//
// WHAT MAKES THE KEY SUFFICIENT (dispatchers/uaf_shortttl.go has the full
// statement): RAFullListKeyInputs calls SetIdentity with the whole IdentityClass
// — the #423 SubjectBindingSet digest and the #435 sub-generation — and
// CacheEntryClassRAFullList is not identity-free, so ComputeKey writes that
// identity segment. Two requesters owed different narrowings hold different
// binding sets (#588: a userAccessFilter cannot narrow on the REQUESTER, only
// on each RETURNED object), so they hold different keys.
//
// SCOPE NOTE, so a later reader does not over-read these arms: resolveRA is
// STUBBED here, so no refilter actually runs. What is under test is the cache
// layer's keying and stamping for a UAF-DECLARING RA — not the refilter's own
// correctness, which is a4/#588's ground.
//
// RED on the pre-#592 tree: the cross-user arms fail at the FIRST serve, because
// the bypass returns served=false and no cell is ever populated to be isolated.

package apiref

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/ptr"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// a1UAFRA is `ra(jq)` plus an api-step declaring a userAccessFilter — the ONLY
// difference between the two arms. Same Filter, so both arms derive the SAME
// sliceShape and the SAME raKey: a difference cannot be an artefact of the
// fixture keying somewhere else.
func a1UAFRA(jq string) *templatesv1.RESTAction {
	return &templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{
		Filter: ptr.To(jq),
		API: []*templatesv1.API{
			{Name: "list-panels"},
			{Name: "refilter", UserAccessFilter: &templatesv1.UserAccessFilterSpec{
				Verb: "get", Group: "", Resource: "configmaps", NamespaceFrom: ".metadata.namespace",
			}},
		},
	}}
}

// TestA1_RAFullList_UAFCrossUser_NoSharedCellServe is the re-pointed arm (e).
//
// Two users whose first-match bindings differ (the f6 fixture's crbA/crbB) both
// resolve the SAME UAF-bearing RESTAction at the SAME coordinates. Each must be
// served its OWN rows and neither may see the other's — now because the cells
// are key-distinct, not because the layer refused to run.
//
// The arm also pins the two things that make caching the narrowed body SAFE
// rather than merely faster: the stored cell carries HasUAF (so
// UAFTTLOverrideForEntry caps its RBAC-staleness window), and the acceptance
// counter moves.
func TestA1_RAFullList_UAFCrossUser_NoSharedCellServe(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	// A non-zero cap, so "the cap is applied" is DISTINGUISHABLE from "the knob
	// is off". With UAF_RESOLVED_TTL_SECONDS unset, UAFTTLOverrideForEntry
	// returns 0 for a UAF cell and a non-UAF cell alike and the assertion below
	// would pass vacuously on a tree that never stamps HasUAF at all.
	t.Setenv("UAF_RESOLVED_TTL_SECONDS", "300")
	cache.ResetResolvedCacheForTest()
	newF6Watcher(t, f6BuildFixture()...)
	cache.RegisterUAFPutDeclineMetricsForTest()
	cache.ResetUAFPutDeclineCountersForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

	// DISTINCT coordinates from the other arms in this package: own name, own
	// raKey, own shape, no cross-test coupling. (#471 made the sliceability memo
	// reset with the store, so this is no longer load-bearing against sibling
	// leakage — it stays because it is still the cheaper invariant, and because
	// under -count>1 a test's second iteration IS its own sibling.)
	const ns, name = "krateo-system", "a1-uaf-panels"

	ctxA := f6CtxWithUser(t, "admin", []string{"system:masters"})
	ctxB := f6CtxWithUser(t, "dev-1", []string{"devs"})

	// PRECONDITION: the two users must derive DIFFERENT raKeys for this RA.
	// Without it every assertion below could pass for the wrong reason (one
	// shared cell that happens to hold the right rows because only one user
	// ever populated it).
	_, raKeyA, okA := seedFullListRAKey(ctxA, gvr(), ns, name, nil)
	_, raKeyB, okB := seedFullListRAKey(ctxB, gvr(), ns, name, nil)
	if !okA || !okB || raKeyA == "" || raKeyB == "" {
		t.Fatalf("PRECONDITION: both users need a live raKey (non-empty first-match BindingUID); "+
			"okA=%v keyA=%q okB=%v keyB=%q", okA, raKeyA, okB, raKeyB)
	}
	if raKeyA == raKeyB {
		t.Fatalf("PRECONDITION FAILED — THE WHOLE ARGUMENT FOR #592 IS THIS INEQUALITY: two users with "+
			"different first-match bindings derived the SAME raFullList key %q. If this ever fires, caching a "+
			"userAccessFilter-narrowed body on this class is a cross-tenant leak and the bypass must come back.", raKeyA)
	}

	var callsA, callsB atomic.Int64
	resolveA := f6ContentWithMarker(t, "MARKER-A", &callsA)
	resolveB := f6ContentWithMarker(t, "MARKER-B", &callsB)

	// --- user A: a UAF-bearing RA is now SERVED and POPULATED. ---
	gotA, servedA, err := raFullListServe(ctxA, gvr(), ns, name, a1UAFRA(raSliceJQ), 5, 1, nil, resolveA)
	if err != nil {
		t.Fatalf("user A: serve must be clean; got %v", err)
	}
	if !servedA {
		t.Fatalf("#592 RED: raFullListServe REFUSED a userAccessFilter-bearing RESTAction. The 1.12.3 bypass is " +
			"back (or was never removed): the 4a accelerator is off for every UAF carrier, so each page is a fresh " +
			"unpaginated resolve and the cohort's first paint is cold.")
	}
	if !f6ContainsMarker(t, gotA, "MARKER-A") {
		t.Fatalf("user A was not served its own rows; got %+v", gotA)
	}
	entryA, hitA := cache.ResolvedCache().Get(raKeyA)
	if !hitA {
		t.Fatalf("#592 RED: no raFullList cell under user A's key %q — served without populating, so the next "+
			"caller pays the full resolve again and the accelerator never warms", raKeyA)
	}

	// THE SAFETY STAMP. A cell holding narrowed rows cannot leak (the key
	// separates requesters) but it CAN go stale: the key folds the requester's
	// BINDINGS, not the per-object verdicts the refilter re-evaluates, so an
	// out-of-band RBAC change makes this requester's own view wrong with nothing
	// to evict it. HasUAF is what makes the #118 (d) cap apply to that window.
	if entryA.Inputs == nil || !entryA.Inputs.HasUAF {
		t.Fatalf("#592 RED: the stored raFullList cell does NOT carry HasUAF, so UAFTTLOverrideForEntry returns 0 " +
			"and the narrowed body inherits the FULL store TTL. Caching it is only defensible with the cap on.")
	}
	if want := cache.UAFResolvedTTL(); entryA.TTLOverride != want {
		t.Fatalf("#592 RED: raFullList cell TTLOverride = %v, want the short UAF cap %v — the stamp reached Inputs "+
			"but not the entry, so nothing actually bounds the staleness window", entryA.TTLOverride, want)
	}

	// --- user B: served its OWN rows, from its OWN cell. ---
	gotB, servedB, err := raFullListServe(ctxB, gvr(), ns, name, a1UAFRA(raSliceJQ), 5, 1, nil, resolveB)
	if err != nil || !servedB {
		t.Fatalf("user B: serve must succeed; served=%v err=%v", servedB, err)
	}
	if f6ContainsMarker(t, gotB, "MARKER-A") {
		t.Fatalf("A-1 CROSS-TENANT LEAK (the defect the 1.12.3 bypass existed to prevent): user B was served user "+
			"A's userAccessFilter-narrowed rows. got=%+v", gotB)
	}
	if !f6ContainsMarker(t, gotB, "MARKER-B") {
		t.Fatalf("user B was not served its own rows; got %+v", gotB)
	}
	if _, hitB := cache.ResolvedCache().Get(raKeyB); !hitB {
		t.Fatalf("no raFullList cell under user B's key %q", raKeyB)
	}

	// Both cells coexist — neither Put overwrote the other.
	if _, stillA := cache.ResolvedCache().Get(raKeyA); !stillA {
		t.Fatalf("user A's cell vanished when user B populated: one key is serving both requesters")
	}

	// The acceptance counter counts CELLS STORED, so both arms land on it.
	if n := cache.RAFullListUAFCached(); n != 2 {
		t.Fatalf("snowplow_ra_full_list_uaf_cached_total must tick once per stored UAF cell (2 users, 2 cells); got %d", n)
	}
}

// TestA1_RAFullList_UAFWarmCell_CrossUserMisses is the re-pointed ordering arm.
//
// It used to prove the bypass sat BEFORE the sliceability fast path, so a
// UAF-bearing RA could not be served a cell some earlier caller had warmed. That
// ordering hazard is gone with the bypass, but the hazard UNDERNEATH it is the
// one that always mattered and is now load-bearing in a new way: with the cell
// populated again, there IS a warm, known-sliceable cell sitting under user A's
// key, and the fast path is exactly where a key defect would surface as a serve.
//
// So: warm it under A, then come in as B. B must miss and resolve for itself.
// A key that folded less than the full binding set would hand B the warm cell
// through the fast path, and THAT is the leak in its post-#592 shape.
func TestA1_RAFullList_UAFWarmCell_CrossUserMisses(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	newF6Watcher(t, f6BuildFixture()...)
	cache.RegisterUAFPutDeclineMetricsForTest()
	cache.ResetUAFPutDeclineCountersForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

	const ns, name = "krateo-system", "a1-uaf-warm-panels"
	ctxA := f6CtxWithUser(t, "admin", []string{"system:masters"})
	ctxB := f6CtxWithUser(t, "dev-1", []string{"devs"})

	var callsA atomic.Int64
	resolveA := f6ContentWithMarker(t, "MARKER-A", &callsA)

	// Warm A's cell AND its known-sliceable verdict via the real serve path.
	if _, served, err := raFullListServe(ctxA, gvr(), ns, name, a1UAFRA(raSliceJQ),
		5, 1, nil, resolveA); err != nil || !served {
		t.Fatalf("setup: the UAF warm-up must serve post-#592; served=%v err=%v", served, err)
	}
	_, raKeyA, _ := seedFullListRAKey(ctxA, gvr(), ns, name, nil)
	if _, hit := cache.ResolvedCache().Get(raKeyA); !hit {
		t.Fatalf("setup: expected a warm raFullList cell under A's key %q", raKeyA)
	}
	shape := seedFullListShape(gvr(), ns, name, a1UAFRA(raSliceJQ))
	if sliceable, known := cache.SliceabilityLookup(raKeyA, shape); !known || !sliceable {
		t.Fatalf("setup: expected a known-SLICEABLE verdict under A's key — without it this arm never reaches the "+
			"fast path it exists to test; known=%v sliceable=%v", known, sliceable)
	}

	// Now user B at the SAME coordinates and the SAME shape. B's resolve closure
	// MUST run: a hit on A's warm cell would serve without resolving.
	var callsB atomic.Int64
	resolveB := f6ContentWithMarker(t, "MARKER-B", &callsB)
	gotB, servedB, err := raFullListServe(ctxB, gvr(), ns, name, a1UAFRA(raSliceJQ), 5, 1, nil, resolveB)
	if err != nil || !servedB {
		t.Fatalf("user B: serve must succeed; served=%v err=%v", servedB, err)
	}
	if f6ContainsMarker(t, gotB, "MARKER-A") {
		t.Fatalf("A-1 WARM-CELL CROSS-TENANT LEAK: user B was served user A's warm userAccessFilter-narrowed cell "+
			"through the sliceability fast path. The raFullList key is not separating the two binding sets. got=%+v", gotB)
	}
	if callsB.Load() == 0 {
		t.Fatalf("user B's resolve never ran, so B was served from a cell it did not populate — the strongest form " +
			"of the warm-cell leak, and it can be true even when the returned content looks right")
	}

	// A's cell is untouched: B populating its own must not evict or rewrite A's.
	entryA, stillA := cache.ResolvedCache().Get(raKeyA)
	if !stillA {
		t.Fatalf("user A's warm cell was evicted by user B's populate")
	}
	bodyA, err := decodeRAFullList(entryA.RawJSON)
	if err != nil {
		t.Fatalf("user A's cell did not decode: %v", err)
	}
	if !f6ContainsMarker(t, bodyA, "MARKER-A") {
		t.Fatalf("user A's cell no longer holds A's rows — B's populate overwrote it in place")
	}
}

// TestA1_RAFullList_ObservedUAFLimb_StampsFromTheSinkDelta arms the SECOND
// signal, and it exists because mutating found the first version of #592 had
// left it inert.
//
// R-1's lesson is that one signal is not enough: an RA that declares no
// userAccessFilter of its own but NESTS one below produces a narrowed body no
// declaration at this frame can see. The declared limb above reads the RA in
// hand; this limb reads cache.UAFTouchedSink, which the dispatcher installs and
// every nested resolve inherits. With it inert, a nested-UAF raFullList cell is
// stored holding narrowed rows with NO cap — the one shape where caching the
// body is least defensible.
//
// It also pins that the limb reads a DELTA rather than Count() > 0. This ctx's
// sink is INHERITED from the enclosing widget resolve and may already carry that
// resolve's own refilters; an absolute test would stamp cells this path's own
// resolve never narrowed, capping unrelated traffic at the short TTL. So the arm
// pre-bumps the sink before the serve and asserts that alone does NOT stamp.
func TestA1_RAFullList_ObservedUAFLimb_StampsFromTheSinkDelta(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("UAF_RESOLVED_TTL_SECONDS", "300")
	cache.ResetResolvedCacheForTest()
	newF6Watcher(t, f6BuildFixture()...)
	cache.RegisterUAFPutDeclineMetricsForTest()
	cache.ResetUAFPutDeclineCountersForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

	const ns = "krateo-system"
	panels := panelDict(40)

	// --- (a) PRE-EXISTING sink count, no refilter during THIS resolve -------
	// The enclosing widget resolve already ran a refilter. This path's own
	// resolve runs none, so its cell must NOT be stamped.
	t.Run("inherited_count_alone_does_not_stamp", func(t *testing.T) {
		const name = "a1-observed-inherited"
		ctx, sink := cache.WithUAFTouchedSink(ctxWithUser(t))
		sink.Bump() // somebody else's refilter, before we start
		sink.Bump()

		var calls atomic.Int64
		if _, served, err := raFullListServe(ctx, gvr(), ns, name, ra(raSliceJQ),
			10, 1, nil, stubResolveRA(t, panels, &calls)); err != nil || !served {
			t.Fatalf("setup: a non-UAF RA must serve; served=%v err=%v", served, err)
		}
		_, raKey, _ := seedFullListRAKey(ctx, gvr(), ns, name, nil)
		entry, hit := cache.ResolvedCache().Get(raKey)
		if !hit {
			t.Fatalf("setup: expected a populated cell under %q", raKey)
		}
		if entry.Inputs != nil && entry.Inputs.HasUAF {
			t.Fatalf("#592 RED: the observed limb read the sink's ABSOLUTE count, so a refilter that ran in the " +
				"ENCLOSING resolve stamped a cell this path never narrowed. Unrelated raFullList traffic is now " +
				"capped at the short UAF TTL and churns.")
		}
		if entry.TTLOverride != 0 {
			t.Fatalf("#592 RED: an unnarrowed cell took the short cap (%v)", entry.TTLOverride)
		}
	})

	// --- (b) a refilter DURING this resolve stamps, with no declaration -----
	t.Run("refilter_during_resolve_stamps_without_a_declaration", func(t *testing.T) {
		const name = "a1-observed-nested"
		ctx, sink := cache.WithUAFTouchedSink(ctxWithUser(t))
		sink.Bump() // same inherited noise as (a), so the two differ only in the delta

		// A resolve closure that bumps the sink — standing in for a NESTED RA
		// whose own userAccessFilter runs below this frame. The RA passed to
		// raFullListServe declares NOTHING, so only the observed limb can see it.
		var calls atomic.Int64
		inner := stubResolveRA(t, panels, &calls)
		nested := func(c context.Context, perPage, page int) (map[string]any, error) {
			cache.UAFTouchedSinkFromContext(c).Bump()
			return inner(c, perPage, page)
		}

		if _, served, err := raFullListServe(ctx, gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, nested); err != nil || !served {
			t.Fatalf("setup: serve must succeed; served=%v err=%v", served, err)
		}
		_, raKey, _ := seedFullListRAKey(ctx, gvr(), ns, name, nil)
		entry, hit := cache.ResolvedCache().Get(raKey)
		if !hit {
			t.Fatalf("setup: expected a populated cell under %q", raKey)
		}
		if entry.Inputs == nil || !entry.Inputs.HasUAF {
			t.Fatalf("#592 RED: a refilter ran DURING this resolve and the cell was not stamped. The observed limb " +
				"is inert, so a NESTED userAccessFilter — which no declaration at this frame can see — produces a " +
				"stored, narrowed, UNCAPPED raFullList cell. This is the R-1 shape exactly.")
		}
		if want := cache.UAFResolvedTTL(); entry.TTLOverride != want {
			t.Fatalf("#592 RED: observed-limb cell TTLOverride=%v, want %v", entry.TTLOverride, want)
		}
	})
}

// TestA1_RAFullList_NonUAF_Unchanged is the CONTROL, and it is the arm that
// keeps the two above from passing by accident. #592 must change the UAF path
// and NOTHING else: a non-UAF RA must serve, populate, carry no HasUAF stamp,
// take no short cap, and not move the UAF counter. Without this, a change that
// simply stamped every raFullList cell as UAF-bearing would satisfy every
// assertion above while capping the whole class at 300s.
func TestA1_RAFullList_NonUAF_Unchanged(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("UAF_RESOLVED_TTL_SECONDS", "300")
	cache.ResetResolvedCacheForTest()
	newF6Watcher(t, f6BuildFixture()...)
	cache.RegisterUAFPutDeclineMetricsForTest()
	cache.ResetUAFPutDeclineCountersForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

	const ns, name = "krateo-system", "a1-nonuaf-panels"
	ctx := ctxWithUser(t)
	panels := panelDict(40)

	var calls atomic.Int64
	resolve := stubResolveRA(t, panels, &calls)
	got, served, err := raFullListServe(ctx, gvr(), ns, name, ra(raSliceJQ), 10, 1, nil, resolve)
	if err != nil || !served {
		t.Fatalf("control: a NON-UAF RA must still be served by the 4a path; served=%v err=%v", served, err)
	}
	ref, _ := resolve(ctx, 10, 1)
	assertCanonEqual(t, got, ref, "control page1")

	_, raKey, _ := seedFullListRAKey(ctx, gvr(), ns, name, nil)
	entry, hit := cache.ResolvedCache().Get(raKey)
	if !hit {
		t.Fatalf("control: a verified-sliceable NON-UAF RA must POPULATE the raFullList cell under %q", raKey)
	}
	if entry.Inputs != nil && entry.Inputs.HasUAF {
		t.Fatalf("control RED: a NON-UAF RA's cell was stamped HasUAF — the stamp is unconditional, so the whole " +
			"raFullList class now takes the short UAF cap and churns")
	}
	if entry.TTLOverride != 0 {
		t.Fatalf("control RED: a NON-UAF cell carries TTLOverride=%v, want 0 (standard TTL)", entry.TTLOverride)
	}
	if n := cache.RAFullListUAFCached(); n != 0 {
		t.Fatalf("control: a NON-UAF RA must not move snowplow_ra_full_list_uaf_cached_total; got %d", n)
	}
}
