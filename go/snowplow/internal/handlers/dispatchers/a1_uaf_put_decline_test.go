// a1_uaf_put_decline_test.go — 1.12.3 A-1 falsifiers for the three restactions
// L1 Put sites.
//
// THE DEFECT: the restactions key folds BindingUID + RBACSubGen; a
// userAccessFilter (UAF) stage narrows the body PER REQUESTER PER OBJECT, a
// dependency the key never sees. Two users sharing the first-match binding for
// `get restactions` derive the SAME key and the hit path writes entry.RawJSON
// verbatim — one user's rows are served to the other. The 1.12.3 mitigation
// declines the L1 Put for a UAF-bearing RA at all three sites; the request is
// still served its own correctly-narrowed 200.
//
// THE ARMS HERE are the per-site WIRING falsifiers — each drives the REAL
// function through the REAL shared gate (declineUAFPut) with a UAF fixture and
// a non-UAF CONTROL, so a decline that is actually an "everything is broken,
// nothing is cached" regression cannot pass. The cross-user ACCEPTANCE arm (the
// one that proves the leak itself is closed, per
// feedback_seed_group_shared_cell_needs_resolve_output_arm) is
// TestA1_UAFCrossUser_NoSharedCellServe in a1_uaf_cross_user_isolation_test.go.
//
// RED on origin/main: every arm below fails there — main Puts the UAF entry at
// each of the three sites (it only stamps a shorter TTL on it).

package dispatchers

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// a1RAUnstructured builds the RESTAction CR the dispatch path FETCHES and then
// converts with the REAL runtime.DefaultUnstructuredConverter — so the UAF
// stanza travels the production route into the typed CR the gate reads, rather
// than being hand-set on a typed struct. `uaf` selects the ONLY difference
// between the two arms: an api-step carrying userAccessFilter vs not.
func a1RAUnstructured(uaf bool) *unstructured.Unstructured {
	step := map[string]any{"name": "list-namespaces", "path": "/api/v1/namespaces"}
	if uaf {
		step["userAccessFilter"] = map[string]any{
			"verb":          "get",
			"group":         "",
			"resource":      "configmaps",
			"namespaceFrom": ".metadata.name",
		}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": h1RAName, "namespace": h1NS},
		"spec":       map[string]any{"api": []any{step}},
	}}
}

// ---------------------------------------------------------------------------
// Arm (a) — CUSTOMER DISPATCH Put site (restactions.go)
// ---------------------------------------------------------------------------

// TestA1_Dispatch_UAFDeclinesPut_ControlPuts drives the REAL restActionHandler
// (RESTAction().ServeHTTP) over the REAL key derivation and the REAL cache
// handle, twice under the SAME coordinates — so both arms derive the IDENTICAL
// key and the ONLY variable is the userAccessFilter stanza on the fetched CR.
//
//   - UAF arm: 200 with the resolved body, and NOTHING is Put under the derived
//     key (nor a dep edge recorded); the decline counter ticks exactly once.
//   - CONTROL arm: the same request shape without the UAF stanza still Puts and
//     still records its dep — proving the decline is UAF-driven and not a
//     caching regression.
//
// RED on origin/main: the UAF arm finds the entry Put (main caches it with a
// short TTL), and RestactionsUAFPutDeclined() is 0 (the counter does not exist).
func TestA1_Dispatch_UAFDeclinesPut_ControlPuts(t *testing.T) {
	h1BuildWatcher(t)
	cache.RegisterUAFPutDeclineMetricsForTest()
	cache.ResetUAFPutDeclineCountersForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

	reqCtx := h1ReqCtx(h1User)
	key, handle, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: expected a live, cacheable key (non-empty BindingUID) — without it the Put is declined for the #95 reason and this arm would pass vacuously; key=%q handle=%v inputs=%+v",
			key, handle != nil, inputs)
	}
	if _, ok := handle.Get(key); ok {
		t.Fatalf("PRECONDITION: the derived key must be cold before the arms run")
	}

	resolved := &templatesv1.RESTAction{}
	resolved.SetName(h1RAName)
	resolved.SetNamespace(h1NS)

	// serve runs one full dispatch against a CR with/without the UAF stanza and
	// returns the response recorder.
	serve := func(t *testing.T, uaf bool) *httptest.ResponseRecorder {
		t.Helper()
		restore := installRAFakes(t, a1RAUnstructured(uaf), func() bool { return true },
			func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
				return resolved, nil
			})
		defer restore()
		rec := httptest.NewRecorder()
		req := httptest.NewRequest("GET", "/call", nil).WithContext(reqCtx)
		RESTAction().ServeHTTP(rec, req)
		return rec
	}

	// --- UAF arm: served AND cached. #561 inverted this. ---
	//
	// This arm used to assert the cell stayed EMPTY, with the stated reason
	// "that cell is keyed on BindingUID+RBACSubGen only". That premise is no
	// longer true: #423 added SubjectBindingSet to the identity segment, so two
	// co-bound users with different per-object narrowings derive DIFFERENT keys
	// and occupy DIFFERENT cells. restactions is identity-bound, so the Put
	// proceeds.
	//
	// The isolation this arm used to protect is now asserted where it belongs —
	// on the served bytes, by TestA1_UAFCrossUser_NoSharedCellServe, which drives
	// two real identities. Here we assert the mechanism that replaced the
	// decline: the cell IS written, it carries the UAF marker so the short cap
	// applies, and its dep edge exists so invalidation can reach it.
	rec := serve(t, true)
	if rec.Code != 200 {
		t.Fatalf("arm (a): a UAF RESTAction must still be SERVED normally; got %d body=%s", rec.Code, rec.Body.String())
	}
	if len(bytes.TrimSpace(rec.Body.Bytes())) == 0 {
		t.Fatalf("arm (a): the response body must carry the resolved result")
	}
	uafEntry, uafCached := handle.Get(key)
	if !uafCached {
		t.Fatalf("arm (a) #561: a UAF-bearing RESTAction in an IDENTITY-BOUND class must now be CACHED under %q. "+
			"An empty cell means the decline is still firing, which is the 10.9x p95 latency cost #561 measured.", key)
	}
	if !bytes.Equal(uafEntry.RawJSON, rec.Body.Bytes()) {
		t.Fatalf("arm (a): the Put'd bytes must equal the served bytes; put=%q served=%q", uafEntry.RawJSON, rec.Body.Bytes())
	}
	if uafEntry.Inputs == nil || !uafEntry.Inputs.HasUAF {
		t.Fatalf("arm (a): a UAF-bearing cell must carry HasUAF so uafTTLOverrideForEntry stamps the short "+
			"UAF_RESOLVED_TTL_SECONDS cap instead of the full TTL; Inputs=%+v", uafEntry.Inputs)
	}
	if !depRecordedFor(key, h1RAGVR, h1NS, h1RAName) {
		t.Fatalf("arm (a): a cell that IS written must record its dep edge, or a DELETE/UPDATE can never invalidate it")
	}
	if got := cache.RestactionsUAFPutDeclined(); got != 0 {
		t.Fatalf("arm (a): an identity-bound class must not decline; counter=%d want 0", got)
	}

	// --- CONTROL arm: the same coordinates WITHOUT the UAF stanza still cache. ---
	rec = serve(t, false)
	if rec.Code != 200 {
		t.Fatalf("arm (a) control: expected 200; got %d body=%s", rec.Code, rec.Body.String())
	}
	entry, ok := handle.Get(key)
	if !ok {
		t.Fatalf("arm (a) CONTROL BROKE: a NON-UAF RESTAction was not Put under key %q — the decline is over-broad "+
			"(it would have disabled the restactions cache wholesale, and the UAF arm above would pass vacuously)", key)
	}
	if !bytes.Equal(entry.RawJSON, rec.Body.Bytes()) {
		t.Fatalf("arm (a) control: the Put'd bytes must equal the served bytes; put=%q served=%q", entry.RawJSON, rec.Body.Bytes())
	}
	if !depRecordedFor(key, h1RAGVR, h1NS, h1RAName) {
		t.Fatalf("arm (a) control: a genuine cold Put must still record the self-dep")
	}
	if got := cache.RestactionsUAFPutDeclined(); got != 0 {
		t.Fatalf("arm (a) control: nothing in this test may tick the restactions decline counter since #561; got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Arm (b) — BOOT SEED Put site (phase1_pip_seed.go seedOneRestaction)
// ---------------------------------------------------------------------------

// a1SeedFetchedRestaction is the seed-side twin of a1RAUnstructured: the
// objects.Result seedObjectsGetFn hands back, so the REAL FromUnstructured
// conversion inside seedOneRestaction produces the typed CR the gate reads.
func a1SeedFetchedRestaction(uaf bool) objects.Result {
	step := map[string]any{
		"name":        "list-namespaces",
		"path":        "/api/v1/namespaces",
		"endpointRef": map[string]any{"name": "krateo-kube", "namespace": "krateo-system"},
	}
	if uaf {
		step["userAccessFilter"] = map[string]any{
			"verb":          "get",
			"group":         "",
			"resource":      "configmaps",
			"namespaceFrom": ".metadata.name",
		}
	}
	return objects.Result{
		GVR: restActionGVR,
		Unstructured: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": restActionGVR.Group + "/" + restActionGVR.Version,
			"kind":       "RESTAction",
			"metadata":   map[string]any{"name": "uaf-seed-ra", "namespace": "krateo-system"},
			"spec":       map[string]any{"api": []any{step}},
		}},
	}
}

// TestA1_Seed_UAFSkipsResolveAndPut_ControlSeeds drives the REAL
// seedOneRestaction end-to-end (fetch → convert → gates → resolve+Put tail),
// with the fetch and the resolve+Put TAIL seamed exactly as the #113 M15 arm
// does. A UAF-bearing RA must never REACH the tail (the seed short-circuits
// before the resolve — its Put could never happen, so paying for the resolve is
// waste); a non-UAF RA must still reach it.
//
// Reaching the tail is the right observable: the tail is where the ONLY seed Put
// lives, so "tail not reached" is strictly stronger than "no Put happened".
//
// RED on origin/main: the UAF RA reaches the tail there (main seeds it and only
// stamps a short TTL) — the widest form of the leak, since the seed warms one
// cohort representative's narrowing for every co-bound member to read on their
// first /call.
func TestA1_Seed_UAFSkipsResolveAndPut_ControlSeeds(t *testing.T) {
	const user = "userGranted"
	buildGrantedRestactionWatcher(t, user)
	cache.RegisterUAFPutDeclineMetricsForTest()
	cache.ResetUAFPutDeclineCountersForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

	ref := templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: "uaf-seed-ra", Namespace: "krateo-system"},
		APIVersion: restActionGVR.Group + "/" + restActionGVR.Version,
		Resource:   restActionGVR.Resource,
	}

	var tailReached bool
	origPut := seedRestactionResolveAndPutFn
	origGet := seedObjectsGetFn
	t.Cleanup(func() {
		seedRestactionResolveAndPutFn = origPut
		seedObjectsGetFn = origGet
	})
	seedRestactionResolveAndPutFn = func(
		_, _ context.Context, _ *templatesv1.RESTAction, _ templatesv1.ObjectReference,
		_, _ string, _ cacheHandle, _ *cache.ResolvedKeyInputs, _ objects.Result,
		_ *cache.StageErrorSink, _ *cache.ExternalTouchedSink,
	) error {
		tailReached = true
		return nil
	}

	run := func(t *testing.T, uaf bool) bool {
		t.Helper()
		tailReached = false
		seedObjectsGetFn = func(_ context.Context, _ templatesv1.ObjectReference) objects.Result {
			return a1SeedFetchedRestaction(uaf)
		}
		if err := seedOneRestaction(seedCohortCtx(user), "cohort-granted", ref, "krateo-system", seedModeBoot); err != nil {
			t.Fatalf("seedOneRestaction returned %v; want nil (both the skip and the success path return nil)", err)
		}
		return tailReached
	}

	// #561 INVERTED THIS ARM, and the reasoning is worth stating because the old
	// text names a real hazard.
	//
	// It used to say: the seed warms a per-BINDING cell under a cohort
	// REPRESENTATIVE identity, so co-bound members would be served the
	// representative's narrowing. That is only a hazard if the representative
	// and the member can share a key while deserving different rows. They
	// cannot, for the same reason the serve path is safe:
	//
	//   - the representative is {Username:"", Groups:[g]}, whose SubjectBindingSet
	//     equals that of a real group-only member — seed/customer key parity,
	//     pinned by TestKey423_SeedRepresentativeParity;
	//   - equal binding set ⇒ equal RBAC verdicts (#588's falsifier) ⇒ the
	//     representative's narrowing IS the member's narrowing;
	//   - a member holding ANY additional binding derives a DIFFERENT key and
	//     simply does not reach this cell.
	//
	// So the seed must now warm these cells — and should, since not warming them
	// means a cold resolve on first read for exactly the widgets #561 made
	// cacheable (measured: 1,814ms p95 uncached vs 166ms cached).
	if !run(t, true) {
		t.Fatal("arm (b) #561: seedOneRestaction must now REACH the resolve+Put tail for a UAF-bearing RESTAction " +
			"in an identity-bound class. Short-circuiting leaves the cell cold, which is the latency cost #561 set " +
			"out to remove.")
	}
	if got := cache.RestactionsUAFPutDeclined(); got != 0 {
		t.Fatalf("arm (b): the seed must not decline an identity-bound class; got %d ticks, want 0", got)
	}

	if !run(t, false) {
		t.Fatal("arm (b) CONTROL BROKE: seedOneRestaction did NOT reach the tail for a NON-UAF RESTAction — the skip " +
			"is over-broad (it would have disabled the restactions boot seed wholesale, making the arm above vacuous).")
	}
	if got := cache.RestactionsUAFPutDeclined(); got != 0 {
		t.Fatalf("arm (b) control: nothing here may tick the restactions decline counter since #561; got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Arm (c) — REFRESHER re-Put site (resolve_populate.go)
// ---------------------------------------------------------------------------

// TestA1_Refresher_DeclinesCarriedHasUAF_ControlRefreshes drives the REAL
// resolveAndPopulateL1 over the REAL store with the resolve seam stubbed. The
// refresher holds no RESTAction CR, so its ONLY signal is the HasUAF the
// original Put carried on Inputs — this arm proves that carried flag is
// load-bearing at the re-Put gate.
//
// This site is DEFENSIVE SYMMETRY: since 1.12.3 no UAF cell can be created, so
// the refresher should never see one. It is wired anyway because a residual cell
// (written by a pre-1.12.3 process image, or by a future path) must not be
// refreshed into a longer life — the same posture as the external-touched gate
// beside it.
//
// RED on origin/main: the entry is overwritten with the fresh bytes there.
func TestA1_Refresher_DeclinesCarriedHasUAF_ControlRefreshes(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.RegisterUAFPutDeclineMetricsForTest()
	cache.ResetUAFPutDeclineCountersForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

	c := cache.ResolvedCache()
	restore := setResolveOnceForTest(func(context.Context, cache.ResolvedKeyInputs) ([]byte, error) {
		return []byte(`{"fresh":1}`), nil
	})
	t.Cleanup(restore)

	// refresh seeds a prior entry under inputs' key, runs one refresh cycle, and
	// returns the entry's bytes afterwards.
	refresh := func(t *testing.T, hasUAF bool, name string) string {
		t.Helper()
		inputs := cache.ResolvedKeyInputs{
			CacheEntryClass:        "restactions",
			Group:                  "templates.krateo.io",
			Version:                "v1",
			Resource:               "restactions",
			Namespace:              "krateo-system",
			Name:                   name,
			BindingUID:             "uid-shared-binding",
			RepresentativeUsername: "alice",
			HasUAF:                 hasUAF,
		}
		key := cache.ComputeKey(inputs)
		c.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: []byte(`{"prior":1}`), Inputs: &inputs})
		if err := resolveAndPopulateL1(context.Background(), inputs, nil, nil); err != nil {
			t.Fatalf("resolveAndPopulateL1(%s): %v", name, err)
		}
		e, ok := c.Get(key)
		if !ok {
			t.Fatalf("setup(%s): the entry vanished during the refresh", name)
		}
		return string(e.RawJSON)
	}

	// #561 INVERTED THIS ARM. It used to require the refresher to keep the prior
	// entry, on the premise that "the key does not separate co-bound users".
	// #423 made that false for an identity-bound class, so the refresher now
	// re-Puts — which is the whole point: a cell nothing can refresh goes stale
	// and then costs a full cold resolve on the next read.
	//
	// The refresher resolves under the cell's own cohort identity (#576, merged),
	// so the bytes it writes are narrowed for the same identity the key encodes.
	if got := refresh(t, true, "uaf-ra"); got != `{"fresh":1}` {
		t.Fatalf("arm (c) #561: a UAF-bearing cell in an IDENTITY-BOUND class must now be REFRESHED — its key "+
			"separates co-bound users, so there is nothing to protect by freezing it. Entry is %s, want the fresh "+
			"bytes.", got)
	}
	if got := cache.RestactionsUAFPutDeclined(); got != 0 {
		t.Fatalf("arm (c): the refresher must not decline an identity-bound cell; got %d ticks, want 0", got)
	}

	if got := refresh(t, false, "plain-ra"); got != `{"fresh":1}` {
		t.Fatalf("arm (c) CONTROL BROKE: a NON-UAF cell must still be refreshed; entry is %s, want the fresh bytes "+
			"(otherwise the decline is over-broad and has disabled the refresher wholesale)", got)
	}
	if got := cache.RestactionsUAFPutDeclined(); got != 0 {
		t.Fatalf("arm (c) control: nothing here may tick the restactions decline counter since #561; got %d", got)
	}
}

// ---------------------------------------------------------------------------
// Anti-shadow-drift: all three sites route through the ONE shared gate
// ---------------------------------------------------------------------------

// SUPERSEDED: the hand-listed three-file wiring guard that used to live here has
// been replaced by TestA1_EveryResolvedEntryPutSiteIsUAFGatedOrWaived
// (a1_uaf_site_enumeration_test.go), which enumerates ResolvedEntry Put sites
// STRUCTURALLY from the AST instead of from a list.
//
// The hand-list is worth remembering as a lesson rather than as code: it named
// restactions.go, resolve_populate.go and phase1_pip_seed.go, it passed, and it
// was guarding an open hot carrier the whole time. It could not have found the
// widgets class, because it was written from the same mistaken belief as the
// comment it was guarding. A guard built from the author's own enumeration can
// only confirm what the author already believed.

// TestA1_DeclineUAFPut_PredicateAndCounter pins the gate's own contract: it
// declines exactly when the entry is UAF-bearing, is nil-safe, and bumps the
// counter on (and only on) a decline — the property that makes
// snowplow_restactions_uaf_put_declined_total unable to drift from the gate.
func TestA1_DeclineUAFPut_PredicateAndCounter(t *testing.T) {
	// #561 REWROTE THIS CONTRACT. The gate no longer asks only "is this entry
	// UAF-bearing?" — it asks "is this entry UAF-bearing AND is its class keyed
	// WITHOUT identity?".
	//
	// WHY. The decline was written against the pre-#423 key, where two users
	// sharing a first-match binding derived an IDENTICAL key. #423 folds
	// SubjectBindingSet into the identity segment, which ComputeKey writes iff
	// !IsIdentityFreeClass. So:
	//
	//   identity-BOUND (restactions, widgets) — two requesters whose narrowed
	//     bodies differ necessarily hold different binding sets, hence different
	//     keys, hence different cells. No decline needed. Proven by #588's
	//     falsifier and end-to-end by the two cross-user arms.
	//
	//   identity-FREE (widgetContent) — the key folds NO identity, so #423
	//     separates nothing and ONE cell is shared by EVERY requester. The
	//     decline is still the only thing standing between a per-requester
	//     narrowed body and a cross-tenant leak. It STAYS.
	//
	// The arms below pin both halves. Deleting either one re-opens a leak or
	// silently re-disables the cache for the hot carrier.
	cache.ResetUAFPutDeclineCountersForTest()
	t.Cleanup(cache.ResetUAFPutDeclineCountersForTest)

	const bound = "restactions"                     // identity-BOUND
	const free = cache.CacheEntryClassWidgetContent // identity-FREE

	// Sanity: the two class names must actually differ in the property this
	// whole rule turns on, or every arm below is vacuous.
	if cache.IsIdentityFreeClass(bound) {
		t.Fatalf("precondition: %q must be identity-BOUND", bound)
	}
	if !cache.IsIdentityFreeClass(free) {
		t.Fatalf("precondition: %q must be identity-FREE", free)
	}

	if declineUAFPut(nil, nil) {
		t.Fatal("declineUAFPut(nil, nil) must be false — no inputs, no sink, no decline")
	}
	if declineUAFPut(&cache.ResolvedKeyInputs{CacheEntryClass: bound}, nil) {
		t.Fatal("a non-UAF entry must NOT be declined — every non-UAF cell caches byte-identically to 1.12.2")
	}

	// ── IDENTITY-BOUND: no longer declined, on EITHER limb. ──────────────────
	if declineUAFPut(&cache.ResolvedKeyInputs{CacheEntryClass: bound, HasUAF: true}, nil) {
		t.Fatal("#561: a UAF-DECLARING entry in an IDENTITY-BOUND class must NOT be declined — the key folds " +
			"SubjectBindingSet, so two requesters with different narrowings occupy different cells")
	}
	_, boundSink := cache.WithUAFTouchedSink(context.Background())
	boundSink.Bump()
	if declineUAFPut(&cache.ResolvedKeyInputs{CacheEntryClass: bound}, boundSink) {
		t.Fatal("#561: an OBSERVED refilter in an IDENTITY-BOUND class must NOT be declined either")
	}
	if got := cache.RestactionsUAFPutDeclined(); got != 0 {
		t.Fatalf("no identity-bound decline may happen; counter=%d want 0", got)
	}

	// ── IDENTITY-FREE: still declined, on BOTH limbs. ────────────────────────
	//
	// LIMB 1 — the DECLARATION, available pre-resolve.
	if !declineWidgetUAFPut(&cache.ResolvedKeyInputs{CacheEntryClass: free, HasUAF: true}, nil) {
		t.Fatal("LEAK: a UAF-DECLARING entry in an IDENTITY-FREE class MUST still be declined — its key folds no " +
			"identity at all, so one cell is shared by every requester and #423 separates nothing")
	}
	// LIMB 2 — the OBSERVED refilter, with HasUAF false.
	_, freeSink := cache.WithUAFTouchedSink(context.Background())
	freeSink.Bump()
	if !declineWidgetUAFPut(&cache.ResolvedKeyInputs{CacheEntryClass: free}, freeSink) {
		t.Fatal("LEAK: an entry whose resolve OBSERVED a refilter MUST still be declined in an IDENTITY-FREE " +
			"class even though its own Inputs declare no UAF")
	}
	if got := cache.WidgetsUAFPutDeclined(); got != 2 {
		t.Fatalf("both identity-free declines must tick the widgets-class counter; got %d want 2", got)
	}
	if got := cache.RestactionsUAFPutDeclined(); got != 0 {
		t.Fatalf("an identity-free widgetContent decline must NOT tick the restactions counter; got %d", got)
	}

	// ── The two limbs are reported distinctly, and neither firing means proceed.
	if r := uafDeclineReason(&cache.ResolvedKeyInputs{CacheEntryClass: free, HasUAF: true}, nil); r != uafDeclineDeclared {
		t.Fatalf("declaration limb must report %q; got %q", uafDeclineDeclared, r)
	}
	if r := uafDeclineReason(&cache.ResolvedKeyInputs{CacheEntryClass: free}, freeSink); r != uafDeclineObserved {
		t.Fatalf("sink limb must report %q; got %q", uafDeclineObserved, r)
	}
	if r := uafDeclineReason(nil, nil); r != "" {
		t.Fatalf("neither limb firing must report \"\" (proceed); got %q", r)
	}
	if r := uafDeclineReason(&cache.ResolvedKeyInputs{CacheEntryClass: bound, HasUAF: true}, freeSink); r != "" {
		t.Fatalf("#561: an identity-BOUND class must report \"\" (proceed) on both limbs; got %q", r)
	}

	// The gate reads HasUAF, which the CR-bearing sites stamp from the ONE
	// predicate on the API type. Pin that delegation here so a future inline
	// re-implementation in this package is caught.
	uaf := &templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{API: []*templatesv1.API{
		nil,
		{Name: "plain"},
		{Name: "refilter", UserAccessFilter: &templatesv1.UserAccessFilterSpec{Verb: "get", Resource: "configmaps"}},
	}}}
	if !restactionHasUAFStage(uaf) || !uaf.HasUserAccessFilterStage() {
		t.Fatal("restactionHasUAFStage must agree with RESTAction.HasUserAccessFilterStage — they are the same predicate")
	}
	plain := &templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{API: []*templatesv1.API{{Name: "plain"}}}}
	if restactionHasUAFStage(plain) || plain.HasUserAccessFilterStage() {
		t.Fatal("an RA with no userAccessFilter stage must not be reported UAF-bearing")
	}
	var nilRA *templatesv1.RESTAction
	if nilRA.HasUserAccessFilterStage() {
		t.Fatal("the predicate must be nil-receiver-safe (the apiref bypass calls it on a possibly-nil ra)")
	}
}
