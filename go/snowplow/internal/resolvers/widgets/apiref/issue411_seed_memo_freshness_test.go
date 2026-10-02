// issue411_seed_memo_freshness_test.go — #411 falsifiers: a SeedResolveMemo hit
// must not reuse a body whose deps moved since it was produced. RED-first.
//
// THE DEFECT. Within one seed pass, widgets sharing (RA, identity, extras,
// perPage, page) reuse ONE memoised RESTAction body. #277 replays the memo's
// captured dep edges onto each hitter's L1 key, but nothing checked that the
// BODY was still fresh at the hit:
//  1. widget A produces the memo body at T0;
//  2. a dep of that body changes at T1 > T0;
//  3. widget B's resolve starts at T2 > T1 (its #375 startSeq) and hits the
//     memo, serving the pre-T1 body;
//  4. #375's accepted-Put check looks only at bumps AFTER B's startSeq, so the
//     T1 bump is invisible: B stores the stale body as fresh.
//
// HARNESS. The #277 e2e idiom (TestEdge3_E2E_ResolveB2B3WiringThroughApirefResolve):
// a real ResourceWatcher over a dynamic.fake serving the RESTAction CR to
// objects.Get, the real DepTracker + ResolvedCacheStore, a PRE-WARMED raKey
// (resident + known-sliceable) so every widget resolve is served through the
// REAL apiref.Resolve → (memo | raFullListServe 4a fast path) wiring. The
// backing mutation is driven through cache.Deps().OnUpdate → OnObjectEvent —
// the decision site the informer bridge calls, which is also where #375 bumps
// the coordinate's generation. The refresher is modelled by draining the
// dirty-marked keys the real fan-out produced: raKey is re-resolved first (its
// edge-2 dirty-mark), then any resident widget re-served over it.

package apiref

import (
	"context"
	"sync"
	"testing"
	"time"

	pmaps "github.com/krateo-platformops/plumbing/maps"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type i411Fixture struct {
	t       *testing.T
	base    context.Context
	gRA     schema.GroupVersionResource
	backing schema.GroupVersionResource
	raName  string
	raKey   string
	raIn    cache.ResolvedKeyInputs
	marker  *string
	resolve func(context.Context, int, int) (map[string]any, error)
	memo    *cache.SeedResolveMemo
	opts    ResolveOptions

	mu     sync.Mutex
	marked map[string]bool
}

func i411Setup(t *testing.T, raName string) *i411Fixture {
	t.Helper()
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()

	const ns = edge3BackingNS
	gRA := gvr()
	backing := edge3BackingGVR()
	seed := append(f6BuildFixture(), edge3RAUnstructured(ns, raName))
	edge3NewWatcher(t, seed...)

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
	f := &i411Fixture{
		t: t, gRA: gRA, backing: backing, raName: raName, marker: &marker,
		base:   f6CtxWithUser(t, "admin", []string{"system:masters"}),
		memo:   cache.NewSeedResolveMemo(pmaps.DeepCopyJSON),
		marked: map[string]bool{},
	}
	f.resolve = edge3StubResolveRA(t, f.marker, backing)
	f.raIn = cache.RAFullListKeyInputs(gRA.Group, gRA.Version, gRA.Resource, ns, raName, "C:crb-a-f6-uid", nil)
	f.raKey = cache.ComputeKey(f.raIn)
	shape := seedFullListShape(gRA, ns, raName, ra(raSliceJQ))
	fullDict, err := f.resolve(cache.WithL1KeyContext(f.base, f.raKey), 0, 0) // edge-2 under raKey
	if err != nil {
		t.Fatalf("pre-warm resolve failed: %v", err)
	}
	store.PutRAFullList(f.raKey, f.raIn, fullDict)
	cache.RecordSliceability(f.raKey, shape, true)
	d.Record(context.Background(), f.raKey, gRA, ns, raName) // RA-CR self-dep

	f.opts = ResolveOptions{ApiRef: templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: raName, Namespace: ns},
		Resource:   "restactions",
		APIVersion: "templates.krateo.io/v1",
	}, PerPage: 5, Page: 1}

	// The refresher hook only RECORDS the real fan-out's dirty-marks; drain()
	// processes them afterwards in dependency order (raKey before widgets).
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
		f.mu.Lock()
		f.marked[k] = true
		f.mu.Unlock()
	})
	t.Cleanup(func() { d.SetRefreshHook(nil) })
	return f
}

// seedResolve runs ONE seed-pass widget resolve for key through the REAL
// apiref.Resolve with the pass's memo installed, modelling seedOneWidget's
// post-readyz terminal Put: capture the gen BEFORE the resolve, PutIfGen after.
// Returns the resolved body.
func (f *i411Fixture) seedResolve(key, name string) map[string]any {
	f.t.Helper()
	store := cache.ResolvedCache()
	gen := store.CaptureGen(key)
	ctx := cache.WithSeedResolveMemo(cache.WithL1KeyContext(f.base, key), f.memo)
	body, err := Resolve(ctx, f.opts)
	if err != nil {
		f.t.Fatalf("seed apiref.Resolve(%s) failed: %v", key, err)
	}
	store.PutIfGen(ctx, key, &cache.ResolvedEntry{RawJSON: edge3MustJSON(f.t, body), Inputs: edge3WidgetInputs(name)}, gen)
	return body
}

// mutateBacking flips the backing marker to NEW and drives ONE real backing
// UPDATE through the dep tracker's decision site (generation bump + dirty-mark).
func (f *i411Fixture) mutateBacking() {
	*f.marker = "NEW"
	cache.Deps().OnUpdate(f.backing, edge3BackingNS, edge3BackingName)
}

// drain models the refresher processing the dirty-marks the real fan-out
// produced: raKey (edge-2) is re-resolved and re-pinned first, then every
// dirty-marked resident widget in widgets is re-served over the fresh raKey cell
// and Put — exactly what a keepwarm refresh of a resident widget cell writes.
func (f *i411Fixture) drain(widgets map[string]string) {
	f.t.Helper()
	f.mu.Lock()
	marked := f.marked
	f.marked = map[string]bool{}
	f.mu.Unlock()
	if !marked[f.raKey] {
		f.t.Fatalf("setup: the backing UPDATE did not dirty-mark raKey (edge-2) — marked=%v", marked)
	}
	store := cache.ResolvedCache()
	fresh, err := f.resolve(cache.WithL1KeyContext(f.base, f.raKey), 0, 0)
	if err != nil {
		f.t.Fatalf("raKey refresh resolve failed: %v", err)
	}
	store.PutRAFullList(f.raKey, f.raIn, fresh)
	for key, name := range widgets {
		if !marked[key] {
			continue
		}
		rctx := cache.WithL1KeyContext(f.base, key) // refresher ctx: NO memo
		got, ok, err := raFullListServe(rctx, f.gRA, edge3BackingNS, f.raName, ra(raSliceJQ), 5, 1, nil, f.resolve)
		if err != nil || !ok {
			f.t.Fatalf("refresh re-serve of %q failed: ok=%v err=%v", key, ok, err)
		}
		store.Put(key, &cache.ResolvedEntry{RawJSON: edge3MustJSON(f.t, got), Inputs: edge3WidgetInputs(name)})
	}
}

// TestIssue411_MemoHitAfterDepMove_ResolvesFresh — arm 1 (RED on main) + the
// no-cascade arm. w1 produces the memo body (OLD). A real backing UPDATE lands
// and the refresher re-pins raKey with NEW. w2 — a cold widget whose seed
// resolve starts AFTER the move — must NOT reuse w1's pre-move body: it resolves
// fresh (through the 4a fast path over the refreshed raKey) and its stored cell
// carries NEW. w3, after w2, must hit the REPLACED entry (one fresh resolve
// replaced it; a hit-turned-miss does not cascade).
func TestIssue411_MemoHitAfterDepMove_ResolvesFresh(t *testing.T) {
	f := i411Setup(t, "i411-arm1-ra")
	const w1Key, w2Key, w3Key = "L1_i411_arm1_w1", "L1_i411_arm1_w2", "L1_i411_arm1_w3"

	before := cache.RAFullListServeSnapshot()
	body1 := f.seedResolve(w1Key, "w1")
	if cache.RAFullListServeSnapshot().Hit-before.Hit != 1 {
		t.Fatalf("[C-prove-path] w1 must be produced by the 4a fast-path HIT through apiref.Resolve")
	}
	if !f6ContainsMarker(t, body1, "OLD") {
		t.Fatalf("setup: w1's body must be the pre-move OLD body")
	}

	f.mutateBacking()
	f.drain(nil) // the refresher re-pins raKey (NEW); no resident widget to refresh

	beforeW2 := cache.RAFullListServeSnapshot()
	body2 := f.seedResolve(w2Key, "w2")
	if f6ContainsMarker(t, body2, "OLD") || !f6ContainsMarker(t, body2, "NEW") {
		t.Fatalf("#411 RED: w2's seed resolve began AFTER the backing dep moved, yet it reused w1's pre-move memo body " +
			"(served OLD) — the memo hit accepted a body whose deps moved since it was produced, and #375's Put-check " +
			"cannot see a bump older than w2's own startSeq")
	}
	if !edge3ServedContainsMarker(t, w2Key, "NEW") || edge3ServedContainsMarker(t, w2Key, "OLD") {
		t.Fatalf("#411 RED: w2's STORED cell does not reflect the dep change")
	}
	if cache.RAFullListServeSnapshot().Hit-beforeW2.Hit != 1 {
		t.Fatalf("w2 must have resolved fresh (one 4a serve over the refreshed raKey); serve delta=%+v",
			cache.RAFullListServeSnapshot())
	}
	if !edge3EdgesContainBacking(cache.Deps().EdgesUnder(w2Key), f.backing) {
		t.Fatalf("w2's fresh resolve must carry edge-3: %v", cache.Deps().EdgesUnder(w2Key))
	}

	// No cascade: w3 hits the entry w2's fresh resolve replaced.
	hitsBefore, _ := f.memo.Stats()
	beforeW3 := cache.RAFullListServeSnapshot()
	body3 := f.seedResolve(w3Key, "w3")
	hitsAfter, _ := f.memo.Stats()
	if hitsAfter-hitsBefore != 1 {
		t.Fatalf("no-cascade: w3 must HIT the memo entry w2's fresh resolve replaced; hit delta=%d", hitsAfter-hitsBefore)
	}
	if a := cache.RAFullListServeSnapshot(); a.Hit != beforeW3.Hit || a.VerifiedSlice != beforeW3.VerifiedSlice || a.Repopulate != beforeW3.Repopulate {
		t.Fatalf("no-cascade: w3 re-resolved instead of reusing the replaced entry: before=%+v after=%+v", beforeW3, a)
	}
	if !f6ContainsMarker(t, body3, "NEW") || f6ContainsMarker(t, body3, "OLD") {
		t.Fatalf("no-cascade: w3 must be served the REPLACED (NEW) body")
	}
	if !edge3EdgesContainBacking(cache.Deps().EdgesUnder(w3Key), f.backing) {
		t.Fatalf("#277: w3's memo hit must replay edge-3 onto w3Key: %v", cache.Deps().EdgesUnder(w3Key))
	}
}

// TestIssue411_MemoHitNoDepChange_Reuses — arm 2 (no amplification). With no dep
// event between production and hit, w2 reuses w1's memo body: one memo hit, no
// raFullListServe, and #277's replayed edge-3 lands on w2Key.
func TestIssue411_MemoHitNoDepChange_Reuses(t *testing.T) {
	f := i411Setup(t, "i411-arm2-ra")
	const w1Key, w2Key = "L1_i411_arm2_w1", "L1_i411_arm2_w2"

	f.seedResolve(w1Key, "w1")

	hitsBefore, missBefore := f.memo.Stats()
	before := cache.RAFullListServeSnapshot()
	body2 := f.seedResolve(w2Key, "w2")
	hitsAfter, missAfter := f.memo.Stats()
	if hitsAfter-hitsBefore != 1 || missAfter != missBefore {
		t.Fatalf("arm 2: with no dep change w2 must HIT the memo (hit delta=%d, miss delta=%d)",
			hitsAfter-hitsBefore, missAfter-missBefore)
	}
	if a := cache.RAFullListServeSnapshot(); a.Hit != before.Hit || a.VerifiedSlice != before.VerifiedSlice || a.Repopulate != before.Repopulate {
		t.Fatalf("arm 2: w2 re-resolved with no dep change (amplification): before=%+v after=%+v", before, a)
	}
	if !f6ContainsMarker(t, body2, "OLD") {
		t.Fatalf("arm 2: w2 must be served the memoised body")
	}
	if !edge3EdgesContainBacking(cache.Deps().EdgesUnder(w2Key), f.backing) {
		t.Fatalf("#277: w2's memo hit must replay edge-3 onto w2Key: %v", cache.Deps().EdgesUnder(w2Key))
	}
}

// TestIssue411_KeepwarmResidentNotOverwrittenByStaleMemo — arm 3 (keepwarm,
// RED on main). w2 is a RESIDENT widget cell (a prior pass stored it, with its
// edge-3). Within the current seed pass w1 produces the memo body (OLD). A real
// backing UPDATE then dirty-marks raKey AND w2 (it has the edge); the refresher
// writes fresh NEW content into w2. The seed pass then reaches w2: its memo-hit
// PutIfGen is ACCEPTED (the gen moves only on removal; a refresh does not move
// it), so a stale memo body would overwrite the refresher's fresh cell. The cell
// must still carry NEW afterwards.
func TestIssue411_KeepwarmResidentNotOverwrittenByStaleMemo(t *testing.T) {
	f := i411Setup(t, "i411-arm3-ra")
	const w1Key, w2Key = "L1_i411_arm3_w1", "L1_i411_arm3_w2"
	store := cache.ResolvedCache()

	// Resident w2 from a PRIOR pass (no memo on that ctx): OLD body + edge-3.
	prior, ok, err := raFullListServe(cache.WithL1KeyContext(f.base, w2Key), f.gRA, edge3BackingNS, f.raName,
		ra(raSliceJQ), 5, 1, nil, f.resolve)
	if err != nil || !ok {
		t.Fatalf("prior-pass w2 serve failed: ok=%v err=%v", ok, err)
	}
	store.Put(w2Key, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, prior), Inputs: edge3WidgetInputs("w2")})

	// Current pass: w1 produces the memo body (OLD).
	f.seedResolve(w1Key, "w1")

	f.mutateBacking()
	f.mu.Lock()
	w2Marked := f.marked[w2Key]
	f.mu.Unlock()
	if !w2Marked {
		t.Fatalf("setup: resident w2 (with edge-3) must be dirty-marked by the backing UPDATE")
	}
	f.drain(map[string]string{w2Key: "w2"})
	if !edge3ServedContainsMarker(t, w2Key, "NEW") {
		t.Fatalf("setup: the refresher must have written NEW into resident w2")
	}

	// The seed pass reaches w2.
	f.seedResolve(w2Key, "w2")
	if edge3ServedContainsMarker(t, w2Key, "OLD") || !edge3ServedContainsMarker(t, w2Key, "NEW") {
		t.Fatalf("#411 RED (keepwarm): the seed pass's memo-hit Put overwrote the refresher's FRESH w2 cell with " +
			"w1's pre-move memo body — the PutIfGen was accepted (a refresh does not move the gen) and nothing " +
			"remarks it, so w2 stays stale until the next dep event or TTL")
	}
}

// TestIssue411_MemoHitAfterRESTActionEdit_ResolvesFresh — the RESTAction CR is
// the body's FIRST dep: objects.Get reads it (and Records it under the widget
// key) BEFORE the memo capture opens, so it is not among the captured deps.
// The memo key folds (RA ns/name, identity, extras, page) but not the RA's
// spec, so an RA edit between w1's production and w2's hit would let w2 serve
// a body computed from the PRE-EDIT spec although w2 itself read the post-edit
// CR. The edit (a real UPDATE through the dep tracker's decision site) must
// turn w2's lookup into a miss.
func TestIssue411_MemoHitAfterRESTActionEdit_ResolvesFresh(t *testing.T) {
	f := i411Setup(t, "i411-raedit-ra")
	const w1Key, w2Key = "L1_i411_raedit_w1", "L1_i411_raedit_w2"

	// Unpaginated (the Statistic/Tag shape the memo exists for): the body comes
	// from the real restactions.Resolve, not the 4a fast path — so the RA CR is
	// not replayed into the capture from raKey's edges.
	f.opts.PerPage, f.opts.Page = 0, 0
	f.seedResolve(w1Key, "w1")
	cache.Deps().OnUpdate(f.gRA, edge3BackingNS, f.raName) // the RESTAction CR is edited

	hitsBefore, missBefore := f.memo.Stats()
	f.seedResolve(w2Key, "w2")
	hitsAfter, missAfter := f.memo.Stats()
	if hitsAfter != hitsBefore || missAfter-missBefore != 1 {
		t.Fatalf("#411 RED (RA edit): w2's resolve began after its RESTAction CR was edited, yet it reused w1's "+
			"pre-edit memo body (hit delta=%d, miss delta=%d)", hitsAfter-hitsBefore, missAfter-missBefore)
	}
}
