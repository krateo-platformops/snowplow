// issue406_seed_memo_test.go — #406 second driver (dev-411): the boot/post-readyz SEED
// pass resolves widgets through apiref.Resolve with a SeedResolveMemo. An RA edit that
// lands BEFORE the seed widget resolve dirty-marks raKey (resident, not yet refreshed);
// the widget cells do not exist yet, so nothing marks them, and their #375 sink cannot
// see the bump (it predates startSeq). The memo-MISS widget slices the stale raKey
// through the 4a fast path; every memo-HIT sibling replays that same stale body. When
// raKey is then refreshed with the new content, all of them must be remarked and
// converge.
//
// Drives the REAL apiref.Resolve (memo B2/B3 wiring + raFullListServe) per the
// reference_apiref_resolve_e2e_unit_harness recipe, then drains the dirty-mark queue
// with the production refresh sequence (CaptureGen → re-resolve → ReplaceIfGen).

package apiref

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	pmaps "github.com/krateo-platformops/plumbing/maps"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue406_SeedMemo_RAEditBeforeSeed_AllSeededWidgetsConverge(t *testing.T) {
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	const raName = "ra-406-seed-memo"
	const ns = edge3BackingNS
	gRA := gvr()
	backing := edge3BackingGVR()
	edge3NewWatcher(t, append(f6BuildFixture(), edge3RAUnstructured(ns, raName))...)
	if added, syncCh := cache.Global().EnsureResourceType(gRA); added {
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("restactions informer did not sync")
		}
	}
	if !edge3WaitServable(t, gRA, 3*time.Second) {
		t.Fatalf("restactions GVR never became servable")
	}
	store := cache.ResolvedCache()
	d := cache.Deps()
	d.SetStore(store)

	marker := "OLD"
	resolve := edge3StubResolveRA(t, &marker, backing)
	base := f6CtxWithUser(t, "admin", []string{"system:masters"})

	// raKey warm at OLD (a prior first-sight): resident cell + known verdict + edges.
	raIn := f6KeyInputs(gRA.Group, gRA.Version, gRA.Resource, ns, raName, "C:crb-a-f6-uid", nil)
	raKey := cache.ComputeKey(raIn)
	full, err := resolve(cache.WithL1KeyContext(base, raKey), 0, 0)
	if err != nil {
		t.Fatalf("pre-warm resolve: %v", err)
	}
	store.PutRAFullList(raKey, raIn, full)
	cache.RecordSliceability(raKey, seedFullListShape(gRA, ns, raName, ra(raSliceJQ)), true)
	d.Record(context.Background(), raKey, gRA, ns, raName)

	// Dirty-mark queue (workqueue FIFO + dedup), Lever C applied synchronously.
	var queue []string
	queued := map[string]bool{}
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
		cache.InvalidateSliceabilityForKey(k)
		if !queued[k] {
			queued[k] = true
			queue = append(queue, k)
		}
	})

	// The RA is edited BEFORE the seed pass reaches its widgets: raKey is marked, no
	// widget cell exists yet.
	marker = "NEW"
	d.OnUpdate(gRA, ns, raName)
	if !queued[raKey] {
		t.Fatalf("PRECONDITION: the RA edit must dirty-mark raKey")
	}

	// The seed pass: three widgets share (RA, identity, page) → one memo MISS + two HITs.
	memo := cache.NewSeedResolveMemo(pmaps.DeepCopyJSON)
	opts := ResolveOptions{
		ApiRef: templatesv1.ObjectReference{
			Reference:  templatesv1.Reference{Name: raName, Namespace: ns},
			Resource:   "restactions",
			APIVersion: "templates.krateo.io/v1",
		},
		PerPage: 5, Page: 1,
	}
	widgets := []string{"L1_w406_seed_a", "L1_w406_seed_b", "L1_w406_seed_c"}
	hit0 := cache.RAFullListServeSnapshot().Hit
	for i, w := range widgets {
		ctx := cache.WithSeedResolveMemo(cache.WithL1KeyContext(base, w), memo)
		body, err := Resolve(ctx, opts)
		if err != nil {
			t.Fatalf("seed Resolve(%s): %v", w, err)
		}
		// Boot seed terminal Put: plain (#323), via PutThenRemark(resCtx) (seedTerminalPut).
		store.PutThenRemark(ctx, w, &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, body), Inputs: edge3WidgetInputs("w406seed" + itoa2(i))})
	}
	if got := cache.RAFullListServeSnapshot().Hit - hit0; got != 1 {
		t.Fatalf("[prove-path] want exactly one 4a fast-path HIT (the memo MISS), the rest memo HITs; got %d", got)
	}
	if hits, misses := memo.Stats(); hits != 2 || misses != 1 {
		t.Fatalf("[prove-path] want 1 memo miss + 2 hits, got misses=%d hits=%d", misses, hits)
	}
	for _, w := range widgets {
		if !seed406Has(t, w, "OLD") {
			t.Fatalf("PRECONDITION: seeded %s must hold the stale OLD slice (the bug's entry state)", w)
		}
	}

	// Refresher drains: raKey first (the only marked key), then whatever it remarks.
	for n := 0; len(queue) > 0; n++ {
		if n > 16 {
			t.Fatalf("remark storm: queue=%v", queue)
		}
		k := queue[0]
		queue = queue[1:]
		delete(queued, k)
		gen0 := store.CaptureGen(k)
		rctx := cache.WithL1KeyContext(base, k)
		var e *cache.ResolvedEntry
		if k == raKey {
			f, err := resolve(rctx, 0, 0)
			if err != nil {
				t.Fatalf("raKey refresh: %v", err)
			}
			in := raIn
			e = &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, f), Inputs: &in}
		} else {
			d.Record(rctx, k, gRA, ns, raName)
			got, ok, err := raFullListServe(rctx, gRA, ns, raName, ra(raSliceJQ), 5, 1, nil, resolve)
			if err != nil || !ok {
				t.Fatalf("widget refresh %s: ok=%v err=%v", k, ok, err)
			}
			e = &cache.ResolvedEntry{RawJSON: edge3MustJSON(t, got), Inputs: edge3WidgetInputs(k)}
		}
		if !store.ReplaceIfGen(rctx, k, e, gen0) {
			t.Fatalf("refresh %s: ReplaceIfGen refused", k)
		}
	}
	for _, w := range widgets {
		if !seed406Has(t, w, "NEW") {
			t.Fatalf("#406 RED (seed memo): seeded widget %s is STALE (OLD) after raKey was refreshed to NEW — "+
				"it holds a slice (directly, or via a memo hit) of the pre-edit raKey and nothing re-marked it", w)
		}
	}
}

func seed406Has(t *testing.T, key, marker string) bool {
	t.Helper()
	e, ok := cache.ResolvedCache().GetNoTouch(key)
	if !ok {
		t.Fatalf("%s not resident", key)
	}
	var body map[string]any
	if err := json.Unmarshal(e.RawJSON, &body); err != nil {
		t.Fatalf("decode %s: %v", key, err)
	}
	return f6ContainsMarker(t, body, marker)
}
