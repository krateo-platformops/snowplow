package apiref

// issue1319_stage_outcomes_test.go — #1319: a widget's RESTAction stage
// outcomes reach the widget's collector on every way apiref.Resolve can answer
// without running the page-keyed resolve itself.
//
//   F1 the 4a full list: the first sight stores the UNPAGINATED resolve's
//      outcomes with the raKey cell; a hit (no resolve) replays them;
//   F2 the seed memo: the miss (here a 4a hit) hands its outcomes to its caller
//      and stores them; each memo hit replays them to its own caller.
//
// The stub resolves report through api.ReplayStageOutcomes, the claim a real
// top-level resolve makes on the collector its ctx carries.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

const (
	f1319Full = `[{"name":"list","ok":false,"reason":"Forbidden"}]`
	f1319Page = `[{"name":"list","ok":false,"reason":"NotFound"}]`
)

func TestIssue1319_F1_FullListStoresAndReplaysItsOutcomes(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	newF6Watcher(t, f6BuildFixture()...)

	const raName = "ra-1319-full-list"
	var calls atomic.Int64
	inner := stubResolveRA(t, panelDict(40), &calls)
	// The unpaginated resolve and the page-keyed one report DIFFERENT values,
	// so the arm can tell which one the cell keeps.
	resolve := func(ctx context.Context, perPage, page int) (map[string]any, error) {
		if perPage > 0 && page > 0 {
			api.ReplayStageOutcomes(ctx, f1319Page)
		} else {
			api.ReplayStageOutcomes(ctx, f1319Full)
		}
		return inner(ctx, perPage, page)
	}

	ctx1, o1 := api.WithStageOutcomes(ctxWithUser(t))
	if _, ok, err := raFullListServe(ctx1, gvr(), "krateo-system", raName, ra(raSliceJQ), 10, 1, nil, resolve); err != nil || !ok {
		t.Fatalf("first sight: ok=%v err=%v", ok, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("SETUP: the first sight must resolve twice (page-keyed + unpaginated), got %d", calls.Load())
	}
	if got := api.StorableStageOutcomes(o1); got != f1319Page {
		t.Fatalf("F1: the first sight's caller reads %q, want its own page-keyed resolve's %q", got, f1319Page)
	}
	keyInputs, raKey, ok := seedFullListRAKey(ctx1, gvr(), "krateo-system", raName, nil)
	if !ok {
		t.Fatalf("SETUP: no raKey (inputs %+v)", keyInputs)
	}
	cell, ok := cache.ResolvedCache().GetNoTouch(raKey)
	if !ok {
		t.Fatal("SETUP: the first sight did not store the full list")
	}
	if cell.StageOutcomes != f1319Full {
		t.Fatalf("F1 RED: the full-list cell keeps %q, want the unpaginated resolve's %q", cell.StageOutcomes, f1319Full)
	}

	ctx2, o2 := api.WithStageOutcomes(ctxWithUser(t))
	if _, ok, err := raFullListServe(ctx2, gvr(), "krateo-system", raName, ra(raSliceJQ), 10, 2, nil, resolve); err != nil || !ok {
		t.Fatalf("hit: ok=%v err=%v", ok, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("SETUP: page 2 must be a hit (no resolve), resolves=%d", calls.Load())
	}
	if got := api.StorableStageOutcomes(o2); got != f1319Full {
		t.Fatalf("F1 RED: a full-list hit reads %q, want the stored %q", got, f1319Full)
	}
}

func TestIssue1319_F2_SeedMemoMissAndHitsCarryTheOutcomes(t *testing.T) {
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	const raName = "ra-1319-seed-memo"
	const ns = edge3BackingNS
	gRA := gvr()
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

	marker := "M1319"
	resolve := edge3StubResolveRA(t, &marker, edge3BackingGVR())
	base := f6CtxWithUser(t, "admin", []string{"system:masters"})
	raIn := f6KeyInputs(gRA.Group, gRA.Version, gRA.Resource, ns, raName, "C:crb-a-f6-uid", nil)
	raKey := cache.ComputeKey(raIn)
	rctx := cache.WithL1KeyContext(base, raKey)
	full, err := resolve(rctx, 0, 0)
	if err != nil {
		t.Fatalf("pre-warm resolve: %v", err)
	}
	if put := store.PutRAFullListIfGenOutcome(rctx, fixtureStoreVerdict, raKey, raIn, full, f1319Full, store.CaptureGen(raKey)); !put.Stored() {
		t.Fatal("SETUP: the full-list cell was not stored")
	}
	cache.RecordSliceability(raKey, seedFullListShape(gRA, ns, raName, ra(raSliceJQ)), true)
	d.Record(context.Background(), raKey, gRA, ns, raName)

	memo := cache.NewSeedResolveMemo()
	opts := ResolveOptions{
		ApiRef: templatesv1.ObjectReference{
			Reference:  templatesv1.Reference{Name: raName, Namespace: ns},
			Resource:   "restactions",
			APIVersion: "templates.krateo.io/v1",
		},
		PerPage: 5, Page: 1,
	}
	for _, w := range []string{"L1_w1319_a", "L1_w1319_b", "L1_w1319_c"} {
		ctx, o := api.WithStageOutcomes(cache.WithSeedResolveMemo(cache.WithL1KeyContext(base, w), memo))
		if _, err := Resolve(ctx, opts); err != nil {
			t.Fatalf("Resolve(%s): %v", w, err)
		}
		if got := api.StorableStageOutcomes(o); got != f1319Full {
			t.Fatalf("F2 RED: widget %s's resolve reads %q, want the full list's %q", w, got, f1319Full)
		}
	}
	if hits, misses := memo.Stats(); hits != 2 || misses != 1 {
		t.Fatalf("SETUP: want 1 memo miss + 2 hits, got misses=%d hits=%d", misses, hits)
	}
}
