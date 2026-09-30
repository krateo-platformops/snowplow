// ra_full_list_s323_gen_guard_test.go — #323 per-carrier real-race REFUSAL arm
// for the PutRAFullList producer (raFullListServe: CaptureGen(raKey) before the
// unpaginated re-resolve → PutRAFullListIfGen at the tail).
//
// The resolveRA param IS the seam (no production seam needed) — it sits between
// the raKey generation capture and the tail PutRAFullListIfGen. The wrapper runs
// the REAL resolve (so the byte-verify + slice succeed), then DELETE-evicts raKey
// — bumping its generation past the captured value — so the tail
// PutRAFullListIfGen REFUSES: the pre-delete full body is not resurrected (cell
// absent) and the refusal counter bumps. The request still serves the fresh slice
// (raFullListServe returns the verified Go-slice regardless). Body-independent
// discriminator (counter + cell-absent); catches a wrong capture key (M1) and a
// capture-after-resolve (M2). RED-captured by neutering the guard.

package apiref

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestRAServe_S323_ResolveRacingDelete_RefusedNotResurrected(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	newF6Watcher(t, f6BuildFixture()...)

	const raName = "compositions-panels-s323"
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatalf("resolved cache nil under RESOLVED_CACHE_ENABLED=true")
	}
	// The raKey the production path derives (admin re-derives the f6 first-match
	// BindingUID C:crb-a-f6-uid — matching TestRAServe_* in this file).
	keyInputs := cache.RAFullListKeyInputs(gvr().Group, gvr().Version, gvr().Resource,
		"krateo-system", raName, "C:crb-a-f6-uid", nil)
	raKey := cache.ComputeKey(keyInputs)
	refusedBefore := store.Stats().PutRefusedGenerationMovedTotal

	panels := panelDict(40)
	var calls atomic.Int64
	base := stubResolveRA(t, panels, &calls)
	// Wrap the resolve seam: the unpaginated full resolve runs AFTER
	// raFullListServe captured raKey's generation and BEFORE its tail
	// PutRAFullListIfGen — DELETE-evict raKey there, bumping the generation past
	// the captured value.
	resolve := func(rctx context.Context, perPage, page int) (map[string]any, error) {
		full, err := base(rctx, perPage, page)
		store.Put(raKey, &cache.ResolvedEntry{RawJSON: []byte(`{"racer":true}`)})
		store.DeleteForTest(raKey)
		return full, err
	}
	ctx := ctxWithUser(t)

	_, _, err := raFullListServe(ctx, gvr(), "krateo-system", raName,
		ra(raSliceJQ), 10, 1, nil, resolve)
	if err != nil {
		t.Fatalf("raFullListServe error: %v", err)
	}

	if got := store.Stats().PutRefusedGenerationMovedTotal; got != refusedBefore+1 {
		t.Fatalf("#323 RAFullList producer A5 RED: the tail PutRAFullListIfGen was NOT refused (refused_total %d->%d) — a DELETE during the unpaginated resolve must refuse the raKey Put (wrong key M1 / capture-after-resolve M2 resurrects here).", refusedBefore, got)
	}
	if _, present := store.Get(raKey); present {
		t.Fatalf("#323 RAFullList producer A5 RED: the DELETE-evicted raKey cell was RESURRECTED under %q", raKey)
	}
}
