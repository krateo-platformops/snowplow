// sensitive398_ra_full_list_test.go — #398 raFullList carrier arms, one per Put
// site in raFullListServe: the FIRST-SIGHT verify-then-Put and the REPOPULATE
// (known-sliceable verdict, cell absent) Put. The resolve closure stands in for
// the RA's steps and bumps the ctx's sensitive sink exactly as the resolver's
// per-call dispatch does for a core v1/secrets call.
package apiref

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func s398Rows(t *testing.T, sensitive bool, calls *atomic.Int64) func(context.Context, int, int) (map[string]any, error) {
	rows := k423PerUserRows(t, calls)
	return func(ctx context.Context, perPage, page int) (map[string]any, error) {
		if sensitive {
			cache.SensitiveTouchedSinkFromContext(ctx).Bump()
		}
		return rows(ctx, perPage, page)
	}
}

func s398CellHoldsSentinel(raKey string) bool {
	e, ok := cache.ResolvedCache().GetNoTouch(raKey)
	return ok && e != nil && strings.Contains(string(e.RawJSON), k423Sentinel)
}

func s398RAFullListSetup(t *testing.T, name string) (context.Context, string) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)
	newF6Watcher(t, k423RAFixture()...)
	ctx := f6CtxWithUser(t, "alice-423", []string{"portal-423"}) // allowed: the sentinel row is in her rows
	ctx, _ = cache.WithSensitiveTouchedSink(ctx)                 // installed by the widget handler in production
	_, raKey, ok := seedFullListRAKey(ctx, gvr(), "krateo-system", name, nil)
	if !ok {
		t.Fatalf("PRE: alice must derive a raKey")
	}
	return ctx, raKey
}

func TestS398_Carrier_RAFullList_FirstSightNotPut(t *testing.T) {
	const name = "s398-ra-first-sight"
	ctx, raKey := s398RAFullListSetup(t, name)
	var calls atomic.Int64
	before := cache.SensitiveSkippedPutForTest()
	got, served, err := raFullListServe(ctx, gvr(), "krateo-system", name, ra(raSliceJQ), 10, 1, nil, s398Rows(t, true, &calls))
	if err != nil || !served || got == nil {
		t.Fatalf("SETUP: the first-sight serve must still serve alice; served=%v err=%v", served, err)
	}
	if s398CellHoldsSentinel(raKey) {
		t.Fatalf("#398 RAFULLLIST FIRST-SIGHT: a full list built from a Secret read was cached")
	}
	if cache.SensitiveSkippedPutForTest() <= before {
		t.Fatalf("the first-sight sensitive decline did not fire")
	}
}

func TestS398_Carrier_RAFullList_RepopulateNotPut(t *testing.T) {
	const name = "s398-ra-repopulate"
	ctx, raKey := s398RAFullListSetup(t, name)
	var calls atomic.Int64
	// Establish a known-sliceable verdict + a cell from a NON-sensitive resolve...
	if _, served, err := raFullListServe(context.WithoutCancel(ctx), gvr(), "krateo-system", name, ra(raSliceJQ), 10, 1, nil,
		s398Rows(t, false, &calls)); err != nil || !served {
		t.Fatalf("SETUP: first non-sensitive serve; served=%v err=%v", served, err)
	}
	if _, ok := cache.ResolvedCache().GetNoTouch(raKey); !ok {
		t.Fatalf("SETUP: the non-sensitive serve must have cached the cell")
	}
	// ...then the cell is evicted and the RA (now with a Secret step) is resolved
	// again: the REPOPULATE branch.
	cache.ResolvedCache().DeleteForTest(raKey)
	ctx2, _ := cache.WithSensitiveTouchedSink(f6CtxWithUser(t, "alice-423", []string{"portal-423"}))
	before := cache.SensitiveSkippedPutForTest()
	if _, served, err := raFullListServe(ctx2, gvr(), "krateo-system", name, ra(raSliceJQ), 10, 1, nil,
		s398Rows(t, true, &calls)); err != nil || !served {
		t.Fatalf("SETUP: the repopulate serve must still serve alice; served=%v err=%v", served, err)
	}
	if s398CellHoldsSentinel(raKey) {
		t.Fatalf("#398 RAFULLLIST REPOPULATE: a full list built from a Secret read was re-cached")
	}
	if cache.SensitiveSkippedPutForTest() <= before {
		t.Fatalf("the repopulate sensitive decline did not fire")
	}
}
