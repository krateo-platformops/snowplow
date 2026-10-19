package cache

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestServeCellIf765 — #765 content-stamp reuse reads through ServeCellIf, the
// #643 served-cell path for an internal reader that may reject what it read:
//   - accepted: the body is served and the cell is noted on the resolve's sink;
//   - rejected: hit, NOT served, and NOT noted (a cell that served nothing must
//     not be folded into a declined-external mark);
//   - absent: no hit;
//   - inert (dry run): served, never noted;
//   - metric-neutral: neither read moves hit_total or miss_total.
func TestServeCellIf765(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	ResetResolvedCacheForTest()
	t.Cleanup(ResetResolvedCacheForTest)
	c := ResolvedCache()
	if c == nil {
		t.Fatalf("PRECONDITION: no resolved cache")
	}
	const key = "served-cell-765"
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"items":[]}`), ContentAsOf: 7})
	dk := DepKey{GVR: schema.GroupVersionResource{Group: "g", Version: "v1", Resource: "rs"}, Namespace: "ns", Name: listWildcard}
	h0, m0 := c.hitTotal.Load(), c.missTotal.Load()

	ctx := WithDepGenSink(context.Background())
	e, hit, served := ServeCellIf(ctx, c, key, func(e *ResolvedEntry) bool { return e.ContentAsOf >= 8 }, dk)
	if !hit || served || e == nil {
		t.Fatalf("rejected read: hit=%v served=%v entry=%v, want hit, not served, the entry", hit, served, e != nil)
	}
	if got := ServedCellsFromContext(ctx); len(got) != 0 {
		t.Fatalf("RED: a REJECTED cell was noted served: %+v", got)
	}

	e, hit, served = ServeCellIf(ctx, c, key, func(e *ResolvedEntry) bool { return e.ContentAsOf >= 7 }, dk)
	if !hit || !served || e == nil {
		t.Fatalf("accepted read: hit=%v served=%v", hit, served)
	}
	got := ServedCellsFromContext(ctx)
	if len(got) != 1 || got[0].Key != key || len(got[0].Covers) != 1 || got[0].Covers[0] != dk {
		t.Fatalf("RED: the accepted cell was not noted exactly once with its dep: %+v", got)
	}

	if _, hit, served = ServeCellIf(ctx, c, "absent-765", nil, dk); hit || served {
		t.Fatalf("absent key: hit=%v served=%v", hit, served)
	}

	ictx := WithInert(WithDepGenSink(context.Background()))
	if _, _, served = ServeCellIf(ictx, c, key, nil, dk); !served {
		t.Fatalf("inert read not served")
	}
	if got := ServedCellsFromContext(ictx); len(got) != 0 {
		t.Fatalf("RED: an inert read noted a served cell: %+v", got)
	}
	if c.hitTotal.Load() != h0 || c.missTotal.Load() != m0 {
		t.Fatalf("RED: ServeCellIf moved hit/miss accounting: hit %d→%d miss %d→%d", h0, c.hitTotal.Load(), m0, c.missTotal.Load())
	}
}
