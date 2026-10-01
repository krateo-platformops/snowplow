// issue376_debug_peek_test.go — #376: a /debug inspection must be a PURE PEEK. Before
// #376 the debug handlers read the resolved cache via the warmth-stamping Get, so an
// operator looking at a cell stamped its lastRead (faking #316 warmth), bumped hit_total
// and moved it in the LRU — observing changed the system. depsKeyViewFor now uses
// GetNoTouch; this arm proves the peek is hit- AND lastRead-neutral.

package handlers

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue376_DebugDepsKeyView_IsPurePeek(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "20")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "86400")
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	c := cache.ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled")
	}

	in := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "ns", Name: "debugpeek"}
	key := cache.ComputeKey(in)
	// Non-seeded, body within TTL but lastRead COLD (aged past TTL): Put old then
	// ReplaceIfGen fresh inherits the old lastRead — so the cell is in NEITHER warm
	// bucket unless something stamps its lastRead.
	c.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"v":"old"}`), Inputs: &in, CreatedAt: time.Now().Add(-100 * time.Second)})
	gen := c.CaptureGen(key)
	if !c.ReplaceIfGen(key, &cache.ResolvedEntry{RawJSON: []byte(`{"v":"fresh"}`), Inputs: &in, CreatedAt: time.Now().Add(-1 * time.Second)}, gen) {
		t.Fatal("setup: ReplaceIfGen must succeed")
	}

	h0, m0 := c.Stats().HitTotal, c.Stats().MissTotal
	_ = depsKeyViewFor(key) // the /debug ?key view — reads the cell via GetNoTouch
	if s := c.Stats(); s.HitTotal != h0 || s.MissTotal != m0 {
		t.Fatalf("#376 debug peek: a /debug read must not move hit/miss (hit %d->%d miss %d->%d)", h0, s.HitTotal, m0, s.MissTotal)
	}
	c.ReapPastMaxEntryAgeForTest()
	if got := c.Stats().WarmLastRead; got != 0 {
		t.Fatalf("#376 debug peek: a /debug read must not stamp lastRead (warm_lastread stayed %d, want 0)", got)
	}

	// Control: a CUSTOMER Get DOES stamp lastRead → the cell enters warm_lastread.
	if _, ok := c.Get(key); !ok {
		t.Fatal("control: customer Get must hit (body within TTL)")
	}
	c.ReapPastMaxEntryAgeForTest()
	if got := c.Stats().WarmLastRead; got != 1 {
		t.Fatalf("#376 debug peek control: a customer Get must stamp lastRead (warm_lastread=%d, want 1)", got)
	}
}
