// issue376_keepwarm_getnotouch_test.go — #376 (third internal caller): the boot/
// keepwarm seed sweep's liveness read (seedSkipDecision) must use GetNoTouch, not the
// warmth-stamping Get, so a repeated sweep never fakes #315/#316 warmth or inflates
// the customer hit ratio — WHILE keeping the lazy maxAge/TTL evict, which is the
// LOAD-BEARING 24h bound on a warm cell the reaper never cold-evicts (pop-A self-heal).

package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// WIRING: seedSkipDecision reads via GetNoTouch and NOT Get, for BOTH the keepwarm
// sweep and the seedModeBoot liveness read (after the Lever-A short-circuit).
func TestIssue376_SeedSkipDecision_UsesGetNoTouch(t *testing.T) {
	in := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "ns", Name: "kw"}
	key := cache.ComputeKey(in)

	hKW := &getRecordingHandle{}
	if seedSkipDecision(context.Background(), seedModeKeepwarm, hKW, key, "widgets", "ns/kw", "") {
		t.Fatal("keepwarm: a MISS must not be skipped (must re-resolve)")
	}
	if !hKW.sawGetNoTouch(key) {
		t.Fatalf("WIRING #376: keepwarm seed read must use GetNoTouch; noTouchKeys=%v", hKW.noTouchKeys)
	}
	if hKW.sawGetTouch(key) {
		t.Fatal("WIRING #376: keepwarm seed read must NOT use the warmth-stamping Get")
	}

	// seedModeBoot with an empty ctx → the declined-external set is nil → not Marked →
	// the decision reaches the liveness read (now GetNoTouch).
	hBoot := &getRecordingHandle{}
	if seedSkipDecision(context.Background(), seedModeBoot, hBoot, key, "widgets", "ns/kw", "") {
		t.Fatal("boot: an unmarked MISS must not be skipped")
	}
	if !hBoot.sawGetNoTouch(key) || hBoot.sawGetTouch(key) {
		t.Fatalf("WIRING #376: seedModeBoot liveness read must be GetNoTouch; noTouch=%v touch=%v", hBoot.noTouchKeys, hBoot.getKeys)
	}
}

// LOAD-BEARING keep-evict: a WARM cell past maxEntryAge read by the keepwarm sweep's
// GetNoTouch is EVICTED (evict_max_age +1, miss_total unchanged per the symmetric
// ruling), the decision returns "not skipped" so the sweep re-resolves, and a re-Put
// with a fresh BornAt re-seats the cell (pop-A 24h self-heal). The reaper never cold-
// evicts a warm cell, so this Get-path evict is the ONLY thing bounding it at maxAge.
func TestIssue376_KeepwarmSweep_EvictsPastMaxAge_ThenReseatsFresh(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "3600")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "86400")
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000") // no reaper tick during the test
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)

	c := cache.ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled")
	}

	in := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "ns", Name: "warmstale"}
	key := cache.ComputeKey(in)
	// A WARM (SeededAtBoot) cell already past maxEntryAge (BornAt 48h ago). The reaper
	// would KEEP this (warm → not cold-evicted, counted in warm_past_max_age), so the
	// seed sweep's GetNoTouch is the only maxAge bound.
	c.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"v":"stale"}`), Inputs: &in, SeededAtBoot: true, BornAt: time.Now().Add(-48 * time.Hour), CreatedAt: time.Now()})

	evBefore := c.Stats().EvictMaxAgeTotal
	missBefore := c.Stats().MissTotal

	if seedSkipDecision(context.Background(), seedModeKeepwarm, c, key, "widgets", "ns/warmstale", "") {
		t.Fatal("#376 keep-evict: a past-maxAge cell must NOT be skipped — it must re-resolve")
	}
	if got := c.Stats().EvictMaxAgeTotal; got != evBefore+1 {
		t.Fatalf("#376 keep-evict: the keepwarm GetNoTouch must evict the past-maxAge cell (evict_max_age %d -> %d)", evBefore, got)
	}
	if got := c.Stats().MissTotal; got != missBefore {
		t.Fatalf("#376 keep-evict: the GetNoTouch evict must NOT bump miss_total (symmetric neutrality); %d -> %d", missBefore, got)
	}
	if _, ok := c.GetNoTouch(key); ok {
		t.Fatal("#376 keep-evict: the cell must be gone after the maxAge evict")
	}

	// Model the sweep's re-resolve + re-Put with a FRESH BornAt (zero → cold-fill now):
	// the cell is re-seated within the cap and no longer evicts (pop-A self-heal).
	c.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"v":"fresh"}`), Inputs: &in, SeededAtBoot: true, CreatedAt: time.Now()})
	if _, ok := c.GetNoTouch(key); !ok {
		t.Fatal("#376 keep-evict: after re-Put with a fresh BornAt the cell must be resident (not immediately re-evicted)")
	}
}

// TestIssue376_WarmGauge_KeepwarmSweepLeavesNeitherBucket is arch-1217's strengthened
// gauge arm: it drives the REAL keepwarm sweep path (seedSkipDecision → GetNoTouch) over
// a non-seeded, body-fresh-but-lastRead-cold cell and asserts the sweep leaves it in
// NEITHER warm bucket; a customer Get then moves it into warm_lastread. REDs if the sweep
// read were a touching Get (it would stamp lastRead → the cell lands in warm_lastread,
// defeating the gauge's "collapse toward ~0 on an unbrowsed cluster" purpose).
func TestIssue376_WarmGauge_KeepwarmSweepLeavesNeitherBucket(t *testing.T) {
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

	in := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "ns", Name: "sweepcold"}
	key := cache.ComputeKey(in)
	// Non-seeded, body within TTL (age-skippable) but lastRead COLD (aged past TTL):
	// Put old (lastRead=CreatedAt=-100s), ReplaceIfGen fresh (CreatedAt=-1s) inherits the
	// old lastRead. In neither warm bucket (not seeded; lastRead age 100s > TTL 20s).
	c.Put(key, &cache.ResolvedEntry{RawJSON: []byte(`{"v":"old"}`), Inputs: &in, CreatedAt: time.Now().Add(-100 * time.Second)})
	gen := c.CaptureGen(key)
	if !c.ReplaceIfGen(context.Background(), key, &cache.ResolvedEntry{RawJSON: []byte(`{"v":"fresh"}`), Inputs: &in, CreatedAt: time.Now().Add(-1 * time.Second)}, gen) {
		t.Fatal("setup: ReplaceIfGen must succeed")
	}

	// The real keepwarm sweep reads via GetNoTouch and age-skips (young body) — no stamp.
	if !seedSkipDecision(context.Background(), seedModeKeepwarm, c, key, "widgets", "ns/sweepcold", "") {
		t.Fatal("setup: a body-young cell must be keepwarm age-skipped (the sweep read happened via GetNoTouch)")
	}
	c.ReapPastMaxEntryAgeForTest()
	if s := c.Stats(); s.WarmLastRead != 0 || s.WarmSeeded != 0 {
		t.Fatalf("#376 gauge: the keepwarm sweep must leave an internal-only-read cell in NEITHER bucket; warm_seeded=%d warm_lastread=%d", s.WarmSeeded, s.WarmLastRead)
	}

	// A CUSTOMER Get stamps lastRead → the cell moves into warm_lastread.
	if _, ok := c.Get(key); !ok {
		t.Fatal("control: customer Get must hit (body within TTL)")
	}
	c.ReapPastMaxEntryAgeForTest()
	if s := c.Stats(); s.WarmLastRead != 1 || s.WarmSeeded != 0 {
		t.Fatalf("#376 gauge control: after a customer Get the cell must be in warm_lastread; warm_seeded=%d warm_lastread=%d", s.WarmSeeded, s.WarmLastRead)
	}
}
