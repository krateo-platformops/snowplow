// issue315_316_lazy_eviction_test.go — #315 (cold-evict) + #316 (proactive
// refresh) close the two LAZY-EVICTION HOLES that #248's suppressed-only reaper
// left open.
//
// The two holes, confirmed in code on the base tree:
//   - #315: reapPastMaxEntryAge (resolved.go) reaps ONLY suppressed past-
//     maxEntryAge cells (the RefreshSuppressedReason gate). A NON-suppressed
//     COLD cell (Put once, NEVER Get-read → never hits Get's lazy maxAge evict;
//     never dirty-marked → never declined → NOT suppressed) past maxEntryAge is
//     reaped by NEITHER path and lives forever.
//   - #316: TTL (CreatedAt) is enforced ONLY inside Get. A WARM cell whose
//     dirty-mark was MISSED (fresh indexer, missed enqueue — NOT a stale
//     indexer, which is #244) is never proactively re-resolved; it goes stale
//     until a Get cold-navs it at TTL.
//
// The discriminator (arch-1217 ruling): a per-residency lastRead timestamp,
// set on the Get-HIT path. WARM = (read within W) OR SeededAtBoot/keepwarm;
// COLD = neither. #315 evicts COLD past-maxEntryAge cells (no future lazy-Get,
// so no cold-nav cost); a WARM past-maxEntryAge cell is NEVER evicted (C3 —
// that would manufacture a cold navigation) and is instead kept + refreshed.
//
// These drive the PRODUCTION functions (reapPastMaxEntryAge, the summary-tick
// pass) against a real store, not the goroutine — the same discipline as
// issue248_maxage_reaper_test.go. MetadataForKey is read-only (no lazy Get
// evict, no LRU touch, no lastRead write), so a disappearance is the REAPER.

package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// TestIssue315_ColdNonSuppressedPastMaxAge_IsReaped is the #315 discriminating
// falsifier. A COLD (never Get-read, not seeded) NON-suppressed cell aged past
// maxEntryAge must be evicted by the read-independent reaper.
//
// RED on the base tree: reapPastMaxEntryAge reaps suppressed-only, so this cold
// non-suppressed cell is never evicted (it is the exact #315 hole — and the
// cell the #248 test asserts KEPT, an assertion #315 inverts).
func TestIssue315_ColdNonSuppressedPastMaxAge_IsReaped(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "60")
	// Inert summary goroutine: driven directly below, no leaked fast-ticker.
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)

	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}

	// COLD: not suppressed, never Get-read, not seeded; BornAt 2h ago (> 60s cap).
	coldKey := i248PutSuppressedOld(t, store, "cold", 2*time.Hour, false)

	beforeMaxAge := store.Stats().EvictMaxAgeTotal
	beforeDelete := store.Stats().EvictDeleteTotal

	reaped := store.reapPastMaxEntryAge()

	if _, ok := store.MetadataForKey(coldKey); ok {
		t.Fatalf("RED (#315): a COLD non-suppressed past-maxEntryAge cell was NOT reaped — " +
			"reapPastMaxEntryAge reaps suppressed-only, so a never-read, never-dirty-marked cell " +
			"past the cap lives forever (the #315 hole; the #248 test wrongly asserts this cell KEPT)")
	}
	if reaped < 1 {
		t.Fatalf("#315: reap count = %d, want >=1 (the cold cell)", reaped)
	}
	if got := store.Stats().EvictMaxAgeTotal; got != beforeMaxAge+1 {
		t.Fatalf("#315: the cold reap must count on evict_max_age (Get-lazy + reaper share it); delta=%d want 1", got-beforeMaxAge)
	}
	if got := store.Stats().EvictDeleteTotal; got != beforeDelete {
		t.Fatalf("#315: the reap must NOT move evict_delete_total (the informer-DELETE H1 discriminator); moved %d->%d", beforeDelete, got)
	}
}

// TestIssue315_GetHitStampsLastRead is the discriminator SUBSTRATE arm: a Get HIT
// stamps the read-recency (LastReadSeconds moves from -1 "never traffic-read" to
// >=0). A boot-SEED cell starts at -1 (warm via SeededAtBoot, not lastRead) until
// real traffic reads it — the clean way to observe the Get stamp in isolation (a
// cold-fill Put is warm at insert, see TestIssue315_ColdFillPut_IsImmediatelyWarm).
func TestIssue315_GetHitStampsLastRead(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "team-a", Name: "w"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in, SeededAtBoot: true})

	// Seeded, never traffic-read → LastReadSeconds == -1.
	if m, ok := c.MetadataForKey(key); !ok || m.LastReadSeconds != -1 {
		t.Fatalf("a seeded, never-traffic-read cell must report LastReadSeconds=-1; ok=%v got=%d", ok, m.LastReadSeconds)
	}
	// A Get HIT stamps it.
	if _, ok := c.Get(key); !ok {
		t.Fatal("precondition: within-TTL Get must hit")
	}
	if m, ok := c.MetadataForKey(key); !ok || m.LastReadSeconds < 0 {
		t.Fatalf("after a Get HIT, LastReadSeconds must be >=0 (read-recency stamped); ok=%v got=%d", ok, m.LastReadSeconds)
	}
}

// i316WarmApproachingTTL puts a widgets cell whose body is `ttlAgo` old (so
// TTLRemaining = ttl-ttlAgo), records its dep edge, and — when warmViaGet — makes
// it lastRead-warm with one HIT. Returns the key. Caller sets the store TTL via
// RESOLVED_CACHE_TTL_SECONDS before ResolvedCache() is built.
func i316WarmApproachingTTL(t *testing.T, c *ResolvedCacheStore, name string, ttlAgo time.Duration, warmViaGet bool) string {
	t.Helper()
	gvr := coherenceProbeGVR()
	const ns = "team-a"
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: ns, Name: name}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(coherenceV1), Inputs: &in, CreatedAt: time.Now().Add(-ttlAgo)})
	Deps().Record(context.Background(), key, gvr, ns, name)
	if warmViaGet {
		if _, ok := c.Get(key); !ok {
			t.Fatalf("precondition: %s must still be a HIT (within TTL) to warm it via Get", name)
		}
	}
	return key
}

// TestIssue316_MissedDirtyMark_WarmCellProactivelyRefreshed is the C2 falsifier:
// a WARM cell whose dirty-mark was MISSED (fresh indexer, no OnUpdate → nothing
// enqueued it), aged past TTL*3/4, is caught by the read-independent pass and
// re-resolved from the FRESH source → serves v2, stays a HIT (no cold nav).
//
// RED on the base tree: no pass exists, so the cell stays v1 until a Get cold-navs
// it at TTL; proactive_refresh_total is 0.
func TestIssue316_MissedDirtyMark_WarmCellProactivelyRefreshed(t *testing.T) {
	cleanup := withCleanRefresher(t, 4, 0)
	defer cleanup()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "20")              // TTL/4 = 5s
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "86400") // off-axis (large)
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000") // drive the pass directly
	t.Setenv(envRefresherRateFloorSeconds, "0")               // no floor delay in test
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	resetRefresherForTest()

	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	Deps().SetStore(c)

	// WARM (Get-hit), body 16s old → TTLRemaining ~4s < TTL/4 (5s): approaching TTL,
	// with a comfortable margin before the async refresh must land.
	key := i316WarmApproachingTTL(t, c, "missed", 16*time.Second, true)

	// FRESH indexer: the refresher re-resolves to v2.
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, used ResolvedKeyInputs) error {
		c.Put(k, &ResolvedEntry{RawJSON: []byte(coherenceV2), Inputs: &used})
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	// CRITICAL: NO Deps().OnUpdate — the dirty-mark is MISSED. Nothing enqueued it.
	before := c.Stats().ProactiveRefreshTotal

	// Drive the read-independent pass (the summary tick's call).
	c.reapPastMaxEntryAge()

	if got := c.Stats().ProactiveRefreshTotal; got != before+1 {
		t.Fatalf("RED (#316): the pass did not enqueue the warm approaching-TTL cell "+
			"(proactive_refresh_total %d->%d) — a missed dirty-mark then leaves it to cold-nav at TTL", before, got)
	}

	// The refresher re-resolves from the FRESH source → cell serves v2, stays HIT.
	deadline := time.Now().Add(4 * time.Second)
	fresh := false
	for time.Now().Before(deadline) {
		if e, ok := c.Get(key); ok && string(e.RawJSON) == coherenceV2 {
			fresh = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !fresh {
		t.Fatalf("#316: after the pass enqueued it, the warm cell must be re-resolved to the FRESH v2 and stay HIT (no cold nav)")
	}
}

// TestIssue316_StaleSource_PassCannotManufactureFreshness is the #244-out-of-scope
// DISCRIMINATOR: identical to the fresh case except the re-resolve reads a STALE
// source (RefreshFunc returns v1). The pass DID its job (it enqueued, the refresher
// ran) but freshness CANNOT be manufactured from a stale indexer — so the cell
// still serves v1. This keeps #316's claim honest: it fixes missed-ENQUEUE with a
// fresh indexer, NOT a stale indexer (that is #244's self-heal domain).
func TestIssue316_StaleSource_PassCannotManufactureFreshness(t *testing.T) {
	cleanup := withCleanRefresher(t, 4, 0)
	defer cleanup()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "20")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "86400")
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	t.Setenv(envRefresherRateFloorSeconds, "0")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	resetRefresherForTest()

	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	Deps().SetStore(c)

	key := i316WarmApproachingTTL(t, c, "stale", 16*time.Second, true)

	var reresolves atomic.Int32
	// STALE indexer: the re-resolve reads the same stale v1 (models a lagging
	// informer cache — #244, not #316).
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, used ResolvedKeyInputs) error {
		reresolves.Add(1)
		c.Put(k, &ResolvedEntry{RawJSON: []byte(coherenceV1), Inputs: &used})
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	before := c.Stats().ProactiveRefreshTotal
	c.reapPastMaxEntryAge()

	// The pass DID enqueue (it does its job regardless of source freshness).
	if got := c.Stats().ProactiveRefreshTotal; got != before+1 {
		t.Fatalf("#316: the pass must still enqueue the warm approaching-TTL cell (%d->%d)", before, got)
	}
	// Wait for the re-resolve to actually run.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && reresolves.Load() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if reresolves.Load() == 0 {
		t.Fatalf("#316: the enqueued cell was never re-resolved — the refresher did not run the handler")
	}
	// ...yet the cell still serves v1 (a stale source cannot be made fresh here).
	if e, ok := c.Get(key); !ok || string(e.RawJSON) != coherenceV1 {
		t.Fatalf("#316 DISCRIMINATOR: with a STALE source the cell must still serve v1 — #316 does NOT own the "+
			"stale-indexer case (#244). ok=%v body=%s", ok, func() string {
			if e != nil {
				return string(e.RawJSON)
			}
			return ""
		}())
	}
}

// TestIssue316_NoAmplification_WithinTTLWarmNotEnqueued is the positive control:
// a WARM cell comfortably WITHIN its TTL (TTLRemaining > TTL/4) is NOT enqueued by
// the pass — refresh is scoped to the approaching-TTL warm set, never
// refresh-everything (the 0.30.185 amplification the floor exists to prevent).
func TestIssue316_NoAmplification_WithinTTLWarmNotEnqueued(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "20") // TTL/4 = 5s
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "86400")
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)

	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	Deps().SetStore(c)

	// WARM (Get-hit) but body only 2s old → TTLRemaining ~18s > TTL/4 (5s): NOT
	// approaching TTL, so NOT a refresh candidate.
	_ = i316WarmApproachingTTL(t, c, "young-warm", 2*time.Second, true)
	// COLD: last read 100s ago (> W=TTL 20s) and never re-read — NOT in the warm
	// set. Its body is past TTL (TTLRemaining=0, well under TTL/4) so it WOULD be
	// a refresh candidate if warm, which makes this a sharp cold-vs-warm control:
	// it must NOT be enqueued (cold), and within maxAge it is NOT cold-evicted.
	_ = i316WarmApproachingTTL(t, c, "cold-stale", 100*time.Second, false)

	before := c.Stats().ProactiveRefreshTotal
	c.reapPastMaxEntryAge()
	if got := c.Stats().ProactiveRefreshTotal; got != before {
		t.Fatalf("#316 no-amplification: neither a within-TTL warm cell nor a COLD stale cell "+
			"may be enqueued; proactive_refresh_total moved %d->%d", before, got)
	}
}

// TestIssue316_TTLOverrideCell_NotProactivelyRefreshed (arch-1217 addition): a
// WARM cell approaching its EFFECTIVE TTL but carrying a TTLOverride>0 (the short
// bounded-staleness stopgap — UAF #118-d / degraded #36 R1-L2) must NOT be
// proactively refreshed. Refreshing it resets CreatedAt and extends the very life
// the override deliberately bounds. Left to its own short-TTL expiry.
func TestIssue316_TTLOverrideCell_NotProactivelyRefreshed(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_TTL_SECONDS", "20") // standard TTL/4 = 5s
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "86400")
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)

	c := ResolvedCache()
	if c == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	Deps().SetStore(c)

	gvr := coherenceProbeGVR()
	const ns = "team-a"
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: ns, Name: "override"}
	key := ComputeKey(in)
	// TTLOverride=8s, body 7s old → EFFECTIVE TTLRemaining ~1s (< standard TTL/4=5),
	// so it WOULD be a refresh candidate if not for the TTLOverride exclusion.
	c.Put(key, &ResolvedEntry{
		RawJSON:     []byte(coherenceV1),
		Inputs:      &in,
		CreatedAt:   time.Now().Add(-7 * time.Second),
		TTLOverride: 8 * time.Second,
	})
	Deps().Record(context.Background(), key, gvr, ns, "override")
	if _, ok := c.Get(key); !ok {
		t.Fatal("precondition: the override cell (1s left on its 8s effective TTL) must HIT → warm")
	}

	before := c.Stats().ProactiveRefreshTotal
	c.reapPastMaxEntryAge()
	if got := c.Stats().ProactiveRefreshTotal; got != before {
		t.Fatalf("#316: a TTLOverride>0 cell must NOT be proactively refreshed (extending its "+
			"deliberate short bounded-staleness stopgap); proactive_refresh_total moved %d->%d", before, got)
	}
}

// TestIssue315_ColdFillPut_IsImmediatelyWarm (arch-1217 addition): a cold-fill
// (non-seeded fresh Put — a customer Get-MISS → populate → serve) is warm IMMEDIATELY
// (lastRead stamped at the fill), not only after a 2nd read. Without this a
// genuinely-used cell would be treated COLD for one cycle and cold-nav at TTL. A
// boot-seed Put, by contrast, leaves lastRead unset (warm via the seed signal).
func TestIssue315_ColdFillPut_IsImmediatelyWarm(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)

	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "team-a", Name: "coldfill"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &in}) // cold-fill (SeededAtBoot=false)
	if m, ok := c.MetadataForKey(key); !ok || m.LastReadSeconds < 0 {
		t.Fatalf("a cold-fill (non-seeded) Put must be immediately warm (LastReadSeconds>=0, no 2nd read); ok=%v got=%d", ok, m.LastReadSeconds)
	}

	sin := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "team-a", Name: "seed"}
	skey := ComputeKey(sin)
	c.Put(skey, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &sin, SeededAtBoot: true})
	if m, ok := c.MetadataForKey(skey); !ok || m.LastReadSeconds != -1 {
		t.Fatalf("a boot-seed Put must leave LastReadSeconds=-1 (warm via SeededAtBoot, not lastRead); ok=%v got=%d", ok, m.LastReadSeconds)
	}
}

// TestIssue315_LastReadWarmPastMaxAge_NotEvicted (arch-1217 #3 coverage) exercises
// the LIVE, common path the (false) "past-maxAge ⟹ seed-only-warm" invariant would
// mask: a cell that is past maxEntryAge AND lastRead-WARM (not seeded). In
// production this is any cell Get-read shortly before it crossed maxAge and not
// re-read since — resident past the cap carrying a lastRead still within TTL (the
// ~one-TTL [crossing, crossing+TTL] window). Constructed directly here: BornAt
// long-past (past the cap) but a fresh non-seeded Put stamps lastRead=CreatedAt=now
// (the cold-fill rule) → warm via the lastRead branch ALONE (SeededAtBoot=false).
// It must be COUNTED in warm_past_max_age (C4) and NEVER cold-evicted (C3) — proving
// the reaper's warm check does not rely on the false seed-only invariant.
func TestIssue315_LastReadWarmPastMaxAge_NotEvicted(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "60")
	// Standard TTL left at its default (3600s) → W=3600s, so a lastRead stamped
	// ~now is comfortably within TTL = warm, while BornAt 2h ago is past the 60s cap.
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)

	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}

	const ns = "krateo-system"
	in := widgetInputs(gvrFlexes(), ns, "lastread-warm")
	key := ComputeKey(*in)
	store.Put(key, &ResolvedEntry{
		RawJSON:   []byte(`{"lrw":1}`),
		Inputs:    in,
		BornAt:    time.Now().Add(-2 * time.Hour), // past the 60s cap
		CreatedAt: time.Now(),                     // fresh body → cold-fill stamps lastRead=now
		// SeededAtBoot: false — warmth MUST come from lastRead, not seed.
	})

	// Precondition: warm via lastRead ONLY (not seed), and past the cap.
	m, ok := store.MetadataForKey(key)
	if !ok {
		t.Fatal("precondition: cell must be resident after Put")
	}
	if m.SeededAtBoot {
		t.Fatal("precondition: cell must NOT be seeded — this arm exercises the lastRead warm branch")
	}
	if m.LastReadSeconds < 0 || m.LastReadSeconds >= 3600 {
		t.Fatalf("precondition: cell must be lastRead-warm (0 <= LastReadSeconds < TTL 3600); got %d", m.LastReadSeconds)
	}
	if m.LifetimeSeconds <= 60 {
		t.Fatalf("precondition: cell must be past the 60s maxEntryAge cap; LifetimeSeconds=%d", m.LifetimeSeconds)
	}

	beforeMaxAge := store.Stats().EvictMaxAgeTotal
	reaped := store.reapPastMaxEntryAge()

	if reaped != 0 {
		t.Fatalf("#315 C3: a lastRead-warm past-maxEntryAge cell must NOT be reaped; reaped=%d", reaped)
	}
	if _, ok := store.MetadataForKey(key); !ok {
		t.Fatal("#315 C3: a lastRead-warm (non-seeded) past-maxEntryAge cell was cold-evicted — " +
			"the reaper wrongly treated a served cell as cold (the false seed-only invariant); " +
			"this manufactures a cold navigation")
	}
	if got := store.Stats().EvictMaxAgeTotal; got != beforeMaxAge {
		t.Fatalf("#315 C3: evict_max_age moved (%d->%d) — a warm cell was evicted", beforeMaxAge, got)
	}
	if got := store.Stats().WarmPastMaxAge; got != 1 {
		t.Fatalf("#315 C4: warm_past_max_age = %d, want 1 (the lastRead-warm past-cap cell must be counted, not just seeded ones)", got)
	}
}
