// issue248_maxage_reaper_test.go — #248: a UAF-declined refresh permanently
// suppresses a cell on the first decline (refresher skips it forever), and BOTH
// TTL and maxEntryAge are enforced ONLY inside Get — so a suppressed cell that
// nobody reads is evicted by neither and freezes forever (~143 widgets cells @
// ~19h on 057). The fix is a read-independent maxEntryAge reaper on the
// startResolvedCacheSummary tick that evicts suppressed-and-past-maxEntryAge cells
// (R2 scope) through the same internal the lazy Get maxAge path uses (evict_max_age,
// NOT evict_delete), and exposes a resident-suppressed gauge (during-defect detector).
//
// This drives reapPastMaxEntryAge — the EXACT function the summary tick invokes
// (resolved.go, startResolvedCacheSummary loop) — against a real store, rather than
// the 1s goroutine itself: the summary goroutine has no stop, so driving it at a
// fast cadence leaks a goroutine whose per-tick global-stats reads race the -race
// suite. The tick→reap wiring is a one-line call at the ticker loop.

package cache

import (
	"testing"
	"time"
)

// i248PutSuppressedOld puts a resident widgets-class entry whose BornAt is `age`
// in the past, records its self dep edge, and (when suppress) marks it
// refresh-suppressed via the REAL NoteRefreshDecline UAF path. Returns the key.
func i248PutSuppressedOld(t *testing.T, store *ResolvedCacheStore, name string, age time.Duration, suppress bool) string {
	t.Helper()
	gvr := gvrFlexes()
	const ns = "krateo-system"
	inputs := widgetInputs(gvr, ns, name)
	key := ComputeKey(*inputs)
	born := time.Now().Add(-age)
	store.Put(key, &ResolvedEntry{
		RawJSON:   []byte(`{"frozen":"` + name + `"}`),
		Inputs:    inputs,
		BornAt:    born,
		CreatedAt: born,
	})
	Deps().Record(key, gvr, ns, name)
	if suppress {
		// The real UAF decline: permanent-suppress on the first occurrence
		// (resolve_populate.go:391 → NoteRefreshDecline(key,"uaf",true)).
		NoteRefreshDecline(key, "uaf", true)
		if _, ok := RefreshSuppressedReason(key); !ok {
			t.Fatalf("precondition: %s must be suppressed after NoteRefreshDecline", name)
		}
	}
	if _, ok := store.MetadataForKey(key); !ok {
		t.Fatalf("precondition: %s must be resident after Put", name)
	}
	return key
}

// TestIssue248_MaxAgeReaper drives the production reap (reapPastMaxEntryAge, the
// exact function the summary tick calls) against a real store. R2 scope:
//   - suppressed + past maxEntryAge   → REAPED via evict_max_age (NOT evict_delete),
//     dep edges stripped (the frozen class — RED on unfixed main: never evicted)
//   - suppressed + YOUNG              → NOT reaped (never over-evict a within-cap cell)
//   - NOT suppressed + past maxEntryAge → NOT reaped (a served cell hits the lazy Get
//     evict on its own reads; reaping it here would manufacture a cold navigation)
//   - the resident-suppressed GAUGE reads the live suppressed-resident count.
//
// MetadataForKey is read-only (no lazy Get evict, no LRU touch, no counter), so a
// disappearance is the REAPER, not a read-triggered eviction.
func TestIssue248_MaxAgeReaper(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "60")
	// Large summary cadence: the goroutine startResolvedCacheSummary launches is
	// inert for this test (it never ticks), so no leaked fast-ticker races the
	// -race suite; the reap is driven directly below.
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "36000")
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)

	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	// ResolvedCache() wires Deps().SetStore.

	gvr := gvrFlexes()
	const ns = "krateo-system"
	frozenKey := i248PutSuppressedOld(t, store, "frozen", 2*time.Hour, true) // suppressed + old  → reaped
	youngKey := i248PutSuppressedOld(t, store, "young", 1*time.Second, true) // suppressed + young → kept
	unsupKey := i248PutSuppressedOld(t, store, "unsup", 2*time.Hour, false)  // old + NOT suppressed → kept (R2)

	beforeMaxAge := store.Stats().EvictMaxAgeTotal
	beforeDelete := store.Stats().EvictDeleteTotal

	reaped := store.reapPastMaxEntryAge()

	// --- the frozen class is reaped ---
	if reaped != 1 {
		t.Fatalf("RED (#248): exactly ONE cell (suppressed+old) must be reaped, got %d — on unfixed main "+
			"there is no read-independent reaper, so a never-read suppressed cell freezes forever (~143@19h)", reaped)
	}
	if _, ok := store.MetadataForKey(frozenKey); ok {
		t.Fatalf("#248: the suppressed past-maxEntryAge cell must be reaped")
	}
	if got := store.Stats().EvictMaxAgeTotal; got != beforeMaxAge+1 {
		t.Fatalf("#248: the reap must count on evict_max_age (Get-lazy + reaper share it); delta=%d want 1", got-beforeMaxAge)
	}
	if got := store.Stats().EvictDeleteTotal; got != beforeDelete {
		t.Fatalf("#248: the reap must NOT move evict_delete_total (the informer-DELETE H1 live discriminator); moved %d->%d", beforeDelete, got)
	}
	if n := len(Deps().CollectMatchesForTest(gvr, ns, "frozen")); n != 0 {
		t.Fatalf("#248: %d dep edge(s) survived the reap — removeElementLocked must strip them alongside the store delete", n)
	}

	// --- discriminators: the reaper does not over-evict ---
	if _, ok := store.MetadataForKey(youngKey); !ok {
		t.Fatalf("#248 discriminator: a suppressed but YOUNG cell must NOT be reaped (never over-evict a within-cap cell)")
	}
	if _, ok := store.MetadataForKey(unsupKey); !ok {
		t.Fatalf("#248 R2 discriminator: an old but NOT-suppressed cell must NOT be reaped (a served cell hits the lazy Get evict; reaping here would manufacture a cold nav)")
	}

	// --- gauge (during-defect detector) ---
	// A second reap walk — after `frozen` is gone — sees the steady state: `young`
	// is still suppressed+resident (1); `unsup` is resident but NOT suppressed (0).
	if reaped2 := store.reapPastMaxEntryAge(); reaped2 != 0 {
		t.Fatalf("#248: a second reap must evict nothing (young is within-cap, unsup is unsuppressed); reaped=%d", reaped2)
	}
	if got := store.Stats().SuppressedResident; got != 1 {
		t.Fatalf("#248 gauge: suppressed_resident = %d, want 1 (the young suppressed cell; the reaped one is gone, the unsuppressed one does not count)", got)
	}
}

// TestIssue248_MaxAgeReaper_WiredIntoSummaryTick is the minimal WIRING arm
// (freshness-audit): it proves the reap is actually HOSTED in the
// startResolvedCacheSummary `case <-t.C:` arm. The logic test above would stay
// GREEN even if the reap were correct-but-never-invoked-by-the-ticker (dead code —
// the seam-is-a-floor gap); this closes it. It drives the REAL post-#206 stoppable
// ticker at a 1s cadence: start → let a tick fire (poll the read-only
// MetadataForKey until the frozen cell is reaped) → stop+join. The stop is observed
// by the NEXT tick, after the current bounded/batched walk returns, so there is no
// mid-walk preemption. Behavioral only (did the tick reap?), NOT a predicate re-test.
func TestIssue248_MaxAgeReaper_WiredIntoSummaryTick(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "60")
	t.Setenv("RESOLVED_CACHE_SUMMARY_EVERY_SECONDS", "1") // fast real tick
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest) // #206: stops + joins the summary goroutine (no leak)

	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	// ResolvedCache() starts the (now stoppable, #206) 1s summary goroutine.

	key := i248PutSuppressedOld(t, store, "wired", 2*time.Hour, true)
	before := store.Stats().EvictMaxAgeTotal

	deadline := time.Now().Add(12 * time.Second)
	reaped := false
	for time.Now().Before(deadline) {
		if _, ok := store.MetadataForKey(key); !ok {
			reaped = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// stop+join BEFORE asserting so no further tick mutates the counters mid-assert
	// (start → tick → stop → join). Idempotent with the t.Cleanup reset above.
	stopResolvedCacheSummaryForTest()

	if !reaped {
		t.Fatalf("WIRING (#248): a suppressed past-maxEntryAge cell survived ~12s of real 1s summary ticks — " +
			"the reap is NOT hosted in the startResolvedCacheSummary case <-t.C: arm (the logic can be correct " +
			"while the reap is never invoked by the ticker — seam-is-a-floor).")
	}
	if got := store.Stats().EvictMaxAgeTotal; got <= before {
		t.Fatalf("WIRING (#248): the tick reap did not move evict_max_age (%d -> %d)", before, got)
	}
}
