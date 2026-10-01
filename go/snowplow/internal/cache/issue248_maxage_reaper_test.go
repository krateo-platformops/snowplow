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
	"context"
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
	Deps().Record(context.Background(), key, gvr, ns, name)
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
// exact function the summary tick calls) against a real store.
//
// #315 INVERTED this test's original R2 rule. The old rule was "NOT suppressed +
// past maxEntryAge → NOT reaped (a served cell hits the lazy Get evict; reaping
// here would manufacture a cold nav)". That rationale SILENTLY conflated
// not-suppressed ≡ SERVED ≡ warm. A not-suppressed cell that is NEVER served
// (never Get-read → never hits the lazy maxAge evict) is exactly the #315 hole:
// it freezes forever. #315 splits the not-suppressed case by read-recency:
//   - suppressed + past maxEntryAge     → REAPED (#248 frozen class), evict_max_age
//   - suppressed + YOUNG                → NOT reaped (never over-evict within-cap)
//   - NOT-suppressed COLD + past cap    → REAPED (#315 — never served, no future
//     lazy Get, no cold-nav cost). THIS INVERTS the old assertion.
//   - NOT-suppressed WARM + past cap    → NOT reaped (C3 — a warm cell IS served;
//     evicting it manufactures the cold nav; it is kept + refreshed by #316) and
//     counted into the warm-past-maxAge AT-RISK gauge (#315 C4).
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
	frozenKey := i248PutSuppressedOld(t, store, "frozen", 2*time.Hour, true) // suppressed + old   → reaped (#248)
	youngKey := i248PutSuppressedOld(t, store, "young", 1*time.Second, true) // suppressed + young → kept
	coldKey := i248PutSuppressedOld(t, store, "cold", 2*time.Hour, false)    // NOT-suppressed COLD + old → reaped (#315)

	// NOT-suppressed WARM + old → KEPT (C3). Warm via SeededAtBoot; body FRESH
	// (CreatedAt=now) so it is past the maxAge cap on the KEY (BornAt 2h) without
	// being a #316 approaching-TTL refresh candidate — isolating the eviction axis.
	warmInputs := widgetInputs(gvrFlexes(), ns, "warmseed")
	warmKey := ComputeKey(*warmInputs)
	store.Put(warmKey, &ResolvedEntry{
		RawJSON:      []byte(`{"warm":"seed"}`),
		Inputs:       warmInputs,
		BornAt:       time.Now().Add(-2 * time.Hour),
		CreatedAt:    time.Now(),
		SeededAtBoot: true,
	})
	if _, ok := store.MetadataForKey(warmKey); !ok {
		t.Fatalf("precondition: warmseed must be resident after Put")
	}

	beforeMaxAge := store.Stats().EvictMaxAgeTotal
	beforeDelete := store.Stats().EvictDeleteTotal

	reaped := store.reapPastMaxEntryAge()

	// --- both past-cap non-warm classes are reaped: #248 frozen + #315 cold ---
	if reaped != 2 {
		t.Fatalf("#248+#315: exactly TWO cells must be reaped (suppressed-frozen + cold-non-suppressed), got %d", reaped)
	}
	if _, ok := store.MetadataForKey(frozenKey); ok {
		t.Fatalf("#248: the suppressed past-maxEntryAge cell must be reaped")
	}
	if _, ok := store.MetadataForKey(coldKey); ok {
		t.Fatalf("#315: a COLD (never-read, not-seeded) NOT-suppressed past-maxEntryAge cell must be reaped — " +
			"this INVERTS the old R2 assertion that wrongly kept it (it is never served, so no lazy Get ever evicts it)")
	}
	if got := store.Stats().EvictMaxAgeTotal; got != beforeMaxAge+2 {
		t.Fatalf("#248+#315: both reaps must count on evict_max_age (Get-lazy + reaper share it); delta=%d want 2", got-beforeMaxAge)
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
	if _, ok := store.MetadataForKey(warmKey); !ok {
		t.Fatalf("#315 C3 discriminator: a WARM (seeded) NOT-suppressed past-maxEntryAge cell must NOT be reaped — " +
			"evicting a served cell manufactures a cold nav; it is kept and refreshed by #316")
	}

	// --- gauges (during-defect detectors) ---
	// A second reap — after `frozen` and `cold` are gone — sees the steady state:
	// `young` is still suppressed+resident (suppressed_resident=1); `warmseed` is
	// warm+past-cap+resident (warm_past_max_age=1) and is NEVER evicted.
	if reaped2 := store.reapPastMaxEntryAge(); reaped2 != 0 {
		t.Fatalf("#248/#315: a second reap must evict nothing (young within-cap, warmseed is warm→C3); reaped=%d", reaped2)
	}
	if got := store.Stats().SuppressedResident; got != 1 {
		t.Fatalf("#248 gauge: suppressed_resident = %d, want 1 (the young suppressed cell; frozen is gone, the non-suppressed cells do not count)", got)
	}
	if got := store.Stats().WarmPastMaxAge; got != 1 {
		t.Fatalf("#315 C4 gauge: warm_past_max_age = %d, want 1 (warmseed: warm + past cap, kept by C3)", got)
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
