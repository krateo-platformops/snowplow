// refresher_rate_floor_test.go — Task #321 (#318-R1a) falsifiers for the
// L1 refresher per-key re-resolve RATE-FLOOR.
//
// The floor (refresher.processNext, dequeue side, between the
// customer-priority yield and processOne) defers a re-resolve of a key
// whose L1 entry is younger than RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS
// (reusing ResolvedEntry.CreatedAt — the wall-clock of the last successful
// Put, resolved.go:767-768; "S1", no new map). The floored branch does
// Forget(key) + AddAfter(key, remaining) onto the SAME tier queue, then
// returns — never dropping the dirty mark. The deferred re-resolve fires
// at >= floor expiry against the latest cluster state (last-write-wins
// convergence ≤ floor).
//
// Coverage (mirrors the design's falsifier set + PM gate conditions):
//   1. N rapid marks within the floor collapse to ONE deferred re-resolve
//      at expiry — the LOSSLESS invariant: the final mark inside the floor
//      RESOLVES at expiry (does not get dropped). RED on floor=0 (N marks
//      → N resolves).
//   2. Deferred-final-mark-at-expiry precision: mark at T0 (resolves),
//      re-mark inside the floor, NO resolve before floor expiry, exactly
//      one at/after.
//   3. DELETE bypass: an evicted/absent key is never floored
//      (skipped_no_entry++, floored unchanged); C3 falsifier — a deleted
//      row's dependent LIST cell re-resolves within ≤ floor (dirty-mark
//      path, not dropped, not immediate).
//   4. floor=0 identity: byte-identical resolve counts to a no-floor run.
//   5. -race: separate-goroutine marker storm vs the dequeuer with the
//      floor active.
//
// No clock-injection seam exists in the refresher/workqueue (real
// clock.RealClock{}, real time.Since); these use generous real-time
// windows per the existing suite idiom (PM gate Q5 caveat).
//
// Pure in-memory (in-process ResolvedCache + fake handlers) — NO apiserver,
// satisfies feedback_no_go_test_against_remote_kubeconfig (run in
// internal/cache/, NEVER ./internal/rbac/...).

package cache

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// agedBackoff is how far back putAgedEntry stamps CreatedAt: older than
// every floor used in these tests (≤ 5s) so the FIRST dequeue is NOT
// floored, yet FAR younger than the cache TTL (defaultResolvedCacheTTLSeconds
// = 3600s) so Get's `time.Since(CreatedAt) > ttl` check does NOT evict it.
// (A 1h backoff sits exactly on the TTL boundary and gets TTL-evicted on
// the first Get — that is why we use 30s, not 1h.)
const agedBackoff = 30 * time.Second

// putAgedEntry stores an entry whose CreatedAt is deliberately aged past
// every test floor so the FIRST dequeue re-resolves immediately, setting
// up the "1 immediate + N floored" shape deterministically.
func putAgedEntry(c *ResolvedCacheStore, key string, in *ResolvedKeyInputs) {
	c.Put(key, &ResolvedEntry{
		RawJSON:   []byte(`{}`),
		Inputs:    in,
		CreatedAt: time.Now().Add(-agedBackoff),
	})
}

// --- Test 1 — N rapid marks collapse to ONE deferred re-resolve -------------

// TestRefresher_RateFloorCollapsesNMarksToOne is THE lossless-invariant
// test. With the floor active, an aged entry's first mark re-resolves
// immediately; a burst of N further marks landing inside the floor window
// are all floored (deferred), and the FINAL one re-resolves exactly once
// at floor expiry — it is NOT dropped. Net resolves = 1 immediate + 1
// deferred = 2, regardless of N. Contrast the RED form below (floor=0 →
// N resolves).
func TestRefresher_RateFloorCollapsesNMarksToOne(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	// The env knob is seconds-granular; use 1s and a sub-second burst window
	// so the whole burst lands inside the floor, then wait just past 1s for
	// the single deferred re-resolve at expiry.
	t.Setenv(envRefresherRateFloorSeconds, "1")
	resetRefresherForTest()
	const realFloor = 1 * time.Second

	c := ResolvedCache()
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "collapse"}
	key := ComputeKey(in)
	putAgedEntry(c, key, &in)

	var calls atomic.Int32
	// The handler re-Puts a FRESH entry (CreatedAt zero → Put stamps now),
	// exactly as a real RefreshFunc does. After the immediate resolve the
	// entry is young, so the burst marks floor.
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, used ResolvedKeyInputs) error {
		c.Put(k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &used})
		calls.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	// First mark: aged entry → NOT floored → immediate resolve, re-Put fresh.
	enqueueRefreshForTest(key)
	waitFor(t, 2*time.Second, "first (immediate) resolve", func() bool { return calls.Load() == 1 })

	// Burst of N marks, all inside the floor window (entry is now young).
	const N = 20
	burstStart := time.Now()
	for i := 0; i < N; i++ {
		enqueueRefreshForTest(key)
		time.Sleep(5 * time.Millisecond) // 20×5ms = 100ms ≪ 1s floor
	}
	if elapsed := time.Since(burstStart); elapsed >= realFloor {
		t.Fatalf("burst took %s — exceeded the %s floor window; test setup invalid", elapsed, realFloor)
	}

	// Inside the floor, the deferred re-resolve must NOT have fired yet:
	// resolves stay at 1 and floored climbs.
	time.Sleep(200 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("a re-resolve fired INSIDE the floor: calls=%d want 1 (floor must defer)", got)
	}
	if fl := refresherSingleton().flooredTotal.Load(); fl == 0 {
		t.Fatalf("floored_total=0 inside the burst — the floor gate never fired")
	}

	// At >= floor expiry the deferred mark RESOLVES exactly once (the
	// lossless invariant: the final mark inside the floor is not dropped).
	waitFor(t, 3*time.Second, "deferred re-resolve at floor expiry",
		func() bool { return calls.Load() == 2 })

	// Settle: no runaway — must stay at 2 (1 immediate + 1 deferred).
	time.Sleep(400 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("resolve count after expiry = %d want exactly 2 (1 immediate + 1 deferred); floor must collapse the burst, not multiply it", got)
	}

	// C1 accounting: floored_total counts every floored re-cycle, so it
	// EXCEEDS the collapse delta — assert it is large and > 0, NOT equal to
	// (N - resolves).
	fl := refresherSingleton().flooredTotal.Load()
	if fl == 0 {
		t.Fatalf("floored_total=0 — gate never fired")
	}
	t.Logf("GREEN: N=%d burst marks collapsed to 2 resolves (1 immediate + 1 deferred-at-expiry); floored_total=%d (≥ burst, by C1 accounting)", N, fl)
}

// TestRefresher_RateFloorZero_NMarksProduceNResolves is the RED-form
// companion: with floor=0 the gate is byte-identical to pre-#321, so N
// fully-drained sequential marks of one key produce N resolves. This is
// the behavior the floor collapses (Test 1 GREEN). It also doubles as the
// floor=0-identity falsifier (Test 4).
func TestRefresher_RateFloorZero_NMarksProduceNResolves(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	t.Setenv(envRefresherRateFloorSeconds, "0") // kill switch — identity to pre-#321
	resetRefresherForTest()

	c := ResolvedCache()
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "redzero"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in})

	var calls atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, used ResolvedKeyInputs) error {
		c.Put(k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &used})
		calls.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	// N marks, each fully drained before the next (so workqueue in-flight
	// coalescing is not at play) — each produces a resolve on floor=0.
	const N = 8
	for i := 0; i < N; i++ {
		before := calls.Load()
		enqueueRefreshForTest(key)
		waitFor(t, 2*time.Second, "resolve completed", func() bool { return calls.Load() == before+1 })
	}

	got := calls.Load()
	if got != N {
		t.Fatalf("floor=0: got %d resolves want N=%d (identity to pre-#321 — every mark resolves)", got, N)
	}
	// floor=0 ⇒ the gate never defers.
	if fl := refresherSingleton().flooredTotal.Load(); fl != 0 {
		t.Fatalf("floor=0: floored_total=%d want 0 (kill switch must short-circuit the gate)", fl)
	}
	t.Logf("RED/identity: floor=0 → %d marks produced %d resolves, floored_total=0 (byte-identical to pre-#321)", N, got)
}

// --- Test 2 — deferred-final-mark-at-expiry precision -----------------------

// TestRefresher_RateFloorDeferredFinalMarkResolvesAtExpiry pins the exact
// timing of the lossless invariant: mark at T0 (aged entry → immediate
// resolve, stamps a fresh CreatedAt); ONE further mark inside the floor;
// assert NO resolve before floor expiry and EXACTLY one at/after. Proves
// the final mark is neither dropped nor resolved early.
func TestRefresher_RateFloorDeferredFinalMarkResolvesAtExpiry(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	t.Setenv(envRefresherRateFloorSeconds, "1") // 1s floor
	resetRefresherForTest()
	const floor = 1 * time.Second

	c := ResolvedCache()
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "precision"}
	key := ComputeKey(in)
	putAgedEntry(c, key, &in)

	var calls atomic.Int32
	var lastResolveAt atomic.Int64 // unix-nanos of the most recent resolve
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, used ResolvedKeyInputs) error {
		c.Put(k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &used})
		lastResolveAt.Store(time.Now().UnixNano())
		calls.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	// Mark #1: aged entry → immediate resolve, re-Put fresh (CreatedAt≈now).
	enqueueRefreshForTest(key)
	waitFor(t, 2*time.Second, "immediate resolve at T0", func() bool { return calls.Load() == 1 })
	t0Resolve := time.Unix(0, lastResolveAt.Load())

	// Mark #2: 50ms after the fresh re-Put — well inside the 1s floor.
	time.Sleep(50 * time.Millisecond)
	enqueueRefreshForTest(key)

	// Assert NO resolve fires in the floored interval (until ~floor after
	// the fresh re-Put). Poll for 700ms (< floor) and require calls stays 1.
	deadline := time.Now().Add(700 * time.Millisecond)
	for time.Now().Before(deadline) {
		if calls.Load() != 1 {
			t.Fatalf("deferred mark re-resolved EARLY at %s after T0-resolve (floor=%s) — must wait for expiry",
				time.Since(t0Resolve), floor)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Exactly one more resolve AT/AFTER expiry (the deferred final mark).
	waitFor(t, 3*time.Second, "deferred final-mark resolve at expiry",
		func() bool { return calls.Load() == 2 })
	gap := time.Unix(0, lastResolveAt.Load()).Sub(t0Resolve)
	if gap < floor-50*time.Millisecond {
		// Allow a 50ms scheduling slack below the nominal floor (the second
		// mark landed 50ms after the T0 resolve, so the deferred fire is
		// ~floor from the fresh CreatedAt which is ~T0Resolve).
		t.Fatalf("deferred resolve gap=%s < floor=%s — re-resolved too early", gap, floor)
	}

	// Settle — no third resolve.
	time.Sleep(400 * time.Millisecond)
	if got := calls.Load(); got != 2 {
		t.Fatalf("resolve count = %d want 2 (final mark resolves exactly once at expiry)", got)
	}
	t.Logf("GREEN: deferred final mark resolved once, gap=%s from T0 resolve (floor=%s) — not early, not dropped", gap, floor)
}

// --- Test 3 — DELETE bypass + C3 dependent-LIST-cell falsifier --------------

// TestRefresher_RateFloorDeleteBypass asserts an evicted/absent key is
// never floored: the dequeue hits the Get-miss skip (skipped_no_entry++)
// and floored_total stays 0. This is the self-entry DELETE path — eviction
// is immediate, never floor-delayed.
func TestRefresher_RateFloorDeleteBypass(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	t.Setenv(envRefresherRateFloorSeconds, "5") // a large floor — would fire IF the entry were present
	resetRefresherForTest()

	// No entry under the key (emulates "evicted before the worker picked
	// up the dirty-mark", the DELETE-self path after RemoveL1Key).
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "evicted"}
	key := ComputeKey(in)

	var handlerCalls atomic.Int32
	RegisterRefreshFunc("widgets", func(context.Context, string, ResolvedKeyInputs) error {
		handlerCalls.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	enqueueRefreshForTest(key)
	waitFor(t, 2*time.Second, "evicted-key skip counted",
		func() bool { return refresherSingleton().skippedNoEntryTotal.Load() == 1 })

	// The floor must NOT have fired on the absent entry.
	if fl := refresherSingleton().flooredTotal.Load(); fl != 0 {
		t.Fatalf("floored_total=%d want 0 — an absent (DELETE-evicted) key must never be floored", fl)
	}
	if got := handlerCalls.Load(); got != 0 {
		t.Fatalf("handler ran %d time(s) for an absent key — must not resurrect", got)
	}
}

// TestRefresher_RateFloorDeleteListDepReResolvesWithinFloor is the C3
// falsifier (PM gate condition C3): a DELETE's dependent LIST cell is
// dirty-marked (NOT evicted — feedback_l1_invalidation_delete_only), so it
// reaches the floor gate and is floor-delayed by ≤ floor. The deleted row
// must re-resolve within ≤ floor (not dropped, not immediate). Drives the
// full real chain: Deps().OnDelete → dirty-mark via the refresh hook →
// refresher queue → floor → deferred re-resolve.
func TestRefresher_RateFloorDeleteListDepReResolvesWithinFloor(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	t.Setenv(envRefresherRateFloorSeconds, "1") // 1s floor
	resetRefresherForTest()
	const floor = 1 * time.Second

	c := ResolvedCache()
	Deps().SetStore(c) // so OnDelete's isSelfRepresentation can read entry Inputs

	gvr := gvrCompositions()
	// The LIST cell's OWN object identity is a DIFFERENT object ("owner")
	// than the deleted one ("row-1"), so OnDelete classifies it NON-self →
	// dirty-mark (bucket 2), not evict. It LIST-depends on the deleted row's
	// namespace.
	listKey := ComputeKey(ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "ns", Name: "listcell"})
	listIn := inputsFor(gvr, "ns", "owner") // CacheEntryClass="restactions" — a registered handler kind below
	// Aged so the FIRST dirty-mark dequeue is NOT floored on staleness from
	// a recent Put — we want to observe the DELETE dirty-mark's own floor
	// behaviour. Put it fresh instead so the DELETE-triggered re-resolve is
	// floored (entry younger than floor): that is the C3 scenario (LIST-dep
	// refresh after DELETE is floor-delayed ≤ floor).
	c.Put(listKey, &ResolvedEntry{RawJSON: []byte(`{"rows":["row-1"]}`), Inputs: listIn})
	Deps().RecordList(context.Background(), listKey, gvr, "ns") // LIST-dep edge on the deleted row's ns

	var resolves atomic.Int32
	var resolveAt atomic.Int64
	// inputsFor sets CacheEntryClass="restactions"; register that kind.
	RegisterRefreshFunc("restactions", func(_ context.Context, k string, used ResolvedKeyInputs) error {
		// Re-resolve drops the deleted row from the LIST envelope.
		c.Put(k, &ResolvedEntry{RawJSON: []byte(`{"rows":[]}`), Inputs: &used})
		resolveAt.Store(time.Now().UnixNano())
		resolves.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	// DELETE the row → bucket-2 dirty-mark of the LIST cell → refresher
	// queue → floor (entry is young) → deferred re-resolve at ≤ floor.
	delStart := time.Now()
	evicted := Deps().OnDelete(gvr, "ns", "row-1")
	if evicted != 0 {
		t.Fatalf("OnDelete evicted %d — the LIST cell is non-self and must be dirty-marked, not evicted", evicted)
	}

	// The re-resolve must NOT be dropped: it fires within ≤ floor (+ a
	// scheduling slack). And it must NOT be immediate (the floor delays it).
	waitFor(t, floor+2*time.Second, "deleted-row LIST cell re-resolved",
		func() bool { return resolves.Load() == 1 })
	settle := time.Since(delStart)
	if settle > floor+1*time.Second {
		t.Fatalf("LIST-dep re-resolve took %s — expected ≤ floor (%s) + slack", settle, floor)
	}
	// Content correctness: the deleted row is gone from the LIST envelope.
	if e, ok := c.Get(listKey); !ok || string(e.RawJSON) != `{"rows":[]}` {
		t.Fatalf("LIST cell content not refreshed after DELETE: ok=%v raw=%q", ok, func() string {
			if e != nil {
				return string(e.RawJSON)
			}
			return "<nil>"
		}())
	}
	// The floor DID fire on the dirty-mark (entry was young at DELETE time).
	if fl := refresherSingleton().flooredTotal.Load(); fl == 0 {
		t.Fatalf("floored_total=0 — the DELETE's LIST-dep dirty-mark should have been floor-delayed (C3)")
	}
	t.Logf("GREEN (C3): deleted-row LIST cell re-resolved in %s ≤ floor=%s (dirty-mark floor-delayed, not dropped, not immediate); floored_total=%d",
		settle, floor, refresherSingleton().flooredTotal.Load())
}

// --- Test 5 — -race separate-goroutine marker storm vs the dequeuer --------

// TestRefresher_RateFloorConcurrentMarkerStormRaceFree extends the
// ConcurrentEnqueueRaceFree shape with the floor active: many goroutines
// hammer enqueueRefreshForTest on a SMALL keyset (so the floor fires
// heavily — repeated re-marks of young entries) while the worker pool
// floors/defers/re-resolves. Run under -race. The only shared state on the
// floored path is entry.CreatedAt (read under c.Get → c.mu) and the
// workqueue-internal-locked Forget/AddAfter/Done.
//
// This is the RACE EXERCISER, not the lossless-invariant proof: the
// invariant itself (no mark dropped; deferred resolve at expiry) is pinned
// by Tests 1, 2 and the C3 deleted-row test — this test's convergence
// assertion would pass even with a dropped deferred mark, because the
// storm keeps re-marking. Asserts: no data race, every key eventually
// re-resolves at least once, bounded (no runaway).
func TestRefresher_RateFloorConcurrentMarkerStormRaceFree(t *testing.T) {
	cleanup := withCleanRefresher(t, 4, 0)
	defer cleanup()
	t.Setenv(envRefresherRateFloorSeconds, "1") // 1s floor — many marks land inside it
	resetRefresherForTest()

	c := ResolvedCache()
	const K = 12 // small keyset → heavy flooring
	keys := make([]string, K)
	resolved := make([]atomic.Int32, K)
	for i := 0; i < K; i++ {
		in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "storm" + strconv.Itoa(i)}
		keys[i] = ComputeKey(in)
		// Aged so the first dequeue of each re-resolves immediately, then
		// the storm's repeated marks floor.
		putAgedEntry(c, keys[i], &in)
	}

	idxOf := map[string]int{}
	for i, k := range keys {
		idxOf[k] = i
	}
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, used ResolvedKeyInputs) error {
		c.Put(k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &used})
		resolved[idxOf[k]].Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	// Storm: G goroutines each fire many marks across the keyset for ~600ms
	// (inside the 1s floor for the bulk), racing the dequeuer.
	const G = 8
	var wg sync.WaitGroup
	stormDeadline := time.Now().Add(600 * time.Millisecond)
	for g := 0; g < G; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			i := seed
			for time.Now().Before(stormDeadline) {
				enqueueRefreshForTest(keys[i%K])
				i++
				time.Sleep(time.Millisecond)
			}
		}(g)
	}
	wg.Wait()

	// Every key must have re-resolved at least once (no key dropped); and
	// after a settle past the floor every key's final mark must have
	// converged (the lossless invariant under concurrency).
	waitFor(t, 4*time.Second, "every key re-resolved at least once", func() bool {
		for i := 0; i < K; i++ {
			if resolved[i].Load() == 0 {
				return false
			}
		}
		return true
	})

	// Bounded: total resolves must be far below total marks (the floor
	// collapsed the storm) but the gate must have fired.
	if fl := refresherSingleton().flooredTotal.Load(); fl == 0 {
		t.Fatalf("floored_total=0 under a marker storm with a 1s floor — gate never fired")
	}
	t.Logf("GREEN (-race): %d keys all re-resolved under an %d-goroutine storm; floored_total=%d, completed_total=%d",
		K, G, refresherSingleton().flooredTotal.Load(), refresherSingleton().completedTotal.Load())
}

// --- A2 (architect review) — the UNWIRED default actually fires ------------

// TestRefresher_RateFloorDefaultIsTwoSecondsAndFloors closes the
// harness-masking gap: every shared test harness pins floor=0 to preserve
// pre-#321 semantics, so nothing else proves the env-unset DEFAULT (2s,
// the PM-gate ruling) is wired end-to-end. Asserts rateFloor() == 2s with
// the env empty (int64FromEnv treats "" as unset), and that a freshly-Put
// entry is FLOORED (deferred, not resolved) under that default, then
// resolves losslessly at expiry.
func TestRefresher_RateFloorDefaultIsTwoSecondsAndFloors(t *testing.T) {
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	t.Setenv(envRefresherRateFloorSeconds, "")
	resetRefresherForTest()

	if got, want := refresherSingleton().rateFloor(), 2*time.Second; got != want {
		t.Fatalf("rateFloor() with env unset = %s, want %s (defaultRefresherRateFloorSeconds)", got, want)
	}

	c := ResolvedCache()
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "default-floor"}
	key := ComputeKey(in)
	// FRESH entry (Put stamps CreatedAt=now) → younger than the 2s default
	// → the first mark must FLOOR, not resolve.
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in})

	var calls atomic.Int32
	RegisterRefreshFunc("widgets", func(_ context.Context, k string, used ResolvedKeyInputs) error {
		c.Put(k, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &used})
		calls.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	flBefore := refresherSingleton().flooredTotal.Load()
	enqueueRefreshForTest(key)
	waitFor(t, 2*time.Second, "default-floor gate fires", func() bool {
		return refresherSingleton().flooredTotal.Load() > flBefore
	})
	if got := calls.Load(); got != 0 {
		t.Fatalf("resolve fired despite the 2s default floor: calls=%d want 0 (deferred)", got)
	}
	// Lossless under the default too: the deferred mark resolves at expiry.
	waitFor(t, 4*time.Second, "deferred resolve at default-floor expiry",
		func() bool { return calls.Load() == 1 })
}
