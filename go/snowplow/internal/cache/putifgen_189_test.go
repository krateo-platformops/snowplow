// putifgen_189_test.go — #189 generation-guarded Put (store primitive).
//
// DEFECT: an in-flight resolve for key K captures nothing about K's liveness; if
// K is DELETE-evicted (dep tracker) between the resolve start and its tail Put,
// the plain Put RESURRECTS the pre-delete body, which is then served. PutIfGen
// closes it: the caller captures K's generation BEFORE the resolve and the tail
// write is refused if a removal bumped the generation in between — one c.mu
// critical section, no Get-then-Put TOCTOU.
//
// This suite covers the STORE primitive. The per-carrier dispatcher A5 arms
// (real resolve + real DELETE-evict interleaved) live with each threaded site.
//
// Cross-discrimination: the REFUSE arms drive a REAL removal funnel
// (DeleteForTest → deleteForDep; LRU eviction → removeElementLocked), never an
// installed gen mismatch; the ACCEPT arms prove PutIfGen does not over-refuse a
// legit write with no intervening removal (cold-fill and replace-in-place).

package cache

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func entry189(body string) *ResolvedEntry {
	return &ResolvedEntry{RawJSON: []byte(body)}
}

// ACCEPT arm — a cold fill (key never present, gen 0) is stored, not over-refused.
func TestPutIfGen_S189_ColdFillAcceptsAtGenZero(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	gen0 := c.CaptureGen("k") // absent → 0
	if gen0 != 0 {
		t.Fatalf("cold key gen want 0, got %d", gen0)
	}
	if !c.PutIfGen("k", entry189(`{"v":1}`), gen0) {
		t.Fatalf("#189: a cold-fill PutIfGen (no intervening removal) must be ACCEPTED")
	}
	if _, ok := c.Get("k"); !ok {
		t.Fatalf("#189: accepted cold-fill must be readable")
	}
	if r := c.Stats().PutRefusedGenerationMovedTotal; r != 0 {
		t.Fatalf("#189: cold-fill accept must not count a refusal, got %d", r)
	}
}

// ACCEPT arm — a replace-in-place with the SAME captured gen (a benign refresh,
// no removal) succeeds and does not over-refuse.
func TestPutIfGen_S189_ReplaceInPlaceSameGenAccepts(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.Put("k", entry189(`{"v":1}`))
	gen0 := c.CaptureGen("k")
	if !c.PutIfGen("k", entry189(`{"v":2}`), gen0) {
		t.Fatalf("#189: a replace-in-place PutIfGen with unchanged gen must be ACCEPTED")
	}
	got, ok := c.Get("k")
	if !ok || string(got.RawJSON) != `{"v":2}` {
		t.Fatalf("#189: accepted replace must store the new body, got ok=%v body=%s", ok, got.RawJSON)
	}
	if r := c.Stats().PutRefusedGenerationMovedTotal; r != 0 {
		t.Fatalf("#189: benign replace must not count a refusal, got %d", r)
	}
}

// REFUSE arm (core) — a real DELETE-evict (deleteForDep) between capture and Put
// bumps the generation; the stale tail Put is refused and the deleted body is
// NOT resurrected. RED on the pre-#189 plain Put (which resurrects).
func TestPutIfGen_S189_RefuseAfterRealDeleteEvict(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.Put("k", entry189(`{"v":"pre-delete"}`))
	gen0 := c.CaptureGen("k") // dispatcher captures before its resolve

	// The object is DELETE-evicted during the resolve (real deleteForDep funnel).
	c.DeleteForTest("k")

	// The resolve's tail Put lands with the pre-delete body.
	if c.PutIfGen("k", entry189(`{"v":"pre-delete"}`), gen0) {
		t.Fatalf("#189: a PutIfGen carrying the pre-DELETE generation must be REFUSED")
	}
	if _, ok := c.Get("k"); ok {
		t.Fatalf("#189: the DELETE-evicted body must NOT be resurrected — the eviction is authoritative")
	}
	if r := c.Stats().PutRefusedGenerationMovedTotal; r != 1 {
		t.Fatalf("#189: the refusal must be counted once, got %d", r)
	}
}

// REFUSE arm — an LRU eviction (removeElementLocked funnel) between capture and
// Put also bumps the generation. This is the funnel that closes the
// LRU-evict-THEN-DELETE interleave, so it must tombstone too.
func TestPutIfGen_S189_RefuseAfterLRUEvict(t *testing.T) {
	c := newResolvedCache(2, 1<<20, time.Hour) // tiny cap → forced LRU eviction
	c.Put("k", entry189(`{"v":1}`))
	gen0 := c.CaptureGen("k")
	// Push k out of the LRU (maxEntries=2): two fresh keys evict it via
	// removeElementLocked.
	c.Put("a", entry189(`{"v":"a"}`))
	c.Put("b", entry189(`{"v":"b"}`))
	if _, ok := c.Get("k"); ok {
		t.Fatalf("setup: k should have been LRU-evicted")
	}
	if c.PutIfGen("k", entry189(`{"v":1}`), gen0) {
		t.Fatalf("#189: a PutIfGen carrying the pre-LRU-eviction generation must be REFUSED")
	}
	if _, ok := c.Get("k"); ok {
		t.Fatalf("#189: an LRU-evicted key must not be resurrected by a stale in-flight Put")
	}
}

// OVER-REFUSAL + SELF-HEAL arm (TL + freshness-audit contract) — paired with the
// no-eviction ACCEPT arms above, this pins the FULL contract: ANY eviction
// between capture and Put, INCLUDING a benign LRU eviction of still-valid data,
// refuses the fill (the bounded over-refusal the bump-both-funnels design
// accepts), AND the very next re-resolve with a FRESH gen0 SUCCEEDS — so the LRU
// cost is a known, tested cold-miss-then-refill, never a silent permanent
// refusal. This makes the over-refusal a pinned behaviour, not a surprise.
func TestPutIfGen_S189_LRUOverRefuseThenReResolveSelfHeals(t *testing.T) {
	c := newResolvedCache(2, 1<<20, time.Hour) // tiny cap → forced LRU eviction
	c.Put("k", entry189(`{"v":1}`))
	staleGen := c.CaptureGen("k")
	// Benign LRU eviction of k (still-VALID data — no DELETE), via two fresh keys.
	c.Put("a", entry189(`{"v":"a"}`))
	c.Put("b", entry189(`{"v":"b"}`))

	// The in-flight fill carrying the pre-eviction gen is REFUSED (over-refusal).
	if c.PutIfGen("k", entry189(`{"v":1}`), staleGen) {
		t.Fatalf("#189: a fill carrying the pre-LRU-eviction gen must be refused (bounded over-refusal)")
	}
	// SELF-HEAL: the next resolve captures a FRESH gen and its fill SUCCEEDS —
	// proving the over-refusal is a recoverable cold miss, not a dead key.
	freshGen := c.CaptureGen("k")
	if !c.PutIfGen("k", entry189(`{"v":1}`), freshGen) {
		t.Fatalf("#189: the self-healing re-resolve (fresh gen0) must ACCEPT — the over-refusal must be a cold miss, not a permanent refusal")
	}
	if _, ok := c.Get("k"); !ok {
		t.Fatalf("#189: after the self-healing re-resolve the key must be resident again")
	}
}

// BOUND arm — the tombstone is not permanent: past tombstoneTTL a fresh capture
// reports gen 0 again and a PutIfGen succeeds, so the guard self-heals within
// the request-write-deadline window (no unbounded refusal).
func TestPutIfGen_S189_TombstoneExpiryAllowsFreshPut(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.SetTombstoneTTL(30 * time.Millisecond)
	c.Put("k", entry189(`{"v":1}`))
	staleGen := c.CaptureGen("k")
	c.DeleteForTest("k")
	// A stale in-flight Put is still refused while the tombstone is live.
	if c.PutIfGen("k", entry189(`{"v":1}`), staleGen) {
		t.Fatalf("#189: within the tombstone window the stale Put must be refused")
	}
	time.Sleep(60 * time.Millisecond) // outlive the tombstone
	freshGen := c.CaptureGen("k")
	if freshGen != 0 {
		t.Fatalf("#189: after tombstone expiry the key gen must reset to 0, got %d", freshGen)
	}
	if !c.PutIfGen("k", entry189(`{"v":2}`), freshGen) {
		t.Fatalf("#189: after tombstone expiry a fresh-capture PutIfGen must be ACCEPTED (no permanent refusal)")
	}
	if _, ok := c.Get("k"); !ok {
		t.Fatalf("#189: post-expiry accepted Put must be readable")
	}
}

// DISABLED arm — tombstoneTTL<=0 disables the guard: no tombstone is recorded,
// so a post-delete cold gen is 0 and the plain-Put-equivalent proceeds.
func TestPutIfGen_S189_TTLZeroDisablesGuard(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.SetTombstoneTTL(0)
	c.Put("k", entry189(`{"v":1}`))
	c.DeleteForTest("k")
	if g := c.CaptureGen("k"); g != 0 {
		t.Fatalf("#189: with the guard disabled no tombstone should survive a delete, gen got %d", g)
	}
}

// ReplaceIfGen ACCEPT — a live key at the captured generation is replaced.
func TestReplaceIfGen_S189_ReplacesLiveKeyAtSameGen(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.Put("k", entry189(`{"v":1}`))
	gen0 := c.CaptureGen("k")
	if !c.ReplaceIfGen("k", entry189(`{"v":2}`), gen0) {
		t.Fatalf("#189: ReplaceIfGen must replace a live key at the captured generation")
	}
	got, ok := c.Get("k")
	if !ok || string(got.RawJSON) != `{"v":2}` {
		t.Fatalf("#189: replaced body want {\"v\":2}, got ok=%v body=%s", ok, got.RawJSON)
	}
}

// ReplaceIfGen REFUSE (never inserts) — an absent key is refused, NOT inserted.
// This preserves the refresher's "evicted → do not resurrect / no signal"
// contract: a refresh must never create a cell for an evicted key.
func TestReplaceIfGen_S189_RefusesAbsentKeyNeverInserts(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	if c.ReplaceIfGen("cold", entry189(`{"v":1}`), 0) {
		t.Fatalf("#189: ReplaceIfGen must REFUSE an absent key (replace-only, never insert)")
	}
	if _, ok := c.Get("cold"); ok {
		t.Fatalf("#189: ReplaceIfGen must not have inserted the absent key")
	}
	c.Put("k", entry189(`{"v":1}`))
	gen0 := c.CaptureGen("k")
	c.DeleteForTest("k")
	if c.ReplaceIfGen("k", entry189(`{"v":1}`), gen0) {
		t.Fatalf("#189: ReplaceIfGen must REFUSE a DELETE-evicted key (no resurrection)")
	}
	if _, ok := c.Get("k"); ok {
		t.Fatalf("#189: ReplaceIfGen must not resurrect the DELETE-evicted key")
	}
}

// ReplaceIfGen REFUSE — a live key whose generation moved (a cold request
// re-inserted it) is not clobbered by a stale refresh.
func TestReplaceIfGen_S189_RefusesWhenGenMovedUnderLiveKey(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.Put("k", entry189(`{"v":1}`))
	staleGen := c.CaptureGen("k")
	c.DeleteForTest("k")
	freshGen := c.CaptureGen("k")
	if !c.PutIfGen("k", entry189(`{"v":"fresh"}`), freshGen) {
		t.Fatalf("setup: cold re-insert should succeed")
	}
	if c.ReplaceIfGen("k", entry189(`{"v":"stale"}`), staleGen) {
		t.Fatalf("#189: ReplaceIfGen must REFUSE a stale-gen replace over a re-inserted live key")
	}
	got, _ := c.Get("k")
	if string(got.RawJSON) != `{"v":"fresh"}` {
		t.Fatalf("#189: the re-inserted body must not be clobbered by a stale refresh, got %s", got.RawJSON)
	}
}

// RACE arm — a real DELETE-evict interleaved with the guarded Put under -race.
// The invariant: it is never the case that the delete has completed AND the
// pre-delete body is servable. Either the Put won the race (stored before the
// delete, then the delete removed it → absent) or the delete won (Put refused →
// absent). A resurrection would leave the body present after a completed delete.
func TestPutIfGen_S189_ConcurrentDeleteEvictRace(t *testing.T) {
	for i := 0; i < 200; i++ {
		c := newResolvedCache(10, 1<<20, time.Hour)
		key := fmt.Sprintf("k%d", i)
		c.Put(key, entry189(`{"v":"pre-delete"}`))
		gen0 := c.CaptureGen(key)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); c.DeleteForTest(key) }()
		go func() { defer wg.Done(); c.PutIfGen(key, entry189(`{"v":"pre-delete"}`), gen0) }()
		wg.Wait()

		// After BOTH complete, if the delete is authoritative the body must be
		// gone. PutIfGen can only have stored if it observed the pre-delete gen
		// (i.e. it ran fully before the delete bumped it), and then the delete
		// removed it. A live entry here means a resurrected post-delete body.
		if _, ok := c.Get(key); ok {
			t.Fatalf("#189 race: the pre-delete body is servable after a completed DELETE-evict — resurrected")
		}
	}
}
