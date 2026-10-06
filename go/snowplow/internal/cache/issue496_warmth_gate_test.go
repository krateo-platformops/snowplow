//go:build unit || integration

package cache

import (
	"context"
	"testing"
	"time"
)

// issue496 — the warmth gate on the #378 re-mint.
//
// WHY THESE ARMS EXIST AND THE 14 #378 ARMS DO NOT COVER THIS. Every #378 arm
// sets TTL >= maxEntryAge (e.g. TTL=60s, maxAge=4s). Production INVERTS that
// ratio: TTL=3600s against maxEntryAge=86400s, i.e. 1:24. With TTL >= maxAge a
// cell cold-filled at t=0 is still "read within the TTL" when it reaches the
// cap, so it is WARM for the whole run and the gate can never discriminate —
// the #378 suite goes 14/14 green on a gated build and on an ungated one
// alike. These arms use the production RATIO so the gate's behaviour is
// actually reachable, per the standing rule that an arm which cannot fail is
// not coverage.
//
// SHAPE: ttl:maxAge = 1:24 (300ms : 7.2s), so remintLead() = min(ttl,
// maxAge/2) = ttl = 300ms and the lead window is [6.9s, 7.2s) — exactly one
// TTL wide, which is the production geometry.
// GEOMETRY. What the gate needs is the production INVERSION — TTL well below
// maxEntryAge, so remintLead() = min(TTL, maxAge/2) = TTL and a cell can be
// read-COLD while its body is still fresh. The existing #378 arms all set
// TTL >= maxAge, which makes that state unreachable.
//
// The exact 1:24 of production (3600s:86400s) would give a window only
// maxAge/24 wide, so a first cut of these arms used TTL=300ms/maxAge=7.2s and a
// fixed Sleep landing mid-window with ~150ms of slack each side. That is a
// load-sensitive fixed-duration arm — the #501 defect — and under -race it can
// overshoot the cap and fail its own precondition. Replaced by 1:6 (still
// firmly inverted, still L=TTL) plus i496WaitForWindow, which POLLS to the
// window edge instead of guessing at it, so the slack is ~1s regardless of load.
const (
	i496TTL    = time.Second
	i496MaxAge = 6 * i496TTL // L = min(TTL, maxAge/2) = TTL → window [5s, 6s), 1s wide
)

func i496Store(t *testing.T) *ResolvedCacheStore {
	t.Helper()
	c := newResolvedCache(0, 0, i496TTL)
	c.maxEntryAge = i496MaxAge
	if lead := c.remintLead(); lead != i496TTL {
		t.Fatalf("SETUP: remintLead()=%v, want %v — the window geometry is not the production one", lead, i496TTL)
	}
	return c
}

// i496IsWarm reports the store's OWN warmth verdict for a key, under the lock
// the predicate requires. Asserted as a PRECONDITION so an arm can never
// silently test the wrong state (e.g. a bounds retune making a "cold" cell warm).
func i496IsWarm(t *testing.T, c *ResolvedCacheStore, key string) bool {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.index[key]
	if !ok {
		t.Fatalf("SETUP: key %q is not resident", key)
	}
	return c.warmLocked(el.Value.(*lruItem), time.Now())
}

// i496WaitForWindow blocks until the cell's BornAt age is inside the lead
// window [maxAge − L, maxAge), by REAL elapse, then returns. It polls rather
// than sleeping a computed duration, so machine load shifts WHEN it returns and
// never WHETHER the arm is in the right state — and it fails loudly if the cell
// is already past the cap, which is an environment problem and must not be
// mistaken for the defect under test.
func i496WaitForWindow(t *testing.T, c *ResolvedCacheStore, key string, bornAt time.Time) {
	t.Helper()
	start := i496MaxAge - c.remintLead()
	deadline := time.Now().Add(i496MaxAge + 5*time.Second)
	for {
		age := time.Since(bornAt)
		if age >= i496MaxAge {
			t.Fatalf("SETUP: the cell is already PAST the cap (age %v >= %v) before the arm acted — "+
				"the machine stalled; this is an environment failure, not the #496 defect", age, i496MaxAge)
		}
		if age >= start {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("SETUP: never reached the lead window (age %v, want >= %v)", age, start)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestIssue496_ColdCellInTheWindowIsNotReMinted — the falsifier.
//
// A COLD cell inside the lead window must NOT have its BornAt reset, because a
// re-mint would move the max-age clock out from under the reaper's cold-evict
// (`pastMaxAge && !warm`) and the TTL cannot reclaim it either (the same
// refresh resets CreatedAt). That is the #259 / #191 bound #496 restores.
//
// RED without the gate: pre-#496 inRemintWindowLocked consults only the age, so
// this cell IS re-minted and the assertion on BornAt fails.
func TestIssue496_ColdCellInTheWindowIsNotReMinted(t *testing.T) {
	c := i496Store(t)
	in := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, Namespace: "i496", Name: "cold"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"items":[{"v":"born"}]}`), Inputs: &in})

	e0, ok := c.GetNoTouch(key)
	if !ok {
		t.Fatal("SETUP: cell not resident after Put")
	}

	// Drive the REAL boundary: let the cell age into the window by elapsing,
	// never by installing a crossed state (no SetBornAtForTest here).
	i496WaitForWindow(t, c, key, e0.BornAt)

	// PRECONDITION — this arm proves nothing unless the cell is genuinely cold.
	// (The window itself is guaranteed by i496WaitForWindow.)
	if i496IsWarm(t, c, key) {
		t.Fatalf("SETUP: the cell is WARM (ttl %v, BornAt age %v) — this arm must drive a COLD cell",
			i496TTL, time.Since(e0.BornAt))
	}
	refusedBefore := c.Stats().RemintRefusedColdTotal

	if !c.ReplaceIfGenRefresh(context.Background(), key, &ResolvedEntry{RawJSON: []byte(`{"items":[{"v":"fresh"}]}`), Inputs: &in}, c.CaptureGen(key)) {
		t.Fatal("SETUP: ReplaceIfGenRefresh refused a live in-window cell; the write must be ACCEPTED (only the re-mint is gated)")
	}

	e1, ok := c.GetNoTouch(key)
	if !ok {
		t.Fatal("#496 RED: the accepted refresh must REPLACE the cell, not remove it")
	}
	if !e1.BornAt.Equal(e0.BornAt) {
		t.Errorf("#496 RED: a COLD cell in the lead window was RE-MINTED — BornAt moved %v → %v (+%v). "+
			"The reaper's cold-evict can no longer reclaim it, and TTL cannot either, so the cell is immortal (#259/#191)",
			e0.BornAt, e1.BornAt, e1.BornAt.Sub(e0.BornAt))
	}
	if got := c.Stats().RemintRefusedColdTotal; got != refusedBefore+1 {
		t.Errorf("#496: remint_refused_cold_total = %d, want %d — the refusal must be OBSERVABLE, "+
			"or an operator cannot tell a working gate from an unreached window", got, refusedBefore+1)
	}
	// The body must still be fresh: #496 gates the CLOCK, never the content.
	if string(e1.RawJSON) != `{"items":[{"v":"fresh"}]}` {
		t.Errorf("#496: the refresh body must still land (got %s) — the gate must not suppress the write", e1.RawJSON)
	}
}

// TestIssue496_WarmCellInTheWindowIsStillReMinted is the other half, and it is
// what stops the fix from being "never re-mint anything". It also pins the
// measured claim in inRemintWindowLocked's doc comment: with the production
// geometry the window is exactly one TTL wide, so a cell read inside the window
// is warm at the refresh and #378's own population is untouched by the gate.
func TestIssue496_WarmCellInTheWindowIsStillReMinted(t *testing.T) {
	c := i496Store(t)
	in := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, Namespace: "i496", Name: "warm"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"items":[{"v":"born"}]}`), Inputs: &in})
	e0, _ := c.GetNoTouch(key)

	i496WaitForWindow(t, c, key, e0.BornAt)

	// Keep the BODY fresh the way the refresher does, with a plain (non-freshMint)
	// ReplaceIfGen: it resets CreatedAt and INHERITS BornAt, so the cell is
	// TTL-live again while staying inside the lead window by its birth. This is
	// not scaffolding — it IS the production shape this issue is about: with
	// TTL:maxAge = 1:24 a cell only reaches the window at all because refreshes
	// keep its body alive, which is why its read-recency can be stale while its
	// content is current. Without this the cell is TTL-expired and the customer
	// Get below lazily evicts it instead of hitting.
	if !c.ReplaceIfGen(context.Background(), key, &ResolvedEntry{RawJSON: []byte(`{"items":[{"v":"kept"}]}`), Inputs: &in}, c.CaptureGen(key)) {
		t.Fatal("SETUP: the body-keeping ReplaceIfGen was refused")
	}
	eKept, ok := c.GetNoTouch(key)
	if !ok {
		t.Fatal("SETUP: cell not resident after the body-keeping refresh")
	}
	if !eKept.BornAt.Equal(e0.BornAt) {
		t.Fatalf("SETUP: a non-freshMint ReplaceIfGen must INHERIT BornAt (%v → %v); the arm would not be in the window",
			e0.BornAt, eKept.BornAt)
	}
	if age := time.Since(eKept.BornAt); age < i496MaxAge-i496TTL || age >= i496MaxAge {
		t.Fatalf("SETUP: BornAt age %v left the lead window [%v, %v) while keeping the body fresh",
			age, i496MaxAge-i496TTL, i496MaxAge)
	}

	// A real CUSTOMER read inside the window — Get (touching), not GetNoTouch,
	// which is what makes the cell warm at the moment of the refresh.
	if _, ok := c.Get(key); !ok {
		t.Fatal("SETUP: the in-window customer read must HIT")
	}
	if !i496IsWarm(t, c, key) {
		t.Fatal("SETUP: the cell must be WARM after a touching Get — otherwise this arm tests nothing")
	}
	refusedBefore := c.Stats().RemintRefusedColdTotal

	if !c.ReplaceIfGenRefresh(context.Background(), key, &ResolvedEntry{RawJSON: []byte(`{"items":[{"v":"fresh"}]}`), Inputs: &in}, c.CaptureGen(key)) {
		t.Fatal("SETUP: ReplaceIfGenRefresh refused a live in-window cell")
	}
	e1, _ := c.GetNoTouch(key)
	if !e1.BornAt.After(e0.BornAt) {
		t.Errorf("#496 RED: a WARM cell in the lead window must STILL be re-minted (BornAt %v → %v) — "+
			"gating on warmth must not cost #378 its own goal", e0.BornAt, e1.BornAt)
	}
	if got := c.Stats().RemintRefusedColdTotal; got != refusedBefore {
		t.Errorf("#496: remint_refused_cold_total moved (%d → %d) for a WARM cell — the counter must count only refusals",
			refusedBefore, got)
	}
}

// TestIssue496_SeededCellLosesSeedWarmthAtItsFirstRefresh pins the reason the
// gate is not a loophole for the boot-prewarm set. warmLocked counts
// SeededAtBoot, so a seeded cell IS re-minted on its first in-window refresh —
// but that write overwrites the entry with SeededAtBoot=false (the field doc on
// ResolvedEntry.SeededAtBoot), so it is gated from then on unless a customer
// reads it. Without this, "seeded cells stay immortal" would be a live hole and
// #496 would only half-close #259.
func TestIssue496_SeededCellLosesSeedWarmthAtItsFirstRefresh(t *testing.T) {
	c := i496Store(t)
	in := ResolvedKeyInputs{CacheEntryClass: CacheEntryClassRAFullList, Namespace: "i496", Name: "seeded"}
	key := ComputeKey(in)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"items":[{"v":"born"}]}`), Inputs: &in, SeededAtBoot: true})

	if !i496IsWarm(t, c, key) {
		t.Fatal("SETUP: a SeededAtBoot cell must read WARM before any read")
	}
	eSeed, _ := c.GetNoTouch(key)
	i496WaitForWindow(t, c, key, eSeed.BornAt)
	if !i496IsWarm(t, c, key) {
		t.Fatal("SETUP: seed warmth must not expire on its own — it is a provenance flag, not a clock")
	}

	// First in-window refresh: re-minted (seed warmth), and the entry it writes
	// carries SeededAtBoot=false.
	if !c.ReplaceIfGenRefresh(context.Background(), key, &ResolvedEntry{RawJSON: []byte(`{"items":[{"v":"r1"}]}`), Inputs: &in}, c.CaptureGen(key)) {
		t.Fatal("SETUP: first refresh refused")
	}
	e1, _ := c.GetNoTouch(key)
	if e1.SeededAtBoot {
		t.Fatalf("#496: a refresher re-Put must re-classify the cell as traffic (SeededAtBoot=false); " +
			"if it did not, seeded cells would stay warm forever and the gate would never reclaim them")
	}
	if i496IsWarm(t, c, key) {
		t.Errorf("#496: after losing seed provenance and with no customer read, the cell must be COLD — " +
			"otherwise the #259 bound stays open for the whole boot-prewarm set")
	}

	// Age it back into the window from its NEW birth; now the gate must refuse.
	i496WaitForWindow(t, c, key, e1.BornAt)
	refusedBefore := c.Stats().RemintRefusedColdTotal
	if !c.ReplaceIfGenRefresh(context.Background(), key, &ResolvedEntry{RawJSON: []byte(`{"items":[{"v":"r2"}]}`), Inputs: &in}, c.CaptureGen(key)) {
		t.Fatal("SETUP: second refresh refused")
	}
	e2, _ := c.GetNoTouch(key)
	if !e2.BornAt.Equal(e1.BornAt) {
		t.Errorf("#496 RED: the ex-seeded, never-read cell was re-minted again (BornAt %v → %v) — "+
			"the boot-prewarm set would be immortal", e1.BornAt, e2.BornAt)
	}
	if got := c.Stats().RemintRefusedColdTotal; got != refusedBefore+1 {
		t.Errorf("#496: the refusal of the ex-seeded cell must be counted (%d → %d)", refusedBefore, got)
	}
}
