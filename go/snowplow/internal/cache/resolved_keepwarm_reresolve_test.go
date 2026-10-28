// resolved_keepwarm_reresolve_test.go — #102 GTTL c1 the RE-RESOLVE-NOT-EXTEND
// crux (design §1.1-1.2 + the design-gate HARD invariant): the keep-warm sweep
// must RE-RESOLVE (fresh bytes via a genuine Put, which slides CreatedAt),
// NEVER lease-renew / touch CreatedAt without fresh content. These arms pin the
// load-bearing STORE semantics the whole c1 design rests on, so a future edit
// that tried a CreatedAt-touch shortcut would go RED here.
//
// Store-level + hermetic (newResolvedCache, backdated CreatedAt) — mirrors
// resolved_catalog_ttl_test.go. No cluster.

package cache

import (
	"testing"
	"time"
)

// TestKeepwarm_ReResolvePutSlidesCreatedAtWithFreshContent — a Put (what the
// sweep's seedOneWidget/seedOneRestaction do) RESETS CreatedAt AND stores the
// new bytes. An entry aged past its TTL, re-Put, is a HIT again with the FRESH
// content. This is the mechanism c1 relies on: the sweep re-resolves before TTL
// and the re-Put keeps the cell warm with current data.
func TestKeepwarm_ReResolvePutSlidesCreatedAtWithFreshContent(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Second) // 1s standard TTL

	// Warm cell, backdated 2s → already past the 1s TTL (would MISS on Get).
	c.put("k", &ResolvedEntry{
		RawJSON:   []byte(`{"v":"stale"}`),
		CreatedAt: time.Now().Add(-2 * time.Second),
	})
	if _, ok := c.Get("k"); ok {
		t.Fatalf("setup: an entry aged 2s under a 1s TTL must be a MISS before the re-resolve")
	}

	// The SWEEP action: a genuine re-resolve Put with FRESH bytes (CreatedAt
	// left zero → the store stamps time.Now(), resolved.go:807-808).
	c.put("k", &ResolvedEntry{
		RawJSON: []byte(`{"v":"fresh"}`),
	})
	got, ok := c.Get("k")
	if !ok {
		t.Fatalf("RE-RESOLVE crux: after a re-resolve Put the cell must be a HIT (CreatedAt slid to now) — "+
			"if this MISSes, Put is not resetting CreatedAt and the sweep cannot keep cells warm")
	}
	if string(got.RawJSON) != `{"v":"fresh"}` {
		t.Fatalf("RE-RESOLVE crux: the re-Put must store FRESH content, got %q — a CreatedAt-touch/lease-renewal "+
			"shortcut (extend life without re-resolving) would leave STALE bytes here (the staleness-backstop violation)", got.RawJSON)
	}
}

// TestKeepwarm_ReadDoesNotSlideCreatedAt_QuietCellStillExpires — THE crux
// discriminator (design §1.2): a Get does an LRU MoveToFront but NEVER touches
// CreatedAt, so a cell that is READ-HOT but DATA-QUIET (no re-Put) STILL expires
// at TTL. This is exactly why the sweep must RE-RESOLVE, not lease-renew.
//
// DETERMINISTIC (no sleep-timing races): two assertions, both on backdated
// CreatedAt like resolved_catalog_ttl_test.go —
//   (1) a Get NEVER changes CreatedAt (read the entry pointer's CreatedAt before
//       and after a HIT — they must be identical); AND
//   (2) reading a cell whose CreatedAt is already backdated PAST the TTL is a
//       MISS (lazy-on-Get eviction) — the read does not rescue it.
// The crux MUTATION (make Get slide CreatedAt, a lease-renewal touch) FAILS arm
// (1) directly (CreatedAt changes on read) and arm (2) if the slide precedes the
// expiry check — either way RED, with no wall-clock dependence.
func TestKeepwarm_ReadDoesNotSlideCreatedAt_QuietCellStillExpires(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour) // long TTL — arm (1) is timing-free

	// (1) A HIT must not move CreatedAt. Put a fresh entry, capture CreatedAt,
	// Get it several times, assert CreatedAt is byte-identical after each read.
	c.put("hot", &ResolvedEntry{RawJSON: []byte(`{"v":1}`)})
	got, ok := c.Get("hot")
	if !ok {
		t.Fatalf("setup: fresh entry must be a HIT")
	}
	created := got.CreatedAt
	if created.IsZero() {
		t.Fatalf("setup: Put must stamp CreatedAt")
	}
	for i := 0; i < 5; i++ {
		g, ok := c.Get("hot")
		if !ok {
			t.Fatalf("read %d must be a HIT", i)
		}
		if !g.CreatedAt.Equal(created) {
			t.Fatalf("RE-RESOLVE crux: a Get SLID CreatedAt (%v → %v) — reads must NEVER renew the lease. "+
				"If a read renews CreatedAt, a read-hot but data-quiet cell would never expire and the whole "+
				"§1.2 premise (and the need for a RE-RESOLVING sweep, not a touch) collapses; a CreatedAt-touch "+
				"sweep would be masking staleness", created, g.CreatedAt)
		}
	}

	// (2) A quiet cell already aged past the TTL is a MISS on read — the read
	// does not rescue it (only a re-resolve Put would). Deterministic via a
	// short TTL + backdated CreatedAt (no sleep).
	c2 := newResolvedCache(10, 1<<20, time.Second)
	c2.put("quiet", &ResolvedEntry{
		RawJSON:   []byte(`{"v":1}`),
		CreatedAt: time.Now().Add(-2 * time.Second), // aged 2s under a 1s TTL
	})
	if _, ok := c2.Get("quiet"); ok {
		t.Fatalf("RE-RESOLVE crux: a data-quiet cell aged past TTL MUST be a MISS on read (lazy-on-Get evict) — "+
			"the read must not keep it warm; only a re-resolving Put resets CreatedAt")
	}
	if c2.evictTTLTotal.Load() == 0 {
		t.Fatalf("RE-RESOLVE crux: expected a lazy-on-Get TTL eviction (evictTTLTotal>0) for the expired quiet cell")
	}
}
