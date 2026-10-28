package cache

// store_clock_930_test.go — #930 S1: the store clock and the dirty-window clock
// are COMPLETE: every age the store decides or reports on a cell reads the
// store clock, and the stale-served age reads the dirty-window clock.
//
// Each arm drives a virtual clock through hours while the host's wall clock
// moves by milliseconds, so ANY reader left on time.Now sees an age near zero
// and the assertion on it reads RED: a Get, GetNoTouch, CellPutSeq,
// CellServeIdentity, the metadata ages and read-recency, RangeMetadata,
// RangeMetadataSample, the generation tombstone's window, the lazy TTL and
// max-age evictions and the reaper's cold, suppressed and warm verdicts.
//
//   C1  store clock, per reader (TTL leg, max-age leg, reaper leg).
//   C2  the stale-served age is the dirty-window clock's mark-to-hit interval.

import (
	"sync/atomic"
	"testing"
	"time"
)

// c930Clock is a virtual clock: it reads the instant it was last moved to.
type c930Clock struct{ at atomic.Int64 }

func newC930Clock() *c930Clock {
	k := &c930Clock{}
	k.at.Store(time.Now().UnixNano())
	return k
}
func (k *c930Clock) now() time.Time          { return time.Unix(0, k.at.Load()) }
func (k *c930Clock) advance(d time.Duration) { k.at.Add(int64(d)) }

func c930Put(c *ResolvedCacheStore, name string) string {
	in := ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "c930", Name: name}
	k := ComputeKey(in)
	c.put(k, &ResolvedEntry{RawJSON: []byte(`{"c930":1}`), Inputs: &in})
	return k
}

func c930Meta(t *testing.T, c *ResolvedCacheStore, k string) ResolvedEntryMeta {
	t.Helper()
	m, ok := c.MetadataForKey(k)
	if !ok {
		t.Fatalf("PRE: %s must be resident", k[:8])
	}
	return m
}

func TestIssue930_C1_EveryStoreAgeReadsTheStoreClock(t *testing.T) {
	const ttl, maxAge, tombstoneTTL = 60 * time.Second, 600 * time.Second, 30 * time.Second
	c := newResolvedCache(0, 0, ttl)
	c.maxEntryAge = maxAge
	c.SetTombstoneTTL(tombstoneTTL)
	clk := newC930Clock()
	c.SetClockForTest(clk.now)

	// ── TTL leg: a Get at +30 s stamps read-recency on the store clock, the
	// metadata ages read it, and the lazy TTL bound evicts at +61 s. ──
	a, a2 := c930Put(c, "a"), c930Put(c, "a2")
	clk.advance(30 * time.Second)
	if _, ok := c.Get(a); !ok {
		t.Fatalf("C1 RED: a 30 s old cell (TTL %v) missed on Get", ttl)
	}
	m := c930Meta(t, c, a)
	if m.AgeSeconds != 30 || m.LifetimeSeconds != 30 || m.TTLRemainingSeconds != 30 || m.LastReadSeconds != 0 {
		t.Fatalf("C1 RED: metadata at +30 s on the store clock: age=%d lifetime=%d ttl_remaining=%d last_read=%d, "+
			"want 30/30/30/0 — a reader is not on the store clock", m.AgeSeconds, m.LifetimeSeconds, m.TTLRemainingSeconds, m.LastReadSeconds)
	}
	ageOf := func(rng func(func(ResolvedEntryMeta) bool)) int64 {
		got := int64(-1)
		rng(func(x ResolvedEntryMeta) bool {
			if x.KeyHash == a {
				got = x.AgeSeconds
			}
			return true
		})
		return got
	}
	if got := ageOf(c.RangeMetadata); got != 30 {
		t.Fatalf("C1 RED: RangeMetadata reads age %d at +30 s on the store clock", got)
	}
	if got := ageOf(func(fn func(ResolvedEntryMeta) bool) { c.RangeMetadataSample(1, fn) }); got != 30 {
		t.Fatalf("C1 RED: RangeMetadataSample reads age %d at +30 s on the store clock", got)
	}
	if _, live := c.CellPutSeq(a); !live {
		t.Fatalf("C1 RED: CellPutSeq reads a 30 s old cell as not live")
	}
	clk.advance(31 * time.Second) // body age 61 s > TTL
	if _, live := c.CellPutSeq(a); live {
		t.Fatalf("C1 RED: CellPutSeq reads a cell past its TTL on the store clock as live")
	}
	if _, _, live := c.CellServeIdentity(a); live {
		t.Fatalf("C1 RED: CellServeIdentity reads a cell past its TTL on the store clock as live")
	}
	ttl0 := c.Stats().EvictTTLTotal
	if _, ok := c.GetNoTouch(a); ok {
		t.Fatalf("C1 RED: GetNoTouch served a cell 61 s old on the store clock (TTL %v)", ttl)
	}
	if d := c.Stats().EvictTTLTotal - ttl0; d != 1 {
		t.Fatalf("C1 RED: the lazy TTL eviction did not count (evict_ttl Δ=%d)", d)
	}
	// The eviction tombstoned the key's generation for tombstoneTTL on the store
	// clock: held at once, released once that window has passed on it.
	// a2 is evicted with it; its tombstone is left for the amortised sweep.
	if _, ok := c.GetNoTouch(a2); ok {
		t.Fatalf("PRE: a2 must be evicted past its TTL with a")
	}
	clk.advance(tombstoneTTL - time.Second)
	if c.CaptureGen(a) == 0 {
		t.Fatalf("C1 RED: the generation tombstone was released %v into its %v window on the store clock",
			tombstoneTTL-time.Second, tombstoneTTL)
	}
	clk.advance(2 * time.Second)
	if g := c.CaptureGen(a); g != 0 {
		t.Fatalf("C1 RED: the generation tombstone is still held %v past its window on the store clock (gen %d)", time.Second, g)
	}
	// The next tombstone runs the amortised sweep (at most once per window on
	// the store clock): a2's expired tombstone is reclaimed, the new one kept.
	b := c930Put(c, "b")
	be, _ := c.GetNoTouch(b)
	if !c.EvictUnrefreshable(b, be) {
		t.Fatalf("PRE: b must be evicted")
	}
	c.mu.Lock()
	tombs := len(c.tombstones)
	c.mu.Unlock()
	if tombs != 1 {
		t.Fatalf("C1 RED: %d generation tombstones held after the sweep, want 1 (b's) — the sweep's window is "+
			"not on the store clock", tombs)
	}

	// ── max-age leg and reaper leg: three cells whose bodies are kept young by
	// re-Puts (which inherit BornAt and read-recency) until their LIFETIME is
	// past maxAge. w is read by a customer inside the cap (warm); d is never read
	// after its insert (cold); s is suppressed (refresh-by-traffic-only). ──
	w, d, s := c930Put(c, "w"), c930Put(c, "d"), c930Put(c, "s")
	for age := time.Duration(0); age < maxAge; age += 50 * time.Second {
		clk.advance(50 * time.Second)
		for _, name := range []string{"w", "d", "s"} {
			c930Put(c, name)
		}
	}
	NoteRefreshDecline(s, "c930", true)
	t.Cleanup(func() { refreshSuppressed.Delete(s) })
	// lifetime 600 s, bodies 0 s old. A customer read of w at the cap's edge.
	if _, ok := c.Get(w); !ok {
		t.Fatalf("PRE: w at lifetime %v (= the cap) must still be served", maxAge)
	}
	clk.advance(11 * time.Second) // lifetime 611 s > maxAge, bodies 11 s < TTL
	if m := c930Meta(t, c, d); m.LifetimeSeconds != 611 || m.AgeSeconds != 11 {
		t.Fatalf("C1 RED: lifetime/age on the store clock = %d/%d, want 611/11", m.LifetimeSeconds, m.AgeSeconds)
	}
	c.reapPastMaxEntryAge()
	// Has: residency only, with no lazy eviction of its own.
	if c.Has(d) {
		t.Fatalf("C1 RED: the reaper kept a COLD cell 11 s past the cap on the store clock")
	}
	if c.Has(s) {
		t.Fatalf("C1 RED: the reaper kept a SUPPRESSED cell 11 s past the cap on the store clock")
	}
	if !c.Has(w) {
		t.Fatalf("C1 RED: the reaper evicted a cell read 11 s ago on the store clock (warm) — C3 must spare it")
	}
	ma0 := c.Stats().EvictMaxAgeTotal
	if _, ok := c.Get(w); ok {
		t.Fatalf("C1 RED: Get served a cell 611 s into its life on the store clock (cap %v)", maxAge)
	}
	if dd := c.Stats().EvictMaxAgeTotal - ma0; dd != 1 {
		t.Fatalf("C1 RED: the lazy max-age eviction did not count (evict_max_age Δ=%d)", dd)
	}
}

func TestIssue930_C2_StaleServedAgeReadsTheDirtyWindowClock(t *testing.T) {
	const age = 250 * time.Millisecond
	cleanup := withCleanRefresher(t, 1, 0)
	defer cleanup()
	c := ResolvedCache()
	Deps().SetStore(c)
	clk := newC930Clock()
	t.Cleanup(SetDirtyWindowClockForTest(clk.now))
	k := c930Put(c, "stale")
	DirtyMarkForTest(k) // the window opens at the virtual instant (no worker runs it)
	clk.advance(age)
	if _, ok := c.Get(k); !ok {
		t.Fatal("PRE: the dirty cell must be served")
	}
	st := resolvedCacheStatsByStatOf(c.Stats())
	if st["stale_served_total"] != 1 || st["stale_served_age_ms_max"] != age.Milliseconds() {
		t.Fatalf("C2 RED: stale_served_total=%d stale_served_age_ms_max=%d, want 1 and exactly %d — the stale-served "+
			"age is not the dirty-window clock's mark-to-hit interval", st["stale_served_total"], st["stale_served_age_ms_max"], age.Milliseconds())
	}
}
