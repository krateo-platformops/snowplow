// resolved_max_entry_age_test.go — 1.12.6 C5 (design §7) falsifiers: the
// bounded entry LIFETIME (BornAt + RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS).
//
// THE DEFECT. The store's TTL is measured from CreatedAt, which every Put
// resets — a refresh re-Put included. An entry the refresher (or the keep-warm
// sweep) keeps re-Putting therefore never expires, so a wrong refresh (a stale
// body, a missed dep edge) is served for the life of the pod. Nothing in the
// event pipeline can catch a mistake the pipeline itself made; a lifetime
// bound measured from the FIRST Put can.
//
// ARMS
//
//	G1   (RED on main, no new symbols) — through the production singleton with
//	     the knob at "1": a key re-Put every 400 ms (each re-Put fresh, so the
//	     TTL never fires) is still evicted on the first Get after 1 s. On main
//	     the entry lives forever.
//	G1c  the mechanism: BornAt is inherited across a replace-in-place Put, the
//	     eviction counts on evict_max_age_total (not evict_ttl_total), and the
//	     dep edges go with the entry.
//	G2   (control) knob "0" → an entry born 25 h ago is served; a fresh-birth
//	     entry is served under the default.
//	G3   BornAt on a FIRST Put: zero → now; non-zero → honoured.
package cache

import (
	"testing"
	"time"
)

func TestMaxEntryAge_G1_RePutsDoNotExtendTheLifetime(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "1")
	ResetResolvedCacheForTest()
	t.Cleanup(ResetResolvedCacheForTest)
	c := ResolvedCache()
	if c == nil {
		t.Fatal("resolved cache disabled")
	}
	inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "g1"}
	key := ComputeKey(inputs)

	born := time.Now()
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":0}`), Inputs: &inputs})
	// Re-Put every 400 ms for ~1.3 s: three re-Puts, each with a fresh
	// CreatedAt, exactly what a dirty-mark → refresh → re-Put cycle does.
	for i := 1; i <= 3; i++ {
		time.Sleep(400 * time.Millisecond)
		c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &inputs})
		if time.Since(born) < time.Second {
			if _, ok := c.Get(key); !ok {
				t.Fatalf("G1: entry evicted at %v, BEFORE the 1 s max age", time.Since(born))
			}
		}
	}
	if time.Since(born) < 1100*time.Millisecond {
		time.Sleep(1100*time.Millisecond - time.Since(born))
	}
	if _, ok := c.Get(key); ok {
		t.Fatalf("G1 RED: entry still served %v after its first Put despite RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS=1 — "+
			"a re-Put resets the TTL, so a wrongly-refreshed cell lives for the life of the pod", time.Since(born))
	}
}

func TestMaxEntryAge_G1c_BornAtInheritedAndCountedSeparately(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.maxEntryAge = time.Hour
	Deps().SetStore(c)
	t.Cleanup(func() { ResetDepsForTest() })
	inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "g1c"}
	key := ComputeKey(inputs)

	born := time.Now().Add(-2 * time.Hour)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":0}`), Inputs: &inputs, BornAt: born})
	// Bypass Get (which would evict): a replace-in-place Put must INHERIT
	// the birth even though the new entry's own BornAt is zero and its
	// CreatedAt is fresh.
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &inputs})
	c.mu.Lock()
	el := c.index[key]
	got := el.Value.(*lruItem).entry
	c.mu.Unlock()
	if !got.BornAt.Equal(born) {
		t.Fatalf("G1c: BornAt after re-Put = %v, want the inherited %v (a re-Put reset the lifetime clock)", got.BornAt, born)
	}
	if time.Since(got.CreatedAt) > time.Second {
		t.Fatalf("G1c: CreatedAt was not refreshed by the re-Put — the TTL must still be per-Put")
	}
	if _, ok := c.Get(key); ok {
		t.Fatalf("G1c: entry born 2 h ago served under a 1 h max age")
	}
	st := c.Stats()
	if st.EvictMaxAgeTotal != 1 || st.EvictTTLTotal != 0 {
		t.Fatalf("G1c: evict_max_age_total=%d evict_ttl_total=%d, want 1/0 — the two reasons must stay distinguishable", st.EvictMaxAgeTotal, st.EvictTTLTotal)
	}
	if st.Entries != 0 {
		t.Fatalf("G1c: %d entries resident after the max-age eviction", st.Entries)
	}
}

func TestMaxEntryAge_G2_ZeroDisablesAndFreshEntriesServe(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "0")
	if got := maxEntryAgeFromEnv(); got != 0 {
		t.Fatalf("G2: knob \"0\" → %v, want 0 (disabled, preserved — not the default)", got)
	}
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "-5")
	if got := maxEntryAgeFromEnv(); got != 86400*time.Second {
		t.Fatalf("G2: negative → %v, want the 86400 s default", got)
	}
	t.Setenv("RESOLVED_CACHE_MAX_ENTRY_AGE_SECONDS", "")
	if got := maxEntryAgeFromEnv(); got != 86400*time.Second {
		t.Fatalf("G2: unset → %v, want the 86400 s default", got)
	}

	c := newResolvedCache(10, 1<<20, time.Hour)
	c.maxEntryAge = 0
	inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "g2"}
	key := ComputeKey(inputs)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &inputs, BornAt: time.Now().Add(-25 * time.Hour)})
	if _, ok := c.Get(key); !ok {
		t.Fatalf("G2: with the bound disabled an old-born entry must still be served")
	}

	d := newResolvedCache(10, 1<<20, time.Hour) // default 24 h bound
	if d.maxEntryAge != 86400*time.Second {
		t.Fatalf("G2: constructor default = %v, want 24 h", d.maxEntryAge)
	}
	d.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &inputs})
	if _, ok := d.Get(key); !ok {
		t.Fatalf("G2: a fresh-born entry must be served under the default bound")
	}
	if d.Stats().EvictMaxAgeTotal != 0 {
		t.Fatalf("G2: evict_max_age_total moved for a fresh entry")
	}
}

func TestMaxEntryAge_G3_FirstPutBirth(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	inputs := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "g3"}
	key := ComputeKey(inputs)
	before := time.Now()
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &inputs})
	e, _ := c.Get(key)
	if e.BornAt.IsZero() || e.BornAt.Before(before) || !e.BornAt.Equal(e.CreatedAt) {
		t.Fatalf("G3: first Put with zero BornAt → %v (CreatedAt %v), want BornAt == CreatedAt == now", e.BornAt, e.CreatedAt)
	}
	other := ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "g3b"}
	okey := ComputeKey(other)
	explicit := time.Now().Add(-10 * time.Minute)
	c.Put(okey, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &other, BornAt: explicit})
	oe, _ := c.Get(okey)
	if !oe.BornAt.Equal(explicit) {
		t.Fatalf("G3: explicit BornAt on a first Put not honoured: %v", oe.BornAt)
	}
}
