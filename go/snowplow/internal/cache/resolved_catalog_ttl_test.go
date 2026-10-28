// resolved_catalog_ttl_test.go — R1 Layer 2 (#36) falsifiers for the
// bounded CATALOG_UNSERVABLE_TTL_SECONDS backstop.
//
// The invariant: an entry stamped with a short per-entry TTLOverride
// (set when its GVR was not servable at Put time) MUST self-evict within
// that bound even when the store's standard ttl is long — the
// bounded-staleness floor that caps a missed dirty-mark. A healthy entry
// (no override) keeps the standard ttl. The override can only TIGHTEN the
// bound, never extend past the standard ttl.

package cache

import (
	"testing"
	"time"
)

// TestTTLOverride_ShortBackstopEvictsBeforeStandardTTL is the discriminating
// control: the store ttl is a long 1h, but the entry carries a 1s override
// → after the override window the entry is a MISS, while a sibling without
// an override is still a HIT.
func TestTTLOverride_ShortBackstopEvictsBeforeStandardTTL(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour) // standard ttl = 1h

	// Degraded entry: stored "while not servable" → short 1s override,
	// CreatedAt backdated 2s so it is already past the override but FAR
	// inside the 1h standard ttl.
	c.put("k-degraded", &ResolvedEntry{
		RawJSON:     []byte(`{}`),
		CreatedAt:   time.Now().Add(-2 * time.Second),
		TTLOverride: 1 * time.Second,
	})
	// Healthy entry: no override, same backdate → governed by the 1h ttl.
	c.put("k-healthy", &ResolvedEntry{
		RawJSON:   []byte(`{}`),
		CreatedAt: time.Now().Add(-2 * time.Second),
	})

	if _, ok := c.Get("k-degraded"); ok {
		t.Fatalf("backstop FAIL: the degraded entry (1s override, age 2s) must be evicted as a MISS, "+
			"not served — the bounded-staleness floor did not fire")
	}
	if _, ok := c.Get("k-healthy"); !ok {
		t.Fatalf("the healthy entry (no override, age 2s, 1h ttl) must still be a HIT — the override "+
			"must not affect entries that don't carry it")
	}
}

// TestTTLOverride_HonouredWithinWindow: a degraded entry is still a HIT
// inside its override window (the backstop bounds staleness, it does not
// evict eagerly).
func TestTTLOverride_HonouredWithinWindow(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.put("k", &ResolvedEntry{
		RawJSON:     []byte(`{}`),
		CreatedAt:   time.Now(), // fresh
		TTLOverride: 1 * time.Hour,
	})
	if _, ok := c.Get("k"); !ok {
		t.Fatalf("a fresh entry within its override window must be a HIT")
	}
}

// TestEffectiveTTL_OverrideOnlyTightens: the override can only shorten the
// bound — an override LONGER than the store ttl must not extend the entry's
// life past the standard ttl.
func TestEffectiveTTL_OverrideOnlyTightens(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour) // standard 1h

	// Override of 10h (longer than the 1h standard) on an entry aged 2h:
	// the entry is past the 1h standard ttl, so effectiveTTL must clamp to
	// the standard ttl and evict it — the override must NOT keep it alive.
	c.put("k", &ResolvedEntry{
		RawJSON:     []byte(`{}`),
		CreatedAt:   time.Now().Add(-2 * time.Hour),
		TTLOverride: 10 * time.Hour,
	})
	if _, ok := c.Get("k"); ok {
		t.Fatalf("an override LONGER than the standard ttl must not extend life past it — "+
			"the entry aged 2h (>1h standard) must be evicted")
	}

	// Unit-level: effectiveTTLLocked returns the SHORTER of the two.
	c.mu.Lock()
	defer c.mu.Unlock()
	got := c.effectiveTTLLocked(&ResolvedEntry{TTLOverride: 10 * time.Hour})
	if got != time.Hour {
		t.Fatalf("effectiveTTL with a longer override = %v, want the standard 1h (override only tightens)", got)
	}
	got = c.effectiveTTLLocked(&ResolvedEntry{TTLOverride: 30 * time.Second})
	if got != 30*time.Second {
		t.Fatalf("effectiveTTL with a shorter override = %v, want 30s", got)
	}
	got = c.effectiveTTLLocked(&ResolvedEntry{}) // no override
	if got != time.Hour {
		t.Fatalf("effectiveTTL with no override = %v, want the standard 1h", got)
	}
}

// TestCatalogUnservableTTL_DisabledByDefault: the env accessor returns 0
// (disabled) unless set, so the override is purely additive.
func TestCatalogUnservableTTL_DisabledByDefault(t *testing.T) {
	t.Setenv("CATALOG_UNSERVABLE_TTL_SECONDS", "")
	if d := CatalogUnservableTTL(); d != 0 {
		t.Fatalf("CatalogUnservableTTL unset must be 0 (disabled); got %v", d)
	}
	t.Setenv("CATALOG_UNSERVABLE_TTL_SECONDS", "0")
	if d := CatalogUnservableTTL(); d != 0 {
		t.Fatalf("CatalogUnservableTTL=0 must be 0 (disabled); got %v", d)
	}
	t.Setenv("CATALOG_UNSERVABLE_TTL_SECONDS", "45")
	if d := CatalogUnservableTTL(); d != 45*time.Second {
		t.Fatalf("CatalogUnservableTTL=45 must be 45s; got %v", d)
	}
	t.Setenv("CATALOG_UNSERVABLE_TTL_SECONDS", "garbage")
	if d := CatalogUnservableTTL(); d != 0 {
		t.Fatalf("CatalogUnservableTTL invalid must be 0 (disabled); got %v", d)
	}
}
