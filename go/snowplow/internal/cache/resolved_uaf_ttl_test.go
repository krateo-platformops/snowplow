// resolved_uaf_ttl_test.go — #118 (d) interim short-TTL falsifiers, store level.
//
// A UAF-bearing resolved cell carries a short TTLOverride so its RBAC-staleness
// window is capped; a non-UAF cell keeps the standard ttl (no collateral
// shortening); the knob defaults to 0 (disabled → no override → today's
// behavior). These are the store-level + env-accessor arms; the C-118-6
// both-Put-sites RED arm lives in the dispatchers package (it needs the two Put
// sites' override derivation).

package cache

import (
	"testing"
	"time"
)

// TestUAFResolvedTTL_DisabledByDefault: the env accessor is 0 (disabled) unless
// set — the override is purely additive, so toggle-off is byte-identical today.
func TestUAFResolvedTTL_DisabledByDefault(t *testing.T) {
	t.Setenv("UAF_RESOLVED_TTL_SECONDS", "")
	if d := UAFResolvedTTL(); d != 0 {
		t.Fatalf("UAFResolvedTTL unset must be 0 (disabled); got %v", d)
	}
	t.Setenv("UAF_RESOLVED_TTL_SECONDS", "0")
	if d := UAFResolvedTTL(); d != 0 {
		t.Fatalf("UAFResolvedTTL=0 must be 0 (disabled); got %v", d)
	}
	t.Setenv("UAF_RESOLVED_TTL_SECONDS", "30")
	if d := UAFResolvedTTL(); d != 30*time.Second {
		t.Fatalf("UAFResolvedTTL=30 must be 30s; got %v", d)
	}
	t.Setenv("UAF_RESOLVED_TTL_SECONDS", "garbage")
	if d := UAFResolvedTTL(); d != 0 {
		t.Fatalf("UAFResolvedTTL invalid must be 0 (disabled); got %v", d)
	}
}

// TestUAFShortTTL_CapsUAFCell_NotNonUAF is the store-level discriminator: with
// the store ttl a long 1h, a UAF cell carrying the short override self-evicts
// within the bound, while a NON-UAF cell (no override) stays a HIT — proving the
// cap is scoped to UAF cells and does not collaterally shorten every cell.
func TestUAFShortTTL_CapsUAFCell_NotNonUAF(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour) // standard ttl = 1h

	// UAF cell: short 1s override, aged 2s → past the override, far inside 1h.
	c.put("k-uaf", &ResolvedEntry{
		RawJSON:     []byte(`{}`),
		CreatedAt:   time.Now().Add(-2 * time.Second),
		TTLOverride: 1 * time.Second,
	})
	// Non-UAF cell: NO override, same age → governed by the 1h standard ttl.
	c.put("k-plain", &ResolvedEntry{
		RawJSON:   []byte(`{}`),
		CreatedAt: time.Now().Add(-2 * time.Second),
	})

	if _, ok := c.Get("k-uaf"); ok {
		t.Fatal("#118(d): a UAF cell with the short override (age 2s > 1s override) must be a MISS — the staleness cap did not fire")
	}
	if _, ok := c.Get("k-plain"); !ok {
		t.Fatal("#118(d): a NON-UAF cell (no override, age 2s, 1h ttl) must still be a HIT — the UAF cap must not collaterally shorten non-UAF cells")
	}
}
