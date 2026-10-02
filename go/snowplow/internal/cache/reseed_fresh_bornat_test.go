package cache

import (
	"context"
	"testing"
	"time"
)

// TestReMintFreshBornAt_258 — the #258/#378 fresh-mint mechanism arms (arch C5,
// marker-free design). ReplaceIfGenReMint is the SOLE freshMint=true carrier; a
// plain refresh ReplaceIfGen inherits BornAt. (The REAL nested-resolve arm — only
// the TARGET cell resets while nested cells Put during the resolve INHERIT — lands
// with the reseed core that drives a real resolve, arch req#3.)
func TestReMintFreshBornAt_258(t *testing.T) {
	store := newResolvedCache(100, 1<<20, time.Hour)
	store.maxEntryAge = 0 // isolate the BornAt mechanism from max-age reaping on Get
	const K = "widgets|w1|u=alice|g=|sg=1"
	past := time.Now().Add(-1 * time.Hour)

	store.Put(K, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), BornAt: past, CreatedAt: time.Now()})
	if got, ok := store.Get(K); !ok || !got.BornAt.Equal(past) {
		t.Fatalf("seed: BornAt=%v ok=%v, want %v", bornAtOf(got), ok, past)
	}

	// ARM refresh-inherits (C5 restriction): a plain gen-guarded ReplaceIfGen
	// INHERITS BornAt — the refresh/keepwarm path can never reset the max-age clock.
	gen := store.CaptureGen(K)
	if !store.ReplaceIfGen(context.Background(), K, &ResolvedEntry{RawJSON: []byte(`{"v":2}`)}, gen) {
		t.Fatal("ReplaceIfGen (refresh) refused unexpectedly")
	}
	if got, _ := store.Get(K); !got.BornAt.Equal(past) {
		t.Fatalf("refresh re-Put must INHERIT BornAt (C5); got %v want %v", got.BornAt, past)
	}

	// ARM target-resets: ReplaceIfGenReMint (the SOLE freshMint carrier) RESETS
	// BornAt to a fresh birth — the #378 same-key re-mint that heals the age without
	// evicting. RED without the freshMint mechanism (putCoreLocked would inherit).
	gen = store.CaptureGen(K)
	if !store.ReplaceIfGenReMint(context.Background(), K, &ResolvedEntry{RawJSON: []byte(`{"v":3}`)}, gen) {
		t.Fatal("ReplaceIfGenReMint refused unexpectedly")
	}
	if got, _ := store.Get(K); !got.BornAt.After(past) {
		t.Fatalf("re-mint must RESET BornAt to fresh; got %v (want > %v)", got.BornAt, past)
	}
}

func bornAtOf(e *ResolvedEntry) time.Time {
	if e == nil {
		return time.Time{}
	}
	return e.BornAt
}
