// seeded_at_boot_test.go — coverage-audit M11.
//
// ResolvedEntry.SeededAtBoot (resolved.go) is the #130 F3 boot-seed provenance
// marker: true iff the entry was written by the Phase-1 boot seed, false once
// real traffic (a refresher/dispatch re-Put) overwrites the cell. The store's
// replace-in-place path (Put, resolved.go) rebinds old.entry = entry, so the
// NEW entry's SeededAtBoot must win — a traffic re-Put correctly re-classifies a
// seed-warmed cell to traffic-warmed.
//
// RED PROOF: an impl that PRESERVES the old entry's SeededAtBoot across a
// replace-in-place (mutating fields but keeping the prior provenance) would keep
// reporting true after the traffic re-Put — proven by the shadow store
// putPreservingSeededAtBoot below.
package cache

import (
	"testing"
	"time"
)

// TestSeededAtBoot_RoundTripsThroughGet is M11: a boot-seeded entry reports
// SeededAtBoot==true on Get; a same-key traffic re-Put flips it to false.
func TestSeededAtBoot_RoundTripsThroughGet(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)

	// Boot seed Put: SeededAtBoot=true.
	c.Put("k", &ResolvedEntry{
		RawJSON:      []byte(`{"seed":true}`),
		SeededAtBoot: true,
		Inputs:       &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassWidgets},
	})
	got, ok := c.Get("k")
	if !ok {
		t.Fatalf("Get after boot-seed Put should hit")
	}
	if !got.SeededAtBoot {
		t.Fatalf("a boot-seeded entry must report SeededAtBoot==true")
	}

	// Traffic re-Put of the SAME key: SeededAtBoot=false (the natural zero).
	c.Put("k", &ResolvedEntry{
		RawJSON:      []byte(`{"traffic":true}`),
		SeededAtBoot: false,
		Inputs:       &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassWidgets},
	})
	got, ok = c.Get("k")
	if !ok {
		t.Fatalf("Get after traffic re-Put should hit")
	}
	if got.SeededAtBoot {
		t.Fatalf("a traffic re-Put must re-classify the cell: SeededAtBoot expected false, got true")
	}
	// The content must also be the NEW entry's (proves it's a genuine replace,
	// not the marker read off a stale entry).
	if string(got.RawJSON) != `{"traffic":true}` {
		t.Fatalf("replace-in-place must serve the new RawJSON, got %q", got.RawJSON)
	}
}

// --- RED-arm proof ----------------------------------------------------------

// putPreservingSeededAtBoot models the WRONG replace-in-place: it copies the new
// entry's content but PRESERVES the prior entry's SeededAtBoot provenance. It
// operates on the real store via Get to read the current provenance, then Puts a
// merged entry — mirroring exactly the defect (a re-Put that forgets to reset
// the boot marker).
func putPreservingSeededAtBoot(c *ResolvedCacheStore, key string, entry *ResolvedEntry) {
	if prior, ok := c.Get(key); ok {
		// Field copy, not `*entry` (ResolvedEntry holds an atomic since #444).
		merged := &ResolvedEntry{RawJSON: entry.RawJSON, Inputs: entry.Inputs, Pinned: entry.Pinned}
		merged.SeededAtBoot = prior.SeededAtBoot // the defect: carry over old provenance
		c.Put(key, merged)
		return
	}
	c.Put(key, entry)
}

// TestSeededAtBoot_PreserveOnReplace_RedArm proves the M11 GREEN arm
// discriminates: the preserve-provenance wrong impl keeps reporting true after a
// traffic re-Put — the exact behavior the GREEN arm forbids.
func TestSeededAtBoot_PreserveOnReplace_RedArm(t *testing.T) {
	c := newResolvedCache(10, 1<<20, time.Hour)
	c.Put("k", &ResolvedEntry{
		RawJSON:      []byte(`{"seed":true}`),
		SeededAtBoot: true,
		Inputs:       &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassWidgets},
	})
	// Traffic re-Put through the WRONG path.
	putPreservingSeededAtBoot(c, "k", &ResolvedEntry{
		RawJSON:      []byte(`{"traffic":true}`),
		SeededAtBoot: false,
		Inputs:       &ResolvedKeyInputs{CacheEntryClass: CacheEntryClassWidgets},
	})
	got, ok := c.Get("k")
	if !ok {
		t.Fatalf("entry must still be present")
	}
	if !got.SeededAtBoot {
		t.Fatalf("RED-arm sanity: the preserve-provenance wrong impl SHOULD still report " +
			"SeededAtBoot==true after a traffic re-Put; the M11 test is not discriminating otherwise")
	}
}
