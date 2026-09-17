// issue1127_obs_lifetime_test.go — 1.12.7 observability: the inspector must
// report LifetimeSeconds (now − BornAt), the value the max-age bound acts on.
//
// WHY IT IS NOT ageSeconds. ageSeconds measures the BODY, from CreatedAt, and
// every refresh re-Put resets it. BornAt is the age of the KEY and a re-Put
// inherits it. A cell the refresher keeps warm therefore reports a small
// ageSeconds forever however long it has been resident, which is precisely the
// case where an operator needs to know how close it is to the max-age bound —
// and that bound is enforced ON READ, so an unread entry sits past it
// indefinitely without breaching anything.
package cache

import (
	"testing"
	"time"
)

// TestIssue1127Obs_LifetimeSeconds_SurvivesARePutThatResetsAge — the
// discriminating case. A re-Put must reset ageSeconds and must NOT reset
// lifetimeSeconds; if both move together the new field carries no information
// the old one did not.
func TestIssue1127Obs_LifetimeSeconds_SurvivesARePut(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	store := newResolvedCache(100, 1<<20, time.Hour)
	const key = "L1_obs-lifetime"

	born := time.Now().Add(-90 * time.Minute)
	store.Put(key, &ResolvedEntry{
		RawJSON:   []byte(`{}`),
		Inputs:    &ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "lifetime-flex"},
		CreatedAt: born,
		BornAt:    born,
	})

	// A refresh re-Put: a NEW body, so CreatedAt is now; BornAt is inherited.
	store.Put(key, &ResolvedEntry{
		RawJSON: []byte(`{"refreshed":true}`),
		Inputs:  &ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "lifetime-flex"},
	})

	meta, ok := store.MetadataForKey(key)
	if !ok {
		t.Fatalf("premise: the entry is not resident after the re-Put")
	}

	// The body is new, so its age is ~0.
	if meta.AgeSeconds > 60 {
		t.Fatalf("premise: ageSeconds=%d after a re-Put; it should measure the NEW body", meta.AgeSeconds)
	}
	// The KEY is 90 minutes old and the re-Put must not have reset that.
	if meta.LifetimeSeconds < 80*60 {
		t.Fatalf("RED (1.12.7 obs): lifetimeSeconds=%d after a re-Put of a 90-minute-old key, "+
			"want ≥ %d. If a re-Put resets it, the field tracks the body like ageSeconds and "+
			"cannot answer how close the entry is to the max-age bound — which is the one "+
			"question it was added for, because a keep-warm cell reports a small age forever",
			meta.LifetimeSeconds, 80*60)
	}
	if meta.LifetimeSeconds <= meta.AgeSeconds {
		t.Fatalf("RED (1.12.7 obs): lifetimeSeconds=%d is not greater than ageSeconds=%d on a "+
			"re-Put entry; the two fields are not measuring different things",
			meta.LifetimeSeconds, meta.AgeSeconds)
	}
}

// TestIssue1127Obs_LifetimeSeconds_AppearsOnTheFullWalkToo — the single-key
// lookup and the walk must agree; a field populated on only one path is a trap
// for whoever reads the wrong one.
func TestIssue1127Obs_LifetimeSeconds_AppearsOnTheFullWalk(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	store := newResolvedCache(100, 1<<20, time.Hour)
	born := time.Now().Add(-45 * time.Minute)
	store.Put("L1_walk-lifetime", &ResolvedEntry{
		RawJSON:   []byte(`{}`),
		Inputs:    &ResolvedKeyInputs{CacheEntryClass: "widgets", Name: "walk-flex"},
		CreatedAt: born,
		BornAt:    born,
	})

	seen := 0
	store.RangeMetadata(func(m ResolvedEntryMeta) bool {
		if m.KeyHash != "L1_walk-lifetime" {
			return true
		}
		seen++
		if m.LifetimeSeconds < 40*60 {
			t.Errorf("RED (1.12.7 obs): the full walk reports lifetimeSeconds=%d for a "+
				"45-minute-old key, want ≥ %d — the field is populated on the single-key lookup "+
				"but not here", m.LifetimeSeconds, 40*60)
		}
		return true
	})
	if seen != 1 {
		t.Fatalf("premise: the walk did not visit the entry (seen=%d)", seen)
	}
}

// TestIssue1127Obs_LifetimeSeconds_ZeroWhenBornAtUnset — an entry written
// before the field existed must report 0, not an epoch-sized number that would
// read as decades past the bound.
func TestIssue1127Obs_LifetimeSeconds_ZeroWhenBornAtUnset(t *testing.T) {
	if got := lifetimeSecondsOf(&ResolvedEntry{}, time.Now()); got != 0 {
		t.Fatalf("RED (1.12.7 obs): an entry with an unset BornAt reports lifetimeSeconds=%d, "+
			"want 0 — an epoch-derived value would read as decades past the max-age bound", got)
	}
	if got := lifetimeSecondsOf(nil, time.Now()); got != 0 {
		t.Fatalf("1.12.7 obs: a nil entry reports lifetimeSeconds=%d, want 0", got)
	}
}
