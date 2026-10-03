package cache

import (
	"context"
	"testing"
	"time"
)

// #444 — the recent-hitter pool: bounded, LRU, deduplicated, alloc-free on a
// repeat front hit, carried across a replace-in-place, and the guarded eviction
// spares a body a customer re-Put after the refresher's decision.

func TestRecentHitters_BoundedLRUDedup(t *testing.T) {
	e := &ResolvedEntry{}
	for _, u := range []string{"a", "b", "c", "d", "e"} {
		e.NoteHitter(u, []string{"g"})
	}
	got := e.RecentHitters()
	if len(got) != recentHittersCap {
		t.Fatalf("pool must be capped at %d, got %d", recentHittersCap, len(got))
	}
	if got[0].Username != "e" || got[len(got)-1].Username != "b" {
		t.Fatalf("most recent first, oldest dropped: %+v", got)
	}
	e.NoteHitter("c", []string{"g"}) // re-hit moves to front, no duplicate
	got = e.RecentHitters()
	if got[0].Username != "c" || len(got) != recentHittersCap {
		t.Fatalf("re-hit must move to front without duplicating: %+v", got)
	}
	seen := map[string]bool{}
	for _, h := range got {
		if seen[h.Username] {
			t.Fatalf("duplicate identity in pool: %+v", got)
		}
		seen[h.Username] = true
	}
	// Same username, different groups = a different identity tuple.
	e.NoteHitter("c", []string{"other"})
	if got = e.RecentHitters(); got[0].Groups[0] != "other" || got[1].Username != "c" {
		t.Fatalf("groups are part of the identity tuple: %+v", got)
	}
}

func TestRecentHitters_RepeatFrontHitAllocatesNothing(t *testing.T) {
	e := &ResolvedEntry{}
	g := []string{"portal"}
	e.NoteHitter("alice", g)
	if n := testing.AllocsPerRun(100, func() { e.NoteHitter("alice", g) }); n != 0 {
		t.Fatalf("a repeat hit by the front identity must not allocate (hot hit path); allocs=%v", n)
	}
}

func TestRecentHitters_CarriedAcrossReplaceInPlace(t *testing.T) {
	c := newResolvedCache(64, 1<<20, time.Hour)
	const key = "k444"
	first := &ResolvedEntry{RawJSON: []byte(`1`)}
	c.Put(key, first)
	first.NoteHitter("dave", []string{"portal"})
	gen := c.CaptureGen(key)
	if !c.ReplaceIfGen(context.Background(), key, &ResolvedEntry{RawJSON: []byte(`2`)}, gen) {
		t.Fatalf("replace refused")
	}
	got, _ := c.Get(key)
	if h := got.RecentHitters(); len(h) != 1 || h[0].Username != "dave" {
		t.Fatalf("a refresh re-Put must keep the cell's hitter pool; got %+v", h)
	}
}

func TestEvictUnrefreshable_SparesALaterPut(t *testing.T) {
	c := newResolvedCache(64, 1<<20, time.Hour)
	const key = "k444e"
	decided := &ResolvedEntry{RawJSON: []byte(`old`)}
	c.Put(key, decided)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`customer-refill`)}) // lands after the decision
	if c.EvictUnrefreshable(key, decided) {
		t.Fatalf("must not evict a body put after the refresher's decision")
	}
	if got, ok := c.Get(key); !ok || string(got.RawJSON) != "customer-refill" {
		t.Fatalf("the later Put must survive")
	}
	cur, _ := c.GetNoTouch(key)
	if !c.EvictUnrefreshable(key, cur) {
		t.Fatalf("the decided entry must be evicted")
	}
	if _, ok := c.GetNoTouch(key); ok {
		t.Fatalf("entry still resident")
	}
	if c.Stats().EvictNoRepresentativeTotal != 1 || c.Stats().EvictDeleteTotal != 0 {
		t.Fatalf("counted on evict_no_representative_total only (never the informer-DELETE discriminator): %+v", c.Stats())
	}
}
