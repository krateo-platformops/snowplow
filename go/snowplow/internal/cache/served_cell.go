// served_cell.go — #643 PR-B: which cached L1 cells a resolve SERVED its
// inputs from, and whether they are still the same cells.
//
// WHY. A declined-external mark records the facts its resolve decided
// external-ness from (declined_external_engine.go). One of those facts can be a
// CACHED input: an apistage content cell (a LIST served from L1) or a RAFullList
// cell. Such a cell can be dirty-marked and not yet refreshed — stale-while-
// revalidate — so the resolve decided from a stale body. Its deps moved BEFORE
// the resolve began, so the deps-moved rule (d4) cannot see it; and when the
// refresher later re-Puts the cell, no dep event fires. Without this, the mark
// would hold forever over a cell that has since become cacheable (I-1).
//
// THE RULE. Every L1 body read on the resolve path goes through ServeCell, which
// notes (key, putSeq, covered deps) on the resolve's dep-gen sink. The mark keeps
// the notes; it retires when any served cell is absent or carries another
// putSeq (re-Put, or evicted and refilled). The deps a served cell COVERS are
// judged by the cell, not by d4: while the cell is unchanged a re-resolve would
// read the same stale body and re-dial, so the mark holds (no re-dial loop
// while stale); the refresh's re-Put retires it.
//
// putSeq, not CreatedAt: a store-wide monotonic sequence stamped on the lruItem
// by every accepted Put (putCoreLocked). An identical-bytes re-Put also takes a
// new putSeq — one extra resolve, accepted.
//
// NOT COVERED, deliberately: the SeedResolveMemo. It is per-seed-pass, and a hit
// is already rejected when its captured deps moved (#411), so a memo body never
// outlives the pass that produced it.
//
// GUARD: scripts/checkresolvedentrysites class 3 fails CI on any direct
// (*ResolvedCacheStore).Get / .GetNoTouch reference under internal/resolvers that
// is neither routed through ServeCell nor waived as a non-serving probe.

package cache

import (
	"context"
	"sync/atomic"
	"time"
)

// storePutSeq issues L1 Put identities. Never reused.
var storePutSeq atomic.Uint64

// ServedCell is one L1 cell a resolve served a body from.
type ServedCell struct {
	Key    string
	PutSeq uint64
	// Covers are the deps whose current state this cell's body stands for.
	Covers []DepKey
}

// PutSeqOf returns key's current Put identity, or ok=false when the key holds
// no LIVE entry — absent, or past its effective TTL or max entry age by the
// SAME predicate the read path applies (itemExpiredLocked). Expiry is lazy, so
// an expired cell nobody read is still indexed: it must read as gone here or a
// mark served from it would outlive it. A pure read: no eviction, no recency,
// no hit accounting.
func (c *ResolvedCacheStore) PutSeqOf(key string) (uint64, bool) {
	if c == nil || key == "" {
		return 0, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.index[key]
	if !ok {
		return 0, false
	}
	item := el.Value.(*lruItem)
	if pastTTL, pastMaxAge := c.itemExpiredLocked(item, time.Now()); pastTTL || pastMaxAge {
		return 0, false
	}
	return item.putSeq, true
}

// itemExpiredLocked is THE liveness predicate for an indexed cell: past its
// effective TTL (per-entry TTLOverride, else the store TTL) measured from
// CreatedAt, or past the max entry age measured from BornAt. Used by getCore
// (which then evicts) and PutSeqOf (which only reports). Caller holds c.mu.
func (c *ResolvedCacheStore) itemExpiredLocked(item *lruItem, now time.Time) (pastTTL, pastMaxAge bool) {
	if eff := c.effectiveTTLLocked(item.entry); eff > 0 && now.Sub(item.entry.CreatedAt) > eff {
		pastTTL = true
	}
	if c.maxEntryAge > 0 && !item.entry.BornAt.IsZero() && now.Sub(item.entry.BornAt) > c.maxEntryAge {
		pastMaxAge = true
	}
	return pastTTL, pastMaxAge
}

// ServeCell is THE L1 body read for the resolve path (#643). It reads the
// cell — touch=true is the customer Get (recency + hit accounting),
// touch=false the internal GetNoTouch — and, on a hit, notes the cell on ctx's
// dep-gen sink with the Put identity it was read at and the deps it covers.
//
// The identity is read BEFORE the body: a re-Put landing between the two leaves
// a note older than the body, which can only retire a mark early (safe), never
// hold one over a body it did not see.
func ServeCell(ctx context.Context, c *ResolvedCacheStore, key string, touch bool, covers ...DepKey) (*ResolvedEntry, bool) {
	if c == nil {
		return nil, false
	}
	seq, live := c.PutSeqOf(key)
	var entry *ResolvedEntry
	var hit bool
	if touch {
		entry, hit = c.Get(key)
	} else {
		entry, hit = c.GetNoTouch(key)
	}
	if hit && entry != nil && live {
		NoteServedCell(ctx, key, seq, covers...)
	}
	return entry, hit
}

// NoteServedCell appends one served cell to ctx's dep-gen sink and its
// ancestors (whose bodies embed this one). No sink → no-op.
func NoteServedCell(ctx context.Context, key string, putSeq uint64, covers ...DepKey) {
	if key == "" || Inert(ctx) {
		return
	}
	sc := ServedCell{Key: key, PutSeq: putSeq, Covers: append([]DepKey(nil), covers...)}
	for s := depGenSinkFromContext(ctx); s != nil; s = s.parent {
		s.mu.Lock()
		s.served = append(s.served, sc)
		s.mu.Unlock()
	}
}

// ServedCellsFromContext returns a copy of the cells ctx's resolve served.
func ServedCellsFromContext(ctx context.Context) []ServedCell {
	s := depGenSinkFromContext(ctx)
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ServedCell(nil), s.served...)
}

// ServedCellsChanged reports whether any served cell is no longer the cell that
// was read: absent from the store, or at another Put identity.
func ServedCellsChanged(served []ServedCell) bool {
	if len(served) == 0 {
		return false
	}
	c := ResolvedCache()
	for _, sc := range served {
		seq, ok := c.PutSeqOf(sc.Key)
		if !ok || seq != sc.PutSeq {
			return true
		}
	}
	return false
}

// DepsMovedSinceUncovered is d4 for the deps NOT covered by a served cell: those
// are judged by ServedCellsChanged instead. A list dep (Name "") is covered by a
// cell covering the same GVR + namespace.
func DepsMovedSinceUncovered(deps []DepKey, stamp uint64, served []ServedCell) bool {
	if len(served) == 0 {
		return DepsMovedSince(deps, stamp)
	}
	covered := map[DepKey]struct{}{}
	for _, sc := range served {
		for _, dk := range sc.Covers {
			covered[dk] = struct{}{}
		}
	}
	var uncovered []DepKey
	for _, dk := range deps {
		if _, ok := covered[dk]; ok {
			continue
		}
		uncovered = append(uncovered, dk)
	}
	return DepsMovedSince(uncovered, stamp)
}
