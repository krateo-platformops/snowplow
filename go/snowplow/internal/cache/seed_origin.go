// seed_origin.go — #841: which seed class created a cell, and whether a
// customer ever read it.
//
// THE QUESTION. hits_seed_attributable answers "did a BOOT seed warm a cell a
// customer then hit", but it reads ResolvedEntry.SeededAtBoot, which a refresher
// re-Put clears and which no post-readyz seed may stamp (#507: it is also half
// of the warm predicate). Bench run 12 read 0 there for the whole run and could
// not say why: the base cohorts never matched a real key, the learned classes
// were never seeded, or the refresher had cleared the flag. SeedOrigin is the
// separate, METADATA-ONLY answer, per seed class:
//
//   - seeded:    cells a seed INSERTED (a seed re-Put of a resident cell is not
//     a new cell and keeps that cell's own origin);
//   - hits:      customer Get hits on a cell a seed inserted;
//   - cells_hit: of those cells, how many were hit at least once.
//
// The origin belongs to the KEY's lineage, like BornAt: it is set by the insert
// and every replace-in-place keeps it (putCoreLocked), so a refresher re-Put of
// a seeded cell still counts the hits the seed bought, and a seed re-Put of a
// cell traffic created does not claim it. It never reaches warmLocked, a key or
// a log line, and it carries no identity: two fixed classes, counts only.
package cache

import (
	"context"
	"expvar"
	"sync"
	"sync/atomic"
)

// The seed classes a seed target belongs to.
const (
	// SeedOriginBase — a binding representative (EnumeratePrewarmTargetsForGVR).
	SeedOriginBase = "base"
	// SeedOriginLearned — a learned identity class (#262).
	SeedOriginLearned = "learned"
)

type seedOriginCounters struct {
	seeded, hits, cellsHit atomic.Uint64
}

var seedOriginStats struct {
	base, learned seedOriginCounters
}

// baseTargets counts the representative targets the learned wrapper
// (dispatchers.enumeratePrewarmTargetsWithLearned) kept because a known
// requester derives their key, and dropped because none does — one count per
// (unit × representative) enumeration, cumulative.
var baseTargets struct {
	kept, unmatched atomic.Uint64
}

// NoteBaseTargets records one enumeration's kept / unmatched representatives.
func NoteBaseTargets(kept, unmatched int) {
	if kept > 0 {
		baseTargets.kept.Add(uint64(kept))
	}
	if unmatched > 0 {
		baseTargets.unmatched.Add(uint64(unmatched))
	}
}

// BaseTargetCounts returns the cumulative kept / unmatched representatives.
func BaseTargetCounts() (kept, unmatched uint64) {
	return baseTargets.kept.Load(), baseTargets.unmatched.Load()
}

// seedOriginCountersFor returns the counters of a known origin, nil otherwise
// (a traffic cell, or an origin this file does not know).
func seedOriginCountersFor(origin string) *seedOriginCounters {
	switch origin {
	case SeedOriginBase:
		return &seedOriginStats.base
	case SeedOriginLearned:
		return &seedOriginStats.learned
	}
	return nil
}

// noteSeedOriginInsertLocked counts a seed-inserted cell. Called by
// putCoreLocked's fresh-insert branch only.
func noteSeedOriginInsertLocked(entry *ResolvedEntry) {
	if entry == nil {
		return
	}
	if c := seedOriginCountersFor(entry.SeedOrigin); c != nil {
		c.seeded.Add(1)
	}
}

// noteSeedOriginHitLocked counts a customer hit on a seed-inserted cell, and
// the cell's first such hit. Called by getCore's customer (touch) hit path
// under c.mu, which also guards item.seedHitSeen.
func noteSeedOriginHitLocked(item *lruItem) {
	if item == nil || item.entry == nil {
		return
	}
	c := seedOriginCountersFor(item.entry.SeedOrigin)
	if c == nil {
		return
	}
	c.hits.Add(1)
	if !item.seedHitSeen {
		item.seedHitSeen = true
		c.cellsHit.Add(1)
	}
}

// SeedOriginCounts is one class's counters.
type SeedOriginCounts struct {
	Seeded, Hits, CellsHit uint64
}

// SeedOriginStats returns the counters of one seed class.
func SeedOriginStats(origin string) SeedOriginCounts {
	c := seedOriginCountersFor(origin)
	if c == nil {
		return SeedOriginCounts{}
	}
	return SeedOriginCounts{Seeded: c.seeded.Load(), Hits: c.hits.Load(), CellsHit: c.cellsHit.Load()}
}

// ResetSeedOriginStatsForTest zeroes the counters.
func ResetSeedOriginStatsForTest() {
	for _, c := range []*seedOriginCounters{&seedOriginStats.base, &seedOriginStats.learned} {
		c.seeded.Store(0)
		c.hits.Store(0)
		c.cellsHit.Store(0)
	}
	baseTargets.kept.Store(0)
	baseTargets.unmatched.Store(0)
}

type seedOriginCtxKey struct{}

// WithSeedOrigin tags a seed resolve's ctx with its target's seed class; the
// seed primitives stamp it on the cell they Put.
func WithSeedOrigin(ctx context.Context, origin string) context.Context {
	return context.WithValue(ctx, seedOriginCtxKey{}, origin)
}

// SeedOriginFromContext is the seed class on ctx, "" off the seed path.
func SeedOriginFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	o, _ := ctx.Value(seedOriginCtxKey{}).(string)
	return o
}

// init publishes the surface only when the cache is on (CFG-1).
//
//	snowplow_prewarm_seed_origin — {"base"|"learned": {seeded, hits, cells_hit}}
//	snowplow_prewarm_base_targets — {kept, unmatched} representatives enumerated
func init() {
	if Disabled() {
		return
	}
	RegisterSeedOriginExpvar()
}

var seedOriginExpvarOnce sync.Once

// RegisterSeedOriginExpvar publishes snowplow_prewarm_seed_origin. Idempotent.
func RegisterSeedOriginExpvar() {
	seedOriginExpvarOnce.Do(func() {
		expvar.Publish("snowplow_prewarm_seed_origin", expvar.Func(func() any {
			out := make(map[string]map[string]uint64, 2)
			for _, o := range []string{SeedOriginBase, SeedOriginLearned} {
				s := SeedOriginStats(o)
				out[o] = map[string]uint64{"seeded": s.Seeded, "hits": s.Hits, "cells_hit": s.CellsHit}
			}
			return out
		}))
		expvar.Publish("snowplow_prewarm_base_targets", expvar.Func(func() any {
			kept, unmatched := BaseTargetCounts()
			return map[string]uint64{"kept": kept, "unmatched": unmatched}
		}))
	})
}
