// refresh_resolve_memo.go — #598: the refresher stops re-parsing the same
// RESTAction once per widget.
//
// THE DEFECT, MEASURED ON 057/1.12.43. One object event dirty-marks EVERY cell
// whose dep set names that coordinate, and the refresher then re-resolves each
// one independently. On the hottest coordinate
// (observability.krateo.io/v1alpha1 incidents, 33.5% of all dirty marks) that
// is 75 widgets, and 44 of them are backed by the SAME RESTAction
// `incident-detail` with IDENTICAL keyExtras ["name","namespace"]. So one
// incident change produces 44 resolves of one RESTAction, per identity class,
// each paying its own parseListEnvelope — the irreducible per-resolve cost
// #578 established. Fleet-wide: 368 widgets carrying an apiRef resolve through
// 101 distinct (RA, keyExtras) pairs = 3.64x duplication, with the largest
// collapse sitting exactly on the hottest coordinate.
//
// THIS IS NOT A NEW MECHANISM. It is cache.SeedResolveMemo (#130 F4), written
// against the identical shape on the boot seed — "~71 statistics/tag widgets
// whose apiRef points at one of a SMALL set of heavy RESTActions … collapses to
// (#distinct RA) × (#cohorts) × (#distinct pages)". The seed got that memo; the
// refresher never did. All this file does is install the existing memo on the
// refresher's resolve context and wire it to the dep tracker.
//
// WHY A PROCESS-HELD MEMO IS SAFE HERE, where F4 deliberately refused one.
// F4's objection was teardown: a process-global cache "can never serve a stale
// RA body to a later seed pass or (critically) to a user /call". Both halves
// still hold, by different means:
//
//   - A USER /call NEVER SEES IT. The memo is context-carried and installed at
//     exactly one site, resolveAndPopulateL1 (the refresher's re-resolve). A
//     /call context has no memo, so SeedResolveMemoFromContext returns nil and
//     apiref.Resolve's lookup is a strict no-op there. That is unchanged and is
//     the load-bearing half.
//   - A STALE BODY IS ALREADY REFUSED. #411 made a memo hit conditional on none
//     of the entry's captured deps having moved past the entry's Stamp
//     (DepTracker.depMovedSince). A refresh is triggered BY a dep moving, so the
//     first cell of a burst necessarily misses, resolves fresh, and Stores an
//     entry stamped after the move; its siblings then hit that fresh entry.
//     Freshness is therefore a property of the memo's existing guard, not of how
//     long this file keeps it alive.
//
// #883 F4 — REFUSED IS NOT ENOUGH: A STALE BODY IS DELETED. The #411 guard
// refused a stale entry but left it in the memo, so every body whose key was
// never looked up again stayed resident until the wholesale swap: bench run 15
// read it as a ~1 GB/h rising floor (DeepCopyJSON 12 -> 774 MB). The memo now
// drops an entry when a Load finds it stale, when the dep tracker moves one of
// its deps, and when the resolved cache has removed every L1 key it served
// (cache.TrackDepMoves, wired below); it stores the RESTAction's encoded result
// rather than a decoded deep copy. With those signals the #598 wholesale swap
// is gone. Its size is published as snowplow_resolve_memo (entries, body_bytes).
//
// WHY THE COLLAPSE ACTUALLY LANDS. DepTracker.OnObjectEvent collects every
// matching key for one object event FIRST and then enqueues them in a single
// loop, and the refresher's queue is a client-go workqueue, which is FIFO. So a
// coordinate's whole fan-out enters the queue back-to-back and drains as one
// contiguous window. The duplicate resolves are adjacent in time by
// construction — this file does not have to create that adjacency, and no queue
// change is needed. Rate-limited re-adds after a failed resolve, and keys
// re-dirtied while processing, land outside their burst; those are a minority
// and mean the realised collapse sits below the structural 3.64x.
//
// IDENTITY NEVER COLLAPSES. The memo key folds username + sorted groups + RBAC
// class because the bodies are RBAC-filtered; dropping it is a cross-user leak
// with a RED test already guarding it (F4's divergent-output memo test). The
// saving here is the WIDGET dimension only.

package dispatchers

import (
	"expvar"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// refreshMemo holds the one memo installed on refresher resolves for the life of
// the process (#883: never swapped; see refreshResolveMemo).
var refreshMemo atomic.Pointer[cache.SeedResolveMemo]

// RefreshMemoStats returns the live memo's hit/miss counters, or (0,0) when no
// memo has been created yet.
func RefreshMemoStats() (hits, misses uint64) { return refreshMemo.Load().Stats() }

// refreshResolveMemo returns the memo to install on a refresher resolve,
// creating it on first use.
//
// #883 — NO WHOLESALE SWAP. #598 bounded this memo by swapping it for an empty
// one once its cumulative misses exceeded the live resolved-cache entry count.
// That was a count bound standing in for the real signal, and it neither held
// memory down (bench run 15: the floor rose ~1 GB/h under it) nor kept only
// servable bodies (it threw live entries away with dead ones). The memo now
// keeps exactly the entries that can still be served, by three signals:
//   - a captured dep moved (Load finds it stale, or the dep tracker's bump
//     re-judges it — cache.TrackDepMoves);
//   - every L1 key it served (the producer's and each hitter's) has left the
//     resolved cache — the cache's eviction / delete strips the key's dep edges
//     through DepTracker.RemoveL1Key, which the memo follows;
//   - and nothing else: an entry whose deps are unmoved and that still serves a
//     resident cell is exactly the entry the refresher will reuse.
//
// The memo is tracked BEFORE it is published, so no Store into it can precede
// its tracking; the loser of the creation race untracks its own.
func refreshResolveMemo() *cache.SeedResolveMemo {
	if m := refreshMemo.Load(); m != nil {
		return m
	}
	fresh := cache.NewSeedResolveMemo()
	untrack := cache.TrackDepMoves(fresh)
	if refreshMemo.CompareAndSwap(nil, fresh) {
		return fresh
	}
	// Lost the race — use the winner's memo, never a second one.
	untrack()
	return refreshMemo.Load()
}

// RefreshMemoSize is the #883 gauge for the installed refresher memo: how many
// bodies it holds and their summed encoded size. (0, 0) before the first one.
func RefreshMemoSize() (entries, bodyBytes int64) {
	m := refreshMemo.Load()
	return m.Entries(), m.BodyBytes()
}

// publishResolveMemoExpvar installs snowplow_resolve_memo (#883). Named outside
// the snowplow_refresher_ prefix, which C7 reserves for tagged refresherStats
// fields. Idempotent: expvar panics on a duplicate Publish.
func publishResolveMemoExpvar() {
	resolveMemoPublishOnce.Do(func() {
		expvar.Publish("snowplow_resolve_memo", expvar.Func(func() any {
			entries, bodyBytes := RefreshMemoSize()
			stale, moved, l1Gone := cache.ResolveMemoDropTotals()
			return map[string]any{
				// GAUGES — the installed refresher memo right now. They track the
				// live working set: read them against snowplow_resolved_cache
				// entries, never as a climb on their own.
				"entries":    entries,
				"body_bytes": bodyBytes,
				// Process totals of entries deleted (every memo): by a Load that
				// found the entry stale, by a dep move nobody read, and by the
				// resolved cache removing the last L1 key the entry served.
				"dropped_stale_total":   stale,
				"dropped_moved_total":   moved,
				"dropped_l1_gone_total": l1Gone,
			}
		}))
	})
}

var resolveMemoPublishOnce sync.Once

func init() {
	// CFG-1 gate, as publishRefreshRebuildExpvar: the memo only exists under
	// the cache, so a cache-off pod publishes nothing rather than zeros.
	if cache.Disabled() {
		return
	}
	publishResolveMemoExpvar()
}
