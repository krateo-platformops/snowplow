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
// refresher's resolve context and bound its lifetime.
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
	"sync/atomic"

	pmaps "github.com/krateo-platformops/plumbing/maps"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// refreshMemo holds the memo currently installed on refresher resolves. Swapped
// wholesale rather than evicted per entry (see refreshResolveMemo).
var refreshMemo atomic.Pointer[cache.SeedResolveMemo]

// refreshMemoSwapsTotal counts wholesale swaps. Exposed for the falsifier and
// for diagnosing a memo that is churning rather than collapsing: a swap rate
// near the miss rate means the bound is being hit constantly and the memo is
// holding nothing long enough to be useful.
var refreshMemoSwapsTotal atomic.Uint64

// RefreshMemoSwaps is the test/diagnostic accessor.
func RefreshMemoSwaps() uint64 { return refreshMemoSwapsTotal.Load() }

// RefreshMemoStats returns the live memo's hit/miss counters, or (0,0) when no
// memo has been created yet.
func RefreshMemoStats() (hits, misses uint64) { return refreshMemo.Load().Stats() }

// refreshResolveMemo returns the memo to install on a refresher resolve,
// creating one on first use and swapping it wholesale once it has stored more
// distinct bodies than there are cells for it to serve.
//
// THE BOUND IS DERIVED, NOT CHOSEN. The memo can never usefully hold more
// distinct keys than there are resident cells eligible to be refreshed, so the
// live resolved-cache entry count IS the ceiling — no invented constant, and it
// tracks the cluster's real shape (feedback_self_adapt_no_magic_env_knobs).
// Misses are the right proxy for stored entries: every miss ends in a Store.
// Below one entry (a cold or cache-off process) no bound is applied, because a
// memo that has stored nothing cannot be over budget.
//
// A SWAP CANNOT CORRUPT ANYTHING. The old memo is simply dropped; in-flight
// resolves holding a pointer to it keep using it until they finish, and every
// body it served was already deep-copied on Load. The worst a swap costs is a
// few duplicate resolves, which is the state this file exists to improve on —
// so the failure mode of the bound is "less saving", never "wrong answer".
func refreshResolveMemo() *cache.SeedResolveMemo {
	m := refreshMemo.Load()
	if m == nil {
		fresh := cache.NewSeedResolveMemo(pmaps.DeepCopyJSON)
		if refreshMemo.CompareAndSwap(nil, fresh) {
			return fresh
		}
		// Lost the race — use the winner's memo, never a second one.
		return refreshMemo.Load()
	}

	_, misses := m.Stats()
	if cap := refreshMemoCap(); cap > 0 && misses > cap {
		fresh := cache.NewSeedResolveMemo(pmaps.DeepCopyJSON)
		if refreshMemo.CompareAndSwap(m, fresh) {
			refreshMemoSwapsTotal.Add(1)
			return fresh
		}
		// Another goroutine swapped first; take theirs.
		return refreshMemo.Load()
	}
	return m
}

// refreshMemoCap is the live resolved-cache entry count — the number of cells
// that exist to be refreshed, and therefore the most distinct bodies the memo
// could ever usefully hold. 0 means "unknown or empty": apply no bound.
func refreshMemoCap() uint64 {
	stats := cache.ResolvedCacheStatsByStat()
	if stats == nil {
		return 0
	}
	n, ok := stats["entries"]
	if !ok || n <= 0 {
		return 0
	}
	return uint64(n)
}
