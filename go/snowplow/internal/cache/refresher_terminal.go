// refresher_terminal.go — 1.12.6 C4 (design-1.12.6-event-hardening §6), as
// amended by #547: terminal semantics for every refresh outcome.
//
// THE RULE. No refresh outcome may end in "forget the key and keep the entry".
// Every path ends in exactly one of:
//
//   - re-Put        fresh bytes stored (the success path, unchanged);
//   - evict         the entry's basis is GONE — the self-object 404 at the
//     drop point (1.12.5) and every store removal;
//   - known-stale   (#547) the refresh ended without an accepted re-Put — a
//     decline, a drop after the requeue budget, a suppressed
//     skip. The cell stays resident and its stamp stays
//     (dirty_window.go): the serve path counts every hit on it
//     and revalidates it on demand under the requester
//     (known_stale.go); a substrate reader reads through it.
//
// RETIRED BY #547 (owner ruling, #547 comment 6140460857):
//
//   - REFRESH_DROP_EVICT_MAX_PER_MINUTE and its breaker. 1.12.6 evicted a
//     non-404 deterministic failure at the drop point while the breaker had
//     budget and, over budget, kept today's drop-to-TTL — the stale body
//     resident and SERVED as fresh. On 057 80% of the drop points took that
//     branch (#547 trace). An eviction is a cold navigation and forget-as-fresh
//     is the defect, so both give way to known-stale; there is no budget left to
//     tune.
//   - REFRESH_SUPPRESS_AFTER_DECLINES (K consecutive stage-error / empty-full
//     declines before suppression). A transient decline is now KNOWN-STALE and
//     retried on the next read (demand, under the requester) or change (the
//     informer re-enqueues it) — self-adapting, no K.
//
// SUPPRESSION (§6.4, #191) remains for the STRUCTURALLY PERMANENT declines only
// — external endpoint, UAF cell, sensitive resource, unsupported handler kind —
// which no retry of the background refresh can ever turn into a Put. A per-key
// marker held HERE (package state), never on the resident *ResolvedEntry (Get
// returns the live pointer; mutating it is a data race). Consulted in
// processNext right after the entry Get and before the rate floor: a suppressed
// key is Forgotten and skipped (and its window marked known-stale, reason
// suppressed). Cleared by every accepted Put (a real Put is the "next real Put"
// the suppression waits for) and by every store eviction, so the marker cannot
// outlive its key. Since #547 (team ruling (b)) it also gates the demand and
// background revalidations of a known-stale cell: the skip is counted on
// known_stale_revalidation_skipped_total.
//
// Package-level rather than on the refresher struct so that the store's Put /
// eviction path can clear a marker WITHOUT constructing the refresher
// singleton (whose workqueues spawn goroutines — the C1 N1 off-path lesson).
package cache

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// ErrRefreshUnsupported — item 4 row 4: the resolver seam has no handler for
// the entry's kind. It never will for this entry, so the decline site
// suppresses the key (K=1) instead of skipping it to TTL forever.
var ErrRefreshUnsupported = errors.New("refresh unsupported for this entry kind")

// ErrContentNotServable — item 4 row 9: an apistage CONTENT entry whose single
// K8s call is not pivot-servable right now (pre-sync informer, metadata-only
// GVR). Pre-1.12.6 this was a (nil, nil) skip-to-TTL with no 404
// discrimination at all — the one carrier with a single layer where every
// other class has two. Typed, it enters the normal requeue budget and reaches
// the drop point like any other deterministic failure.
var ErrContentNotServable = errors.New("apistage content call not servable")

// --- suppression ---------------------------------------------------------

var (
	// refreshSuppressed: l1Key -> reason. Presence == suppressed.
	refreshSuppressed sync.Map

	refreshSuppressedSetTotal   atomic.Uint64 // keys marked suppressed
	refreshSuppressedSkipsTotal atomic.Uint64 // dequeues skipped because suppressed
	refreshDeclineNotedTotal    atomic.Uint64 // decline sites reached (all reasons)
)

// NoteRefreshDecline records one decline of l1Key at a refresh decline site.
// permanent == true marks the key suppressed (the structurally permanent
// declines: external endpoint, UAF cell, sensitive resource, unsupported kind).
// A non-permanent decline (stage error, empty full) never suppresses since #547:
// the dequeue's window ends KNOWN-STALE and the cell is retried on the next read
// or change. It returns true when the key is (now) suppressed. Idempotent for an
// already suppressed key.
func NoteRefreshDecline(l1Key, reason string, permanent bool) bool {
	if l1Key == "" {
		return false
	}
	refreshDeclineNotedTotal.Add(1)
	if _, already := refreshSuppressed.Load(l1Key); already {
		return true
	}
	if !permanent {
		return false
	}
	refreshSuppressed.Store(l1Key, reason)
	refreshSuppressedSetTotal.Add(1)
	return true
}

// RefreshSuppressedReason reports whether l1Key is suppressed and why.
func RefreshSuppressedReason(l1Key string) (string, bool) {
	v, ok := refreshSuppressed.Load(l1Key)
	if !ok {
		return "", false
	}
	return v.(string), true
}

// clearRefreshSuppression forgets the marker for l1Key. Called by the store on every Put (the "next real Put")
// and on every eviction (the marker must not outlive its key). Cheap: two
// sync.Map deletes, no allocation.
func clearRefreshSuppression(l1Key string) {
	refreshSuppressed.Delete(l1Key)
}

// resetRefreshTerminalForTest clears the suppression state and counters.
// Called from resetRefresherForTest.
func resetRefreshTerminalForTest() {
	refreshSuppressed.Range(func(k, _ any) bool { refreshSuppressed.Delete(k); return true })
	refreshSuppressedSetTotal.Store(0)
	refreshSuppressedSkipsTotal.Store(0)
	refreshDeclineNotedTotal.Store(0)
	resetKnownStaleForTest()
}

// AddRefreshTerminalCountersForTest moves the C4 counters through their
// production atomics (no refresher built) and marks suppressedKeys synthetic
// keys refresh-by-traffic-only, so a cross-package arm can prove the OTLP mirror
// reads the LIVE counters rather than constants. Production callers MUST NOT use
// this; pair with ResetRefreshTerminalForTest in t.Cleanup.
func AddRefreshTerminalCountersForTest(suppressedSet, suppressedSkips uint64, suppressedKeys int) {
	refreshSuppressedSetTotal.Add(suppressedSet)
	refreshSuppressedSkipsTotal.Add(suppressedSkips)
	for i := 0; i < suppressedKeys; i++ {
		refreshSuppressed.Store(fmt.Sprintf("test-suppressed-%d", i), "test")
	}
}

// ResetRefreshTerminalForTest is the exported twin of resetRefreshTerminalForTest
// for cross-package arms. Production callers MUST NOT use this.
func ResetRefreshTerminalForTest() { resetRefreshTerminalForTest() }

// RefreshTerminalStats is the read-only snapshot of the C4 counters (expvar
// + OTLP + tests).
type RefreshTerminalStats struct {
	// 1.12.6 C7: tagged like refresherStats — the counters ride the shared
	// snowplow_refresher instrument, suppressed_keys is a gauge of its own.
	// `stat:"-"` fields are diagnostics read by the arms only. #547 retired the
	// drop-point breaker's drop_evict / drop_evict_suspended counters with the
	// breaker itself.
	SuppressedSetTotal   uint64 `stat:"suppressed_set"`
	SuppressedSkipsTotal uint64 `stat:"suppressed_skips"`
	DeclineNotedTotal    uint64 `stat:"-"` // arms only
	SuppressedKeys       int    `stat:"suppressed_keys" kind:"gauge" desc:"Live count of L1 keys marked refresh-by-traffic-only after a structurally permanent decline (1.12.6 #191, #547); bounded by the store."`
}

// RefreshTerminalStatsSnapshot reads the counters. It does NOT construct the
// refresher singleton and it does NOT read refresherInstance at all (B1):
// every counter it reports is a package-level atomic, so a /debug/vars scrape
// or an OTLP collection racing the singleton's first construction is an
// ordinary atomic load.
func RefreshTerminalStatsSnapshot() RefreshTerminalStats {
	s := RefreshTerminalStats{
		SuppressedSetTotal:   refreshSuppressedSetTotal.Load(),
		SuppressedSkipsTotal: refreshSuppressedSkipsTotal.Load(),
		DeclineNotedTotal:    refreshDeclineNotedTotal.Load(),
	}
	refreshSuppressed.Range(func(_, _ any) bool { s.SuppressedKeys++; return true })
	return s
}
