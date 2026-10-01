// informer_dispatch_metrics.go — Tag 0.30.96: stable serve-rate counters
// for the 0.30.95 resolver pivot (`dispatchViaInformer`).
//
// 0.30.95 shipped the pivot with per-call `log.Debug` lines only
// (`informer_dispatch.list_served`, `.get_served`, `.fallthrough.*`).
// The Phase 6 bench proved those are unreadable at 50K request volume —
// the serve rate could not be measured. 0.30.96 adds package-level
// atomic counters + a STABLE single-line `informer_dispatch.summary` so
// the WHOLE pivot (resolver path here + the `objects.Get` path) is
// observable on the re-bench.
//
// Counters here are incremented inside `dispatchViaInformer`. The summary
// goroutine is lazy-started on first dispatch (sync.Once-bounded — one
// goroutine for the process lifetime, never started when the pivot is
// inactive — #57: the pivot is implicit-on-cache, so it never starts
// when the cache subsystem is off). This mirrors `objects` package's
// `startObjectsGetSummary` and `cache`'s `startResolvedCacheSummary`.

package api

import (
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Pivot serve-rate counters. Package-level atomics — safe to Add/Load
// without external locking.
var (
	// dispatchInformerListServed counts LIST calls answered from the
	// informer indexer (marshalled into the apiserver LIST envelope).
	dispatchInformerListServed atomic.Uint64
	// dispatchInformerGetServed counts GET-by-name calls answered from
	// the informer indexer.
	dispatchInformerGetServed atomic.Uint64
	// dispatchInformerFallthrough counts calls that took the apiserver
	// branch under the active pivot — write verb, subresource, non-
	// apiserver path, passthrough / cache-off, metadata-only GVR,
	// not-synced informer, GET-miss, or marshal failure.
	dispatchInformerFallthrough atomic.Uint64
	// dispatchInformerRBACDropped counts INDIVIDUAL list items dropped
	// by the Tag-0.30.100 post-LIST per-item RBAC filter — the user
	// lacked a `list` grant for the item's namespace. A non-zero value
	// is the expected steady state for narrow-RBAC users; it is the
	// falsifier signal that the pivot is no longer over-exposing.
	dispatchInformerRBACDropped atomic.Uint64
	// dispatchInformerSyncWaitServed counts Gate-6 dispatches that were
	// served from cache AFTER a Ship 0.30.121 R2-b bounded sync-wait — a
	// GVR that was unsynced on entry became servable within
	// RESOLVER_SYNC_WAIT_MS. A non-zero value means the R2-b knob is
	// converting would-be apiserver LISTs into cache serves; it stays 0
	// when the knob is at its default (0 = disabled).
	dispatchInformerSyncWaitServed atomic.Uint64
)

// DispatchInformerStats is an atomic snapshot of the pivot serve-rate
// counters. Fields may drift by a single in-flight call — fine for log
// aggregation. Exported so tests can assert increments deterministically.
type DispatchInformerStats struct {
	ListServed     uint64
	GetServed      uint64
	Fallthrough    uint64
	RBACDropped    uint64
	SyncWaitServed uint64
}

// DispatchInformerStatsSnapshot returns the current counter values.
func DispatchInformerStatsSnapshot() DispatchInformerStats {
	return DispatchInformerStats{
		ListServed:     dispatchInformerListServed.Load(),
		GetServed:      dispatchInformerGetServed.Load(),
		Fallthrough:    dispatchInformerFallthrough.Load(),
		RBACDropped:    dispatchInformerRBACDropped.Load(),
		SyncWaitServed: dispatchInformerSyncWaitServed.Load(),
	}
}

// Summary goroutine knobs. Mirrors resolved.go's
// `RESOLVED_CACHE_SUMMARY_EVERY_SECONDS` pattern.
const (
	envDispatchSummaryEvery       = "RESOLVER_DISPATCH_SUMMARY_EVERY_SECONDS"
	defaultDispatchSummarySeconds = 60
)

var (
	dispatchSummaryOnce sync.Once
	// dispatchSummaryStop signals the summary goroutine to exit. It is
	// created inside the sync.Once and read only there and by the test
	// seam, so it needs no separate lock. Production never closes it (the
	// goroutine's lifetime is the process's); the test seam does (#221).
	dispatchSummaryStop chan struct{}
	// dispatchSummaryWG joins the summary goroutine so a stop can prove it
	// has actually exited — the goroutine-leak arm (#221) relies on this.
	dispatchSummaryWG sync.WaitGroup
)

// startDispatchSummary launches a single bounded goroutine that emits an
// `informer_dispatch.summary` INFO line every N seconds. Lifecycle bound:
// a sync.Once guarantees exactly one goroutine at a time; in production the
// process never stops it, but the loop now selects on a stop channel so the
// deferred `t.Stop()` is REACHABLE and the goroutine can be joined and
// drained by the test seam (#221 — an unstoppable goroutine is
// indistinguishable from a leak and makes every log-capture test racy).
//
// Started lazily on the first `dispatchViaInformer` call so the goroutine
// never exists when the pivot is inactive (#57: implicit-on-cache — never
// when the cache subsystem is off).
func startDispatchSummary() {
	dispatchSummaryOnce.Do(func() {
		every := time.Duration(dispatchSummaryEverySeconds()) * time.Second
		stop := make(chan struct{})
		dispatchSummaryStop = stop
		dispatchSummaryWG.Add(1)
		go func() {
			defer dispatchSummaryWG.Done()
			t := time.NewTicker(every)
			defer t.Stop()
			for {
				select {
				case <-stop:
					return
				case <-t.C:
					s := DispatchInformerStatsSnapshot()
					// STABLE single-line falsifier shape (greppable):
					//   informer_dispatch.summary list_served=N get_served=M
					//   apiserver_fallthrough=K
					slog.Info("informer_dispatch.summary",
						slog.String("subsystem", "cache"),
						slog.Uint64("list_served", s.ListServed),
						slog.Uint64("get_served", s.GetServed),
						slog.Uint64("apiserver_fallthrough", s.Fallthrough),
						slog.Uint64("rbac_dropped", s.RBACDropped),
						slog.Uint64("sync_wait_served", s.SyncWaitServed),
					)
				}
			}
		}()
	})
}

// stopDispatchSummaryForTest stops the summary goroutine started by
// startDispatchSummary, blocks until it has actually exited (joined
// WaitGroup), and resets the sync.Once so a subsequent start relaunches it.
// TEST-ONLY — production never stops the goroutine (its lifetime is the
// process's), mirroring this package's other `*ForTest` reset seams
// (resetInternalClientCacheForTest / resetDiscoveryClientCacheForTest). It
// is idempotent: safe to call when the goroutine was never started.
func stopDispatchSummaryForTest() {
	if dispatchSummaryStop != nil {
		close(dispatchSummaryStop)
	}
	dispatchSummaryWG.Wait()
	dispatchSummaryStop = nil
	dispatchSummaryOnce = sync.Once{}
}

// ResetDispatchSummaryForTest is the EXPORTED variant of
// stopDispatchSummaryForTest, for cross-package tests (internal/handlers/
// dispatchers) that capture the default slog logger: it stops + joins the
// summary goroutine so its 60s-default ticker cannot write into a test's
// captured buffer concurrently with the test's read (#221 — the leaked-
// goroutine-vs-log-capture race). Idempotent and restart-capable (the Once is
// reset), so a later dispatch relaunches the summary. Production MUST NOT call
// it (the goroutine's lifetime is the process's).
func ResetDispatchSummaryForTest() {
	stopDispatchSummaryForTest()
}

// DispatchSummaryRunningForTest reports whether the summary emitter is
// currently running, read from this package's OWN lifecycle state (the stop
// channel the Once installs) rather than a process-global
// runtime.NumGoroutine() sample. TEST-ONLY. Order-independent: it reflects
// exactly this emitter, immune to goroutine churn from neighbour tests (#368).
func DispatchSummaryRunningForTest() bool {
	return dispatchSummaryStop != nil
}

// dispatchSummaryEverySeconds resolves the summary interval from the env
// knob, falling back to the default on unset / non-int / non-positive.
func dispatchSummaryEverySeconds() int {
	v := os.Getenv(envDispatchSummaryEvery)
	if v == "" {
		return defaultDispatchSummarySeconds
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return defaultDispatchSummarySeconds
	}
	return n
}
