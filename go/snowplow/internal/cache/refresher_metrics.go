// refresher_metrics.go — Ship OBS-1 (0.30.186): expose the refresher
// atomic counters (declared at refresher.go:101-106, snapshotted by
// refresherStatsSnapshot at refresher.go:357) via expvar so /debug/vars
// reports them. Read-only — zero new populate paths, zero changes to
// the queue / handler / processOne hot path.
//
// PRIOR ART
//
//   - internal/handlers/dispatchers/phase1_pip_metrics.go:94-127 —
//     established expvar.Publish + expvar.Func + sync.Once idiom for
//     dispatcher-side counters.
//   - internal/cache/cohort_gate_memo_metrics.go:99 — established
//     pattern for an expvar.Func that walks live runtime state at
//     scrape time (here: r.queue.Len() and the snapshot atomics).
//
// REGISTRATION TIMING
//
// init() runs at process start before main, gated by Disabled() so
// the keys do NOT appear under CACHE_ENABLED=false (transparent-
// fallback contract — project_cache_off_is_transparent_fallback).
// expvar.Func is lazy: it reads refresherSingleton() at scrape time,
// so registration is safe even when the refresher pool has not been
// started yet (the singleton is constructed by the first caller; a
// pre-start scrape simply returns zero counters from the freshly
// allocated struct).
//
// COST PER SCRAPE
//
// Eight atomic.Uint64.Load + one workqueue.Len() (which takes the
// queue's internal RWMutex briefly). Scrape rate is ~every 10-60s
// from prometheus / the tester poller; the 1.0× per-/call hot path
// is unchanged (no populate amplification, per
// feedback_refresher_populate_amplification).

package cache

import (
	"expvar"
	"sync"
)

// refresherMetricsOnce guards expvar.Publish so the registration runs
// at most once per process even if both init() and the test helper
// invoke registerRefresherMetrics. expvar.Publish panics on duplicate
// key; sync.Once prevents that.
var refresherMetricsOnce sync.Once

func init() {
	// CFG-1 mirror (same gate as the other cache-side expvar publishers):
	// under CACHE_ENABLED=false the refresher does not run and these
	// counters MUST NOT be registered.
	if Disabled() {
		return
	}
	registerRefresherMetrics()
}

// registerRefresherMetrics performs the expvar.Publish calls for the
// nine refresher observability keys. Guarded by refresherMetricsOnce
// so it is safe to call from init() and from RegisterExpvarForTest.
//
// All values are expvar.Func — evaluated lazily at scrape time, so
// there is no per-/call cost and the refresher singleton is read on
// demand (handles the pre-StartRefresher window gracefully).
func registerRefresherMetrics() {
	refresherMetricsOnce.Do(func() {
		expvar.Publish("snowplow_refresher_enqueue_total", expvar.Func(func() any {
			return refresherStatsSnapshot().enqueued
		}))
		expvar.Publish("snowplow_refresher_completed_total", expvar.Func(func() any {
			return refresherStatsSnapshot().completed
		}))
		expvar.Publish("snowplow_refresher_failed_total", expvar.Func(func() any {
			return refresherStatsSnapshot().failed
		}))
		expvar.Publish("snowplow_refresher_retried_total", expvar.Func(func() any {
			return refresherStatsSnapshot().retried
		}))
		expvar.Publish("snowplow_refresher_dropped_total", expvar.Func(func() any {
			return refresherStatsSnapshot().dropped
		}))
		expvar.Publish("snowplow_refresher_skipped_no_entry_total", expvar.Func(func() any {
			return refresherStatsSnapshot().skippedNoEntry
		}))
		expvar.Publish("snowplow_refresher_skipped_no_handler_total", expvar.Func(func() any {
			return refresherStatsSnapshot().skippedNoHandler
		}))
		expvar.Publish("snowplow_refresher_skipped_stage_error_total", expvar.Func(func() any {
			return refresherStatsSnapshot().skippedStageError
		}))
		// Live queue depth — useful for diagnosing back-pressure (a
		// climbing depth with stagnant completed_total means workers
		// are stuck). workqueue.TypedRateLimitingInterface.Len() is
		// part of the interface and is safe to call concurrently.
		expvar.Publish("snowplow_refresher_queue_depth", expvar.Func(func() any {
			r := refresherInstance
			if r == nil || r.queue == nil {
				return 0
			}
			return r.queue.Len()
		}))
		// Ship #98 / 0.30.215 — customer-priority yield observability.
		// _yielded_total ticks every time a worker spent at least one
		// yield-poll parked because a customer /call was in flight.
		// _capped_total ticks every time refresherYieldMaxParked fired —
		// proceeded-anyway count. Both are POST-DEPLOY falsifier evidence
		// (AC-98.3, AC-98.8): _yielded_total MUST be > 0 under a customer
		// burst (if 0 the hook is broken); _capped_total MUST stay near 0
		// (if it climbs steadily the customer-inflight counter is leaking
		// or a sustained-pressure pathological case is unfolding).
		expvar.Publish("snowplow_refresher_yielded_total", expvar.Func(func() any {
			return refresherStatsSnapshot().yielded
		}))
		expvar.Publish("snowplow_refresher_capped_total", expvar.Func(func() any {
			return refresherStatsSnapshot().capped
		}))
		// Task #321 (#318-R1a) — per-key re-resolve rate-floor falsifier.
		// _floored_total ticks every time the dequeue-side floor gate
		// DEFERRED a key (entry younger than the floor) instead of
		// re-resolving it now. THE primary post-deploy falsifier readout:
		// under the install-churn storm completed_total collapses (target Δ
		// < 30,000 vs 141,381) while _floored_total is > 0 and rising. Per
		// PM gate C1 _floored_total EXCEEDS the completed-collapse delta (it
		// counts the bounded immediate-repush re-cycles too) — it is
		// qualitative evidence the gate fired, NOT equal to the collapse.
		expvar.Publish("snowplow_refresher_floored_total", expvar.Func(func() any {
			return refresherStatsSnapshot().floored
		}))
		// 1.12.6 C4 (§6) — terminal-semantics counters. drop_evict_total is
		// the non-404 drop-point evictions performed; drop_evict_suspended_
		// total the ones the breaker refused (a mass failure — the entries
		// stayed resident and served); suppressed_* the #191 refresh-by-
		// traffic-only markers. Alerting pair: drop_evict_suspended_total > 0
		// means "an outage-shaped failure burst", not a cache defect.
		expvar.Publish("snowplow_refresher_drop_evict_total", expvar.Func(func() any {
			return RefreshTerminalStatsSnapshot().DropEvictTotal
		}))
		expvar.Publish("snowplow_refresher_drop_evict_suspended_total", expvar.Func(func() any {
			return RefreshTerminalStatsSnapshot().DropEvictSuspendedTotal
		}))
		expvar.Publish("snowplow_refresher_suppressed_set_total", expvar.Func(func() any {
			return RefreshTerminalStatsSnapshot().SuppressedSetTotal
		}))
		expvar.Publish("snowplow_refresher_suppressed_skips_total", expvar.Func(func() any {
			return RefreshTerminalStatsSnapshot().SuppressedSkipsTotal
		}))
		expvar.Publish("snowplow_refresher_suppressed_keys", expvar.Func(func() any {
			return RefreshTerminalStatsSnapshot().SuppressedKeys
		}))
	})
}

// RegisterRefresherMetricsForTest forces refresher expvar registration
// under tests that flip CACHE_ENABLED=true via t.Setenv after init()
// already ran with CACHE_ENABLED unset. Idempotent (sync.Once-guarded).
// Production callers MUST NOT use this function.
func RegisterRefresherMetricsForTest() {
	registerRefresherMetrics()
}
