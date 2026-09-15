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
		// 1.12.6 C7: one top-level expvar per tagged stat of refresherStats +
		// RefreshTerminalStats, named by StatFamily.ExpvarKey
		// (snowplow_refresher_<stat>_total for counters, snowplow_refresher_<stat>
		// for gauges) — the same rule the OTLP mirror and the parity arm apply.
		// Each closure reads ONE coherent RefresherStatsByStat() map; none of
		// them constructs the refresher (#203). Per-stat meaning lives on the
		// struct fields and in docs/architecture/observability.md.
		for _, fam := range TaggedStatFamilies() {
			if fam.ExpvarPrefix != "snowplow_refresher_" {
				continue
			}
			for _, spec := range fam.Specs {
				stat := spec.Stat
				expvar.Publish(fam.ExpvarKey(spec), expvar.Func(func() any {
					return RefresherStatsByStat()[stat]
				}))
			}
		}
	})
}

// RegisterRefresherMetricsForTest forces refresher expvar registration
// under tests that flip CACHE_ENABLED=true via t.Setenv after init()
// already ran with CACHE_ENABLED unset. Idempotent (sync.Once-guarded).
// Production callers MUST NOT use this function.
func RegisterRefresherMetricsForTest() {
	registerRefresherMetrics()
}
