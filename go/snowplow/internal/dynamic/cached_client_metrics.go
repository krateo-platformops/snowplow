// cached_client_metrics.go — Task #326. Expose the SA-discovery singleton
// counters (declared at cached_client.go, snapshotted by
// SADiscoveryStatsSnapshot) via the snowplow_sa_discovery_* expvar family so
// /debug/vars reports them. Read-only — zero changes to the build / lookup /
// invalidate hot path; the counters are bumped in cached_client.go and merely
// surfaced here.
//
// PRIOR ART — mirrors internal/cache/refresher_metrics.go exactly:
//   - expvar.Publish + expvar.Func + sync.Once register-once idiom.
//   - init() gated by cache.Disabled() so the keys do NOT appear under
//     CACHE_ENABLED=false (transparent-fallback contract —
//     project_cache_off_is_transparent_fallback). The dynamic package already
//     imports internal/cache (cache.Disabled() is used elsewhere), so this is
//     cycle-free: cache does NOT import dynamic.
//   - expvar.Func is lazy — it reads SADiscoveryStatsSnapshot() at scrape
//     time, so registration is safe even before the singleton is built (a
//     pre-build scrape returns zeros from the freshly-allocated atomics).
//
// PLACEMENT JUSTIFICATION — these three counters are owned by the SA-discovery
// subsystem (cached_client.go), so they register from the package that owns
// the atomics, exactly like cache/refresher_metrics.go registers cache's own
// atomics and dispatchers/phase1_pip_metrics.go registers dispatchers' own.
// The fallback counter is bumped here (in SharedSADiscoveryClient's error
// returns) even though the fallback Warn fires schema-side, keeping all three
// SA-discovery counters in one source of truth (see cached_client.go header).

package dynamic

import (
	"expvar"
	"sync"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// saDiscoveryMetricsOnce guards expvar.Publish so registration runs at most
// once per process even if both init() and the test helper invoke it.
// expvar.Publish panics on a duplicate key; sync.Once prevents that.
var saDiscoveryMetricsOnce sync.Once

func init() {
	// CFG-1 mirror (same gate as the cache-side expvar publishers): under
	// CACHE_ENABLED=false the SA-discovery singleton is not used on the hot
	// path and these counters MUST NOT be registered.
	if cache.Disabled() {
		return
	}
	registerSADiscoveryMetrics()
}

// registerSADiscoveryMetrics performs the expvar.Publish calls for the three
// SA-discovery observability keys. Guarded by saDiscoveryMetricsOnce so it is
// safe to call from init() and from RegisterSADiscoveryMetricsForTest.
//
// All values are expvar.Func — evaluated lazily at scrape time, so there is no
// per-/call cost and the counters are read on demand.
func registerSADiscoveryMetrics() {
	saDiscoveryMetricsOnce.Do(func() {
		expvar.Publish("snowplow_sa_discovery_builds_total", expvar.Func(func() any {
			return SADiscoveryStatsSnapshot().Builds
		}))
		expvar.Publish("snowplow_sa_discovery_invalidations_total", expvar.Func(func() any {
			return SADiscoveryStatsSnapshot().Invalidations
		}))
		expvar.Publish("snowplow_sa_discovery_fallbacks_total", expvar.Func(func() any {
			return SADiscoveryStatsSnapshot().Fallbacks
		}))
		// #608 — an impersonating config handed to the SA singleton (refused).
		expvar.Publish("snowplow_sa_discovery_impersonated_rejected_total", expvar.Func(func() any {
			return SADiscoveryStatsSnapshot().ImpersonatedRejected
		}))
	})
}

// RegisterSADiscoveryMetricsForTest forces SA-discovery expvar registration
// under tests that flip CACHE_ENABLED=true via t.Setenv after init() already
// ran with CACHE_ENABLED unset. Idempotent (sync.Once-guarded). Production
// callers MUST NOT use this function.
func RegisterSADiscoveryMetricsForTest() {
	registerSADiscoveryMetrics()
}
