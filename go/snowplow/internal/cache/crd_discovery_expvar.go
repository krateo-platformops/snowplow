// crd_discovery_expvar.go — Ship L (0.30.246) expvar exposure for the
// CRD-discovery bridge counters (closes followup #143).
//
// PURPOSE: the CRD-discovery bridge counters (DiscoveryInvoked /
// DiscoverySkippedNG / DeletesProcessed / etc.) live in atomic.Uint64
// inside the crdDiscovery singleton; pre-Ship-L they were only readable
// via the test-helper CRDDiscoveryStatsSnapshot. Had the bridge been
// expvar-exposed at /debug/vars/snowplow_crd_discovery, the Ship
// 0.30.233 bytesObject regression would have surfaced in seconds: a
// non-zero DiscoverySkippedNG counter on a healthy cluster is a flashing
// red flag. Instead the silent-skip behaviour hid the defect for 13
// ships until Phase 6 S4 tripped on it.
//
// REGISTRATION: mirrors the existing cache expvar pattern
// (fallthrough_meter_expvar.go, controller_health_expvar.go,
// refresher_metrics.go): sync.Once-guarded registration called from
// init() unless CACHE_ENABLED=false (per project memory
// project_cache_off_is_transparent_fallback — under CACHE_ENABLED=false
// the cache subsystem does not exist and these gauges MUST NOT register).
//
// KEY: snowplow_crd_discovery (matches snowplow_* naming convention).
// VALUE: flat map[string]uint64 — matches the
// snowplow_bindings_by_gvr_delta_skipped_non_typed shape at
// bindings_by_gvr_metrics.go:60.

package cache

import (
	"expvar"
	"sync"
)

var crdDiscoveryExpvarOnce sync.Once

func init() {
	if Disabled() {
		return
	}
	registerCRDDiscoveryExpvar()
}

// registerCRDDiscoveryExpvar performs the expvar.Publish for the
// bridge counters. Guarded by sync.Once so it is safe to call from
// both init() and the test helper RegisterExpvarForTest.
//
// The publish handler reads counters via CRDDiscoveryStatsSnapshot
// (which reads each atomic.Uint64) so every /debug/vars hit observes
// a coherent point-in-time snapshot — no torn reads.
func registerCRDDiscoveryExpvar() {
	crdDiscoveryExpvarOnce.Do(func() {
		// 1.12.6 C7: the map is DERIVED from the CRDDiscoveryStats struct tags
		// (stats_families.go) — the same map the OTLP mirror ranges over, so
		// the two surfaces cannot drift and a new counter needs one tag, not
		// four edits. Per-stat meaning lives on the struct fields and in
		// docs/architecture/observability.md (guarded by TestC7_Docs_*).
		expvar.Publish("snowplow_crd_discovery", expvar.Func(func() any {
			return CRDDiscoveryStatsByStat()
		}))
	})
}
