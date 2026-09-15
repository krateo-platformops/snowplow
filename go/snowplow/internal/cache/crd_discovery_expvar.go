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
		expvar.Publish("snowplow_crd_discovery", expvar.Func(func() any {
			s := CRDDiscoveryStatsSnapshot()
			return map[string]uint64{
				"events_enqueued": s.EventsEnqueued,
				"events_dropped":  s.EventsDropped,
				// 1.12.5 / #187 — parked > 0 means the worker fell 256 events
				// behind and the informer processor goroutine had to wait. A
				// DROPPED lifecycle event is now a last resort after the park
				// deadline; for a DELETE it means an informer is never torn
				// down and its dependent L1 entries stay resident until TTL.
				"events_parked":    s.EventsParked,
				"events_processed": s.EventsProcessed,
				// ADD + UPDATE path
				"discovery_invoked":    s.DiscoveryInvoked,
				"discovery_skipped_ng": s.DiscoverySkippedNG,
				// DELETE path (Ship L)
				"deletes_processed": s.DeletesProcessed,
				"delete_skipped_ng": s.DeleteSkippedNG,
				"panics_recovered":  s.PanicsRecovered,
				// schema-widen relist (followup-crd-schema-widen-informer-relist)
				"schema_relists_fired": s.SchemaRelistsFired,
				"schema_unchanged":     s.SchemaUnchanged,
				// 1.12.5 / #187 — post-sync re-fire of the relist dirty-mark.
				// postsync lagging schema_relists_fired means relisted
				// informers are not syncing, which strands L1 entries whose
				// objects were deleted inside the teardown window.
				"relist_dirtymark_postsync_total": s.RelistDirtyMarkPostSync,
				"relist_postsync_timeout_total":   s.RelistPostSyncTimeout,
				// 1.12.6 C2 — relist delta bridge. enqueued moving with
				// timeout at ZERO across real CRD schema changes is the soak
				// evidence that retires the 1.12.5 re-fire above; timeout
				// > 0 on a healthy cluster means the bridge is NOT covering
				// the teardown window and the re-fire must stay.
				"relist_bridge_runs_total":     s.RelistBridgeRuns,
				"relist_bridge_enqueued_total": s.RelistBridgeEnqueued,
				"relist_bridge_timeout_total":  s.RelistBridgeTimeout,
				"relist_bridge_aborted_total":  s.RelistBridgeAborted,
			}
		}))
	})
}
