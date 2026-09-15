// refresh_broadcaster_expvar.go — Ship 1 (live-refresh-coherence, option A).
//
// expvar exposure for the live-refresh broadcaster counters at
// /debug/vars/snowplow_refresh_broadcaster. Mirrors the established cache
// expvar pattern (crd_discovery_expvar.go, fallthrough_meter_expvar.go):
// sync.Once-guarded expvar.Publish from init(), skipped under
// CACHE_ENABLED=false (the cache subsystem does not exist there, so these
// gauges MUST NOT register — project_cache_off_is_transparent_fallback).
//
// These counters back the falsifier artifacts: refreshDeliveredTotal proves
// signals reach subscribers (9.2), refreshDroppedTotal proves the slow-
// consumer drop arm fired without stalling the refresher (9.6), and
// subscribers is the connection-scale gauge (design §10).
//
// 1.12.6 item 7 (C12, design §6.5): the eviction-path and stream-lifetime
// numbers are expvars + OTel instruments, NOT log lines — the chart ships
// LOG_LEVEL=warn (helm/snowplow/values.yaml), so an INFO diagnostic is dark
// in production. Same expvar key (no new top-level name; the C0 cache-off
// structural guard counts publishers, not fields):
//
//	armed_keys           distinct l1Keys with >=1 armed connection (len(keySubs))
//	max_sink_depth       high-water mark of a subscriber sink after a send (lag)
//	evict_published      PublishEviction calls that reached >=1 subscriber
//	evict_deferred       eviction signals the per-subscriber bucket deferred —
//	                     the number that proves the C10 bound engaged
//	stream_seconds_total accumulated /refreshes stream lifetime (s)
//	streams_closed_total streams that ended; quotient = mean stream lifetime
//
// Alertable (design §6.5): sustained dropped > 0 => a wedged consumer;
// delivered == 0 while subscribers > 0 and published > 0 => the key-space
// mismatch (L7), live.

package cache

import (
	"expvar"
	"sync"
)

var refreshBroadcasterExpvarOnce sync.Once

func init() {
	if Disabled() {
		return
	}
	registerRefreshBroadcasterExpvar()
}

// registerRefreshBroadcasterExpvar publishes the broadcaster counters. The
// handler reads one RefreshBroadcasterStatsSnapshot so every scrape observes
// a coherent point-in-time snapshot.
func registerRefreshBroadcasterExpvar() {
	refreshBroadcasterExpvarOnce.Do(func() {
		// 1.12.6 C7: derived from the RefreshBroadcasterStats struct tags, one
		// coherent snapshot per scrape (stats_families.go).
		expvar.Publish("snowplow_refresh_broadcaster", expvar.Func(func() any {
			return RefreshBroadcasterStatsByStat()
		}))
	})
}

// RegisterRefreshBroadcasterExpvarForTest forces the publisher to run when
// CACHE_ENABLED was not set at process start (the init() gate ran first).
// Idempotent (sync.Once). Test-only — production MUST NOT call it; same
// discipline as RegisterDepsExpvarForTest.
func RegisterRefreshBroadcasterExpvarForTest() {
	registerRefreshBroadcasterExpvar()
}
