// stats_families.go — 1.12.6 C7. The derived `stat -> value` maps for the
// three families whose surfaces used to be hand-written field lists
// (crd_discovery, refresh_broadcaster, refresher). Each function is the
// ONE thing expvar and the OTLP mirror read, so the two cannot disagree,
// and the key set comes from the struct tags (stats_by_tag.go), so a new
// field cannot be forgotten by one surface and remembered by another.

package cache

import "reflect"

// CRDDiscoveryStatsByStat flattens the CRD-discovery bridge counters into
// the map /debug/vars snowplow_crd_discovery and the OTLP
// snowplow_crd_discovery{stat=...} instrument both publish.
func CRDDiscoveryStatsByStat() map[string]int64 {
	return statsInt64ByTag(CRDDiscoveryStatsSnapshot())
}

// RefreshBroadcasterStatsByStat flattens the broadcaster snapshot into the
// map /debug/vars snowplow_refresh_broadcaster publishes. Values keep their
// Go kind (stream_seconds_total is a float64); the OTLP mirror walks
// RefreshBroadcasterStatSpecs to pick Int64 vs Float64 instruments.
func RefreshBroadcasterStatsByStat() map[string]any {
	return statsByTag(RefreshBroadcasterStatsSnapshot())
}

// RefreshBroadcasterStatSpecs lists the broadcaster stats in declaration
// order, with their kind, so internal/metrics can create one instrument per
// stat WITHOUT naming any of them by hand.
func RefreshBroadcasterStatSpecs() []StatSpec {
	return statSpecsOf(reflect.TypeOf(RefreshBroadcasterStats{}))
}

// refresherStatSpecs lists the refresher stats: the pool counters and
// queue-depth gauge (refresherStats) followed by the 1.12.6 C4 terminal
// counters (RefreshTerminalStats). One family on every surface.
func refresherStatSpecs() []StatSpec {
	return append(statSpecsOf(reflect.TypeOf(refresherStats{})),
		statSpecsOf(reflect.TypeOf(RefreshTerminalStats{}))...)
}

// RefresherStatSpecs is refresherStatSpecs for internal/metrics.
func RefresherStatSpecs() []StatSpec { return refresherStatSpecs() }

// RefresherStatsByStat flattens the refresher pool counters AND the C4
// terminal counters into one `stat -> value` map. It never constructs the
// refresher (#203): before the first enqueue, or under CACHE_ENABLED=false,
// the pool counters read zero from a nil peek and the terminal counters are
// package atomics.
func RefresherStatsByStat() map[string]int64 {
	out := statsInt64ByTag(refresherStatsSnapshot())
	for k, v := range statsInt64ByTag(RefreshTerminalStatsSnapshot()) {
		out[k] = v
	}
	return out
}
