// resolved_cache_expvar.go — 1.12.4 §7b. expvar exposure for the L1
// resolved-output store's occupancy and lifetime counters.
//
// WHY THIS FILE EXISTS. Every field below already exists on
// ResolvedCacheStats and is already computed by Stats(). Until now the
// ONLY place it went was the periodic `resolved_cache.summary` INFO line
// emitted by startResolvedCacheSummary — and the chart ships
// LOG_LEVEL=warn, which suppresses INFO entirely. So in production the
// numbers that say whether the L1 cache is full, thrashing, or evicting
// were computed every N seconds and then thrown away. This publishes
// them on the surface that is actually readable (/debug/vars, and via
// the OTLP mirror in internal/metrics).
//
// SHAPE. One expvar key, `snowplow_resolved_cache`, returning a
// map[string]int64 keyed by stat name — the same "one counter keyed by
// stat" idiom the OTLP mirror already uses for snowplow_refresher and
// snowplow_crd_discovery, so the two surfaces map 1:1 with no per-stat
// key sprawl at /debug/vars.
//
// NON-CONSTRUCTING. The closure reads resolvedCachePublished (an
// atomic.Pointer written inside resolvedCacheOnce), NOT ResolvedCache(),
// which would lazily build the store, wire the dep tracker and start the
// summary goroutine. A telemetry scrape must never create the thing it
// is measuring. Before the store is built the closure returns an empty
// map rather than zeros, so "not built yet" and "built and empty" are
// distinguishable.
//
// COST. Stats() takes the store's full exclusive mutex (resolved.go
// Stats), not an RLock, and the body is order.Len() plus a handful of
// field reads — microseconds. startResolvedCacheSummary already calls it
// on a timer, so this is not a new class of contention; it is the same
// call on two more read paths. Noted deliberately and NOT "fixed" in
// this release (design §7b / gate §C).
//
// CFG-1 (cache-off compliance, project_cache_off_is_transparent_fallback).
// Under CACHE_ENABLED=false there is no L1 store, so the key MUST NOT be
// registered at all — not registered-with-zeros. The gate is at init()
// time, matching fallthrough_meter_expvar.go, and
// RegisterResolvedCacheExpvarForTest is the sync.Once-guarded seam for
// tests that flip CACHE_ENABLED with t.Setenv after init() already ran.
package cache

import (
	"expvar"
	"sync"
)

// resolvedCacheExpvarOnce guards the Publish call against the
// duplicate-key panic when both init() and the test seam run.
var resolvedCacheExpvarOnce sync.Once

func init() {
	if Disabled() {
		return
	}
	registerResolvedCacheExpvar()
}

// registerResolvedCacheExpvar publishes snowplow_resolved_cache.
// Idempotent (sync.Once), so init() and the test seam can both call it.
func registerResolvedCacheExpvar() {
	resolvedCacheExpvarOnce.Do(func() {
		expvar.Publish("snowplow_resolved_cache", expvar.Func(func() any {
			return ResolvedCacheStatsByStat()
		}))
	})
}

// RegisterResolvedCacheExpvarForTest forces registration under tests
// that flip CACHE_ENABLED=true via t.Setenv after init() already ran
// with the var unset. Idempotent. Production callers MUST NOT use it —
// the init() gate is the authoritative CFG-1 mechanism.
func RegisterResolvedCacheExpvarForTest() {
	registerResolvedCacheExpvar()
}

// ResolvedCacheStatsByStat flattens the live ResolvedCacheStats into the
// `stat -> value` map both the expvar key and the OTLP observable
// callback publish, so the two surfaces cannot drift.
//
// Returns an EMPTY map (not a map of zeros) when the store has not been
// constructed, so a caller can tell "no cache" from "empty cache".
//
// The per-class members are limited to the store/evict totals that
// already exist. Per-class RESIDENT and EXPIRED accounting is only half
// available — ResidentEntries/ResidentBytes are global and EvictTTLTotal
// is global — and adding it means new counters on the Put/evict path,
// which is behaviour-adjacent. Deferred to 1.13.0 (design §7b scope note).
func ResolvedCacheStatsByStat() map[string]int64 {
	c := resolvedCachePublished.Load()
	if c == nil {
		return map[string]int64{}
	}
	return resolvedCacheStatsByStatOf(c.Stats())
}

// resolvedCacheStatsByStatOf is the flattening itself, separated from the
// live-store read so the KEY SET can be derived from a zero value with no
// process-global state (1.12.6 C7 N1: the docs and matrix guards must not
// depend on whether a sibling test left the store published).
func resolvedCacheStatsByStatOf(s ResolvedCacheStats) map[string]int64 {
	return map[string]int64{
		// occupancy vs its two ceilings
		"entries":     int64(s.Entries),
		"bytes":       s.Bytes,
		"max_entries": int64(s.MaxEntries),
		"max_bytes":   s.MaxBytes,
		// lifetime effectiveness
		"hit_total":   int64(s.HitTotal),
		"miss_total":  int64(s.MissTotal),
		"store_total": int64(s.StoreTotal),
		// why entries leave — lru means the budget is the binding
		// constraint, ttl means staleness, delete means invalidation
		"evict_lru_total":               int64(s.EvictLRUTotal),
		"evict_ttl_total":               int64(s.EvictTTLTotal),
		"evict_max_age_total":           int64(s.EvictMaxAgeTotal),           // 1.12.6 C5 bounded lifetime
		"evict_no_representative_total": int64(s.EvictNoRepresentativeTotal), // #444
		"evict_delete_total":            int64(s.EvictDeleteTotal),
		// #345 — #248's resident-suppressed GAUGE (current count, up/down), not a
		// total. Non-zero DURING the UAF-decline-freeze; distinguishes decline-
		// frozen cells from never-yet-refreshed ones. Was computed by the #248
		// reaper every summary tick and thrown away into the LOG_LEVEL=warn-
		// suppressed INFO line; now on /debug/vars + OTLP (rides snowplow_resolved_
		// cache, the Int64ObservableGauge whose ObserveInt64 reports the current
		// value each callback — correct gauge semantics — via metrics.go's range
		// over ResolvedCacheStatsByStat(), OTLP-observed by construction).
		"suppressed_resident": int64(s.SuppressedResident),
		// #315 C4 detector — resident WARM cells past maxEntryAge, un-re-minted
		// (a GAUGE): the AT-RISK population the read-independent pass keeps (C3)
		// rather than cold-evicts, and keeps body-fresh via #316. Re-mint deferred.
		"warm_past_max_age": int64(s.WarmPastMaxAge),
		// #376 — resident WARM cells split by warmth SOURCE (both GAUGES). warm_seeded
		// = boot-prewarmed cells; warm_lastread = cells kept warm by a read-within-TTL
		// lastRead and NOT seeded. Decomposes the warm working set so GetNoTouch's
		// effect is observable: post-fix, warm_lastread on an unbrowsed cluster
		// collapses toward ~0 (internal reads no longer fake lastRead warmth) while
		// warm_seeded holds. Rides snowplow_resolved_cache on OTLP by construction.
		"warm_seeded":   int64(s.WarmSeeded),
		"warm_lastread": int64(s.WarmLastRead),
		// #316 — proactive refreshes ENQUEUED by the read-independent pass (a
		// monotonic total): non-zero DURING a missed-dirty-mark defect the pass is
		// catching, zero if the pass is inert. Read next to snowplow_refresher's
		// completed_total (the refresher's dedup collapses these enqueues).
		"proactive_refresh_total": int64(s.ProactiveRefreshTotal),
		// Ship 4a resident region
		"resident_entries":      int64(s.ResidentEntries),
		"resident_bytes":        s.ResidentBytes,
		"max_resident_bytes":    s.MaxResidentBytes,
		"resident_pin_total":    int64(s.ResidentPinTotal),
		"resident_demote_total": int64(s.ResidentDemoteTotal),
		// per-class store/evict — the three classes that carry their own
		// counters today
		"apistage_store_total":       int64(s.ApistageStoreTotal),
		"apistage_evict_total":       int64(s.ApistageEvictTotal),
		"widget_content_store_total": int64(s.WidgetContentStoreTotal),
		"widget_content_evict_total": int64(s.WidgetContentEvictTotal),
		"ra_full_list_store_total":   int64(s.RAFullListStoreTotal),
		"ra_full_list_evict_total":   int64(s.RAFullListEvictTotal),
		// #189 write-side generation guard — refusals of a stale in-flight Put
		// that would have resurrected a DELETE-evicted body.
		"put_refused_generation_moved_total": int64(s.PutRefusedGenerationMovedTotal),
		// #261 case-3 serve-time detector — L1 hits of an at-risk cell whose
		// serve-time first-permitting binding's roleRef was unresolved at the
		// last bindings_by_gvr index build. AT-RISK SERVE POPULATION proxy (off-
		// zero => reassess), NOT a confirmed-leak count. Rides both this expvar
		// (snowplow_resolved_cache) and the OTLP RegisterCallback that ranges
		// ResolvedCacheStatsByStat() (metrics.go) — OTLP-observed by construction.
		"serve_missed_rotation_atrisk_roleref_unresolved": int64(s.ServeMissedRotationAtriskRolerefUnresolved),
		// #261 case-2 serve-time detector — L1 hits of an at-risk cell whose
		// serve-time first-permitting binding's winning subject matched only
		// through an implicit group (system:authenticated / synthetic
		// system:serviceaccounts[:ns]). AT-RISK SERVE POPULATION proxy (off-zero
		// => reassess), NOT a confirmed-leak count. Rides both this expvar
		// (snowplow_resolved_cache) and the OTLP RegisterCallback that ranges
		// ResolvedCacheStatsByStat() (metrics.go) — OTLP-observed by construction.
		"serve_missed_rotation_atrisk_implicit_group": int64(s.ServeMissedRotationAtriskImplicitGroup),
	}
}
