// reflector_path_stats.go — #237 A3's tagged counter family.
//
// COUNTER HONESTY (feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector).
// Every stat below gets both readings, because several of them read ZERO in
// the healthy state and zero is also what a broken instrument reads.
//
//	stat                              during "we silently fell back to LIST"   healthy
//	--------------------------------- ---------------------------------------- ----------------
//	transport_requests_total          climbing                                  climbing
//	collection_requests_total         climbing                                  climbing
//	watchlist_established_total       0 or FLAT                                 climbs
//	list_cache_eligible_total         climbing                                  ~0 after boot
//	list_page_total                   climbing                                  ~0 after boot
//	list_etcd_delegated_total         climbing                                  0 AFTER C1 SHIPS
//	watch_plain_total                 climbs either way                         climbs
//	path_transitions_total            +1 per GVR that flipped                   flat after boot
//
// The two denominators come FIRST because they are what make every zero below
// them readable, and they are the pair that detects a broken detector:
//
//   - transport_requests_total == 0        → the wrapper is not installed, or
//     the process issues no requests at all. Every other zero here is then
//     meaningless.
//   - transport_requests_total > 0 AND collection_requests_total == 0 → the
//     wrapper IS installed and sees traffic but recognises no LIST or WATCH at
//     all. That is a broken classifier (an API path shape it cannot parse),
//     and it is the reading that would otherwise masquerade as "the reflector
//     is quiet". ALARMABLE.
//
// There is deliberately no "unclassified" bucket: every collection GET lands
// in exactly one of the five by construction, so such a counter could only
// ever read 0 — a counter incapable of being non-zero is not an instrument.
// The (transport > 0, collection == 0) pair carries that meaning instead, and
// it is reachable.
//
// NAMES. The stat tags carry the reflector_ prefix verbatim so the counter
// #237 and the C1 follow-up name — reflector_list_etcd_delegated_total — is
// greppable on the wire. Its full address is
// /debug/vars → snowplow_reflector_path.reflector_list_etcd_delegated_total,
// and on OTLP snowplow_reflector_path{stat="reflector_list_etcd_delegated_total"}.
package cache

import (
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Process-scoped counters. Package-level rather than per-ResourceWatcher: the
// transport wrapper is installed on the shared rest.Config and covers informer
// families that have no watcher (secrets, controller-health), and an operator
// asking "which path are our reflectors taking" wants one number per process.
var (
	reflectorTransportRequestsTotal    atomic.Uint64
	reflectorCollectionRequestsTotal   atomic.Uint64
	reflectorWatchListEstablishedTotal atomic.Uint64
	reflectorWatchPlainTotal           atomic.Uint64
	reflectorListPageTotal             atomic.Uint64
	reflectorListEtcdDelegatedTotal    atomic.Uint64
	reflectorListCacheEligibleTotal    atomic.Uint64
	reflectorPathTransitionsTotal      atomic.Uint64
)

// ReflectorPathStats is the tagged family.
type ReflectorPathStats struct {
	TransportRequestsTotal    uint64 `stat:"reflector_transport_requests_total" desc:"Every outbound request the reflector-path wrapper delegated. DENOMINATOR, not a detector: a zero here makes every other zero in this family meaningless."`
	CollectionRequestsTotal   uint64 `stat:"reflector_collection_requests_total" desc:"Collection-endpoint GETs (LIST or WATCH) classified into the five buckets below. Non-zero transport_requests with a ZERO here means the wrapper is installed but recognises no LIST at all — a broken classifier, not a quiet reflector."`
	WatchListEstablishedTotal uint64 `stat:"reflector_watchlist_established_total" desc:"WATCHes carrying sendInitialEvents: the initial state streamed from the apiserver watch cache. Read against the two list_* counters — a flat value here alone proves nothing."`
	WatchPlainTotal           uint64 `stat:"reflector_watch_plain_total" desc:"WATCHes without sendInitialEvents: ordinary re-watch after a clean timeout. Climbs under BOTH regimes, so it is not a discriminator; it is the proof the wrapper is wired."`
	ListPageTotal             uint64 `stat:"reflector_list_page_total" desc:"LISTs carrying a continue token. A continue page is an etcd read by definition and the page size bounds it — correct traffic, not a bypass."`
	ListEtcdDelegatedTotal    uint64 `stat:"reflector_list_etcd_delegated_total" desc:"LISTs carrying a limit together with a resourceVersion that is neither empty nor 0: delegated to etcd, SKIPPING the watch cache. Non-zero by construction on today's binary; must read 0 once listOptionsTweak mirrors client-go's rule."`
	ListCacheEligibleTotal    uint64 `stat:"reflector_list_cache_eligible_total" desc:"LISTs the apiserver watch cache can serve (resourceVersion empty or 0, or no limit)."`
	PathTransitionsTotal      uint64 `stat:"reflector_path_transitions_total" desc:"Per-GVR reflector-path changes, including the first observation of each GVR. Flat after boot in steady state; a later increment means a GVR flipped between watchlist and list and is paired with a WARN naming it."`
}

// reflectorPathStatsOverride is the test seam every C7 family carries.
var reflectorPathStatsOverride atomic.Pointer[ReflectorPathStats]

// SetReflectorPathStatsForTest installs (or clears, with nil) a fixed snapshot.
func SetReflectorPathStatsForTest(s *ReflectorPathStats) {
	reflectorPathStatsOverride.Store(s)
}

// ReflectorPathStatsSnapshot reads the family coherently.
func ReflectorPathStatsSnapshot() ReflectorPathStats {
	if o := reflectorPathStatsOverride.Load(); o != nil {
		return *o
	}
	return ReflectorPathStats{
		TransportRequestsTotal:    reflectorTransportRequestsTotal.Load(),
		CollectionRequestsTotal:   reflectorCollectionRequestsTotal.Load(),
		WatchListEstablishedTotal: reflectorWatchListEstablishedTotal.Load(),
		WatchPlainTotal:           reflectorWatchPlainTotal.Load(),
		ListPageTotal:             reflectorListPageTotal.Load(),
		ListEtcdDelegatedTotal:    reflectorListEtcdDelegatedTotal.Load(),
		ListCacheEligibleTotal:    reflectorListCacheEligibleTotal.Load(),
		PathTransitionsTotal:      reflectorPathTransitionsTotal.Load(),
	}
}

// ReflectorPathStatsByStat is the family's Values function.
func ReflectorPathStatsByStat() map[string]any {
	return statsByTag(ReflectorPathStatsSnapshot())
}

// ResetReflectorPathStatsForTest zeroes every counter, the per-GVR map and the
// override, so an arm starts from a known state.
func ResetReflectorPathStatsForTest() {
	reflectorTransportRequestsTotal.Store(0)
	reflectorCollectionRequestsTotal.Store(0)
	reflectorWatchListEstablishedTotal.Store(0)
	reflectorWatchPlainTotal.Store(0)
	reflectorListPageTotal.Store(0)
	reflectorListEtcdDelegatedTotal.Store(0)
	reflectorListCacheEligibleTotal.Store(0)
	reflectorPathTransitionsTotal.Store(0)
	reflectorPathStatsOverride.Store(nil)
	reflectorPaths.mu.Lock()
	reflectorPaths.registered = map[schema.GroupVersionResource]struct{}{}
	reflectorPaths.current = map[schema.GroupVersionResource]string{}
	reflectorPaths.mu.Unlock()
}
