// store_repair.go — snowplow#237 deliverable B: the repair verb for the
// divergences the forced pass finds. Since #870 the verb is a BUILD
// (store_swap.go): the replacement store is built beside the live one and
// swapped in, and only what changed is marked.
//
// # WHY THE FORCED PASS NEEDS A REPAIR VERB AND THE SNAPSHOT PATH DOES NOT
//
// On the snapshot path there is a Replace, so repair is client-go's and it is
// already correct: processDeltas writes the store and THEN calls the handler.
// On the forced pass there is no Replace, so without a verb the pass would
// detect and do nothing — and routing it to submitDepEvent would be worse than
// nothing: that lands on probeObjectState -> GetByKey on the STILL-STALE
// indexer -> objExists -> dirty-mark -> the refresher re-resolves from the same
// stale store and reproduces the same stale bytes, turning the counter green
// over a still-wrong widget. #237 measured exactly that shape from the outside:
// eight consecutive /call requests over twelve minutes returning a
// byte-identical stale body.
//
// A detector that fires while the symptom persists is worse than no detector,
// because it converts an open investigation into a closed one.
//
// # WHY NOT CORRECT THE KEYS IN PLACE (design v5 §7, measured)
//
//  1. The only ordered writer of the store is processDeltas under the
//     informer's private FIFO lock, and the Indexer has no conditional write:
//     store 10, GET 20, watch 30, correction written — the store ends at 20
//     while the apiserver holds 30 (TestH1, 3 of 3).
//  2. A correction's notification travels outside the listener order: the
//     store holds 25 while the handler last delivered 20 (TestH2, 3 of 3).
//  3. Injecting through the watch moves lastSyncRV, and exists only for
//     streaming informers.
//  4. CompareResourceVersion accepts only integer resourceVersions, so
//     aggregated APIs would need a special case.
//
// # THE BOUNDS THAT REMAIN
//
//   - ONE BUILD PER GVR. A re-detection while a build is open JOINS it: it
//     adds its keys (the earliest detection kept per key) and never constructs
//     a second copy.
//   - CIRCUIT BREAKER (bound 3). If the next verification of a GVR — on the
//     NEW copy (results are bound to the instance they compared, §3) — still
//     reports divergence after a swap, repairs of it stop and the breaker
//     latches and raises (noteVerified). Detection continues under suppression
//     (bound 4): silence must never be produced by the breaker.
//   - NO SIZE GATE (bound 5): the 50K composition GVR is where staleness hurts
//     most.
//   - THE GVR MUST BE REBUILDABLE (bound 6, #244): only an owned informer can
//     have a replacement built beside it. See requestStoreBuild.
//
// # WHAT #870 DELETED (owner ruling 2)
//
// The serialised repair queue and its worker, the one-at-a-time bound across
// all GVRs, the 2-minute sync wait (relistPostSyncWait), the idle poll
// (storeRepairIdlePoll), and the queue gauges store_repairs_pending /
// store_repair_queue_age_seconds. Serialisation existed because a repair made
// its GVR non-servable for the rebuild window, so N concurrent repairs sent N
// widget classes to the apiserver at once. A build never makes its GVR
// non-servable, so there is no blast radius to contain; and every build ends
// on an event (store_swap.go §2.4), so nothing waits on a timer. The open
// windows are visible on store_swap_known_stale_window_open_seconds_max.
package cache

import "k8s.io/apimachinery/pkg/runtime/schema"

// requestStoreRepair asks for a build with reason divergence for gvr, carrying
// the confirmed-divergent keys. Returns false when it was refused (not
// registered, factory-built — counted on store_repair_unsupported_total — or
// suppressed by the breaker).
func requestStoreRepair(gvr schema.GroupVersionResource, keys []string) bool {
	rw := Global()
	if rw == nil {
		return false
	}
	return rw.requestStoreBuild(gvr, reasonDivergence, keys)
}

// confirmedKeys is every key a report confirmed divergent, whatever its class:
// the build's known-stale coordinates (owner ruling 3).
func (r divergenceReport) confirmedKeys() []string {
	out := make([]string, 0, r.total())
	for _, ks := range [][]string{r.lostUpdate, r.lostDelete, r.lostAdd, r.uidMismatch} {
		out = append(out, ks...)
	}
	return out
}
