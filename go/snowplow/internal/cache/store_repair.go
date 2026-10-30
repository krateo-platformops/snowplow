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
// the confirmed-divergent keys of a loss judged against ONE copy. live (nil:
// unbound) reports, under the same rw.mu as the build decision, whether that
// copy is still the GVR's live one: a judgement of a copy a swap has replaced
// is dropped (verification_skipped{instance_changed}). The loss is evidence
// about the replaced copy only; the new copy was built from its own LIST and
// is judged by its own next pass (#870 F6; #928 delta). When that LIST is at
// least as new as the loss's read, the new copy already holds the read's
// state, and a request would start a second, redundant build. When it is
// older (an apiserver-wide event drop every watcher missed), the new copy
// diverges too, and its own pass detects and repairs it within one
// verification window: the delay F6 already accepts for a pass open across a
// swap (#928 delta review, residual). Returns false when it was refused
// (not registered, factory-built — counted on store_repair_unsupported_total —
// suppressed by the breaker) or dropped.
func requestStoreRepair(gvr schema.GroupVersionResource, keys []string, live func(rw *ResourceWatcher) bool) bool {
	// Bound 6 first, from the RECORDED property alone: a factory-built GVR is
	// refused, out loud, whatever state the watcher is in.
	if owned, known, _ := storeVerificationOwnership(gvr); known && !owned {
		refuseUnownedBuild(gvr, reasonDivergence, keys)
		return false
	}
	rw := Global()
	if rw == nil {
		return false
	}
	return rw.requestStoreBuildBound(gvr, reasonDivergence, keys, live)
}

// liveInstanceIs and liveDeliveryLogIs are the bindings a repair request
// carries (requestStoreRepair): the copy a loss was judged against — the
// instance a forced pass compared, or the delivery log whose pending key
// resolved — is still the GVR's live one. Evaluated under rw.mu.
func liveInstanceIs(gvr schema.GroupVersionResource, inst *storeInstance) func(rw *ResourceWatcher) bool {
	return func(rw *ResourceWatcher) bool { return inst != nil && rw.instances[gvr] == inst }
}

func liveDeliveryLogIs(log *deliveryLog) func(rw *ResourceWatcher) bool {
	return func(rw *ResourceWatcher) bool { return log != nil && rw.deliveries[log.gvr] == log }
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
