// deps_expvar.go — 1.12.5 / #187. expvar exposure for the dependency
// tracker and the informer->DepTracker bridge.
//
// WHY THIS FILE EXISTS. DepStats and DepWatchStats have existed since
// 0.30.110 and every number in them was already computed. The ONLY place
// they went was the periodic `resolved_cache.summary` INFO line — and the
// chart ships LOG_LEVEL=warn, which suppresses INFO entirely. So when #187
// asked the live 057 pod the one question that mattered, "did DELETE-driven
// invalidation stop?", there was no surface that could answer it: the
// eviction counter, the dirty-mark counter and the DELETE queue depth were
// all computed and thrown away. Diagnosis had to proceed by inference from
// error logs instead. This is the same gap 1.12.4 §7b closed for the L1
// store; this closes it for the layer that decides what leaves the store.
//
// THE THREE NUMBERS THAT ANSWER #187:
//
//	evict_delete_total          frozen across a known CR deletion = DELETE
//	                            handling is not reaching the store.
//	delete_worker_panics_total  non-zero = at least one eviction was LOST.
//	                            Pre-1.12.5 the first panic killed the worker
//	                            process-wide; now each one costs one event,
//	                            and this is how many.
//	dep_event_queue_depth       growing without bound = the single dep-event
//	                            worker's drain is behind (1.12.6 C1: one
//	                            typed, dedup'd, unbounded queue; the 1.12.5
//	                            delete_queue_cap / _full surface is retired).
//
// SHAPE. One expvar key, `snowplow_deps`, returning map[string]int64 keyed
// by stat — the same "one gauge keyed by stat" idiom snowplow_resolved_cache
// and snowplow_crd_discovery use, so /debug/vars and the OTLP mirror map 1:1
// with no per-stat key sprawl.
//
// NON-STARTING. The closure reads the Deps() and depWatch singletons, both of
// which are plain allocations — no goroutine, no informer, no store. The
// dep-event queue (which DOES spawn client-go goroutines at construction) is
// built by startWorker on the first submitDepEvent, never by a read, so a
// telemetry scrape never creates the thing it measures — the OFF arm in
// issue1126_c1_state_derived_test.go pins this by counting goroutines across
// a DepsStatsByStat() call under CACHE_ENABLED=false (1.12.6 C1 follow-up,
// architect N1). Before anything has been recorded the map reads as zeros,
// which is the truthful answer for a tracker that exists and is idle.
//
// CFG-1 (cache-off compliance, project_cache_off_is_transparent_fallback).
// Under CACHE_ENABLED=false there is no dep tracking, so the key MUST NOT be
// registered at all — not registered-with-zeros. The gate is at init() time,
// matching resolved_cache_expvar.go, and RegisterDepsExpvarForTest is the
// sync.Once-guarded seam for tests that flip CACHE_ENABLED with t.Setenv
// after init() already ran.

package cache

import (
	"expvar"
	"sync"
)

// depsExpvarOnce guards the Publish call against the duplicate-key panic
// when both init() and the test seam run.
var depsExpvarOnce sync.Once

func init() {
	if Disabled() {
		return
	}
	registerDepsExpvar()
}

// registerDepsExpvar publishes snowplow_deps. Idempotent (sync.Once), so
// init() and the test seam can both call it.
func registerDepsExpvar() {
	depsExpvarOnce.Do(func() {
		expvar.Publish("snowplow_deps", expvar.Func(func() any {
			return DepsStatsByStat()
		}))
	})
}

// RegisterDepsExpvarForTest forces registration under tests that flip
// CACHE_ENABLED=true via t.Setenv after init() already ran with the var
// unset. Idempotent. Production callers MUST NOT use it — the init() gate is
// the authoritative CFG-1 mechanism.
func RegisterDepsExpvarForTest() {
	registerDepsExpvar()
}

// DepsStatsByStat flattens DepStats + DepWatchStats + the refresher's
// self-NotFound eviction counter into the `stat -> value` map both the
// expvar key and the OTLP observable callback publish, so the two surfaces
// cannot drift.
//
// The refresher counter belongs here rather than under snowplow_refresher
// because it counts an EVICTION: an operator asking "what left L1 because
// its object went away?" reads evict_delete_total, and this says how much of
// that arrived via the refresher's own re-fetch rather than via an informer
// DELETE event. The two are siblings and reading them apart would invite the
// wrong conclusion.
func DepsStatsByStat() map[string]int64 {
	d := Deps().Stats()
	w := DepWatchStatsSnapshot()
	rc := DepsReconcileStatsSnapshot()
	return map[string]int64{
		// --- dep records: occupancy vs its ceiling ---
		"records":      d.TotalRecords,
		"max_records":  d.MaxRecords,
		"record_total": int64(d.RecordTotal),
		// dropped_cap > 0 means new dep edges are being SILENTLY dropped:
		// entries become dirty-markable-but-not-evictable, which is the
		// #187 H4 shape. Alert on it.
		"dropped_cap":     int64(d.RecordDroppedCap),
		"dropped_no_key":  int64(d.RecordDroppedNoKey),
		"remove_l1_total": int64(d.RemoveL1Total),

		// --- what the tracker DOES with events ---
		// evict_delete_total is INFORMER-DELETE-DRIVEN ONLY. It is the H1
		// live discriminator: delete a throwaway CR with a live L1 entry and
		// watch it move; frozen means the DELETE bridge is dead. The
		// refresher's self-404 evictions deliberately do NOT fold in here
		// (architect Finding 2) — they have their own counter below, because
		// a folded number moves for two unrelated reasons and the procedure
		// stops working on any cluster that deletes CRs.
		"evict_delete_total":   int64(d.EvictDeleteTotal),
		"dirty_mark_total":     int64(d.DirtyMarkTotal),
		"enqueue_update_total": int64(d.EnqueueUpdateTotal),
		// The refresher leg: entries evicted because the re-fetch of their
		// OWN object returned a confirmed apiserver 404. evict_self_gone_total
		// is the tracker-side count (evictions that actually removed an
		// entry); self_notfound_evict_total is the refresher-side count of
		// times the branch fired. They differ only when the entry had already
		// gone by some other route.
		"evict_self_gone_total":     int64(d.EvictSelfGoneTotal),
		"self_notfound_evict_total": int64(RefresherSelfNotFoundEvictTotal()),
		// 1.12.6 C4: the drop point evicts after a deterministic NON-404
		// failure (403/500/timeout/parse/not-servable) exhausted the requeue
		// budget under the breaker. Its OWN counter, so evict_self_gone_total
		// keeps meaning "confirmed 404" during an apiserver outage (when this
		// one climbs and snowplow_refresher_drop_evict_suspended_total says
		// the breaker is holding the rest back).
		"evict_drop_point_total": int64(d.EvictDropPointTotal),

		// --- the informer bridge: ADD gate ---
		"add_propagated":       int64(w.AddPropagated),
		"add_dropped_pre_sync": int64(w.AddDroppedPreSync),
		"add_nil_syncch":       int64(w.AddNilSyncCh),

		// --- the informer bridge: the unified dep-event worker (1.12.6 C1) ---
		// events_submitted_total counts coordinates enqueued by all three
		// handlers. dep_event_queue_depth is the live pending count on the
		// typed workqueue (unbounded, dedup'd — the 1.12.5 delete_queue_cap /
		// delete_queue_full_total surface is RETIRED: there is no overflow
		// case any more). delete_worker_panics_total keeps its 1.12.5 name —
		// it is still the "an action was LOST" signature the H1 procedure
		// reads; the worker it names is now the dep-event worker.
		"events_submitted_total":     int64(w.EventsSubmitted),
		"dep_event_queue_depth":      int64(w.DeleteQueueDepth),
		"delete_worker_panics_total": int64(w.DeleteWorkerPanics),
		// probe outcomes: what the worker derived each action from.
		// probe_unknown_total is the rate at which the indexer was not
		// authoritative (relist windows, unsynced informers);
		// probe_unknown_degraded_total counts budget exhaustions — NON-ZERO
		// MEANS AN INFORMER IS NOT RECOVERING. Alert on it.
		"probe_exists_total":           int64(w.ProbeExists),
		"probe_absent_total":           int64(w.ProbeAbsent),
		"probe_unknown_total":          int64(w.ProbeUnknown),
		"probe_unknown_degraded_total": int64(w.ProbeUnknownDegraded),
		// 1.12.7 — the CONSEQUENCE of a degrade, which probe_unknown_degraded_total
		// does not carry: the verdict reached at least one dependent entry and
		// evicted nothing, so the eviction decision was deferred to the
		// refresher. Read next to probe_unknown_degraded_total: that one counts
		// budget exhaustions, this one counts the ones that had something at
		// stake.
		//
		// 1.12.8 CORRECTION — "had something at stake" was misleading as the
		// whole story, and its FIRST live reading proved it: 28,622 / 29,069 =
		// 98.5% of this counter on 057 was a SINGLE structurally unanswerable
		// probe on metrics.k8s.io, whose resources advertise ["get","list"] with
		// no `watch`. A GVR whose informer can NEVER become servable (conjunct 3
		// can never clear) contributes to this counter INDEFINITELY, at a rate
		// set by the reflector's relist cadence rather than by anything actually
		// at stake — it had a dependent entry, but no eviction was ever possible
		// or appropriate.
		//
		// So: this counter is only interpretable once the unwatchable-GVR gate
		// (resourceAuthoritativelyUnwatchable, servable.go) is in place to keep
		// such informers from being registered at all. With that gate live, a
		// non-zero reading means what the 1.12.7 sentence above says. Without
		// it, the number is dominated by a GVR that can never recover. If this
		// counter is high, FIRST check /debug/servable for a GVR with
		// watchBroken=true that is never going to clear.
		"on_object_event_degraded_no_evict_total": int64(d.OnObjectEventDegradedNoEvict),

		// --- the sampled reconcile audit (1.12.6 C3, deps_reconcile.go) ---
		// reconcile_divergence_total is THE pipeline-health number: each unit
		// is a resident entry whose object was ABSENT from a synced indexer
		// and that no event had evicted — a DELETE the pipeline lost. A
		// steady non-zero rate means a handler, the queue or the relist
		// bridge is dropping events; alert on it. reconcile_sampled_total is
		// the entries visited, reconcile_probed_total the unique coordinates
		// probed (the divergence denominator — cohort copies share one
		// probe); reconcile_unknown_total counts coordinates skipped because
		// the indexer was not authoritative; reconcile_skipped_no_edge_total
		// counts ABSENT entries the worker could not evict (no self dep edge
		// — dropped_cap), kept OUT of divergence so the alert above stays
		// true. Each key publishes the field it is named after (arch N2).
		"reconcile_ticks_total":           int64(rc.Ticks),
		"reconcile_sampled_total":         int64(rc.Sampled),
		"reconcile_probed_total":          int64(rc.Probed),
		"reconcile_divergence_total":      int64(rc.Divergence),
		"reconcile_unknown_total":         int64(rc.Unknown),
		"reconcile_skipped_no_edge_total": int64(rc.SkippedNoEdge),
		"reconcile_panics_total":          int64(rc.Panics),
	}
}
