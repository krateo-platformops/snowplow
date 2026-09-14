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
//	delete_queue_depth          pinned near delete_queue_cap = the drain is
//	                            behind (and, on a pre-1.12.5 image, the
//	                            signature of a dead worker).
//
// SHAPE. One expvar key, `snowplow_deps`, returning map[string]int64 keyed
// by stat — the same "one gauge keyed by stat" idiom snowplow_resolved_cache
// and snowplow_crd_discovery use, so /debug/vars and the OTLP mirror map 1:1
// with no per-stat key sprawl.
//
// NON-STARTING. The closure reads the Deps() and depWatch singletons, both of
// which are plain allocations — no goroutine, no informer, no store. Reading
// them cannot start the DELETE worker (only submitDeleteEvent does that), so
// a telemetry scrape never creates the thing it measures. Before anything has
// been recorded the map reads as zeros, which is the truthful answer for a
// tracker that exists and is idle.
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
		// evict_delete_total covers both routes an object-gone observation
		// can arrive by: an informer DELETE (OnDelete bucket 1) and the
		// refresher's confirmed self-object 404 (1.12.5 #187 (i)).
		"evict_delete_total":   int64(d.EvictDeleteTotal),
		"dirty_mark_total":     int64(d.DirtyMarkTotal),
		"enqueue_update_total": int64(d.EnqueueUpdateTotal),
		// ...of which, the refresher leg.
		"self_notfound_evict_total": int64(RefresherSelfNotFoundEvictTotal()),

		// --- the informer bridge: ADD gate ---
		"add_propagated":       int64(w.AddPropagated),
		"add_dropped_pre_sync": int64(w.AddDroppedPreSync),
		"add_nil_syncch":       int64(w.AddNilSyncCh),

		// --- the informer bridge: DELETE worker (the #187 H1 surface) ---
		"delete_queue_depth":         int64(w.DeleteQueueDepth),
		"delete_queue_cap":           int64(w.DeleteQueueCap),
		"delete_queue_full_total":    int64(w.DeleteQueueFull),
		"delete_worker_panics_total": int64(w.DeleteWorkerPanics),
	}
}
