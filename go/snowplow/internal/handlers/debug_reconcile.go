package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// debugReconcileBody is the JSON body returned by /debug/reconcile.
type debugReconcileBody struct {
	// CacheEnabled is false when the resolved cache is off or no watcher is
	// installed; the audit then has nothing to walk and every count is 0.
	CacheEnabled bool `json:"cacheEnabled"`
	// DryRun echoes cache.ReconcileReport.DryRun (#238): true when this body was
	// produced by ?dryRun=1 (the divergent set was REPORTED, not evicted).
	// Always present (no omitempty) so a saved reconcile.json states its mode in
	// BOTH modes and can be trusted afterwards as a pre-repair observation.
	DryRun bool `json:"dryRun"`
	// Sampled / Probed / Divergent / Unknown / SkippedNoEdge mirror
	// cache.ReconcileReport.
	Sampled   int `json:"sampled"`
	Probed    int `json:"probed"`
	Divergent int `json:"divergent"`
	Unknown   int `json:"unknown"`
	// SkippedNoEdge: ABSENT entries with no self dep edge (dropped_cap) —
	// not evictable by the worker, counted apart from divergence.
	SkippedNoEdge int `json:"skippedNoEdge"`
	// Batches / SnapshotHoldMicros / MaxBatchHoldMicros / Truncated: the
	// chunked walk's measured store-mutex holds (PM condition 3).
	Batches            int   `json:"batches"`
	SnapshotHoldMicros int64 `json:"snapshotHoldMicros"`
	MaxBatchHoldMicros int64 `json:"maxBatchHoldMicros"`
	Truncated          bool  `json:"truncated"`
	// Entries is the divergent set, METADATA ONLY (key hash / class / gvr /
	// namespace / name) — never a body, never a body hash.
	Entries []cache.ReconcileEntry `json:"entries"`
}

// DebugReconcile is the opt-in FULL reconcile audit (1.12.6 C3,
// internal/cache/deps_reconcile.go). It walks every resident resolved-cache
// entry, probes each entry's own object against the informer indexer and
// hands every ABSENT coordinate to the dep-event worker — the same path an
// informer DELETE takes — then reports the divergent set.
//
// DEFAULT IS A RECONCILE, NOT A DRY RUN: without ?dryRun, divergent entries are
// evicted by the worker after this returns. That is the point (an operator who
// suspects a stranded entry wants it gone), and the reason the route sits
// behind the debug JWT gate with its siblings rather than being anonymous.
//
// ?dryRun=1 (#238) REPORTS the divergent set WITHOUT evicting it and WITHOUT
// moving any reconcile counter — observe-before-repair. The #237 need: the
// informer store and the L1 layer produce byte-identical /call output, so the
// only way to tell "store stale" from "cell stale" is to look BEFORE anything
// repairs, and every other lever (an annotation poke firing a watch UPDATE)
// repairs the store as it clears the cache. The response echoes the mode in
// "dryRun" so a captured reconcile.json is self-describing. `dryRun` is parsed
// with strconv.ParseBool: absent → the reconcile default (unchanged);
// present-but-unparseable → 400 (never silently fall to the DESTRUCTIVE
// default).
//
// COST: the full walk is CHUNKED (cache.RangeMetadataBatched): the store
// mutex is held once for a key snapshot and then per batch of 512 entries,
// with the probes between batches outside it, so a customer /call waits at
// most one batch (measured, reported as maxBatchHoldMicros) — never the
// whole residency. Wall time is capped (truncated=true past it). Fine on
// demand; still not something to poll.
//
// STRUCTURAL LEAK GUARD: the rows are cache.ReconcileEntry — five strings
// (key hash / class / gvr / ns / name), the same projection /debug/apistage
// exposes. No body, no hash, no key inputs. Resolved output is per-identity
// RBAC-sensitive; this surface cannot carry it.
//
// @Summary Full resolved-cache reconcile audit
// @Description Walks every resident resolved-output cache entry, probes its own object against the informer indexer and evicts (via the dep-event worker) those whose object is absent. Returns the divergent set as metadata only. Never returns resolved bodies. With dryRun=1 the divergent set is reported WITHOUT eviction (observe-before-repair, #238); the response echoes the mode in the dryRun field.
// @ID debug-reconcile
// @Produce  json
// @Param dryRun query bool false "report the divergent set without evicting it (observe-before-repair)"
// @Success 200 {object} debugReconcileBody
// @Failure 400 {string} string "unparseable dryRun"
// @Router /debug/reconcile [get]
func DebugReconcile() http.HandlerFunc {
	return func(wri http.ResponseWriter, req *http.Request) {
		// ?dryRun: absent → false (reconcile, today's default); present but
		// unparseable → 400, never a silent fall to the DESTRUCTIVE default.
		dryRun := false
		if q := req.URL.Query(); q.Has("dryRun") {
			v, err := strconv.ParseBool(q.Get("dryRun"))
			if err != nil {
				http.Error(wri, "dryRun must be a boolean (1/true/0/false)", http.StatusBadRequest)
				return
			}
			dryRun = v
		}

		rep, ok := cache.ReconcileFullDryRun(dryRun)
		body := debugReconcileBody{
			CacheEnabled:       ok,
			DryRun:             rep.DryRun,
			Sampled:            rep.Sampled,
			Probed:             rep.Probed,
			Divergent:          rep.Divergent,
			Unknown:            rep.Unknown,
			SkippedNoEdge:      rep.SkippedNoEdge,
			Batches:            rep.Batches,
			SnapshotHoldMicros: rep.SnapshotHoldMicros,
			MaxBatchHoldMicros: rep.MaxBatchHoldMicros,
			Truncated:          rep.Truncated,
			Entries:            rep.Entries,
		}
		if body.Entries == nil {
			body.Entries = []cache.ReconcileEntry{}
		}
		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(wri).Encode(body)
	}
}
