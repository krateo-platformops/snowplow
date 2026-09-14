package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// debugReconcileBody is the JSON body returned by /debug/reconcile.
type debugReconcileBody struct {
	// CacheEnabled is false when the resolved cache is off or no watcher is
	// installed; the audit then has nothing to walk and every count is 0.
	CacheEnabled bool `json:"cacheEnabled"`
	// Sampled / Probed / Divergent / Unknown mirror cache.ReconcileReport.
	Sampled   int `json:"sampled"`
	Probed    int `json:"probed"`
	Divergent int `json:"divergent"`
	Unknown   int `json:"unknown"`
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
// NOT A DRY RUN: divergent entries are evicted by the worker after this
// returns. That is the point (an operator who suspects a stranded entry
// wants it gone), and the reason the route sits behind the debug JWT gate
// with its siblings rather than being anonymous.
//
// COST: the full walk holds the store mutex for its duration (RangeMetadata),
// unlike the periodic ticker's bounded sample. Fine on demand; not something
// to poll.
//
// STRUCTURAL LEAK GUARD: the rows are cache.ReconcileEntry — five strings
// (key hash / class / gvr / ns / name), the same projection /debug/apistage
// exposes. No body, no hash, no key inputs. Resolved output is per-identity
// RBAC-sensitive; this surface cannot carry it.
//
// @Summary Full resolved-cache reconcile audit
// @Description Walks every resident resolved-output cache entry, probes its own object against the informer indexer and evicts (via the dep-event worker) those whose object is absent. Returns the divergent set as metadata only. Never returns resolved bodies.
// @ID debug-reconcile
// @Produce  json
// @Success 200 {object} debugReconcileBody
// @Router /debug/reconcile [get]
func DebugReconcile() http.HandlerFunc {
	return func(wri http.ResponseWriter, req *http.Request) {
		rep, ok := cache.ReconcileFull()
		body := debugReconcileBody{
			CacheEnabled: ok,
			Sampled:      rep.Sampled,
			Probed:       rep.Probed,
			Divergent:    rep.Divergent,
			Unknown:      rep.Unknown,
			Entries:      rep.Entries,
		}
		if body.Entries == nil {
			body.Entries = []cache.ReconcileEntry{}
		}
		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(wri).Encode(body)
	}
}
