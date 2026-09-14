package handlers

import (
	"encoding/json"
	"net/http"
	"sort"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// debugApistageBody is the JSON body returned by /debug/apistage.
type debugApistageBody struct {
	// CacheEnabled mirrors the resolved-output cache state. When the
	// resolved cache is off there are no entries to report (Entries is
	// empty) — the endpoint still returns 200 so it is usable as a
	// diagnostic regardless of the flag.
	CacheEnabled bool `json:"cacheEnabled"`
	// Count is len(Entries) — convenience for a quick "how many entries".
	Count int `json:"count"`
	// Entries is the per-entry METADATA snapshot (sorted by key hash for
	// stable output across requests).
	Entries []cache.ResolvedEntryMeta `json:"entries"`
}

// DebugApistage is the read-only resolved-output-cache entry diagnostic
// (R1 design §6 Mode 1, docs/design-r1-allcompositionresources-invalidation-2026-06-26.md).
//
// It dumps, per cached resolved-output entry, METADATA ONLY:
// class / key-hash / GVR coordinates / derived path / stage hash / age /
// ttl-remaining / pinned / items_count / rawjson_bytes — the signal needed
// to diagnose a degraded apistage entry (e.g. an `allCompositionResources`
// entry whose `path` is `/api/v1/configmaps` with an `itemsCount` reflecting
// the cluster-wide LIST, or a stale `getComposition` entry with a large
// `ageSeconds`) without a kubectl exec into the pod, and to VERIFY R1 (after
// a composition update, `ageSeconds` resets + the path/items_count reflect
// the 26 by-name fan-out).
//
// STRUCTURAL LEAK GUARD (PM F-2): resolved output is per-identity
// RBAC-sensitive; a content dump would be a cross-user leak. This handler
// returns cache.ResolvedEntryMeta values produced by RangeMetadata, a type
// that is STRUCTURALLY INCAPABLE of carrying RawJSON / parsed Items / the
// Extras key-inputs (no []byte, no slice-of-object, no map field). The guard
// is the type, not this comment — see
// TestRangeMetadata_StructurallyCannotLeakContent.
//
// READ-ONLY: RangeMetadata takes the store mutex in a read-walk and mutates
// NO state — no Put, no evict, no LRU touch. Calling it has zero effect on
// serving behaviour.
//
// NOT gated behind the cache flag: when the resolved cache is off the
// snapshot is empty but the endpoint still returns 200 with
// cacheEnabled=false, so it is available for debugging in both modes.
// Mounted next to /debug/servable and /debug/vars (operator-level, NOT the
// per-user /call surface).
//
// @Summary Resolved-cache entry metadata diagnostic
// @Description Read-only metadata-only snapshot of resolved-output cache entries (class/path/gvr/age/ttl/items_count). Never returns resolved bodies.
// @ID debug-apistage
// @Produce  json
// @Success 200 {object} debugApistageBody
// @Router /debug/apistage [get]
func DebugApistage() http.HandlerFunc {
	return func(wri http.ResponseWriter, req *http.Request) {
		store := cache.ResolvedCache() // nil when the resolved cache is off
		var rows []cache.ResolvedEntryMeta

		// 1.12.5 / #187 — single-entry lookup. The whole #187 inference chain
		// had to be built from the client's subsequent child fetches because
		// nobody could look at ONE resident entry and say how old it was or
		// whether its body had changed. `?key_hash=<hex>` answers that in one
		// request; the body sha256 is populated only on this path (hashing
		// every body under the store mutex would stall serving), and it is a
		// HASH rather than the body because L1 cells are per-identity and a
		// body dump would be a cross-identity read.
		if kh := req.URL.Query().Get("key_hash"); kh != "" {
			body := debugApistageBody{CacheEnabled: store != nil}
			if store != nil {
				if meta, ok := store.MetadataForKey(kh); ok {
					body.Entries = []cache.ResolvedEntryMeta{meta}
					body.Count = 1
				}
			}
			wri.Header().Set("Content-Type", "application/json")
			wri.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(wri).Encode(body)
			return
		}

		// RangeMetadata is nil-receiver safe; guard anyway for clarity.
		if store != nil {
			store.RangeMetadata(func(m cache.ResolvedEntryMeta) bool {
				rows = append(rows, m)
				return true
			})
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].KeyHash < rows[j].KeyHash })

		body := debugApistageBody{
			CacheEnabled: store != nil,
			Count:        len(rows),
			Entries:      rows,
		}
		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(wri).Encode(body)
	}
}
