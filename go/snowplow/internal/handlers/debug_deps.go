package handlers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// debug_deps.go — the edge-3 dep-graph diagnostic (issue #277 + C2/C3). It is
// the on-cluster instrument for the serve-seam staleness class: it answers
// "does THIS L1 cell carry a backing edge?" (?key) and surfaces the residual
// class of resident cells that carry none (?filter=missing-backing-edges) or
// have outlived the standard TTL while sliding (?filter=stale-risk).
//
// METADATA-ONLY [C-debug-surface] (feedback_debug_surface_never_dumps_per_
// identity_cache_bodies): it NEVER returns a resolved body and NEVER a pre-hash
// key input. l1Key is the ComputeKey HASH — already non-reversible — emitted
// verbatim as keyHash; username/groups/extras are never reconstructed. Edges
// are cluster resource COORDINATES (group/version/resource/namespace/name),
// which are cluster shape, not identity. The only body-derived field is
// bodySha256 (a digest, never the bytes). Same JWT gate + same metadata
// contract as /debug/apistage and its siblings (debug_routes.go).

const (
	// debugDepsStandardTTLSeconds is the store's standard entry TTL. An entry
	// whose BornAt is older than this while its CreatedAt keeps sliding under
	// refresh is a stale-risk candidate (bounded only by the 86400 hard cap).
	debugDepsStandardTTLSeconds = 3600
	// debugDepsSampleCap bounds the ?filter SAMPLE size (entries returned).
	debugDepsSampleCap = 100
	// debugDepsScanCap bounds the number of reverse-index KEYS a ?filter
	// inspects (each costs one per-key store.Get) — C-DIAG-CAP. It is
	// INDEPENDENT of the sample: on a healthy cluster the sample never fills,
	// so without this bound the scan would walk every resident key (O(resident),
	// unbounded at 50K). Hitting it sets Truncated=true.
	debugDepsScanCap = 1000
)

// depsEdgeCoord is one dependency edge as a cluster coordinate. Name == "*"
// denotes a LIST-scope edge.
type depsEdgeCoord struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Resource  string `json:"resource"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// depsKeyView is the ?key=<keyHash> response: the edges under the key plus
// metadata-only freshness for the cell (never a body).
type depsKeyView struct {
	KeyHash   string          `json:"keyHash"`
	EdgeCount int             `json:"edgeCount"`
	Edges     []depsEdgeCoord `json:"edges"`
	// Present iff a resident L1 cell exists under keyHash.
	Resident        bool   `json:"resident"`
	AgeSeconds      int64  `json:"ageSeconds,omitempty"`
	BornAgeSeconds  int64  `json:"bornAgeSeconds,omitempty"`
	Pinned          bool   `json:"pinned,omitempty"`
	CacheEntryClass string `json:"cacheEntryClass,omitempty"`
	TTLOverrideSecs int64  `json:"ttlOverrideSeconds,omitempty"`
	ExternalTTL     bool   `json:"externalTTL,omitempty"`
	BodySha256      string `json:"bodySha256,omitempty"`
	HasBackingEdge  bool   `json:"hasBackingEdge"`
}

// depsFilterSample is one flagged entry in a ?filter response.
type depsFilterSample struct {
	KeyHash        string `json:"keyHash"`
	BornAgeSeconds int64  `json:"bornAgeSeconds,omitempty"`
}

// depsFilterView is the ?filter=... response: a bounded sample + the total
// match count so an operator knows whether the sample was truncated.
type depsFilterView struct {
	Filter    string             `json:"filter"`
	Count     int                `json:"count"`
	Truncated bool               `json:"truncated"`
	Sample    []depsFilterSample `json:"sample"`
}

// DebugDeps is the read-only edge-3 dep-graph diagnostic. Modes:
//
//	?key=<keyHash>              — edges + metadata-only freshness for one cell.
//	?filter=missing-backing-edges — resident widgets/widgetContent cells whose
//	                             edge set has NO backing data GVR (only the
//	                             widget-self GVR + templates.krateo.io/*): the
//	                             C1/C2/C3 serve-seam staleness class detector.
//	?filter=stale-risk         — resident cells whose bornAgeSeconds exceeds the
//	                             standard TTL (a sliding CreatedAt kept them
//	                             resident past the intended lifetime).
//	(no params)                — a tiny usage summary (so a bare authenticated
//	                             GET is a valid 200, not a 400).
//
// @Summary Edge-3 dependency-graph diagnostic (metadata only)
// @Description Read-only. ?key=<keyHash> returns the cell's dependency edges (cluster coordinates) + metadata-only freshness; ?filter=missing-backing-edges / stale-risk surface the serve-seam staleness class. Never a resolved body, never a pre-hash key input.
// @ID debug-deps
// @Produce json
// @Success 200 {object} depsKeyView
// @Router /debug/deps [get]
func DebugDeps() http.HandlerFunc {
	return func(wri http.ResponseWriter, req *http.Request) {
		q := req.URL.Query()
		wri.Header().Set("Content-Type", "application/json")

		switch {
		case q.Get("key") != "":
			writeJSON(wri, depsKeyViewFor(q.Get("key")))
		case q.Get("filter") == "missing-backing-edges":
			writeJSON(wri, depsFilterMissingBacking())
		case q.Get("filter") == "stale-risk":
			writeJSON(wri, depsFilterStaleRisk())
		default:
			writeJSON(wri, map[string]any{
				"usage": []string{
					"/debug/deps?key=<keyHash>",
					"/debug/deps?filter=missing-backing-edges",
					"/debug/deps?filter=stale-risk",
				},
				"note": "metadata-only: never a resolved body or a pre-hash key input",
			})
		}
	}
}

func writeJSON(wri http.ResponseWriter, v any) {
	wri.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(wri).Encode(v)
}

// depsKeyViewFor builds the ?key view: the edges under keyHash plus the
// resident cell's metadata-only freshness (never a body).
func depsKeyViewFor(keyHash string) depsKeyView {
	edges := cache.Deps().EdgesUnder(keyHash)
	view := depsKeyView{
		KeyHash:   keyHash,
		EdgeCount: len(edges),
		Edges:     toEdgeCoords(edges),
	}
	entry, ok := cache.ResolvedCache().Get(keyHash)
	view.HasBackingEdge = hasBackingEdge(entry, edges)
	if !ok || entry == nil {
		return view
	}
	now := time.Now()
	view.Resident = true
	if !entry.CreatedAt.IsZero() {
		view.AgeSeconds = int64(now.Sub(entry.CreatedAt).Seconds())
	}
	if !entry.BornAt.IsZero() {
		view.BornAgeSeconds = int64(now.Sub(entry.BornAt).Seconds())
	}
	view.Pinned = entry.Pinned
	view.ExternalTTL = entry.ExternalTTL
	if entry.TTLOverride > 0 {
		view.TTLOverrideSecs = int64(entry.TTLOverride.Seconds())
	}
	if entry.Inputs != nil {
		view.CacheEntryClass = entry.Inputs.CacheEntryClass
	}
	if len(entry.RawJSON) > 0 {
		sum := sha256.Sum256(entry.RawJSON)
		view.BodySha256 = hex.EncodeToString(sum[:])
	}
	return view
}

// depsFilterMissingBacking flags resident widgets/widgetContent cells whose
// edge set carries NO backing data GVR (only the widget-self GVR +
// templates.krateo.io/*) — the serve-seam staleness class. Lock-free reverse
// scan; the sample + per-key store.Get calls are capped.
func depsFilterMissingBacking() depsFilterView {
	out := depsFilterView{Filter: "missing-backing-edges"}
	inspected := 0
	cache.Deps().RangeEdges(func(l1Key string, edges []cache.DepKey) bool {
		if inspected >= debugDepsScanCap {
			// C-DIAG-CAP: bound the per-key store.Get scan work independently of
			// the sample (which may never fill on a healthy cluster).
			out.Truncated = true
			return false
		}
		inspected++
		entry, ok := cache.ResolvedCache().Get(l1Key)
		if !ok || entry == nil || entry.Inputs == nil {
			return true
		}
		if !isWidgetClass(entry.Inputs.CacheEntryClass) {
			return true
		}
		if hasBackingEdge(entry, edges) {
			return true
		}
		out.Count++
		if len(out.Sample) < debugDepsSampleCap {
			out.Sample = append(out.Sample, depsFilterSample{KeyHash: l1Key})
		} else {
			out.Truncated = true
		}
		return true
	})
	return out
}

// depsFilterStaleRisk flags resident cells whose bornAgeSeconds exceeds the
// standard TTL (a sliding CreatedAt kept them resident past their intended
// lifetime; bounded only by the 86400 hard cap).
func depsFilterStaleRisk() depsFilterView {
	out := depsFilterView{Filter: "stale-risk"}
	now := time.Now()
	inspected := 0
	// RangeKeys (not RangeEdges): stale-risk needs only the key, so skip
	// materializing each key's edge slice.
	cache.Deps().RangeKeys(func(l1Key string) bool {
		if inspected >= debugDepsScanCap {
			out.Truncated = true // C-DIAG-CAP
			return false
		}
		inspected++
		entry, ok := cache.ResolvedCache().Get(l1Key)
		if !ok || entry == nil || entry.BornAt.IsZero() {
			return true
		}
		bornAge := int64(now.Sub(entry.BornAt).Seconds())
		if bornAge <= debugDepsStandardTTLSeconds {
			return true
		}
		out.Count++
		if len(out.Sample) < debugDepsSampleCap {
			out.Sample = append(out.Sample, depsFilterSample{KeyHash: l1Key, BornAgeSeconds: bornAge})
		} else {
			out.Truncated = true
		}
		return true
	})
	return out
}

// toEdgeCoords projects DepKeys to JSON coordinate structs.
func toEdgeCoords(edges []cache.DepKey) []depsEdgeCoord {
	out := make([]depsEdgeCoord, 0, len(edges))
	for _, e := range edges {
		out = append(out, depsEdgeCoord{
			Group:     e.GVR.Group,
			Version:   e.GVR.Version,
			Resource:  e.GVR.Resource,
			Namespace: e.Namespace,
			Name:      e.Name,
		})
	}
	return out
}

// hasBackingEdge reports whether edges include at least one BACKING data GVR —
// any edge that is neither the entry's own widget-self GVR nor a
// templates.krateo.io/* coordinate (the RESTAction / widget template the widget
// always depends on). An entry with none is a serve-seam staleness candidate.
//
// N3 — FALSE-NEGATIVE DIRECTION (the detector UNDER-reports, never over-reports).
// This is a heuristic: any non-self, non-templates edge counts as "backing". A
// widget that declares a STATIC resourcesRefs dependency (e.g. a ConfigMap it
// reads directly) carries such an edge, so it reads as has-backing EVEN IF the
// RA-derived edge-3 is missing — it would NOT be flagged. So a low/zero
// missing-backing-edges count does NOT prove every cell is edge-3-complete;
// treat it as "no OBVIOUS serve-seam gap", not a clean bill. It never
// false-POSITIVES: a flagged cell genuinely has no backing GVR at all.
func hasBackingEdge(entry *cache.ResolvedEntry, edges []cache.DepKey) bool {
	selfG, selfR := "", ""
	if entry != nil && entry.Inputs != nil {
		selfG, selfR = entry.Inputs.Group, entry.Inputs.Resource
	}
	for _, e := range edges {
		if e.GVR.Group == selfG && e.GVR.Resource == selfR {
			continue // the widget-self edge
		}
		if e.GVR.Group == "templates.krateo.io" {
			continue // the RESTAction / widget template (always present)
		}
		return true
	}
	return false
}

func isWidgetClass(class string) bool {
	return class == "widgets" || class == "widgetContent"
}
