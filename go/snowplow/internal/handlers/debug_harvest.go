package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// debugHarvestBody is the JSON body returned by /debug/harvest.
type debugHarvestBody struct {
	// Available is false when no harvester has been published — prewarm is
	// off, or boot has not reached the engine yet. Distinct from "the
	// harvesters are empty", which is a healthy post-forget state; conflating
	// the two would let a misconfigured pod read as a successful forget.
	Available bool `json:"available"`
	// NavEntries counts harvested ENTRIES, not distinct widgets: a widget is
	// held once per pagination tuple it was reached under and the seed writes
	// a cell per entry.
	NavEntries int `json:"navEntries"`
	// ApiRefs counts distinct RESTAction references (deduped by ns/name).
	ApiRefs int `json:"apiRefs"`

	// Queried echoes the coordinate when one was asked about, so a captured
	// response is self-describing.
	Queried *debugHarvestCoordinate `json:"queried,omitempty"`
	// HeldByNavHarvester / HeldByApiRefHarvester answer the single question
	// this endpoint exists for. Only meaningful when Queried is set.
	HeldByNavHarvester    bool `json:"heldByNavHarvester,omitempty"`
	HeldByApiRefHarvester bool `json:"heldByApiRefHarvester,omitempty"`
}

// debugHarvestCoordinate is a coordinate — and nothing else.
type debugHarvestCoordinate struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Resource  string `json:"resource"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// DebugHarvest is the read-only Phase-1 harvester diagnostic (1.12.7).
//
// WHAT IT IS FOR. 1.12.7 F6b/F6c drop a confirmed-gone object from both
// harvesters, which is what stops a deleted widget being re-resolved into L1
// on every seed pass. That state was exposed nowhere, so the fix could be
// verified in tests and not in production — and the revised acceptance step
// has to show the harvester actually FORGOT the coordinate. The counter
// snowplow_phase1_harvest_forgotten_total says a forget happened; this says
// what is held right now, which is what you check after deleting an object.
//
// TWO MODES, mirroring /debug/apistage:
//
//	GET /debug/harvest
//	    counts only — how many entries each harvester holds.
//	GET /debug/harvest?group=&version=&resource=&namespace=&name=
//	    the same counts plus two booleans for that one coordinate.
//	    `group` may be empty for a core-group resource; `resource` and
//	    `name` are required for a lookup.
//
// METADATA ONLY — NEVER CONTENT, and structurally so. The harvested value is
// a whole widget CR, and what the seed makes of it is per-identity resolved
// output, so dumping any of it here would be the cross-identity read the
// standing rule forbids. The accessors behind this handler return only ints
// and bools (see dispatchers.HarvestCounts / HarvestHoldsCoordinate): there is
// no field on debugHarvestBody that can carry a captured object, which is the
// guard — not this comment.
//
// READ-ONLY: both accessors take a snapshot copy and mutate nothing. Calling
// this has no effect on what the seed will do.
//
// Mounted next to /debug/apistage and /debug/servable, behind the same JWT
// gate (operator-level, NOT the per-user /call surface).
//
// @Summary Phase-1 harvester diagnostic
// @Description Read-only counts of what the Phase-1 harvesters hold, plus a per-coordinate "is it still held?" lookup. Metadata only; never returns harvested objects.
// @ID debug-harvest
// @Produce  json
// @Success 200 {object} debugHarvestBody
// @Router /debug/harvest [get]
func DebugHarvest() http.HandlerFunc {
	return func(wri http.ResponseWriter, req *http.Request) {
		navEntries, apiRefs, ok := dispatchers.HarvestCounts()
		body := debugHarvestBody{
			Available:  ok,
			NavEntries: navEntries,
			ApiRefs:    apiRefs,
		}

		q := req.URL.Query()
		resource, name := q.Get("resource"), q.Get("name")
		if resource != "" && name != "" {
			coord := debugHarvestCoordinate{
				Group:     q.Get("group"),
				Version:   q.Get("version"),
				Resource:  resource,
				Namespace: q.Get("namespace"),
				Name:      name,
			}
			body.Queried = &coord
			gvr := schema.GroupVersionResource{
				Group: coord.Group, Version: coord.Version, Resource: coord.Resource,
			}
			nav, apiRef, avail := dispatchers.HarvestHoldsCoordinate(gvr, coord.Namespace, coord.Name)
			// avail can only disagree with ok under a concurrent boot; trust
			// the later read so the booleans and Available never contradict.
			body.Available = avail
			body.HeldByNavHarvester = nav
			body.HeldByApiRefHarvester = apiRef
		}

		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(wri).Encode(body)
	}
}
