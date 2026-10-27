// stage_outcomes_header.go — #904: every RESTAction /call answer carries
// X-Snowplow-Stage-Outcomes, a cold resolve and an L1 hit alike.
//
// The cold path sets it from the resolve's own collector (restactions.go). A
// hit answers with the value the resolve that STORED the cell produced, kept on
// the entry (cache.ResolvedEntry.StageOutcomes) by newResolveEntry, the one
// constructor every restactions-class Put site that resolves builds with: the
// /call dispatcher, the refresher and the cohort seed.
// "" on the entry means every stage succeeded, the header's [] — so a cell
// stored by a site that never failed a stage costs no memory for it.
package dispatchers

import (
	"net/http"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// stageOutcomesAllSucceeded is the header value of a resolve with no failed,
// skipped or not-run stage.
const stageOutcomesAllSucceeded = "[]"

// newResolveEntry is THE constructor of the L1 entry a dispatcher Put stores
// for a resolved body that may be a restactions-class cell: the /call
// dispatcher, the cohort seed and the refresher (which re-Puts every class)
// all build through it, so the stage outcomes travel with every stored
// RESTAction body by construction, never by a hand-placed stamp (#904, #911
// re-review). It sets what every such entry needs — the body, its key inputs,
// the UAF TTL cap and, for a restactions-class cell only (the only cell that
// answers a RESTAction /call), the outcomes ("" when every stage succeeded).
// Callers add their site-specific fields.
func newResolveEntry(class string, raw []byte, inputs *cache.ResolvedKeyInputs, outcomes *api.StageOutcomes) *cache.ResolvedEntry {
	// put-verdict: a constructor, not a Put site — each caller (restActionHandler.ServeHTTP, seedRestactionResolveAndPutProd, resolveAndPopulateL1) decides whether to store before it Puts.
	return &cache.ResolvedEntry{
		RawJSON:       raw,
		Inputs:        inputs,
		TTLOverride:   uafTTLOverrideForEntry(inputs),
		StageOutcomes: storableStageOutcomes(class, outcomes),
	}
}

// storableStageOutcomes is the value newResolveEntry keeps on the entry: ""
// for a cell of any other class, and when every stage succeeded.
func storableStageOutcomes(class string, o *api.StageOutcomes) string {
	if class != "restactions" {
		return ""
	}
	v := o.HeaderValue()
	if v == stageOutcomesAllSucceeded {
		return ""
	}
	return v
}

// setStoredStageOutcomesHeader sets the header of an L1 hit of a
// restactions-class cell from the entry.
func setStoredStageOutcomesHeader(wri http.ResponseWriter, entry *cache.ResolvedEntry) {
	v := stageOutcomesAllSucceeded
	if entry != nil && entry.StageOutcomes != "" {
		v = entry.StageOutcomes
	}
	wri.Header().Set(util.HeaderStageOutcomes, v)
}
