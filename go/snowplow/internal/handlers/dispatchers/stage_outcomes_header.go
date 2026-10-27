// stage_outcomes_header.go — #904: every RESTAction /call answer carries
// X-Snowplow-Stage-Outcomes, a cold resolve and an L1 hit alike.
//
// The cold path sets it from the resolve's own collector (restactions.go). A
// hit answers with the value the resolve that STORED the cell produced, kept on
// the entry (cache.ResolvedEntry.StageOutcomes) by every restactions-class Put
// site that resolves: the /call dispatcher, the refresher and the cohort seed.
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

// storableStageOutcomes is the value a restactions-class Put keeps on the
// entry: "" when every stage succeeded.
func storableStageOutcomes(o *api.StageOutcomes) string {
	v := o.HeaderValue()
	if v == stageOutcomesAllSucceeded {
		return ""
	}
	return v
}

// storableStageOutcomesFor is storableStageOutcomes for a refresher Put, which
// serves every class: only a restactions-class cell answers a RESTAction
// /call, so only it keeps the outcomes.
func storableStageOutcomesFor(class string, o *api.StageOutcomes) string {
	if class != "restactions" {
		return ""
	}
	return storableStageOutcomes(o)
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
