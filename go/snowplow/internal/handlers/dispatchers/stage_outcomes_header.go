// stage_outcomes_header.go — X-Snowplow-Stage-Outcomes on a /call answer, a
// cold resolve and an L1 hit alike.
//
// RESTACTION /call (#904): the header is ALWAYS present; [] means every stage
// succeeded.
//
// WIDGET /call (#1319): the header carries the stages of the widget's apiRef
// RESTAction, same name and format, and is PRESENT ONLY when at least one of
// them did not succeed. Absent means complete, so a portal talking to an older
// snowplow (which never sends it on a widget) reads every widget as complete,
// exactly as it did before. Widget-agnostic: no widget kind, route or page is
// read; a widget without an apiRef has no RESTAction stages and is complete.
//
// The cold path sets it from the resolve's own collector (restactions.go,
// widgets.go). A hit answers with the value the resolve that STORED the cell
// produced, kept on the entry (cache.ResolvedEntry.StageOutcomes) by every Put
// site that resolves: the /call dispatchers, the refresher, the cohort seed
// and the walker's content populate. "" on the entry means every stage
// succeeded — so a cell stored complete costs no memory for it.
//
// WHICH CELLS CAN HOLD OUTCOMES. Storage is decided by the shared Put-gate
// verdict (cache.PutGateVerdict), unchanged by #1319 and identical for every
// class: a result whose resolve recorded a stage error is not stored unless
// every error is a per-identity denial (#681 DenialOnly: Forbidden, a definite
// NotFoundForRequester). So a stored cell's outcomes are one of two kinds:
//   - the denials of the binding-set class it is keyed by;
//   - an aborted resolve's (api.abortedResolve: the resolve-level Aborted row
//     plus every stage NotRun). The stage graph could not be ordered, or there
//     was no identity or client config. No stage ran, so no stage error was
//     recorded, and the gate stores it.
//
// A stage cut by the page budget (Timeout, #1115) or any other stage failure
// is answered cold with its header and never stored (#1319 storage decision,
// docs/howto-frontend-widget-stage-outcomes.md §5).
package dispatchers

import (
	"net/http"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// stageOutcomesAllSucceeded is the RESTAction header value of a resolve with
// no failed, skipped or not-run stage.
const stageOutcomesAllSucceeded = "[]"

// storableStageOutcomes is the value a Put keeps on the entry: "" when every
// stage succeeded (api.StorableStageOutcomes).
func storableStageOutcomes(o *api.StageOutcomes) string {
	return api.StorableStageOutcomes(o)
}

// setStoredStageOutcomesHeader sets the header of an L1 hit of a
// restactions-class cell from the entry: always present (#904).
func setStoredStageOutcomesHeader(wri http.ResponseWriter, entry *cache.ResolvedEntry) {
	v := stageOutcomesAllSucceeded
	if entry != nil && entry.StageOutcomes != "" {
		v = entry.StageOutcomes
	}
	wri.Header().Set(util.HeaderStageOutcomes, v)
}

// setWidgetStageOutcomesHeader sets a widget /call's header from a stored
// value (a StorableStageOutcomes value, or an entry's StageOutcomes): present
// only when a stage did not succeed (#1319), removed otherwise so a complete
// answer never carries one.
func setWidgetStageOutcomesHeader(wri http.ResponseWriter, stored string) {
	if stored == "" {
		wri.Header().Del(util.HeaderStageOutcomes)
		return
	}
	wri.Header().Set(util.HeaderStageOutcomes, stored)
}
