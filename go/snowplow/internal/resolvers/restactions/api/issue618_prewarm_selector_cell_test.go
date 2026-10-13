// issue618_prewarm_selector_cell_test.go — #618 / #602: the boot-prewarm path
// for a cluster-list cell that carries a list query.
//
// No other #618 arm calls PrewarmClusterListCells, so two wrong-data defects on
// that path survived the PR's own arms (gate of PR #628):
//
//   - the prewarm keys the cell WITHOUT cell.Query — the SELECTOR body then
//     lands under the UNSELECTED cluster key, and an unselected stage is served
//     only the env=prod rows;
//   - populateClusterListCellSync's Inputs drop the query — the refresher then
//     rebuilds the UNSELECTED path into the selector cell (or the selector cell
//     never refreshes and goes stale).
//
// The oracle is the hand-written seed in list_query_618_falsifier_test.go:
// `env=prod` selects exactly <ns>-w1 in every namespace.
package api

import (
	"log/slog"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue618_PrewarmSelectorCellKeyAndInputs(t *testing.T) {
	newQ618Watcher(t)
	stage := &templates.API{Name: "sel", Path: q618NSPath("team-a") + "?labelSelector=env%3Dprod"}
	gvr, canon, ok := extractClusterListTargetFromStage(stage)
	if !ok || canon == "" {
		t.Fatalf("stage not enumerable: ok=%v canon=%q", ok, canon)
	}
	store := cache.ResolvedCache()
	roster := ClusterListCellRoster{Cells: []ClusterListCellEntry{{APICall: stage, GVR: gvr, Query: canon}}}
	pop, att, err := PrewarmClusterListCells(q618Ctx(), slog.Default(), roster, endpoints.Endpoint{ServerURL: "http://test.invalid"}, store, 1)
	t.Logf("prewarm populated=%d attempted=%d err=%v", pop, att, err)
	if pop != 1 {
		t.Fatalf("prewarm did not populate the selector cell (populated=%d attempted=%d err=%v)", pop, att, err)
	}

	// Key: the selector body must NOT sit under the unselected cluster key.
	if e, hit := store.GetNoTouch(cache.ComputeKey(contentKeyInputsQ(gvr, "", "", ""))); hit && e != nil {
		t.Fatalf("the UNSELECTED cluster cell holds %v — a selector body was keyed without its query", namesInEnvelopeBytes(t, e.RawJSON))
	}
	e, hit := store.GetNoTouch(cache.ComputeKey(contentKeyInputsQ(gvr, "", "", canon)))
	if !hit || e == nil {
		t.Fatalf("selector cell absent under the query-carrying key")
	}
	if got := lit(namesInEnvelopeBytes(t, e.RawJSON)...); !eqNames(got, names(all3, "w1")) {
		t.Fatalf("selector cell holds %v, want %v", got, names(all3, "w1"))
	}

	// Inputs: the refresher rebuilds from e.Inputs, so they must carry the query.
	if e.Inputs == nil {
		t.Fatalf("selector cell has nil Inputs — the refresher cannot rebuild it")
	}
	if e.Inputs.Query != canon {
		t.Fatalf("cell Inputs.Query=%q, want %q — the refresher would rebuild the unselected path", e.Inputs.Query, canon)
	}
	raw, err := RefreshContentEntry(q618Ctx(), *e.Inputs)
	if err != nil {
		t.Fatal(err)
	}
	if got := lit(namesInEnvelopeBytes(t, raw)...); !eqNames(got, names(all3, "w1")) {
		t.Fatalf("refresh of the selector cell returned %v, want %v", got, names(all3, "w1"))
	}
}
