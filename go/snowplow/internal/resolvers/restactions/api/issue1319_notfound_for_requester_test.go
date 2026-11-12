package api

// issue1319_notfound_for_requester_test.go — #1319 ruling: the definite
// per-identity NotFound (#1080) reads its own closed code,
// NotFoundForRequester ("denied"); a plain 404 recorded from an upstream
// answer stays NotFound ("cut / failed"). A stage holding both reads the plain
// NotFound, deterministically. This file drives the N2 site
// (recordDefiniteNotFound); the N1 site (the requester's own dial of a watched
// absent object) is driven end to end through the real widget path by
// TestIssue1319_W5 ("after a refresher re-resolve").

import (
	"context"
	"sync"
	"testing"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

func n1319Run(stages ...string) *resolveRun {
	_, sink := cache.WithStageErrorSink(context.Background())
	r := &resolveRun{log: scalarFalsifierLogger(), dict: map[string]any{}, ctx: context.Background(),
		outcomes: newStageOutcomes(), stageErrSink: sink}
	r.outcomes.register(stages)
	for _, s := range stages {
		r.outcomes.ran(s)
	}
	return r
}

// n1319Plain404 records a plain 404 the way an upstream answer is recorded:
// the Status value under errorKey, its reason read from the code.
func n1319Plain404(r *resolveRun, mu *sync.Mutex, errs []error, id string, i int) {
	r.recordItemError(mu, errs, id, i, id+"Err",
		map[string]any{"kind": "Status", "status": "Failure", "reason": "NotFound", "code": 404}, "not found", nil)
}

func TestIssue1319_N1_DefiniteNotFoundReadsItsOwnCode(t *testing.T) {
	r := n1319Run("definite", "plain")
	mu := &sync.Mutex{}
	errs := make([]error, 2)
	call := httpcall.RequestOptions{ContinueOnError: true}
	r.recordDefiniteNotFound(mu, errs, "definite", 0, call, "configmaps \"q\" not found")
	n1319Plain404(r, mu, errs, "plain", 1)
	if got := r.outcomes.reasonOf("definite"); got != StageReasonNotFoundForRequester {
		t.Fatalf("N1 RED: the definite per-identity NotFound reads %q, want %q", got, StageReasonNotFoundForRequester)
	}
	if got := r.outcomes.reasonOf("plain"); got != StageReasonNotFound {
		t.Fatalf("N1 RED: a plain 404 reads %q, want %q", got, StageReasonNotFound)
	}
	if StageReasonGroup(StageReasonNotFoundForRequester) != StageGroupDenied || StageReasonGroup(StageReasonNotFound) != StageGroupCutFailed {
		t.Fatal("N1 RED: NotFoundForRequester must be denied, NotFound cut / failed")
	}
	// The errorKey body is unchanged: the canonical 404 Status.
	if b, ok := r.dict["definiteErr"].(map[string]any); ok && statusCodeOf(b["code"]) != 404 {
		t.Fatalf("N1: the definite answer's errorKey body changed: %v", b)
	}
}

func TestIssue1319_N2_APlain404OutranksTheDefiniteAnswerInOneStage(t *testing.T) {
	for _, order := range []string{"definite first", "plain first"} {
		r := n1319Run("s")
		mu := &sync.Mutex{}
		errs := make([]error, 2)
		call := httpcall.RequestOptions{ContinueOnError: true}
		if order == "definite first" {
			r.recordDefiniteNotFound(mu, errs, "s", 0, call, "not found")
			n1319Plain404(r, mu, errs, "s", 1)
		} else {
			n1319Plain404(r, mu, errs, "s", 1)
			r.recordDefiniteNotFound(mu, errs, "s", 0, call, "not found")
		}
		if got := r.outcomes.reasonOf("s"); got != StageReasonNotFound {
			t.Fatalf("N2 RED (%s): the stage reads %q, want the plain %q", order, got, StageReasonNotFound)
		}
	}
}
