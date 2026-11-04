// undialled_write.go — #1112 D2: a declared write that snowplow does not send
// is a stage failure with a reason, never a silent success.
//
// THE DEFECT. Three guards decide, before any egress, that a rendered call must
// not be dialled, and each only logged it at DEBUG: the #288 malformed-path
// guard, the #302 jq-error skip (path, payload or header), and the #293
// unrendered-template skip. For a read that is right (there is nothing sensible
// to read, and the page renders what it can). For a WRITE it told the user
// "done" with nothing sent: a Zendesk comment PUT to `/api/v2/tickets/<id>` with
// a non-numeric id parsed as a core apiserver path, failed the DNS-1123 name
// check, and the stage answered 0 requests, outcomes `[]`, no error.
//
// THE RULE. A write stage (any method but GET, HEAD, OPTIONS, TRACE) with real
// coordinates that snowplow refuses to send FAILS, with a reason from the
// closed outcome set (stage_outcomes.go):
//   - InvalidPath   — the #288 guard refused the rendered path. Since #1112 the
//     guard judges only a stage that dials this cluster's apiserver
//     (createRequestOptionsWithSkips), so it never refuses an external
//     endpoint's path;
//   - TemplateError — the path, payload or a header did not render: a jq error
//     (#302) or an unrendered ${...} (#293);
//   - NotSent       — the catch-all: the stage's items were planned but at least
//     one was neither sent nor failed with a reason of its own.
//
// Each refused item is one failure (an iterator write with one refused item of
// three sends the other two and FAILS), recorded the way every stage failure is:
// the error under the stage's errorKey, the stage-error sink bumped (no Put
// accepts the result), the outcome, and — without continueOnError — the
// resolve's StageFailure (#709), so a /call whose filter cannot produce a value
// answers with the code instead of a body built from nothing.
//
// NOT A FAILURE: an iterator write that yields zero items has nothing to send
// and succeeds (#904 owner ruling: the "no data volumes" delete lifecycle). A
// dry run's refusal keeps StageNotExecuted, a #904 dependent skip keeps
// Skipped, a #903 background refusal and a #905 stale-pass hold keep their own
// recorded errors: each already reports itself.
//
// The errorKey message and the log line are identity-free: the stage name, the
// verb and the cause only — never the rendered path, the payload or the jq error
// text (which can carry caller input and response data; the DEBUG skip line
// keeps the detail for triage, as before).
package api

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// undialledWriteCause is the outcome reason and the errorKey phrase for one
// refused write item, from the skip verdict that refused it.
func undialledWriteCause(sr skipReason) (reason, why string) {
	switch sr.class {
	case reasonEmptyInterp:
		return StageReasonInvalidPath, "its rendered path is not a valid Kubernetes object path (#288)"
	case reasonUnrenderedTemplate:
		return StageReasonTemplateError, "its path holds an unrendered ${...} template (#293)"
	case reasonJQPathError:
		return StageReasonTemplateError, "its path template failed to evaluate (#302)"
	case reasonJQPayloadError:
		return StageReasonTemplateError, "its payload template failed to evaluate (#302)"
	case reasonJQHeaderError:
		return StageReasonTemplateError, "a header template failed to evaluate (#302)"
	}
	return StageReasonNotSent, "it was refused before it was sent"
}

// undialledWriteCode is the status a refused write reports: the request
// snowplow would have sent is invalid (400) when a template or the path is,
// and snowplow itself did not send it (500) otherwise.
func undialledWriteCode(reason string) int {
	switch reason {
	case StageReasonInvalidPath, StageReasonTemplateError:
		return http.StatusBadRequest
	}
	return http.StatusInternalServerError
}

// failUndialledWrites records one failure per refused item of a write stage
// (the plan's skips). A read stage's skips stay silent, as before. Runs on the
// stage-loop goroutine before any worker exists, so dict needs no lock.
func (r *resolveRun) failUndialledWrites(id string, apiCall *templates.API, skips []skippedOption) {
	if !isWriteVerb(apiCall.Verb) {
		return
	}
	for _, sk := range skips {
		reason, why := undialledWriteCause(sk.reason)
		r.failUndialledWrite(id, apiCall, reason, why)
	}
}

// failUnsentWrites is the catch-all: after a write stage's workers finished,
// a stage that planned n items, sent fewer and recorded no failure of its own
// did not do what it declared. It fails with NotSent. Runs after g.Wait, so no
// worker is running.
func (r *resolveRun) failUnsentWrites(sc *stageCtx) {
	if !isWriteVerb(sc.apiCall.Verb) || sc.sent.Load() >= int64(len(sc.calls)) || r.outcomes.failed(sc.id) {
		return
	}
	r.failUndialledWrite(sc.id, sc.apiCall, StageReasonNotSent, "it was not sent and recorded no other failure")
}

// failUndialledWrite records the failure of one write item that was not sent.
func (r *resolveRun) failUndialledWrite(id string, apiCall *templates.API, reason, why string) {
	verb := ptr.Deref(apiCall.Verb, http.MethodGet)
	code := undialledWriteCode(reason)
	msg := fmt.Sprintf("write stage %q verb %s was not sent: %s", id, verb, why)
	accumulateErrorKey(r.dict, ptr.Deref(apiCall.ErrorKey, "error"), map[string]any{
		"kind": "Status", "apiVersion": "v1", "status": "Failure",
		"code": code, "reason": reason, "message": msg,
	})
	r.taintNarrow() // #603 PR-2 — a failed stage narrows nothing
	r.stageErrSink.Bump(id, msg)
	r.outcomes.fail(id, reason)
	if !ptr.Deref(apiCall.ContinueOnError, false) && r.failure == nil {
		r.failure = &StageFailure{Stage: id, Code: code,
			Reason: string(response.New(code, nil).Reason), Message: msg}
	}
	r.log.Warn("write stage not sent",
		slog.String("name", id),
		slog.String("verb", verb),
		slog.String("reason", reason),
		slog.String("restaction_namespace", r.opts.RESTActionNamespace),
		slog.String("restaction_name", r.opts.RESTActionName))
}
