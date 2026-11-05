// undialled_write.go — #1112 D2: a declared write that snowplow does not send
// is a stage failure with a reason, never a silent success.
//
// THE DEFECT. Three guards decide, before any egress, that a rendered call must
// not be dialled, and each only logged it at DEBUG: the #288 malformed-path
// guard, the #302 jq-error skip (path, payload or header), and the #293
// unrendered-template skip. Not dialling is right; staying silent was not. For
// a READ it left the stage's key unset with no outcome and no errorKey, so
// whatever stood there looked like the stage's output (#1116: a caller-seeded
// extras key did, until #1116 refused those). For a WRITE it told the user
// "done" with nothing sent: a Zendesk comment PUT to `/api/v2/tickets/<id>` with
// a non-numeric id parsed as a core apiserver path, failed the DNS-1123 name
// check, and the stage answered 0 requests, outcomes `[]`, no error.
//
// THE RULE. A write stage (any method but GET, HEAD, OPTIONS, TRACE) with real
// coordinates that snowplow refuses to send FAILS, with a reason from the
// closed outcome set (stage_outcomes.go):
//   - InvalidPath   — the #288 guard refused the rendered path. Its collapse
//     check (a path segment that rendered empty) judges every stage, a named
//     endpoint's included; its Kubernetes name-validity check judges only a
//     stage that dials this cluster's apiserver (createRequestOption). The
//     errorKey message says which (undialledWriteCause). The path template's
//     own refusals (#1123) are InvalidPath too;
//   - TemplateError — the path, payload or a header did not render: a jq error
//     (#302) or an unrendered ${...} (#293);
//   - NotSent       — the catch-all: the stage's items were planned but at least
//     one was neither sent nor failed with a reason of its own.
//
// #1116 — the same rule holds for a READ stage's refused item (InvalidPath,
// TemplateError; the NotSent catch-all stays write-only): a body built around
// a read that was never made is not the stage's answer, so it fails the stage
// and is never stored, as any per-item stage error is. Each refused read is
// counted by resolve work class (refused_read_metrics.go) and a read stage's
// refusals are logged at WARN once per stage (failRefusedItems).
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
		if sr.collapsed {
			return StageReasonInvalidPath, "a path segment rendered empty (#288)"
		}
		return StageReasonInvalidPath, "its rendered path is not a valid Kubernetes object path (#288)"
	case reasonPathValueRefused:
		return StageReasonInvalidPath, "its path holds a value that cannot be one segment or one query value, or a percent-escape in an apiserver path (#1123)"
	case reasonUnrenderedTemplate:
		return StageReasonTemplateError, "its path holds an unrendered ${...} template (#293)"
	case reasonJQPathError:
		return StageReasonTemplateError, "its path template failed to evaluate (#302)"
	case reasonJQPayloadError:
		return StageReasonTemplateError, "its payload template failed to evaluate (#302)"
	case reasonJQHeaderError:
		return StageReasonTemplateError, "a header template failed to evaluate (#302)"
	case reasonPathNotAsServed:
		return StageReasonInvalidPath, "the apiserver would read its rendered path as a different path (#1137)"
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

// failRefusedItems records one failure per refused item of a stage (the plan's
// skips), write or read. #1116: a READ item a guard refused used to stay
// silent (no outcome, no errorKey), so nothing told the caller the stage's
// value was missing; before #1116 a caller-supplied extras key could even
// stand in for it unseen. Now it is the same failure a refused write is: the
// reason (InvalidPath / TemplateError), the errorKey record, the stage-error
// sink (no Put stores a body built around the missing read, as for every
// per-item stage error), and the StageFailure without continueOnError.
//
// LOG VOLUME. A refused write is a WARN per item, as #1112 ruled: each is an
// action that did not happen. A refused read is counted per item
// (snowplow_refused_read_total{class}) and logged at WARN ONCE per stage, with
// the number of refused items: an iterator read that refuses many items, or a
// background resolve that refuses reads routinely, shows in the counter, not
// as one WARN line per item. Runs on the stage-loop goroutine before any
// worker exists, so dict needs no lock.
func (r *resolveRun) failRefusedItems(id string, apiCall *templates.API, skips []skippedOption) {
	if len(skips) == 0 {
		return
	}
	var firstReason string
	for _, sk := range skips {
		reason, why := undialledWriteCause(sk.reason)
		if firstReason == "" {
			firstReason = reason
		}
		r.failUndialledWrite(id, apiCall, reason, why)
	}
	if !isWriteVerb(apiCall.Verb) {
		r.log.Warn("read stage not sent",
			slog.String("name", id),
			slog.String("verb", ptr.Deref(apiCall.Verb, http.MethodGet)),
			slog.String("reason", firstReason),
			slog.Int("refused_items", len(skips)),
			slog.String("restaction_namespace", r.opts.RESTActionNamespace),
			slog.String("restaction_name", r.opts.RESTActionName))
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

// failUndialledWrite records the failure of one item, of a write or a read
// stage, that was not sent. The message names the stage's class ("write
// stage" / "read stage"), the stage, the verb and the cause only. A write item
// is logged here; a read stage's refusals are logged once by failRefusedItems.
func (r *resolveRun) failUndialledWrite(id string, apiCall *templates.API, reason, why string) {
	verb := ptr.Deref(apiCall.Verb, http.MethodGet)
	code := undialledWriteCode(reason)
	class := "write"
	if !isWriteVerb(apiCall.Verb) {
		class = "read"
		recordRefusedRead(r.ctx)
	}
	msg := fmt.Sprintf("%s stage %q verb %s was not sent: %s", class, id, verb, why)
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
	if class == "read" {
		return // logged once per stage by failRefusedItems
	}
	r.log.Warn("write stage not sent",
		slog.String("name", id),
		slog.String("verb", verb),
		slog.String("reason", reason),
		slog.String("restaction_namespace", r.opts.RESTActionNamespace),
		slog.String("restaction_name", r.opts.RESTActionName))
}
