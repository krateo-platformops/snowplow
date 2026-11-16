// uaf_non_read.go — #1281: a stage that carries userAccessFilter on a verb
// other than GET or HEAD is never sent.
//
// THE GAP. userAccessFilter switches a stage's credential to snowplow's
// ServiceAccount (resolveStageEndpoint) and then narrows what comes back to
// what the user may see (refilter.go). That is sound only for a read. The one
// thing that kept it off a write was the CRD's first stage rule (core.go, on
// API: userAccessFilter only on GET/HEAD), and admission checks it only when a
// RESTAction is created or updated. A RESTAction stored before the rule
// existed, or admitted while it was absent or relaxed, with userAccessFilter on
// a POST, PUT, PATCH or DELETE stage, sent that write as the ServiceAccount:
// the user's RBAC never decided it.
//
// THE RULE. The resolver refuses such a stage on the same verbs the CRD rule
// rejects (templates.API.UserAccessFilterOnNonReadVerb is that rule in Go).
// It decides on the STAGE, in the orchestrator loop of ResolveWithFailure,
// before the #904 dependent-write decision and before runStage: no endpoint is
// resolved, no item is planned, nothing is sent. Deciding on the stage, not on
// its items, makes the report independent of the data: an iterator that
// yields zero items and a dependency that failed are refused and reported the
// same way. It holds for every kind of resolve: a dry run (where it replaces
// #443's StageNotExecuted), a background resolve (before #903's refusal), and
// an inline draft (whose userAccessFilter stage dials the caller's
// clientconfig rather than the ServiceAccount, but which the CRD would refuse
// to store).
//
// The refused stage fails the way a stage snowplow refused to send fails
// (failUndialledWrite's shape): a {"reason","message"} record under the
// stage's errorKey, the stage-error sink bumped (no Put stores the result),
// the outcome UserAccessFilterOnNonReadVerb in X-Snowplow-Stage-Outcomes, and,
// without continueOnError, the resolve's StageFailure (a 500: the RESTAction,
// not the request, is wrong). A write stage that depends on it is not sent
// (#904): the reason is a failure.
//
// OBSERVABILITY. A misconfigured RESTAction is a real failure: one WARN per
// refused stage and one count in
// snowplow_uaf_non_read_verb_refused_total{provenance}. The line and the
// message are metadata only: the RESTAction's namespace and name, the stage
// name and the verb. No identity, no path, no payload.
package api

import (
	"expvar"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// refuseUAFOnNonReadVerb refuses stage id when it carries userAccessFilter on
// a verb the CRD rule rejects, and reports whether it did (the orchestrator
// then moves to the next stage). It runs on the orchestrator goroutine,
// between stages, so no worker is running and r.dict needs no lock.
func (r *resolveRun) refuseUAFOnNonReadVerb(id string, apiCall *templates.API) bool {
	if !apiCall.UserAccessFilterOnNonReadVerb() {
		return false
	}
	verb := ptr.Deref(apiCall.Verb, http.MethodGet)
	msg := uafNonReadVerbMessage(id, verb)
	accumulateErrorKey(r.dict, ptr.Deref(apiCall.ErrorKey, "error"),
		map[string]any{"reason": StageReasonUAFOnNonReadVerb, "message": msg})
	r.taintNarrow() // #603 PR-2 — a failed stage narrows nothing
	r.stageErrSink.Bump(id, msg)
	r.outcomes.fail(id, StageReasonUAFOnNonReadVerb)
	if !ptr.Deref(apiCall.ContinueOnError, false) && r.failure == nil {
		r.failure = &StageFailure{Stage: id, Code: http.StatusInternalServerError,
			Reason: string(response.New(http.StatusInternalServerError, nil).Reason), Message: msg}
	}
	recordUAFNonReadVerbRefused(r.opts.Provenance)
	// audit#22 (a) — denied, uaf_on_write_verb: nothing was planned, so its
	// target is not resolved (marked target).
	r.auditStageRefusal(r.ctx, stageRefusal{apiCall: apiCall,
		outcome: audit.OutcomeDenied, reason: audit.DenialUAFOnWriteVerb})
	r.log.WarnContext(r.ctx, "stage refused: userAccessFilter on a verb other than GET or HEAD",
		slog.String("name", id),
		slog.String("verb", verb),
		slog.String("reason", StageReasonUAFOnNonReadVerb),
		slog.String("restaction_namespace", r.opts.RESTActionNamespace),
		slog.String("restaction_name", r.opts.RESTActionName))
	return true
}

// uafNonReadVerbMessage is the text a refused stage records under its
// errorKey. It can reach the resolved output, so it holds the stage name and
// the verb only.
func uafNonReadVerbMessage(stage, verb string) string {
	return fmt.Sprintf("stage %q verb %s was not sent: userAccessFilter dials snowplow's ServiceAccount and is allowed only on a GET or HEAD stage (#1281)", stage, verb)
}

var uafNonReadVerbRefused struct {
	stored         atomic.Uint64
	callerSupplied atomic.Uint64
}

func recordUAFNonReadVerbRefused(p Provenance) {
	if p == ProvenanceCallerSupplied {
		uafNonReadVerbRefused.callerSupplied.Add(1)
		return
	}
	uafNonReadVerbRefused.stored.Add(1)
}

// UAFNonReadVerbRefusedSnapshot returns the {provenance} refusal counts.
func UAFNonReadVerbRefusedSnapshot() map[string]uint64 {
	return map[string]uint64{
		perUserRefProvenanceStored:         uafNonReadVerbRefused.stored.Load(),
		perUserRefProvenanceCallerSupplied: uafNonReadVerbRefused.callerSupplied.Load(),
	}
}

var uafNonReadVerbExpvarOnce sync.Once

func init() { registerUAFNonReadVerbExpvar() }

// registerUAFNonReadVerbExpvar publishes snowplow_uaf_non_read_verb_refused_total.
// Ungated on the cache mode (listed in the CFG-1 nonCacheInitPublishers
// exception): the refusal happens on every RESTAction resolve, cache on or off.
func registerUAFNonReadVerbExpvar() {
	uafNonReadVerbExpvarOnce.Do(func() {
		expvar.Publish("snowplow_uaf_non_read_verb_refused_total", expvar.Func(func() any {
			return UAFNonReadVerbRefusedSnapshot()
		}))
	})
}
