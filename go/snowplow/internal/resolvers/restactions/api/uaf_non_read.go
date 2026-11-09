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
// rejects (templates.API.UserAccessFilterOnNonReadVerb is that rule in Go), at
// the top of dispatchOneCall, where #443's dry-run refusal and #903's
// background-write refusal already sit: before any dispatch branch, so nothing
// is sent, whichever branch would have served the item. It runs first of the
// three: the stage is misconfigured whatever kind of resolve this is, so a dry
// run, a background resolve and a customer request all report the same reason.
// No provenance is exempt: an inline draft's userAccessFilter stage dials the
// caller's clientconfig rather than the ServiceAccount, but a draft refused
// here is one the CRD would refuse to store.
//
// Each refused item is a stage failure, recorded the way every per-item
// failure is (recordItemError): a {"reason","message"} record under the
// stage's errorKey, the stage-error sink bumped (no Put stores the result),
// the outcome UserAccessFilterOnNonReadVerb in X-Snowplow-Stage-Outcomes, and,
// without continueOnError, the resolve's StageFailure (a 500: the RESTAction,
// not the request, is wrong). A write stage that depends on it is not sent
// (#904): the reason is a failure.
//
// OBSERVABILITY. A misconfigured RESTAction is a real failure: WARN per item,
// as #1112 logs every write snowplow did not send, and one count per item in
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

	"github.com/krateo-platformops/plumbing/ptr"
)

// refuseUAFOnNonReadVerb refuses item i of a stage that carries
// userAccessFilter on a verb the CRD rule rejects. It returns true when it
// refused (the caller returns nil: a per-item error never cancels the stage,
// #313 C-A).
func (r *resolveRun) refuseUAFOnNonReadVerb(sc *stageCtx, i int) bool {
	if !sc.apiCall.UserAccessFilterOnNonReadVerb() {
		return false
	}
	call := sc.calls[i]
	verb := ptr.Deref(sc.apiCall.Verb, http.MethodGet)
	msg := uafNonReadVerbMessage(sc.id, verb)
	var itemErr error
	if !call.ContinueOnError {
		itemErr = fmt.Errorf("api %s item %d failed: %s", sc.id, i, msg)
	}
	r.recordItemError(sc.dictMu, sc.itemErrs, sc.id, i, call.ErrorKey,
		map[string]any{"reason": StageReasonUAFOnNonReadVerb, "message": msg}, msg, itemErr)
	recordUAFNonReadVerbRefused(r.opts.Provenance)
	r.log.WarnContext(r.ctx, "stage refused: userAccessFilter on a verb other than GET or HEAD",
		slog.String("name", sc.id),
		slog.String("verb", verb),
		slog.String("reason", StageReasonUAFOnNonReadVerb),
		slog.String("restaction_namespace", r.opts.RESTActionNamespace),
		slog.String("restaction_name", r.opts.RESTActionName))
	return true
}

// uafNonReadVerbMessage is the text a refused item records under its errorKey.
// It can reach the resolved output, so it holds the stage name and the verb
// only.
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
