// background_write.go — #903: a write stage is sent only on a customer's own
// request (cache/background_writes.go has the rule and who counts as a
// customer).
//
// THE DEFECT. #615's "the refresher only reads" was enforced only on a dial of
// snowplow's OWN ServiceAccount endpoint (branchEDecide, keyed on the SA-dial
// provenance), and #268's re-gate and #725's header drop are SA-only too. A
// write stage through an endpointRef Secret or an external URL bypassed all
// three: the refresher, the cohort seed, the prewarm walks and the content
// prewarm sent it with the Secret's own credential and threw the result away.
//
// THE REFUSAL is the same shape as #615's: before ANY egress, the stage item
// records an error under its errorKey (continueOnError honoured), the
// stage-error sink is bumped so every L1 Put declines, and nothing is sent.
// No customer can be served a refused write: a background result is never
// served from a resolve that refused one (its Put is declined), and a write
// stage was never servable from the cache anyway (its egress declines the Put
// as an external touch).
//
// It is refused at the top of dispatchOneCall, beside #443's dry-run refusal,
// so it holds for every dispatch branch. Today only the external branch (E)
// sends a non-GET verb (the informer, apistage, internal-rest-config,
// discovery and in-process branches are GET-only), so this is exactly "branch
// E refuses it", and stays so if a branch ever learns a write.
//
// Background SubjectAccessReview / /render / /diff POSTs stop too: #615 let the
// refresher create an access review as the cohort; the owner ruling's verb set
// (GET/HEAD/OPTIONS/TRACE for every endpoint kind) supersedes that carve-out.
// It costs no cache: such a resolve was never stored (branch E's external
// touch declines the Put).
//
// OBSERVABILITY. A refusal is expected work, not a failure: DEBUG, and one
// count per refused item in snowplow_background_write_refused_total{origin},
// every cell of the closed origin set pre-populated (a 0 is a reading).
package api

import (
	"context"
	"expvar"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

var (
	backgroundWriteRefusedOnce sync.Once
	backgroundWriteRefused     map[string]*atomic.Uint64
)

func backgroundWriteRefusedCells() map[string]*atomic.Uint64 {
	backgroundWriteRefusedOnce.Do(func() {
		backgroundWriteRefused = make(map[string]*atomic.Uint64, len(cache.NonCustomerWriteOrigins))
		for _, o := range cache.NonCustomerWriteOrigins {
			backgroundWriteRefused[o] = new(atomic.Uint64)
		}
	})
	return backgroundWriteRefused
}

// recordBackgroundWriteRefused counts one refused write item under origin. An
// origin outside the closed set folds into background-unattributed.
func recordBackgroundWriteRefused(origin string) {
	cells := backgroundWriteRefusedCells()
	c, ok := cells[origin]
	if !ok {
		c = cells[cache.RefDenialOriginBackgroundUnattributed]
	}
	c.Add(1)
}

// BackgroundWriteRefusedSnapshot returns the {origin} counts.
func BackgroundWriteRefusedSnapshot() map[string]uint64 {
	cells := backgroundWriteRefusedCells()
	out := make(map[string]uint64, len(cells))
	for k, v := range cells {
		out[k] = v.Load()
	}
	return out
}

func init() {
	// CFG-1: under CACHE_ENABLED=false there is no background driver, so the
	// key must not appear at /debug/vars (same gate as every cache-side key).
	if cache.Disabled() {
		return
	}
	expvar.Publish("snowplow_background_write_refused_total", expvar.Func(func() any {
		return BackgroundWriteRefusedSnapshot()
	}))
}

// withBackgroundHopHeader returns the stage with cache.BackgroundHeader set as
// the hop rule needs (stage-local copy; the CR's own slice is never written):
//   - to snowplow itself (toSelf): added when this resolve is not a customer's
//     own request; a header the RESTAction sets itself is kept, since it can
//     only restrict that hop;
//   - to any other host: removed, so it never leaks to an external endpoint.
func withBackgroundHopHeader(apiCall *templates.API, nonCustomer, toSelf bool) *templates.API {
	present := false
	for _, h := range apiCall.Headers {
		if isBackgroundHeader(h) {
			present = true
			break
		}
	}
	switch {
	case toSelf && nonCustomer && !present:
		// slices.Clip: cap == len, so the append always copies into a new
		// array and never writes the CR's own header slice.
		hdrs := append(slices.Clip(apiCall.Headers), cache.BackgroundHeader+": 1")
		local := *apiCall
		local.Headers = hdrs
		return &local
	case !toSelf && present:
		hdrs := make([]string, 0, len(apiCall.Headers))
		for _, h := range apiCall.Headers {
			if !isBackgroundHeader(h) {
				hdrs = append(hdrs, h)
			}
		}
		local := *apiCall
		local.Headers = hdrs
		return &local
	}
	return apiCall
}

// isBackgroundHeader reports whether a "Name: value" stage header names
// cache.BackgroundHeader (case-insensitive, as HTTP header names are).
func isBackgroundHeader(h string) bool {
	name, _, ok := strings.Cut(h, ":")
	return ok && strings.EqualFold(strings.TrimSpace(name), cache.BackgroundHeader)
}

// refuseBackgroundWrite refuses item i of a write stage when the resolve is
// not a customer's own request. It returns true when it refused (the caller
// returns nil: a per-item error never cancels the stage, #313 C-A).
func (r *resolveRun) refuseBackgroundWrite(gctx context.Context, sc *stageCtx, i int) bool {
	call := sc.calls[i]
	if !isWriteVerb(call.Verb) {
		return false
	}
	origin := cache.NonCustomerWriteOrigin(gctx)
	if origin == "" {
		return false
	}
	verb := ptr.Deref(call.Verb, http.MethodGet)
	msg := fmt.Sprintf("background work never writes: stage %q verb %s is sent only on a customer request (%s, #903)", sc.id, verb, origin)
	var itemErr error
	if !call.ContinueOnError {
		itemErr = fmt.Errorf("api %s item %d failed: %s", sc.id, i, msg)
	}
	r.recordItemError(sc.dictMu, sc.itemErrs, sc.id, i, call.ErrorKey, msg, msg, itemErr)
	recordBackgroundWriteRefused(origin)
	r.log.Debug("write stage refused: not a customer request",
		slog.String("name", sc.id),
		slog.String("verb", verb),
		slog.String("origin", origin),
		slog.String("path", call.Path),
		slog.String("restaction_namespace", r.opts.RESTActionNamespace),
		slog.String("restaction_name", r.opts.RESTActionName))
	return true
}
