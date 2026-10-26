// stale_write_once.go — #905: a customer write is sent exactly once, even when
// the resolve is served stale content (D2).
//
// THE DEFECT. A customer /call whose RESTAction has a GET stage served from a
// KNOWN-STALE content cell is "stale-derived" (#710 PR-2b D2). When the same
// resolve also sends a write stage, the write's branch-E egress declines the
// Put (an external touch), so the response is not armable, and the D2 write
// point (dispatchers.staleWritePoint) withdraws it and re-runs the WHOLE
// request with D2 disabled: the write was sent a second time.
//
// THE RULE (generic, no caps or timers), at the one place a write leaves
// snowplow (branch E; every other dispatch branch is GET-only):
//
//	(a) a write is not sent from a pass that is already stale-derived: that
//	    pass is withdrawn and re-run anyway (the refusal is a stage error, so
//	    its Put declines), and the re-run — D2 disabled, every read fresh —
//	    sends it once;
//	(b) once a write is sent, the rest of the resolve is never served stale
//	    (staleServeEligible), so it is never withdrawn and never re-run.
//
// A resolve with no D2 sink (background work, a dry run, the re-run itself, a
// nested resolve filling its own cell) is untouched: BeginWrite is nil-safe.
package api

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// refuseWriteFromStalePass applies rule (a) and records rule (b) for item i.
// It returns true when the write must not be sent from this pass.
func (r *resolveRun) refuseWriteFromStalePass(gctx context.Context, sc *stageCtx, i int) bool {
	call := sc.calls[i]
	if !isWriteVerb(call.Verb) || cache.StaleServeSinkFromContext(gctx).BeginWrite() {
		return false
	}
	verb := ptr.Deref(call.Verb, http.MethodGet)
	msg := fmt.Sprintf("stage %q verb %s is not sent from a pass served stale content; the fresh re-run sends it (#905)", sc.id, verb)
	var itemErr error
	if !call.ContinueOnError {
		itemErr = fmt.Errorf("api %s item %d failed: %s", sc.id, i, msg)
	}
	r.recordItemError(sc.dictMu, sc.itemErrs, sc.id, i, call.ErrorKey, msg, msg, itemErr)
	r.log.Debug("write stage held for the fresh re-run: this pass was served stale content",
		slog.String("name", sc.id),
		slog.String("verb", verb),
		slog.String("restaction_namespace", r.opts.RESTActionNamespace),
		slog.String("restaction_name", r.opts.RESTActionName))
	return true
}
