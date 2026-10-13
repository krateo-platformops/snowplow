// demand_revalidate.go — #547 item 2: revalidate a KNOWN-STALE identity-bound
// cell on demand, under the requester.
//
// A customer hit on a known-stale cell (its latest dirty mark ended without an
// accepted re-Put — cache/known_stale.go) still SERVES the cell: zero cold
// navigations. After the body is written, the handler hands the request to
// revalidateKnownStale, which claims the key's single revalidation slot for the
// stale generation (key, t0) and re-runs THIS handler's cold path on a detached
// copy of the request:
//
//   - IDENTITY AND CREDENTIAL: the requester's. The copy keeps every value of the
//     request ctx (identity, its own UserConfig endpoint) — exactly what the
//     customer's cold read uses, which is the read known to work where the
//     background refresh fails (#547 trace row 7). The requester derived THIS key,
//     so their resolve is a valid body for it, and it becomes the cell's
//     representative (the #444 re-pick with repSourceHitter = the requester): the
//     cold path's Put records cacheInputs, which carry the requester.
//   - DETACHED AND BOUNDED: context.WithoutCancel (the response is already
//     written and the request may end), under DemandRevalidationDeadline — the
//     server's own write deadline, the longest any customer resolve may run. It
//     runs on its own goroutine and never blocks the response.
//   - TERMINAL: the cold path's own generation-guarded PutIfGen, which reports
//     its outcome to the revalidation's sink; an accepted, un-remarked write
//     closes the known-stale window (a dirty→fresh sample) and the cold path's
//     publishIfSubscribed announces it over SSE.
//   - FAILURE (owner ruling 1): no Put → the slot is released, the cell stays
//     resident, served and counted; the next read or change retries. Never an
//     eviction.
//
// The re-entered handler skips both L1 lookups (it must resolve, not hit), the
// customer-in-flight mark (no customer is waiting on it) and the live-caller
// observation (the real request already made it); its per-call record carries
// l1_hit="revalidate".
package dispatchers

import (
	"context"
	"log/slog"
	"net/http"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

// isDemandRevalidation reports whether req is a #547 demand revalidation.
func isDemandRevalidation(req *http.Request) bool {
	_, ok := cache.DemandRevalidationFromContext(req.Context())
	return ok
}

// revalidateKnownStale starts the single demand revalidation of key when key is
// known-stale and none is in flight. h is the handler that served the hit; req
// the customer request. Never blocks.
func revalidateKnownStale(h http.Handler, req *http.Request, key string) {
	if key == "" || isDemandRevalidation(req) || cache.Inert(req.Context()) {
		return
	}
	t0, reason, ok := cache.KnownStale(key)
	if !ok || !cache.ClaimDemandRevalidation(key, t0) {
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(req.Context()), cache.DemandRevalidationDeadline())
	ctx, settle := cache.BeginDemandRevalidation(ctx, key, t0)
	r2 := req.Clone(ctx)
	go func() {
		defer cancel()
		fresh := false
		defer func() {
			if p := recover(); p != nil {
				xcontext.Logger(ctx).Error("demand revalidation panicked; cell stays known-stale",
					slog.String("subsystem", "cache"),
					slog.String("key_hash", key),
					slog.Any("panic", p),
				)
			}
			fresh = settle()
			xcontext.Logger(ctx).Debug("demand revalidation settled",
				slog.String("subsystem", "cache"),
				slog.String("key_hash", key),
				slog.String("known_stale_reason", reason),
				slog.Bool("fresh", fresh),
			)
		}()
		h.ServeHTTP(&discardResponseWriter{}, r2)
	}()
}

// discardResponseWriter swallows the re-entered handler's response: the
// customer already has theirs.
type discardResponseWriter struct{ h http.Header }

func (w *discardResponseWriter) Header() http.Header {
	if w.h == nil {
		w.h = http.Header{}
	}
	return w.h
}
func (w *discardResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *discardResponseWriter) WriteHeader(int)             {}
