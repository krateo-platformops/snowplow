// work_origin.go — #1260: which driver produced a log line.
//
// THE QUESTION. The resolver's WARN and ERROR lines ("api call response
// failure", "unable to resolve api endpoint reference", the UAF verb warning,
// the SA re-gate refusal, the refresher's stage-error decline) fire from a
// customer's request AND from the background drivers. Only a request has a
// span, so before #1260 the only split an operator had was the absent trace_id:
// a trace-less line was either correct background work or a request-path site
// that had lost its context, and the line could not say which.
//
// ONE SOURCE OF TRUTH. WorkOrigin does not stamp a second set of names next to
// the existing ones. Every background driver is already marked at its resolve
// root, and NonCustomerWriteOrigin already names each one from those markers
// (the #563 origins, the #214 prewarm path, the #547 demand revalidation, the
// #903 background hop, the #970 export). WorkOrigin returns that name. It adds
// the roots no marker covers, all attribution only (no gate, counter, key or
// log level reads them):
//
//   - WithRequestWork, at the HTTP server root (main.go, newServerHandler): the
//     positive mark of a served request. Without it "no background marker"
//     would have to be read as "request", and a background goroutine that
//     started from context.Background() would be logged as a request: a
//     guess, which the line must never make.
//   - WithStaleReplaceWork, at the two D2 roots (#710 PR-2b, show then
//     replace; restactions/api apistage_stale_debt.go): a debt's content
//     refill and a dependent cell's replace. Neither carries a marker of its
//     own: the refill runs on a bare background context, and the replace runs
//     the refresher's own resolve, so it would read "refresher". Giving them
//     the background-resolve marker instead would change the memory gates and
//     the SA re-gate for them; a log field must not change behaviour.
//   - WithDetachedWork, at a goroutine that detaches from its caller's
//     context (restactions/api cluster_list.go, the cluster-list cold-miss
//     populate): the same rule as workclass.Detach. Work a background driver
//     detached is still that driver's, so it keeps the driver's name; work a
//     request detached is nobody's request any more, so it gets its own name.
//
// PRECEDENCE. The most specific root wins:
//
//  1. a detached root (stale-replace, or a WithDetachedWork root). A D2
//     replace runs the refresher's resolve (RunRefreshNow → resolveOnceProd
//     stamps "refresher"), but the work is the replace.
//  2. NonCustomerWriteOrigin, in its own order: the driver markers.
//  3. request: a served request with none of the above. A request's own
//     re-dispatches (an export, a demand revalidation replaying it, a
//     background hop entering as one) keep its context values and name
//     themselves above.
//
// NO ORIGIN: OMITTED, NOT "unknown". A context with none of them returns
// ("", false) and the line gets no work_origin field:
//   - the absence already says "unknown"; a value that says it too would be
//     a second spelling of the same fact;
//   - every record logged with no context (boot, informer callbacks, plumbing)
//     would otherwise carry the word, burying the real gaps in noise;
//   - the decorator's no-context path stays byte-identical and
//     allocation-free (F6).
//
// A background driver that lands without a marker therefore shows up as a
// line with neither trace_id nor work_origin. The #1260 censuses (dispatchers
// work_origin_census_1260_test.go, restactions/api
// work_origin_stale_replace_1260_test.go) stop that before it ships.
//
// REQUEST WITHOUT trace_id. Every request gets work_origin=request; its
// trace_id comes from the Server span. A request line with no trace_id is a
// #1201 miss, except where no span exists by design: tracing off, and the
// routes tracing.NewServerHandler does not span (/health, /readyz, the RUM
// upload /telemetry/v1/traces and the /debug/ subtree; internal/tracing
// server.go, spanned).
//
// CLOSED SET, NO IDENTITY. Every value is a compile-time constant in
// WorkOrigins.

package cache

import (
	"context"
	"net/http"
)

const (
	// WorkOriginRequest — a request the HTTP server is serving.
	WorkOriginRequest = "request"
	// WorkOriginStaleReplace — a D2 debt refill or dependent replace.
	WorkOriginStaleReplace = "stale-replace"
	// WorkOriginClusterListPopulate — a cluster-list cold-miss populate a
	// request (or unattributed work) started.
	WorkOriginClusterListPopulate = "cluster-list-populate"
)

// WorkOrigins is the CLOSED set WorkOrigin can return.
var WorkOrigins = append(append([]string{WorkOriginStaleReplace, WorkOriginClusterListPopulate},
	NonCustomerWriteOrigins...), WorkOriginRequest)

type ctxKeyWorkRootType struct{}

var ctxKeyWorkRoot = ctxKeyWorkRootType{}

func withWorkRoot(ctx context.Context, origin string) context.Context {
	if ctx == nil {
		return ctx
	}
	return context.WithValue(ctx, ctxKeyWorkRoot, origin)
}

// WithRequestWork marks ctx as a served request's. Attribution only.
func WithRequestWork(ctx context.Context) context.Context {
	return withWorkRoot(ctx, WorkOriginRequest)
}

// WithStaleReplaceWork marks ctx as a D2 refill's or replace's. Attribution
// only.
func WithStaleReplaceWork(ctx context.Context) context.Context {
	return withWorkRoot(ctx, WorkOriginStaleReplace)
}

// WithDetachedWork marks detached, the context a goroutine detached from
// parent starts from. When parent is a background driver's work, detached
// keeps that driver's name; otherwise (a request, or no origin) it is named
// fresh, which must be a member of WorkOrigins. Attribution only.
func WithDetachedWork(detached, parent context.Context, fresh string) context.Context {
	if o, ok := WorkOrigin(parent); ok && o != WorkOriginRequest {
		return withWorkRoot(detached, o)
	}
	return withWorkRoot(detached, fresh)
}

// RequestWorkMiddleware stamps WithRequestWork on every request it serves.
// main.go installs it once, at the server root.
func RequestWorkMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(WithRequestWork(r.Context())))
	})
}

// WorkOrigin names the driver whose root started the work on ctx (see the file
// comment for the order), and reports false when no root marked it.
func WorkOrigin(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	root, _ := ctx.Value(ctxKeyWorkRoot).(string)
	if root != "" && root != WorkOriginRequest {
		return root, true
	}
	if o := NonCustomerWriteOrigin(ctx); o != "" {
		return o, true
	}
	if root == WorkOriginRequest {
		return root, true
	}
	return "", false
}
