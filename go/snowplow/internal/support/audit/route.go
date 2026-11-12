// route.go — an audited route's audit descriptor (audit#22 (a), AUDIT-4
// installer#410): which routes are audited, and as what, is declared once, where
// the route is registered (package main's routeTable.HandleAudited), and every
// emitter of a record for the route's requests reads that one declaration.
//
// THE RULE. A request refused before its handler ran gets a denied record
// exactly when the same request, admitted, would have produced a record. So a
// refusal never needs a list of routes of its own: it asks the route's
// descriptor what the request asked for (Asked), and the descriptor answers
// with the handler's own rules, or says the request would have produced no
// record (a read, a request the handler would refuse as malformed).
package audit

import (
	"context"
	"net/http"
)

// Route is the audit descriptor of one audited route.
type Route struct {
	// Asked is the record of what req asked for, built by the route handler's
	// own rules from the request alone: the action, the verb, the method and
	// the target (a Kubernetes object, or the route's url.path). The emitter
	// adds what happened (outcome, status code, denial reason, message). ok
	// is false when the request, admitted, would have produced no record.
	Asked func(req *http.Request) (ev Event, ok bool)
}

type routeKey struct{}

// WithRoute returns the middleware that carries route on every request's
// context, for an emitter that runs before the handler (a refusal).
func WithRoute(route Route) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(context.WithValue(req.Context(), routeKey{}, route)))
		})
	}
}

// RouteFrom returns the descriptor of the route ctx's request was served on.
func RouteFrom(ctx context.Context) (Route, bool) {
	r, ok := ctx.Value(routeKey{}).(Route)
	return r, ok && r.Asked != nil
}

// EmitRefused writes the denied record of req, refused by snowplow before its
// handler ran, for reason (one of the request writer's closed denial reasons),
// when its route is audited and the request, admitted, would have produced a
// record. ctx must carry the verified caller. Denied: no authority answered, so
// no status code. The body is message, a fixed phrase: never a value of the
// request.
func EmitRefused(ctx context.Context, req *http.Request, reason, message string) {
	route, ok := RouteFrom(ctx)
	if !ok {
		return
	}
	ev, ok := route.Asked(req.WithContext(ctx))
	if !ok {
		return
	}
	ev.Outcome, ev.StatusCode, ev.DenialReason, ev.Message = OutcomeDenied, 0, reason, message
	Emit(ctx, ev)
}
