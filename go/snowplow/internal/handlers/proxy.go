package handlers

import (
	"log/slog"
	"net/http"
	"net/url"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Dispatcher routes a GET /call whose addressed GVR is handled (restactions /
// widgets) to that per-GVR resolve handler, falling through to next for every
// other GVR. It is GET-ONLY BY DESIGN: a write-verb /call (POST/PUT/PATCH/
// DELETE) is a raw apiserver passthrough and MUST reach handlers.Call()
// unchanged, never a resolve handler. #186 adds ReadDispatcher for the
// body-carrying read route; this GET-only guard is left byte-identical, so no
// existing route's semantics shift.
func Dispatcher(handlers map[string]http.Handler) func(http.Handler) http.Handler {
	return dispatcherForMethod(handlers, http.MethodGet)
}

// ReadDispatcher is the POST /call/read variant (#186). It routes a POST whose
// addressing (apiVersion/resource — still carried in the query, only extras
// moves to the body) names a handled GVR to the SAME resolve handlers GET
// /call uses, so a read carrying a large extras BODY reaches the resolver. It
// is mounted ONLY on POST /call/read; the plain GET-only Dispatcher above is
// untouched. The read route's terminal fallthrough MUST be a read-only Call
// (handlers.CallRead) so an unhandled-GVR POST is a GET passthrough, never a
// create.
func ReadDispatcher(handlers map[string]http.Handler) func(http.Handler) http.Handler {
	return dispatcherForMethod(handlers, http.MethodPost)
}

// dispatcherForMethod is the shared core of Dispatcher/ReadDispatcher. On a
// request whose method equals dispatchMethod it looks up the addressed GVR's
// resolve handler and forwards to it (or to next on a miss); any other method
// falls straight through to next. Splitting on the method keeps GET /call and
// POST /call/read each single-purpose — the GET-only guard that protects the
// write-verb passthrough is preserved exactly for Dispatcher.
func dispatcherForMethod(handlers map[string]http.Handler, dispatchMethod string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		fn := func(wri http.ResponseWriter, req *http.Request) {
			// #282 — a request addressing a SUBRESOURCE (.../{name}/<subresource>,
			// e.g. status) must NOT be served from the resolve/cache handlers: they
			// key on the WHOLE object (ParseGVR + name, no subresource), so a
			// subresource GET would be mis-served the PARENT's cached body. Fall
			// straight through to next (handlers.Call / CallRead), which builds the
			// subresource path (buildURIPath) and hits the apiserver directly. This
			// is uniform for the GET dispatcher and the POST /call/read variant;
			// write verbs already fall through via the method check below. Caching
			// subresources is a separate future feature, out of #282 scope.
			if req.URL.Query().Get("subresource") != "" {
				next.ServeHTTP(wri, req)
				return
			}
			// #443 — the /call-only parameters fall through the same way.
			// raw=true asks for the STORED restactions / widgets object as the
			// caller, so it must skip the resolver. dryRun and fieldValidation
			// are never valid on a read. Falling through on PRESENCE (any
			// value) lets the call handler validate every one of them, so a
			// bad value is a 400 and never a silently resolved read.
			if hasCallOnlyParam(req.URL.Query()) {
				next.ServeHTTP(wri, req)
				return
			}
			if req.Method != dispatchMethod {
				next.ServeHTTP(wri, req)
				return
			}

			log := xcontext.Logger(req.Context())

			api := req.URL.Query().Get("apiVersion")
			if len(api) == 0 {
				log.Warn("missing 'apiVersion' query parameter")
			}

			res := req.URL.Query().Get("resource")
			if len(res) == 0 {
				log.Warn("missing 'resource' query parameter")
			}

			gv, err := schema.ParseGroupVersion(api)
			if err != nil {
				log.Error("unable to create schema.GroupVersion",
					slog.String("api", api), slog.Any("err", err))
			}
			gvr := gv.WithResource(res)

			key := gv.Group
			// Hack caused by new Widgets handlers
			if res == "restactions" {
				key = "restactions." + gv.Group
			}

			h, ok := handlers[key]
			if !ok {
				log.Warn("handler not found", slog.String("gvr", gvr.String()))
				next.ServeHTTP(wri, req)
			} else {
				log.Debug("handler found, forwarding request", slog.String("gvr", gvr.String()))
				h.ServeHTTP(wri, req)
			}
		}

		return http.HandlerFunc(fn)
	}
}

// callOnlyParams are the #443 query parameters only the /call handler acts on.
// A request carrying any of them bypasses the resolve handlers (see
// dispatcherForMethod).
var callOnlyParams = []string{"raw", "dryRun", "fieldValidation"}

func hasCallOnlyParam(q url.Values) bool {
	for _, p := range callOnlyParams {
		if _, ok := q[p]; ok {
			return true
		}
	}
	// reviewer-415 H1 — a misspelling of dryRun / fieldValidation also falls
	// through, so the call handler rejects it instead of a resolver ignoring it.
	for k := range q {
		if nearMissParam(k) != "" {
			return true
		}
	}
	return false
}
