// request_logger.go — #1192. Snowplow-local replacement for plumbing's
// `use.Logger`, mounted at the same place in the chain.
//
// WHY NOT use.Logger. It parses the bearer token and attaches
// user={name,groups} to the request logger, so the caller's name and groups are
// on every line of a customer request. Snowplow's rule (internal/redact) is that
// no identity reaches a log line; the audit record is the one place that stores
// it, and a log line reaches it through the traceId. use.Logger has no option to
// leave the identity out, and what other components put on their own logs is
// not snowplow's to change, so snowplow does not mount it. This is the same
// shape as UserConfig (userconfig.go).
//
// It does what use.Logger does with the identity left out: the traceId reaches
// the logger through xcontext.WithLogger, which stamps it from the context, so
// a line holds it once (use.Logger added its own, and a request's line held two).
package middleware

import (
	"log/slog"
	"net/http"

	xcontext "github.com/krateo-platformops/plumbing/context"
)

// RequestLogger installs the request logger: root with the request's traceId.
// It never reads the Authorization header. use.TraceId runs first in the
// production chain and has already fixed the id; a request that reaches this
// without one gets a new id, so the logger and the context agree.
func RequestLogger(root *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := xcontext.BuildContext(r.Context(),
				xcontext.WithTraceId(xcontext.TraceId(r.Context(), true)),
				xcontext.WithLogger(root),
			)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
