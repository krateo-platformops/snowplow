// debugauthz.go — #1362: the /debug/* surface is AUTHORIZED through Kubernetes
// RBAC, not just authenticated.
//
// WHY. RefreshAuth proves who the caller is. It says nothing about what the
// caller may see, and several /debug routes report metadata about EVERY
// identity's cache entries and about objects the caller's own RBAC may forbid
// (/debug/apistage lists each resident entry's coordinates and item count). So
// an authenticated caller must also be granted the route.
//
// ONE MECHANISM. The grant is the one Kubernetes already has for paths outside
// the API: a ClusterRole rule with `nonResourceURLs`. Each request is
// authorized as the apiserver would authorize the same request against
// itself. The verb is the lowercase HTTP method (get, post, ...), and the path
// is req.URL.Path, checked for the caller's username and groups from the JWT.
// The decision is rbac.EvaluateRBAC, the same authorizer, memo and group-set
// rules every other snowplow RBAC decision uses: in-process against the
// informer-cached RBAC with cache on (zero apiserver calls), and a
// SelfSubjectAccessReview with cache off. No route is special-cased, and there
// is no env knob.
//
// OUTCOMES.
//   - allowed                                 → the handler runs;
//   - denied                                  → 403, the plumbing Status shape
//     every other route's 403 has, with the apiserver's message form;
//   - the evaluator could not decide (error)  → 503 with a NAMED reason. Never
//     200: an undecidable request fails closed.
//
// It runs AFTER RefreshAuth, which put UserInfo on the context. With no
// UserInfo there is no one to authorize, which is a wiring fault: 503
// `no_identity`.

package middleware

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

// The named reasons a /debug request answers 503 with. They are part of the
// response message ("debug authorization unavailable (<reason>): ...") and of
// the WARN log line, so an operator can tell a boot-window 503 from a broken
// review without reading code.
const (
	DebugAuthzReasonNoIdentity        = "no_identity"
	DebugAuthzReasonWatcherNotWired   = "rbac_watcher_not_wired"
	DebugAuthzReasonSnapshotNotBuilt  = "rbac_snapshot_not_built"
	DebugAuthzReasonReviewUnavailable = "review_unavailable"
	DebugAuthzReasonEvaluatorError    = "evaluator_error"
)

// debugAuthzReason names an evaluator error.
func debugAuthzReason(err error) string {
	switch {
	case errors.Is(err, rbac.ErrWatcherNotWired):
		return DebugAuthzReasonWatcherNotWired
	case errors.Is(err, rbac.ErrSnapshotNotBuilt):
		return DebugAuthzReasonSnapshotNotBuilt
	case errors.Is(err, rbac.ErrReviewUnavailable):
		return DebugAuthzReasonReviewUnavailable
	default:
		return DebugAuthzReasonEvaluatorError
	}
}

// DebugAuthz authorizes each request as a non-resource request for the
// identity RefreshAuth put on the context (see the file header).
func DebugAuthz() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(wri http.ResponseWriter, req *http.Request) {
			ctx := req.Context()
			log := xcontext.Logger(ctx)

			ui, err := xcontext.UserInfo(ctx)
			if err != nil {
				debugAuthzUnavailable(wri, req, DebugAuthzReasonNoIdentity, err)
				return
			}

			check := rbac.NonResourceCheck(ui.Username, ui.Groups, req.Method, req.URL.Path)
			// The verdict alone is consumed; skip the binding-UID sort.
			check.SkipBindingUID = true
			allowed, _, err := rbac.EvaluateRBAC(ctx, check)
			if err != nil {
				debugAuthzUnavailable(wri, req, debugAuthzReason(err), err)
				return
			}
			if !allowed {
				log.DebugContext(ctx, "debug.authz.denied",
					slog.String("verb", check.Verb),
					slog.String("path", check.NonResourceURL))
				response.Forbidden(wri, fmt.Errorf("forbidden: User %q cannot %s path %q",
					ui.Username, check.Verb, check.NonResourceURL))
				return
			}
			next.ServeHTTP(wri, req)
		})
	}
}

// debugAuthzUnavailable answers 503 with the named reason and logs it once at
// WARN: a 503 here is a failure, so it belongs at the level that counts
// failures.
func debugAuthzUnavailable(wri http.ResponseWriter, req *http.Request, reason string, err error) {
	ctx := req.Context()
	xcontext.Logger(ctx).WarnContext(ctx, "debug.authz.unavailable",
		slog.String("reason", reason),
		slog.String("path", req.URL.Path),
		slog.Any("err", err))
	response.ServiceUnavailable(wri, fmt.Errorf("debug authorization unavailable (%s): %w", reason, err))
}
