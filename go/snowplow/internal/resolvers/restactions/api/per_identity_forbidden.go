// per_identity_forbidden.go — #681: classify a stage error as a per-identity
// forbidden (typed, never by message text) and replace it with ONE
// identity-free canonical body before it enters the result. #1080 adds the
// other per-identity definite answer, a named object's NotFound under
// continueOnError (N1 / N2, see cache/per_identity_forbidden.go), counted on
// the same sink with its own canonical body.
//
// WHY THE BODY IS REWRITTEN (#681 S1, security). The apiserver's Forbidden
// message names the requester — `configmaps "x" is forbidden: User "alice"
// cannot get ...` — and recordItemError writes it into the result under the
// stage's errorKey. Once a denial-only result is cached, every requester that
// shares the binding-set class is served that body. So both kinds of
// per-identity forbidden are recorded as the SAME canonical form, built only
// from the request's own coordinates:
//
//	{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden",
//	 "code":403,"message":"<resource> \"<name>\" is forbidden: user not
//	 authorized to <verb> <ns>/<name>"}
//
// It is the shape the apiserver 403 already had on the wire (plumbing's
// response.Status: kind / apiVersion / status / message / reason / code), with
// the message replaced by the regateDeniedError text. No username, group, SA
// name or token-derived text can reach it. The normalization applies to EVERY
// resolve, live /call included, so a requester's live body and the cached body
// another class member is served are byte-identical; before #681 the live /call
// carried the apiserver's own message (naming the requester, to that same
// requester), and the background F1 text never reached a user (its result was
// never Put).

package api

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/http/response"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
)

// canonicalForbiddenMessage is the identity-free message for a denied read of
// (gvr, ns, name). It is exactly the regateDeniedError text snowplow's own deny
// of the same read carries (a branch C GET, a served LIST — #961), so an
// in-process deny and the apiserver's 403 are recorded with one body.
func canonicalForbiddenMessage(gvr schema.GroupVersionResource, namespace, name, verb string) string {
	return newRegateForbiddenVerb(gvr, namespace, name, verb).Error()
}

// canonicalForbiddenBody is the identity-free value recorded under errorKey.
func canonicalForbiddenBody(msg string) any {
	return canonicalStatusBody(http.StatusForbidden, msg)
}

// canonicalNotFoundBody is the identity-free value a definite NotFound (#1080)
// records under errorKey: the apiserver's 404 Status shape with the message
// rebuilt from the coordinates.
func canonicalNotFoundBody(msg string) any {
	return canonicalStatusBody(http.StatusNotFound, msg)
}

// canonicalStatusBody is plumbing's Status map for code carrying msg.
func canonicalStatusBody(code int, msg string) any {
	m, err := response.AsMap(response.New(code, errors.New(msg)))
	if err != nil || len(m) == 0 {
		return msg
	}
	return m
}

// readVerbFor is the RBAC verb of a GET on the parsed coordinate.
func readVerbFor(name string) string {
	if name == "" {
		return "list"
	}
	return "get"
}

// classifyOwnDialForbidden reports whether an external-fall-through failure is
// an F2 per-identity forbidden: the apiserver's typed 403 Forbidden on the
// requester's OWN dial of an apiserver resource path, for a read. On ok it
// returns the canonical message.
func classifyOwnDialForbidden(gctx context.Context, apiCall *templates.API, call httpcall.RequestOptions, res *response.Status) (string, bool) {
	if res == nil || res.Code != http.StatusForbidden || res.Reason != response.StatusReasonForbidden {
		return "", false
	}
	if apiCall == nil || apiCall.EndpointRef != nil || cache.ServiceAccountDialFromContext(gctx) {
		return "", false
	}
	if verb := call.Verb; verb != nil && *verb != http.MethodGet {
		return "", false
	}
	gvr, ns, name, ok := cache.ParseAPIServerPathToDep(call.Path)
	if !ok {
		return "", false
	}
	return canonicalForbiddenMessage(gvr, ns, name, readVerbFor(name)), true
}

// definiteNotFoundMessage holds the conditions N1 and N2 share on the call
// (cache/per_identity_forbidden.go, #1080): the stage declares continueOnError,
// it is a plain GET of a NAMED object on a parseable apiserver path (no
// subresource), and the watcher is authoritative for the GVR and does not hold
// the object (cache.WatchedAbsent), so its creation dirty-marks the cell
// through the exact-object edge. On ok it returns the canonical, identity-free
// NotFound message built from the coordinates — the apiserver's own phrasing,
// the one stage_failure.go reports.
func definiteNotFoundMessage(call httpcall.RequestOptions) (string, bool) {
	if !call.ContinueOnError || !isPlainGet(call.Verb) || hasSubresourceSuffix(call.Path) {
		return "", false
	}
	gvr, ns, name, ok := cache.ParseAPIServerPathToDep(call.Path)
	if !ok || name == "" {
		return "", false
	}
	noteDefiniteNotFoundCheck(DefiniteNotFoundBeforeWatchCheck)
	if !cache.WatchedAbsent(gvr, ns, name) {
		return "", false
	}
	noteDefiniteNotFoundCheck(DefiniteNotFoundAfterWatchCheck)
	return apierrors.NewNotFound(gvr.GroupResource(), name).Error(), true
}

// The two #1080 check windows SetDefiniteNotFoundCheckHookForTest opens: right
// before the watcher's absence check, and right after it said absent (the
// classification is decided; the result has not been stored yet).
const (
	DefiniteNotFoundBeforeWatchCheck = "before_watch_check"
	DefiniteNotFoundAfterWatchCheck  = "after_watch_check"
)

// definiteNotFoundCheckHook is a TEST seam (nil in production): it lets an arm
// place an object's creation exactly on one side of the absence check — the
// #375 put-then-remark window (#1080 C6) — instead of racing for it.
var definiteNotFoundCheckHook atomic.Pointer[func(stage string)]

func noteDefiniteNotFoundCheck(stage string) {
	if h := definiteNotFoundCheckHook.Load(); h != nil {
		(*h)(stage)
	}
}

// SetDefiniteNotFoundCheckHookForTest installs fn at the two #1080 check
// windows. Production code MUST NOT call it. Returns a restore func.
func SetDefiniteNotFoundCheckHookForTest(fn func(stage string)) (restore func()) {
	var p *func(string)
	if fn != nil {
		p = &fn
	}
	prev := definiteNotFoundCheckHook.Swap(p)
	return func() { definiteNotFoundCheckHook.Store(prev) }
}

// classifyOwnDialNotFound reports whether an external-fall-through failure is
// an N1 definite NotFound: the apiserver's typed 404 NotFound on the
// requester's OWN dial (the F2 provenance conditions: no endpointRef, not an SA
// dial) of a named object, under definiteNotFoundMessage. The apiserver
// authorized this identity's `get` before it looked the object up.
func classifyOwnDialNotFound(gctx context.Context, apiCall *templates.API, call httpcall.RequestOptions, res *response.Status) (string, bool) {
	if res == nil || res.Code != http.StatusNotFound || res.Reason != response.StatusReasonNotFound {
		return "", false
	}
	if apiCall == nil || apiCall.EndpointRef != nil || cache.ServiceAccountDialFromContext(gctx) {
		return "", false
	}
	return definiteNotFoundMessage(call)
}

// classifyAuthorizedNotFound reports whether a branch C failure is an N2
// definite NotFound: the SA dial's NotFound after the per-identity pre-dial
// authorization allowed this identity's `get` (authorizedNotFoundError), under
// definiteNotFoundMessage.
func classifyAuthorizedNotFound(call httpcall.RequestOptions, err error) (string, bool) {
	if !isAuthorizedNotFound(err) {
		return "", false
	}
	return definiteNotFoundMessage(call)
}

// notePerIdentityForbidden bumps the per-identity definite-answer sink at the
// same site, for the same item, as the stage-error bump: a forbidden (F1, F2)
// or, #1080, a named object's NotFound (N1, N2). external is true for F2 and N1
// (their dial already bumped the ExternalTouchedSink).
func notePerIdentityForbidden(ctx context.Context, external bool) {
	s := cache.PerIdentityForbiddenSinkFromContext(ctx)
	s.BumpStage()
	if external {
		s.BumpExternal()
	}
}
