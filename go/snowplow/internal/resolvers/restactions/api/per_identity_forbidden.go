// per_identity_forbidden.go — #681: classify a stage error as a per-identity
// forbidden (typed, never by message text) and replace it with ONE
// identity-free canonical body before it enters the result.
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

	"github.com/krateo-platformops/plumbing/http/response"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
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
	m, err := response.AsMap(response.New(http.StatusForbidden, errors.New(msg)))
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

// notePerIdentityForbidden bumps the per-identity forbidden sink at the same
// site, for the same item, as the stage-error bump. external is true for F2
// (its dial already bumped the ExternalTouchedSink).
func notePerIdentityForbidden(ctx context.Context, external bool) {
	s := cache.PerIdentityForbiddenSinkFromContext(ctx)
	s.BumpStage()
	if external {
		s.BumpExternal()
	}
}
