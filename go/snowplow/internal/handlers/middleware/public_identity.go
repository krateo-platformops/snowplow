// public_identity.go — #1334: serve a request that presents NO credential at
// all under one dedicated public identity, when the install opts in.
//
// THE IDENTITY. User krateo:public with exactly one impersonated group,
// system:unauthenticated, and nothing else. The apiserver adds no
// system:authenticated to it (k8s.io/apiserver impersonation.go:126-144: an
// impersonated non-anonymous user gets system:authenticated unless the
// impersonated groups already hold system:unauthenticated), and snowplow's own
// evaluator and key fold compute the same effective group set (#1342,
// cache.ImplicitGroup). So what it can reach is exactly: RoleBindings naming the
// USER krateo:public, plus what system:unauthenticated already has — which is
// what any anonymous caller of the public apiserver already gets. No
// authenticator issues that username: authn refuses the `krateo:` prefix
// (authn 0.42.0), and the colon is outside the DNS-1123 form authn issues.
//
// ACCESS IS ORDINARY RBAC. Nothing here names a page, a path, a widget kind or a
// verb. The identity is resolved through the same pipeline as every other
// request; the apiserver (impersonated) and snowplow's evaluator (the same
// group rule) decide what it may read or write.
//
// THE CREDENTIAL. The public identity has no credential of its own, so snowplow
// acts for it ONLY by impersonation: its ServiceAccount with Impersonate-User
// krateo:public and Impersonate-Group system:unauthenticated, behind the #608
// capability check (cache.PermittedImpersonatingRESTConfig). The request context
// carries that impersonating config the way the background refresher carries a
// cohort's (#574/#615): the impersonating *rest.Config, the in-cluster endpoint
// it dials, and the impersonated-dial marker, so
//   - every client-go read (objects.Get, resourcesRefs, a stage's apiserver
//     read, the /list and /call passthroughs) is sent impersonated;
//   - a stage dial of the in-cluster endpoint impersonates too, or is refused
//     before it is sent (branche_impersonation.go);
//   - the per-user clientconfig endpoint on the context is EMPTY: no server, no
//     token, no certificate. A site that would dial "the caller's own
//     clientconfig" has nothing to present and fails before anything is sent;
//     it can never fall back to snowplow's own credential.
//
// No `<user>-clientconfig` Secret is read for the public identity: a real user
// named `krateo-public` (a valid DNS-1123 name) owns `krateo-public-clientconfig`,
// which is exactly what credsecret.For("krateo:public") would name.
//
// WHEN IT APPLIES. Only when the request carries NO Authorization header field
// at all. Any Authorization header — empty, not Bearer, an empty bearer, a token
// that is malformed, badly signed, expired, signed by a retired key, or whose
// session Secret is gone — takes the authenticated path unchanged and is
// refused there with its 401. A broken sign-in never silently becomes the
// public view. Nothing else on the request can name an identity: headers
// (Impersonate-*, X-Remote-User), cookies and query parameters are not read.
//
// UNAVAILABLE IS OUR FAULT. With the opt-in on but the impersonation not
// granted (the chart grant ships with the same value; a roll can put the image
// first), or snowplow's ServiceAccount credential unreadable, the request is
// answered 503 and nothing is dispatched — never served under the
// ServiceAccount, never 401 (the caller did nothing wrong).
//
// TELEMETRY. The public identity reaches logs and counters only as its class,
// PublicIdentityClass. The counters are snowplow_public_identity{outcome}.
package middleware

import (
	"context"
	"errors"
	"expvar"
	"net/http"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

const (
	// PublicUsername is the username the public identity is impersonated as.
	PublicUsername = "krateo:public"
	// PublicGroup is the public identity's only group. Impersonating it is what
	// keeps the apiserver from adding system:authenticated.
	PublicGroup = "system:unauthenticated"
	// PublicIdentityClass is the only form of the public identity that reaches
	// a log line or a counter.
	PublicIdentityClass = "public"
)

// PublicGroups returns a fresh copy of the public identity's group set.
func PublicGroups() []string { return []string{PublicGroup} }

// PublicIdentity serves token-less requests as the public identity. A nil
// *PublicIdentity is the install's default: token-less requests are 401.
type PublicIdentity struct {
	saEndpoint func() (*endpoints.Endpoint, error)
	saConfig   func() (*rest.Config, error)
}

// NewPublicIdentity returns the public identity backed by snowplow's own
// ServiceAccount endpoint and *rest.Config (idynamic.ServiceAccountEndpoint /
// ServiceAccountRESTConfig in production). The ServiceAccount is only ever the
// transport: every request it sends for the public identity is impersonated.
func NewPublicIdentity(saEndpoint func() (*endpoints.Endpoint, error), saConfig func() (*rest.Config, error)) *PublicIdentity {
	return &PublicIdentity{saEndpoint: saEndpoint, saConfig: saConfig}
}

var errPublicIdentityUnavailable = errors.New("the public identity is not available: snowplow may not impersonate it")

var publicIdentityOutcomes = expvar.NewMap("snowplow_public_identity")

const (
	publicOutcomeServed      = "served"
	publicOutcomeUnavailable = "unavailable"
)

// PublicIdentityOutcome reads one snowplow_public_identity counter (tests,
// /debug/vars).
func PublicIdentityOutcome(outcome string) int64 {
	if v, ok := publicIdentityOutcomes.Get(outcome).(*expvar.Int); ok {
		return v.Value()
	}
	return 0
}

// tokenless reports whether req carries no Authorization header field at all.
// An empty value is still a header the client chose to send: it is refused on
// the authenticated path (TokenMissing), never served as public.
func tokenless(req *http.Request) bool {
	_, present := req.Header["Authorization"]
	return !present
}

// Context returns ctx carrying the public identity and its impersonating
// credential, or errPublicIdentityUnavailable.
func (p *PublicIdentity) Context(ctx context.Context) (context.Context, error) {
	if p == nil || p.saEndpoint == nil || p.saConfig == nil {
		return nil, errPublicIdentityUnavailable
	}
	saEP, err := p.saEndpoint()
	if err != nil || saEP == nil {
		return nil, errPublicIdentityUnavailable
	}
	saRC, err := p.saConfig()
	if err != nil || saRC == nil {
		return nil, errPublicIdentityUnavailable
	}
	imp := cache.ImpersonatingRESTConfigNoFallback(ctx, saRC, PublicUsername, PublicGroups())
	if imp == nil || imp.Impersonate.UserName != PublicUsername ||
		len(imp.Impersonate.Groups) != 1 || imp.Impersonate.Groups[0] != PublicGroup {
		return nil, errPublicIdentityUnavailable
	}
	ctx = xcontext.BuildContext(ctx,
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: PublicUsername, Groups: PublicGroups()}),
		// No credential: a site that would dial the caller's own clientconfig
		// has nothing to send (see the file comment).
		xcontext.WithUserConfig(endpoints.Endpoint{}),
	)
	ctx = cache.WithInternalEndpoint(ctx, saEP)
	ctx = cache.WithInternalRESTConfig(ctx, imp)
	return cache.WithImpersonatedDial(ctx), nil
}

// serve answers a token-less request: next under the public identity, or 503.
func (p *PublicIdentity) serve(wri http.ResponseWriter, req *http.Request, next http.Handler, endAuth func()) {
	ctx, err := p.Context(req.Context())
	if err != nil {
		publicIdentityOutcomes.Add(publicOutcomeUnavailable, 1)
		xcontext.Logger(req.Context()).WarnContext(req.Context(), "public identity unavailable; token-less request refused",
			"identity_class", PublicIdentityClass,
			"needed", "the impersonate grant the chart value publicIdentity.enabled renders")
		response.ServiceUnavailable(wri, err)
		return
	}
	publicIdentityOutcomes.Add(publicOutcomeServed, 1)
	endAuth()
	next.ServeHTTP(wri, req.WithContext(ctx))
}
