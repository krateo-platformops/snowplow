// branche_impersonation.go — #615 / #725 (SECURITY): who snowplow's
// ServiceAccount credential acts as, and where it may be sent, on a branch-E
// dial of the SA endpoint.
//
// THE DEFECT (#615). The refresher swaps only its *rest.Config for an
// impersonating copy and stamps cache.WithImpersonatedDial on the WHOLE
// re-resolve ctx; rbac.MustRegateSADial stands down on that marker. A non-UAF
// stage that falls through to branch E dials the SA ENDPOINT through
// httpFetchAllowingNonJSON, which never impersonated: plain SA bearer, no
// Impersonate-* header — an un-gated SA-breadth read under a cohort identity.
//
// THE RULE (the marker is made TRUE, never widened). For a non-UAF SA-endpoint
// dial under the marker, branchEDecide returns exactly one of:
//
//	impersonate  the dial carries the SAME impersonation the ctx's *rest.Config
//	             carries (rc.Impersonate), applied by client-go's own
//	             transport.NewImpersonatingRoundTripper.
//	fail closed  refused BEFORE it is made (a recorded stage error), never a
//	             plain-SA fallback, when any of these holds:
//	               - no impersonating *rest.Config on the ctx;
//	               - the dial's endpoint is not the ctx internal endpoint (the
//	                 in-cluster apiserver the refresher installed). Both sides
//	                 come from that ONE source, so a DNS-name endpoint and an
//	                 IP-literal config (kubernetes.default.svc vs 10.96.0.1,
//	                 as on every real cluster) never disagree (#721 gate F2);
//	               - the request writes: only a GET, or a POST creating an
//	                 ACCESS REVIEW (the four authorization.k8s.io review
//	                 resources, non-persistent queries that read no tenant
//	                 data; a path with dot segments or %-encoding is never
//	                 classified as one) is performed as the cohort.
//
// ON EVERY SA-ENDPOINT DIAL (marker or not, UAF or not):
//   - a stage's own Impersonate-* and Authorization headers are dropped: only
//     snowplow decides the identity and the credential of its own SA dials
//     (#725 — on main a stage's Impersonate-User made the SA act as any user,
//     and its Authorization replaced the SA bearer);
//   - every request — every redirect hop — must go to the host of the first
//     request; any other host is refused before anything is sent, so neither
//     the SA bearer nor Impersonate-* follows a redirect (#721 gate F1). A
//     same-host redirect is still followed.
//
// What is NOT changed: a UAF stage's identity (it dials the SA by design and
// re-narrows per identity in the refilter); an endpointRef stage (not an SA
// dial: its own credential, its own headers, never an Impersonate-* from
// snowplow). RBAC is not widened: a 403 for the cohort is the outcome.

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
)

// branchEDecision is what a non-UAF SA-endpoint dial at branch E does.
type branchEDecision int

const (
	branchEUnmarked    branchEDecision = iota // no marker: unchanged, the ordinary re-gate decides
	branchEImpersonate                        // dial as the cohort
	branchEFailClosed                         // refuse before the dial
)

// accessReviewResources are the authorization.k8s.io resources whose create is
// a non-persistent review query that reads no tenant data.
var accessReviewResources = map[string]bool{
	"subjectaccessreviews":      true,
	"selfsubjectaccessreviews":  true,
	"localsubjectaccessreviews": true,
	"selfsubjectrulesreviews":   true,
}

// branchEDecide decides the identity of a non-UAF SA-endpoint dial at branch E
// under gctx. On branchEFailClosed, reason says why (identity-free).
func branchEDecide(gctx context.Context, call httpcall.RequestOptions) (imp rest.ImpersonationConfig, d branchEDecision, reason string) {
	if !cache.ImpersonatedDialFromContext(gctx) {
		return rest.ImpersonationConfig{}, branchEUnmarked, ""
	}
	v, present := cache.InternalRESTConfigFromContext(gctx)
	rc, isRC := v.(*rest.Config)
	if !present || !isRC || !dynamic.IsImpersonatingConfig(rc) {
		return rest.ImpersonationConfig{}, branchEFailClosed, "no impersonating config for the cohort"
	}
	if !dialsInternalEndpoint(gctx, call) {
		return rest.ImpersonationConfig{}, branchEFailClosed, "the endpoint is not the in-cluster apiserver endpoint"
	}
	verb := strings.ToUpper(ptr.Deref(call.Verb, http.MethodGet))
	if verb != http.MethodGet && !(verb == http.MethodPost && isAccessReviewPath(call.Path)) {
		return rest.ImpersonationConfig{}, branchEFailClosed, "a background refresh never performs a " + verb + " as the cohort"
	}
	return rc.Impersonate, branchEImpersonate, ""
}

// dialsInternalEndpoint reports whether call dials the ctx internal endpoint —
// the in-cluster apiserver endpoint the refresher installed — compared on the
// one string both sides are copied from.
func dialsInternalEndpoint(gctx context.Context, call httpcall.RequestOptions) bool {
	v, ok := cache.InternalEndpointFromContext(gctx)
	ie, isEP := v.(*endpoints.Endpoint)
	if !ok || !isEP || ie == nil || ie.ServerURL == "" || call.Endpoint == nil {
		return false
	}
	return call.Endpoint.ServerURL == ie.ServerURL
}

// isAccessReviewPath reports whether path creates one of the access-review
// resources. A path with a dot segment, %-encoding, a backslash or a query is
// never one: it could resolve to a different resource on the server than the
// one classified here (#721 gate hardening).
func isAccessReviewPath(path string) bool {
	if strings.ContainsAny(path, `%\?#`) {
		return false
	}
	for _, seg := range strings.Split(path, "/") {
		if seg == "." || seg == ".." {
			return false
		}
	}
	gvr, _, name, ok := cache.ParseAPIServerPathToDep(path)
	return ok && name == "" && gvr.Group == "authorization.k8s.io" && accessReviewResources[gvr.Resource]
}

// isSADialControlledHeader reports whether a stage-supplied header names the
// identity (Impersonate-User / -Uid / -Group / -Extra-*) or the credential
// (Authorization) of a request. On a dial of snowplow's own ServiceAccount
// endpoint such headers are dropped: client-go's impersonating round tripper
// DEFERS to an Impersonate-User already present, and a stage Authorization
// replaces the SA bearer (#725).
func isSADialControlledHeader(key string) bool {
	k := strings.ToLower(strings.TrimSpace(key))
	return strings.HasPrefix(k, "impersonate-") || k == "authorization"
}

type endpointImpersonationKey struct{}

// withEndpointImpersonation makes httpFetchAllowingNonJSON dial as imp.
func withEndpointImpersonation(ctx context.Context, imp rest.ImpersonationConfig) context.Context {
	return context.WithValue(ctx, endpointImpersonationKey{}, imp)
}

// impersonateEndpointTransport wraps rt so every request carries the
// impersonation installed by withEndpointImpersonation; rt unchanged without it.
func impersonateEndpointTransport(ctx context.Context, rt http.RoundTripper) http.RoundTripper {
	imp, ok := ctx.Value(endpointImpersonationKey{}).(rest.ImpersonationConfig)
	if !ok {
		return rt
	}
	return transport.NewImpersonatingRoundTripper(transport.ImpersonationConfig{
		UserName: imp.UserName,
		UID:      imp.UID,
		Groups:   imp.Groups,
		Extra:    imp.Extra,
	}, rt)
}

// hostPinnedTransport refuses any request whose host differs from host. It is
// the OUTERMOST layer of an SA-endpoint dial's client: every request the client
// makes — the first and each redirect hop — passes through it BEFORE the layers
// that add the SA bearer and the Impersonate-* headers, so a refused hop sends
// nothing.
type hostPinnedTransport struct {
	host     string // canonical host:port of the first request
	delegate http.RoundTripper
}

func (t *hostPinnedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if got := canonicalHostPort(req.URL); got != t.host {
		return nil, fmt.Errorf("refusing a request to another host on a ServiceAccount-endpoint dial (redirect away from the apiserver)")
	}
	return t.delegate.RoundTrip(req)
}

// pinSADialHost wraps rt so every request must go to the host of first.
func pinSADialHost(first *url.URL, rt http.RoundTripper) http.RoundTripper {
	return &hostPinnedTransport{host: canonicalHostPort(first), delegate: rt}
}

// canonicalHostPort is scheme://host:port, lower-cased, with the scheme's
// default port filled in.
func canonicalHostPort(u *url.URL) string {
	if u == nil {
		return ""
	}
	scheme := strings.ToLower(u.Scheme)
	port := u.Port()
	if port == "" {
		switch scheme {
		case "https":
			port = "443"
		case "http":
			port = "80"
		}
	}
	return scheme + "://" + strings.ToLower(u.Hostname()) + ":" + port
}
