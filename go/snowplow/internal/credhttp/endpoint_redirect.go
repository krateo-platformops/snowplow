package credhttp

// endpoint_redirect.go — #1106: a credential is never sent to a host other than
// the endpoint's own.
//
// THE DEFECT. A named endpointRef carries a third party's own credential (a
// bearer token, basic auth, a client certificate, an AWS signature). Its client
// is not built by Client(), so it kept net/http's default redirect behaviour,
// and net/http's protection does not reach it: Go strips Authorization from a
// redirect to another domain only when the header was set on the REQUEST, while
// the endpoint's bearer and basic credentials are added by a RoundTripper
// (plumbing's bearerAuthRoundTripper, client-go's) on every hop, and a client
// certificate is presented in the TLS handshake to whichever host the hop dials.
// Measured on main 22fe0579: a GET answered 302 to another host reached it with
// `Authorization: Bearer <endpoint token>`; a PUT answered 307 reached it with
// the token AND the full body.
//
// THE RULE. A redirect hop to another ORIGIN (scheme, host and port: a
// downgrade to http on the same host would send the credential in clear, and
// another port is another service) is refused whenever that hop would carry a
// credential: the endpoint presents one (endpointPresentsCredential), or the hop
// request itself carries a credential header (the user's bearer a stage exports
// with exportJwt, an Authorization or Cookie the RESTAction sets; Go keeps those
// on a redirect to a subdomain). It is #723's rule for held credentials narrowed
// to what makes it necessary: a redirect within the endpoint's own origin is
// still followed (the credential goes nowhere it was not already going), and a
// hop that carries no credential at all keeps net/http's behaviour.
//
// Two layers, as for held credentials (credhttp.go):
//  1. CheckRedirect hands the caller the 3xx itself (http.ErrUseLastResponse),
//     so the stage reports it with its own code (#729: 502 naming the redirect);
//  2. a RoundTripper outermost on the client refuses such a hop before a byte
//     reaches the network, for any client built later from a copy whose
//     CheckRedirect was replaced.

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/krateo-platformops/plumbing/endpoints"
)

// maxRedirects is net/http's own bound (Client.CheckRedirect's default policy
// stops after 10 requests), kept because setting CheckRedirect replaces it.
const maxRedirects = 10

// errTooManyRedirects is net/http's own text for that bound.
var errTooManyRedirects = errors.New("stopped after 10 redirects")

// EndpointPresentsCredential reports whether a dial of ep presents a
// credential on every request it sends, whichever host that is: a bearer
// token, basic auth (client-go keys it on the username, plumbing on the
// password, so either), a client certificate, an AWS signature, or userinfo in
// the server URL.
func EndpointPresentsCredential(ep *endpoints.Endpoint) bool {
	if ep == nil {
		return false
	}
	if ep.Token != "" || ep.Username != "" || ep.Password != "" || ep.HasCertAuth() || ep.HasAwsAuth() {
		return true
	}
	u, err := url.Parse(ep.ServerURL)
	return err == nil && u.User != nil
}

// credentialHeaders are the request headers that carry a credential: the ones
// net/http itself treats as sensitive on a redirect, plus Proxy-Authorization.
var credentialHeaders = []string{"Authorization", "Proxy-Authorization", "Cookie", "Cookie2"}

// headerCarriesCredential reports whether h carries a credential header.
func headerCarriesCredential(h http.Header) bool {
	for _, k := range credentialHeaders {
		if h.Get(k) != "" {
			return true
		}
	}
	return false
}

// Origin is the (scheme, host, port) a URL addresses, lower-cased, with the
// scheme's default port made explicit; "" when u has no scheme or host.
func Origin(u *url.URL) string {
	if u == nil || u.Scheme == "" || u.Hostname() == "" {
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
	return scheme + "://" + net.JoinHostPort(strings.ToLower(u.Hostname()), port)
}

// originOf is Origin of a raw URL; "" when it does not parse.
func originOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	return Origin(u)
}

// endpointRedirectPolicy decides one redirect hop of a dial of an endpoint.
type endpointRedirectPolicy struct {
	own          string // the endpoint's origin; "" refuses every credentialed hop
	credentialed bool   // the endpoint presents a credential on every hop
}

// refuses reports whether a hop to u that carries header h must not be sent.
func (p endpointRedirectPolicy) refuses(u *url.URL, h http.Header) bool {
	if !p.credentialed && !headerCarriesCredential(h) {
		return false
	}
	return p.own == "" || Origin(u) != p.own
}

// EndpointClient applies #1106's rule to c, the client of a dial of ep (in
// place), and returns it. It is for an endpoint whose credential snowplow does
// not hold (a named endpointRef); a held credential takes Client(), which
// refuses every redirect.
func EndpointClient(c *http.Client, ep *endpoints.Endpoint) *http.Client {
	p := endpointRedirectPolicy{credentialed: EndpointPresentsCredential(ep)}
	if ep != nil {
		p.own = originOf(ep.ServerURL)
	}
	c.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxRedirects {
			return errTooManyRedirects
		}
		// net/http has already copied the headers it keeps onto req, so
		// req.Header is what this hop would send.
		if p.refuses(req.URL, req.Header) {
			return http.ErrUseLastResponse
		}
		return nil
	}
	c.Transport = &refuseCrossOriginHops{next: c.Transport, policy: p}
	return c
}

// refuseCrossOriginHops is EndpointClient's transport backstop.
type refuseCrossOriginHops struct {
	next   http.RoundTripper
	policy endpointRedirectPolicy
}

func (t *refuseCrossOriginHops) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Response != nil && t.policy.refuses(req.URL, req.Header) {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		return nil, &RedirectRefusedError{StatusCode: req.Response.StatusCode}
	}
	next := t.next
	if next == nil {
		next = http.DefaultTransport
	}
	return next.RoundTrip(req)
}

// WrappedRoundTripper lets client-go/utilnet walk the chain.
func (t *refuseCrossOriginHops) WrappedRoundTripper() http.RoundTripper { return t.next }
