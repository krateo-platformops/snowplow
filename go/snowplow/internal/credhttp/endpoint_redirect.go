package credhttp

// endpoint_redirect.go — #1106: an endpoint dial never sends a request to an
// origin other than the one it was asked to dial.
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
// downgrade to http on the same host would send a credential in clear, and
// another port is another service) is never sent, whatever the endpoint and
// whatever the request's headers. Every endpoint dial is credentialed in a way
// snowplow cannot see: besides the endpoint's own credential, a stage may set
// any header (`X-Api-Key`, `X-Auth-Token`, `PRIVATE-TOKEN`), and net/http keeps
// every header but Authorization, WWW-Authenticate, Proxy-Authorization and the
// cookie headers on a cross-host redirect (and keeps those too on a subdomain
// or another port of the same hostname). So there is no header test to make:
// the hop is refused. A redirect within the dialled origin is still followed
// (the request goes nowhere it was not already going); a held credential
// refuses every redirect (Client, #723), and this rule adds nothing to it.
//
// WHERE: Send, the one place an endpoint request leaves (the stage fetch and
// the /call passthrough), outermost on the client it is handed, so nothing
// installed inside it — the wire dump of a debug endpoint, a plumbing-built
// client for an AWS or password-only endpoint, a tracing wrapper — can replace
// or bypass it (the oasgen #379 trap: a debug wrapper assigned back onto the
// client bypassed that project's checks).
//
// THE ORIGIN IS COMPARED BY NAME, ON PURPOSE. A redirect hop is sent only to
// the origin the first request was sent to: the same scheme, hostname and port,
// so the same DNS name the first request resolved. A credential is bound to
// that name on every request snowplow sends, the first one included, so a hop
// that re-resolves the name reaches nothing the first request could not; pinning
// the hop to the first request's address would defend the hop with a check the
// first request does not have. (An address allowlist, where a check by name and
// a connect by address can disagree, is a different rule, and not this one.)
//
// Two layers, as for held credentials (credhttp.go):
//  1. CheckRedirect hands the caller the 3xx itself (http.ErrUseLastResponse),
//     so the stage reports it with its own code (#729: 502 naming the redirect);
//  2. a RoundTripper outermost on the client refuses such a hop before a byte
//     reaches the network, for any copy whose CheckRedirect was replaced.

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// maxRedirects is net/http's own bound (Client.CheckRedirect's default policy
// stops after 10 requests), kept because setting CheckRedirect replaces it.
const maxRedirects = 10

// errTooManyRedirects is net/http's own text for that bound.
var errTooManyRedirects = errors.New("stopped after 10 redirects")

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

// refusesHop reports whether a redirect hop to u leaves own, the origin of the
// request that started the chain. An unknown origin refuses every hop.
func refusesHop(own string, u *url.URL) bool {
	return own == "" || Origin(u) != own
}

// SameOriginOnly returns a copy of c whose redirects never leave own (the
// origin of the request it will send): CheckRedirect refuses a hop elsewhere
// (the caller gets the 3xx) before c's own policy is asked, and an outermost
// RoundTripper refuses such a hop before it reaches the network. c is not
// modified.
func SameOriginOnly(c *http.Client, own string) *http.Client {
	cp := *c
	inner := c.CheckRedirect
	cp.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= maxRedirects {
			return errTooManyRedirects
		}
		if refusesHop(own, req.URL) {
			return http.ErrUseLastResponse
		}
		if inner != nil {
			return inner(req, via)
		}
		return nil
	}
	cp.Transport = &refuseCrossOriginHops{next: c.Transport, own: own}
	return &cp
}

// refuseCrossOriginHops is SameOriginOnly's transport backstop.
type refuseCrossOriginHops struct {
	next http.RoundTripper
	own  string
}

func (t *refuseCrossOriginHops) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Response != nil && refusesHop(t.own, req.URL) {
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
