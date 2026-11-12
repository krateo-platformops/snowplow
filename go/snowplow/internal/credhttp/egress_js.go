//go:build js && wasm

package credhttp

import (
	"net/http"
	"strings"
	"syscall/js"

	"k8s.io/client-go/transport"
)

// THE BROWSER'S FETCH. On js/wasm every request snowplow sends leaves through
// the browser's fetch: net/http takes the Fetch API path for an http.Transport
// with no dial function of its own, which is what dialguard.Transport is on
// that build (plumbing dialguard/transport_dial_js.go). fetch decides two
// things Go never sees, and each is set in exactly one place:
//
//   - redirects: plumbing's dialguard.RoundTripper sets fetch's redirect mode
//     to "error", so a redirect fails the request instead of being followed
//     where neither CheckRedirect nor RefuseRedirectHops sees the hop.
//     guardedTransport goes through it, and so does every client plumbing
//     builds.
//   - credentials: browserFetchOptions sets fetch's credentials mode to
//     "omit", so the browser attaches none of its own cookies or HTTP
//     credentials and a request carries only what snowplow put on it. Without
//     it fetch's default, same-origin, sends the frame's cookies with a
//     request to the frame's origin (the JWKS, for one). guardedTransport goes
//     through it, and every client plumbing builds does through
//     PlumbingClient.
//
// Every request therefore reaches fetch through one of two chains, both
// carrying both options: guardedTransport (every client built here that names
// no transport of its own, and BrowserEgress, which IS guardedTransport) and a
// plumbing client passed through PlumbingClient (the /call passthrough, the
// plumbing branch of a stage fetch, the JWKS key source).

// BrowserEgress, on js/wasm, replaces the transport it wraps with
// guardedTransport, which sends through the browser's fetch. It is installed
// innermost (Harden; the owned branch of a stage fetch, endpoints_tls.go), so
// every wrapper above it still runs: the credential and Impersonate-* round
// trippers, the redirect refusal, the budget and the counters. Only the
// connection changes hands. The package doc says which policies that leaves
// to the browser.
//
// Where net/http does not use fetch (usesFetch), it is the identity: the
// transport it wraps dials as on a server build.
var BrowserEgress transport.WrapperFunc = func(rt http.RoundTripper) http.RoundTripper {
	if !usesFetch {
		return rt
	}
	return guardedTransport
}

// browserFetchOptions sets fetch's credentials mode to "omit" on every request
// through rt where net/http uses fetch, and is the identity where it does not.
var browserFetchOptions transport.WrapperFunc = func(rt http.RoundTripper) http.RoundTripper {
	if !usesFetch {
		return rt
	}
	return omitCredentials{next: rt}
}

// PlumbingClient is c, a client plumbing built (request.HTTPClientForEndpoint,
// dialguard.Client), as snowplow sends on it: its transport goes through
// browserFetchOptions. plumbing's chain already refuses redirects
// (dialguard.RoundTripper) and leaves the credentials mode at fetch's default.
// nil-safe; a client with no transport of its own is left as it is (plumbing
// builds none).
func PlumbingClient(c *http.Client) *http.Client {
	if c != nil && c.Transport != nil {
		c.Transport = browserFetchOptions(c.Transport)
	}
	return c
}

// usesFetch mirrors net/http's own choice on js/wasm (net/http/roundtrip_js.go,
// jsFetchMissing and jsFetchDisabled): fetch is used when the global fetch
// exists and the program is not running under Node.js, whose argv0 starts
// with "node". Under Node, net/http dials Go's in-process network instead, and
// it refuses the js.fetch: option headers as invalid field names, so setting
// one there would break every request rather than reroute it.
// TestBrowserEgress_UsesFetchIsNetHTTPsChoice checks it against net/http
// itself.
var usesFetch = !js.Global().Get("fetch").IsUndefined() &&
	!(js.Global().Get("process").Type() == js.TypeObject &&
		strings.HasPrefix(js.Global().Get("process").Get("argv0").String(), "node"))

// fetchCredentialsOption is the Header key net/http's fetch path reads fetch's
// credentials mode from, and deletes before it sends.
const fetchCredentialsOption = "js.fetch:credentials"

type omitCredentials struct{ next http.RoundTripper }

func (o omitCredentials) RoundTrip(req *http.Request) (*http.Response, error) {
	// A RoundTripper must not modify its request, and net/http deletes the
	// option key from the Header it is handed, so it goes on a clone. It is
	// set on every request, overriding the caller's.
	r := req.Clone(req.Context())
	r.Header.Set(fetchCredentialsOption, "omit")
	return o.next.RoundTrip(r)
}

// WrappedRoundTripper lets client-go/utilnet walk the chain
// (CloseIdleConnectionsFor).
func (o omitCredentials) WrappedRoundTripper() http.RoundTripper { return o.next }
