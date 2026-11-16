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
// things Go never sees, and plumbing's dialguard.RoundTripper sets both
// (plumbing >= v1.54.0, dialguard/fetch_policy_js.go):
//
//   - redirect mode "error": a redirect fails the request instead of being
//     followed where neither CheckRedirect nor RefuseRedirectHops sees the hop;
//   - credentials mode "omit": the browser attaches none of its own cookies or
//     HTTP credentials, so a request carries only what snowplow put on it.
//     fetch's default, same-origin, would send the frame's cookies with a
//     request to the frame's origin (the JWKS, for one).
//
// Every request reaches fetch through dialguard.RoundTripper: through
// guardedTransport (every client built here that names no transport of its
// own, and BrowserEgress, which IS guardedTransport), or through a client
// plumbing builds, which plumbing wraps in it itself (the /call passthrough,
// do.go; PlumbingEndpointClient and JWKSKeySource, plumbing_clients.go). Only
// credhttp builds plumbing's clients (plumbing_client_fence_1359_test.go), so
// no client can skip it.

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

// usesFetch mirrors net/http's own choice on js/wasm (net/http/roundtrip_js.go,
// jsFetchMissing and jsFetchDisabled): fetch is used when the global fetch
// exists and the program is not running under Node.js, whose argv0 starts
// with "node". Under Node, net/http dials Go's in-process network instead,
// where replacing client-go's transport would drop its dial and TLS settings
// for nothing. TestBrowserEgress_UsesFetchIsNetHTTPsChoice checks it against
// net/http itself.
var usesFetch = !js.Global().Get("fetch").IsUndefined() &&
	!(js.Global().Get("process").Type() == js.TypeObject &&
		strings.HasPrefix(js.Global().Get("process").Get("argv0").String(), "node"))
