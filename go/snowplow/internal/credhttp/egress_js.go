//go:build js && wasm

package credhttp

import (
	"net/http"
	"strings"
	"syscall/js"

	"k8s.io/client-go/transport"
)

// BrowserEgress, on js/wasm, replaces the transport it wraps with the
// browser's fetch. It is installed innermost (Harden; the owned branch of a
// stage fetch, endpoints_tls.go), so every wrapper above it still runs: the
// credential and Impersonate-* round trippers, the redirect refusal, the
// budget and the counters. Only the connection changes hands. The package doc
// says which policies that leaves to the browser.
//
// Where net/http does not use fetch (usesFetch), it is the identity: the
// transport it wraps dials as on a server build.
var BrowserEgress transport.WrapperFunc = func(rt http.RoundTripper) http.RoundTripper {
	if !usesFetch {
		return rt
	}
	return browserFetch{next: fetchTransport}
}

// usesFetch mirrors net/http's own choice on js/wasm (net/http/roundtrip_js.go,
// jsFetchMissing and jsFetchDisabled): fetch is used when the global fetch
// exists and the program is not running under Node.js, whose argv0 starts
// with "node". Under Node, net/http dials Go's in-process network instead, and
// it refuses the js.fetch: option headers as invalid field names, so replacing
// the transport there would break every request rather than reroute it.
var usesFetch = !js.Global().Get("fetch").IsUndefined() &&
	!(js.Global().Get("process").Type() == js.TypeObject &&
		strings.HasPrefix(js.Global().Get("process").Get("argv0").String(), "node"))

// fetchTransport is net/http's Fetch API transport: on js/wasm an
// http.Transport with no dial function of its own takes the fetch path
// (net/http/roundtrip_js.go). One with a dial function dials Go's in-process
// network instead, where nothing outside the module listens.
var fetchTransport http.RoundTripper = &http.Transport{}

// browserFetch sends every request through next with two fetch options set:
//
//   - redirect "error": a redirect fails the request instead of being
//     followed. fetch follows redirects itself, so RefuseRedirectHops would
//     never see a hop; this is how "never follow a redirect" (#723/#724,
//     #1106) holds in a browser. The caller gets an error, not the 3xx.
//   - credentials "omit": the browser attaches none of its own cookies or
//     HTTP credentials, so a request carries only what snowplow put on it.
type browserFetch struct{ next http.RoundTripper }

// The Header keys net/http's fetch path reads its options from (and deletes).
const (
	fetchRedirectOption    = "js.fetch:redirect"
	fetchCredentialsOption = "js.fetch:credentials"
)

func (b browserFetch) RoundTrip(req *http.Request) (*http.Response, error) {
	// A RoundTripper must not modify its request, and net/http deletes the
	// option keys from the Header it is handed, so they go on a clone.
	r := req.Clone(req.Context())
	r.Header.Set(fetchRedirectOption, "error")
	r.Header.Set(fetchCredentialsOption, "omit")
	return b.next.RoundTrip(r)
}
