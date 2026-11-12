package credhttp

// plumbing_clients.go — the clients plumbing builds, handed out by credhttp
// only, each already passed through plumbingClient (#1359).
//
// THE FENCE. Outside this package no code may build or send on a client of
// plumbing's: nothing outside credhttp imports plumbing/dialguard, calls
// plumbing/http/request's HTTPClientForEndpoint or Do, or calls jwtutil's
// NewJWKSKeySource or WithJWKSHTTPClient
// (plumbing_client_fence_1359_test.go, type-checked over every build of the
// module). So every plumbing client snowplow sends on comes from here or from
// Do (do.go), and on js/wasm each sets fetch's credentials mode to "omit"
// (egress_js.go). On a server build plumbingClient is the identity, so each
// client is exactly the one plumbing builds.

import (
	"net/http"
	"time"

	"github.com/krateo-platformops/plumbing/dialguard"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

// PlumbingEndpointClient is plumbing's client for ep (request.HTTPClientForEndpoint),
// built from a copy of ep without Debug so plumbing never installs its wire
// dump (#1112 D4), and passed through plumbingClient. It is the plumbing branch
// of a stage fetch (restactions/api endpoints_tls.go); like
// HTTPClientForEndpoint, it can return a client alongside an error.
func PlumbingEndpointClient(ep *endpoints.Endpoint, ri *request.RequestInfo) (*http.Client, error) {
	cli, err := request.HTTPClientForEndpoint(WithoutPlumbingWireDump(ep), ri)
	return plumbingClient(cli), err
}

// JWKSKeySource is jwtutil.NewJWKSKeySource(url, opts...) with the client
// plumbing builds by default (dialguard.Client) at timeout, passed through
// plumbingClient. The caller cannot replace the client: jwtutil's
// WithJWKSHTTPClient is fenced to this package. timeout must be positive (main
// refuses any other at startup).
func JWKSKeySource(url string, timeout time.Duration, opts ...jwtutil.JWKSOption) *jwtutil.JWKSKeySource {
	all := append([]jwtutil.JWKSOption{jwtutil.WithJWKSHTTPClient(plumbingClient(dialguard.Client(timeout)))}, opts...)
	return jwtutil.NewJWKSKeySource(url, all...)
}
