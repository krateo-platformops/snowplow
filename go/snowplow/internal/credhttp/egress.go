//go:build !(js && wasm)

package credhttp

import (
	"net/http"

	"k8s.io/client-go/transport"
)

// BrowserEgress is nil on a server build: snowplow dials its own connections,
// and a nil wrapper leaves a transport chain exactly as it was
// (transport.Wrappers skips it). Only the js/wasm build sets it
// (egress_js.go).
var BrowserEgress transport.WrapperFunc

// browserFetchOptions is nil on a server build, so guardedTransport is
// dialguard's transport exactly as before (destination.go). Only the js/wasm
// build sets it (egress_js.go).
var browserFetchOptions transport.WrapperFunc

// plumbingClient is c itself on a server build. Only the js/wasm build changes
// it (egress_js.go).
func plumbingClient(c *http.Client) *http.Client { return c }
