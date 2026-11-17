//go:build !(js && wasm)

package credhttp

import "k8s.io/client-go/transport"

// BrowserEgress is nil on a server build: snowplow dials its own connections,
// and a nil wrapper leaves a transport chain exactly as it was
// (transport.Wrappers skips it). Only the js/wasm build sets it
// (egress_js.go).
var BrowserEgress transport.WrapperFunc

// browserBuild reports whether this is the js/wasm build, where the browser's
// fetch replaces the connection (BrowserEgress). NewEndpointTransport takes a
// WrapTransport only there (#1521): on a server build a wrapper could replace
// the base transport and with it the guarded dial.
const browserBuild = false
