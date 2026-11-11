//go:build !(js && wasm)

package credhttp

import "k8s.io/client-go/transport"

// BrowserEgress is nil on a server build: snowplow dials its own connections,
// and a nil wrapper leaves a transport chain exactly as it was
// (transport.Wrappers skips it). Only the js/wasm build sets it
// (egress_js.go).
var BrowserEgress transport.WrapperFunc
