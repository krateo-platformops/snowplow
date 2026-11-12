//go:build js && wasm

package main

import (
	"github.com/krateo-platformops/plumbing/env"

	"github.com/krateo-platformops/snowplow/internal/browserbridge"
)

// The browser build (js/wasm, the Agentiko sandbox): main runs as it does in
// a pod, and the page's requests reach its http.Server over the in-process
// loopback network (internal/browserbridge). bridge.js names the port in the
// PORT variable, which main listens on through the same env.ServicePort read;
// it passes no flags.
func init() {
	port := env.ServicePort("PORT", 0)
	if port == 0 {
		panic("snowplow (js/wasm): PORT is not set; bridge.js sets it to the port main listens on")
	}
	browserbridge.Attach(browserbridge.Addr(port))
}
