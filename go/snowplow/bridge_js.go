//go:build js && wasm

package main

import (
	"github.com/krateo-platformops/plumbing/env"

	"github.com/krateo-platformops/snowplow/internal/browserbridge"
	"github.com/krateo-platformops/snowplow/internal/sandbox/fakeapiserver"
)

// The browser build (js/wasm, the Agentiko sandbox): main runs as it does in
// a pod, and the page's requests reach its http.Server over the in-process
// loopback network (internal/browserbridge). bridge.js names the port in the
// PORT variable, which main listens on through the same env.ServicePort read;
// it passes no flags.
//
// The cluster is the sandbox's fake apiserver (internal/sandbox/fakeapiserver),
// started here from the snapshot bridge.js hands over, on the same in-process
// network. snowplow reaches it exactly as it reaches a real one: its
// in-cluster config names KUBERNETES_SERVICE_HOST, its egress leaves through
// the browser's fetch, and bridge.js's network seam relays that origin to the
// "apiserver" target. No other code knows the fake exists.
func init() {
	port := env.ServicePort("PORT", 0)
	if port == 0 {
		panic("snowplow (js/wasm): PORT is not set; bridge.js sets it to the port main listens on")
	}
	api, err := fakeapiserver.Listen()
	if err != nil {
		panic("snowplow (js/wasm): the sandbox's fake apiserver did not start: " + err.Error())
	}
	browserbridge.Attach(browserbridge.Addr(port), browserbridge.Target{Name: "apiserver", Addr: api})
}
