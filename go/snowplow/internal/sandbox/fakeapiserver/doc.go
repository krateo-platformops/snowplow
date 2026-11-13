//go:build js && wasm

// Package fakeapiserver is the Agentiko sandbox's Kubernetes apiserver: it serves a staged snapshot
// (agentiko-sandbox snowplow-wasm/SNAPSHOT.md) from memory, inside the browser build of snowplow.
// It exists only in the js/wasm build; a server build compiles none of it
// (main_browser_build_test.go), and no product code knows it exists: the js main wires it.
//
// It behaves as kube-apiserver v1.36 does wherever snowplow or client-go can observe it, using the
// libraries the apiserver itself uses where it can:
//
//   - request bodies are decoded with client-go's scheme (k8s.io/client-go/kubernetes/scheme), so
//     a protobuf body from a typed clientset is read as the apiserver reads it, and a built-in
//     object is round-tripped through its typed form (quantities are canonical, as the apiserver
//     stores them). A protobuf body of a kind the scheme does not register is the apiserver's 415.
//     Responses are JSON, which every client's Accept allows;
//   - merge and JSON patches go through gopkg.in/evanphx/json-patch.v4 with the apiserver's error
//     mapping (k8s.io/apiserver pkg/endpoints/handlers/patch.go);
//   - errors are k8s.io/apimachinery/pkg/api/errors Status values, so messages, reasons and details
//     are the apiserver's own;
//   - the watch cache is sized as k8s.io/apiserver's cacher sizes it (watchcache.go), so a watch
//     from a resourceVersion that left the window gets the same 410 Expired.
//
// WHAT IT DOES NOT DO, each answered loudly (a `fake.unimplemented` request-log line and a 500),
// never leniently: strategic merge patch, server-side apply with a fieldManager, deletecollection,
// impersonation of extra fields, and version conversion. Defaulting of built-in kinds is not
// applied: the defaulters live in k8s.io/kubernetes, which no library exports; objects come from a
// cluster that already defaulted them.
//
// Authorization comes before routing, as in the apiserver's handler chain, and is RBAC alone over the
// snapshot's roles and bindings. A rules review lists RBAC's rules and is complete or not as the
// reference cluster answered it (snapshot.json capturedFrom.selfSubjectRulesReview): on a GKE cluster,
// whose IAM webhook authorizer follows RBAC, it is incomplete with the webhook's message.
//
// Every request is logged as one line (Options.Log) in SNAPSHOT.md §4's format; the js main sends
// the lines to the worker's console.
//
// The js main (bridge_js.go) starts it with Listen, from the config bridge.js defines (HostConfig),
// on Go's in-process loopback network, and registers it with browserbridge as the "apiserver"
// target. snowplow's requests to the in-cluster address leave through the browser's fetch, and
// bridge.js's network seam relays them there, as a Service routes a pod's.
package fakeapiserver
