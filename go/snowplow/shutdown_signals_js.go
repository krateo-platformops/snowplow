//go:build js && wasm

package main

import (
	"os"
	"syscall"
)

// shutdownSignals on js/wasm: the server build's list without SIGHUP, which
// the js syscall package does not define. No signal is ever delivered to a
// program running in a browser worker, so main waits on its context for the
// life of the worker, which is what the bridge needs (bridge_js.go).
var shutdownSignals = []os.Signal{
	os.Interrupt,
	syscall.SIGINT,
	syscall.SIGTERM,
	syscall.SIGKILL,
	syscall.SIGQUIT,
}
