//go:build !(js && wasm)

package main

import (
	"os"
	"syscall"
)

// shutdownSignals are the signals that start the server's graceful shutdown
// (main). SIGKILL cannot be caught; it stays listed as it always was.
var shutdownSignals = []os.Signal{
	os.Interrupt,
	syscall.SIGINT,
	syscall.SIGTERM,
	syscall.SIGKILL,
	syscall.SIGHUP,
	syscall.SIGQUIT,
}
