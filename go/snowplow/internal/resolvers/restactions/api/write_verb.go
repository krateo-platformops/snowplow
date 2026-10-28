package api

import (
	"net/http"

	"github.com/krateo-platformops/plumbing/ptr"
)

// isWriteVerb reports whether a stage verb (nil = GET) can change state on the
// server. It is isSafeMethod's complement (stage_audit.go), the one write
// classifier of the resolver, so every rule that asks "is this a write?" —
// #902's audit, #903, #904, #905 — gives the same answer for the same verb.
func isWriteVerb(verb *string) bool {
	return !isSafeMethod(ptr.Deref(verb, http.MethodGet))
}
