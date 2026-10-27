package api

import (
	"net/http"

	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// isWriteVerb reports whether a stage verb (nil = GET) can change state on the
// server. It is isSafeMethod's complement (stage_audit.go), the one write
// classifier of the resolver, so every rule that asks "is this a write?" —
// #902's audit, #903, #904, #905 — gives the same answer for the same verb.
func isWriteVerb(verb *string) bool {
	return !isSafeMethod(ptr.Deref(verb, http.MethodGet))
}

// declaredWriteStage returns the first write stage the RESTAction declares (in
// spec order), or "" if it declares none. It reads the spec only, so it is
// known before any stage runs and for a resolve that stops before running one.
func declaredWriteStage(items []*templates.API) string {
	for _, a := range items {
		if a != nil && isWriteVerb(a.Verb) {
			return a.Name
		}
	}
	return ""
}
