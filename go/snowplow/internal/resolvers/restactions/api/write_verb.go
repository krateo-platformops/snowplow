package api

import (
	"net/http"
	"strings"

	"github.com/krateo-platformops/plumbing/ptr"
)

// isWriteVerb reports whether verb (nil = GET) can change state on the server:
// anything but the safe methods GET, HEAD, OPTIONS and TRACE (RFC 9110 §9.2.1).
// The one write-verb classifier of the resolver: #903 (background work never
// writes) and #904 (a write waits for the stage it depends on) both read it.
func isWriteVerb(verb *string) bool {
	switch strings.ToUpper(strings.TrimSpace(ptr.Deref(verb, http.MethodGet))) {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return false
	}
	return true
}
