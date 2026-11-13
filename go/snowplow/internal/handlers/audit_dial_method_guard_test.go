package handlers

// audit_dial_method_guard_test.go — #1379 review LOW c: the F1 falsifier runs
// in EVERY arm of this package that emits an audit record, by construction.
// The emitter calls the guard for a record whose http.request.method is not
// the method of its krateo.audit.verb (the method snowplow served instead of
// the one it sent), and the guard fails the arm that emitted it.

import (
	"fmt"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

func init() {
	audit.SetDialMethodGuardForTest(func(method, verb string) {
		panic(fmt.Sprintf("F1 falsifier: a record names http.request.method %q, not the method of its verb %q: the request snowplow served, not the one it sent", method, verb))
	})
}
