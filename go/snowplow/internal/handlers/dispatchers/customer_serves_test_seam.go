// customer_serves_test_seam.go — the cross-package test seam over the #1265
// customer-serve recorder, for the OTLP mirror's falsifier in internal/metrics
// (Go test files are not importable across packages; same reason as
// l1_lookup_test_seam.go).
//
// It is a fixture, not a stub of the thing under test: it counts through the
// SAME customerServe.end the dispatchers' deferred recorder runs, upstream of
// the accessors and the observable callback the falsifier asserts on. The
// dispatcher arms that decide outcomes live in
// issue1265_customer_serves_test.go.
//
// Production code MUST NOT call it.
package dispatchers

// RecordCustomerServeForTest counts one decided serve for handler
// (restactions | widgets), outcome (fresh | stale | cold | refused) and, on a
// cold serve, cause. An unknown handler or outcome records nothing.
func RecordCustomerServeForTest(handler, outcome, cause string) {
	s := &customerServe{counted: true, decided: true, cause: cause}
	found := false
	for h, n := range serveHandlerNames {
		if n == handler {
			s.handler, found = serveHandler(h), true
		}
	}
	if !found {
		return
	}
	found = false
	for o, n := range serveOutcomeNames {
		if n == outcome {
			s.outcome, found = serveOutcome(o), true
		}
	}
	if !found {
		return
	}
	s.end()
}
