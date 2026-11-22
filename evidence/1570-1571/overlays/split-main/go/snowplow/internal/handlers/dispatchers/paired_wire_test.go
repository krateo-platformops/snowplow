//go:build paired854

package dispatchers

import "github.com/krateo-platformops/snowplow/internal/cache"

// pairedWireCustomerSignal is main.go's customer-signal wiring for the
// refresher in THIS tree, so the paired harnesses (paired_865_test.go,
// paired_fresh_1570_test.go) run the production yield of the code under test:
// here, the boolean customer-inflight predicate.
func pairedWireCustomerSignal() {
	cache.SetCustomerInflightHook(CustomerInFlight)
}
