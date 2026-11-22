//go:build paired854

package dispatchers

// pairedWireCustomerSignal is main.go's customer-signal wiring for the
// refresher in THIS tree, so the paired harnesses (paired_865_test.go,
// paired_fresh_1570_test.go) run the production yield of the code under test:
// here (#1571), the customer count plus the end-of-request wake.
func pairedWireCustomerSignal() {
	InstallRefresherCustomerSignal()
}
