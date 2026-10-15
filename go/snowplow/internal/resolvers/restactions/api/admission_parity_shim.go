// admission_parity_shim.go — TEST-ONLY exported shims for the cross-package
// ceiling drift-guard parity test (#64 parallel-copy backstop, fold 2026-07-03).
//
// WHY A SEPARATE FILE: nested_resolve_bound.go keeps its OWN ceiling calc (the
// deliberate C1 choice — it is NOT rewired to call cache.AdmissionCeiling; #720
// changed who waits on that gate, not the calc). So these exported wrappers
// live here, outside the gate file. They expose this package's private
// admissionCeiling() + its runtime seams so a test in a package that imports
// BOTH `api` and `cache` (dispatchers) can inject the SAME (GOMEMLIMIT,
// liveHeap) into both arithmetics and assert byte-identical ceilings — the
// mechanical link the two hand-copied calcs otherwise lack.
//
// These wrappers are test-only by INTENT (mirroring the existing
// setRuntimeSeamsForTest / inFlightWeightForTest pattern) and carry ZERO
// customer-path behavior: they are never called from production code.
package api

import "context"

// IsBackgroundNestedTreeForTest exposes the nested-resolve gate's driver
// classification (#720) so the dispatchers driver-level arms can assert that
// each background driver's REAL production ctx is classified BACKGROUND (it
// waits under the ceiling), never CUSTOMER (it would bypass it). TEST-ONLY.
func IsBackgroundNestedTreeForTest(ctx context.Context) bool {
	return isBackgroundNestedTree(ctx)
}

// AdmissionCeilingForTest exposes this package's admissionCeiling() for the
// cross-package parity test. TEST-ONLY.
func AdmissionCeilingForTest() (ceiling int64, unlimited bool) {
	return admissionCeiling()
}

// SetRuntimeSeamsForTest is the exported form of setRuntimeSeamsForTest so the
// cross-package parity test can inject the same (limit, liveHeap) this package's
// admissionCeiling() reads. TEST-ONLY. Restore with ResetNestedResolveBoundForTest.
func SetRuntimeSeamsForTest(limitFn, liveFn func() int64) {
	setRuntimeSeamsForTest(limitFn, liveFn)
}

// ResetNestedResolveBoundForTest is the exported form of
// resetNestedResolveBoundForTest — restores this package's production runtime
// seams after a parity test. TEST-ONLY.
func ResetNestedResolveBoundForTest() {
	resetNestedResolveBoundForTest()
}
