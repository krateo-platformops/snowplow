// external_fetch_budget.go — the byte bound of the external fetch's conversion
// memo (internal/resolvers/restactions/api/external_fetch_cache.go).
//
// ZERO-KNOB, like the admission gates: the bound is a share of the memory the
// process can still give, read from the same samplers as AdmissionCeiling. The
// layer may hold at most the reserve fraction (1/8) of what it already holds
// plus the current admission ceiling, so it grows only while there is headroom
// past the reserve and shrinks back (on its next store) when the heap fills.
// With GOMEMLIMIT unset there is no headroom to read, and the share is taken of
// the L1 store's own byte budget instead, so the layer is never unbounded.
package cache

// ExternalFetchCacheBudget returns the most bytes the external conversion memo
// may hold, given that it holds held bytes now.
func ExternalFetchCacheBudget(held int64) int64 {
	if held < 0 {
		held = 0
	}
	ceiling, unlimited := AdmissionCeiling()
	if unlimited {
		maxBytes := defaultResolvedCacheMaxBytes
		// The non-constructing handle: reading the budget never builds L1.
		if c := resolvedCachePublished.Load(); c != nil {
			maxBytes = c.maxBytes
		}
		return maxBytes / admissionReserveFractionDen * admissionReserveFractionNum
	}
	return (held + ceiling) / admissionReserveFractionDen * admissionReserveFractionNum
}
