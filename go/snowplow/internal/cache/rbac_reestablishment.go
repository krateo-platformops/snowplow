// rbac_reestablishment.go — #260 change-4 (C2): per-RBAC-GVR reflector
// re-establishment counter.
//
// When a reflector re-issues a fresh ESTABLISHMENT request (watchlist OR
// list()+watch()) for an RBAC GVR that is ALREADY established, it re-delivers
// the whole object set → the RBAC snapshot re-evaluates → sub-gen churn. That
// churn rate is the C2 signal the #253/#258/#259 keying decision needs, and the
// global reflector-path transition counter cannot give it per GVR.
//
// #263-SAFE BY CONSTRUCTION: this counter is bumped ONLY from inside
// recordReflectorPath (reflector_path.go), which classifyReflectorRequest
// reaches ONLY AFTER the #263 isForcedVerifyRequest skip. So the tagged
// store-verification forced LIST (RVMatch=NotOlderThan — it LOOKS like a
// re-list) never reaches this counter and is never mis-counted as a
// re-establishment. The count must NOT be derived from the pre-skip wire-shape
// bucket totals (reflectorListCacheEligibleTotal etc.), which include the tagged
// LIST — doing so would re-conflate the forced LIST and regress #334 (the
// #237-B trap #263 removed).

package cache

import (
	"sort"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// rbacReestablishments holds one counter per RBAC GVR. Populated once at init
// from RBACResourceTypes and never re-keyed, so increments/reads are lock-free.
// A GVR absent from the map (not RBAC) is never counted.
var rbacReestablishments = func() map[schema.GroupVersionResource]*atomic.Uint64 {
	m := make(map[schema.GroupVersionResource]*atomic.Uint64, len(RBACResourceTypes))
	for _, gvr := range RBACResourceTypes {
		m[gvr] = new(atomic.Uint64)
	}
	return m
}()

// isRBACTypedGVR reports whether gvr is one of the four typed-RBAC GVRs the
// re-establishment counter is scoped to.
func isRBACTypedGVR(gvr schema.GroupVersionResource) bool {
	_, ok := rbacReestablishments[gvr]
	return ok
}

// recordRBACReestablishment bumps the per-GVR re-establishment counter. Called
// from recordReflectorPath ONLY when the GVR was already established (seen) and
// is an RBAC GVR — i.e. a genuine re-establishment, post-#263-skip. No-op for a
// non-RBAC GVR.
func recordRBACReestablishment(gvr schema.GroupVersionResource) {
	if c := rbacReestablishments[gvr]; c != nil {
		c.Add(1)
	}
}

// RBACReestablishmentSnapshot returns the per-GVR re-establishment counts (OTLP
// {gvr} + /debug/vars). Sorted for stable output.
func RBACReestablishmentSnapshot() map[string]uint64 {
	out := make(map[string]uint64, len(rbacReestablishments))
	for gvr, c := range rbacReestablishments {
		out[gvr.String()] = c.Load()
	}
	return out
}

// RBACReestablishmentTotalForGVR returns the count for one GVR string, or 0 for
// an unknown/non-RBAC GVR. For the §3 per-GVR liveness assertions — a GVR with
// no counter prints NOT OBSERVED, never a bare 0.
func RBACReestablishmentTotalForGVR(gvrString string) (uint64, bool) {
	for gvr, c := range rbacReestablishments {
		if gvr.String() == gvrString {
			return c.Load(), true
		}
	}
	return 0, false
}

// rbacReestablishmentGVRStrings returns the RBAC GVR strings in stable order —
// the closed key set for the /debug/vars + OTLP series.
func rbacReestablishmentGVRStrings() []string {
	out := make([]string, 0, len(rbacReestablishments))
	for gvr := range rbacReestablishments {
		out = append(out, gvr.String())
	}
	sort.Strings(out)
	return out
}

// RecordRBACReestablishmentForTest bumps one RBAC GVR's re-establishment
// counter. TEST-ONLY — lets the OTLP arm (internal/metrics) drive a value
// without the reflector transport harness. No-op for a non-RBAC GVR.
func RecordRBACReestablishmentForTest(gvr schema.GroupVersionResource) {
	recordRBACReestablishment(gvr)
}

// ResetRBACReestablishmentForTest zeroes every per-GVR counter. TEST-ONLY.
func ResetRBACReestablishmentForTest() {
	for _, c := range rbacReestablishments {
		c.Store(0)
	}
}
