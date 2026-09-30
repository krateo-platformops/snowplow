// rbac_watch_errors.go — #260 change-4 (C2): per-RBAC-GVR reflector watch-error
// counter.
//
// watch_errors_total (informer_watch_stats.go) is GLOBAL across every informer
// family, and watchBroken[gvr] is sticky STATE (broken-now), not a RATE. Neither
// gives the per-RBAC-GVR watch-error RATE the #253/#258/#259 keying decision
// needs: its RELATIONSHIP to the re-establishment (churn) rate distinguishes
// watch-driven churn (cluster-health-fixable) from apiserver-inherent churn (a
// 410/compaction re-list with no client-observed watch error) — not 1:1, so
// neither counter subsumes the other.
//
// #263-SAFE FOR FREE: a store-verification forced LIST is a LIST, never a WATCH,
// so it never enters the reflector watch-error handler — this counter cannot see
// it. (The re-establishment counter's #263-safety is by post-skip placement in
// recordReflectorPath; this one's is by code path.)

package cache

import (
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// rbacWatchErrors holds one counter per RBAC GVR. Populated once at init from
// RBACResourceTypes and never re-keyed → lock-free. A non-RBAC GVR is absent and
// never counted.
var rbacWatchErrors = func() map[schema.GroupVersionResource]*atomic.Uint64 {
	m := make(map[schema.GroupVersionResource]*atomic.Uint64, len(RBACResourceTypes))
	for _, gvr := range RBACResourceTypes {
		m[gvr] = new(atomic.Uint64)
	}
	return m
}()

// recordRBACWatchError bumps the per-GVR watch-error counter. Called from the
// reflector watch-error handler (servable.go watchErrorHandlerFor), which fires
// on EVERY ListAndWatch error — so this counts the rate (matching
// watch_errors_total's every-invocation semantic), above the one-shot WARN.
// No-op for a non-RBAC GVR.
func recordRBACWatchError(gvr schema.GroupVersionResource) {
	if c := rbacWatchErrors[gvr]; c != nil {
		c.Add(1)
	}
}

// RBACWatchErrorSnapshot returns the per-GVR watch-error counts (OTLP {gvr} +
// /debug/vars).
func RBACWatchErrorSnapshot() map[string]uint64 {
	out := make(map[string]uint64, len(rbacWatchErrors))
	for gvr, c := range rbacWatchErrors {
		out[gvr.String()] = c.Load()
	}
	return out
}

// RecordRBACWatchErrorForTest bumps one RBAC GVR's watch-error counter. TEST-ONLY
// — lets the OTLP arm drive a value without the informer transport harness.
func RecordRBACWatchErrorForTest(gvr schema.GroupVersionResource) {
	recordRBACWatchError(gvr)
}

// ResetRBACWatchErrorForTest zeroes every per-GVR counter. TEST-ONLY.
func ResetRBACWatchErrorForTest() {
	for _, c := range rbacWatchErrors {
		c.Store(0)
	}
}
