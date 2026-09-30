// issue260_reestablishment_test.go — #260 change-4 (C2): per-RBAC-GVR reflector
// re-establishment counter.
//
// A reflector re-issuing a fresh establishment (watchlist OR list) for an
// ALREADY-established RBAC GVR re-delivers the whole object set → re-evaluates
// RBAC → sub-gen churn. That per-GVR rate is the C2 signal #253/#258/#259 need.
//
// Arms (freshness-audit brief), all driven through classifyReflectorRequest (the
// REAL path, so the #263 isForcedVerifyRequest skip is exercised):
//   A  a 2nd establishment counts (incl. a watchlist→watchlist re-delivery, which
//      the same-path dedup would hide); the FIRST establishment does NOT count.
//   B  #263-PARITY (load-bearing): a TAGGED forced-verification LIST does NOT
//      count — re-conflating it would regress #334 (the #237-B trap). Vacuity: an
//      untagged same-shape re-list DOES count, so the tag is the only difference.
//   C  scope: a non-RBAC GVR re-establishment is never counted.
// (Arm D, OTLP-registered, lives in internal/metrics.)

package cache

import (
	"errors"
	"net/http"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// reflectorReqFor builds a collection GET for (group/version/resource) with the
// given raw query — the shape a reflector puts on the wire.
func reflectorReqFor(t *testing.T, gvr schema.GroupVersionResource, query string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet,
		"https://apiserver/apis/"+gvr.Group+"/"+gvr.Version+"/"+gvr.Resource+"?"+query, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	return req
}

const (
	reflectorWatchListQuery = "watch=true&sendInitialEvents=true"
	// The store-verification forced-LIST wire shape (Limit:0, RV=lastSyncRV,
	// RVMatch=NotOlderThan) — LOOKS like a re-list; #263 excludes it via the ctx tag.
	reflectorForcedShapeQuery = "limit=0&resourceVersion=100&resourceVersionMatch=NotOlderThan"
)

// cleanReflectorGVR gives a GVR a clean established-state slate for a test.
func cleanReflectorGVR(t *testing.T, gvr schema.GroupVersionResource) {
	t.Helper()
	forgetReflectorPath(gvr)
	rememberReflectorPathGVR(gvr)
	t.Cleanup(func() { forgetReflectorPath(gvr) })
}

func TestIssue260_A_ReestablishmentCountedOnSecondNotFirst(t *testing.T) {
	ResetReflectorPathStatsForTest()
	t.Cleanup(ResetReflectorPathStatsForTest)
	ResetRBACReestablishmentForTest()
	t.Cleanup(ResetRBACReestablishmentForTest)

	gvr := RBACResourceTypes[0]
	cleanReflectorGVR(t, gvr)
	gs := gvr.String()

	// FIRST establishment (watchlist) — a first observation, NOT a re-establishment.
	classifyReflectorRequest(reflectorReqFor(t, gvr, reflectorWatchListQuery))
	if got := RBACReestablishmentSnapshot()[gs]; got != 0 {
		t.Fatalf("first establishment counted as a re-establishment: %d, want 0", got)
	}

	// SECOND establishment — SAME path (watchlist re-delivery). Must count: it
	// re-delivers every object → RBAC churn, and the same-path dedup would hide
	// it unless the count precedes the dedup.
	classifyReflectorRequest(reflectorReqFor(t, gvr, reflectorWatchListQuery))
	if got := RBACReestablishmentSnapshot()[gs]; got != 1 {
		t.Fatalf("RED #260: a watchlist→watchlist re-establishment was not counted (%d, want 1) — a "+
			"same-path re-delivery re-evaluates RBAC and must count; placing the count after the "+
			"same-path dedup would hide it", got)
	}

	// THIRD establishment — a FLIP to list. Also a re-establishment.
	classifyReflectorRequest(reflectorReqFor(t, gvr, reflectorForcedShapeQuery))
	if got := RBACReestablishmentSnapshot()[gs]; got != 2 {
		t.Fatalf("RED #260: a watchlist→list re-establishment was not counted (%d, want 2)", got)
	}
}

func TestIssue260_B_ForcedVerifyLISTNotCountedAsReestablishment(t *testing.T) {
	ResetReflectorPathStatsForTest()
	t.Cleanup(ResetReflectorPathStatsForTest)
	ResetRBACReestablishmentForTest()
	t.Cleanup(ResetRBACReestablishmentForTest)
	ResetRBACWatchErrorForTest()
	t.Cleanup(ResetRBACWatchErrorForTest)

	gvr := RBACResourceTypes[0]
	cleanReflectorGVR(t, gvr)
	gs := gvr.String()

	// Establish first (so a re-list on this GVR WOULD otherwise count).
	classifyReflectorRequest(reflectorReqFor(t, gvr, reflectorWatchListQuery))
	if got := RBACReestablishmentSnapshot()[gs]; got != 0 {
		t.Fatalf("precondition: first establishment counted (%d)", got)
	}

	// A TAGGED forced-verification LIST (same wire shape as a re-list) must NOT
	// count — it never reaches recordReflectorPath (the #263 skip returns first).
	forced := reflectorReqFor(t, gvr, reflectorForcedShapeQuery)
	forced = forced.WithContext(withForcedVerifyTag(forced.Context()))
	classifyReflectorRequest(forced)
	if got := RBACReestablishmentSnapshot()[gs]; got != 0 {
		t.Fatalf("RED (#263/#334 regression): a TAGGED forced-verification LIST was counted as a "+
			"re-establishment (%d, want 0) — this re-conflates the forced LIST that #263 de-conflated "+
			"from reflector establishment (the #237-B trap). The count must sit INSIDE "+
			"recordReflectorPath, after the isForcedVerifyRequest skip.", got)
	}
	// The COMBINED #263-parity: a forced-verification pass ticks NEITHER counter.
	// The forced LIST is a LIST, never a WATCH, so it never enters the watch-error
	// handler — the watch-error counter is #263-safe by code path.
	if got := RBACWatchErrorSnapshot()[gs]; got != 0 {
		t.Fatalf("a forced-verification LIST bumped the RBAC watch-error counter (%d, want 0) — it is a "+
			"LIST, not a WATCH, and must never enter the watch-error handler", got)
	}

	// VACUITY: an UNTAGGED same-shape re-list DOES count — the tag is the only
	// difference, so the arm above is not passing because the shape never counts.
	classifyReflectorRequest(reflectorReqFor(t, gvr, reflectorForcedShapeQuery))
	if got := RBACReestablishmentSnapshot()[gs]; got != 1 {
		t.Fatalf("vacuity: an UNTAGGED same-shape re-list did not count (%d, want 1) — the #263-parity "+
			"arm would pass trivially if this shape never counted; the tag must be the sole variable", got)
	}
}

func TestIssue260_C_NonRBACGVRNotCounted(t *testing.T) {
	ResetReflectorPathStatsForTest()
	t.Cleanup(ResetReflectorPathStatsForTest)
	ResetRBACReestablishmentForTest()
	t.Cleanup(ResetRBACReestablishmentForTest)

	gvr := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "pageheaders"}
	cleanReflectorGVR(t, gvr)

	// Establish then re-establish a NON-RBAC GVR.
	classifyReflectorRequest(reflectorReqFor(t, gvr, reflectorWatchListQuery))
	classifyReflectorRequest(reflectorReqFor(t, gvr, reflectorForcedShapeQuery))

	if _, ok := RBACReestablishmentSnapshot()[gvr.String()]; ok {
		t.Fatalf("a non-RBAC GVR appears in the RBAC re-establishment snapshot — the counter is not scoped")
	}
	for k, v := range RBACReestablishmentSnapshot() {
		if v != 0 {
			t.Fatalf("a non-RBAC re-establishment leaked into RBAC bucket %s=%d — scope guard broken", k, v)
		}
	}
}

// TestIssue260_E_WatchErrorCountedPerRBACGVR — the per-RBAC-GVR watch-error RATE,
// driven through the REAL reflector watch-error handler (watchErrorHandlerFor's
// closure, exactly what SetWatchErrorHandler installs), not the FireWatchError
// shortcut (which only flips watchBroken and never runs the handler body).
func TestIssue260_E_WatchErrorCountedPerRBACGVR(t *testing.T) {
	ResetRBACWatchErrorForTest()
	t.Cleanup(ResetRBACWatchErrorForTest)

	rw := &ResourceWatcher{}
	gvr := RBACResourceTypes[0]
	handler := rw.watchErrorHandlerFor(gvr)

	// EVERY ListAndWatch error counts (above the one-shot WARN): two errors → 2.
	handler(nil, errors.New("watch dropped"))
	handler(nil, errors.New("watch dropped again"))
	if got := RBACWatchErrorSnapshot()[gvr.String()]; got != 2 {
		t.Fatalf("RED #260: RBAC watch-error counter = %d, want 2 — every reflector error must count "+
			"(the rate), not just the first (the one-shot WARN)", got)
	}

	// Scope: a non-RBAC GVR's handler never touches the RBAC counter set.
	nonRBAC := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "pageheaders"}
	rw.watchErrorHandlerFor(nonRBAC)(nil, errors.New("watch dropped"))
	if _, ok := RBACWatchErrorSnapshot()[nonRBAC.String()]; ok {
		t.Fatalf("a non-RBAC GVR appears in the RBAC watch-error snapshot — the counter is not scoped")
	}
	for k, v := range RBACWatchErrorSnapshot() {
		if k != gvr.String() && v != 0 {
			t.Fatalf("a non-RBAC watch error leaked into RBAC bucket %s=%d", k, v)
		}
	}
}
