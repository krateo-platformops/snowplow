// servable_confirm_retain_217_test.go — #217 three-state servable confirmation.
//
// DEFECT (TRACED): resourceTypeServed (servable.go:821) returns FALSE on a
// discovery ERROR, conflating "discovery UNKNOWN" with "resource ABSENT".
// applyConfirmLocked (servable.go:308, else ~:322) then RETRACTS a granted
// confirmation on typeServed==false → conjunct-4 (watcher.go:2371) fails →
// servableLocked false → dispatch falls through / evictions degrade. So a
// TRANSIENT discovery blip un-serves a healthy GVR (~10%-of-minutes eviction
// degradation).
//
// FIX: make conjunct-4 THREE-STATE (mirror groupAuthoritativelyAbsent's
// fail-open, servable.go:946-995): resourceTypeServed → (served, known);
// applyConfirmLocked RETRACTS only on a known-ABSENT successful discovery and
// RETAINS (leaves rw.confirmed intact) on UNKNOWN — counting the DETECTOR
// servable_confirm_retained_unknown_total{reason}. Fail-open, NOT fail-STUCK: a
// genuine removal (successful discovery, resource gone) STILL retracts.
//
// Two-armed + cross-discrimination (each counter moves in ITS arm, flat in the
// other — proving transient-error ≠ definite-absent, not just muting all
// retractions):
//   Arm A: N ERRORING refreshes → confirmation SURVIVES + retained_unknown>0 AND
//          retracted_total FLAT. RED on current (today it retracts).
//   Arm B: successful-ABSENT discovery (genuine removal) → retracts (IsServable
//          false) + retracted_total moves AND retained_unknown FLAT.
//   Arm C: an AUTHORITATIVE-absent discovery ERROR (NotFound 404 / Gone 410) →
//          retracts + retracted_total moves AND retained_unknown FLAT. A 404 is
//          the apiserver saying the group/version is GONE — definite-absent, not
//          UNKNOWN — so RefreshDiscovery stays the reconciling backstop for a
//          genuine removal whose CRD-DELETE event was missed. RED on the diff
//          that classed every error as UNKNOWN (freshness-audit BLOCK).
//
// The falsifier drives the REAL rw.RefreshDiscovery over a REAL erroring
// discovery double (not a hand-installed "unknown" state).

package cache_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// retainDiscovery is a ResourceTypeDiscovery double with three modes: SERVED
// (successful discovery lists the resource), ABSENT (successful discovery, empty
// list), and ERROR (ServerResourcesForGroupVersion returns err → the UNKNOWN
// state #217 must fail-open on).
type retainDiscovery struct {
	mu       sync.RWMutex
	served   bool
	err      error
	resource string
}

func (d *retainDiscovery) setServed(s bool) { d.mu.Lock(); d.served, d.err = s, nil; d.mu.Unlock() }
func (d *retainDiscovery) setErr(e error)   { d.mu.Lock(); d.err = e; d.mu.Unlock() }

func (d *retainDiscovery) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.err != nil {
		return nil, d.err
	}
	list := &metav1.APIResourceList{GroupVersion: groupVersion}
	if d.served {
		list.APIResources = []metav1.APIResource{{Name: d.resource, Namespaced: true, Kind: "X"}}
	}
	return list, nil
}

func TestS217_TransientDiscoveryError_RetainsConfirmation(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetInformerWatchStatsForTest()
	t.Cleanup(cache.ResetInformerWatchStatsForTest)

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(newTestScheme(), servableListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })

	disco := &retainDiscovery{resource: s4TestGVR.Resource}
	rw.SetDiscoveryClient(disco)

	_, syncCh := rw.EnsureResourceType(s4TestGVR)
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("informer did not sync within 5s")
	}

	// Setup: served discovery → confirm → servable.
	disco.setServed(true)
	rw.RefreshDiscovery(context.Background())
	if !rw.IsServable(s4TestGVR) {
		t.Fatalf("setup: GVR must be confirmed+servable after a served discovery refresh")
	}

	// ── Arm A — N ERRORING refreshes: confirmation SURVIVES + retained_unknown>0
	// + retracted_total FLAT. RED on current (today an error retracts). ──
	retractedBefore := cache.InformerWatchStatsSnapshot().ConfirmRetractedTotal
	disco.setErr(errors.New("the server is currently unable to handle the request"))
	for i := 0; i < 3; i++ {
		rw.RefreshDiscovery(context.Background())
	}
	if !rw.IsServable(s4TestGVR) {
		t.Fatalf("#217 RED: a TRANSIENT discovery error retracted the confirmation (IsServable→false). " +
			"Conjunct-4 must be three-state and FAIL-OPEN (retain) on UNKNOWN, not conflate error with absent.")
	}
	snap := cache.InformerWatchStatsSnapshot()
	if snap.ConfirmRetainedUnknownTotal == 0 {
		t.Fatalf("#217 RED: retained_unknown detector stayed 0 across 3 erroring refreshes — a retraction " +
			"counter that reads zero during the defect is not a detector")
	}
	if snap.ConfirmRetractedTotal != retractedBefore {
		t.Fatalf("#217 RED: retracted_total moved (%d→%d) during transient errors — errors must not retract",
			retractedBefore, snap.ConfirmRetractedTotal)
	}

	// ── Arm B (cross-discrimination) — a GENUINE removal (successful discovery,
	// resource ABSENT) STILL retracts (fail-open ≠ fail-STUCK): IsServable false
	// + retracted_total moves AND retained_unknown FLAT. ──
	pre := cache.InformerWatchStatsSnapshot()
	disco.setServed(false) // clears err → successful discovery, resource gone
	rw.RefreshDiscovery(context.Background())
	if rw.IsServable(s4TestGVR) {
		t.Fatalf("#217: a genuine removal (successful-ABSENT discovery) MUST retract — fail-open must not " +
			"become fail-stuck. IsServable stayed true.")
	}
	post := cache.InformerWatchStatsSnapshot()
	if post.ConfirmRetractedTotal <= pre.ConfirmRetractedTotal {
		t.Fatalf("#217 Arm B: a genuine removal must move retracted_total (%d→%d)",
			pre.ConfirmRetractedTotal, post.ConfirmRetractedTotal)
	}
	if post.ConfirmRetainedUnknownTotal != pre.ConfirmRetainedUnknownTotal {
		t.Fatalf("#217 CROSS-DISCRIMINATION: a genuine removal moved retained_unknown (%d→%d) — it must stay "+
			"FLAT here (that counter is the transient-error partition, not definite-absent)",
			pre.ConfirmRetainedUnknownTotal, post.ConfirmRetainedUnknownTotal)
	}

	// ── Arm C (freshness-audit BLOCK) — an AUTHORITATIVE-absent discovery ERROR
	// (NotFound 404 / Gone 410) MUST retract, not fail-open. A 404 is the
	// apiserver stating the group/version is gone — definite-absent, the
	// discovery-level twin of Arm B's successful-absent list. Fail-open here
	// would leave RefreshDiscovery unable to reconcile a genuine whole-GV removal
	// whose CRD-DELETE watch event was MISSED → unbounded stale-serve (fail
	// STUCK). Re-confirm, then a real NotFound → retract + retracted_total moves
	// + retained_unknown FLAT. RED on the pre-BLOCK diff (NotFound → UNKNOWN →
	// retained). ──
	disco.setServed(true)
	rw.RefreshDiscovery(context.Background())
	if !rw.IsServable(s4TestGVR) {
		t.Fatalf("Arm C setup: GVR must re-confirm after a served refresh")
	}
	preC := cache.InformerWatchStatsSnapshot()
	disco.setErr(apierrors.NewNotFound(schema.GroupResource{Group: s4TestGVR.Group, Resource: s4TestGVR.Resource}, ""))
	for i := 0; i < 3; i++ {
		rw.RefreshDiscovery(context.Background())
	}
	if rw.IsServable(s4TestGVR) {
		t.Fatalf("#217 Arm C RED: an authoritative NotFound(404) discovery error was RETAINED — a 404 is " +
			"definite-absent (the apiserver says the group/version is gone), not UNKNOWN. Retaining it leaves " +
			"RefreshDiscovery unable to reconcile a genuine removal whose DELETE event was missed (fail-STUCK).")
	}
	postC := cache.InformerWatchStatsSnapshot()
	if postC.ConfirmRetractedTotal <= preC.ConfirmRetractedTotal {
		t.Fatalf("#217 Arm C: an authoritative NotFound must move retracted_total (%d→%d)",
			preC.ConfirmRetractedTotal, postC.ConfirmRetractedTotal)
	}
	if postC.ConfirmRetainedUnknownTotal != preC.ConfirmRetainedUnknownTotal {
		t.Fatalf("#217 Arm C cross-discrimination: an authoritative NotFound moved retained_unknown (%d→%d) — "+
			"a 404 is definite-absent, it must NOT be counted as retain-on-unknown",
			preC.ConfirmRetainedUnknownTotal, postC.ConfirmRetainedUnknownTotal)
	}
}
