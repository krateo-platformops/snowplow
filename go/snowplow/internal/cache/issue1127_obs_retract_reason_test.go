// issue1127_obs_retract_reason_test.go — 1.12.7 review C-3: the {reason} label
// must be WIRED, not merely defined.
//
// WHAT THE OLD ARM GOT WRONG. It called recordConfirmRetracted(reason) directly
// and asserted the bucket moved. That asserts a map increment works. Six reason
// constants are passed at six call sites, and NOT ONE of those wirings was
// tested: swapping crd_deleted for schema_relist at the CRD-delete site left
// every arm green. A label nobody can trust is worse than no label, because an
// operator reading "schema_relist" during an outage chases the wrong thing.
//
// WHAT THIS ASSERTS. A CRD the watcher has CONFIRMED is deleted through the
// real CRD-discovery path — the informer's DeleteFunc, the discovery worker,
// triggerCRDDelete, the teardown — and the arm asserts the retraction landed in
// the crd_deleted bucket specifically. Swap the constant at that call site and
// this goes red naming both buckets.
//
// The confirmation itself is installed directly: what is under test is the
// attribution of the RETRACTION, and driving discovery to grant a confirmation
// first would test conjunct 4, which has its own arms.
package cache

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue1127Obs_ConfirmRetracted_ReasonIsWiredAtTheCRDDeleteSite(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv(envCompositionStreamingList, "true")
	withCleanCRDDiscovery(t)
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	ResetInformerWatchStatsForTest()
	t.Cleanup(ResetInformerWatchStatsForTest)
	SetProcessSARestConfig(nil)

	const group, plural, version = "composition.krateo.io", "gizmos", "v1-9-0"
	gvr := schema.GroupVersionResource{Group: group, Version: version, Resource: plural}

	crdGVR := CRDGVRForTest()
	rw := newRouteRaceWatcher(t, true, crdGVR, gvr)
	t.Cleanup(func() { rw.Stop() })
	crdSync := make(chan struct{})
	close(crdSync)
	rw.mu.Lock()
	rw.syncCh[crdGVR] = crdSync
	rw.mu.Unlock()
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })

	rw.EnsureResourceType(gvr)
	if !rw.IsRegistered(gvr) {
		t.Fatalf("premise: %s is not registered", gvr)
	}
	// The GVR is CONFIRMED servable — the state a retraction acts on. Without
	// this the teardown retracts nothing and the arm would pass vacuously.
	rw.mu.Lock()
	rw.ensureConfirmMapsLocked()
	rw.confirmed[gvr] = struct{}{}
	rw.mu.Unlock()

	handlers := rw.depEventHandlers(crdGVR)
	crd := crdBytesObjWithSchema(t, plural+"."+group, group, plural, version, narrowButtonSchema, "")
	handlers.AddFunc(crd)
	if !WaitCRDDiscoveryProcessedForTest(1, 2000) {
		t.Fatalf("worker did not process the CRD ADD: %s", crdDiscoveryStatsString())
	}

	before := InformerWatchStatsSnapshot().ConfirmRetractedTotal

	// THE REAL DELETE PATH.
	handlers.DeleteFunc(crd)
	if !WaitCRDDiscoveryProcessedForTest(2, 2000) {
		t.Fatalf("worker did not process the CRD DELETE: %s", crdDiscoveryStatsString())
	}

	if rw.IsRegistered(gvr) {
		t.Fatalf("premise: %s is still registered after its CRD was deleted — the teardown this arm "+
			"observes did not run", gvr)
	}
	if got := InformerWatchStatsSnapshot().ConfirmRetractedTotal - before; got != 1 {
		t.Fatalf("RED (C-3): deleting the CRD of a CONFIRMED GVR moved confirm_retracted_total by "+
			"%d, want 1. The GVR silently stopped serving from its informer and nothing counted the "+
			"retraction — the shape that made #217 take a day", got)
	}

	by := ConfirmRetractedByReasonSnapshot()
	if by[confirmRetractCRDDeleted] != 1 {
		t.Fatalf("RED (C-3): the retraction from a CRD DELETE was not attributed to %q. Buckets: %v. "+
			"The reason constants are passed at six call sites and this is the one that says 'the CRD "+
			"itself is gone'; mis-wiring it sends an operator after a schema relist that never "+
			"happened", confirmRetractCRDDeleted, by)
	}
	for _, wrong := range []string{
		confirmRetractSchemaRelist, confirmRetractStaleVersionPruned,
		confirmRetractDiscoveryRefresh, confirmRetractScopedConfirm,
		confirmRetractWalkConfirm, confirmRetractUnspecified,
	} {
		if by[wrong] != 0 {
			t.Fatalf("RED (C-3): a CRD delete also landed in the %q bucket (%v) — the labels are not "+
				"discriminating, so no bucket can be trusted", wrong, by)
		}
	}
}
