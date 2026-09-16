// issue219_f2_prune_test.go — 1.12.7 F2 (#219): when a CRD stops serving a
// version, that version's per-GVR state must be torn down.
//
// THE MEASURED SHAPE. On krateo-057 every composition CRD serves exactly one
// version and an upgrade REPLACES it (portals v1-8-24 -> v1-8-27, installers
// v0-3-206 -> v0-3-208, frontends v1-6-15 -> v1-6-18). The outgoing GVR then
// ceases to exist, its watch breaks for that reason, and nothing ever removes
// its state: both RemoveResourceType call sites iterate crdServedGVRs(u), the
// CRD's CURRENT served versions, which by construction can never name a
// version that has already been dropped. `watch_broken` rose 4 -> 40 over 24 h
// with ZERO decreases, one step per component upgrade, and every dep event for
// those kinds degraded to a dirty-mark forever.
package cache

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestIssue219_F2_ReplacedVersionIsPruned — THE falsifier. A CRD UPDATE that
// replaces its served version must leave NO per-GVR state for the outgoing
// version, and must STOP its informer rather than merely forgetting it.
//
// RED on main: the outgoing GVR stays registered for the life of the process
// and its stop channel is never closed.
func TestIssue219_F2_ReplacedVersionIsPruned(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv(envCompositionStreamingList, "true")
	withCleanCRDDiscovery(t)
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	SetProcessSARestConfig(nil)

	const group, plural = "composition.krateo.io", "portals"
	oldGVR := schema.GroupVersionResource{Group: group, Version: "v1-8-24", Resource: plural}
	newGVR := schema.GroupVersionResource{Group: group, Version: "v1-8-27", Resource: plural}
	// A registered GVR in a DIFFERENT group: the prune is scoped to the CRD's
	// own group+plural and must not touch anything else.
	bystander := schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "buttons"}

	crdGVR := CRDGVRForTest()
	rw := newRouteRaceWatcher(t, true, crdGVR, oldGVR, newGVR, bystander)
	t.Cleanup(func() { rw.Stop() })
	crdSync := make(chan struct{})
	close(crdSync)
	rw.mu.Lock()
	rw.syncCh[crdGVR] = crdSync
	rw.mu.Unlock()
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })

	rw.EnsureResourceType(oldGVR)
	rw.EnsureResourceType(bystander)
	if !rw.IsRegistered(oldGVR) || !rw.IsRegistered(bystander) {
		t.Fatalf("precondition: oldGVR registered=%v bystander registered=%v, want both true",
			rw.IsRegistered(oldGVR), rw.IsRegistered(bystander))
	}
	// Capture the outgoing version's stop channel BEFORE the prune — after
	// RemoveResourceType the map entry is deleted, so "closed" is only
	// observable through a reference taken now. Forgetting the map entry
	// while leaving the reflector goroutine running is the failure this
	// clause exists to catch.
	rw.mu.Lock()
	oldStop, hadStop := rw.informerStop[oldGVR]
	rw.mu.Unlock()
	if !hadStop {
		t.Fatalf("precondition: no per-GVR stop channel for %s after EnsureResourceType", oldGVR)
	}
	select {
	case <-oldStop:
		t.Fatalf("precondition: the outgoing GVR's stop channel is already closed")
	default:
	}

	handlers := rw.depEventHandlers(crdGVR)

	// (1) Observe the CRD serving ONLY v1-8-24 — seeds the fingerprint.
	crdOld := crdBytesObjWithSchema(t, plural+"."+group, group, plural, "v1-8-24", narrowButtonSchema, "")
	handlers.AddFunc(crdOld)
	if !WaitCRDDiscoveryProcessedForTest(1, 2000) {
		t.Fatalf("worker did not process CRD ADD: %s", crdDiscoveryStatsString())
	}

	// (2) The upgrade: the CRD now serves ONLY v1-8-27. The version name is in
	// the fingerprint projection, so this is a real structural change and the
	// relist pass runs.
	crdNew := crdBytesObjWithSchema(t, plural+"."+group, group, plural, "v1-8-27", narrowButtonSchema, "")
	handlers.UpdateFunc(crdOld, crdNew)
	if !WaitCRDDiscoveryProcessedForTest(2, 2000) {
		t.Fatalf("worker did not process CRD UPDATE: %s", crdDiscoveryStatsString())
	}

	if rw.IsRegistered(oldGVR) {
		t.Fatalf("RED (#219): %s is STILL REGISTERED after the CRD stopped serving it. Its informers, "+
			"syncCh, confirmed, watchBroken, informerStop and lastSyncRV entries persist for the life of "+
			"the process and its reflector keeps retrying a LIST/WATCH against an API version the "+
			"apiserver no longer serves; every dep event for that kind degrades to a dirty-mark forever. "+
			"counters: %s", oldGVR, crdDiscoveryStatsString())
	}
	select {
	case <-oldStop:
	default:
		t.Fatalf("RED (#219): %s was unregistered but its per-GVR stop channel is still OPEN — the map "+
			"entry was forgotten while the reflector goroutine kept running. The state must be STOPPED, "+
			"not merely dropped", oldGVR)
	}
	if s := CRDDiscoveryStatsSnapshot(); s.StaleVersionPruned != 1 {
		t.Fatalf("stale_version_pruned_total=%d, want 1 — the retirement must be countable; it is the "+
			"number that should have been moving while watch_broken climbed 4 -> 40 and never fell",
			s.StaleVersionPruned)
	}
	// The prune must not damage the live half of the same upgrade, nor reach
	// outside this CRD.
	if !rw.IsRegistered(bystander) {
		t.Fatalf("over-prune: the bystander GVR %s (different group) was torn down — the prune must be "+
			"scoped to the CRD's own group+plural", bystander)
	}
}

// TestIssue219_F2_UnchangedVersionsArePreserved — the counter-case. A CRD
// UPDATE that widens the schema WITHOUT retiring a version must prune nothing:
// the relist tears the GVR down and re-registers it, which is not a
// retirement, and counting it would make the new counter useless as an
// upgrade signal.
func TestIssue219_F2_UnchangedVersionsArePreserved(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv(envCompositionStreamingList, "true")
	withCleanCRDDiscovery(t)
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	SetProcessSARestConfig(nil)

	const group, plural = "widgets.templates.krateo.io", "buttons"
	target := schema.GroupVersionResource{Group: group, Version: "v1beta1", Resource: plural}

	crdGVR := CRDGVRForTest()
	rw := newRouteRaceWatcher(t, true, crdGVR, target)
	t.Cleanup(func() { rw.Stop() })
	crdSync := make(chan struct{})
	close(crdSync)
	rw.mu.Lock()
	rw.syncCh[crdGVR] = crdSync
	rw.mu.Unlock()
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })

	rw.EnsureResourceType(target)
	handlers := rw.depEventHandlers(crdGVR)

	narrow := crdBytesObjWithSchema(t, plural+"."+group, group, plural, "v1beta1", narrowButtonSchema, "")
	handlers.AddFunc(narrow)
	if !WaitCRDDiscoveryProcessedForTest(1, 2000) {
		t.Fatalf("worker did not process CRD ADD: %s", crdDiscoveryStatsString())
	}
	widened := crdBytesObjWithSchema(t, plural+"."+group, group, plural, "v1beta1", widenedButtonSchema, "")
	handlers.UpdateFunc(narrow, widened)
	if !WaitCRDDiscoveryProcessedForTest(2, 2000) {
		t.Fatalf("worker did not process CRD UPDATE: %s", crdDiscoveryStatsString())
	}

	s := CRDDiscoveryStatsSnapshot()
	if s.SchemaRelistsFired != 1 {
		t.Fatalf("premise: SchemaRelistsFired=%d want 1 — the widen must still relist", s.SchemaRelistsFired)
	}
	if s.StaleVersionPruned != 0 {
		t.Fatalf("stale_version_pruned_total=%d, want 0 — a widen that retires NO version must prune "+
			"nothing; a relist's own teardown+re-register is not a retirement", s.StaleVersionPruned)
	}
	if !rw.IsRegistered(target) {
		t.Fatalf("the still-served GVR %s was pruned — the prune must only remove versions the CRD "+
			"stopped serving", target)
	}
}
