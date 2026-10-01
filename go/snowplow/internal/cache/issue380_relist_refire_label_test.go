package cache

// issue380_relist_refire_label_test.go — #380 (a #237 regression): both
// dirty-mark fires of one relist carry the CALLER's cause label.
//
// relistGVRForRepair (crd_discovery_side_effect.go) is shared by two callers:
//
//   - the CRD schema-relist path (triggerCRDSchemaRelist) -> SCHEMA_RELIST
//   - the store-repair queue (runOneStoreRepair)          -> STORE_REPAIR
//
// It dirty-marks the GVR's dependents twice: once pre-sync, synchronously, and
// once post-sync on the refireRelistDirtyMarkAfterSync goroutine (#187). The
// pre-sync fire used the caller's function. The post-sync re-fire HARDCODED
// OnResourceTypeSchemaRelisted, so every store repair logged half its
// dirty-marks as SCHEMA_RELIST and counted them in the schema_relist #239
// bucket.
//
// WHY THE OLD TEST CAUGHT IT ONLY SOMETIMES. TestForcedPass_RepairsTheStore_
// NotJustL1 asserted "no SCHEMA_RELIST" right after storeRepairsFiredTotal
// ticked, which is BEFORE the replacement informer syncs, so the async re-fire
// usually landed after the assertion had already passed.
//
// WHAT THESE ARMS OBSERVE. The #239 attribution buckets on the test's OWN
// DepTracker, not the process-global slog stream. The relist captures the
// tracker handle on the calling goroutine (relistGVRForRepair), and c3Setup
// gives each test a fresh one, so a relist goroutine left over from another
// test writes into ITS tracker and cannot move these counts.
//
// HOW THEY WAIT. On the label-AGNOSTIC sum of type-path marks reaching 2 (one
// dependent cell x the pre-sync fire + the post-sync re-fire). The wait does
// not depend on which label the re-fire used, so on the unfixed tree it
// completes and the label assertion fails on every run rather than timing out.

import (
	"context"
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// typeMarks sums one type-path cause over both type classes.
func typeMarks(d *DepTracker, cause string) uint64 {
	return dmCount(d, dmPathType, cause, dmClassExactDep) + dmCount(d, dmPathType, cause, dmClassListDep)
}

// typeMarksAnyCause is the label-agnostic sum the arms wait on.
func typeMarksAnyCause(d *DepTracker) uint64 {
	return typeMarks(d, dmCauseSchemaRelist) + typeMarks(d, dmCauseStoreRepair) +
		typeMarks(d, dmCauseCRDDelete) + typeMarks(d, dmCauseCRDAdd)
}

// relistFixture builds the stale-store fixture with ONE resident L1 cell that
// exact-depends on repairGVR demo/panel-a, so each dirty-mark fire marks
// exactly one key. Returns the DepTracker the relist will capture.
func relistFixture(t *testing.T) (*ResourceWatcher, *DepTracker, *cacheEventLog) {
	t.Helper()
	c3Setup(t)
	resetVerificationForArm(t)
	// A fresh crdDiscovery; its cleanup (registered first, so it runs last)
	// closes stopCh and waits workerWG, so this test's re-fire goroutine is
	// gone before the next test starts.
	resetCRDDiscoveryForTest()
	t.Cleanup(resetCRDDiscoveryForTest)
	logs := captureCacheEvents(t)
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	rw, dyn := silentWatchCluster(t, panelObj("demo", "panel-a", "1", "uid-a"))
	store := ResolvedCache()
	d := Deps()
	d.SetStore(store)
	d.SetRefreshHook(func(string, schema.GroupVersionResource) {})
	c3Put(store, "L1_panel-a", repairGVR, "demo", "panel-a")

	// The cluster moves on; the silent watch delivers nothing. Gives the
	// store-repair arm a real divergence to detect.
	if _, err := dyn.Resource(repairGVR).Namespace("demo").
		Update(context.Background(), panelObj("demo", "panel-a", "2", "uid-a"), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating the fake cluster: %v", err)
	}
	attachMetaClient(t, rw, partialMeta("demo", "panel-a", "2", "uid-a"))
	if n := typeMarksAnyCause(d); n != 0 {
		t.Fatalf("precondition: %d type-path dirty-marks before any relist, want 0", n)
	}
	return rw, d, logs
}

// awaitBothFires blocks until the pre-sync fire AND the post-sync re-fire have
// dirty-marked the one dependent cell, whatever label each used.
func awaitBothFires(t *testing.T, d *DepTracker) {
	t.Helper()
	waitForVerify(t, "the pre-sync fire and the post-sync re-fire to dirty-mark the dependent", verifyBound,
		func() bool { return typeMarksAnyCause(d) >= 2 })
	if n := typeMarksAnyCause(d); n != 2 {
		t.Fatalf("type-path dirty-marks = %d, want exactly 2 (pre-sync + post-sync re-fire, one dependent)", n)
	}
}

// Arm 1 — DETERMINISTIC. The real store-repair path: forced verification
// detects the divergence, the real queue worker runs runOneStoreRepair, and both
// fires must be STORE_REPAIR. RED on the unfixed tree on every run: there the
// buckets read store_repair=1, schema_relist=1.
func TestIssue380_StoreRepairPostSyncRefire_IsStoreRepair(t *testing.T) {
	_, d, logs := relistFixture(t)

	forcedVerify(context.Background(), repairGVR)
	if storeRepairQueueDepth() != 1 {
		t.Fatalf("store_repairs_pending = %d after a confirmed divergence, want 1", storeRepairQueueDepth())
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go runStoreRepairQueue(ctx)

	awaitBothFires(t, d)

	if got := typeMarks(d, dmCauseSchemaRelist); got != 0 {
		t.Errorf("RED #380: a store repair attributed %d dirty-mark(s) to schema_relist. The post-sync "+
			"re-fire used a hardcoded SCHEMA_RELIST instead of the caller's STORE_REPAIR, the #237 "+
			"wrong-cause defect inside the shared relist mechanism", got)
	}
	if got := typeMarks(d, dmCauseStoreRepair); got != 2 {
		t.Errorf("store_repair type marks = %d, want 2 (pre-sync fire + post-sync re-fire)", got)
	}
	// The same property on the event-log surface, scoped to repairGVR so an
	// unrelated relist elsewhere in the binary cannot satisfy or trip it.
	if n := countEventTypeForGVR(logs, "SCHEMA_RELIST", repairGVR); n != 0 {
		t.Errorf("RED #380: %d cache_event.consumed type=SCHEMA_RELIST for %s from a store repair", n, repairGVR)
	}
	if n := countEventTypeForGVR(logs, "STORE_REPAIR", repairGVR); n != 2 {
		t.Errorf("cache_event.consumed type=STORE_REPAIR for %s = %d, want 2", repairGVR, n)
	}

	waitForVerify(t, "the repair queue to drain", verifyBound, func() bool {
		return storeRepairQueueDepth() == 0
	})
}

// Arm 2 — NO REGRESSION. The real schema-relist path (triggerCRDSchemaRelist,
// fed a real structural-schema change for repairGVR's CRD): both fires stay
// SCHEMA_RELIST. GREEN before and after the fix; it is RED for an over-fix that
// hardcodes STORE_REPAIR in the re-fire.
func TestIssue380_SchemaRelistPostSyncRefire_StaysSchemaRelist(t *testing.T) {
	_, d, logs := relistFixture(t)

	c := crdDiscoverySingleton()
	c.triggerCRDSchemaRelist(panelCRD(`{"type":"object"}`))
	if n := typeMarksAnyCause(d); n != 0 {
		t.Fatalf("the FIRST observation of a CRD schema relisted (%d marks); it must only record the fingerprint", n)
	}
	c.triggerCRDSchemaRelist(panelCRD(`{"type":"object","x-kubernetes-preserve-unknown-fields":true}`))
	if got := CRDDiscoveryStatsSnapshot().SchemaRelistsFired; got != 1 {
		t.Fatalf("schema_relists_fired = %d, want 1: the schema change did not relist %s", got, repairGVR)
	}

	awaitBothFires(t, d)

	if got := typeMarks(d, dmCauseSchemaRelist); got != 2 {
		t.Errorf("schema_relist type marks = %d, want 2 (pre-sync fire + post-sync re-fire)", got)
	}
	if got := typeMarks(d, dmCauseStoreRepair); got != 0 {
		t.Errorf("a CRD schema relist attributed %d dirty-mark(s) to store_repair; the #380 fix must "+
			"carry each caller's label, not swap one hardcoded label for another", got)
	}
	if n := countEventTypeForGVR(logs, "STORE_REPAIR", repairGVR); n != 0 {
		t.Errorf("%d cache_event.consumed type=STORE_REPAIR for %s from a schema relist", n, repairGVR)
	}
}

// panelCRD is repairGVR's CRD with the given openAPIV3Schema for its single
// served version.
func panelCRD(openAPIV3Schema string) *unstructured.Unstructured {
	var obj map[string]any
	if err := json.Unmarshal([]byte(`{
		"apiVersion":"apiextensions.k8s.io/v1",
		"kind":"CustomResourceDefinition",
		"metadata":{"name":"`+repairGVR.Resource+`.`+repairGVR.Group+`"},
		"spec":{
			"group":"`+repairGVR.Group+`",
			"names":{"plural":"`+repairGVR.Resource+`","kind":"Panel"},
			"versions":[{"name":"`+repairGVR.Version+`","served":true,"storage":true,`+
		`"schema":{"openAPIV3Schema":`+openAPIV3Schema+`}}]
		}
	}`), &obj); err != nil {
		panic(err)
	}
	return &unstructured.Unstructured{Object: obj}
}
