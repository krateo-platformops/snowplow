// issue1126_c2_red_probe_test.go — the E1 arm WITHOUT the C2 symbols, so it
// compiles against main (with issue1126_c2_harness_test.go) and yields a
// BEHAVIOURAL RED there: with the refresher off, a delete the informer missed
// leaves its self entry resident across a schema relist, because no DELETE is
// ever generated for it and nothing bridges the delta. On the C2 tree the
// same probe is GREEN. Kept in the tree as the plain-English statement of the
// gap; the full arm with the bridge counters is in
// issue1126_c2_relist_bridge_test.go.

package cache

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue1126_E1_RedProbe_MissedDeleteSurvivesRelistWithoutTheBridge(t *testing.T) {
	withCleanCRDDiscovery(t)
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	// Cleanups run LIFO: this one runs BEFORE ResetDepsForTest and drains
	// the relist goroutines (re-fire + bridge, on workerWG) before the
	// tracker and store the arm owns are torn down.
	t.Cleanup(resetCRDDiscoveryForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	SetProcessSARestConfig(nil)
	resetRefresherForTest()
	t.Cleanup(resetRefresherForTest)

	store := newResolvedCache(100, 1<<20, time.Hour)
	Deps().SetStore(store)
	Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {}) // dirty-marks go nowhere

	gvr := b5GVR()
	rw, dyn, faults := c2Watcher(t, gvr)
	createObj(t, rw, dyn, gvr, b5NS, "button-x", "x")
	key := "L1_button-x"
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"pre-delete"}`), Inputs: widgetInputs(gvr, b5NS, "button-x")})
	Deps().Record(key, gvr, b5NS, "button-x")

	faults.swallow("button-x")
	if err := dyn.Resource(gvr).Namespace(b5NS).Delete(context.Background(), "button-x", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := rw.probeObjectState(gvr, b5NS, "button-x"); got != objExists {
		t.Fatalf("precondition: probe=%v, the DELETE reached the indexer", got)
	}

	b5DriveRelist(t, rw)

	if !c2WaitGone(store, key, 10*time.Second) {
		t.Fatalf("RED: L1_button-x still resident after the relist — no DELETE is generated for an object " +
			"absent from the fresh LIST and nothing bridged the delta")
	}
}
