// deps_watch_test.go — Ship A (0.30.110) acceptance tests for the
// informer→DepTracker event bridge (R1 + O14).
//
// Internal package test so it can reach the unexported depWatch,
// addEventPostSync, depEventHandlers, and the per-GVR syncCh map.
//
//   AC-R1a — an ADD propagated post-sync dirty-marks LIST-scope deps.
//   AC-R1b — an ADD seen during the informer's initial replay (syncCh
//            still open) is DROPPED; addDroppedPreSync == 1.
//   AC-O14 — a nil syncCh at AddFunc time is treated as drop + one-shot
//            WARN (the gate returns false; the bridge never propagates).
//   R3     — DeleteFunc hands OnDelete to the worker goroutine.

package cache

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// withCleanDepWatch resets the dep tracker + bridge singletons so each
// bridge test starts clean. Returns the cleanup func.
func withCleanDepWatch(t *testing.T) func() {
	t.Helper()
	// Worker first, tracker second — the worker reads Deps() on its own
	// goroutine (1.12.6 C1), so the tracker must not be reset under it.
	resetDepWatchForTest()
	resetDepsForTest()
	return func() {
		resetDepWatchForTest()
		resetDepsForTest()
	}
}

// newGateWatcher builds a bare ResourceWatcher with a controllable
// per-GVR syncCh map — enough to exercise addEventPostSync without
// spinning a real informer factory.
func newGateWatcher() *ResourceWatcher {
	return &ResourceWatcher{
		syncCh: map[schema.GroupVersionResource]chan struct{}{},
	}
}

// --- AC-R1b — ADD during initial replay is dropped --------------------------

func TestACR1b_AddPreSyncDropped(t *testing.T) {
	cleanup := withCleanDepWatch(t)
	defer cleanup()

	gvr := gvrCompositions()
	rw := newGateWatcher()
	// syncCh OPEN ⇒ informer still in its initial LIST replay.
	rw.syncCh[gvr] = make(chan struct{})

	w := depWatchSingleton()
	handlers := rw.depEventHandlers(gvr)

	// Record a LIST-scope dep — if the ADD wrongly propagated, this
	// would be dirty-marked.
	const adminL1 = "L1_admin_list"
	d := Deps()
	d.RecordList(adminL1, gvr, "bench-ns-01")
	marked := 0
	d.SetRefreshHook(func(string, schema.GroupVersionResource) { marked++ })

	// Fire an ADD-equivalent through the real handler closure.
	handlers.AddFunc(unstructuredObj(gvr, "bench-ns-01", "new-obj"))

	if got := DepWatchStatsSnapshot().AddDroppedPreSync; got != 1 {
		t.Fatalf("AC-R1b: addDroppedPreSync=%d want 1", got)
	}
	if got := DepWatchStatsSnapshot().AddPropagated; got != 0 {
		t.Fatalf("AC-R1b: addPropagated=%d want 0 (pre-sync ADD must not propagate)", got)
	}
	if marked != 0 {
		t.Fatalf("AC-R1b: pre-sync ADD dirty-marked %d keys; want 0", marked)
	}
	_ = w
}

// --- AC-R1a — ADD post-sync dirty-marks LIST-scope deps ---------------------

func TestACR1a_AddPostSyncDirtyMarksListDep(t *testing.T) {
	cleanup := withCleanDepWatch(t)
	defer cleanup()

	gvr := gvrCompositions()
	// 1.12.6 C1: a REAL synced informer (post-sync ⇒ the gate passes) whose
	// indexer holds the object, so the worker's probe reads objExists and
	// dirty-marks — exactly the production ADD path.
	rw, dyn := realWatcher(t, gvr)

	const adminL1 = "L1_admin_list"
	d := Deps()
	d.RecordList(adminL1, gvr, "bench-ns-07")
	marked := make(chan string, 8)
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) { marked <- k })

	// The ADD is delivered by the REAL informer (its handler set is the same
	// depEventHandlers closure production wires): creating the object on the
	// fake cluster after sync fires a post-sync AddFunc.
	before := DepWatchStatsSnapshot()
	createObj(t, rw, dyn, gvr, "bench-ns-07", "fresh-obj", "x")

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && DepWatchStatsSnapshot().AddPropagated == before.AddPropagated {
		time.Sleep(5 * time.Millisecond)
	}
	if got := DepWatchStatsSnapshot().AddPropagated - before.AddPropagated; got != 1 {
		t.Fatalf("AC-R1a: addPropagated delta=%d want 1", got)
	}
	if got := DepWatchStatsSnapshot().AddDroppedPreSync - before.AddDroppedPreSync; got != 0 {
		t.Fatalf("AC-R1a: addDroppedPreSync delta=%d want 0 (post-sync ADD must propagate)", got)
	}
	// The dirty-mark now happens on the dep-event worker (off the informer
	// processor goroutine), so wait for it rather than read it inline.
	select {
	case k := <-marked:
		if k != adminL1 {
			t.Fatalf("AC-R1a: post-sync ADD dirty-marked %q, want %q", k, adminL1)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("AC-R1a: post-sync ADD did not dirty-mark the LIST-scope dep within 3s")
	}
}

// --- AC-O14 — nil syncCh → WARN + drop --------------------------------------

func TestACO14_NilSyncChDrops(t *testing.T) {
	cleanup := withCleanDepWatch(t)
	defer cleanup()

	gvr := gvrCompositions()
	rw := newGateWatcher()
	// Deliberately DO NOT populate rw.syncCh[gvr] — it is nil.

	handlers := rw.depEventHandlers(gvr)

	const adminL1 = "L1_admin_list"
	d := Deps()
	d.RecordList(adminL1, gvr, "bench-ns-01")
	marked := 0
	d.SetRefreshHook(func(string, schema.GroupVersionResource) { marked++ })

	// Two ADDs — the WARN must fire at most once, both must drop.
	handlers.AddFunc(unstructuredObj(gvr, "bench-ns-01", "a"))
	handlers.AddFunc(unstructuredObj(gvr, "bench-ns-01", "b"))

	s := DepWatchStatsSnapshot()
	if s.AddNilSyncCh != 2 {
		t.Fatalf("AC-O14: addNilSyncCh=%d want 2 (both ADDs saw nil syncCh)", s.AddNilSyncCh)
	}
	if s.AddPropagated != 0 {
		t.Fatalf("AC-O14: addPropagated=%d want 0 (nil syncCh is fail-safe drop)", s.AddPropagated)
	}
	if s.AddDroppedPreSync != 2 {
		t.Fatalf("AC-O14: addDroppedPreSync=%d want 2", s.AddDroppedPreSync)
	}
	if marked != 0 {
		t.Fatalf("AC-O14: nil-syncCh ADD dirty-marked %d keys; want 0", marked)
	}
}

// --- R3 — DeleteFunc dispatches OnDelete on the worker goroutine -------------

func TestR3_DeleteFuncDispatchesViaWorker(t *testing.T) {
	cleanup := withCleanDepWatch(t)
	defer cleanup()

	d := Deps()
	store := newResolvedCache(100, 1<<20, time.Hour)
	d.SetStore(store)

	gvr := gvrCompositions()
	const l1Key = "L1_self_rep"
	store.Put(l1Key, &ResolvedEntry{
		RawJSON: []byte(`{}`),
		Inputs:  inputsFor(gvr, "ns", "victim"),
	})
	d.Record(l1Key, gvr, "ns", "victim")

	// 1.12.6 C1: a REAL synced informer that never held "victim", so the
	// worker's probe reads objAbsent and evicts the self-representation.
	rw, _ := realWatcher(t, gvr)
	handlers := rw.depEventHandlers(gvr)

	// Fire DELETE through the handler — it hands off to the worker.
	handlers.DeleteFunc(unstructuredObj(gvr, "ns", "victim"))

	// The worker processes asynchronously; poll for the eviction.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := store.Get(l1Key); !ok {
			if got := d.evictDeleteTotal.Load(); got != 1 {
				t.Fatalf("R3: evictDeleteTotal=%d want 1", got)
			}
			return // evicted by the worker — pass
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("R3: DeleteFunc → worker did not evict self-representation within 2s")
}

// unstructuredObj builds a minimal *unstructured.Unstructured carrying
// the (ns, name) identity the bridge handlers extract via metaNSName.
func unstructuredObj(gvr schema.GroupVersionResource, ns, name string) interface{} {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvr.GroupVersion().String(),
		"metadata": map[string]any{
			"namespace": ns,
			"name":      name,
		},
	}}
	return u
}
