// issue1126_c3_red_probe_test.go — the C3 D2 premise WITHOUT the C3 symbols
// (1.12.6 C3 follow-up item 6: committed, like the C2 twin): a DELETE lost
// by the dep-event pipeline (worker stopped, real handler submits onto a
// shut queue) leaves the self entry resident in the PROCESS store, and
// NOTHING on origin/main ever revisits it before its TTL. On this tree the
// reconcile ticker (started through c3StartAudit — the only C3 symbol, kept
// in issue1126_c3_red_probe_wiring_test.go) finds and evicts it within one
// tick, so the same probe is GREEN.
//
// TO RE-RUN THE RED on a checkout of origin/main: copy THIS file plus a
// three-line stub `func c3StartAudit(t *testing.T) {}` into
// internal/cache/ (the wiring file does not compile there — that is the
// point: main has no audit to start) and run it; every other symbol below
// exists on main. Transcript: reports/c3-red-on-main.txt.

package cache

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestIssue1126_D2_RedProbe_LostDeleteLeavesEntryResidentWithNoRecoveryPath(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	resetDepWatchForTest()
	ResetDepsForTest()
	resetResolvedCacheForTest()
	t.Cleanup(func() { resetDepWatchForTest(); ResetDepsForTest(); resetResolvedCacheForTest() })
	Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {})

	// The PROCESS store and the PROCESS watcher, like production: whatever
	// safety net a tree has, this is what it audits.
	store := ResolvedCache()
	if store == nil {
		t.Fatalf("ResolvedCache() nil with CACHE_ENABLED=true")
	}
	Deps().SetStore(store)
	gvr := gvrFlexes()
	rw, dyn := realWatcher(t, gvr)
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })
	createObj(t, rw, dyn, gvr, "demo-system", "flex-x", "x")
	store.Put("L1_flex-x", &ResolvedEntry{RawJSON: []byte(`{"k":"x"}`), Inputs: widgetInputs(gvr, "demo-system", "flex-x")})
	Deps().Record("L1_flex-x", gvr, "demo-system", "flex-x")

	depWatchSingleton().stopWorker()                    // the pipeline drops the next event
	deleteObj(t, rw, dyn, gvr, "demo-system", "flex-x") // indexer no longer holds it
	resetDepWatchForTest()                              // a fresh, live worker — nothing feeds it
	c3StartAudit(t)                                     // no-op stub on main; the 1 s reconcile ticker here

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, alive := store.Get("L1_flex-x"); !alive {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("RED: L1_flex-x still resident 5 s after its object was deleted and the DELETE was lost — " +
		"no audit revisits a resident entry; it serves a deleted object until TTL (#187 shape)")
}
