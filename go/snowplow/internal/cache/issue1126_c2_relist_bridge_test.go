// issue1126_c2_relist_bridge_test.go — 1.12.6 C2 arms (design §4.5, §10 rows
// E1 / D3 / E3, PM gate C10): the relist delta bridge.
//
// Every arm drives the REAL CRD lifecycle handlers (rw.depEventHandlers on
// the CRD meta-GVR) through the REAL triggerCRDSchemaRelist over a REAL
// watcher whose informers list, sync and index on a fake dynamic client; the
// real DepTracker; the real store; the real C1 dep-event worker. Assertions
// are on the SERVED ENTRY (store.Get), with the bridge counters as the
// discriminator between "the bridge did it" and "something else did".
//
// THE REFRESHER NEVER RUNS in these arms (no refresh func registered, no
// StartRefresher): the 1.12.5 pre-sync / post-sync re-fires still dirty-mark,
// but a dirty-mark alone cannot evict anything here. So the ONLY path that
// can remove an entry is bridge → coordinate → worker probe ABSENT → evict.
// That is what makes E1 a falsifier of the bridge rather than of the 1.12.5
// stopgap.
//
//	E1  — an object deleted while the informer MISSED the event (its
//	      tracker delete happened, the watch event was swallowed) is bridged
//	      across the relist: the fresh LIST omits it, before \ after = {it},
//	      its self entry is evicted. A LIVE sibling is untouched. Exactly ONE
//	      coordinate is synthesized.
//	      RED on main: no bridge; with the refresher stubbed off nothing can
//	      evict the entry, it survives to TTL (see issue1126_c2_red_probe_test.go
//	      — the same arm without the C2 symbols, run against main).
//	D3  — a relist with NO deletions synthesizes nothing and evicts nothing.
//	E3  — the fresh informer never syncs: the bridge times out (counted +
//	      WARN), enqueues nothing, entries stay; the 1.12.5 PRE-sync
//	      dirty-mark still fired (the stopgap the bridge does not replace).

package cache

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// c2Setup resets every singleton the relist path touches and returns a
// store wired to the tracker plus a channel of dirty-marked keys.
func c2Setup(t *testing.T) (*ResolvedCacheStore, chan string) {
	t.Helper()
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
	d := Deps()
	d.SetStore(store)
	marked := make(chan string, 256)
	d.SetRefreshHook(func(k string, _ schema.GroupVersionResource) {
		select {
		case marked <- k:
		default:
		}
	})
	return store, marked
}

func c2PutSelf(store *ResolvedCacheStore, gvr schema.GroupVersionResource, ns, name string) string {
	key := "L1_" + name
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"` + name + `"}`), Inputs: widgetInputs(gvr, ns, name)})
	Deps().Record(key, gvr, ns, name) // the self dep edge
	return key
}

// --- E1 — a delete the informer missed is bridged across the relist ---------

func TestIssue1126_E1_RelistBridgeEvictsTheObjectTheFreshListOmits(t *testing.T) {
	store, _ := c2Setup(t)
	gvr := b5GVR()
	rw, dyn, faults := c2Watcher(t, gvr)

	createObj(t, rw, dyn, gvr, b5NS, "button-x", "x")
	createObj(t, rw, dyn, gvr, b5NS, "button-y", "y")
	keyX := c2PutSelf(store, gvr, b5NS, "button-x")
	keyY := c2PutSelf(store, gvr, b5NS, "button-y")

	// THE LOST EVENT: X is deleted on the cluster but the running informer
	// never hears it (the watch event is swallowed). The indexer still holds
	// X, the tracker does not — exactly the state a fresh LIST will expose.
	faults.swallow("button-x")
	if err := dyn.Resource(gvr).Namespace(b5NS).Delete(context.Background(), "button-x", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete button-x: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if got := rw.probeObjectState(gvr, b5NS, "button-x"); got != objExists {
		t.Fatalf("precondition: the swallowed DELETE reached the indexer (probe=%v); the arm is not "+
			"modelling a lost event", got)
	}
	if _, alive := store.Get(keyX); !alive {
		t.Fatalf("precondition: L1_button-x evicted before the relist — a DELETE was delivered")
	}

	// THE RELIST: real CRD ADD (narrow schema) + UPDATE (widened) through the
	// real lifecycle handlers → triggerCRDSchemaRelist → teardown + fresh
	// informer + pre-sync/post-sync dirty-marks + (C2) the bridge.
	b5DriveRelist(t, rw)

	if !c2WaitGone(store, keyX, harnessWaitBound) {
		s := CRDDiscoveryStatsSnapshot()
		t.Fatalf("E1 RED: L1_button-x is still RESIDENT after the relist although the fresh LIST omits "+
			"button-x. No DELETE was ever generated for it (a fresh informer synthesises none), the "+
			"refresher is off so the dirty-marks cannot evict, and nothing bridged the delta. "+
			"bridge runs=%d enqueued=%d timeout=%d aborted=%d",
			s.RelistBridgeRuns, s.RelistBridgeEnqueued, s.RelistBridgeTimeout, s.RelistBridgeAborted)
	}
	if _, alive := store.Get(keyY); !alive {
		t.Fatalf("E1 over-eviction: L1_button-y (object PRESENT in the fresh LIST) was evicted")
	}
	s := CRDDiscoveryStatsSnapshot()
	if s.RelistBridgeRuns != 1 || s.RelistBridgeTimeout != 0 || s.RelistBridgeAborted != 0 {
		t.Fatalf("E1: bridge runs=%d timeout=%d aborted=%d, want 1/0/0", s.RelistBridgeRuns, s.RelistBridgeTimeout, s.RelistBridgeAborted)
	}
	if s.RelistBridgeEnqueued != 1 {
		t.Fatalf("E1: bridge synthesized %d coordinates, want exactly 1 (before \\ after = {button-x})", s.RelistBridgeEnqueued)
	}
	if got := Deps().Stats().EvictDeleteTotal; got != 1 {
		t.Fatalf("E1: evict_delete_total=%d, want 1 — the eviction must be the worker's ABSENT verdict on "+
			"the bridged coordinate, not a refresher route (self_gone=%d)", got, Deps().Stats().EvictSelfGoneTotal)
	}
	if got := DepWatchStatsSnapshot().ProbeAbsent; got == 0 {
		t.Fatalf("E1: probe_absent_total=0 — the bridged coordinate never reached the worker's probe")
	}
}

// --- D3 — a relist with nothing deleted synthesizes nothing -----------------

func TestIssue1126_D3_RelistWithoutDeletesBridgesNothing(t *testing.T) {
	store, _ := c2Setup(t)
	gvr := b5GVR()
	rw, dyn, _ := c2Watcher(t, gvr)

	createObj(t, rw, dyn, gvr, b5NS, "button-x", "x")
	createObj(t, rw, dyn, gvr, b5NS, "button-y", "y")
	keyX := c2PutSelf(store, gvr, b5NS, "button-x")
	keyY := c2PutSelf(store, gvr, b5NS, "button-y")

	b5DriveRelist(t, rw)

	// Let the bridge run: it waits for the fresh sync, then diffs.
	deadline := time.Now().Add(harnessWaitBound)
	for time.Now().Before(deadline) && CRDDiscoveryStatsSnapshot().RelistBridgeRuns == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	waitIndexer(t, rw, gvr, b5NS, "button-x", true)
	waitIndexer(t, rw, gvr, b5NS, "button-y", true)
	time.Sleep(200 * time.Millisecond) // give a wrong bridge time to do damage

	for _, k := range []string{keyX, keyY} {
		if _, alive := store.Get(k); !alive {
			t.Fatalf("D3 over-eviction: %s evicted by a relist that deleted nothing", k)
		}
	}
	s := CRDDiscoveryStatsSnapshot()
	if s.RelistBridgeRuns != 1 || s.RelistBridgeEnqueued != 0 || s.RelistBridgeTimeout != 0 {
		t.Fatalf("D3: bridge runs=%d enqueued=%d timeout=%d, want 1/0/0", s.RelistBridgeRuns, s.RelistBridgeEnqueued, s.RelistBridgeTimeout)
	}
	if got := Deps().Stats().EvictDeleteTotal; got != 0 {
		t.Fatalf("D3: evict_delete_total=%d, want 0", got)
	}
}

// --- E3 — the fresh informer never syncs: timeout counted, nothing enqueued --

func TestIssue1126_E3_RelistBridgeTimeoutIsCountedAndEnqueuesNothing(t *testing.T) {
	t.Setenv(envRelistBridgeTimeoutSeconds, "1")
	store, marked := c2Setup(t)
	gvr := b5GVR()
	rw, dyn, faults := c2Watcher(t, gvr)

	createObj(t, rw, dyn, gvr, b5NS, "button-x", "x")
	keyX := c2PutSelf(store, gvr, b5NS, "button-x")

	// Every LIST from here on blocks: the relisted informer can never sync.
	release := faults.holdLists()
	t.Cleanup(release) // registered AFTER c2Watcher's cleanup → runs BEFORE rw.Stop

	b5DriveRelist(t, rw)

	deadline := time.Now().Add(harnessWaitBound)
	for time.Now().Before(deadline) && CRDDiscoveryStatsSnapshot().RelistBridgeTimeout == 0 {
		time.Sleep(50 * time.Millisecond)
	}
	s := CRDDiscoveryStatsSnapshot()
	if s.RelistBridgeTimeout != 1 {
		t.Fatalf("E3: relist_bridge_timeout_total=%d, want 1 (runs=%d) — the bridge did not give up on an "+
			"informer that never syncs", s.RelistBridgeTimeout, s.RelistBridgeRuns)
	}
	if s.RelistBridgeEnqueued != 0 {
		t.Fatalf("E3: the timed-out bridge synthesized %d coordinates — it must enqueue NOTHING when it "+
			"could not diff", s.RelistBridgeEnqueued)
	}
	if _, alive := store.Get(keyX); !alive {
		t.Fatalf("E3: L1_button-x evicted although nothing could establish its object's state")
	}
	// The 1.12.5 stopgap the bridge does NOT replace: the pre-sync dirty-mark
	// (Deps().OnResourceTypeSchemaRelisted at the relist call site) fired.
	sawX := false
	drain := time.After(2 * time.Second)
	for !sawX {
		select {
		case k := <-marked:
			if k == keyX {
				sawX = true
			}
		case <-drain:
			t.Fatalf("E3: the pre-sync relist dirty-mark never reached L1_button-x — C2 must not have " +
				"touched the 1.12.5 stopgap (PM condition C10)")
		}
	}
	release()
}
