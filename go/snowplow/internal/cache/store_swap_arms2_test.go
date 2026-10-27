package cache

// store_swap_arms2_test.go — #870 arms on a real ResourceWatcher, continued:
// K7 (an in-flight resolve), K11 (a CRD deleted in the gap reaches
// triggerCRDDelete), K11d's mute-boundary variant, K11e (chained swaps and its
// v5.1 extensions) and K12 (the comparator's delivery record).

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func setSwapStepHook(t *testing.T, h func(c *storeInstance, step string)) {
	swapStepHookForTest.Store(&h)
	t.Cleanup(func() { swapStepHookForTest.Store(nil) })
}

// ── K7 — #375 covers the swap ───────────────────────────────────────────────

// TestStoreSwap_K7_InFlightResolveSelfRemarks: a resolve that read the OLD copy
// (its generation captured before the swap) Puts after the swap delivered M:
// its dependency moved, so the accepted Put self-remarks (#375). RED under X1
// (no M delivery: nothing moves the generation).
func TestStoreSwap_K7_InFlightResolveSelfRemarks(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 3))
	c := newResolvedCache(256, 1<<22, time.Hour)
	Deps().SetStore(c)
	var remarks atomic.Int64
	restore := SetDepGenRemarkObserverForTest(func(k, _ string) {
		if k == "k7-cell" {
			remarks.Add(1)
		}
	})
	t.Cleanup(restore)
	Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {})

	const key = "k7-cell"
	Deps().Record(context.Background(), "k7-other", gvr, swapNS, "o01") // a warm bucket
	ctx := WithL1KeyContext(context.Background(), key)
	gen0 := c.CaptureGen(key) // the resolve reads the OLD copy here

	src0 := Deps().dirtyMarkSubmitSourceSnapshot()[dmSourceStoreSwap]
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	f.awaitSwaps(1)
	f.waitFor("the swap delivery to be processed", func() bool {
		return Deps().dirtyMarkSubmitSourceSnapshot()[dmSourceStoreSwap] > src0 && DepWatchStatsSnapshot().DeleteQueueDepth == 0
	})
	time.Sleep(20 * time.Millisecond)

	Deps().Record(ctx, key, gvr, swapNS, "o01")
	if !c.PutIfGen(ctx, key, body375(`{}`), gen0) {
		t.Fatal("K7: the Put was refused outright")
	}
	if got := remarks.Load(); got != 1 {
		t.Fatalf("K7 RED (X1): the in-flight resolve's Put remarked %d time(s), want 1 — the swap's delivery "+
			"of M must move its dependency's generation", got)
	}
}

// ── K11 — a CRD deleted in the gap reaches triggerCRDDelete ─────────────────

func crdObjFor(gvr schema.GroupVersionResource, name, rv, group string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvr.GroupVersion().String(),
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": name, "resourceVersion": rv, "uid": "uid-" + name},
		"spec": map[string]any{
			"group": group,
			"names": map[string]any{"plural": "things", "kind": "Thing"},
			"versions": []any{map[string]any{"name": "v1", "served": true, "storage": true,
				"schema": map[string]any{"openAPIV3Schema": map[string]any{"type": "object"}}}},
		},
	}}
}

// TestStoreSwap_K11_CRDDeletedInTheGapReachesTriggerCRDDelete: the CRD GVR's
// own store misses a CRD delete; a repair build swaps it in; M holds the
// deleted CRD (acted present, the new copy absent) and the dep handler receives
// OnDelete(acted) — so triggerCRDDelete runs. RED under X16 (deliver M through
// submitDepEvent only: no handler-level consumer sees it) and X32.
func TestStoreSwap_K11_CRDDeletedInTheGapReachesTriggerCRDDelete(t *testing.T) {
	withCleanCRDDiscovery(t)
	gvr := CRDGVRForTest()
	crd := crdObjFor(gvr, "things.gap870.krateo.io", "1", "gap870.krateo.io")
	crd.SetNamespace("")
	f := newSwapFixCluster(t, gvr, crd)
	before := CRDDiscoveryStatsSnapshot().DeletesProcessed
	// The CRD is deleted on the cluster; the live copy's watch never says so.
	if err := f.fake.Tracker().Delete(gvr, "", crd.GetName()); err != nil {
		t.Fatal(err)
	}
	f.request(reasonDivergence, crd.GetName())
	f.awaitSwaps(1)
	f.waitFor("triggerCRDDelete", func() bool { return CRDDiscoveryStatsSnapshot().DeletesProcessed > before })
	if e := f.rec.forKey(crd.GetName()); len(e) == 0 || e[len(e)-1].kind != "delete" {
		t.Fatalf("K11 RED: the handler-level consumers did not receive the CRD's delete (%v)", e)
	}
}

// ── K11d (variant) — a write while muted, just before the unmute ────────────

// TestStoreSwap_K11d_WriteJustBeforeUnmuteIsDelivered: a write lands on the new
// copy WHILE IT IS MUTED, its notification processed muted (seen only), just
// before the unmute; M is computed after the unmute, so the key is in M and the
// consumer receives it. RED under X37 (compute M before the unmute).
func TestStoreSwap_K11d_WriteJustBeforeUnmuteIsDelivered(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 3))
	var once sync.Once
	setSwapStepHook(t, func(c *storeInstance, step string) {
		if c.gvr != gvr || step != "before_unmute" {
			return
		}
		once.Do(func() {
			o := swapObj(gvr, "o02", "5", "late")
			f.clusterSet(o)
			f.watcher(1).Modify(o)
			// Wait until the muted adapter has processed it (seen only).
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				if v, ok, _ := c.depAdpt.seenObject(swapKey("o02")); ok && rvOf(v) == "5" {
					return
				}
				time.Sleep(time.Millisecond)
			}
		})
	})
	evs0 := len(f.rec.all())
	f.request(reasonSchema)
	f.awaitSwaps(1)
	f.waitFor("the late write to reach the consumer", func() bool {
		e := f.rec.forKey(swapKey("o02"))
		return len(e) > 0 && e[len(e)-1].v == "late"
	})
	if got := f.rec.since(evs0); len(got) != 1 {
		t.Fatalf("K11d: %d consumer events, want exactly 1: %v", len(got), got)
	}
}

// ── K11e — chained swaps ─────────────────────────────────────────────────────

// chainFix holds B0's listener (the live copy's consumer) on a live write, so
// the next install waits on it.
func chainHoldB0(t *testing.T, f *swapFix, gvr schema.GroupVersionResource) (release func()) {
	held := make(chan struct{})
	unblock := make(chan struct{})
	var once sync.Once
	f.rec.setBlock(func(e swapEvent) {
		if e.key == swapKey("o00") && e.v == "held" {
			once.Do(func() { close(held) })
			<-unblock
		}
	})
	o := swapObj(gvr, "o00", "9", "held")
	f.clusterSet(o)
	f.watcher(0).Modify(o)
	<-held
	var ronce sync.Once
	return func() { ronce.Do(func() { close(unblock); f.rec.setBlock(nil) }) }
}

// TestStoreSwap_K11e_ChainedSwaps: B1 installs and waits on B0 (held); B2 then
// installs, retires B1 and waits on B0 AND B1. B1 delivers nothing; every key
// B1 would have delivered reaches the consumers through B2, exactly once, and
// each coordinate is counted once (B2 absorbs B1's accounting, Q6). RED under
// X33 (one per-instance record, no acted), X38 (a superseded swap still
// delivers, or B2 waits only for B1).
func TestStoreSwap_K11e_ChainedSwaps(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 4))
	release := chainHoldB0(t, f, gvr)
	t.Cleanup(release)
	evs0 := len(f.rec.all())

	f.clusterSet(swapObj(gvr, "o01", "2", "b1"))
	f.request(reasonDivergence, swapKey("o01"))
	f.waitFor("B1's install", func() bool { return StoreSwapStatsSnapshot().Swaps == 1 })
	c1 := f.live()

	f.clusterSet(swapObj(gvr, "o02", "2", "b2"))
	f.request(reasonDivergence, swapKey("o02"))
	f.waitFor("B2's install", func() bool { return StoreSwapStatsSnapshot().Swaps == 2 })
	if f.live() == c1 {
		t.Fatal("precondition: B2 is not live")
	}
	release()
	f.awaitSwaps(2)
	f.waitFor("o01 and o02 at the consumer", func() bool {
		return len(f.rec.forKey(swapKey("o01"))) > 1 && len(f.rec.forKey(swapKey("o02"))) > 1
	})
	time.Sleep(50 * time.Millisecond)
	for _, k := range []string{"o01", "o02"} {
		n := 0
		for _, e := range f.rec.since(evs0) {
			if e.key == swapKey(k) {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("K11e RED (X33/X38): %s reached the consumer %d time(s) after the chain, want exactly once (%v)", k, n, f.rec.since(evs0))
		}
	}
	if s := StoreSwapStatsSnapshot(); s.Superseded != 1 {
		t.Fatalf("K11e: superseded = %d, want 1", s.Superseded)
	}
	if got := storeSwapStats.keys.get(reasonDivergence, outcomeSwap); got != 2 {
		t.Fatalf("K11e (Q6): keys_total{divergence,swap} = %d, want 2 — each coordinate counted once", got)
	}
	f.waitFor("keys_open to return to 0", func() bool {
		ko, _, _ := storeSwapGauges()
		return ko[reasonDivergence] == 0
	})
}

// TestStoreSwap_K11e1_InstallBetweenStep3AndUnmute — v5.1 Q3. B2 installs
// between B1's step-3 check and its unmute: B1 delivers on a copy B2 stops;
// whatever client-go drops there reaches the consumers through B2's M. No key
// is lost; B1 (past step 3) keeps and counts its own coordinates. RED under X42
// (skip computing M after the retiring wait).
func TestStoreSwap_K11e1_InstallBetweenStep3AndUnmute(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 4))
	var c1 atomic.Pointer[storeInstance]
	gate := make(chan struct{})
	var once sync.Once
	setSwapStepHook(t, func(c *storeInstance, step string) {
		if c.gvr == gvr && step == "after_step3" && c == c1.Load() {
			once.Do(func() { <-gate })
		}
	})
	f.clusterSet(swapObj(gvr, "o01", "2", "b1"))
	f.request(reasonDivergence, swapKey("o01"))
	c1.Store(f.rw.storeBuildForTest(gvr))
	f.waitFor("B1 past step 3", func() bool {
		f.rw.mu.RLock()
		defer f.rw.mu.RUnlock()
		return c1.Load() != nil && c1.Load().passedSt3
	})
	// B2 installs while B1 is parked between its step-3 check and its unmute.
	f.clusterSet(swapObj(gvr, "o02", "2", "b2"))
	f.request(reasonDivergence, swapKey("o02"))
	f.waitFor("B2's install", func() bool { return StoreSwapStatsSnapshot().Swaps == 2 })
	go func() { time.Sleep(20 * time.Millisecond); close(gate) }()
	f.awaitSwaps(2)
	f.waitFor("both keys at the consumer", func() bool {
		a, b := f.rec.forKey(swapKey("o01")), f.rec.forKey(swapKey("o02"))
		return len(a) > 1 && a[len(a)-1].v == "b1" && len(b) > 1 && b[len(b)-1].v == "b2"
	})
	// B1 passed its step-3 check, so it keeps its own coordinates (Q6) and
	// counts them when it resumes; each coordinate is counted exactly once.
	f.waitFor("both builds' coordinates counted", func() bool {
		return storeSwapStats.keys.get(reasonDivergence, outcomeSwap) >= 2
	})
	time.Sleep(20 * time.Millisecond)
	if got := storeSwapStats.keys.get(reasonDivergence, outcomeSwap); got != 2 {
		t.Fatalf("K11e-1 (Q6): keys_total{divergence,swap} = %d, want 2", got)
	}
}

// TestStoreSwap_K11e2_StalledPredecessorStillStopped — v5.1 C3. B1's goroutine
// is held BEFORE it closes B0's stop channel; B2 closes it (every waiter closes
// every retiring copy), B0's Run returns, and B2 completes. RED under X41 (a
// waiter closes only its predecessor's stop: B2 hangs on B0).
func TestStoreSwap_K11e2_StalledPredecessorStillStopped(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 3))
	b0 := f.live()
	var c1 atomic.Pointer[storeInstance]
	gate := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	})
	setSwapStepHook(t, func(c *storeInstance, step string) {
		if c.gvr == gvr && step == "before_close_retiring" && c == c1.Load() {
			<-gate
		}
	})
	f.clusterSet(swapObj(gvr, "o01", "2", "b1"))
	f.request(reasonDivergence, swapKey("o01"))
	c1.Store(f.rw.storeBuildForTest(gvr))
	f.waitFor("B1's install", func() bool { return StoreSwapStatsSnapshot().Swaps == 1 })
	f.clusterSet(swapObj(gvr, "o02", "2", "b2"))
	f.request(reasonDivergence, swapKey("o02"))
	select {
	case <-b0.done:
	case <-time.After(15 * time.Second):
		t.Fatal("K11e-2 RED (X41): B0's Run never returned — no waiter but the stalled B1 closes its stop")
	}
	f.waitFor("B2 to complete while B1 is stalled", func() bool {
		f.rw.mu.RLock()
		defer f.rw.mu.RUnlock()
		in := f.rw.instances[gvr]
		return in != nil && in != c1.Load() && in.passedSt3
	})
	close(gate)
}

// ── K12 — the comparator's delivery record ───────────────────────────────────

// TestStoreSwap_K12_ComparatorRecordIsTheLiveSeen: a cell anchored before the
// swap; after it, the key the NEW copy decoded first (a changed key) has its
// slot corrupted; the comparator catches it in two passes. RED under X18 (the
// comparator's record holds only unmuted deliveries: the new copy's initial
// list is missing from it, every key reads as in flight forever, and the
// corruption is never caught).
func TestStoreSwap_K12_ComparatorRecordIsTheLiveSeen(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 5))
	c := f.anchorCell()
	f.clusterSet(swapObj(gvr, "o00", "2", "changed"))
	f.request(reasonDivergence, swapKey("o00"))
	f.awaitSwaps(1)
	f.waitFor("the cell to apply M", func() bool {
		return DepWatchStatsSnapshot().DeleteQueueDepth == 0 && f.readCell(c) == f.fromScratch()
	})
	if !c.CorruptSlotForTest() { // o00 is the first slot
		t.Fatal("precondition: could not corrupt a slot")
	}
	n1, n2 := c.ComparePass(), c.ComparePass()
	if n1 != 0 || n2 == 0 || c.Anchored() {
		t.Fatalf("K12 RED (X18): passes %d,%d anchored=%v after the swap; want 0, >0, false — the comparator "+
			"must judge keys by the live instance's seen", n1, n2, c.Anchored())
	}
}
