package cache

// store_swap_arms2_test.go — #870 arms on a real ResourceWatcher, continued:
// K7 (an in-flight resolve), K11 (a CRD deleted in the gap reaches
// triggerCRDDelete), K11d's mute-boundary variant, K11e (chained swaps and its
// v5.1 extensions) and K12 (the comparator's delivery record).

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
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

func setSwapWaitHook(t *testing.T, h func(c, r *storeInstance)) {
	swapWaitHookForTest.Store(&h)
	t.Cleanup(func() { swapWaitHookForTest.Store(nil) })
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

	// The swap delivery's dep event has STAMPED its buckets (the #375 bump):
	// decided by that event, not by a sleep (#928 F4).
	stamped := make(chan struct{})
	var stampOnce sync.Once
	t.Cleanup(SetDepBumpHookForTest(func(stage string) {
		if stage == "after_store" {
			stampOnce.Do(func() { close(stamped) })
		}
	}))
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	f.awaitSwaps(1)
	select {
	case <-stamped:
	case <-time.After(30 * time.Second):
		t.Fatal("K7: the swap delivery's dep event never stamped its buckets")
	}

	Deps().Record(ctx, key, gvr, swapNS, "o01")
	if !c.putIfGen(ctx, key, body375(`{}`), gen0) {
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
	// The recording consumer is ANOTHER listener: its delivery is awaited on
	// its own record, never inferred from the lifecycle handler's (#928 F4).
	f.waitFor("the CRD's delete at the recording consumer", func() bool {
		e := f.rec.forKey(crd.GetName())
		return len(e) > 0 && e[len(e)-1].kind == "delete"
	})
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
	b0 := f.live()
	release := chainHoldB0(t, f, gvr)
	t.Cleanup(release)
	evs0 := len(f.rec.all())

	f.clusterSet(swapObj(gvr, "o01", "2", "b1"))
	f.request(reasonDivergence, swapKey("o01"))
	f.waitFor("B1's install", func() bool { return StoreSwapStatsSnapshot().Swaps == 1 })
	c1 := f.live()

	// C3 — B2 must wait for EVERY retiring copy, B0 included. B0's listener is
	// held, so B0's Run cannot return: the arm releases it on the event "B2
	// waits for B0". If B2 instead completes its swap while B0 is still held,
	// it never waited for B0 (X38). Both are events, never a timing guess.
	waitsB0 := make(chan struct{})
	var wOnce sync.Once
	setSwapWaitHook(t, func(c, r *storeInstance) {
		if c.gvr == gvr && c != c1 && r == b0 {
			wOnce.Do(func() { close(waitsB0) })
		}
	})
	f.clusterSet(swapObj(gvr, "o02", "2", "b2"))
	f.request(reasonDivergence, swapKey("o02"))
	f.waitFor("B2's install", func() bool { return StoreSwapStatsSnapshot().Swaps == 2 })
	c2 := f.live()
	if c2 == c1 {
		t.Fatal("precondition: B2 is not live")
	}
	select {
	case <-waitsB0:
	case <-c2.delivered:
		t.Fatal("K11e RED (X38): B2 completed its swap while B0's listener was still delivering — B2 never waited for B0 (C3: every waiter waits for every retiring copy)")
	case <-time.After(30 * time.Second):
		t.Fatal("K11e: B2 neither waited for B0 nor completed")
	}
	release()
	f.awaitSwaps(2)
	f.waitFor("o01 and o02 at the consumer", func() bool {
		return len(f.rec.forKey(swapKey("o01"))) > 1 && len(f.rec.forKey(swapKey("o02"))) > 1
	})
	f.fence() // every listener has delivered everything queued before it
	// Exactly once each: B0's held write to o00 (delivered by B0 itself once
	// released), and the two repaired keys (through B2). Nothing else.
	counts := map[string]int{}
	for _, e := range f.rec.since(evs0) {
		if e.key != fenceKey {
			counts[e.key]++
		}
	}
	for _, k := range []string{"o00", "o01", "o02"} {
		if counts[swapKey(k)] != 1 {
			t.Fatalf("K11e RED (X33/X38): %s reached the consumer %d time(s) after the chain, want exactly once (%v)",
				k, counts[swapKey(k)], f.rec.since(evs0))
		}
	}
	if len(counts) != 3 {
		t.Fatalf("K11e RED (X33): keys that did not change reached the consumer: %v", f.rec.since(evs0))
	}
	if s := StoreSwapStatsSnapshot(); s.Superseded != 1 {
		t.Fatalf("K11e: superseded = %d, want 1", s.Superseded)
	}
	// #928 bench — B1 superseded (absorbed by B2), B2 installed (G2).
	wantGVROutcomes(t, "K11e (G2)", gvr, map[string]uint64{gvrOutcomeInstalled: 1, gvrOutcomeSuperseded: 1})
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
	var c1, c2 atomic.Pointer[storeInstance]
	gate := make(chan struct{})
	var once, once2 sync.Once
	setSwapStepHook(t, func(c *storeInstance, step string) {
		if c.gvr == gvr && step == "after_step3" && c == c1.Load() {
			once.Do(func() { <-gate })
		}
		// A write lands on B2's copy DURING its retiring wait (muted: seen
		// only). M computed after the wait holds it; one computed before the
		// wait would drop it for good (X42).
		if c.gvr == gvr && step == "before_close_retiring" && c == c2.Load() {
			once2.Do(func() {
				o := swapObj(gvr, "o03", "5", "during-wait")
				f.clusterSet(o)
				f.watcher(2).Modify(o)
				deadline := time.Now().Add(10 * time.Second)
				for time.Now().Before(deadline) {
					if v, ok, _ := c.depAdpt.seenObject(swapKey("o03")); ok && rvOf(v) == "5" {
						return
					}
					time.Sleep(time.Millisecond)
				}
			})
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
	c2.Store(f.rw.storeBuildForTest(gvr))
	f.waitFor("B2's install", func() bool { return StoreSwapStatsSnapshot().Swaps == 2 })
	close(gate) // B1 resumes on a copy B2 has retired
	f.awaitSwaps(2)
	f.waitFor("every changed key at the consumer", func() bool {
		a, b, c := f.rec.forKey(swapKey("o01")), f.rec.forKey(swapKey("o02")), f.rec.forKey(swapKey("o03"))
		return len(a) > 1 && a[len(a)-1].v == "b1" && len(b) > 1 && b[len(b)-1].v == "b2" &&
			len(c) > 1 && c[len(c)-1].v == "during-wait"
	})
	// B1 passed its step-3 check, so it keeps its own coordinates (Q6) and
	// counts them when it resumes; each coordinate is counted exactly once.
	// B1's install has completed too (its own completion event): every count
	// it makes is in.
	select {
	case <-c1.Load().delivered:
	case <-time.After(30 * time.Second):
		t.Fatal("K11e-1: B1's install never completed")
	}
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

// ── K11c-5 — a same-version write reaches the adapter on every informer kind ─

// TestStoreSwap_K11c5_SameVersionWriteIsDelivered pins the client-go behaviour
// the identity rule of §2.7 depends on: snowplow registers its handlers with
// resync period 0, so a listener is always "syncing" and a store write that
// keeps the resourceVersion is DELIVERED — every store write queues exactly one
// notification per listener. On every informer kind snowplow builds: the
// standalone dynamic informer, the shared-factory one and the metadata-only
// one. RED under X51 (a handler registered with a resync period > 0).
func TestStoreSwap_K11c5_SameVersionWriteIsDelivered(t *testing.T) {
	check := func(t *testing.T, f *swapFix, key string, o any) {
		n := len(f.rec.forKey(key))
		f.watcher(0).Modify(o.(k8sruntimeObject))
		f.waitFor("the same-version write at the consumer", func() bool { return len(f.rec.forKey(key)) > n })
	}
	t.Run("standalone_dynamic", func(t *testing.T) {
		gvr := swapGVR(t)
		f := newSwapFix(t, gvr, objs(gvr, 2))
		check(t, f, swapKey("o00"), swapObj(gvr, "o00", "1", "same-rv-new-bytes"))
		if e := f.rec.forKey(swapKey("o00")); e[len(e)-1].v != "same-rv-new-bytes" {
			t.Fatalf("K11c-5 RED (X51): %v", e)
		}
	})
	t.Run("factory_dynamic", func(t *testing.T) {
		gvr := swapGVR(t)
		f := newSwapFix(t, gvr, objs(gvr, 2), withFactoryBuilt())
		if f.live().shape.owned() {
			t.Fatal("precondition: a factory-built informer")
		}
		check(t, f, swapKey("o00"), swapObj(gvr, "o00", "1", "same-rv-new-bytes"))
	})
	t.Run("metadata_only", func(t *testing.T) {
		gvr := swapGVR(t)
		f := newSwapFixMetadata(t, gvr, 2)
		pm := &metav1.PartialObjectMetadata{
			TypeMeta:   metav1.TypeMeta{APIVersion: gvr.GroupVersion().String(), Kind: "Panel"},
			ObjectMeta: metav1.ObjectMeta{Namespace: swapNS, Name: "m0", ResourceVersion: "1", UID: "u", Labels: map[string]string{"x": "y"}},
		}
		check(t, f, swapKey("m0"), pm)
	})
}

type k8sruntimeObject = k8sruntime.Object

// TestStoreSwap_TransformChainIsIdempotent — on client-go v0.36.5 a watch-list
// transforms each object TWICE. The whole chain a copy installs (delivery log,
// strip, reuse) must give the same store object the second time: same bytes,
// and, when the live copy holds an equal object, the same shared pointer.
func TestStoreSwap_TransformChainIsIdempotent(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	live := f.live()
	c := newStoreInstance(gvr, live.gi, live.shape, make(chan struct{}))
	idx := live.gi.Informer().GetIndexer()
	c.reuse.Store(&idx)
	dl := newDeliveryLog(gvr)
	chain := c.reuseTransform(dl.transform(StripBulkyFieldsForResourceType(gvrResourceTypeString(gvr), gvr)))
	for _, in := range []*unstructured.Unstructured{
		swapObj(gvr, "o00", "1", "v1"),        // equal to the live copy's: reused
		swapObj(gvr, "o01", "5", "changed"),   // different: the copy's own
		swapObj(gvr, "brand-new", "1", "new"), // absent from the live copy
	} {
		once, err := chain(in.DeepCopy())
		if err != nil {
			t.Fatal(err)
		}
		twice, err := chain(once)
		if err != nil {
			t.Fatal(err)
		}
		b1, _ := itemBytes(once)
		b2, _ := itemBytes(twice)
		if string(b1) != string(b2) {
			t.Fatalf("transform chain is not idempotent for %s:\n once:  %s\n twice: %s", in.GetName(), b1, b2)
		}
		if in.GetName() == "o00" {
			liveObj, _, _ := idx.GetByKey(swapKey("o00"))
			if once != liveObj || twice != liveObj {
				t.Fatalf("reuse is not idempotent: the second pass did not hand back the live object")
			}
		}
	}
}

// ── K9 — no barrier on the live copy's state ────────────────────────────────

// TestStoreSwap_K9_LiveCopyInAFailingWatchDoesNotBlockTheSwap — F1. The live
// copy's watch is torn and every re-establishment fails (a failing
// watch-list/re-watch attempt) when the build is constructed; nothing in the
// build waits on the live copy's reflector, so the swap completes on the new
// copy's own sync.
func TestStoreSwap_K9_LiveCopyInAFailingWatchDoesNotBlockTheSwap(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 3))
	f.watchErr.Store(true)
	f.watcher(0).Stop() // the live copy's watch ends; every retry fails
	f.clusterSet(swapObj(gvr, "o00", "2", "v2"))
	f.request(reasonDivergence, swapKey("o00"))
	f.awaitSwaps(1)
	if o, _ := f.storeObj(swapKey("o00")); rvOf(o) != "2" {
		t.Fatalf("K9: the swapped-in copy holds rv %q, want 2", rvOf(o))
	}
}
