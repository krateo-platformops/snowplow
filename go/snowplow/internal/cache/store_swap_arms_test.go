package cache

// store_swap_arms_test.go — #870 arms on a real ResourceWatcher (design v5 §8
// + v5.1). Each arm names the mutation(s) that must turn it RED.
//
// The kind variants of K1/K3/K9/K10 and the acceptance shape live in
// store_swap_kind_integration_test.go (integration tag, run by CI).

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// swapDisco serves the arm's GVR, so conjunct 4 (confirmed) is real.
type swapDisco struct{ gvr schema.GroupVersionResource }

func (d swapDisco) ServerResourcesForGroupVersion(gv string) (*metav1.APIResourceList, error) {
	if gv != d.gvr.GroupVersion().String() {
		return &metav1.APIResourceList{GroupVersion: gv}, nil
	}
	return &metav1.APIResourceList{GroupVersion: gv, APIResources: []metav1.APIResource{
		{Name: d.gvr.Resource, Namespaced: true, Kind: "Panel", Verbs: metav1.Verbs{"get", "list", "watch"}},
	}}, nil
}

func objs(gvr schema.GroupVersionResource, n int) []*unstructured.Unstructured {
	out := make([]*unstructured.Unstructured, n)
	for i := range out {
		out[i] = swapObj(gvr, fmt.Sprintf("o%02d", i), "1", "v1")
	}
	return out
}

// confirm wires the discovery client and confirms the GVR (conjunct 4).
func (f *swapFix) confirm() {
	f.t.Helper()
	f.rw.SetDiscoveryClient(swapDisco{gvr: f.gvr})
	f.rw.ConfirmResourceTypes(context.Background(), []schema.GroupVersionResource{f.gvr})
	f.rw.mu.RLock()
	_, ok := f.rw.confirmed[f.gvr]
	f.rw.mu.RUnlock()
	if !ok {
		f.t.Fatal("precondition: the GVR is not confirmed")
	}
}

// ── K1 — the #870 window is gone ─────────────────────────────────────────────

// TestStoreSwap_K1_ServableThroughout: during an open build the GVR stays
// servable (confirmed, epoch unchanged), the live copy keeps delivering, and the
// only LIST is the new copy's. RED under X4 (stop the old copy before the
// install, or drop the confirmation).
func TestStoreSwap_K1_ServableThroughout(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 4))
	f.confirm()
	epoch0 := f.rw.ServabilityEpoch(gvr)
	lists0 := f.listCount()
	release := f.holdList(lists0 + 1) // the candidate's LIST

	f.clusterSet(swapObj(gvr, "o01", "2", "v2")) // a divergence: the live copy did not see it
	f.request(reasonDivergence, swapKey("o01"))
	f.waitFor("the candidate's LIST", func() bool { return f.listCount() == lists0+1 })

	// Mid-build: servable, confirmed, same epoch.
	for i := 0; i < 20; i++ {
		if !f.rw.IsServable(gvr) {
			t.Fatalf("K1 RED: the GVR left servable during the build")
		}
		time.Sleep(time.Millisecond)
	}
	if e := f.rw.ServabilityEpoch(gvr); e != epoch0 {
		t.Fatalf("K1 RED: the servability epoch moved during the build (%d → %d)", epoch0, e)
	}
	// The live copy keeps watching: a live event during the build is delivered.
	n := len(f.rec.all())
	f.clusterSet(swapObj(gvr, "o02", "3", "v3"))
	f.watcher(0).Modify(swapObj(gvr, "o02", "3", "v3"))
	f.settle(n + 1)
	if got := f.rec.forKey(swapKey("o02")); len(got) == 0 || got[len(got)-1].v != "v3" {
		t.Fatalf("K1 RED: a live event during the build did not reach the consumer (%v) — the live copy must keep watching", got)
	}

	release()
	f.awaitSwaps(1)
	if !f.rw.IsServable(gvr) {
		t.Fatal("K1 RED: not servable after the swap")
	}
	if e := f.rw.ServabilityEpoch(gvr); e != epoch0 {
		t.Fatalf("K1 RED (X3): the servability epoch moved at the swap (%d → %d)", epoch0, e)
	}
	if got := f.listCount() - lists0; got != 1 {
		t.Fatalf("K1: %d LISTs of the GVR for one repair, want 1 (the new copy's own)", got)
	}
}

// ── K2 — only what changed is marked, once ───────────────────────────────────

// TestStoreSwap_K2_ExactMarks: swap deliveries = |M|; no type-path dirty-mark
// moves; no type_event unanchor; the anchored list cell applies exactly the
// keys of M in its partition and is NOT refilled. RED under X2 (mark the whole
// type), X3 (bump the epoch at the swap) and X8 (the new copy acts before the
// unmute).
func TestStoreSwap_K2_ExactMarks(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 6))
	c := f.anchorCell()
	d := Deps()
	type0 := typeMarksAnyCause(d)
	src0 := d.dirtyMarkSubmitSourceSnapshot()[dmSourceStoreSwap]
	ls0 := ListCellStatsSnapshot()
	evs0 := len(f.rec.all())

	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.clusterSet(swapObj(gvr, "o04", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"), swapKey("o04"))
	f.awaitSwaps(1)
	f.settle(evs0 + 2)
	f.waitFor("both swap deliveries to reach the dep handler", func() bool {
		return d.dirtyMarkSubmitSourceSnapshot()[dmSourceStoreSwap]-src0 >= 2
	})
	f.fence() // every listener has delivered everything queued before it

	s := StoreSwapStatsSnapshot()
	if s.MKeys != 2 || s.Deliveries != 2 {
		t.Fatalf("K2 RED: |M|=%d, dep-handler swap deliveries=%d, want 2 and 2", s.MKeys, s.Deliveries)
	}
	if got := d.dirtyMarkSubmitSourceSnapshot()[dmSourceStoreSwap] - src0; got != 2 {
		t.Fatalf("K2 RED: %d store_swap submissions, want 2 (= |M|)", got)
	}
	if got := typeMarksAnyCause(d) - type0; got != 0 {
		t.Fatalf("K2 RED (X2): the swap moved %d type-path dirty-mark(s); only the keys of M may be marked", got)
	}
	// #928 bench — the swapped GVR is named, by its String() spelling (G1).
	wantGVROutcomes(t, "K2 (G1)", gvr, map[string]uint64{gvrOutcomeInstalled: 1})
	ls := ListCellStatsSnapshot()
	if got := ls.Unanchors["type_event"] - ls0.Unanchors["type_event"]; got != 0 {
		t.Fatalf("K2 RED: %d type_event unanchor(s) at the swap", got)
	}
	if got := ls.Unanchors["servability_epoch"] - ls0.Unanchors["servability_epoch"]; got != 0 {
		t.Fatalf("K2 RED (X3): %d servability_epoch unanchor(s) at the swap", got)
	}
	f.readCell(c) // a read drains pending
	if !c.Anchored() {
		t.Fatalf("K2 RED: the cell lost its anchor at the swap — that is a full fill (unanchors %v)", ListCellStatsSnapshot().Unanchors)
	}
	ls = ListCellStatsSnapshot()
	if got := ls.AppliedKeys - ls0.AppliedKeys; got != 2 {
		t.Fatalf("K2 RED: the cell applied %d key(s), want |M ∩ partition| = 2", got)
	}
	if got := ls.Anchors - ls0.Anchors; got != 0 {
		t.Fatalf("K2 RED: %d new anchor(s) — a full fill happened", got)
	}
	// Exactly one consumer event per key of M, none for the others.
	evs := withoutFence(f.rec.since(evs0))
	if len(evs) != 2 || keysOf(evs) != keysOf([]swapEvent{
		{kind: "update", key: swapKey("o01"), rv: "2", v: "v2", oldRV: "1"},
		{kind: "update", key: swapKey("o04"), rv: "2", v: "v2", oldRV: "1"},
	}) {
		t.Fatalf("K2 RED (X8): consumer events after the build: %v, want exactly the two updates of M", evs)
	}
}

// ── K3 — the swap repairs ────────────────────────────────────────────────────

// TestStoreSwap_K3_RepairIsReal: every divergent key equals the cluster after
// the swap, and the consumer acted on the corrected object. RED under X1
// (install without delivering M).
func TestStoreSwap_K3_RepairIsReal(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 5))
	evs0 := len(f.rec.all())
	f.clusterSet(swapObj(gvr, "o00", "7", "fixed"))
	f.clusterDelete("o03")
	f.clusterSet(swapObj(gvr, "new", "1", "added"))
	f.request(reasonDivergence, swapKey("o00"), swapKey("o03"), swapKey("new"))
	f.awaitSwaps(1)
	f.settle(evs0 + 3)
	// The store equals the cluster (the quorum read is the tracker here; the kind
	// arm reads a real quorum GET).
	for _, name := range []string{"o00", "o01", "o02", "o04", "new"} {
		want, _ := f.fake.Tracker().Get(gvr, swapNS, name)
		got, ok := f.storeObj(swapKey(name))
		if !ok || rvOf(got) != rvOf(want) || specV(got) != specV(want) {
			t.Fatalf("K3 RED: %s in the store = %v/%s, the cluster holds %s/%s", name, ok, rvOf(got), rvOf(want), specV(want))
		}
	}
	if _, ok := f.storeObj(swapKey("o03")); ok {
		t.Fatal("K3 RED: a key the cluster deleted is still in the store")
	}
	want := []swapEvent{
		{kind: "update", key: swapKey("o00"), rv: "7", v: "fixed", oldRV: "1"},
		{kind: "delete", key: swapKey("o03"), oldRV: "1"},
		{kind: "add", key: swapKey("new"), rv: "1", v: "added"},
	}
	if got := f.rec.since(evs0); keysOf(got) != keysOf(want) {
		t.Fatalf("K3 RED (X1): the consumer received %v after the swap, want %v", got, want)
	}
}

// ── K4 — the dropped tail ────────────────────────────────────────────────────

// TestStoreSwap_K4_DroppedTailIsInM: the OLD copy's consumer is held across the
// install while its watch updates and deletes objects; the notifications
// client-go drops when the old copy stops are delivered by the swap, because
// `acted` lags. RED under X6 (M from the new copy vs the old store).
func TestStoreSwap_K4_DroppedTailIsInM(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 4))
	lists0 := f.listCount()
	releaseList := f.holdList(lists0 + 1)
	evs0 := len(f.rec.all())

	// Hold the live copy's consumer on its next delivery.
	held := make(chan struct{})
	unblock := make(chan struct{})
	var once sync.Once
	f.rec.setBlock(func(e swapEvent) {
		if e.key == swapKey("o00") {
			once.Do(func() { close(held) })
			<-unblock
		}
	})
	f.request(reasonDivergence, swapKey("o00"))
	f.waitFor("the candidate's LIST", func() bool { return f.listCount() == lists0+1 })

	// Live writes: o00 (held at the consumer), then o01 updated and o02 deleted
	// behind it. The cluster follows every one, so the new copy LISTs them all.
	for _, o := range []*unstructured.Unstructured{
		swapObj(gvr, "o00", "2", "a"), swapObj(gvr, "o01", "2", "b"),
	} {
		f.clusterSet(o)
		f.watcher(0).Modify(o)
	}
	f.clusterDelete("o02")
	f.watcher(0).Delete(swapObj(gvr, "o02", "1", "v1"))
	<-held

	// The new copy syncs and installs; the old copy is stopped with its
	// listener held: o01 and o02's notifications are dropped by client-go.
	releaseList()
	f.waitFor("the install", func() bool { return StoreSwapStatsSnapshot().Swaps == 1 })
	close(unblock)
	f.rec.setBlock(nil)
	f.awaitSwaps(1)
	f.waitFor("the dropped tail to reach the consumer", func() bool {
		return len(f.rec.forKey(swapKey("o01"))) > 1 && len(f.rec.forKey(swapKey("o02"))) > 1
	})
	last := func(k string) swapEvent { e := f.rec.forKey(k); return e[len(e)-1] }
	if e := last(swapKey("o01")); e.kind != "update" || e.v != "b" {
		t.Fatalf("K4 RED (X6): the consumer's last event for o01 is %v, want its update to b", e)
	}
	if e := last(swapKey("o02")); e.kind != "delete" {
		t.Fatalf("K4 RED (X6): the consumer's last event for o02 is %v, want its delete", e)
	}
	_ = evs0
}

// ── K5 — the diff is by content ──────────────────────────────────────────────

// TestStoreSwap_K5_SchemaWidenByContent: a schema build whose new copy LISTs
// objects with the SAME resourceVersion and different bytes (a widen) serves
// the new field after the swap, and M is exactly the objects whose bytes
// changed. RED under X5 (compare by resourceVersion).
func TestStoreSwap_K5_SchemaWidenByContent(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 5))
	evs0 := len(f.rec.all())
	// The widen: o01 and o03 now carry a field the narrow schema pruned. Same rv.
	for _, n := range []string{"o01", "o03"} {
		o := swapObj(gvr, n, "1", "v1")
		o.Object["spec"].(map[string]any)["widened"] = "yes"
		f.clusterSet(o)
	}
	f.request(reasonSchema)
	f.awaitSwaps(1)
	f.settle(evs0 + 2)
	if s := StoreSwapStatsSnapshot(); s.MKeys != 2 {
		t.Fatalf("K5 RED (X5): |M| = %d, want 2 (the objects whose bytes the widen changed)", s.MKeys)
	}
	for _, n := range []string{"o01", "o03"} {
		obj, _ := f.storeObj(swapKey(n))
		b, _ := itemBytes(obj)
		if !strings.Contains(string(b), `"widened":"yes"`) {
			t.Fatalf("K5 RED: %s is still served pruned after the swap: %s", n, b)
		}
	}
	if got := f.rec.since(evs0); len(got) != 2 {
		t.Fatalf("K5 RED: %d consumer events, want 2: %v", len(got), got)
	}
}

// ── K6 — reuse ───────────────────────────────────────────────────────────────

// TestStoreSwap_K6_UnchangedObjectsAreShared: after a swap every unchanged
// key's object is POINTER-IDENTICAL to the old copy's. RED under X9 (reuse off).
func TestStoreSwap_K6_UnchangedObjectsAreShared(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 8))
	before := map[string]any{}
	for i := 0; i < 8; i++ {
		k := swapKey(fmt.Sprintf("o%02d", i))
		before[k], _ = f.storeObj(k)
	}
	f.clusterSet(swapObj(gvr, "o05", "2", "v2"))
	f.request(reasonDivergence, swapKey("o05"))
	f.awaitSwaps(1)
	for k, old := range before {
		now, _ := f.storeObj(k)
		if k == swapKey("o05") {
			if now == old {
				t.Fatal("K6: the changed key kept its old object")
			}
			continue
		}
		if now != old {
			t.Fatalf("K6 RED (X9): unchanged %s is a different object after the swap — the new copy duplicated the store", k)
		}
	}
	if got := StoreSwapStatsSnapshot().Reused; got != 7 {
		t.Fatalf("K6 RED (X9): store_swap_reused_objects_total = %d, want 7", got)
	}
}

// ── K8 — teardown during a build ─────────────────────────────────────────────

// TestStoreSwap_K8_TeardownAbortsTheBuild: a CRD delete (the GVR's teardown)
// during the build: the stopped new copy is never installed, the GVR stays
// absent, and the copy's Run returns. RED under X12 (install on the sync
// channel without the re-check) and X13 (the build slot is not purged).
func TestStoreSwap_K8_TeardownAbortsTheBuild(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 3))
	lists0 := f.listCount()
	release := f.holdList(lists0 + 1)
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	cand := f.rw.storeBuildForTest(gvr)
	if cand == nil {
		t.Fatal("precondition: no build")
	}
	f.waitFor("the candidate's LIST", func() bool { return f.listCount() == lists0+1 })

	f.rw.RemoveResourceType(gvr) // a CRD delete
	release()                    // the held LIST answers after the teardown
	select {
	case <-cand.done:
	case <-time.After(10 * time.Second):
		t.Fatal("K8 RED: the aborted copy's Run never returned")
	}
	// The copy's sync goroutine has decided (installed, or given up): the
	// checks below are decided by that event, not by a sleep.
	select {
	case <-cand.syncDone:
	case <-time.After(10 * time.Second):
		t.Fatal("K8 RED: the aborted copy's sync goroutine never returned")
	}
	if f.rw.IsRegistered(gvr) {
		t.Fatal("K8 RED (X12/X13): the GVR is registered again after its teardown — the aborted copy was installed")
	}
	if s := StoreSwapStatsSnapshot(); s.Swaps != 0 {
		t.Fatalf("K8 RED: %d swap(s) after a teardown", s.Swaps)
	}
	if got := storeSwapStats.keys.get(reasonDivergence, outcomeAbortTeardown); got != 1 {
		t.Fatalf("K8: keys_total{divergence,abort_teardown} = %d, want 1", got)
	}
	if !f.rw.everyPerGVRMapClearForTest(gvr) {
		t.Fatal("K8 RED (X13): per-GVR state survives the teardown")
	}
	wantGVROutcomes(t, "K8 (G4)", gvr, map[string]uint64{outcomeAbortTeardown: 1})
}

// ── K8b — owned only ─────────────────────────────────────────────────────────

// TestStoreSwap_K8b_OwnedOnly: a build of a factory-built GVR is refused; its
// live informer is the same instance and still running. RED under X28.
func TestStoreSwap_K8b_OwnedOnly(t *testing.T) {
	c3Setup(t)
	resetVerificationForArm(t)
	gvr := schema.GroupVersionResource{Group: "factory870.krateo.io", Version: "v1", Resource: "panels"}
	rw, dyn := silentWatchClusterFor(t, gvr, false, panelObj("demo", "a", "1", "uid-a"))
	before := rw.liveInstance(gvr)
	if before == nil || before.shape.owned() {
		t.Fatal("precondition: a factory-built live instance")
	}
	if rw.requestStoreBuild(gvr, reasonDivergence, []string{"demo/a"}) {
		t.Fatal("K8b RED (X28): a build of a factory-built GVR was accepted")
	}
	if got := storeRepairUnsupportedTotal.Load(); got != 1 {
		t.Fatalf("K8b: store_repair_unsupported_total = %d, want 1", got)
	}
	if rw.storeBuildForTest(gvr) != nil {
		t.Fatal("K8b RED (X28): a build slot was opened")
	}
	if after := rw.liveInstance(gvr); after != before || after.stopped() {
		t.Fatalf("K8b RED (X28): the live informer was replaced or stopped (same=%v stopped=%v)", after == before, after.stopped())
	}
	// Still running: a cluster update reaches the store through a relist? The
	// silent watch delivers nothing, so read that the informer was not stopped
	// and is still synced and servable instead.
	if !rw.IsServable(gvr) {
		t.Fatal("K8b RED: the factory-built GVR is no longer servable")
	}
	_ = dyn
}

// ── K10 — a stalled copy; no serialisation ───────────────────────────────────

// TestStoreSwap_K10_RefusedAbortsAndStallBlocksNothing: a terminal LIST
// refusal (403) aborts the build (abort_refused); a persistently failing LIST
// (500) leaves its window open (the gauge rising) while ANOTHER GVR's repair
// completes; no goroutine of an aborted copy outlives it. RED under X14 (no
// abort on a terminal refusal) and X15 (one-at-a-time serialisation).
func TestStoreSwap_K10_RefusedAbortsAndStallBlocksNothing(t *testing.T) {
	t.Run("forbidden_aborts", func(t *testing.T) {
		gvr := swapGVR(t)
		var refuse atomic.Bool
		// The refusal is answered only once the test holds the candidate: the
		// abort follows the refused LIST, so the build cannot end before it is
		// read (#928 round 3, F-LOW: read after the request, a fast abort left
		// it nil and <-cand.done panicked the package binary).
		captured := make(chan struct{})
		var release sync.Once
		releaseRefusal := func() { release.Do(func() { close(captured) }) }
		t.Cleanup(releaseRefusal)
		f := newSwapFix(t, gvr, objs(gvr, 2), withListErr(func(int64) error {
			if refuse.Load() {
				<-captured
				return apierrors.NewForbidden(gvr.GroupResource(), "", errors.New("rbac"))
			}
			return nil
		}))
		refuse.Store(true)
		f.clusterSet(swapObj(gvr, "o00", "2", "v2"))
		f.request(reasonDivergence, swapKey("o00"))
		cand := f.rw.storeBuildForTest(gvr)
		releaseRefusal()
		if cand == nil {
			t.Fatal("setup: the request opened no build")
		}
		f.waitFor("abort_refused", func() bool { return storeSwapStats.keys.get(reasonDivergence, outcomeAbortRefused) == 1 })
		if f.rw.storeBuildForTest(gvr) != nil {
			t.Fatal("K10 RED (X14): the build is still open after a terminal refusal")
		}
		select {
		case <-cand.done:
		case <-time.After(10 * time.Second):
			t.Fatal("K10 RED: the refused copy's Run never returned")
		}
		if !f.rw.IsServable(gvr) {
			t.Fatal("K10: the live copy stopped serving after its build was refused")
		}
		f.waitFor("the refused build's per-GVR outcome", func() bool { return gvrOutcomes(gvr)[outcomeAbortRefused] == 1 })
		wantGVROutcomes(t, "K10 (G3)", gvr, map[string]uint64{outcomeAbortRefused: 1})
	})
	t.Run("stall_blocks_no_other_gvr", func(t *testing.T) {
		gvrA := swapGVR(t)
		gvrB := schema.GroupVersionResource{Group: gvrA.Group, Version: "v1", Resource: gvrA.Resource + "b"}
		var fail atomic.Bool
		fa := newSwapFix(t, gvrA, objs(gvrA, 2), withListErr(func(int64) error {
			if fail.Load() {
				return apierrors.NewInternalError(errors.New("etcd down"))
			}
			return nil
		}), withExtraGVR(gvrB))
		clk := newC930Clock()
		t.Cleanup(SetDirtyWindowClockForTest(clk.now))
		fail.Store(true)
		fa.clusterSet(swapObj(gvrA, "o00", "2", "v2"))
		fa.request(reasonDivergence, swapKey("o00"))
		fa.waitFor("the stalled copy's failing LIST", func() bool { return fa.listCount() >= 2 })
		clk.advance(7 * time.Second)
		_, winMax, _ := storeSwapGauges()
		if winMax[reasonDivergence] != 7 {
			t.Fatalf("K10: window_open_seconds_max = %v 7 s into the stalled build, want 7", winMax[reasonDivergence])
		}
		// A second GVR on the SAME watcher repairs while A is stalled.
		fa.fake.Tracker().Create(gvrB, swapObj(gvrB, "b0", "1", "v1"), swapNS)
		_, syncB := fa.rw.EnsureResourceType(gvrB)
		<-syncB
		fa.waitFor("B servable", func() bool { return fa.rw.IsServable(gvrB) })
		fa.fake.Tracker().Update(gvrB, swapObj(gvrB, "b0", "2", "v2"), swapNS)
		if !fa.rw.requestStoreBuild(gvrB, reasonDivergence, []string{swapKey("b0")}) {
			t.Fatal("B's build was refused")
		}
		fa.waitFor("B's swap while A is stalled", func() bool { return StoreSwapStatsSnapshot().Swaps == 1 })
		if fa.rw.storeBuildForTest(gvrA) == nil {
			t.Fatal("K10: A's stalled build ended without an event")
		}
		clk.advance(3 * time.Second)
		_, winMax2, _ := storeSwapGauges()
		if winMax2[reasonDivergence] != 10 {
			t.Fatalf("K10: A's open window = %v after 10 s of stall, want 10 (rising while it stalls)", winMax2[reasonDivergence])
		}
		if st := fa.rw.ServabilityEpoch(gvrA); !fa.rw.IsServable(gvrA) {
			t.Fatalf("K10: A left servable while its build stalled (epoch %d)", st)
		}
	})
}

// ── K13 — verification across a swap ────────────────────────────────────────

// TestStoreSwap_K13_VerificationBindsToTheInstance: a forced-pass result for
// the instance a swap replaced is discarded (it neither latches nor requests);
// a re-detection during the build JOINS it (one copy, earliest detection kept);
// the breaker still latches on a truly ineffective swap (TestForcedPass_
// RepairBounds/circuit_breaker...). RED under X19 (construction touches the
// registry) and X20 (results not bound to an instance).
func TestStoreSwap_K13_VerificationBindsToTheInstance(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 3))
	old := f.live()
	lists0 := f.listCount()
	release := f.holdList(lists0 + 1)
	f.clusterSet(swapObj(gvr, "o00", "2", "v2"))
	// The pass that found the divergence marked the registry BEFORE it asked
	// for the build; constructing the build must not wipe it (X19 wipes it at
	// construction, so this state must exist before the request).
	markRepairPending(gvr, true)
	storeVerify.mu.Lock()
	storeVerify.gvrs[gvr].suppressed = false
	storeVerify.gvrs[gvr].factoryDivergentSince = time.Unix(1, 0)
	storeVerify.mu.Unlock()
	f.request(reasonDivergence, swapKey("o00"))
	cand := f.rw.storeBuildForTest(gvr)

	// Re-detection during the build: joins, constructs nothing.
	f.request(reasonDivergence, swapKey("o00"), swapKey("o01"))
	if c := f.rw.storeBuildForTest(gvr); c != cand {
		t.Fatal("K13 RED: a re-detection constructed a second copy")
	}
	if got := storeRepairsFiredTotal.Load(); got != 1 {
		t.Fatalf("K13: store_repairs_fired_total = %d after a join, want 1", got)
	}
	keysOpen, _, _ := storeSwapGauges()
	if keysOpen[reasonDivergence] != 2 {
		t.Fatalf("K13: keys_open{divergence} = %d after the join, want 2", keysOpen[reasonDivergence])
	}
	release()
	f.awaitSwaps(1)
	// X19: construction must not have touched the registry.
	row, _ := verificationRowFor(gvr)
	storeVerify.mu.Lock()
	since := storeVerify.gvrs[gvr].factoryDivergentSince
	storeVerify.mu.Unlock()
	if since.IsZero() || !row.repairedAwaitingVerify {
		t.Fatalf("K13 RED (X19): the build wiped the verification registry (row=%+v)", row)
	}
	// A result for the OLD instance arriving after the swap is discarded.
	latched, discarded := f.rw.noteVerifiedFor(gvr, old, 3, 1)
	if !discarded || latched {
		t.Fatalf("K13 RED (X20): a pass on the replaced instance was judged (latched=%v discarded=%v)", latched, discarded)
	}
	if row, _ := verificationRowFor(gvr); !row.repairedAwaitingVerify {
		t.Fatal("K13 RED (X20): the discarded result consumed the breaker's pending verdict")
	}
}

// ── K14 / K14b — concurrent reasons ──────────────────────────────────────────

// TestStoreSwap_K14_SchemaSupersedesCarryingReasons: a widen during a repair
// build leaves ONE new copy, under the new schema, carrying both reasons and
// the confirmed keys. RED under X21.
func TestStoreSwap_K14_SchemaSupersedesCarryingReasons(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 3))
	lists0 := f.listCount()
	release1 := f.holdList(lists0 + 1)
	f.clusterSet(swapObj(gvr, "o00", "2", "v2"))
	f.request(reasonDivergence, swapKey("o00"))
	first := f.rw.storeBuildForTest(gvr)
	f.waitFor("the first copy's LIST", func() bool { return f.listCount() == lists0+1 })
	f.request(reasonSchema)
	second := f.rw.storeBuildForTest(gvr)
	if second == nil || second == first {
		t.Fatal("K14 RED (X21): a schema change did not supersede the running copy")
	}
	if !first.stopped() {
		t.Fatal("K14 RED (X21): the superseded copy is still running")
	}
	release1()
	f.awaitSwaps(1)
	if f.live() != second {
		t.Fatal("K14 RED: the installed copy is not the one built under the new schema")
	}
	if s := StoreSwapStatsSnapshot(); s.Swaps != 1 {
		t.Fatalf("K14: %d swaps, want 1", s.Swaps)
	}
	if got := storeSwapStats.keys.get(reasonDivergence, outcomeSwap); got != 1 {
		t.Fatalf("K14 RED (X21): keys_total{divergence,swap} = %d, want 1 — the confirmed keys were dropped", got)
	}
	if got := storeSwapStats.builds.get(reasonSchema, outcomeSwap) + storeSwapStats.keys.get(reasonSchema, outcomeSwap); got == 0 {
		// The schema reason's coordinates are M, which here holds o00.
		t.Fatalf("K14 RED (X21): the schema reason was dropped at the supersede")
	}
}

// TestStoreSwap_K14b_SchemaSurvivesACleanPass: a clean verification pass during
// a merged build removes only `divergence`; the swap still happens. A clean pass
// on a divergence-only build aborts it (abort_clean). RED under X27.
func TestStoreSwap_K14b_SchemaSurvivesACleanPass(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 3))
	lists0 := f.listCount()
	release := f.holdList(lists0 + 1)
	f.request(reasonSchema)
	f.request(reasonDivergence, swapKey("o00"))
	f.rw.noteCleanPass(gvr, f.live())
	if f.rw.storeBuildForTest(gvr) == nil {
		t.Fatal("K14b RED (X27): a clean pass aborted a build that still has reason schema")
	}
	if got := storeSwapStats.keys.get(reasonDivergence, outcomeAbortClean); got != 1 {
		t.Fatalf("K14b: keys_total{divergence,abort_clean} = %d, want 1", got)
	}
	release()
	f.awaitSwaps(1)

	// A divergence-only build: the clean pass aborts it.
	lists1 := f.listCount()
	f.holdList(lists1 + 1)
	f.request(reasonDivergence, swapKey("o01"))
	cand := f.rw.storeBuildForTest(gvr)
	f.rw.noteCleanPass(gvr, f.live())
	if f.rw.storeBuildForTest(gvr) != nil {
		t.Fatal("K14b: a clean pass left a divergence-only build open")
	}
	select {
	case <-cand.done:
	case <-time.After(10 * time.Second):
		t.Fatal("K14b: the aborted copy's Run never returned")
	}
	wantGVROutcomes(t, "K14b (G5)", gvr, map[string]uint64{gvrOutcomeInstalled: 1, outcomeAbortClean: 1})
}

// ── K15 — a copy that syncs behind ───────────────────────────────────────────

// TestStoreSwap_K15_BehindCopyConvergesAndIsCounted: the new copy LISTs v2
// while the live copy already delivered v3; the swap pushes v2 (an older body),
// the new copy's own watch then delivers v3, the consumer converges on v3, and
// store_swap_regressed_keys counts the key. RED under X22.
func TestStoreSwap_K15_BehindCopyConvergesAndIsCounted(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	// The cluster's LIST answer is v2; the live copy gets v3 by its watch.
	f.clusterSet(swapObj(gvr, "o00", "2", "v2"))
	f.watcher(0).Modify(swapObj(gvr, "o00", "3", "v3"))
	f.waitFor("the live copy at v3", func() bool { o, _ := f.storeObj(swapKey("o00")); return rvOf(o) == "3" })
	f.request(reasonSchema)
	f.awaitSwaps(1)
	// Every listener has processed the swap delivery (it pushed v2): only
	// then does the new copy's watch deliver v3 (it came after its LIST). A v3
	// written before the swap delivery is processed would be read as S
	// itself (#928 F4: the arm decides its interleaving by an event).
	f.fence()
	f.watcher(1).Modify(swapObj(gvr, "o00", "3", "v3"))
	f.waitFor("convergence on v3", func() bool {
		e := f.rec.forKey(swapKey("o00"))
		return len(e) > 0 && e[len(e)-1].rv == "3" && e[len(e)-1].kind == "update" && e[len(e)-1].oldRV == "2"
	})
	if got := StoreSwapStatsSnapshot().RegressedKeys; got != 1 {
		t.Fatalf("K15 RED (X22): store_swap_regressed_keys = %d, want 1", got)
	}
}

// ── R1 — the epoch rule ──────────────────────────────────────────────────────

// TestStoreSwap_R1_CellEqualsAFromScratchListing: a cell anchored before the
// swap equals a from-scratch listing after it, with no unanchor. RED under X3
// (bump the epoch) and X7 (drop one key from M).
//
// Two loss sets: several lost writes (update, update, delete, create) and a
// single lost write. With several, the anchored cell re-renders from the new
// copy as soon as ANY key of M dirties it, so a dropped key of M hides behind
// the others at the cell; the per-key check below catches it at the consumer.
// With one, the dropped key is the only one and the cell itself stays stale.
func TestStoreSwap_R1_CellEqualsAFromScratchListing(t *testing.T) {
	t.Run("several_losses", func(t *testing.T) {
		r1Case(t, func(f *swapFix, gvr schema.GroupVersionResource) map[string]string {
			f.clusterSet(swapObj(gvr, "o00", "2", "x"))
			f.clusterSet(swapObj(gvr, "o03", "2", "y"))
			f.clusterDelete("o05")
			f.clusterSet(swapObj(gvr, "zz", "1", "new"))
			return map[string]string{"o00": "x", "o03": "y", "o05": "", "zz": "new"}
		})
	})
	t.Run("one_loss", func(t *testing.T) {
		r1Case(t, func(f *swapFix, gvr schema.GroupVersionResource) map[string]string {
			f.clusterSet(swapObj(gvr, "o02", "2", "only"))
			return map[string]string{"o02": "only"}
		})
	})
}

// r1Case: lose the writes lose() makes (the live copy never sees them), repair,
// and require (1) the anchored cell equals a from-scratch listing, and (2) the
// consumer's last event for every lost key carries the cluster's state ("" =
// deleted).
func r1Case(t *testing.T, lose func(f *swapFix, gvr schema.GroupVersionResource) map[string]string) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 6))
	c := f.anchorCell()
	lost := lose(f, gvr)
	var keys []string
	for k := range lost {
		keys = append(keys, swapKey(k))
	}
	sort.Strings(keys)
	f.request(reasonDivergence, keys...)
	f.awaitSwaps(1)
	f.fence() // every listener has delivered everything queued before it
	f.waitFor("the dep worker to drain", func() bool { return DepWatchStatsSnapshot().DeleteQueueDepth == 0 })
	got := f.readCell(c)
	if !c.Anchored() {
		t.Fatalf("R1 RED (X3): the cell unanchored at the swap: %v", ListCellStatsSnapshot().Unanchors)
	}
	want := f.fromScratch()
	if got != want {
		t.Fatalf("R1 RED (X7): the anchored cell differs from a from-scratch listing after the swap:\n cell: %s\n full: %s", got, want)
	}
	for name, v := range lost {
		evs := f.rec.forKey(swapKey(name))
		if len(evs) == 0 {
			t.Fatalf("R1 RED (X7): lost key %s never reached the consumer", name)
		}
		last := evs[len(evs)-1]
		if gone := v == ""; gone != (last.kind == "delete") || last.v != v {
			t.Fatalf("R1 RED (X7): the consumer's last event for lost key %s is %v, want the cluster's state (v=%q)", name, last, v)
		}
	}
}

// ── generic shapes ───────────────────────────────────────────────────────────

// TestStoreSwap_GenericShapes: K2/K6 on a stock-informer GVR (the standalone
// dynamic informer, *Unstructured) and on a metadata-only GVR
// (*PartialObjectMetadata, which no read renders as a whole item). Reuse and
// the diff work through the same bytes rule on every shape. RED under X10.
func TestStoreSwap_GenericShapes(t *testing.T) {
	t.Run("stock_unstructured", func(t *testing.T) {
		gvr := swapGVR(t)
		f := newSwapFix(t, gvr, objs(gvr, 4))
		f.genericCheck()
	})
	t.Run("metadata_only", func(t *testing.T) {
		gvr := swapGVR(t)
		f := newSwapFixMetadata(t, gvr, 4)
		f.genericCheck()
	})
}

func (f *swapFix) genericCheck() {
	t := f.t
	before := map[string]any{}
	keys := []string{}
	for _, o := range f.live().gi.Informer().GetIndexer().List() {
		k := keyOf(o)
		before[k] = o
		keys = append(keys, k)
	}
	evs0 := len(f.rec.all())
	changed := keys[0]
	ns, name, _ := strings.Cut(changed, "/")
	_ = ns
	f.clusterSet(swapObj(f.gvr, name, "9", "changed"))
	f.request(reasonDivergence, changed)
	f.awaitSwaps(1)
	f.settle(evs0 + 1)
	for k, old := range before {
		now, _ := f.storeObj(k)
		if k == changed {
			continue
		}
		if now != old {
			t.Fatalf("generic RED (X10): %s's unchanged %T was not shared after the swap", k, old)
		}
	}
	if s := StoreSwapStatsSnapshot(); s.MKeys != 1 {
		t.Fatalf("generic RED (X10): |M| = %d, want 1", s.MKeys)
	}
}

// ── never blocks ─────────────────────────────────────────────────────────────

// TestStoreSwap_NeverBlocks: (a) the reuse transform runs while rw.mu is held
// for writing; (b) a swap completes while the old dep handler is mid-ADD inside
// syncChState (it takes rw.mu.RLock); (c) a swap of the CRD GVR completes with
// its lifecycle queue full. All complete. RED under X11 (the transform takes
// rw.mu) and X29 (the retiring wait holds rw.mu).
func TestStoreSwap_NeverBlocks(t *testing.T) {
	t.Run("a_transform_under_rw_mu", func(t *testing.T) {
		gvr := swapGVR(t)
		f := newSwapFix(t, gvr, objs(gvr, 3))
		f.request(reasonSchema)
		// Hold rw.mu for writing while the candidate's LIST is transformed.
		f.rw.mu.Lock()
		done := make(chan struct{})
		go func() {
			defer close(done)
			c := f.rw.builds[gvr].inst
			deadline := time.Now().Add(10 * time.Second)
			for !c.gi.Informer().HasSynced() && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
		}()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			f.rw.mu.Unlock()
			t.Fatal("never-blocks (a) RED (X11): the candidate could not sync while rw.mu was held")
		}
		synced := f.rw.builds[gvr].inst.gi.Informer().HasSynced()
		f.rw.mu.Unlock()
		if !synced {
			t.Fatal("never-blocks (a) RED (X11): the candidate's transform waited on rw.mu")
		}
		f.awaitSwaps(1)
	})
	t.Run("b_swap_while_old_dep_handler_mid_add", func(t *testing.T) {
		gvr := swapGVR(t)
		f := newSwapFix(t, gvr, objs(gvr, 3))
		// Hold the OLD copy's dep handler inside its ADD (after syncChState's
		// RLock is released, the hook runs at the top of the ADD).
		entered := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		hook := func(g schema.GroupVersionResource) func() {
			if g == gvr {
				once.Do(func() { close(entered) })
				<-release
			}
			return func() {}
		}
		depAddDeliveryHookForTest.Store(&hook)
		t.Cleanup(func() { depAddDeliveryHookForTest.Store(nil) })
		f.clusterSet(swapObj(gvr, "added", "1", "v"))
		f.watcher(0).Add(swapObj(gvr, "added", "1", "v"))
		<-entered
		f.request(reasonSchema)
		f.waitFor("the install's step 1", func() bool { return StoreSwapStatsSnapshot().Swaps == 1 })
		// The retiring wait is waiting on the held handler, with NO lock held:
		// readers and writers of rw.mu proceed.
		lockFree := make(chan struct{})
		go func() { f.rw.mu.Lock(); f.rw.mu.Unlock(); close(lockFree) }()
		select {
		case <-lockFree:
		case <-time.After(10 * time.Second):
			close(release)
			t.Fatal("never-blocks (b) RED (X29): rw.mu is held across the retiring wait")
		}
		close(release)
		depAddDeliveryHookForTest.Store(nil)
		f.awaitSwaps(1)
	})
	t.Run("c_crd_gvr_with_lifecycle_queue_full", func(t *testing.T) {
		withCleanCRDDiscovery(t)
		gvr := CRDGVRForTest()
		f := newSwapFix(t, gvr, nil)
		saturateCRDLifecycleQueueForTest(t)
		f.request(reasonSchema)
		f.awaitSwaps(1)
	})
}

// ── per-GVR field parity ─────────────────────────────────────────────────────

// TestStoreSwap_PerGVRFieldsCoverEveryMap: every map keyed by GVR on the
// watcher is in perGVRFields (purged by a teardown, written or kept by an
// install), except the ones that deliberately outlive or precede an informer.
func TestStoreSwap_PerGVRFieldsCoverEveryMap(t *testing.T) {
	notPerInformer := map[string]string{
		"eagerSet":            "the set of GVRs eager registration covered; not per-informer state",
		"unservedVersions":    "#807: deliberately OUTLIVES the informer",
		"retainUnknownWarned": "#217 one-shot WARN episode per GVR; discovery state, not per-informer",
	}
	listed := map[string]bool{}
	for _, f := range perGVRFields {
		listed[f.name] = true
	}
	gvrT := reflect.TypeOf(schema.GroupVersionResource{})
	rt := reflect.TypeOf(ResourceWatcher{})
	for i := 0; i < rt.NumField(); i++ {
		fld := rt.Field(i)
		if fld.Type.Kind() != reflect.Map || fld.Type.Key() != gvrT {
			continue
		}
		if _, ok := notPerInformer[fld.Name]; ok {
			continue
		}
		if !listed[fld.Name] {
			t.Errorf("per-GVR map %s is not in perGVRFields: a teardown would not purge it and a swap would not write it", fld.Name)
		}
		delete(listed, fld.Name)
	}
	for name := range listed {
		t.Errorf("perGVRFields lists %s, which is not a per-GVR map of ResourceWatcher", name)
	}
}

func goroutinesNow() int { runtime.GC(); return runtime.NumGoroutine() }

var _ = goroutinesNow
