package cache

// store_swap_review928_arms_test.go — the arms the #928 review asked for
// (issuecomment-6500063618): F1 (a candidate's watch state at the install),
// F3 (the rules no arm pinned: RX6, RX9, RX11, RX16, RX25, RX28) and F5 (the
// displaced record's bound, the schema refusal's own reason). Every
// interleaving is fixed by a hook or an event, never by a sleep.

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ── K17 — F1: the install judges the new copy's watch state NOW ──────────────

// TestStoreSwap_K17_InstalledBrokenCopyIsAnExitFromServable: the candidate's
// watch breaks AFTER its sync and before its install, and its position does
// not move. The install must copy that state (the GVR is not servable after
// the swap) and, because that is an exit from servable, move the epoch (owner
// ruling 1). A confirm pass with no position advance keeps it broken (the
// install wrote the new copy's own position as the conjunct-3 baseline); a
// watch event that advances the position heals it, by the live copy's rule.
// RED under F1b (never copy the state, RX5), F1c (no epoch bump at the
// install), F1d (no position at the error) and RX6 (no baseline at the
// install). RV-B (store_swap_review928_test.go) is its twin: an error the
// copy has since listed past is NOT copied (F1a).
func TestStoreSwap_K17_InstalledBrokenCopyIsAnExitFromServable(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 2))
	f.confirm()
	epoch0 := f.rw.ServabilityEpoch(gvr)
	var cand atomic.Pointer[storeInstance]
	var once sync.Once
	setSwapStepHook(t, func(c *storeInstance, step string) {
		if c.gvr != gvr || step != "before_install" {
			return
		}
		once.Do(func() {
			cand.Store(c)
			// The candidate's real watch-error handler, as its reflector
			// would call it: the watch dropped after the sync.
			f.rw.watchErrorHandlerForInstance(gvr, c)(nil, apierrors.NewInternalError(errors.New("watch dropped")))
		})
	})
	f.clusterSet(swapObj(gvr, "o01", "2", "v2"))
	f.request(reasonDivergence, swapKey("o01"))
	f.awaitSwaps(1)
	if f.rw.IsServable(gvr) {
		t.Fatal("K17 RED (F1b): the install dropped the new copy's broken watch — the GVR serves a copy whose watch is down")
	}
	if got := f.rw.ServabilityEpoch(gvr); got != epoch0+1 {
		t.Fatalf("K17 RED (F1c): the epoch is %d after an install that left the GVR unservable, want %d (ruling 1: every exit from servable moves it)", got, epoch0+1)
	}
	// No advance: a confirm pass must not heal it (the baseline is the new
	// copy's position, written at the install).
	f.confirm()
	if f.rw.IsServable(gvr) {
		t.Fatal("K17 RED (RX6): a confirm pass healed the watch with no position advance — the install did not write the new copy's position as the conjunct-3 baseline")
	}
	// A watch event on the new copy advances its position: healed.
	o := swapObj(gvr, "o00", "9", "moved")
	f.clusterSet(o)
	f.watcher(1).Modify(o)
	f.waitFor("the new copy's position to advance", func() bool {
		return cand.Load().gi.Informer().LastSyncResourceVersion() == "9"
	})
	f.confirm()
	if !f.rw.IsServable(gvr) {
		t.Fatal("K17: the watch never healed after its position advanced")
	}
}

// ── K18 — RX11: the comparator never judges a key the swap changed ───────────

// TestStoreSwap_K18_TheComparatorNeverJudgesAKeyTheSwapChanged: between the
// moment the comparator's record becomes the new copy's `seen` and the moment
// the swap delivery of a changed key reaches the dep handler, an anchored
// cell's slot for it is the OLD object while the indexer and the record both
// hold the NEW one. Two consecutive comparator passes in that window (the
// two-pass rule's confirmation) must judge nothing: the install made the key
// pending in every cell first (markChangedPending). RED under RX11.
func TestStoreSwap_K18_TheComparatorNeverJudgesAKeyTheSwapChanged(t *testing.T) {
	gvr := swapGVR(t)
	f := newSwapFix(t, gvr, objs(gvr, 3))
	c := f.anchorCell()
	var diverged atomic.Int64
	var once sync.Once
	ran := make(chan struct{})
	setSwapStepHook(t, func(in *storeInstance, step string) {
		if in.gvr != gvr || step != "before_unmute" {
			return
		}
		once.Do(func() {
			diverged.Add(int64(c.ComparePass()))
			diverged.Add(int64(c.ComparePass()))
			close(ran)
		})
	})
	f.clusterSet(swapObj(gvr, "o00", "2", "changed"))
	f.request(reasonDivergence, swapKey("o00"))
	f.awaitSwaps(1)
	<-ran
	if n := diverged.Load(); n != 0 {
		t.Fatalf("K18 RED (RX11): two comparator passes before the swap delivery found %d divergence(s) — a key the swap changed was judged", n)
	}
	if !c.Anchored() {
		t.Fatalf("K18 RED (RX11): the cell unanchored: %v", ListCellStatsSnapshot().Unanchors)
	}
}

// ── K12b — RX28: no record, nothing judged ───────────────────────────────────

// TestStoreSwap_K12b_NoComparatorRecordReadsInFlight: with no comparator
// record for a GVR (no live instance), nothing can be judged: every key reads
// as in flight, present or absent in the indexer. RED under RX28.
func TestStoreSwap_K12b_NoComparatorRecordReadsInFlight(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "norecord870.krateo.io", Version: "v1", Resource: "panels"}
	comparatorRecord.Delete(gvr)
	k := listObjKey{ns: swapNS, name: "k"}
	if !deliveredMismatch(gvr, k, idOf([]byte(`{"v":"x"}`))) {
		t.Fatal("K12b RED (RX28): with no record a present key was judged settled")
	}
	if !deliveredMismatch(gvr, k, rawID{}) {
		t.Fatal("K12b RED (RX28): with no record an absent key was judged settled")
	}
}

// ── K16h — RX16, RX25: a dequeue that began before the open closes nothing ───

// TestStoreSwap_K16h_DequeueBeganBeforeTheOpenClosesNothing (v5.3, Q3): the
// dependent's refresh BEGINS before the swap (a live event) and is held across
// the install. It then writes — reading the new copy — and its frame is
// written. Its dequeue began before the ledger opened, so it may have read the
// pre-swap copy: the entry is NOT refreshed and the frame closes NOTHING. The
// next refresh (it began after the open) refreshes the entry and its frame
// closes it pushed; the window is exactly detection → that frame. RED under
// RX25 (a dequeue that began before the open still refreshes) and RX16 (a
// written frame closes a still-stale entry).
func TestStoreSwap_K16h_DequeueBeganBeforeTheOpenClosesNothing(t *testing.T) {
	r := newLedgerRig(t, 2)
	dep := r.cell("o00", "")
	v := r.view(dep)
	rel1 := r.holdRefresh(dep)
	o := swapObj(r.gvr, "o00", "2", "live")
	r.clusterSet(o)
	r.watcher(0).Modify(o)
	r.waitFor("the pre-swap refresh to begin", func() bool { return r.began(dep) == 1 })
	r.clk.advance(time.Second)
	r.clusterSet(swapObj(r.gvr, "o00", "3", "corrected")) // the live copy never hears rv 3
	r.request(reasonDivergence, swapKey("o00"))           // detection, at t0+1s
	r.awaitSwaps(1)
	rel2 := r.holdRefresh(dep) // the NEXT refresh of dep waits here
	rel1()
	r.waitFor("the pre-swap refresh's frame to be written", func() bool { return v.wrote(dep) == 1 })
	if n := closes(reasonDivergence, closePushed) + closes(reasonDivergence, closePushedOnArm); n != 0 {
		t.Fatalf("K16h RED (RX25/RX16): a frame from a dequeue that began before the ledger opened closed %d entr(y/ies)", n)
	}
	if windowEnded(reasonDivergence) {
		t.Fatal("K16h RED (RX25/RX16): the window ended on a frame from a pre-open dequeue")
	}
	r.waitFor("the post-open refresh to begin", func() bool { return r.began(dep) == 2 })
	r.clk.advance(2 * time.Second)
	rel2()
	r.waitFor("the post-open refresh's frame to close the entry", func() bool {
		return v.wrote(dep) == 2 && closes(reasonDivergence, closePushed) == 1 && windowEnded(reasonDivergence)
	})
	if win := storeSwapWindowSeconds(reasonDivergence, outcomeSwap); win != 2 {
		t.Fatalf("K16h: window_seconds_total{divergence,swap} = %vs, want exactly 2 (detection → the post-open frame)", win)
	}
}

// ── K16i — RX9: a viewer that arms after the swap holds the window ───────────

// TestStoreSwap_K16i_ArmAfterTheSwapHoldsTheWindow (C5a): A has a viewer at
// the swap, B has none. While A still holds the window, a viewer arms B: B's
// entry now holds it too. A is pushed at t0+3s — the window stays open for B —
// and B at t0+5s, which ends it: window_seconds_total = 5. RED under RX9 (an
// arm never makes the entry a holder: the window ends at A's push, 3 s).
func TestStoreSwap_K16i_ArmAfterTheSwapHoldsTheWindow(t *testing.T) {
	r := newLedgerRig(t, 3)
	a := r.cell("o00", "")
	b := r.cell("o01", "")
	va := r.view(a)
	relA := r.holdRefresh(a)
	relB := r.holdRefresh(b)
	r.clusterSet(swapObj(r.gvr, "o00", "2", "x"))
	r.clusterSet(swapObj(r.gvr, "o01", "2", "y"))
	r.request(reasonDivergence, swapKey("o00"), swapKey("o01")) // detection, t0
	r.awaitSwaps(1)
	if windowEnded(reasonDivergence) {
		t.Fatal("setup: the window ended at the install although A's viewer holds it")
	}
	vb := r.view(b) // B's viewer arms AFTER the swap
	r.clk.advance(3 * time.Second)
	relA()
	r.waitFor("A pushed", func() bool { return va.wrote(a) == 1 && closes(reasonDivergence, closePushed) == 1 })
	if windowEnded(reasonDivergence) {
		t.Fatal("K16i RED (RX9): the window ended at A's push although B's viewer armed while it was open")
	}
	r.clk.advance(2 * time.Second)
	relB()
	r.waitFor("B pushed and the window ended", func() bool {
		return vb.wrote(b) == 1 && windowEnded(reasonDivergence)
	})
	if win := storeSwapWindowSeconds(reasonDivergence, outcomeSwap); win != 5 {
		t.Fatalf("K16i RED (RX9): window_seconds_total{divergence,swap} = %vs, want exactly 5 (detection → B's push)", win)
	}
}

// ── F5 — the schema refusal's own reason; the displaced record's bound ───────

// TestStoreSwap_K8c_SchemaChangeOnAFactoryGVRIsNotARepair: a schema change on
// a factory-built GVR is refused like any build there, but it is counted and
// logged as what it is (store_swap_schema_unowned_total), never as a repair
// that cannot happen (store_repair_unsupported_total). RED under F5a.
func TestStoreSwap_K8c_SchemaChangeOnAFactoryGVRIsNotARepair(t *testing.T) {
	c3Setup(t)
	resetVerificationForArm(t)
	gvr := schema.GroupVersionResource{Group: "factory870s.krateo.io", Version: "v1", Resource: "panels"}
	rw, _ := silentWatchClusterFor(t, gvr, false, panelObj("demo", "a", "1", "uid-a"))
	u0, s0 := storeRepairUnsupportedTotal.Load(), StoreSwapStatsSnapshot().SchemaUnowned
	if rw.requestStoreBuild(gvr, reasonSchema, nil) {
		t.Fatal("K8c: a schema build of a factory-built GVR was accepted")
	}
	if got := storeRepairUnsupportedTotal.Load() - u0; got != 0 {
		t.Fatalf("K8c RED (F5a): a schema change counted %d store_repair_unsupported_total", got)
	}
	if got := StoreSwapStatsSnapshot().SchemaUnowned - s0; got != 1 {
		t.Fatalf("K8c RED (F5a): store_swap_schema_unowned_total moved by %d, want 1", got)
	}
}

// TestStoreSwap_K15b_SameVersionSwapRecordsNoDisplacement: a swap delivery that
// replaces the consumer's object at the SAME resourceVersion (a schema widen)
// can never be followed by that version coming back, so it records nothing in
// displaced — a 50K widen leaves no per-key residue for the instance's life. A
// swap delivery that replaced a DIFFERENT version records it (K15 counts the
// regression it may reveal). RED under F5b (record every replaced version).
func TestStoreSwap_K15b_SameVersionSwapRecordsNoDisplacement(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "adapter870.krateo.io", Version: "v1", Resource: "panels"}
	v2 := swapObj(gvr, "k", "2", "v2")
	r := newAdapterRig(t, liveActedOn(t, v2), true)
	w := r.obj("2", "widened") // same resourceVersion, different bytes
	r.write(w)
	r.nAdd(w) // muted initial list
	r.nUnmute()
	r.nSwap()
	if got := r.seq(); !eqSeq(got, []string{"update:2:widened"}) {
		t.Fatalf("setup: consumer sequence %v, want the widened object once", got)
	}
	r.a.mu.Lock()
	n := len(r.a.displaced)
	r.a.mu.Unlock()
	if n != 0 {
		t.Fatalf("K15b RED (F5b): a same-version swap delivery recorded %d displaced version(s)", n)
	}

	// A different version is recorded.
	r2 := newAdapterRig(t, liveActedOn(t, v2), true)
	v3 := r2.obj("3", "v3")
	r2.write(v3)
	r2.nAdd(v3)
	r2.nUnmute()
	r2.nSwap()
	r2.a.mu.Lock()
	got, ok := r2.a.displaced["swap/k"]
	r2.a.mu.Unlock()
	if !ok || got != "2" {
		t.Fatalf("K15b: a swap delivery that replaced rv 2 by rv 3 recorded displaced=%q (present=%v), want \"2\"", got, ok)
	}
}
