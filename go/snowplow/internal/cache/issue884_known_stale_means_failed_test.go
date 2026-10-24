// issue884_known_stale_means_failed_test.go — #884 (owner ruling F5): KNOWN-STALE
// means "the last refresh FAILED", not "the data moved since".
//
// THE DEFECT (bench run 15). Under steady churn (≥ 10 edits/s) a mark lands
// during every resolve of a hot cell. A known-stale cell then never left the
// state: endKnownStaleOnPut returned when the Put's resolve began before the
// latest mark (start < s.seq), the refresher's remarked settle carried the flag
// over (settleWindow keepOpen: stale: s.stale), and every customer read started
// a demand revalidation — a full re-resolve — plus its #740 rerun.
//
// LOGIC-ONLY ARMS. Each drives the real state machine (markDirty, beginWindow,
// the store's Put methods, settleDequeue, BeginDemandRevalidation) on a real
// store and the refresher singleton, with no worker running: no timing, no
// sleeps.
//
//	F1  the issue's falsifier: a known-stale cell, then an accepted demand
//	    revalidation Put whose resolve began BEFORE a later mark → not
//	    known-stale, the window still OPEN with its original t0, the next Get
//	    counted on stale_served and NOT on known_stale_served, the revalidation
//	    settles Fresh with NO rerun although a read was absorbed into it, and
//	    the re-opened window is handed to the refresher (queued).
//	    Mutation: restore `|| start < s.seq` in endKnownStaleOnPut → RED.
//	F1b the churn shape: the resolve read the list that moved, so the accepted
//	    Put is also #375 remarked → the revalidation still settles Fresh, no
//	    rerun, never counted failed. Mutation: Fresh = accepted && !remarked → RED.
//	F2  the refresher's accepted-but-remarked settle ends the flag even when
//	    the Put itself could not (a Put with no #375 dep-gen sink: the nil-sink
//	    fail-fresh remark) → not known-stale, window open from t0.
//	    Mutation: restore `stale: s.stale` in the accepted keep-open case → RED.
//	F3  #547 guarantees about a FAILED refresh are kept: a retried error keeps
//	    the flag; a declined refresh of the re-opened window makes it
//	    known-stale again; a demand attempt whose Put was NOT accepted still
//	    earns its absorbed read's #740 rerun; a Put with no sink ends nothing.
package cache

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// k884Cell makes one resident widgets cell KNOWN-STALE (declined) through the
// real window state machine and returns its key and the window's t0.
func k884Cell(t *testing.T) (*ResolvedCacheStore, *refresher, string, time.Time) {
	t.Helper()
	c, keys := terminalFixture(t, 1)
	key := keys[0]
	r := refresherSingleton()
	t0 := time.Now().Add(-time.Second)
	depEventSeq.Add(1)
	r.markDirty(key, t0)
	bt0, open := r.beginWindow(key)
	if !open || !bt0.Equal(t0) {
		t.Fatalf("PRECONDITION: the mark opened no window (open=%v)", open)
	}
	r.settleDequeue(key, t0, true, nil) // no Put outcome: declined
	if _, reason, ks := KnownStale(key); !ks || reason != "declined" {
		t.Fatalf("PRECONDITION: the cell is not known-stale (declined) after a declined refresh (ks=%v reason=%q)", ks, reason)
	}
	return c, r, key, t0
}

func k884Stat(name string) int64 { return ResolvedCacheStatsByStat()[name] }

func TestIssue884_F1_AnAcceptedPutEndsKnownStaleEvenWhenANewerMarkLanded(t *testing.T) {
	c, r, key, t0 := k884Cell(t)
	resetKnownStaleForTest()
	t.Cleanup(resetKnownStaleForTest)
	for r.queue.Len() > 0 { // a clean queue: the re-open's enqueue is observable
		k, _ := r.queue.Get()
		r.queue.Done(k)
	}

	// A customer read claims the demand slot; a second read is absorbed into the
	// attempt in flight (it arms a #740 rerun).
	if !ClaimDemandRevalidation(key, t0) {
		t.Fatalf("PRECONDITION: the first read did not claim the demand slot")
	}
	if ClaimDemandRevalidation(key, t0) {
		t.Fatalf("PRECONDITION: the second read claimed a second slot instead of being absorbed")
	}
	dctx, settle := BeginDemandRevalidation(context.Background(), key, t0)
	rctx := WithL1KeyContext(dctx, key) // the revalidation's resolve begins here
	gen := c.CaptureGen(key)

	// The data moves AFTER the resolve began: a later mark.
	depEventSeq.Add(1)
	r.markDirty(key, time.Now())
	if s, ok := dirtyStampFor(key); !ok || s.seq <= mustStartSeq(t, rctx) {
		t.Fatalf("PRECONDITION: the later mark is not newer than the resolve's start")
	}

	if !c.PutIfGen(rctx, key, &ResolvedEntry{RawJSON: []byte(`{"body":"new"}`), Inputs: mustInputs(t, c, key)}, gen) {
		t.Fatalf("PRECONDITION: the revalidation's Put was refused")
	}

	if _, reason, ks := KnownStale(key); ks {
		t.Fatalf("F1 RED: the cell is still KNOWN-STALE (%s) after an accepted Put whose resolve a later mark "+
			"overtook — known-stale must mean the last refresh FAILED; under churn this keeps every read "+
			"re-resolving (#884)", reason)
	}
	s, open := dirtyStampFor(key)
	if !open {
		t.Fatalf("F1 RED: the window closed — the later mark's change is not in this body, so it must stay an open window")
	}
	if !s.t0.Equal(t0) {
		t.Fatalf("F1 RED: the window's t0 moved (%v -> %v): dirty→fresh must measure from the first mark", t0, s.t0)
	}
	if s.inflight || s.stale != knownStaleNone {
		t.Fatalf("F1 RED: the window is not an ordinary open one (inflight=%v stale=%v)", s.inflight, s.stale)
	}
	if r.queue.Len() == 0 {
		t.Fatalf("F1 RED: the re-opened window was not handed to the refresher — an ordinary open window must " +
			"have its refresh pending (#710 stale-then-replace)")
	}

	staleServed0, ksServed0 := k884Stat("stale_served_total"), k884Stat("known_stale_served_total")
	if _, hit := c.Get(key); !hit {
		t.Fatalf("PRECONDITION: the cell is not resident")
	}
	if d := k884Stat("stale_served_total") - staleServed0; d != 1 {
		t.Fatalf("F1 RED: the next Get moved stale_served_total by %d, want 1 (served stale while the refresher replaces it)", d)
	}
	if d := k884Stat("known_stale_served_total") - ksServed0; d != 0 {
		t.Fatalf("F1 RED: the next Get was counted as a KNOWN-STALE serve (+%d)", d)
	}

	res := settle()
	if !res.Fresh || res.Rerun {
		t.Fatalf("F1 RED: the demand revalidation did not settle on its accepted Put (fresh=%v rerun=%v) — "+
			"an absorbed read must not rerun a revalidation that succeeded", res.Fresh, res.Rerun)
	}
	if n := DemandRevalidationsInFlight(); n != 0 {
		t.Fatalf("F1 RED: %d demand revalidation slot(s) still held after the accepted Put settled", n)
	}
	if got := KnownStaleRevalidationRerunBySlot()["demand"]; got != 0 {
		t.Fatalf("F1 RED: rerun{demand} = %d, want 0", got)
	}
	if f := k884Stat("known_stale_revalidation_failed_total"); f != 0 {
		t.Fatalf("F1 RED: the accepted revalidation was counted as failed (%d)", f)
	}
}

// F1b — the churn shape: the revalidation's resolve READ the list that then
// moved (a real dep event mid-resolve), so its accepted Put is also #375
// remarked. The revalidation still settles on the accepted Put: Fresh, no
// rerun, never counted failed; the remark is an ordinary open window.
// Mutation: Fresh = accepted && !remarked (the pre-#884 settle) → RED.
func TestIssue884_F1b_ARemarkedDemandPutStillSettlesTheRevalidation(t *testing.T) {
	c, r, key, t0 := k884Cell(t)
	resetKnownStaleForTest()
	t.Cleanup(resetKnownStaleForTest)
	gvr := schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "fullstackapps"}

	if !ClaimDemandRevalidation(key, t0) || ClaimDemandRevalidation(key, t0) {
		t.Fatalf("PRECONDITION: claim + absorbed read")
	}
	dctx, settle := BeginDemandRevalidation(context.Background(), key, t0)
	rctx := WithL1KeyContext(dctx, key)
	Deps().RecordList(rctx, key, gvr, "ns") // the resolve LISTs the namespace
	gen := c.CaptureGen(key)
	Deps().bumpCoordinateGen(gvr, "ns", "obj-1") // an edit lands mid-resolve
	r.markDirty(key, time.Now())                 // and dirty-marks the cell

	moved0 := Deps().movedRemarkGuardedTotal.Load()
	if !c.PutIfGen(rctx, key, &ResolvedEntry{RawJSON: []byte(`{"body":"new"}`), Inputs: mustInputs(t, c, key)}, gen) {
		t.Fatalf("PRECONDITION: the revalidation's Put was refused")
	}
	if Deps().movedRemarkGuardedTotal.Load() != moved0+1 {
		t.Fatalf("PRECONDITION: the accepted Put was not #375 remarked")
	}
	if _, reason, ks := KnownStale(key); ks {
		t.Fatalf("F1b RED: still KNOWN-STALE (%s) after the accepted revalidation Put (#884)", reason)
	}
	if s, open := dirtyStampFor(key); !open || !s.t0.Equal(t0) {
		t.Fatalf("F1b: want the window open from t0 after the remark (open=%v)", open)
	}
	res := settle()
	if !res.Fresh || res.Rerun {
		t.Fatalf("F1b RED: a remarked but ACCEPTED revalidation did not settle (fresh=%v rerun=%v) — a moved "+
			"input is an ordinary open window for the refresher, not a failed revalidation (#884)", res.Fresh, res.Rerun)
	}
	if f := k884Stat("known_stale_revalidation_failed_total"); f != 0 {
		t.Fatalf("F1b RED: the accepted revalidation was counted failed (%d)", f)
	}
	if n := DemandRevalidationsInFlight(); n != 0 {
		t.Fatalf("F1b RED: %d demand slot(s) still held", n)
	}
}

func TestIssue884_F2_ARemarkedAcceptedRefreshEndsKnownStaleAtTheSettle(t *testing.T) {
	c, r, key, t0 := k884Cell(t)

	// The refresher dequeues the cell again (a later mark re-queued it).
	depEventSeq.Add(1)
	r.markDirty(key, time.Now())
	bt0, open := r.beginWindow(key)
	if !open || !bt0.Equal(t0) {
		t.Fatalf("PRECONDITION: the dequeue found no window from t0")
	}
	// Its terminal write is ACCEPTED, but the resolve carried no #375 dep-gen
	// sink: the Put proves nothing about the mark (endKnownStaleOnPut ends
	// nothing) and the guard remarks it fail-fresh. The refresh still succeeded.
	octx, o := withRefreshOutcome(context.Background(), key)
	gen := c.CaptureGen(key)
	stored := c.ReplaceIfGenRefresh(octx, key, &ResolvedEntry{RawJSON: []byte(`{"body":"new"}`), Inputs: mustInputs(t, c, key)}, gen)
	NoteRefreshWrite(octx, key, stored)
	if !stored || !o.accepted.Load() || !o.remarked.Load() {
		t.Fatalf("PRECONDITION: want an accepted, remarked write (stored=%v accepted=%v remarked=%v)",
			stored, o.accepted.Load(), o.remarked.Load())
	}
	remarked0 := r.dirtyStats.unfresh[unfreshRemarked].Load()
	r.settleDequeue(key, t0, true, o)
	if r.dirtyStats.unfresh[unfreshRemarked].Load() != remarked0+1 {
		t.Fatalf("PRECONDITION: the settle did not classify the dequeue as remarked")
	}

	if _, reason, ks := KnownStale(key); ks {
		t.Fatalf("F2 RED: the cell is still KNOWN-STALE (%s) after an accepted (remarked) refresh — the "+
			"keep-open settle carried the flag over (#884)", reason)
	}
	s, open := dirtyStampFor(key)
	if !open || !s.t0.Equal(t0) || s.inflight {
		t.Fatalf("F2: want an ordinary open window from t0 after a remark (open=%v)", open)
	}
}

func TestIssue884_F3_AFailedRefreshStillKeepsTheCellKnownStale(t *testing.T) {
	c, r, key, t0 := k884Cell(t)
	resetKnownStaleForTest()
	t.Cleanup(resetKnownStaleForTest)

	// (a) a retried error (transient failure) keeps the flag.
	depEventSeq.Add(1)
	r.markDirty(key, time.Now())
	if _, open := r.beginWindow(key); !open {
		t.Fatalf("PRECONDITION: no window")
	}
	r.windowContinue(key, t0)
	if _, reason, ks := KnownStale(key); !ks || reason != "declined" {
		t.Fatalf("F3a: a retried (failed) refresh ended the known-stale state (ks=%v reason=%q)", ks, reason)
	}

	// (b) a Put with no dep-gen sink ends nothing (it proves nothing).
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"body":"plain"}`), Inputs: mustInputs(t, c, key)})
	if _, _, ks := KnownStale(key); !ks {
		t.Fatalf("F3b: a plain Put with no #375 sink ended the known-stale state")
	}

	// (c) a demand attempt whose Put was NOT accepted (it declined) keeps the
	// absorbed read's #740 rerun.
	if !ClaimDemandRevalidation(key, t0) || ClaimDemandRevalidation(key, t0) {
		t.Fatalf("PRECONDITION: claim + absorbed read")
	}
	_, settle := BeginDemandRevalidation(context.Background(), key, t0)
	if res := settle(); res.Fresh || !res.Rerun {
		t.Fatalf("F3c: a failed demand attempt with an absorbed read must rerun (fresh=%v rerun=%v)", res.Fresh, res.Rerun)
	}
	_, settle2 := BeginDemandRevalidation(context.Background(), key, t0)
	if res := settle2(); res.Rerun {
		t.Fatalf("F3c: the rerun looped without another read")
	}

	// (d) an accepted Put re-opens the window (newer mark); its next refresh
	// DECLINES → known-stale again.
	rctx := WithL1KeyContext(context.Background(), key)
	depEventSeq.Add(1)
	r.markDirty(key, time.Now())
	if !c.PutIfGen(rctx, key, &ResolvedEntry{RawJSON: []byte(`{"body":"new"}`), Inputs: mustInputs(t, c, key)}, c.CaptureGen(key)) {
		t.Fatalf("PRECONDITION: Put refused")
	}
	if _, _, ks := KnownStale(key); ks {
		t.Fatalf("PRECONDITION: the accepted Put did not end the known-stale state")
	}
	if _, open := r.beginWindow(key); !open {
		t.Fatalf("PRECONDITION: the re-opened window is gone")
	}
	r.settleDequeue(key, t0, true, nil)
	if _, reason, ks := KnownStale(key); !ks || reason != "declined" {
		t.Fatalf("F3d: the re-opened window's declined refresh did not make the cell known-stale again (ks=%v reason=%q)", ks, reason)
	}
}

func mustStartSeq(t *testing.T, ctx context.Context) uint64 {
	t.Helper()
	s, ok := DepGenStartSeqFromContext(ctx)
	if !ok {
		t.Fatalf("PRECONDITION: no dep-gen sink on the resolve ctx")
	}
	return s
}

func mustInputs(t *testing.T, c *ResolvedCacheStore, key string) *ResolvedKeyInputs {
	t.Helper()
	e, ok := c.GetNoTouch(key)
	if !ok || e.Inputs == nil {
		t.Fatalf("PRECONDITION: %s not resident with inputs", key)
	}
	in := *e.Inputs
	return &in
}
