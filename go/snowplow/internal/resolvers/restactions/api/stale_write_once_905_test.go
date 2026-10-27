package api

// stale_write_once_905_test.go — #905 review F5 at the D2 decision site.
//
// staleServeEligible's WriteSent check is only the fast path: a write of the
// same request can begin AFTER it (a parallel nested resolve:true item), so
// serveStaleWithDebt itself must refuse the stale serve under the sink's lock
// (NoteIfNoWrite). The cache package pins the primitive
// (internal/cache/stale_write_once_905_test.go); this arm pins its use.
//
//   F5  a known-stale content read reaching the decision after a write of its
//       request was sent is NOT served stale: no stale serve noted, no debt
//       opened, and the reader reads through. Control: the same decision on a
//       sink with no write serves stale.
//   N1  a write in a NESTED resolve that re-scoped its L1 key (a resolve:true
//       item filling its own cell) is decided on the REQUEST's sink: when the
//       request's pass was served stale, the nested write is held for the
//       fresh re-run (0 sends), never sent from the stale pass and then again.
//       Control: the same nested write on a request not served stale is sent
//       once.
//
// Mutations (logic-only): serveStaleWithDebt notes the serve without
// NoteIfNoWrite's write check → F5 RED; refuseWriteFromStalePass reads the
// L1-scoped sink (StaleServeSinkFromContext) instead of the request's
// (StaleServeSinkForWrite) → N1 RED.

import (
	"net/http"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestStale905_F5_ASentWriteRefusesTheStaleServeAtTheDecision(t *testing.T) {
	f := newD2Fixture(t)
	release := HoldStaleDebtRefillsForTest()
	t.Cleanup(release)

	ctx, sink := cache.WithStaleServeSink(cache.WithL1KeyContext(r1Ctx(), "dep-905"), "dep-905", "restactions")
	// The parallel nested item's write: begun after this read passed
	// staleServeEligible, before the decision.
	if !sink.BeginWrite() {
		t.Fatal("PRE: the first write of a fresh sink must be sent")
	}
	if e, served := serveStaleWithDebt(ctx, sink, f.store, f.contentKey, fill710ListCall(), "", cache.DepKey{}); served || e != nil {
		t.Fatal("#905 F5 RED: a known-stale read was served stale after a write of its request was sent — " +
			"the D2 re-run would send that write again")
	}
	if sink.StaleDerived() {
		t.Fatal("#905 F5 RED: the refused stale serve was noted on the sink")
	}
	if st := StaleServeStatsSnapshot(); st.ServeTotal != 0 || st.DebtOpen != 0 {
		t.Fatalf("#905 F5 RED: the refused stale serve counted or opened a debt: %+v", st)
	}

	// Control: with no write sent, the same decision serves stale.
	ctx2, sink2 := cache.WithStaleServeSink(cache.WithL1KeyContext(r1Ctx(), "dep-905c"), "dep-905c", "restactions")
	if _, served := serveStaleWithDebt(ctx2, sink2, f.store, f.contentKey, fill710ListCall(), "", cache.DepKey{}); !served {
		t.Fatal("PRE (control): a known-stale read with no write sent must be served stale — else F5 is vacuous")
	}
	if !sink2.StaleDerived() {
		t.Fatal("PRE (control): the stale serve was not noted")
	}
}

func TestStale905_N1_ANestedWriteIsDecidedOnTheRequestSink(t *testing.T) {
	run := func(t *testing.T, servedStale bool) int {
		t.Helper()
		api, _ := upstream410(t)
		ext, extDials := upstream410(t)
		env410(t, api.URL, ext.URL)
		// The customer request: its L1 key scoped, its D2 sink on it.
		ctx, sink := cache.WithStaleServeSink(cache.WithL1KeyContext(ctxWithUser(user410, group410), "outer"), "outer", "restactions")
		if servedStale && !sink.NoteIfNoWrite("content-c") {
			t.Fatal("PRE: the request's stale serve must be noted")
		}
		// A nested resolve:true item re-scopes the L1 key to its own cell.
		nested := cache.WithL1KeyContext(ctx, "nested-cell")
		if cache.StaleServeSinkFromContext(nested) != nil {
			t.Fatal("PRE: a re-scoped nested resolve must be out of D2 scope for its reads")
		}
		resolve410(nested, api.URL, extStage410("render", http.MethodPost, "/render"))
		return len(extDials())
	}
	if n := run(t, true); n != 0 {
		t.Fatalf("#905 F5 RED: a nested write under a request served stale was sent %d time(s) from the stale pass — "+
			"the request's fresh re-run sends it again", n)
	}
	if n := run(t, false); n != 1 {
		t.Fatalf("PRE (control): the nested write on a request not served stale must be sent once, got %d", n)
	}
}
