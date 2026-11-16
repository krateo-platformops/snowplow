// list_cell_854_f3d_test.go — #854 through the refresher and #643 PR-B.
//
// The incremental twin of TestIssue643_F3d_StaleListCell_HeldWhileStale_
// PutAfterRealRefresh, on the same scenario (h643F3dRun): boot marks iter-w
// declined-external, its list is emptied, two background passes run while the
// list cell still holds the pre-delete body, then the REAL apistage refresh of
// the list cell runs, then a pass must Put iter-w for every cohort with 0 dials.
//
// With the incremental cell that refresh is resolveAndPopulateL1's #854 path:
// it drains the cell's pending deltas into a new generation and returns with NO
// store write. So:
//
//	F4r (C3) — the refresh writes nothing to the store: the content key's
//	          generation and Put identity do not move, nothing is published,
//	          no rebuild compare runs; refresh_skipped counts it.
//	#643     — the declined-external mark retires anyway: a served cell's
//	          identity is its Put AND its body (ServedCell.BodySeq). Without
//	          the body half, a mark taken over an incremental cell would hold
//	          forever — the cell's content changes without a Put (I-1).
package dispatchers

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestListCell854_F3dTwin_RefreshDrainsWithNoStoreWriteAndTheMarkRetires(t *testing.T) {
	t.Cleanup(cache.SetListCellAnchoringForTest(true))
	var bootBody uint64
	dialsB, dialsC, put := h643F3dRun(t, h643F3dRefresh, func() {
		// Runs after the boot pass, before the deletes: the boot generation.
		for _, k := range cache.ResolvedCache().KeysForTest() {
			if e, ok := cache.ResolvedCache().GetNoTouch(k); ok && e.ListCell() != nil {
				_, bootBody, _ = cache.ResolvedCache().CellServeIdentity(k)
			}
		}
	}, func(cellKey string, refresh func()) {
		store := cache.ResolvedCache()
		e, ok := store.GetNoTouch(cellKey)
		if !ok || e.ListCell() == nil || !e.ListCell().Anchored() {
			t.Fatal("PRECONDITION: the h643things list cell is not an anchored incremental cell")
		}
		gen0 := store.CaptureGen(cellKey)
		seq0, _ := store.CellPutSeq(cellKey)
		pub0, _, _, _ := cache.RefreshBroadcasterCounters()
		ri0, rc0, rn0 := RefreshRebuildStats()
		skipped0 := cache.ListCellStatsSnapshot().RefreshSkipped

		refresh()

		if g := store.CaptureGen(cellKey); g != gen0 {
			t.Fatalf("F4r (C3): the refresh moved the content key's generation %d → %d", gen0, g)
		}
		if s, _ := store.CellPutSeq(cellKey); s != seq0 {
			t.Fatalf("F4r RED (C3): the refresh re-Put the incremental list cell (putSeq %d → %d) — it must drain, not re-list", seq0, s)
		}
		if pub, _, _, _ := cache.RefreshBroadcasterCounters(); pub != pub0 {
			t.Fatalf("F4r (C3): the refresh published %d refresh signal(s)", pub-pub0)
		}
		if ri, rc, rn := RefreshRebuildStats(); ri != ri0 || rc != rc0 || rn != rn0 {
			t.Fatal("F4r (C3): the refresh ran a rebuild compare")
		}
		if got := cache.ListCellStatsSnapshot().RefreshSkipped - skipped0; got != 1 {
			t.Fatalf("F4r: refresh_skipped moved by %d, want 1", got)
		}
		// The passes before the refresh already read — and so drained — the
		// cell: its body moved past the boot one, without a single Put.
		if _, body1, _ := store.CellServeIdentity(cellKey); body1 == bootBody {
			t.Fatal("F4r: the cell's body never moved past the boot generation — the deletes were not applied")
		}
	})
	if dialsB != 0 {
		t.Fatalf("#643 twin: pass B dialled %d while the marks were covered by the unchanged cell", dialsB)
	}
	if dialsC != 0 {
		t.Fatalf("#643 twin: after the drain the emptied iterator must make 0 external dials; got %d", dialsC)
	}
	if put != h643K {
		t.Fatalf("#643 twin RED: iter-w Put for %d of %d cohorts after the list cell drained — a mark over an incremental cell must retire on a new body, or it holds forever (I-1)", put, h643K)
	}
}
