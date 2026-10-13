package dispatchers

// issue643_prb_f3e_served_cell_expiry_test.go — #643 PR-B F3e (architect-ch
// must-fix 1): a served input cell that EXPIRES with nobody reading it still
// sits in the store's index (expiry is lazy), yet it is no longer a live cell.
// A mark served from it must retire on the next background pass — reason
// served_cell_changed — and the unit must be resolved again, not held over a
// dead input past the cell's lifetime.
//
// The cell is aged past the store's max entry age through SetBornAtForTest (the
// same lazy bound getCore applies), with no read in between. The liveness
// predicate is one helper shared by getCore and PutSeqOf (itemExpiredLocked);
// the mutation run that made PutSeqOf ignore it is recorded in the PR.
//
// Not t.Parallel(): reassigns package vars.

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func h643RetiredCount(reason string) uint64 {
	for _, c := range cache.DeclinedExternalRetiredSnapshot() {
		if c.Reason == reason {
			return c.Count
		}
	}
	return 0
}

func TestIssue643_F3e_ExpiredServedCell_RetiresTheMark(t *testing.T) {
	env := h643Setup(t, h643Opts{cohorts: h643K, internal: 1, things: 2, extra: []h643Extra{h643IterExtra}})
	cache.ResetDeclinedExternalRetiredForTest()
	t.Cleanup(cache.ResetDeclinedExternalRetiredForTest)
	h643Budget(t, nil)
	if d := env.pass(prewarmScope{kind: scopeKindBoot}); d != h643K*2 {
		t.Fatalf("precondition: boot dialled %d, want %d", d, h643K*2)
	}
	cellKey, _ := h643F3dCell(t)
	covered := 0
	for _, m := range cache.EngineDeclinedExternalSet().MarksForTest() {
		for _, sc := range m.Served {
			if sc.Key == cellKey {
				covered++
			}
		}
	}
	if covered == 0 {
		t.Fatal("precondition: no iter-w mark was served from the list cell")
	}
	// Still live and unchanged: the next pass holds every mark.
	if d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643WidgetGVR}); d != 0 {
		t.Fatalf("precondition: with every input unchanged the pass must skip; dialled %d", d)
	}

	// The cell dies of old age with nobody reading it: past the max entry age.
	if !cache.ResolvedCache().SetBornAtForTest(cellKey, time.Now().Add(-48*time.Hour)) {
		t.Fatal("precondition: could not age the list cell")
	}
	if _, live := cache.ResolvedCache().PutSeqOf(cellKey); live {
		t.Error("PutSeqOf reports an expired cell as live — the mark would outlive its input")
	}

	r0 := h643RetiredCount(cache.DeclinedExternalRetiredServedCellChanged)
	d := env.pass(prewarmScope{kind: scopeKindGVRDiscovered, gvr: h643RAGVR})
	if got := h643RetiredCount(cache.DeclinedExternalRetiredServedCellChanged) - r0; got != uint64(covered) {
		t.Fatalf("F3e: %d marks retired with served_cell_changed, want %d (every mark served from the expired cell)", got, covered)
	}
	// The list still has 2 items, so the re-resolve touches external again:
	// the units were resolved, not skipped.
	if d != covered*2 {
		t.Fatalf("F3e: the pass after the cell expired dialled %d, want %d (the retired units re-resolved)", d, covered*2)
	}
}
