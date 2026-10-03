package dispatchers

// reseed_navorder_test.go — #258 C6 (zero-cold-nav ordering). The reseed set must
// be ordered by the walk's first-nav priority (NavOrder ASC), so the pages a
// rotated identity opens first are warmed first — not by the harvester map's
// random iteration order, and not by name.
//
// Fixture: three widgets harvested in the order first-nav → middle → last, whose
// NAMES sort the opposite way (zz < mm < aa reversed), so a name sort is RED too.
// The enumeration is repeated to defeat a lucky map order. RED: drop the
// NavOrder sort in enumerateRotatedResidentTargets.

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestS258_ReseedTargetsOrderedByNavOrder(t *testing.T) {
	s258BuildWatcher(t)
	cache.ResetRBACShiftHooksForTest()
	t.Cleanup(cache.ResetRBACShiftHooksForTest)
	var rs cache.RotatedSubjectSet
	cache.RegisterRBACShiftHook(func(r cache.RotatedSubjectSet) { rs = r })
	cache.NotifyRBACShiftForTest([]cache.RotatedSubject{{Kind: "User", Name: a1Alice}})

	navHarv := newNavWidgetHarvester()
	want := []string{"zz-first-nav", "mm-middle", "aa-last"} // harvest (= NavOrder) order
	for _, name := range want {
		e := reseedWidgetEntry()
		w := e.W.DeepCopy()
		w.SetName(name)
		navHarv.harvestNavWidget(w, e.GVR, e.PerPage, e.Page, e.KeyPerPage, e.KeyPage)
	}
	deps := rePrewarmDeps{harvester: newContentPrewarmHarvester(), navHarv: navHarv, authnNS: h1NS}

	for iter := 0; iter < 20; iter++ {
		reqs := enumerateRotatedResidentTargets(context.Background(), deps, rs)
		if len(reqs) != len(want) {
			t.Fatalf("iter %d: want %d alice targets, got %d", iter, len(want), len(reqs))
		}
		for i, r := range reqs {
			if got := r.widget.W.GetName(); got != want[i] {
				names := make([]string, len(reqs))
				for k := range reqs {
					names[k] = reqs[k].widget.W.GetName()
				}
				t.Fatalf("iter %d: reseed order %v, want NavOrder order %v (first-nav pages must warm first)",
					iter, names, want)
			}
		}
	}
}
