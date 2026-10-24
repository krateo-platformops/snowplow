//go:build unit || integration

// issue883_memo_typelevel_test.go — #883 review follow-ups on PR #885:
//   - the type-level dep events (bumpResourceTypeGen → notifyResolveMemosTypeMoved)
//     delete the memo entries they move, and only those;
//   - the pending bucket-prune / L1-removal state does not grow with repeats
//     between drains.

package cache

import (
	"fmt"
	"testing"
)

// TestIssue883_TypeLevelEventDropsWhatItMoves — a schema relist moves every dep
// of the GVR (exact and LIST), so only the entry on another GVR stays; a CRD
// becoming available moves only the GVR's LIST deps, so an exact dep with its
// own bucket stays. No Load is made: the type-level bump itself must delete.
func TestIssue883_TypeLevelEventDropsWhatItMoves(t *testing.T) {
	store := func(t *testing.T) (*DepTracker, *SeedResolveMemo) {
		t.Helper()
		ResetDepsForTest()
		t.Cleanup(ResetDepsForTest)
		d := Deps()
		m := NewSeedResolveMemo()
		t.Cleanup(TrackDepMoves(m))
		stamp := DepGenEpochNow()
		for _, e := range []struct {
			key string
			dep DepKey
		}{
			{"exact", DepKey{GVR: gvr883Hot, Namespace: "ns-t", Name: "obj-1"}},
			{"list", DepKey{GVR: gvr883Hot, Namespace: "ns-t", Name: listWildcard}},
			{"calm", DepKey{GVR: gvr883Calm, Namespace: "ns-t", Name: "q"}},
		} {
			record883(t, d, "L1_883_type_producer", e.dep)
			m.Store(e.key, memoBody(map[string]any{"k": e.key}), []DepKey{e.dep}, stamp)
		}
		return d, m
	}

	t.Run("schema_relist_moves_every_dep_of_the_gvr", func(t *testing.T) {
		d, m := store(t)
		d.OnResourceTypeSchemaRelisted(gvr883Hot)
		if got := fmt.Sprint(memoKeys883(m)); got != "[calm]" {
			t.Fatalf("#883 RED (type-level): after a schema relist of the hot GVR the memo holds %s, want [calm]", got)
		}
	})

	t.Run("crd_available_moves_only_list_deps", func(t *testing.T) {
		d, m := store(t)
		d.OnResourceTypeAvailable(gvr883Hot)
		if got := fmt.Sprint(memoKeys883(m)); got != "[calm exact]" {
			t.Fatalf("#883 RED (type-level, list-only): after CRD_ADD of the hot GVR the memo holds %s, want [calm exact]", got)
		}
	})
}

// TestIssue883_PendingTrackerDropsAreASet — the same coordinate pruned, and the
// same L1 key removed, 200 times between two drains leave ONE pending item each;
// the drain then empties them.
func TestIssue883_PendingTrackerDropsAreASet(t *testing.T) {
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	d := Deps()
	m := NewSeedResolveMemo()
	defer TrackDepMoves(m)()
	drainResolveMemoBucketGone()

	dep := DepKey{GVR: gvr883Calm, Namespace: "ns-set", Name: "q"}
	for i := 0; i < 200; i++ {
		record883(t, d, "L1_883_churned", dep) // re-creates the bucket and the key's edges
		d.RemoveL1Key("L1_883_churned")        // prunes both again, no drain in between
	}
	if b, l := pendingTrackerDropsForTest(); b != 1 || l != 1 {
		t.Fatalf("#883 RED: 200 repeats of one prune and one removal left %d pending bucket(s) and %d pending L1 key(s), want 1 and 1", b, l)
	}
	m.Load("probe-none") // a memo operation drains
	if b, l := pendingTrackerDropsForTest(); b != 0 || l != 0 {
		t.Fatalf("the drain left %d bucket(s) and %d L1 key(s) pending", b, l)
	}
}
