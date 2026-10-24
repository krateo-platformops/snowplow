//go:build unit || integration

// issue883_memo_l1_link_test.go — #883 follow-up (owner ruling 2026-10-24):
// with the #598 wholesale swap removed, the resolve memo must not outgrow the
// set of L1 keys it can still serve. The case the swap was bounding and the dep
// signals cannot: entries whose deps NEVER move and that nobody loads again —
// the cells of an identity that stopped visiting, evicted from the resolved
// cache. Each memo entry now lives exactly as long as at least one L1 key it
// served (producer or hitter) is still in the resolved cache.

package cache

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// TestIssue883_MemoNeverOutgrowsLiveServableKeys — 30 identities visit in turn,
// 6 cells each, then stop visiting. Their deps live on a GVR that never moves,
// so no dep signal ever fires. The resolved cache holds 12 cells and evicts the
// rest (LRU, the real eviction funnel). After every visit the memo must hold
// exactly the entries with at least one consumer among the RESIDENT cells —
// computed from the test's own model of LRU residency and of which cell
// produced or hit which entry — and therefore never more entries than there
// are resident cells.
func TestIssue883_MemoNeverOutgrowsLiveServableKeys(t *testing.T) {
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	d := Deps()
	const cells = 12
	store := newResolvedCache(cells, 1<<30, time.Hour)
	d.SetStore(store)
	m := NewSeedResolveMemo()
	defer TrackDepMoves(m)()

	var putOrder []string                       // model: LRU order (no reads, so Put order)
	consumersOf := map[string]map[string]bool{} // model: memo key -> L1 keys it served
	link := func(memoKey, l1 string) {
		if consumersOf[memoKey] == nil {
			consumersOf[memoKey] = map[string]bool{}
		}
		consumersOf[memoKey][l1] = true
	}

	const visitors, perVisit = 30, 6
	for u := 0; u < visitors; u++ {
		shared := fmt.Sprintf("memo-u%d-shared", u)
		for c := 0; c < perVisit; c++ {
			l1 := fmt.Sprintf("L1-u%d-c%d", u, c)
			dep := DepKey{GVR: gvr883Calm, Namespace: "ns-l1", Name: fmt.Sprintf("u%d-c%d", u, c)}
			record883(t, d, l1, dep) // the resolve's own reads (objects.Get records the RA CR)
			body := memoBody(map[string]any{"u": float64(u), "c": float64(c)})
			if c%2 == 0 { // a cell with its own RESTAction tuple
				key := fmt.Sprintf("memo-u%d-c%d", u, c)
				m.StoreLayeredWithStates(key, l1, body, []DepKey{dep}, nil, DepGenEpochNow(), nil)
				link(key, l1)
			} else if _, _, _, _, _, ok := m.LoadLayeredWithStates(shared, l1); ok {
				link(shared, l1) // a sibling widget hits the identity's shared tuple
			} else {
				m.StoreLayeredWithStates(shared, l1, body, []DepKey{dep}, nil, DepGenEpochNow(), nil)
				link(shared, l1)
			}
			store.Put(l1, &ResolvedEntry{RawJSON: body})
			putOrder = append(putOrder, l1)
		}

		// Model: the resident cells are the last `cells` Puts.
		resident := map[string]bool{}
		for _, k := range putOrder[max(0, len(putOrder)-cells):] {
			resident[k] = true
		}
		got := map[string]bool{}
		for _, k := range store.KeysForTest() {
			got[k] = true
		}
		if fmt.Sprint(sortedKeys883(got)) != fmt.Sprint(sortedKeys883(resident)) {
			t.Fatalf("harness: the store's residency %v is not the LRU model %v", sortedKeys883(got), sortedKeys883(resident))
		}

		// Evictions reach the memo at its next operation (they are logged under
		// the store lock, never acted on there).
		m.Load("probe-none")

		var want []string
		for key, cs := range consumersOf {
			for l1 := range cs {
				if resident[l1] {
					want = append(want, key)
					break
				}
			}
		}
		sort.Strings(want)
		if keys := memoKeys883(m); fmt.Sprint(keys) != fmt.Sprint(want) {
			t.Fatalf("#883 RED (memo vs live keys): after visitor %d the memo holds %d entries %v, want the %d serving a resident cell %v",
				u, len(keys), keys, len(want), want)
		}
		if n := m.Entries(); n > cells {
			t.Fatalf("#883 RED: %d memo entries for %d resident cells", n, cells)
		}
	}
	if _, _, l1Gone := ResolveMemoDropTotals(); l1Gone == 0 {
		t.Fatal("the arm never exercised an L1-gone deletion")
	}
}

// TestIssue883_MemoLinkToAnEvictedKeyDoesNotKeepAnEntry — the link is made and
// then checked: a Store or a hit for an L1 key the resolved cache has already
// dropped cannot keep the entry alive (its removal was logged before the link
// existed, so nothing would ever undo it later).
func TestIssue883_MemoLinkToAnEvictedKeyDoesNotKeepAnEntry(t *testing.T) {
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	d := Deps()
	m := NewSeedResolveMemo()
	defer TrackDepMoves(m)()
	dep := DepKey{GVR: gvr883Calm, Namespace: "ns-ghost", Name: "q"}

	record883(t, d, "L1-gone", dep)
	d.RemoveL1Key("L1-gone") // evicted before the producer's Store lands
	m.StoreLayeredWithStates("memo-ghost", "L1-gone", memoBody(map[string]any{"x": 1.0}), []DepKey{dep}, nil, DepGenEpochNow(), nil)
	if keys := memoKeys883(m); len(keys) != 0 {
		t.Fatalf("#883 RED: an entry produced for an already-evicted L1 key was kept: %v", keys)
	}

	record883(t, d, "L1-live", dep)
	m.StoreLayeredWithStates("memo-shared", "L1-live", memoBody(map[string]any{"x": 2.0}), []DepKey{dep}, nil, DepGenEpochNow(), nil)
	record883(t, d, "L1-late", dep)
	d.RemoveL1Key("L1-late")
	if _, _, _, _, _, ok := m.LoadLayeredWithStates("memo-shared", "L1-late"); !ok {
		t.Fatal("a live entry must still serve the hit")
	}
	d.RemoveL1Key("L1-live")
	m.Load("probe-none")
	if keys := memoKeys883(m); len(keys) != 0 {
		t.Fatalf("#883 RED: a hit from an already-evicted L1 key kept the entry alive after its only live consumer left: %v", keys)
	}
}

func sortedKeys883(s map[string]bool) []string {
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
