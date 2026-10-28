// deps_entry_class_test.go — #479 1b / #603 PR-0: dirty-marks by ENTRY CLASS.
//
// snowplow_deps_dirty_mark_by_entry_class_total{class} splits the same marks
// #239 counts by the CacheEntryClass of the marked key. The class is read off
// the dep edge (the key's depSet, carried on the forward edge), never by a
// store read on the mark path. These arms pin:
//
//	EC1 per-class attribution, both orders a cell can be built in: Put then
//	    Record (content cells, apistage.go) and Record then Put (resolver cells)
//	EC2 Σ by_entry_class == dirty_mark_total, and the unregistered guard
//	EC3 an edge whose cell is gone (or never stored) is "unknown", not dropped
//	EC4 a busy store lock defers the class to the next store; never blocks
//	EC5 the type path attributes too
//	EC6 behaviour identity: the marked/evicted sets do not depend on the class
//
// Named mutations each turns RED (run by hand, recorded in the PR body):
//
//	M1 drop the class note in putCoreLocked      → EC1 record-then-put RED
//	M2 drop the depSet peek in recordInternal    → EC1 put-then-record RED
//	M3 count entry class over `matched` (incl. evicted) instead of marked → EC2 RED
//	M4 drop the forward-edge CAS (ds never on the edge) → EC1 RED (all unknown)
package cache

import (
	"context"
	"testing"
	"time"
)

func ecTracker(t *testing.T) (*DepTracker, *ResolvedCacheStore) {
	t.Helper()
	d := newTestDepTracker(t, 10_000)
	store := newResolvedCache(1_000, 1<<24, time.Hour)
	d.SetStore(store)
	return d, store
}

func ecEntry(class, ownName string) *ResolvedEntry {
	in := inputsFor(gvrCompositions(), "ns", ownName)
	in.CacheEntryClass = class
	return &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: in}
}

func ecSum(m map[string]uint64) uint64 {
	var s uint64
	for _, v := range m {
		s += v
	}
	return s
}

// EC1 + EC2 + EC3 — every class, both build orders, plus an edge with no cell.
func TestPR0_EntryClass_AttributionAndSumInvariant(t *testing.T) {
	d, store := ecTracker(t)
	ctx := context.Background()
	gvr := gvrCompositions()

	// Put THEN Record (the content-cell order: apistage.go records after PutIfGen).
	for i, class := range RemintClasses {
		k := "L1-put-then-record-" + class
		store.put(k, ecEntry(class, "own"+itoa(i)))
		d.RecordList(ctx, k, gvr, "ns")
	}
	// Record THEN Put (the resolver order: edges are recorded during the resolve).
	for i, class := range RemintClasses {
		k := "L1-record-then-put-" + class
		d.RecordList(ctx, k, gvr, "ns")
		store.put(k, ecEntry(class, "own2"+itoa(i)))
	}
	// An edge whose cell was never stored: attributed unknown, still counted.
	d.RecordList(ctx, "L1-no-cell", gvr, "ns")

	_, marked := d.OnObjectEvent(gvr, "ns", "some-object", objExists)
	want := 2*len(RemintClasses) + 1
	if marked != want {
		t.Fatalf("marked=%d, want %d", marked, want)
	}

	got := d.dirtyMarkByEntryClassSnapshot()
	for _, class := range RemintClasses {
		if got[class] != 2 {
			t.Errorf("by_entry_class[%s]=%d, want 2 (one per build order) — snapshot=%v", class, got[class], got)
		}
	}
	if got[dmEntryClassUnknown] != 1 {
		t.Errorf("by_entry_class[unknown]=%d, want 1 (the edge with no cell)", got[dmEntryClassUnknown])
	}
	if got[dmEntryClassUnregistered] != 0 {
		t.Errorf("by_entry_class[unregistered]=%d, want 0", got[dmEntryClassUnregistered])
	}
	if s, total := ecSum(got), d.dirtyMarkTotal.Load(); s != total {
		t.Fatalf("Σ by_entry_class=%d != dirty_mark_total=%d", s, total)
	}
	if len(got) != dmEntryClassN {
		t.Errorf("label set has %d values, want the closed %d (pre-registered, zero included)", len(got), dmEntryClassN)
	}
}

// EC2 — the guard: a class nobody registered lands in "unregistered", and
// still sums.
func TestPR0_EntryClass_UnregisteredGuard(t *testing.T) {
	d, store := ecTracker(t)
	gvr := gvrCompositions()
	store.put("L1-odd", ecEntry("someNewClass", "own"))
	d.RecordList(context.Background(), "L1-odd", gvr, "ns")
	// A real class string can never alias a sentinel label.
	store.put("L1-alias", ecEntry(dmEntryClassUnknown, "own2"))
	d.RecordList(context.Background(), "L1-alias", gvr, "ns")

	d.OnObjectEvent(gvr, "ns", "x", objExists)
	got := d.dirtyMarkByEntryClassSnapshot()
	if got[dmEntryClassUnregistered] != 2 {
		t.Fatalf("unregistered=%d, want 2 — snapshot=%v", got[dmEntryClassUnregistered], got)
	}
	if got[dmEntryClassUnknown] != 0 {
		t.Errorf("unknown=%d, want 0 (a stored class named \"unknown\" is unregistered)", got[dmEntryClassUnknown])
	}
	if s, total := ecSum(got), d.dirtyMarkTotal.Load(); s != total {
		t.Fatalf("Σ=%d != total=%d", s, total)
	}
}

// EC3 + EC6 — DELETE: the self entry is EVICTED (not marked) and must not be
// counted; the list dependent is marked and counted under its class.
func TestPR0_EntryClass_DeleteCountsMarkedNotEvicted(t *testing.T) {
	d, _, _, _ := threeClassDeps(t) // self/exact/list, all class "restactions"
	gvr := gvrCompositions()
	evicted, marked := d.OnObjectEvent(gvr, "ns", "obj", objAbsent)
	if evicted != 1 || marked != 2 {
		t.Fatalf("evicted=%d marked=%d, want 1/2 (behaviour unchanged by the counter)", evicted, marked)
	}
	got := d.dirtyMarkByEntryClassSnapshot()
	if got["restactions"] != 2 {
		t.Errorf("restactions=%d, want 2 (the evicted self entry is not a mark)", got["restactions"])
	}
	if s, total := ecSum(got), d.dirtyMarkTotal.Load(); s != total {
		t.Fatalf("Σ=%d != total=%d", s, total)
	}
}

// EC4 — a busy store mutex never blocks a Record: the class is simply not
// learned, and the next accepted store of the key fills it.
func TestPR0_EntryClass_BusyStoreLockDefersNeverBlocks(t *testing.T) {
	d, store := ecTracker(t)
	gvr := gvrCompositions()
	store.put("L1-busy", ecEntry(CacheEntryClassApistage, "own"))

	store.mu.Lock()
	done := make(chan struct{})
	go func() {
		d.RecordList(context.Background(), "L1-busy", gvr, "ns")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		store.mu.Unlock()
		t.Fatal("RecordList blocked on the store mutex — the class peek must try-lock")
	}
	store.mu.Unlock()

	d.OnObjectEvent(gvr, "ns", "x", objExists)
	if got := d.dirtyMarkByEntryClassSnapshot(); got[dmEntryClassUnknown] != 1 {
		t.Fatalf("after a busy peek: unknown=%d, want 1 — snapshot=%v", got[dmEntryClassUnknown], got)
	}
	store.put("L1-busy", ecEntry(CacheEntryClassApistage, "own"))
	d.OnObjectEvent(gvr, "ns", "x", objExists)
	if got := d.dirtyMarkByEntryClassSnapshot(); got[CacheEntryClassApistage] != 1 {
		t.Fatalf("after the next store: apistage=%d, want 1 — snapshot=%v", got[CacheEntryClassApistage], got)
	}
}

// EC3 — a strip (RemoveL1Key) and a re-record start the key's class over; the
// key's class is fixed by its hash, so relearning yields the same value.
func TestPR0_EntryClass_RelearnedAfterStrip(t *testing.T) {
	d, store := ecTracker(t)
	gvr := gvrCompositions()
	ctx := context.Background()
	store.put("L1-w", ecEntry("widgets", "own"))
	d.RecordList(ctx, "L1-w", gvr, "ns")
	d.RemoveL1Key("L1-w")
	d.RecordList(ctx, "L1-w", gvr, "ns") // depSet recreated; peek relearns from the resident entry
	d.OnObjectEvent(gvr, "ns", "x", objExists)
	if got := d.dirtyMarkByEntryClassSnapshot(); got["widgets"] != 1 {
		t.Fatalf("widgets=%d, want 1 — snapshot=%v", got["widgets"], got)
	}
}

// EC5 — the type path (CRD lifecycle) attributes by class too, and sums.
func TestPR0_EntryClass_TypePath(t *testing.T) {
	d, store := ecTracker(t)
	gvr := gvrCompositions()
	ctx := context.Background()
	store.put("L1-ra", ecEntry(CacheEntryClassRAFullList, "own"))
	d.RecordList(ctx, "L1-ra", gvr, "ns")
	store.put("L1-wc", ecEntry(CacheEntryClassWidgetContent, "own2"))
	d.Record(ctx, "L1-wc", gvr, "ns", "named")

	if n := d.OnResourceTypeRemoved(gvr); n != 2 {
		t.Fatalf("type path marked %d, want 2", n)
	}
	got := d.dirtyMarkByEntryClassSnapshot()
	if got[CacheEntryClassRAFullList] != 1 || got[CacheEntryClassWidgetContent] != 1 {
		t.Fatalf("type path by class wrong: %v", got)
	}
	if s, total := ecSum(got), d.dirtyMarkTotal.Load(); s != total {
		t.Fatalf("Σ=%d != total=%d", s, total)
	}
}

// EC7 — the create-time peek (which takes the customer-facing c.mu) runs once
// per NEW depSet: a refresh-style re-Put and the idempotent HIT re-record
// (apistage.go) never re-peek. #564 removed a per-mark c.mu acquisition; this
// pins that the class plumbing did not put a per-refresh one back.
//
// SCOPE: "once per depSet", driven through store.Put. It does NOT prove that
// the production refresher's replace path never strips and re-records a key
// (which would recreate the depSet and re-peek); that rests on the strip
// living only in the delete primitive (F6a), which is INFERRED, not pinned here.
func TestPR0_EntryClass_PeekOncePerDepSetNotPerRefresh(t *testing.T) {
	d, store := ecTracker(t)
	gvr := gvrCompositions()
	ctx := context.Background()
	store.put("L1-c", ecEntry(CacheEntryClassApistage, "own"))
	d.RecordList(ctx, "L1-c", gvr, "ns")
	for i := 0; i < 3; i++ {
		store.put("L1-c", ecEntry(CacheEntryClassApistage, "own")) // refresh re-Put (replace in place)
		d.RecordList(ctx, "L1-c", gvr, "ns")                       // HIT re-record (idempotent)
		d.Record(ctx, "L1-c", gvr, "ns", "named"+itoa(i))          // a NEW edge on the SAME depSet
	}
	if a, l := d.classPeekAttempts.Load(), d.classPeekLearned.Load(); a != 1 || l != 1 {
		t.Fatalf("class peeks attempted=%d learned=%d, want 1/1 (once per new depSet)", a, l)
	}
}

// The /debug/vars view carries the split under "by_entry_class", ranging the
// SAME accessor the OTLP callback ranges (C7 anti-drift).
func TestPR0_EntryClass_ExpvarView(t *testing.T) {
	m, ok := DirtyMarkAttributionExpvar().(map[string]any)
	if !ok {
		t.Fatal("DirtyMarkAttributionExpvar is not a map")
	}
	v, ok := m["by_entry_class"].(map[string]uint64)
	if !ok {
		t.Fatalf("by_entry_class missing or wrong type in the /debug/vars view: %T", m["by_entry_class"])
	}
	if len(v) != dmEntryClassN {
		t.Fatalf("by_entry_class has %d labels, want %d", len(v), dmEntryClassN)
	}
}

// EC6 — exact beats wildcard is preserved by the mark collector: a key with
// BOTH an exact and a list edge on the coordinate is classified exact_dep, as
// collectMatchesWithDep would (#239 class), and is marked once.
func TestPR0_EntryClass_ExactBeatsWildcardPreserved(t *testing.T) {
	d, store := ecTracker(t)
	gvr := gvrCompositions()
	ctx := context.Background()
	store.put("L1-both", ecEntry("widgets", "owner"))
	d.RecordList(ctx, "L1-both", gvr, "ns")
	d.Record(ctx, "L1-both", gvr, "ns", "obj")
	_, marked := d.OnObjectEvent(gvr, "ns", "obj", objExists)
	if marked != 1 {
		t.Fatalf("marked=%d, want 1", marked)
	}
	if got := dmCount(d, dmPathObject, dmCauseAddUpdate, dmClassExactDep); got != 1 {
		t.Fatalf("exact_dep=%d, want 1 (exact must beat the wildcard)", got)
	}
	if got := dmCount(d, dmPathObject, dmCauseAddUpdate, dmClassListDep); got != 0 {
		t.Fatalf("list_dep=%d, want 0", got)
	}
}
