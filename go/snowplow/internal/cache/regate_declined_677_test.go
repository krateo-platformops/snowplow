package cache

// regate_declined_677_test.go — #677 (2): the refresher's known-declined mark
// retires on the cache-side rules (d) Put, (e) evicted, (c) GONE, and does so
// through the SAME chokepoints #656's declined-external mark uses, so one Put
// clears both marks.

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

var raGVR677 = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}

func mark677(def DepKey) *RegateDeclinedMark {
	return &RegateDeclinedMark{RBACPublishSeq: 7, Defs: []DefinitionVersion{{Key: def, UID: "u", ResourceVersion: "1"}}}
}

func retired677(reason string) uint64 {
	for _, c := range RegateDeclinedRetiredSnapshot() {
		if c.Reason == reason {
			return c.Count
		}
	}
	return 0
}

// Rule (d): any accepted Put, every variant — and it is the SAME chokepoint as
// #656: one Put clears BOTH the declined-external mark and this one.
func TestIssue677_PutRetiresBothMarksAtOneChokepoint(t *testing.T) {
	ResetRegateDeclinedForTest()
	t.Cleanup(ResetRegateDeclinedForTest)
	ResetEngineDeclinedExternalSetForTest()
	t.Cleanup(ResetEngineDeclinedExternalSetForTest)
	c := newResolvedCache(10, 1<<20, time.Hour)
	inputs := ResolvedKeyInputs{CacheEntryClass: "restactions", Name: "ra677"}
	key := ComputeKey(inputs)
	c.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":0}`), Inputs: &inputs})

	MarkRegateDeclined(key, mark677(DepKey{GVR: raGVR677, Namespace: "ns", Name: "ra677"}))
	EngineDeclinedExternalSet().Mark(key)
	other := "some-other-key"
	MarkRegateDeclined(other, mark677(DepKey{GVR: raGVR677, Namespace: "ns", Name: "x"}))

	gen := c.CaptureGen(key)
	if !c.PutIfGen(nil, key, &ResolvedEntry{RawJSON: []byte(`{"v":1}`), Inputs: &inputs}, gen) {
		t.Fatal("setup: PutIfGen refused")
	}
	if _, ok := LookupRegateDeclined(key); ok {
		t.Fatal("RED: an accepted L1 Put did not retire the #677 known-declined mark (rule d)")
	}
	if EngineDeclinedExternalSet().Marked(key) {
		t.Fatal("RED: the same Put did not retire the #656 declined-external mark — the chokepoint is not shared")
	}
	if _, ok := LookupRegateDeclined(other); !ok {
		t.Fatal("a Put of one key retired another key's mark")
	}
	if got := retired677(RegateDeclinedRetiredPutUnmark); got != 1 {
		t.Fatalf("retired_total{put_unmark} = %d, want 1", got)
	}
}

// Rule (e): both eviction bodies (LRU/TTL funnel and the DELETE funnel).
func TestIssue677_EvictionRetiresTheMark(t *testing.T) {
	ResetRegateDeclinedForTest()
	t.Cleanup(ResetRegateDeclinedForTest)

	t.Run("delete_for_dep", func(t *testing.T) {
		c := newResolvedCache(10, 1<<20, time.Hour)
		inputs := ResolvedKeyInputs{CacheEntryClass: "restactions", Name: "del"}
		key := ComputeKey(inputs)
		c.Put(key, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &inputs})
		MarkRegateDeclined(key, mark677(DepKey{GVR: raGVR677, Name: "del"}))
		c.deleteForDep(key)
		if _, ok := LookupRegateDeclined(key); ok {
			t.Fatal("RED: the mark outlived its key's DELETE eviction")
		}
	})
	t.Run("lru", func(t *testing.T) {
		c := newResolvedCache(1, 1<<20, time.Hour)
		in1 := ResolvedKeyInputs{CacheEntryClass: "restactions", Name: "lru1"}
		in2 := ResolvedKeyInputs{CacheEntryClass: "restactions", Name: "lru2"}
		k1, k2 := ComputeKey(in1), ComputeKey(in2)
		c.Put(k1, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in1})
		MarkRegateDeclined(k1, mark677(DepKey{GVR: raGVR677, Name: "lru1"}))
		c.Put(k2, &ResolvedEntry{RawJSON: []byte(`{}`), Inputs: &in2}) // evicts k1 (cap 1)
		if c.Has(k1) {
			t.Fatal("setup: k1 was not LRU-evicted")
		}
		if _, ok := LookupRegateDeclined(k1); ok {
			t.Fatal("RED: the mark outlived its key's LRU eviction")
		}
	})
	if got := retired677(RegateDeclinedRetiredEvicted); got != 2 {
		t.Fatalf("retired_total{evicted} = %d, want 2", got)
	}
}

// Rule (c): a deleted definition coordinate retires every mark that recorded it.
func TestIssue677_GoneRetiresMarksOnThatCoordinateOnly(t *testing.T) {
	ResetRegateDeclinedForTest()
	t.Cleanup(ResetRegateDeclinedForTest)
	ra := DepKey{GVR: raGVR677, Namespace: "krateo-system", Name: "global-search"}
	MarkRegateDeclined("k-a", mark677(ra))
	MarkRegateDeclined("k-b", mark677(ra))
	MarkRegateDeclined("k-c", mark677(DepKey{GVR: raGVR677, Namespace: "krateo-system", Name: "other"}))

	notifyObjectGone(ra.GVR, ra.Namespace, ra.Name)

	for _, k := range []string{"k-a", "k-b"} {
		if _, ok := LookupRegateDeclined(k); ok {
			t.Fatalf("RED: %s survived the deletion of its RESTAction (rule c)", k)
		}
	}
	if _, ok := LookupRegateDeclined("k-c"); !ok {
		t.Fatal("a deletion retired a mark on a different coordinate")
	}
	if got := retired677(RegateDeclinedRetiredGone); got != 2 {
		t.Fatalf("retired_total{gone} = %d, want 2", got)
	}
	if RegateDeclinedLen() != 1 {
		t.Fatalf("marks gauge = %d, want 1", RegateDeclinedLen())
	}
}

// The retire breakdown is a closed set with every cell pre-populated.
func TestIssue677_RetiredReasonsClosedAndPrepopulated(t *testing.T) {
	ResetRegateDeclinedForTest()
	t.Cleanup(ResetRegateDeclinedForTest)
	snap := RegateDeclinedRetiredSnapshot()
	if len(snap) != len(RegateDeclinedRetiredReasons) {
		t.Fatalf("%d cells, want %d", len(snap), len(RegateDeclinedRetiredReasons))
	}
	RecordRegateDeclinedRetired("not-a-reason", 3)
	for _, c := range RegateDeclinedRetiredSnapshot() {
		if c.Count != 0 {
			t.Fatalf("a reason outside the closed set moved %s", c.Reason)
		}
	}
}
