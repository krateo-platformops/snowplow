//go:build unit || integration

// issue890_refused_put_strips_edges_test.go — #890: a refused gen-guarded write
// of a cell that is not resident leaves no dep edges behind (and so no resolve
// memo entry linked to it), without ever leaving a STORED cell short of edges.

package cache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

type refusal890 struct {
	name string
	put  func(c *ResolvedCacheStore, key string, e *ResolvedEntry, gen uint64) bool
}

var refusals890 = []refusal890{
	{"PutIfGen", func(c *ResolvedCacheStore, k string, e *ResolvedEntry, g uint64) bool {
		return c.putIfGen(context.Background(), k, e, g)
	}},
	{"ReplaceIfGen", func(c *ResolvedCacheStore, k string, e *ResolvedEntry, g uint64) bool {
		return c.replaceIfGen(context.Background(), k, e, g)
	}},
	{"ReplaceIfGenRefresh", func(c *ResolvedCacheStore, k string, e *ResolvedEntry, g uint64) bool {
		return c.replaceIfGenRefresh(context.Background(), k, e, g)
	}},
}

func setup890(t *testing.T) (*DepTracker, *ResolvedCacheStore, *SeedResolveMemo) {
	t.Helper()
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	d := Deps()
	store := newResolvedCache(1_000, 1<<30, time.Hour)
	d.SetStore(store)
	m := NewSeedResolveMemo()
	t.Cleanup(TrackDepMoves(m))
	return d, store, m
}

func resident890(c *ResolvedCacheStore, key string) bool {
	_, ok := c.GetNoTouch(key)
	return ok
}

// TestIssue890_RefusedPutOfAnEvictedCellLeavesNoEdges — the reviewer's
// interleaving, on the real store, tracker and memo, for every refusing write:
//  1. a refresher resolve of cell K is in flight (it captured K's generation);
//  2. K is evicted (edges stripped, K logged as gone);
//  3. a dep event drains the log — nothing is linked to K yet;
//  4. the in-flight resolve records deps under K again;
//  5. the memo Store links K (the tracker check passes: the edges are back);
//  6. K's write-back is refused by the tombstone.
//
// After 6: K is not resident, the tracker holds no edge for K, and once the
// memo acts on the tracker it holds no entry for K.
func TestIssue890_RefusedPutOfAnEvictedCellLeavesNoEdges(t *testing.T) {
	for _, r := range refusals890 {
		t.Run(r.name, func(t *testing.T) {
			d, store, m := setup890(t)
			const k = "L1_890_K"
			dep := DepKey{GVR: gvr883Calm, Namespace: "ns-890", Name: "q"}

			record883(t, d, k, dep)
			resident := &ResolvedEntry{RawJSON: []byte(`{"v":"old"}`)}
			store.put(k, resident)
			gen := store.CaptureGen(k) // 1. the refresher resolve starts

			if !store.EvictUnrefreshable(k, resident) { // 2. eviction
				t.Fatal("harness: the eviction did not happen")
			}
			d.OnUpdate(gvr883Other, "ns-other", "x")                                         // 3. drain
			record883(t, d, k, dep)                                                          // 4. the in-flight resolve records again
			m.StoreLayeredWithStates("memo-orphan", k, memoBody(map[string]any{"v": "new"}), // 5.
				[]DepKey{dep}, nil, DepGenEpochNow(), nil, PutGateEffects{})
			if !d.hasEdges(k) || len(memoKeys883(m)) != 1 {
				t.Fatalf("harness: before the write-back K must have edges and a linked memo entry (edges=%v memo=%v)", d.hasEdges(k), memoKeys883(m))
			}

			if r.put(store, k, &ResolvedEntry{RawJSON: []byte(`{"v":"new"}`)}, gen) { // 6.
				t.Fatal("harness: the write-back of an evicted cell must be refused")
			}
			if resident890(store, k) {
				t.Fatal("the refused write stored K")
			}
			if d.hasEdges(k) {
				t.Fatalf("#890 RED (%s): the refused write-back left dep edges for non-resident K", r.name)
			}
			m.Load("probe-none") // the memo acts on the tracker at its next operation
			if keys := memoKeys883(m); len(keys) != 0 {
				t.Fatalf("#890 RED (%s): the memo still holds %v for non-resident K", r.name, keys)
			}
		})
	}
}

// TestIssue890_RefusalOfAResidentKeyKeepsItsEdges — a refusal because a NEWER
// write owns the resident cell must not touch that cell's edges.
func TestIssue890_RefusalOfAResidentKeyKeepsItsEdges(t *testing.T) {
	for _, r := range refusals890 {
		t.Run(r.name, func(t *testing.T) {
			d, store, _ := setup890(t)
			k := fmt.Sprintf("L1_890_res_%s", r.name)
			dep := DepKey{GVR: gvr883Calm, Namespace: "ns-890", Name: "res"}
			record883(t, d, k, dep)
			first := &ResolvedEntry{RawJSON: []byte(`{}`)}
			store.put(k, first)
			stale := store.CaptureGen(k)
			store.EvictUnrefreshable(k, first)
			fresh := store.CaptureGen(k)
			record883(t, d, k, dep)
			if !store.putIfGen(context.Background(), k, &ResolvedEntry{RawJSON: []byte(`{"n":1}`)}, fresh) {
				t.Fatal("harness: the newer write must be stored")
			}
			if r.put(store, k, &ResolvedEntry{RawJSON: []byte(`{"n":0}`)}, stale) {
				t.Fatal("harness: the stale write must be refused")
			}
			if !resident890(store, k) || !d.hasEdge(k, dep) {
				t.Fatalf("#890 RED: refusing a stale write stripped the resident cell's edges (resident=%v edge=%v)", resident890(store, k), d.hasEdge(k, dep))
			}
		})
	}
}
