package cache

// list_cell_854_test.go — #854: the incremental list cell's apply, at the cache
// layer, against a real informer.
//
//   C6   a byte-equal re-delivery with a NEW indexer slice (a relist that changed
//        nothing) rebinds the slot to the indexer's current slice — no decode,
//        and the old slice is no longer referenced (it is not pinned at up to 2×
//        the raw bytes).
//   R0   a re-delivery of the SAME indexer object (a resync) changes nothing: no
//        new body, no decode, the body identity kept.
//   CH   chunked copy-on-write: one change copies one chunk, and the reported
//        per-generation allocation is bounded by a few chunks, not the table.

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var c854GVR = schema.GroupVersionResource{Group: "c854.krateo.io", Version: "v1", Resource: "things"}

func c854Obj(i int) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "c854.krateo.io/v1", "kind": "Thing",
		"metadata": map[string]any{"namespace": "c854", "name": fmt.Sprintf("t-%04d", i)},
		"spec":     map[string]any{"i": int64(i)},
	}}
}

func c854Funcs() ListCellFuncs {
	return ListCellFuncs{
		Decode: func(raw []byte) (*unstructured.Unstructured, error) {
			var m map[string]any
			if err := json.Unmarshal(raw, &m); err != nil {
				return nil, err
			}
			return &unstructured.Unstructured{Object: m}, nil
		},
		Match: func([]byte) (bool, bool) { return true, true },
		Marshal: func(raws [][]byte) ([]byte, error) {
			out := []byte(`{"apiVersion":"c854.krateo.io/v1","items":[`)
			for i, r := range raws {
				if i > 0 {
					out = append(out, ',')
				}
				out = append(out, r...)
			}
			return append(out, `],"kind":"ThingList"}`...), nil
		},
	}
}

// c854Anchor builds a watcher with n objects and anchors a cell over them.
func c854Anchor(t *testing.T, n int) (*ResourceWatcher, *ListCell) {
	t.Helper()
	rw, c, _ := c854AnchorWith(t, n, c854Funcs(), nil)
	return rw, c
}

// c854AnchorWith is c854Anchor with the cell's funcs, a label set on every
// seeded object, and the dynamic client (for real writes through the informer).
func c854AnchorWith(t *testing.T, n int, f ListCellFuncs, labels map[string]string) (*ResourceWatcher, *ListCell, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	objs := make([]*unstructured.Unstructured, n)
	for i := range objs {
		objs[i] = c854Obj(i)
		if labels != nil {
			objs[i].SetLabels(labels)
		}
	}
	return c854AnchorObjs(t, objs, f)
}

// c854AnchorObjs anchors a cell over the given objects (namespace c854).
func c854AnchorObjs(t *testing.T, objs []*unstructured.Unstructured, f ListCellFuncs) (*ResourceWatcher, *ListCell, *dynamicfake.FakeDynamicClient) {
	t.Helper()
	n := len(objs)
	t.Setenv("CACHE_ENABLED", "true")
	ResetListCellsForTest()
	t.Cleanup(ResetListCellsForTest)
	var seed []runtime.Object
	for _, o := range objs {
		seed = append(seed, o)
	}
	kinds := rbacListKinds()
	kinds[c854GVR] = "ThingList"
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(rbacScheme(t), kinds, seed...)
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rw.Stop)
	_, syncCh := rw.EnsureResourceType(c854GVR)
	select {
	case <-syncCh:
	case <-time.After(5 * time.Minute):
		t.Fatal("informer did not sync")
	}
	prev := Global()
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(prev) })
	deadline := time.Now().Add(10 * time.Second)
	for !rw.IsServable(c854GVR) {
		if time.Now().After(deadline) {
			t.Fatal("GVR never servable")
		}
		time.Sleep(5 * time.Millisecond)
	}
	keyed, ok := rw.listRawKeyedServable(c854GVR, "c854")
	if !ok || len(keyed) != n {
		t.Fatalf("listing: ok=%v n=%d", ok, len(keyed))
	}
	keyed, _ = (&ListCell{funcs: f}).filter(keyed)
	raws := make([][]byte, len(keyed))
	items := make([]*unstructured.Unstructured, len(keyed))
	for i, kr := range keyed {
		raws[i] = kr.raw
		items[i], _ = f.Decode(kr.raw)
	}
	env, _ := f.Marshal(raws)
	c := AnchorListCell(ListCellAnchor{Key: "c854-key", GVR: c854GVR, Namespace: "c854", Envelope: env, Items: items,
		APIVersion: "c854.krateo.io/v1", Kind: "ThingList", Funcs: f})
	if c == nil {
		t.Fatal("PRECONDITION: the cell did not anchor")
	}
	return rw, c, dyn
}

func (rw *ResourceWatcher) c854Indexer(t *testing.T) interface {
	Update(obj interface{}) error
	GetByKey(key string) (interface{}, bool, error)
} {
	t.Helper()
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	return rw.informers[c854GVR].Informer().GetIndexer()
}

func TestListCell854_C6_ByteEqualRedeliveryRebindsWithoutDecode(t *testing.T) {
	rw, c := c854Anchor(t, 40)
	idx := rw.c854Indexer(t)
	key := listObjKey{ns: "c854", name: "t-0007"}
	old, _, _ := idx.GetByKey("c854/t-0007")
	oldRaw, _ := storeItemJSON(old)
	// A relist that changed nothing: a NEW bytesObject with the same bytes.
	fresh, err := newBytesObjectFromRaw(append([]byte(nil), oldRaw...))
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Update(fresh); err != nil {
		t.Fatal(err)
	}
	newRaw, _ := storeItemJSON(fresh)
	if idOf(newRaw) == idOf(oldRaw) {
		t.Fatal("PRECONDITION: the re-delivery reuses the old slice")
	}
	decodes0 := listCellStats.decodes.Load()
	noteListCellsPending(c854GVR, key.ns, key.name)
	if _, ok := c.Snapshot(DepGenEpochNow() + 1); !ok {
		t.Fatal("unanchored")
	}
	g := c.gen.Load()
	ci, si, found := locate(g.body.chunks, key)
	if !found {
		t.Fatal("slot missing")
	}
	s := g.body.chunks[ci].slots[si]
	if idOf(s.raw) != idOf(newRaw) || s.objSrc != idOf(newRaw) {
		t.Fatal("C6 RED: the slot still references the OLD indexer slice — every byte-equal re-delivery would pin a second copy of the raw bytes")
	}
	if listCellStats.decodes.Load() != decodes0 {
		t.Fatal("C6: a byte-equal re-delivery re-decoded the object")
	}
}

func TestListCell854_R0_SameObjectRedeliveryKeepsTheBody(t *testing.T) {
	_, c := c854Anchor(t, 40)
	b0 := c.gen.Load().body
	gens0, decodes0 := listCellStats.generations.Load(), listCellStats.decodes.Load()
	for i := 0; i < 40; i++ { // a resync re-delivers every object unchanged
		noteListCellsPending(c854GVR, "c854", fmt.Sprintf("t-%04d", i))
	}
	if _, ok := c.Snapshot(DepGenEpochNow() + 1); !ok {
		t.Fatal("unanchored")
	}
	if c.gen.Load().body != b0 {
		t.Fatal("R0 RED: a resync of unchanged objects built a new body — every resync would move every served cell's identity")
	}
	if listCellStats.generations.Load() != gens0 || listCellStats.decodes.Load() != decodes0 {
		t.Fatal("R0: a resync counted a generation or a decode")
	}
}

func TestListCell854_CH_OneChangeCopiesOneChunk(t *testing.T) {
	const n = 2500
	rw, c := c854Anchor(t, n)
	chunk := c.ChunkSizeForTest()
	if chunk < 50 || chunk > 51 {
		t.Fatalf("PRECONDITION: chunk size %d for %d slots, want ⌈√n⌉ = 50", chunk, n)
	}
	idx := rw.c854Indexer(t)
	o := c854Obj(1234)
	o.Object["spec"] = map[string]any{"i": int64(-1)}
	bo, err := newBytesObject(o)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Update(bo); err != nil {
		t.Fatal(err)
	}
	b0 := c.gen.Load().body
	max0 := listCellStats.genAllocMax.Load()
	listCellStats.genAllocMax.Store(0)
	t.Cleanup(func() { listCellStats.genAllocMax.Store(max0) })
	noteListCellsPending(c854GVR, "c854", "t-1234")
	if _, ok := c.Snapshot(DepGenEpochNow() + 1); !ok {
		t.Fatal("unanchored")
	}
	b1 := c.gen.Load().body
	shared := 0
	for i := range b1.chunks {
		if i < len(b0.chunks) && b1.chunks[i] == b0.chunks[i] {
			shared++
		}
	}
	if shared != len(b0.chunks)-1 {
		t.Fatalf("CH RED: %d of %d chunks shared after one change, want all but one — a change must copy one chunk, not the table", shared, len(b0.chunks))
	}
	if got, bound := listCellStats.genAllocMax.Load(), int64(4*chunk)*slotBytes; got > bound {
		t.Fatalf("CH: one generation allocated %d bytes, want ≤ %d (≈ one chunk + the chunk list)", got, bound)
	}
}
