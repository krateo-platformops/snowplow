// list_cell_854_anchor_test.go — #854: the anchor's pairing proof and the
// #643 served-cell identity of an incremental cell.
//
//	F1r  an event that lands between the fill's dispatch and the anchor's
//	     listing: the fill's items no longer match the indexer, so the anchor
//	     is REFUSED (anchor_raced) and the entry stays a plain fill — never a
//	     cell that claims a stamp over an item decoded from older bytes.
//	F7b  #643 PR-B: a served incremental cell's identity is its Put AND its body.
//	     A change applied without a Put must read as "changed" to a
//	     declined-external mark taken over the cell, or the mark holds forever.
//	F1m  the informer's stored objects carry no managedFields (the decoder's
//	     stripManagedFields is a second guard, not the only one): measured here
//	     so the equivalent-mutant claim for it is evidence, not assertion.
package api

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/snowplow/internal/cache"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestListCell854_F1r_EventBetweenFillAndAnchorRefusesTheAnchor(t *testing.T) {
	dyn := newInc854Watcher(t, 5)
	store := cache.ResolvedCache()
	orig := dispatchViaInformerFn
	t.Cleanup(func() { dispatchViaInformerFn = orig })
	var once sync.Once
	dispatchViaInformerFn = func(ctx context.Context, call httpcall.RequestOptions) ([]byte, bool) {
		raw, ok := orig(ctx, call)
		once.Do(func() {
			// The fill has read the informer; a change lands before its anchor.
			before := cache.DepGenEpochNow()
			if _, err := dyn.Resource(r1FsaGVR).Namespace(r1Ns).Update(context.Background(), fill710FSA(1, 9), metav1.UpdateOptions{}); err != nil {
				t.Error(err)
			}
			if !fill710WaitFor(10*time.Second, func() bool { return cache.ContentCoordinateMovedSince(r1FsaGVR, r1Ns, "", before) }) {
				t.Error("the racing update never reached the dep hook")
			}
		})
		return raw, ok
	}
	raced0 := cache.ListCellStatsSnapshot().Unanchors["anchor_raced"]
	if _, served, ok, _ := apistageContentServe(r1Ctx(), store, fill710ListCall(), false); !ok || !served {
		t.Fatal("the fill was not served")
	}
	e, ok := store.GetNoTouch(fill710ListKey())
	if !ok {
		t.Fatal("no entry stored")
	}
	if e.ListCell() != nil && e.ListCell().Anchored() {
		v, _ := e.ListCell().Snapshot(cache.DepGenEpochNow())
		t.Fatalf("F1r RED: the cell anchored over a fill an event raced (managed served=%d) — the pairing proof is gone", inc854Managed(v, stamp765Item))
	}
	if got := cache.ListCellStatsSnapshot().Unanchors["anchor_raced"] - raced0; got != 1 {
		t.Fatalf("F1r: unanchors{anchor_raced} moved by %d, want 1", got)
	}
}

func TestListCell854_F7b_ServedIdentityMovesWithTheBody(t *testing.T) {
	dyn := newInc854Watcher(t, 5)
	store := cache.ResolvedCache()
	key := fill710ListKey()
	c := inc854Anchor(t, store, fill710ListCall())
	seq, body, ok := store.CellServeIdentity(key)
	if !ok || body == 0 {
		t.Fatalf("PRECONDITION: served identity of an incremental cell: seq=%d body=%d ok=%v", seq, body, ok)
	}
	sc := []cache.ServedCell{{Key: key, PutSeq: seq, BodySeq: body}}
	if cache.ServedCellsChanged(sc) {
		t.Fatal("F7b: an unchanged cell reads as changed")
	}
	stamp765Change(t, dyn, 4)
	if _, ok := c.Snapshot(cache.DepGenEpochNow()); !ok {
		t.Fatal("unanchored")
	}
	if s2, _ := store.CellPutSeq(key); s2 != seq {
		t.Fatalf("PRECONDITION: the change was a Put (putSeq %d → %d)", seq, s2)
	}
	if !cache.ServedCellsChanged(sc) {
		t.Fatal("F7b RED (#643): a served incremental cell changed without a Put and still reads as unchanged — a declined-external mark over it would hold forever (I-1)")
	}
}

func TestListCell854_F1m_InformerObjectsCarryNoManagedFields(t *testing.T) {
	dyn := newInc854Watcher(t, 0)
	if _, err := dyn.Resource(r1FsaGVR).Namespace(r1Ns).Create(context.Background(), inc854Obj(r1Ns, "mf", 1, ""), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if !fill710WaitFor(10*time.Second, func() bool {
		raw, ok := dispatchViaInformer(cache.WithApistageContentResolve(r1Ctx()), fill710ListCall())
		return ok && bytes.Contains(raw, []byte(`"mf"`))
	}) {
		t.Fatal("the object never reached the informer")
	}
	raw, _ := dispatchViaInformer(cache.WithApistageContentResolve(r1Ctx()), fill710ListCall())
	if bytes.Contains(raw, []byte("managedFields")) {
		t.Fatal("F1m: the informer serves managedFields — the decoder's strip is load-bearing, and F1 must cover it")
	}
}
