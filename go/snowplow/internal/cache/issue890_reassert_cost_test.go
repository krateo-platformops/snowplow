//go:build unit || integration

// issue890_reassert_cost_test.go — #892 re-review B1 / F-a: the accepted write's
// re-assert asks for the EXACT edge (a recorded LIST edge must not mask a missing
// exact one), and costs nothing when no strip touched the key since the resolve
// recorded under it.

package cache

import (
	"context"
	"testing"
)

// TestIssue890_ListEdgeDoesNotMaskAMissingExactEdge — F-a. R2 records an exact
// dep and the LIST of its namespace under K. A strip takes both; another resolve
// of K re-records only the LIST. R2's accepted write must restore the exact edge:
// the fan-out view (a LIST edge "matches" every coordinate of the namespace) says
// it is there, the exact index says it is not.
func TestIssue890_ListEdgeDoesNotMaskAMissingExactEdge(t *testing.T) {
	d, store, _ := setup890(t)
	const k = "L1_890_fa"
	exact := DepKey{GVR: gvr883Hot, Namespace: "ns-fa", Name: "obj-1"}

	store.put(k, &ResolvedEntry{RawJSON: []byte(`{}`)})
	old, _ := store.GetNoTouch(k)
	store.EvictUnrefreshable(k, old)
	ctxR2 := WithL1KeyContext(context.Background(), k)
	gen := store.CaptureGen(k)
	d.Record(ctxR2, k, exact.GVR, exact.Namespace, exact.Name)
	d.RecordList(ctxR2, k, exact.GVR, exact.Namespace)

	d.RemoveL1Key(k)                                                                       // a strip (a concurrent refusal / release)
	d.RecordList(WithL1KeyContext(context.Background(), k), k, exact.GVR, exact.Namespace) // another resolve: LIST only

	if !store.putIfGen(ctxR2, k, &ResolvedEntry{RawJSON: []byte(`{"r":2}`)}, gen) {
		t.Fatal("harness: R2 must be stored")
	}
	if !d.hasExactEdge(k, exact) {
		t.Fatal("#890 F-a RED: the exact edge R2 recorded was not restored — the LIST edge masked it")
	}
}

// TestIssue890_ReassertCostsNothingWithoutAStrip — B1. With no strip between the
// resolve's records and its accepted write, the re-assert never checks an edge;
// with one, it checks (once) and restores.
func TestIssue890_ReassertCostsNothingWithoutAStrip(t *testing.T) {
	d, store, _ := setup890(t)
	dep := DepKey{GVR: gvr883Calm, Namespace: "ns-cost", Name: "q"}

	const k = "L1_890_nostrip"
	ctx := WithL1KeyContext(context.Background(), k)
	d.Record(ctx, k, dep.GVR, dep.Namespace, dep.Name)
	before := d.reassertCheckedTotal.Load()
	for i := 0; i < 50; i++ {
		if !store.putIfGen(ctx, k, &ResolvedEntry{RawJSON: []byte(`{}`)}, store.CaptureGen(k)) {
			t.Fatal("harness: the write must be stored")
		}
	}
	if got := d.reassertCheckedTotal.Load() - before; got != 0 {
		t.Fatalf("#890 B1 RED: %d of 50 accepted writes checked their edges although no strip touched the key", got)
	}

	// FU-1 — the refresher shape: the key is resident and its edges already
	// exist, so every record of the re-resolve is a dedup. With no strip, its
	// accepted write must still make zero full checks.
	const kr = "L1_890_refresh"
	first := WithL1KeyContext(context.Background(), kr)
	d.Record(first, kr, dep.GVR, dep.Namespace, dep.Name)
	d.RecordList(first, kr, dep.GVR, dep.Namespace)
	if !store.putIfGen(first, kr, &ResolvedEntry{RawJSON: []byte(`{}`)}, store.CaptureGen(kr)) {
		t.Fatal("harness: the first fill must be stored")
	}
	before = d.reassertCheckedTotal.Load()
	for i := 0; i < 20; i++ {
		re := WithL1KeyContext(context.Background(), kr) // a refresher re-resolve
		gen := store.CaptureGen(kr)
		d.Record(re, kr, dep.GVR, dep.Namespace, dep.Name) // dedup
		d.RecordList(re, kr, dep.GVR, dep.Namespace)       // dedup
		if !store.replaceIfGen(re, kr, &ResolvedEntry{RawJSON: []byte(`{"r":1}`)}, gen) {
			t.Fatal("harness: the refresh must be stored")
		}
	}
	if got := d.reassertCheckedTotal.Load() - before; got != 0 {
		t.Fatalf("#890 FU-1 RED: %d of 20 refresher-shaped writes (every record a dedup, no strip) took the full check", got)
	}

	const k2 = "L1_890_strip"
	ctx2 := WithL1KeyContext(context.Background(), k2)
	d.Record(ctx2, k2, dep.GVR, dep.Namespace, dep.Name)
	d.RemoveL1Key(k2)
	before = d.reassertCheckedTotal.Load()
	if !store.putIfGen(ctx2, k2, &ResolvedEntry{RawJSON: []byte(`{}`)}, store.CaptureGen(k2)) {
		t.Fatal("harness: the write must be stored")
	}
	if got := d.reassertCheckedTotal.Load() - before; got != 1 {
		t.Fatalf("after a strip the accepted write must check once, checked %d", got)
	}
	if !d.hasExactEdge(k2, dep) {
		t.Fatal("the stripped edge was not restored")
	}
}
