//go:build unit || integration

// issue890_dedup_skip_soundness_test.go — #892 review of 9068433b: the identity
// skip must stay sound when a resolve's own records are dedups of edges that
// already existed (the refresher shape, FU-1).

package cache

import (
	"context"
	"testing"
)

// TestIssue890_DedupFirstRecordThenStripKeepsTheEdge — probe (i). R2's first own
// record is a dedup of an existing edge (dk_a), then a strip removes the key's
// edge set, then R2 records a new dep (dk_b), which creates a new set. R2's write
// must keep dk_a: the dedup noted the old set, the new record found another, so
// the write takes the full check and restores dk_a.
func TestIssue890_DedupFirstRecordThenStripKeepsTheEdge(t *testing.T) {
	d, store, _ := setup890(t)
	const k = "L1_890_dedup_first"
	a := DepKey{GVR: gvr883Calm, Namespace: "ns-dd", Name: "a"}
	b := DepKey{GVR: gvr883Calm, Namespace: "ns-dd", Name: "b"}

	r1 := WithL1KeyContext(context.Background(), k)
	d.Record(r1, k, a.GVR, a.Namespace, a.Name)
	if !store.putIfGen(r1, k, &ResolvedEntry{RawJSON: []byte(`{}`)}, store.CaptureGen(k)) {
		t.Fatal("harness: R1 must be stored")
	}

	r2 := WithL1KeyContext(context.Background(), k)
	gen := store.CaptureGen(k)
	d.Record(r2, k, a.GVR, a.Namespace, a.Name) // a dedup: dk_a already recorded
	d.RemoveL1Key(k)                            // a strip
	d.Record(r2, k, b.GVR, b.Namespace, b.Name) // a new record: a new edge set
	if !store.putIfGen(r2, k, &ResolvedEntry{RawJSON: []byte(`{"r":2}`)}, gen) {
		t.Fatal("harness: R2 must be stored")
	}
	if !d.hasExactEdge(k, a) || !d.hasExactEdge(k, b) {
		t.Fatalf("#890 RED (dedup-first): R2's stored cell lost an edge its resolve recorded (a=%v b=%v)",
			d.hasExactEdge(k, a), d.hasExactEdge(k, b))
	}
}

// TestIssue890_DedupWithNoEdgeSetForcesTheFullCheck — probe (ii), the narrow
// window. R2's own record dedups while another record of the key (R1) sits
// between its forward insert and its reverse insert, so the key has no edge set
// to name. Then R1's reverse insert lands (set S0), a strip removes S0, and R2's
// next own record creates S1 and notes it. At R2's write the key's set IS the one
// R2 noted — but R2's dedup'd dep was in neither. The missing set at the dedup
// must have forced the full check.
//
// The mid-record state is set up directly on the indexes (in-package): a forward
// edge present with no reverse set is exactly what R1 leaves between its two
// inserts.
func TestIssue890_DedupWithNoEdgeSetForcesTheFullCheck(t *testing.T) {
	d, store, _ := setup890(t)
	const k = "L1_890_window"
	a := DepKey{GVR: gvr883Calm, Namespace: "ns-win", Name: "a"}
	b := DepKey{GVR: gvr883Calm, Namespace: "ns-win", Name: "b"}

	// R1 between its forward and reverse inserts of (k, a).
	d.Record(context.Background(), k, a.GVR, a.Namespace, a.Name)
	d.reverse.Delete(k)

	r2 := WithL1KeyContext(context.Background(), k)
	gen := store.CaptureGen(k)
	d.Record(r2, k, a.GVR, a.Namespace, a.Name) // a forward dedup with no edge set to name

	// R1's reverse insert lands: set S0 holding (k, a).
	s0 := &depSet{}
	s0.deps.Store(a, struct{}{})
	s0.count.Add(1)
	d.reverse.Store(k, s0)

	d.RemoveL1Key(k)                            // a strip removes S0 and the forward edge
	d.Record(r2, k, b.GVR, b.Namespace, b.Name) // R2's next own record creates and notes S1
	if !store.putIfGen(r2, k, &ResolvedEntry{RawJSON: []byte(`{"r":2}`)}, gen) {
		t.Fatal("harness: R2 must be stored")
	}
	if !d.hasExactEdge(k, a) {
		t.Fatal("#890 RED (no-set window): R2's stored cell lost the dep it dedup'd while the key had no edge set")
	}
}
