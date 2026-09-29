// edge3_capture_test.go — F-INV: the edge-3 capture/replay primitive
// invariants (issue #277 + the C2 carrier).
//
//	[C1-A] ReplayEdges writes via Record/RecordList→recordInternal ONLY, so an
//	       ACTIVE capture observes a replay. A capture opened on a key returns
//	       exactly the edges replayed under it — proving the replay routes
//	       through the tap and not a direct index write (a direct write would
//	       be invisible to the capture and EndCapture would come back empty).
//	[C2-B / DECISION 1] the per-key capture registry is a STACK of buffers,
//	       re-entrancy + concurrency safe. Nested captures both observe an
//	       edge; concurrent Begin/End/Record on one key is race-free (-race).

package cache

import (
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func edge3GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "apps.krateo.io", Version: "v1", Resource: "compositions"}
}

func edge3ContainsDep(edges []DepKey, want DepKey) bool {
	for _, e := range edges {
		if e == want {
			return true
		}
	}
	return false
}

// TestEdge3_FINV_ReplayObservedByActiveCapture — [C1-A]. An open capture on a
// key must observe an edge REPLAYED under that key, for both an exact edge and
// a list edge. If ReplayEdges ever wrote the forward/reverse maps directly
// (bypassing recordInternal), the capture would come back empty — RED.
func TestEdge3_FINV_ReplayObservedByActiveCapture(t *testing.T) {
	resetDepsForTest()
	d := Deps()
	const l1 = "L1_finv_replay"
	g := edge3GVR()
	exact := DepKey{GVR: g, Namespace: "ns-a", Name: "obj-1"}
	list := DepKey{GVR: g, Namespace: "ns-b", Name: listWildcard}

	h := d.BeginCapture(l1)
	if got := d.activeCaptures.Load(); got != 1 {
		t.Fatalf("activeCaptures=%d after one BeginCapture, want 1", got)
	}
	d.ReplayEdges(l1,[]DepKey{exact, list})
	edges := d.EndCapture(l1,h)

	if got := d.activeCaptures.Load(); got != 0 {
		t.Fatalf("activeCaptures=%d after EndCapture, want 0", got)
	}
	if !edge3ContainsDep(edges, exact) {
		t.Fatalf("[C1-A] RED: the active capture did NOT observe the replayed EXACT edge — ReplayEdges bypassed recordInternal. got=%v", edges)
	}
	if !edge3ContainsDep(edges, list) {
		t.Fatalf("[C1-A] RED: the active capture did NOT observe the replayed LIST edge. got=%v", edges)
	}
	// The replay also actually recorded the edges (EdgesUnder sees them).
	if under := d.EdgesUnder(l1); !edge3ContainsDep(under, exact) || !edge3ContainsDep(under, list) {
		t.Fatalf("ReplayEdges did not record under the key: EdgesUnder=%v", under)
	}
}

// TestEdge3_FINV_TapBeforeDedup — [C2-A]. An edge the key ALREADY holds must
// still land in a later capture (the tap is before the idempotent dedup
// early-return). A memo's stored deps must be COMPLETE regardless of prior
// records.
func TestEdge3_FINV_TapBeforeDedup(t *testing.T) {
	resetDepsForTest()
	d := Deps()
	const l1 = "L1_finv_dedup"
	g := edge3GVR()
	e := DepKey{GVR: g, Namespace: "ns", Name: "pre-existing"}

	// Pre-record the edge OUTSIDE any capture — the key already holds it.
	d.Record(l1,e.GVR, e.Namespace, e.Name)

	// Now open a capture and re-record the SAME edge: the tap must observe it
	// even though recordInternal's dedup early-returns.
	h := d.BeginCapture(l1)
	d.Record(l1,e.GVR, e.Namespace, e.Name)
	edges := d.EndCapture(l1,h)
	if !edge3ContainsDep(edges, e) {
		t.Fatalf("[C2-A] RED: an idempotent re-Record of an already-held edge was NOT captured (tap runs after the dedup early-return). got=%v", edges)
	}
}

// TestEdge3_FINV_NestedCapturesBothObserve — [C2-B / DECISION 1]. A per-key
// STACK: an inner capture nested inside an outer one, both observe an edge
// recorded while both are open; each EndCapture returns by handle identity.
func TestEdge3_FINV_NestedCapturesBothObserve(t *testing.T) {
	resetDepsForTest()
	d := Deps()
	const l1 = "L1_finv_nested"
	g := edge3GVR()
	e := DepKey{GVR: g, Namespace: "ns", Name: "shared"}

	outer := d.BeginCapture(l1)
	inner := d.BeginCapture(l1)
	if got := d.activeCaptures.Load(); got != 2 {
		t.Fatalf("activeCaptures=%d with two open captures, want 2", got)
	}
	d.Record(l1,e.GVR, e.Namespace, e.Name)
	innerEdges := d.EndCapture(l1,inner)
	outerEdges := d.EndCapture(l1,outer)

	if !edge3ContainsDep(innerEdges, e) {
		t.Fatalf("[C2-B] RED: inner capture did not observe the edge: %v", innerEdges)
	}
	if !edge3ContainsDep(outerEdges, e) {
		t.Fatalf("[C2-B] RED: outer capture did not observe the edge (stack tapped only the top): %v", outerEdges)
	}
	if got := d.activeCaptures.Load(); got != 0 {
		t.Fatalf("activeCaptures=%d after both EndCapture, want 0", got)
	}
}

// TestEdge3_FINV_DoubleEndCaptureIsIdempotent — N1. The resolve.go safety-net
// ends a capture TWICE on one handle (storeMemo's EndCapture on the normal
// path, then the deferred EndCapture at function return). A second EndCapture on
// an already-removed handle must NOT decrement activeCaptures again (else an
// unbalanced gate would drive it negative and disable the tap process-wide) and
// must still return the observed edges. Pins that correct-by-construction
// property.
func TestEdge3_FINV_DoubleEndCaptureIsIdempotent(t *testing.T) {
	resetDepsForTest()
	d := Deps()
	const l1 = "L1_finv_double_end"
	g := edge3GVR()
	e := DepKey{GVR: g, Namespace: "ns", Name: "obj"}

	h := d.BeginCapture(l1)
	d.Record(l1,e.GVR, e.Namespace, e.Name)

	first := d.EndCapture(l1,h)
	if !edge3ContainsDep(first, e) {
		t.Fatalf("first EndCapture did not return the captured edge: %v", first)
	}
	if got := d.activeCaptures.Load(); got != 0 {
		t.Fatalf("activeCaptures=%d after the first EndCapture, want 0", got)
	}

	// Second EndCapture on the SAME (already-removed) handle — the safety-net
	// double-end.
	second := d.EndCapture(l1,h)
	if got := d.activeCaptures.Load(); got != 0 {
		t.Fatalf("N1 RED: a second EndCapture on an already-ended handle decremented activeCaptures to %d (want 0) — the storeMemo+deferred double-end would drive the gate negative and disable the capture tap", got)
	}
	if !edge3ContainsDep(second, e) {
		t.Fatalf("N1: the second EndCapture must still return the same observed edges, got %v", second)
	}
}

// TestEdge3_FINV_ConcurrentCapturesRace — [C2-B / DECISION 1] the -race arm:
// concurrent BeginCapture/Record/ReplayEdges/EndCapture on ONE key. Must be
// race-free and leave activeCaptures balanced at 0.
func TestEdge3_FINV_ConcurrentCapturesRace(t *testing.T) {
	resetDepsForTest()
	d := Deps()
	const l1 = "L1_finv_race"
	g := edge3GVR()
	const workers = 64

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			h := d.BeginCapture(l1)
			d.Record(l1,g, "ns", "obj")
			d.ReplayEdges(l1,[]DepKey{{GVR: g, Namespace: "ns", Name: listWildcard}})
			_ = d.EndCapture(l1,h)
		}(i)
	}
	wg.Wait()

	if got := d.activeCaptures.Load(); got != 0 {
		t.Fatalf("[C2-B] RED: activeCaptures=%d after balanced concurrent Begin/End, want 0 (a prune race dropped a decrement or a slot detach lost a capture)", got)
	}
}
