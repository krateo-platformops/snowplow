// rbac_shift_accumulator.go — #258. The engine-side MERGE-ACCUMULATOR for
// RBAC-shift reseed scopes (arch ruling: the accumulator is an engine-held field,
// the prewarmScope stays PAYLOAD-FREE and coalesces on scope identity). subjectKey
// is cache-internal, so the accumulator — which must UNION rotated sets across
// flushes — lives here and exposes Merge/Drain/Len to the dispatchers engine.
//
// LIFECYCLE (TL's in-flight + pending pair, ≤2 reseeds per burst):
//   - the RBAC-shift hook MERGEs each flush's RotatedSubjectSet into the pending
//     set (union) and enqueues the payload-free "rbac-shift" scope (coalesces);
//   - the handler DRAINs (take-and-reset) at the start of each run and reseeds
//     exactly the drained subjects;
//   - a rotation arriving mid-reseed Merges into the FRESH post-drain set and the
//     hook's enqueue marks the already-processing scope dirty (client-go), so the
//     worker re-runs it exactly once after the current reseed — never lost, never
//     more than one queued. Union-then-drain mirrors pendingSubGenBumps.

package cache

import "sync"

// RBACShiftAccumulator unions rotated-subject sets across flushes and drains them
// atomically. Safe for concurrent Merge (flush goroutine) + Drain (engine
// worker). Construct with NewRBACShiftAccumulator.
type RBACShiftAccumulator struct {
	mu  sync.Mutex
	set map[subjectKey]subGenBumpSource
}

// NewRBACShiftAccumulator returns an empty accumulator.
func NewRBACShiftAccumulator() *RBACShiftAccumulator {
	return &RBACShiftAccumulator{set: map[subjectKey]subGenBumpSource{}}
}

// Merge unions a flush's rotated subjects (OR-ing their source masks, so the
// widening tag carried by any contributing flush survives) into the pending set.
// No-op on an empty set. O(len(r)); called on the flush goroutine via the hook.
func (a *RBACShiftAccumulator) Merge(r RotatedSubjectSet) {
	if len(r.set) == 0 {
		return
	}
	a.mu.Lock()
	for s, m := range r.set {
		a.set[s] |= m
	}
	a.mu.Unlock()
}

// Drain atomically takes the pending set and resets it to empty (take-and-reset),
// returning it as a RotatedSubjectSet for the handler to reseed. A rotation that
// arrives after this returns lands in the fresh empty set; the hook's enqueue
// re-arms the scope so the worker re-runs and drains it next.
func (a *RBACShiftAccumulator) Drain() RotatedSubjectSet {
	a.mu.Lock()
	taken := a.set
	a.set = map[subjectKey]subGenBumpSource{}
	a.mu.Unlock()
	return RotatedSubjectSet{set: taken}
}

// Len reports the pending subject count (telemetry / tests).
func (a *RBACShiftAccumulator) Len() int {
	a.mu.Lock()
	n := len(a.set)
	a.mu.Unlock()
	return n
}
