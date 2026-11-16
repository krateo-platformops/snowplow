// rbac_subgen_never_lowers_1440_test.go — #1440 follow-up: a subject's counter
// never goes back to an earlier RBAC state.
//
// BumpSubjectSubGens draws its state from the clock and THEN writes it into each
// subject. With two writers (the snapshot flush and the #682 epoch rotation run
// on different goroutines; two flushes never overlap in production, the rebuild
// slot serialises them) the writer that drew the EARLIER state can reach a
// shared counter LAST. raiseSubGenTo must then leave the later state in place:
// lowering it would hand the subject's holders a value an earlier state already
// named — the cross-time aliasing #1439 removed.
//
// Before these arms, replacing raiseSubGenTo's loop with a plain Store passed
// every test.
package cache

import (
	"sync"
	"sync/atomic"
	"testing"
)

// The interleaving, step by step and with no scheduling luck: writer A draws
// state 1, writer B draws state 2 and writes it, then A writes.
func TestRaiseSubGenTo_LateWriterWithAnEarlierStateDoesNotLower(t *testing.T) {
	ResetRBACSubGenForTest()
	t.Cleanup(ResetRBACSubGenForTest)

	shared := subjectKey{Kind: subjectKindGroup, Name: "shared"}
	earlier := nextRBACState() // writer A draws
	later := nextRBACState()   // writer B draws
	if earlier != 1 || later != 2 {
		t.Fatalf("precondition: the two draws must be states 1 and 2; got %d and %d", earlier, later)
	}
	raiseSubGenTo(subGenCounterFor(shared), later)   // B writes first
	raiseSubGenTo(subGenCounterFor(shared), earlier) // A writes last
	if got := subGenValue(shared); got != later {
		t.Fatalf("the shared subject holds state %d after a late write of the earlier state %d; want the later state %d",
			got, earlier, later)
	}
	if got := RBACSubGenForSubject("anyone", []string{"shared"}); got != later {
		t.Fatalf("a holder of the shared subject reads %d; want the later state %d", got, later)
	}

	// Same contract for the epoch term, which the rotation writes the same way.
	var epoch atomic.Uint64
	raiseSubGenTo(&epoch, 7)
	raiseSubGenTo(&epoch, 5)
	raiseSubGenTo(&epoch, 7)
	if got := epoch.Load(); got != 7 {
		t.Fatalf("a counter raised to 7, then 5, then 7 holds %d; want 7", got)
	}
}

// Two real bumpers sharing one subject, raced. Each also bumps a subject of its
// own, which records the state that bumper drew; the shared subject must end on
// the later of the two.
func TestBumpSubjectSubGens_TwoWritersLeaveTheLaterStateOnASharedSubject(t *testing.T) {
	ResetRBACSubGenForTest()
	t.Cleanup(ResetRBACSubGenForTest)

	shared := subjectKey{Kind: subjectKindGroup, Name: "shared"}
	ownA := subjectKey{Kind: subjectKindUser, Name: "writer-a"}
	ownB := subjectKey{Kind: subjectKindUser, Name: "writer-b"}
	const rounds = 20000
	lowered, aLater, bLater := 0, 0, 0
	for i := 0; i < rounds; i++ {
		var start, done sync.WaitGroup
		start.Add(1)
		done.Add(2)
		go func() { defer done.Done(); start.Wait(); BumpSubjectSubGens([]subjectKey{ownA, shared}) }()
		go func() { defer done.Done(); start.Wait(); BumpSubjectSubGens([]subjectKey{ownB, shared}) }()
		start.Done()
		done.Wait()
		a, b := subGenValue(ownA), subGenValue(ownB)
		if a > b {
			aLater++
		} else {
			bLater++
		}
		if got := subGenValue(shared); got != max(a, b) {
			lowered++
		}
	}
	if lowered > 0 {
		t.Errorf("the shared subject ended below the later of the two writers' states in %d of %d rounds", lowered, rounds)
	}
	// Each writer must have drawn the later state at least once, or the race
	// was never both ways.
	if aLater == 0 || bLater == 0 {
		t.Fatalf("VACUOUS: one writer always drew the later state (a later=%d, b later=%d)", aLater, bLater)
	}
}
