// process_reset_fortest_test.go — #471: the behavioural counterpart to
// process_reset_guard_test.go.
//
// The AST guard proves ResetCacheProcessStateForTest CALLS the hooks. It cannot
// prove a call actually clears anything — a hook whose body stopped working
// would keep the guard green. These arms read the state back.
//
// They also cover the two sub-claims #471 rests on, in the only places they are
// observable: the sliceability memo now goes with a resolved-cache teardown
// (which is what makes the 34 widgets/apiref call sites -count-safe without
// touching any of them), and ResetCacheProcessStateForTest is safe to call
// twice — a harness in t.Cleanup must not care whether setup already ran it.

package cache

import (
	"testing"
	"time"
)

// memoProbe returns an raKey/shape pair distinct per arm, so these tests cannot
// address each other's verdicts (#471's own lesson).
func memoProbe(name string) (raKey, shape string) {
	return "ra-471-" + name, "shape-471-" + name
}

func TestIssue471_ResetResolvedCacheDropsTheSliceabilityMemo(t *testing.T) {
	raKey, shape := memoProbe("resolved-drops-memo")

	RecordSliceability(raKey, shape, true)
	if sliceable, known := SliceabilityLookup(raKey, shape); !known || !sliceable {
		t.Fatalf("precondition: want a recorded sliceable verdict, got sliceable=%t known=%t",
			sliceable, known)
	}

	// The narrow, mid-test-safe reset — the one the apiref harnesses call.
	ResetResolvedCacheForTest()

	if sliceable, known := SliceabilityLookup(raKey, shape); known {
		t.Errorf("after ResetResolvedCacheForTest the verdict must be UNKNOWN (re-verifiable), "+
			"got sliceable=%t known=%t. This is #471: a known verdict surviving the store it "+
			"describes makes the next first-sight serve skip its byte-verify.", sliceable, known)
	}
}

func TestIssue471_ResetCacheProcessStateClearsEachComposedConcern(t *testing.T) {
	raKey, shape := memoProbe("process-state")

	// Dirty one observable thing per composed concern that has a reader.
	RecordSliceability(raKey, shape, true)
	BumpRBACGenForTest()
	PublishRBACSnapshotForTest(&RBACSnapshot{})

	if RBACGen() == 0 {
		t.Fatalf("precondition: RBACGen must be non-zero after BumpRBACGenForTest")
	}
	if LiveRBACSnapshot() == nil {
		t.Fatalf("precondition: a snapshot must be published after PublishRBACSnapshotForTest")
	}
	if _, known := SliceabilityLookup(raKey, shape); !known {
		t.Fatalf("precondition: the memo verdict must be known after RecordSliceability")
	}

	if err := ResetCacheProcessStateForTest(); err != nil {
		t.Fatalf("ResetCacheProcessStateForTest: %v", err)
	}

	if _, known := SliceabilityLookup(raKey, shape); known {
		t.Errorf("sliceability memo: verdict still known after the process reset")
	}
	if got := RBACGen(); got != 0 {
		t.Errorf("RBACGen: got %d, want 0 after the process reset", got)
	}
	if LiveRBACSnapshot() != nil {
		t.Errorf("rbacSnap: a snapshot is still published after the process reset")
	}
}

func TestIssue471_ResetCacheProcessStateIsIdempotent(t *testing.T) {
	// A harness that resets at setup AND in t.Cleanup must not care, and
	// neither call may trip over a nil singleton it is the first to see.
	for i := 0; i < 3; i++ {
		if err := ResetCacheProcessStateForTest(); err != nil {
			t.Fatalf("call %d: %v", i+1, err)
		}
	}
}

func TestIssue471_WaitRBACRebuildQuiesceReturnsWhenQuiet(t *testing.T) {
	// Nothing in this arm schedules a rebuild, so the quiesce must return nil
	// immediately — including at a zero timeout, where the poll loop body never
	// runs and only the post-deadline re-check can answer. A quiesce that only
	// succeeded by sleeping would fail this.
	if err := WaitRBACRebuildQuiesceForTest(0); err != nil {
		t.Errorf("quiesce at timeout=0 on an idle process: %v", err)
	}
	if err := WaitRBACRebuildQuiesceForTest(time.Second); err != nil {
		t.Errorf("quiesce on an idle process: %v", err)
	}
}
