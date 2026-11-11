package cache

// refresh_arm_once_1325_test.go — #1325 review m1, at the sink: one arm writes
// at most one frame per key, whatever its source.
//
//	O1  the review's probe: an owed mark (#710), subscribe (the owed emit sinks
//	    the key), CatchUp([key]) → ONE frame (RED on ad657d95: 2).
//	O2  a publish that lands between subscribe and CatchUp, then a catch-up
//	    of the same key → one frame.
//	O3  scope: a catch-up of a key this arm did not offer is offered (one
//	    frame), and after CatchUp closed the arm, publishes are never
//	    absorbed (two publishes, consumed in between, are two frames).

import (
	"testing"
	"time"
)

// framesWithin1325 counts the frames that reach st within d.
func framesWithin1325(st RefreshStream, d time.Duration) int {
	n := 0
	deadline := time.After(d)
	for {
		select {
		case <-st.Keys:
			n++
		case <-deadline:
			return n
		}
	}
}

func TestIssue1325_O1_AnOwedEmitAndACatchUpOfOneKeyAreOneFrame(t *testing.T) {
	// #1233: this test writes the store under a complete snapshot.
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	ResetStaleOwedForTest()
	t.Cleanup(ResetStaleOwedForTest)
	MarkStaleOwed(e.key, []byte(`{"body":"stale-derived"}`)) // the cell holds the fresh replacement
	b0 := RefreshBroadcasterStatsSnapshot()
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	st.CatchUp([]string{e.key})
	if n := framesWithin1325(st, 200*time.Millisecond); n != 1 {
		t.Fatalf("O1 RED: %d frames for one key on one arm (owed emit + catch-up), want 1 (#1325 review m1)", n)
	}
	b1 := RefreshBroadcasterStatsSnapshot()
	if b1.ArmDuplicateAbsorbed-b0.ArmDuplicateAbsorbed != 1 || b1.ArmCatchUp != b0.ArmCatchUp {
		t.Fatalf("O1: arm_duplicate_absorbed Δ=%d arm_catch_up Δ=%d, want 1/0",
			b1.ArmDuplicateAbsorbed-b0.ArmDuplicateAbsorbed, b1.ArmCatchUp-b0.ArmCatchUp)
	}
}

func TestIssue1325_O2_APublishBeforeTheCatchUpIsTheArmsOneFrame(t *testing.T) {
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	PublishRefresh(e.key) // a change lands after subscribe, before BehindKeys ran
	st.CatchUp([]string{e.key})
	if n := framesWithin1325(st, 200*time.Millisecond); n != 1 {
		t.Fatalf("O2 RED: %d frames, want 1 — the published frame already makes the tab refetch bytes at least as new "+
			"as the catch-up compared", n)
	}
}

func TestIssue1325_O3_ACatchUpOfAnUnofferedKeyIsSent_AndTheClosedArmAbsorbsNothing(t *testing.T) {
	CompleteRBACSnapshotForTest(t)
	e := newK1062(t)
	st := SubscribeRefreshStream(map[string]struct{}{e.key: {}})
	defer st.Unsub()
	b0 := RefreshBroadcasterStatsSnapshot()
	st.CatchUp([]string{e.key})
	if n := framesWithin1325(st, 200*time.Millisecond); n != 1 {
		t.Fatalf("O3 RED: a catch-up of a key the arm did not offer reached the stream %d time(s), want 1", n)
	}
	if d := RefreshBroadcasterStatsSnapshot().ArmCatchUp - b0.ArmCatchUp; d != 1 {
		t.Fatalf("O3: arm_catch_up Δ=%d, want 1", d)
	}
	for i := 0; i < 2; i++ {
		PublishRefresh(e.key)
		if n := framesWithin1325(st, 200*time.Millisecond); n != 1 {
			t.Fatalf("O3 RED: publish %d after the arm closed reached the stream %d time(s), want 1 — only arm-time "+
				"offers are absorbed", i+1, n)
		}
	}
}
