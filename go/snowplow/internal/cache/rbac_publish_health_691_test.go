package cache

// rbac_publish_health_691_test.go — #691: a STUCK RBAC snapshot rebuild is
// visible and distinguishable from a QUIET cluster.
//
//   S1  stuck: a rebuild that panics on every attempt → rebuild_panics_total
//       grows, unpublished_event_age grows (> 0 and increasing) and the
//       publish seq stays flat.
//   S2  quiet: no RBAC events → unpublished_event_age == 0 while the publish
//       age grows. RED under the "never clears" mutation.
//   S3  healthy: an event that is rebuilt returns unpublished_event_age to 0
//       and resets the publish age.
//   S4  a mid-rebuild event keeps its own (newer) stamp: when the running
//       rebuild completes, the gauge reflects the still-pending event, not 0.

import (
	"sync/atomic"
	"testing"
	"time"
)

func waitPublishHealthQuiet(t *testing.T) {
	t.Helper()
	waitRebuildQuiet(t)
	deadline := time.Now().Add(3 * time.Second)
	for RBACUnpublishedEventAge() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("PRE: unpublished_event_age never returned to 0 after the setup rebuilds (%v)", RBACUnpublishedEventAge())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestIssue691_S1_AlwaysPanickingRebuild_AgeGrows_SeqFlat(t *testing.T) {
	rw := newSnapshotTestWatcher(t, mkRB("ns-a", "viewer-bind", userSub("alice")), mkR("ns-a", "viewer-bind-role"))
	waitPublishHealthQuiet(t)
	panics0, seq0 := RBACRebuildPanicsTotal(), RBACGen()

	var stop atomic.Bool
	SetRebuildBarrierForTest(func() {
		if !stop.Load() {
			panic("#691 S1: deterministic rebuild panic")
		}
	})
	t.Cleanup(func() {
		stop.Store(true)
		SetRebuildBarrierForTest(nil)
		// Let the loop's backoff retry drain before the next test.
		deadline := time.Now().Add(10 * time.Second)
		for (rbacRebuildLock.Load() || rbacRebuildDirty.Load()) && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
	})

	scheduleRBACRebuild(rw) // a revoke arrives
	time.Sleep(250 * time.Millisecond)
	a1 := RBACUnpublishedEventAge()
	time.Sleep(100 * time.Millisecond)
	a2 := RBACUnpublishedEventAge()

	if d := RBACRebuildPanicsTotal() - panics0; d < 2 {
		t.Fatalf("S1 RED: rebuild_panics_total moved by %d, want >= 2 (it retries and panics again)", d)
	}
	if a1 < 200*time.Millisecond || a2 <= a1 {
		t.Fatalf("S1 RED: unpublished_event_age = %v then %v — a stuck rebuild must read > 0 and growing", a1, a2)
	}
	if RBACGen() != seq0 {
		t.Fatalf("S1: the publish seq moved (%d → %d) although every rebuild panicked", seq0, RBACGen())
	}
}

func TestIssue691_S2_QuietCluster_AgeZero_PublishAgeGrows(t *testing.T) {
	// PRE (#691 S2 flake, root-caused by a stack trace of every schedule and
	// publish under a CPU burner, https://github.com/krateo-platformops/snowplow/pull/692#issuecomment-6169101317):
	// the watcher's own boot work must be over before "quiet" is measured, and
	// waiting for it is FATAL on timeout so a real background publisher would
	// still turn this arm RED. Two boot sources publish:
	//   - the one-time INITIAL PUBLISH goroutine (waitAndPublishInitialRBACSnapshot,
	//     under the writer slot). This is the publish that landed inside the
	//     window (1/100 RED); waitRebuildQuiet did not wait for it because it
	//     had not taken the slot yet. rw.WaitRBACQuiescentForTest does.
	//   - the initial-LIST ADDs, each of which schedules a rebuild when it
	//     reaches the snapshot handler (delivered on the handler's own
	//     goroutine). Waited on by event: the seed is 2 objects, so 2
	//     delivered initial-list ADDs.
	var initialAdds atomic.Int32
	countInitialAdd := func() { initialAdds.Add(1) }
	rbacInitialListAddDeliveredHookForTest.Store(&countInitialAdd)
	t.Cleanup(func() { rbacInitialListAddDeliveredHookForTest.Store(nil) })
	rw := newSnapshotTestWatcher(t, mkRB("ns-a", "viewer-bind", userSub("alice")), mkR("ns-a", "viewer-bind-role"))
	if err := rw.WaitRBACQuiescentForTest(10 * time.Second); err != nil {
		t.Fatalf("PRE: %v", err)
	}
	addsDeadline := time.Now().Add(10 * time.Second)
	for initialAdds.Load() < 2 {
		if time.Now().After(addsDeadline) {
			t.Fatalf("PRE: only %d of the 2 seeded objects' initial-list ADDs reached the snapshot handler", initialAdds.Load())
		}
		time.Sleep(time.Millisecond)
	}
	waitRebuildQuiet(t)
	// The realistic quiet state: the LAST event was rebuilt and published,
	// then nothing more happens.
	seq0 := RBACGen()
	scheduleRBACRebuild(rw)
	deadline := time.Now().Add(3 * time.Second)
	for RBACGen() == seq0 || rbacRebuildLock.Load() || rbacRebuildDirty.Load() {
		if time.Now().After(deadline) {
			t.Fatal("PRE: the last event was never rebuilt")
		}
		time.Sleep(time.Millisecond)
	}
	p1 := RBACLastPublishAge()
	time.Sleep(150 * time.Millisecond)
	if a := RBACUnpublishedEventAge(); a != 0 {
		t.Fatalf("S2 RED: quiet cluster reads unpublished_event_age = %v, want 0 — quiet must not look stuck", a)
	}
	if p2 := RBACLastPublishAge(); p2 <= p1 || p2 < 150*time.Millisecond {
		t.Fatalf("S2: last_successful_publish_age did not grow on a quiet cluster (%v → %v)", p1, p2)
	}
}

func TestIssue691_S3_HealthyEvent_AgeReturnsToZero(t *testing.T) {
	rw := newSnapshotTestWatcher(t, mkRB("ns-a", "viewer-bind", userSub("alice")), mkR("ns-a", "viewer-bind-role"))
	waitPublishHealthQuiet(t)
	time.Sleep(50 * time.Millisecond)
	seq0 := RBACGen()
	scheduleRBACRebuild(rw)
	deadline := time.Now().Add(3 * time.Second)
	for RBACGen() == seq0 || RBACUnpublishedEventAge() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("S3 RED: after a healthy rebuild unpublished_event_age = %v (seq %d → %d)", RBACUnpublishedEventAge(), seq0, RBACGen())
		}
		time.Sleep(time.Millisecond)
	}
	if p := RBACLastPublishAge(); p > time.Second {
		t.Fatalf("S3: last_successful_publish_age = %v right after a publish", p)
	}
}

func TestIssue691_S4_MidRebuildEventKeepsItsStamp(t *testing.T) {
	rw := newSnapshotTestWatcher(t, mkRB("ns-a", "viewer-bind", userSub("alice")), mkR("ns-a", "viewer-bind-role"))
	waitPublishHealthQuiet(t)

	// The first rebuild takes the pending stamp; while it runs, a second event
	// arrives (fresh pending stamp) and that event's rebuild panics.
	var calls atomic.Int32
	release := make(chan struct{})
	SetRebuildBarrierForTest(func() {
		switch calls.Add(1) {
		case 1:
			<-release // first rebuild blocks here
		default:
			panic("#691 S4: the follow-up rebuild panics")
		}
	})
	t.Cleanup(func() {
		SetRebuildBarrierForTest(nil)
		deadline := time.Now().Add(10 * time.Second)
		for (rbacRebuildLock.Load() || rbacRebuildDirty.Load()) && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
	})
	scheduleRBACRebuild(rw)
	for calls.Load() < 1 {
		time.Sleep(time.Millisecond)
	}
	scheduleRBACRebuild(rw) // mid-rebuild event
	close(release)
	time.Sleep(150 * time.Millisecond)
	if a := RBACUnpublishedEventAge(); a < 100*time.Millisecond {
		t.Fatalf("S4 RED: the mid-rebuild event's age reads %v after its rebuild panicked — completing the FIRST rebuild must not clear a later event's stamp", a)
	}
}
