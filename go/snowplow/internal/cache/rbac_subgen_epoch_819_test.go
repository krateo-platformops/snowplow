package cache

// rbac_subgen_epoch_819_test.go — #819 part 1: the #682 boot epoch settles
// before Ready, on every readiness path.
//
// Until #1458 the readiness flip itself (MarkPhase1Done) ran the first
// bindings-index BUILD, and with it the bump. There is no build any more: the
// index is marked READY by the initial-publish goroutine, once the RBAC
// handlers have been delivered their initial lists (markBindingsIndexReady),
// and readiness waits for that goroutine to return (#1231;
// TestIssue1458_ReadinessWaitsForTheIndexToBeReady drives that end to end).
// The flip only gives the index its navigated set.
//
//   R1  window edit, index not ready → the ready transition bumps the epoch to
//       1 and rotates the identity-bound key; the readiness flip alone does
//       neither, and is counted.
//   R2  control: no edit in the window → ready, epoch stays 0 (the transition
//       never invents a rotation).
//   R3  a later readiness call, a later pass and a repeated ready never bump
//       again.
//   R4  the flip before ready does not make the index ready, and the navigated
//       set it gave is in force when ready comes.
//   S1  the epoch bump and an RBAC publish each close the identity-class
//       signal armed /refreshes connections wait on (part 2's trigger).

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// global819 installs rw as Global() for the test and resets the readiness flag.
func global819(t *testing.T, rw *ResourceWatcher) {
	t.Helper()
	prev := Global()
	SetGlobal(rw)
	ResetPhase1DoneForTest()
	t.Cleanup(func() {
		SetGlobal(prev)
		ResetPhase1DoneForTest()
	})
}

func TestIssue819_R1_ReadySettlesTheEpoch_TheFlipAloneDoesNot(t *testing.T) {
	rw := newSnapshotTestWatcher(t)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	reset682(t)
	global819(t, rw)
	BumpRBACGenForTest() // the first snapshot is published

	roleHandler682.OnUpdate(role682(noRule682), role682(cmRule682)) // an edit in the window
	before := identityKeys682()
	setBefore := ReadySettleStatsSnapshot().NavigatedSetAtFlip
	if BindingsByGVRIndexBuilt() || RBACSubGenEpoch() != 0 {
		t.Fatal("precondition: the index must not be ready and the epoch 0 before the flip")
	}

	MarkPhase1Done()

	if !IsPhase1Done() {
		t.Fatal("R1: MarkPhase1Done did not flip readiness")
	}
	if got := ReadySettleStatsSnapshot().NavigatedSetAtFlip - setBefore; got != 1 {
		t.Fatalf("R1: navigated_set_at_flip moved by %d, want 1", got)
	}
	if BindingsByGVRIndexBuilt() || RBACSubGenEpoch() != 0 {
		t.Fatalf("R1: the readiness flip made the index ready (%v) or rotated the epoch (%d): only the delivery of the "+
			"RBAC handlers' initial lists may (#1458)", BindingsByGVRIndexBuilt(), RBACSubGenEpoch())
	}

	markBindingsIndexReady() // what the initial-publish goroutine runs before readiness is released

	if !BindingsByGVRIndexBuilt() {
		t.Fatal("R1: markBindingsIndexReady did not make the index ready")
	}
	if got := RBACSubGenEpoch(); got != 1 {
		t.Fatalf("R1 RED: epoch=%d at ready, want 1 — the window edit's rotation must land there, before Ready", got)
	}
	after := identityKeys682()
	if before["restactions"] == after["restactions"] {
		t.Fatal("R1: the identity-bound key did not rotate at ready — #682's guarantee was weakened")
	}
}

func TestIssue819_R2_NoWindowEdit_ReadyWithoutRotating(t *testing.T) {
	rw := newSnapshotTestWatcher(t)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	reset682(t)
	global819(t, rw)
	BumpRBACGenForTest()

	MarkPhase1Done()
	markBindingsIndexReady()

	if !BindingsByGVRIndexBuilt() {
		t.Fatal("R2: the index is not ready")
	}
	if got := RBACSubGenEpoch(); got != 0 {
		t.Fatalf("R2: epoch=%d with no edit in the window — ready rotated every key for nothing", got)
	}
}

func TestIssue819_R3_LaterFlipsPassesAndReadiesNeverBump(t *testing.T) {
	rw := newSnapshotTestWatcher(t)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	reset682(t)
	global819(t, rw)
	BumpRBACGenForTest()
	roleHandler682.OnUpdate(role682(noRule682), role682(cmRule682))

	MarkPhase1Done()
	markBindingsIndexReady()
	// An edit after ready rotates through the per-subject path, never the
	// epoch; nor do a second flip, a prewarm pass's index step or a repeated
	// ready.
	roleHandler682.OnUpdate(role682(cmRule682), role682(noRule682))
	MarkPhase1Done()
	SetNavigatedGVRs([]schema.GroupVersionResource{{Version: "v1", Resource: "configmaps"}})
	markBindingsIndexReady()
	if got := RBACSubGenEpoch(); got != 1 {
		t.Fatalf("R3: epoch=%d, want exactly 1 — a later flip, pass or ready bumped again", got)
	}
}

func TestIssue819_R4_FlipBeforeReady_NavigatedSetIsInForceAtReady(t *testing.T) {
	rw := newSnapshotTestWatcher(t)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	reset682(t)
	global819(t, rw)
	rbacSnap.Store(nil) // no snapshot yet at the flip

	MarkPhase1Done()

	if BindingsByGVRIndexBuilt() {
		t.Fatal("R4: the readiness flip made the index ready over no snapshot (#686)")
	}
	idx := bindingsByGVRSingleton()
	idx.mu.RLock()
	navigated := len(idx.navigated)
	idx.mu.RUnlock()
	if want := len(rw.RegisteredGVRs()); navigated != want || want == 0 {
		t.Fatalf("R4: the flip gave the index %d navigated GVRs, the watcher has %d registered", navigated, want)
	}
	rbacSnap.Store(&RBACSnapshot{})
	markBindingsIndexReady()
	if !BindingsByGVRIndexBuilt() {
		t.Fatal("R4: the index is not ready after markBindingsIndexReady")
	}
	idx.mu.RLock()
	after := len(idx.navigated)
	idx.mu.RUnlock()
	if after != navigated {
		t.Fatalf("R4: ready changed the navigated set from %d to %d GVRs", navigated, after)
	}
}

func TestIssue819_S1_EpochBumpAndPublishSignalIdentityClassMove(t *testing.T) {
	rw := newSnapshotTestWatcher(t)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	reset682(t)
	BumpRBACGenForTest()

	ch := IdentityClassesMayHaveMoved()
	roleHandler682.OnUpdate(role682(noRule682), role682(cmRule682))
	markBindingsIndexReady() // the epoch bump
	select {
	case <-ch:
	default:
		t.Fatal("S1: the epoch bump did not signal an identity-class move")
	}

	ch = IdentityClassesMayHaveMoved()
	select {
	case <-ch:
		t.Fatal("S1: a re-acquired signal is already closed — every waiter would spin")
	default:
	}
	RebuildRBACSnapshotForTest(rw) // a publish
	select {
	case <-ch:
	default:
		t.Fatal("S1: an RBAC snapshot publish did not signal an identity-class move")
	}
}
