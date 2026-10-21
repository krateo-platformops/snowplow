package cache

// rbac_subgen_epoch_819_test.go — #819 part 1: the #682 boot epoch settles
// before Ready, on every readiness path, because the readiness flip itself
// (MarkPhase1Done) runs the first bindings-index build when nothing has yet.
//
//   R1  window edit, index unbuilt, Ready flips → the index is built and the
//       epoch is 1 when MarkPhase1Done returns. RED on main: the flip did not
//       build, so the bump waited for the engine's boot pass (bench run 10:
//       6.5 min after Ready).
//   R2  control: no edit in the window → built, epoch stays 0 (the settle
//       never invents a rotation).
//   R3  a later readiness call and a later rebuild never bump again.
//   R4  no complete snapshot at the flip → the build is requested, not run
//       over nothing; the first complete publish runs it.
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

func TestIssue819_R1_ReadinessFlipSettlesTheEpochBeforeReady(t *testing.T) {
	rw := newSnapshotTestWatcher(t)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	reset682(t)
	global819(t, rw)
	BumpRBACGenForTest() // the first snapshot is published

	roleHandler682.OnUpdate(role682(noRule682), role682(cmRule682)) // an edit in the window
	before := identityKeys682()
	builtBefore := ReadySettleStatsSnapshot().BuiltAtFlip
	if BindingsByGVRIndexBuilt() || RBACSubGenEpoch() != 0 {
		t.Fatal("precondition: the index must be unbuilt and the epoch 0 before the flip")
	}

	MarkPhase1Done()

	if !BindingsByGVRIndexBuilt() {
		t.Fatal("R1 RED: Ready flipped with the bindings index unbuilt — the #682 bump is still owed and will " +
			"rotate every user's keys under tabs armed after Ready (#819)")
	}
	if got := RBACSubGenEpoch(); got != 1 {
		t.Fatalf("R1 RED: epoch=%d when Ready flipped, want 1 — the window edit's rotation must land before Ready", got)
	}
	if !IsPhase1Done() {
		t.Fatal("R1: MarkPhase1Done did not flip readiness")
	}
	if got := ReadySettleStatsSnapshot().BuiltAtFlip - builtBefore; got != 1 {
		t.Fatalf("R1: built_at_flip moved by %d, want 1 — the settle that built before Ready must be counted", got)
	}
	after := identityKeys682()
	if before["restactions"] == after["restactions"] {
		t.Fatal("R1: the identity-bound key did not rotate at the settle — #682's guarantee was weakened")
	}
}

func TestIssue819_R2_NoWindowEdit_SettleBuildsWithoutRotating(t *testing.T) {
	rw := newSnapshotTestWatcher(t)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	reset682(t)
	global819(t, rw)
	BumpRBACGenForTest()

	MarkPhase1Done()

	if !BindingsByGVRIndexBuilt() {
		t.Fatal("R2: the readiness flip did not build the index")
	}
	if got := RBACSubGenEpoch(); got != 0 {
		t.Fatalf("R2: epoch=%d with no edit in the window — the settle rotated every key for nothing", got)
	}
}

func TestIssue819_R3_LaterFlipsAndRebuildsNeverBump(t *testing.T) {
	rw := newSnapshotTestWatcher(t)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	reset682(t)
	global819(t, rw)
	BumpRBACGenForTest()
	roleHandler682.OnUpdate(role682(noRule682), role682(cmRule682))

	MarkPhase1Done()
	// An edit after the settle rotates through the per-subject path, never the
	// epoch; nor do a second flip or the boot pass's own rebuild.
	roleHandler682.OnUpdate(role682(cmRule682), role682(noRule682))
	MarkPhase1Done()
	BuildBindingsByGVRIndex([]schema.GroupVersionResource{{Version: "v1", Resource: "configmaps"}})
	if got := RBACSubGenEpoch(); got != 1 {
		t.Fatalf("R3: epoch=%d, want exactly 1 — a later flip or rebuild bumped again", got)
	}
}

func TestIssue819_R4_NoCompleteSnapshot_BuildIsRequestedNotRunOverNothing(t *testing.T) {
	rw := newSnapshotTestWatcher(t)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	reset682(t)
	global819(t, rw)
	rbacSnap.Store(nil) // no snapshot yet at the flip

	MarkPhase1Done()

	if BindingsByGVRIndexBuilt() {
		t.Fatal("R4: the index claims to be built over no snapshot (#686)")
	}
	idx := bindingsByGVRSingleton()
	idx.mu.RLock()
	pending := idx.pendingNavigated
	idx.mu.RUnlock()
	if pending == nil {
		t.Fatal("R4: the flip neither built nor requested the build — the first publish would not run it")
	}
	rbacSnap.Store(&RBACSnapshot{})
	buildPendingBindingsIndex() // what the first complete publish runs
	if !BindingsByGVRIndexBuilt() {
		t.Fatal("R4: the requested build did not run at the first complete publish")
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
	BuildBindingsByGVRIndex(nil) // the epoch bump
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
