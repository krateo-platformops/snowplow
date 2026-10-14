package cache

// rbac_rebuild_lost_wakeup_687_test.go — #687 (security): scheduleRBACRebuild
// must never lose a wakeup. An RBAC event that lands between the rebuild
// loop's final dirty check and its writer-slot release sets dirty and fails
// the CAS; the loop must still publish it.
//
//   W1  DETERMINISTIC: the exit hook (the exact #687 window) removes a
//       RoleBinding from the informer store WITHOUT an informer event and
//       calls scheduleRBACRebuild itself — the revoke's handler. A publish
//       without the binding must follow within a bound. RED on main: the
//       revoke stays unpublished.
//   W2  RACE: under -race, with a CPU burner, many concurrent schedulers;
//       at most ONE rebuild ever runs at a time, and the loop never ends with
//       dirty set and the slot free (no stuck wakeup).
//   W3  a pickup after release is counted.

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// waitRebuildQuiet waits until no rebuild is in flight and nothing is queued.
func waitRebuildQuiet(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if !rbacRebuildLock.Load() && !rbacRebuildDirty.Load() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("PRE: the RBAC rebuild never went quiet (lock=%v dirty=%v)", rbacRebuildLock.Load(), rbacRebuildDirty.Load())
}

func snapHasRB(ns, name string) bool {
	s := RBACSnapshotForTest()
	if s == nil {
		return false
	}
	for _, rb := range s.RoleBindingsByNS[ns] {
		if rb != nil && rb.Name == name {
			return true
		}
	}
	return false
}

func TestIssue687_W1_EventInExitWindow_IsPublished(t *testing.T) {
	rb := mkRB("ns-a", "viewer-bind", userSub("alice"))
	rw := newSnapshotTestWatcher(t, rb, mkR("ns-a", "viewer-bind-role"))
	waitRebuildQuiet(t)
	if !snapHasRB("ns-a", "viewer-bind") {
		t.Fatal("PRE: the binding is not in the published snapshot")
	}
	rw.mu.RLock()
	store := rw.informers[roleBindingsTypedGVR].Informer().GetIndexer()
	rw.mu.RUnlock()

	var once sync.Once
	SetRBACRebuildExitHookForTest(func() {
		once.Do(func() {
			// The revoke lands in the window: the store changes (no informer
			// event fires here) and its handler schedules a rebuild while the
			// running loop still holds the slot.
			obj, ok, _ := store.Get(rb)
			if !ok {
				t.Error("PRE: binding not in the informer store")
				return
			}
			if err := store.Delete(obj); err != nil {
				t.Errorf("store delete: %v", err)
				return
			}
			scheduleRBACRebuild(rw)
		})
	})
	t.Cleanup(func() { SetRBACRebuildExitHookForTest(nil) })

	pick0 := RBACRebuildPostReleasePickups()
	scheduleRBACRebuild(rw) // any rebuild: its loop exit hits the window
	deadline := time.Now().Add(3 * time.Second)
	for snapHasRB("ns-a", "viewer-bind") {
		if time.Now().After(deadline) {
			t.Fatalf("W1 RED (#687): a revoke that landed between the rebuild loop's final dirty check and its slot "+
				"release was never published (lock=%v dirty=%v) — the revoked subject keeps access until the next RBAC event",
				rbacRebuildLock.Load(), rbacRebuildDirty.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
	// W3 — the rescue is counted.
	if d := RBACRebuildPostReleasePickups() - pick0; d < 1 {
		t.Fatalf("W3: post_release_pickups moved by %d, want >= 1", d)
	}
}

// W4 — a rebuild that panics re-marks dirty and is retried by the loop itself
// (after a bounded backoff), without waiting for another RBAC event.
func TestIssue687_W4_PanickingRebuild_RetriesWithoutAnotherEvent(t *testing.T) {
	rb := mkRB("ns-a", "viewer-bind", userSub("alice"))
	rw := newSnapshotTestWatcher(t, rb, mkR("ns-a", "viewer-bind-role"))
	waitRebuildQuiet(t)
	rw.mu.RLock()
	store := rw.informers[roleBindingsTypedGVR].Informer().GetIndexer()
	rw.mu.RUnlock()
	obj, ok, _ := store.Get(rb)
	if !ok {
		t.Fatal("PRE: binding not in the store")
	}
	if err := store.Delete(obj); err != nil {
		t.Fatal(err)
	}
	var panics atomic.Int32
	SetRebuildBarrierForTest(func() {
		if panics.Add(1) == 1 {
			panic("#687 W4: synthetic rebuild panic")
		}
	})
	t.Cleanup(func() { SetRebuildBarrierForTest(nil) })
	scheduleRBACRebuild(rw)
	deadline := time.Now().Add(3 * time.Second)
	for snapHasRB("ns-a", "viewer-bind") {
		if time.Now().After(deadline) {
			t.Fatalf("W4 RED: after a rebuild panic the revoke was never published without another RBAC event (panics=%d)", panics.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func cpuBurner687(t *testing.T) {
	t.Helper()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2*runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			x := 0
			for {
				select {
				case <-stop:
					return
				default:
					x++
				}
			}
		}()
	}
	t.Cleanup(func() { close(stop); wg.Wait() })
}

func TestIssue687_W2_ConcurrentSchedulers_NoStuckWakeup_NoDoubleRebuild(t *testing.T) {
	rw := newSnapshotTestWatcher(t, mkRB("ns-a", "viewer-bind", userSub("alice")), mkR("ns-a", "viewer-bind-role"))
	waitRebuildQuiet(t)
	cpuBurner687(t)

	var inflight, maxInflight atomic.Int32
	SetRebuildBarrierForTest(func() {
		n := inflight.Add(1)
		for {
			m := maxInflight.Load()
			if n <= m || maxInflight.CompareAndSwap(m, n) {
				break
			}
		}
		runtime.Gosched()
		inflight.Add(-1)
	})
	t.Cleanup(func() { SetRebuildBarrierForTest(nil) })

	for round := 0; round < 20; round++ {
		var wg sync.WaitGroup
		for g := 0; g < 8; g++ {
			wg.Add(1)
			go func() { defer wg.Done(); scheduleRBACRebuild(rw) }()
		}
		wg.Wait()
	}
	// Every scheduled event must be drained: the loop may not end with dirty
	// set and the slot free.
	deadline := time.Now().Add(5 * time.Second)
	for {
		lock, dirty := rbacRebuildLock.Load(), rbacRebuildDirty.Load()
		if !lock && !dirty {
			break
		}
		if !lock && dirty {
			// Give a just-released loop its re-check.
			time.Sleep(2 * time.Millisecond)
			if !rbacRebuildLock.Load() && rbacRebuildDirty.Load() {
				t.Fatal("W2 RED (#687): the rebuild loop exited with dirty set and the slot free — a lost wakeup")
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("W2: the rebuild never drained (lock=%v dirty=%v)", lock, dirty)
		}
		time.Sleep(time.Millisecond)
	}
	if m := maxInflight.Load(); m > 1 {
		t.Fatalf("W2: %d rebuilds ran concurrently — the single-writer slot is broken", m)
	}
}

// W5 — the initial publish runs under the writer slot: while another holder
// has the slot, the watcher's initial publish waits; once released, it
// publishes. RED with the pre-#687 unslotted initial publish (it would race an
// event-driven rebuild and could store an older snapshot last).
func TestIssue687_W5_InitialPublishWaitsForTheSlot(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	waitRebuildQuiet(t)
	gen0 := RBACGen()
	if !rbacRebuildLock.CompareAndSwap(false, true) {
		t.Fatal("PRE: could not take the writer slot")
	}
	released := false
	release := func() {
		if !released {
			released = true
			rbacRebuildLock.Store(false)
		}
	}
	t.Cleanup(release)

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(rbacScheme(t), rbacListKinds(),
		mkRB("ns-a", "viewer-bind", userSub("alice")), mkR("ns-a", "viewer-bind-role"))
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if RBACGen() != gen0 {
		t.Fatalf("W5 RED: the initial publish ran while another holder had the writer slot (gen %d → %d)", gen0, RBACGen())
	}
	release()
	deadline := time.Now().Add(3 * time.Second)
	for RBACGen() == gen0 {
		if time.Now().After(deadline) {
			t.Fatal("W5: the initial publish never ran after the slot was released")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		t.Fatal(err)
	}
}

// W6 — after its release, the loop re-acquires ONLY by CAS: if another
// scheduler took the slot in between, the loop returns and never rebuilds
// concurrently with it.
func TestIssue687_W6_PostReleaseReacquireIsCAS(t *testing.T) {
	rw := newSnapshotTestWatcher(t, mkRB("ns-a", "viewer-bind", userSub("alice")), mkR("ns-a", "viewer-bind-role"))
	waitRebuildQuiet(t)
	var rebuilds atomic.Int32
	SetRebuildBarrierForTest(func() { rebuilds.Add(1) })
	t.Cleanup(func() { SetRebuildBarrierForTest(nil) })

	var once sync.Once
	var atHook atomic.Int32
	tookSlot := make(chan struct{})
	fn := func() {
		once.Do(func() {
			// Another scheduler: dirty set, slot won right after the release.
			rbacRebuildDirty.Store(true)
			if !rbacRebuildLock.CompareAndSwap(false, true) {
				t.Error("PRE: the slot was not free right after the release")
			}
			atHook.Store(rebuilds.Load())
			close(tookSlot)
		})
	}
	rbacRebuildPostReleaseHookForTest.Store(&fn)
	t.Cleanup(func() {
		rbacRebuildPostReleaseHookForTest.Store(nil)
		rbacRebuildDirty.Store(false)
		rbacRebuildLock.Store(false)
	})

	scheduleRBACRebuild(rw)
	select {
	case <-tookSlot:
	case <-time.After(3 * time.Second):
		t.Fatal("PRE: the post-release hook never ran")
	}
	time.Sleep(100 * time.Millisecond)
	if got := rebuilds.Load(); got != atHook.Load() {
		t.Fatalf("W6 RED: the loop rebuilt %d more time(s) after another scheduler took the slot — a concurrent rebuild", got-atHook.Load())
	}
}
