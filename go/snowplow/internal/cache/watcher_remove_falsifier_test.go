// watcher_remove_falsifier_test.go — Ship 0.30.115 (R6) pre-flight
// falsifier for per-GVR informer-lifecycle teardown.
//
// Team rule feedback_falsifier_first_before_ship: this test is written
// BEFORE the production fix and MUST fail against 0.30.114 — where
// RemoveResourceType does not exist and every informer shares the single
// process-wide rw.stopCh, so there is no per-GVR canceller.
//
//   FR6 — RemoveResourceType tears down exactly one GVR's informer:
//         its Run goroutine exits, its rw.informers / rw.syncCh /
//         rw.confirmed / rw.watchBroken / rw.informerStop entries are
//         purged, SIBLING informers survive, and a subsequent Stop()
//         reaps the rest without double-closing any channel.
//
// CRITICAL fixture-safety: every GVR here is SYNTHETIC and the client is
// a fake dynamic client — no test touches a real cluster or a real CRD.

package cache

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// gvrSyntheticN builds the Nth synthetic per-GVR teardown test GVR.
func gvrSyntheticN(n string) schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "synthetic-r6.krateo.io",
		Version:  "v1",
		Resource: "things" + n,
	}
}

// newSyntheticRemoveWatcher builds a CACHE_ENABLED ResourceWatcher
// backed by a fake dynamic client seeded for the given synthetic GVRs.
// SYNTHETIC ONLY — never touches a cluster.
func newSyntheticRemoveWatcher(t *testing.T, gvrs ...schema.GroupVersionResource) *ResourceWatcher {
	t.Helper()
	sch := k8sruntime.NewScheme()
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:                "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:         "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:         "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}: "CustomResourceDefinitionList",
	}
	for i, gvr := range gvrs {
		// The fake dynamic client requires the list-kind to END in "List".
		listKinds[gvr] = "SyntheticR6N" + itoa(i) + "List"
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds)
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher under CACHE_ENABLED=true")
	}
	return rw
}

// TestFalsifierFR6_RemoveResourceTypeTearsDownOneGVR is the R6 pre-flight
// falsifier. It registers three synthetic informers, RemoveResourceTypes
// the middle one, and asserts:
//
//   - the removed GVR's Run goroutine exits (goroutine count drops back
//     toward the pre-registration baseline);
//   - the removed GVR is purged from every per-GVR map;
//   - the two SIBLING informers survive — still registered, still
//     serving;
//   - a final Stop() reaps the survivors without panicking on a
//     double-close.
//
// FAILS on 0.30.114: RemoveResourceType does not exist (compile-time on
// the bare 0.30.114 tree; behavioural once the no-op stub lands).
func TestFalsifierFR6_RemoveResourceTypeTearsDownOneGVR(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)

	gA := gvrSyntheticN("a")
	gB := gvrSyntheticN("b")
	gC := gvrSyntheticN("c")

	rw := newSyntheticRemoveWatcher(t, gA, gB, gC)
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })

	// Register the three synthetic informers (post-Start lazy register —
	// each spawns a Run goroutine + a sync-watcher goroutine).
	for _, g := range []schema.GroupVersionResource{gA, gB, gC} {
		rw.EnsureResourceType(g)
	}
	// Wait until all three informers have synced.
	deadline := time.Now().Add(5 * time.Second)
	for _, g := range []schema.GroupVersionResource{gA, gB, gC} {
		for !rw.IsSynced(g) {
			if time.Now().After(deadline) {
				t.Fatalf("synthetic informer %v did not sync in time", g)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if !rw.IsRegistered(gA) || !rw.IsRegistered(gB) || !rw.IsRegistered(gC) {
		t.Fatalf("setup: not all synthetic informers registered")
	}

	// Goroutine baseline AFTER all three are live and synced.
	runtime.GC()
	beforeRemove := runtime.NumGoroutine()

	// Remove the middle GVR.
	rw.RemoveResourceType(gB)

	// AC-R6.1: gB is purged from rw.informers + every per-GVR map.
	if rw.IsRegistered(gB) {
		t.Fatalf("FR6: gB still registered after RemoveResourceType")
	}
	if !rw.everyPerGVRMapClearForTest(gB) {
		t.Fatalf("FR6: a per-GVR map still holds an entry for the removed gB — leak")
	}
	// AC-R6.2: siblings survive.
	if !rw.IsRegistered(gA) || !rw.IsRegistered(gC) {
		t.Fatalf("FR6: a sibling informer was torn down — over-reach (gA reg=%v gC reg=%v)",
			rw.IsRegistered(gA), rw.IsRegistered(gC))
	}
	// Siblings still serve (informer store reachable, no panic).
	if got := rw.ListObjects(gA, ""); got == nil {
		t.Fatalf("FR6: sibling gA stopped serving after gB removal")
	}
	if got := rw.ListObjects(gC, ""); got == nil {
		t.Fatalf("FR6: sibling gC stopped serving after gB removal")
	}

	// AC-R6.2: the removed GVR's Run + sync-watcher goroutines exit. Poll
	// — goroutine teardown is asynchronous.
	exited := false
	for i := 0; i < 100; i++ {
		runtime.GC()
		if runtime.NumGoroutine() < beforeRemove {
			exited = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !exited {
		t.Fatalf("FR6: goroutine count did not drop after RemoveResourceType "+
			"(before=%d now=%d) — the removed informer's Run goroutine leaked",
			beforeRemove, runtime.NumGoroutine())
	}

	// AC-R6.1 idempotence: a second removal of the same GVR, and a
	// removal of an unknown GVR, are both no-ops (no panic).
	rw.RemoveResourceType(gB)
	rw.RemoveResourceType(gvrSyntheticN("never-registered"))

	// AC-R6.2: a final Stop() reaps the survivors without double-closing
	// any channel (a double-close panics — recover would not catch a
	// panic on another goroutine, so a clean return IS the assertion).
	rw.Stop()
	time.Sleep(50 * time.Millisecond)
}

// TestFalsifierFR6_RemoveThenStopNoDoubleClose drives the channel-close
// ordering that AC-R6.5 calls out: RemoveResourceType closes a GVR's
// stop channel, then Stop() must NOT close it again. A sync.Once (or a
// closed-check under rw.mu) guards the close; without it Stop() panics
// on the already-closed channel.
//
// FAILS on 0.30.114: RemoveResourceType does not exist.
func TestFalsifierFR6_RemoveThenStopNoDoubleClose(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)

	gA := gvrSyntheticN("x")
	rw := newSyntheticRemoveWatcher(t, gA)

	rw.EnsureResourceType(gA)
	deadline := time.Now().Add(5 * time.Second)
	for !rw.IsSynced(gA) {
		if time.Now().After(deadline) {
			t.Fatalf("synthetic informer did not sync in time")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Remove the GVR — closes its per-GVR stop channel.
	rw.RemoveResourceType(gA)
	// Stop() must reap rw.stopCh and any REMAINING per-GVR channels
	// without re-closing gA's (already-closed) channel.
	rw.Stop()
	time.Sleep(50 * time.Millisecond)
	// A second Stop() is also idempotent.
	rw.Stop()
}

// TestShipR6_ConcurrentRemoveAndEnsure asserts AC-R6.5: concurrent
// RemoveResourceType + EnsureResourceType for the SAME GVR must not
// deadlock and must not double-close the per-GVR stop channel. Both
// operations serialise on rw.mu; the channel close is guarded by
// closePerGVRStopLocked's closed-check under the lock. Run under -race
// this also proves no data race on the per-GVR maps.
func TestShipR6_ConcurrentRemoveAndEnsure(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)

	gA := gvrSyntheticN("conc")
	rw := newSyntheticRemoveWatcher(t, gA)
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })

	rw.EnsureResourceType(gA)

	// Hammer Remove + Ensure for the same GVR from two goroutines. The
	// assertion is "completes without deadlock or panic" — a deadlock
	// hangs the test (caught by the package -timeout), a double-close
	// panics the process.
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); rw.RemoveResourceType(gA) }()
		go func() { defer wg.Done(); rw.EnsureResourceType(gA) }()
	}
	wg.Wait()

	// A final Stop() after the churn must still be a clean, no-panic
	// reap of whatever per-GVR channels remain.
	rw.Stop()
	time.Sleep(50 * time.Millisecond)
}

// TestShipR6_ServableFallsThroughAfterRemove asserts AC-R6.3: after
// RemoveResourceType, servable() for the removed GVR returns false
// cleanly (conjunct 1 — not registered) — no panic, no stale serve. The
// CRD-watch path's D2 dirty-mark fires independently of the teardown.
func TestShipR6_ServableFallsThroughAfterRemove(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)

	gA := gvrSyntheticN("srv")
	rw := newSyntheticRemoveWatcher(t, gA)
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })

	rw.EnsureResourceType(gA)
	deadline := time.Now().Add(5 * time.Second)
	for !rw.IsSynced(gA) {
		if time.Now().After(deadline) {
			t.Fatalf("synthetic informer did not sync in time")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Pre-removal: the GVR is servable.
	if !rw.IsServable(gA) {
		t.Fatalf("setup: gA not servable before removal")
	}

	rw.RemoveResourceType(gA)

	// Post-removal: servable() falls through to false (conjunct 1 —
	// unregistered), no panic.
	if rw.IsServable(gA) {
		t.Fatalf("AC-R6.3: gA still reports servable after RemoveResourceType — "+
			"a removed type must fall through cleanly")
	}
	// ListObjects for a removed GVR also falls through without panic
	// (the apiserver/empty path — not a stale informer serve).
	_ = rw.ListObjects(gA, "")
}

// --- R6 Option 1 — re-add after remove must self-heal -----------------------

// syntheticThing builds an unstructured object of the given removable
// GVR — a synthetic composition-style CR for the re-add falsifier.
func syntheticThing(gvr schema.GroupVersionResource, ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvr.Group + "/" + gvr.Version,
		"kind":       "SyntheticThing",
		"metadata":   map[string]any{"namespace": ns, "name": name},
	}}
}

// TestFalsifierFR6ReAdd_DeleteRecreateSelfHeals is the R6 Option 1
// pre-flight falsifier. The shared informer factory caches informers by
// GVR with no eviction API, so a teardown-only R6 would hand a later
// EnsureResourceType the SAME stopped, frozen informer on a CRD
// delete→recreate — a silent stale-serve regression.
//
// Option 1: a removable GVR (IsNavigationDiscoveredGroup true) runs a
// STANDALONE informer. RemoveResourceType drops it; a re-register
// constructs a FRESH one. This test drives delete→recreate and asserts
// the re-added informer is a DISTINCT instance, IsStopped()==false,
// HasSynced() flips true, and serves a row created AFTER recreate.
//
// FAILS against a teardown-only R6: the re-added informer is the same
// stopped instance — IsStopped()==true and the post-recreate row never
// appears.
func TestFalsifierFR6ReAdd_DeleteRecreateSelfHeals(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)

	// A REMOVABLE GVR — its group must be navigation-discovered so
	// addResourceTypeLocked takes the standalone-informer branch.
	gvr := schema.GroupVersionResource{
		Group: "synthetic-r6-removable.krateo.io", Version: "v1", Resource: "removablethings",
	}
	AddNavigationDiscoveredGroup(gvr.Group)

	rw := newSyntheticRemoveWatcher(t, gvr)
	t.Cleanup(func() { rw.Stop(); time.Sleep(50 * time.Millisecond) })

	// First registration — the standalone informer lists + watches.
	rw.EnsureResourceType(gvr)
	waitSynced(t, rw, gvr)
	rw.mu.RLock()
	inf1 := rw.informers[gvr].Informer()
	rw.mu.RUnlock()

	// Tear it down (the CRD-delete path).
	rw.RemoveResourceType(gvr)
	// Poll until the standalone informer's Run goroutine has observed
	// the closed stop channel.
	stopped := false
	for i := 0; i < 100; i++ {
		if inf1.IsStopped() {
			stopped = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !stopped {
		t.Fatalf("FR6-readd: torn-down informer never reported IsStopped()")
	}

	// The CRD is recreated and a NEW object exists for it — create it on
	// the fake client's tracker so the re-added informer's initial LIST
	// picks it up.
	const ns, objName = "bench-ns-readd", "thing-after-recreate"
	if err := rw.dyn.Resource(gvr).Namespace(ns).Delete(context.Background(), objName, metav1.DeleteOptions{}); err == nil {
		_ = err // ignore — object likely absent; Create below is the real step
	}
	created, err := rw.dyn.Resource(gvr).Namespace(ns).Create(
		context.Background(), syntheticThing(gvr, ns, objName), metav1.CreateOptions{})
	if err != nil || created == nil {
		t.Fatalf("FR6-readd: failed to create post-recreate object: %v", err)
	}

	// Re-register — Option 1 must construct a FRESH standalone informer.
	rw.EnsureResourceType(gvr)
	waitSynced(t, rw, gvr)
	rw.mu.RLock()
	inf2 := rw.informers[gvr].Informer()
	rw.mu.RUnlock()

	// A teardown-only R6 hands back the SAME stopped informer here.
	if inf1 == inf2 {
		t.Fatalf("FR6-readd: re-added informer is the SAME instance as the torn-down one — " +
			"the shared factory pinned it; Option 1 standalone informer not used")
	}
	if inf2.IsStopped() {
		t.Fatalf("FR6-readd: re-added informer is already stopped — a frozen store")
	}
	if !inf2.HasSynced() {
		t.Fatalf("FR6-readd: re-added informer never synced")
	}

	// The decisive assertion: the row created AFTER recreate is served
	// by the fresh informer. A frozen (stale) informer would never have
	// it.
	got, ok := rw.GetObject(context.Background(), gvr, ns, objName)
	if !ok || got == nil {
		t.Fatalf("FR6-readd: post-recreate object %s/%s not served by the re-added "+
			"informer — the re-add resurrected a frozen store instead of "+
			"constructing a fresh one", ns, objName)
	}
	if got.GetName() != objName {
		t.Fatalf("FR6-readd: served object name=%q want %q", got.GetName(), objName)
	}
}

// waitSynced blocks until gvr's informer reports synced, or fails the
// test after 5s.
func waitSynced(t *testing.T, rw *ResourceWatcher, gvr schema.GroupVersionResource) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !rw.IsSynced(gvr) {
		if time.Now().After(deadline) {
			t.Fatalf("informer %v did not sync in time", gvr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
