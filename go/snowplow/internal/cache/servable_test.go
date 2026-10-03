// servable_test.go — Tag 0.30.97 servability-signal unit tests.
//
// Covers ListObjectsServable + IsServable, the uniform servability
// predicates added to close the 0.30.95 resolver pivot's check-then-act
// gap (regression journal 2026-05-15: the pivot served an unregistered
// GVR's empty list as `served=true`, zeroing the Compositions feature).
//
// Coverage matrix:
//
//	  SCENARIO                              | ListObjectsServable | IsServable
//	  --------------------------------------|---------------------|------------
//	  unregistered GVR                      | (nil, false)        | false
//	  registered + synced + empty store     | ([], true)          | true
//	  registered + synced + with items      | (items, true)       | true
//	  passthrough mode                      | (routed, true)      | false
//	  nil receiver                          | (nil, false)        | false
//
// Per `feedback_no_special_cases.md`: every assertion uses a generic
// customer GVR — ListObjectsServable / IsServable are uniform predicates
// with no per-GVR carve-out.

package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// servableTestGVR is the customer-style GVR used across the servability
// tests. Secrets are registered in every fake-dynamic scheme builder, so
// the informer LIST does not panic on an unknown List kind.
// #398: core v1/secrets is never informed (IsSensitiveResource), so this
// fixture uses v1/configmaps as its arbitrary core-group GVR instead.
var servableTestGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}

// servableListKinds extends rbacListKinds with the Secrets List entry so
// the fake dynamic client accepts a registered Secret informer's initial
// LIST without panicking.
func servableListKinds() map[schema.GroupVersionResource]string {
	m := rbacListKinds()
	m[servableTestGVR] = "ConfigMapList"
	return m
}

// newSyncedServableWatcher builds a cache=on watcher, registers the
// servable test GVR against an empty store, waits for its initial LIST
// to complete, and wires t.Cleanup.
func newSyncedServableWatcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), servableListKinds())

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher under CACHE_ENABLED=true")
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	added, syncCh := rw.EnsureResourceType(servableTestGVR)
	if !added {
		t.Fatalf("EnsureResourceType: want added=true; informer unexpectedly pre-registered")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("EnsureResourceType: informer did not sync within 5s")
	}
	return rw
}

// TestListObjectsServable_UnregisteredGVR is the regression-critical
// case: the pivot must NOT serve an unregistered GVR. ListObjectsServable
// returns (nil, false) so the caller falls through to the apiserver
// rather than emitting an empty list it cannot vouch for.
func TestListObjectsServable_UnregisteredGVR(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), servableListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	// Do NOT register servableTestGVR.
	items, servable := rw.ListObjectsServable(servableTestGVR, "")
	if servable {
		t.Fatalf("unregistered GVR: want servable=false; got true with %d items", len(items))
	}
	if items != nil {
		t.Fatalf("unregistered GVR: want nil items; got %d", len(items))
	}
}

// TestListObjectsServable_RegisteredNotSynced asserts that a GVR which
// is registered but whose initial LIST has not completed reports
// servable=false. The fake dynamic client syncs near-instantly, so this
// test exercises the predicate at the lock level: ListObjectsServable
// returns (nil,false) when HasSynced is false. We reach the not-synced
// window by checking BEFORE the sync channel closes — best-effort; if
// the informer already synced, the unregistered-GVR test above and the
// synced tests below jointly pin the same false/true partition.
func TestListObjectsServable_RegisteredNotSynced(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), servableListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	added, syncCh := rw.EnsureResourceType(servableTestGVR)
	if !added {
		t.Fatalf("EnsureResourceType: want added=true")
	}

	// Race the sync: ListObjectsServable while the informer may still
	// be in its initial-LIST window. The contract under test is that
	// the result is internally consistent — servable implies the
	// indexer was read after HasSynced; not-servable implies a nil
	// slice. We assert that invariant rather than a fixed bool, since
	// the fake's sync timing is nondeterministic.
	for i := 0; i < 200; i++ {
		items, servable := rw.ListObjectsServable(servableTestGVR, "")
		if !servable && items != nil {
			t.Fatalf("not-servable must yield nil items; got %d", len(items))
		}
		if servable {
			break
		}
		time.Sleep(time.Millisecond)
	}

	// After sync completes the GVR must be servable.
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("informer did not sync within 5s")
	}
	if _, servable := rw.ListObjectsServable(servableTestGVR, ""); !servable {
		t.Fatalf("registered + synced GVR: want servable=true; got false")
	}
}

// TestListObjectsServable_SyncedEmpty is the over-correction guard: a
// registered+synced informer with a genuinely-empty store must STILL
// return ([], true). 0.30.97 must not regress genuinely-empty-but-synced
// reads into apiserver fallthrough — that is a real answer the watcher
// can vouch for.
func TestListObjectsServable_SyncedEmpty(t *testing.T) {
	rw := newSyncedServableWatcher(t)

	items, servable := rw.ListObjectsServable(servableTestGVR, "")
	if !servable {
		t.Fatalf("synced + empty: want servable=true; got false (over-correction — genuinely-empty is still servable)")
	}
	if items == nil {
		t.Fatalf("synced + empty: want non-nil empty slice; got nil")
	}
	if len(items) != 0 {
		t.Fatalf("synced + empty: want 0 items; got %d", len(items))
	}
}

// newUnstructuredSecret builds a v1/Secret as *unstructured.Unstructured
// for seeding the dynamic fake. The servable test GVR (secrets) is
// registered via EnsureResourceType — the full-unstructured path — so
// the informer indexer holds *unstructured.Unstructured, matching the
// type ListObjectsServable / ListObjects assert. (RBAC GVRs use the
// 0.30.6 typed-converting transform and are intentionally NOT used
// here.)
func newUnstructuredSecret(ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]any{
				"namespace": ns,
				"name":      name,
			},
		},
	}
}

// TestListObjectsServable_SyncedWithItems asserts the happy path: a
// registered+synced informer with seeded objects returns (items, true),
// and that namespace filtering mirrors ListObjects exactly (cluster-wide
// vs namespace-scoped vs empty partition).
//
// The secrets GVR is registered via EnsureResourceType (full-
// unstructured path); seeding the dynamic fake up front puts the
// objects in the informer's initial LIST.
func TestListObjectsServable_SyncedWithItems(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	seedA := newUnstructuredSecret("ns-a", "secret-a")
	seedB := newUnstructuredSecret("ns-a", "secret-b")
	seedC := newUnstructuredSecret("ns-b", "secret-c")
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), servableListKinds(), seedA, seedB, seedC)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	added, syncCh := rw.EnsureResourceType(servableTestGVR)
	if !added {
		t.Fatalf("EnsureResourceType: want added=true")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("EnsureResourceType: informer did not sync within 5s")
	}
	if !rw.IsServable(servableTestGVR) {
		t.Fatalf("setup: secrets informer must be servable after sync")
	}

	// Cluster-wide → 3 items.
	all, servable := rw.ListObjectsServable(servableTestGVR, "")
	if !servable {
		t.Fatalf("synced + items (cluster-wide): want servable=true; got false")
	}
	if len(all) != 3 {
		t.Fatalf("cluster-wide: want 3 secrets; got %d", len(all))
	}

	// Namespace-scoped → 2 items in ns-a.
	nsA, servable := rw.ListObjectsServable(servableTestGVR, "ns-a")
	if !servable {
		t.Fatalf("synced + items (ns-a): want servable=true; got false")
	}
	if len(nsA) != 2 {
		t.Fatalf("ns-a: want 2 secrets; got %d", len(nsA))
	}

	// Namespace with no objects → ([], true) — still servable.
	nsEmpty, servable := rw.ListObjectsServable(servableTestGVR, "ns-with-nothing")
	if !servable {
		t.Fatalf("synced + empty namespace partition: want servable=true; got false")
	}
	if len(nsEmpty) != 0 {
		t.Fatalf("empty namespace: want 0 secrets; got %d", len(nsEmpty))
	}

	// Parity: ListObjectsServable items must match the legacy
	// ListObjects materialization exactly for the same (gvr, ns).
	legacy := rw.ListObjects(servableTestGVR, "ns-a")
	if len(legacy) != len(nsA) {
		t.Fatalf("parity: ListObjects=%d ListObjectsServable=%d for ns-a", len(legacy), len(nsA))
	}
}

// TestListObjectsServable_PassthroughMode asserts that in passthrough
// mode (cache=off + dyn provided) ListObjectsServable routes to the
// apiserver and reports servable=true — the apiserver-routed list IS
// authoritative.
func TestListObjectsServable_PassthroughMode(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")

	seedA := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "crb-a"}}
	seedB := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "crb-b"}}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), rbacListKinds(), seedA, seedB)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil || !rw.IsPassthrough() {
		t.Fatalf("expected passthrough watcher under CACHE_ENABLED=false")
	}
	t.Cleanup(rw.Stop)

	crbGVR := schema.GroupVersionResource{
		Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings",
	}
	items, servable := rw.ListObjectsServable(crbGVR, "")
	if !servable {
		t.Fatalf("passthrough: want servable=true (apiserver-routed list is authoritative); got false")
	}
	if len(items) != 2 {
		t.Fatalf("passthrough: want 2 ClusterRoleBindings from apiserver; got %d", len(items))
	}
}

// TestListObjectsServable_NilReceiver covers the defensive nil-receiver
// guard — production callers fetch rw via cache.Global() which is nil
// when the cache subsystem is disabled.
func TestListObjectsServable_NilReceiver(t *testing.T) {
	var rw *cache.ResourceWatcher
	items, servable := rw.ListObjectsServable(servableTestGVR, "")
	if servable {
		t.Fatalf("nil receiver: want servable=false; got true")
	}
	if items != nil {
		t.Fatalf("nil receiver: want nil items; got %d", len(items))
	}
}

// TestIsServable_UnregisteredGVR asserts IsServable=false for a GVR with
// no registered informer.
func TestIsServable_UnregisteredGVR(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), servableListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	if rw.IsServable(servableTestGVR) {
		t.Fatalf("unregistered GVR: want IsServable=false; got true")
	}
}

// TestIsServable_RegisteredSynced asserts IsServable=true once a
// registered informer has completed its initial LIST.
func TestIsServable_RegisteredSynced(t *testing.T) {
	rw := newSyncedServableWatcher(t)

	if !rw.IsServable(servableTestGVR) {
		t.Fatalf("registered + synced GVR: want IsServable=true; got false")
	}
}

// TestIsServable_PassthroughMode asserts IsServable=false in passthrough
// mode — there are no informers to vouch for; callers route directly to
// the apiserver.
func TestIsServable_PassthroughMode(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newTestScheme(), rbacListKinds())
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil || !rw.IsPassthrough() {
		t.Fatalf("expected passthrough watcher")
	}
	t.Cleanup(rw.Stop)

	crbGVR := schema.GroupVersionResource{
		Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings",
	}
	if rw.IsServable(crbGVR) {
		t.Fatalf("passthrough: want IsServable=false; got true")
	}
}

// TestIsServable_NilReceiver covers the defensive nil-receiver guard.
func TestIsServable_NilReceiver(t *testing.T) {
	var rw *cache.ResourceWatcher
	if rw.IsServable(servableTestGVR) {
		t.Fatalf("nil receiver: want IsServable=false; got true")
	}
}
