// proactive_ra_seed_test.go — falsifiers for the cache-side RBAC-reachable
// RESTAction enumeration source (Option A). Package cache so it can reach
// the unexported index build + informer-seed helpers + the
// PublishRBACSnapshotForTest seam. NON-DESTRUCTIVE — synthetic in-process
// snapshots + a fake dynamic client; never touches the rbac package's
// destructive TestMain. Safe under `go test ./internal/cache/...`.

package cache

import (
	"sort"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// waitInformerSynced blocks until the GVR's boot-anchored informer has
// completed its initial LIST+Replace (HasSynced), or fails on a bounded
// deadline. This is the readiness precondition seedRestActionItem depends
// on: EnsureResourceType registers the informer and its reflector does an
// asynchronous initial LIST from the fake dynamic client (empty — no
// RESTAction objects exist) followed by a store Replace([], rv). If the
// test seeds items into the indexer via GetIndexer().Add() BEFORE that
// Replace lands, the Replace wipes some or all of them — the CI-order
// flake where RBACReachableRestActionRefs saw 2 (or 0) of the 3 seeded
// refs. Waiting for HasSynced guarantees the initial Replace has already
// occurred, so subsequent manual Adds are stable (the fake client emits no
// watch events, so the reflector never Replaces again). Mirrors the
// rw.IsSynced gate in TestFalsifierFR6_RemoveResourceTypeTearsDownOneGVR.
func waitInformerSynced(t *testing.T, rw *ResourceWatcher, gvr schema.GroupVersionResource) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !rw.IsSynced(gvr) {
		if time.Now().After(deadline) {
			t.Fatalf("precondition: informer for %v did not sync within deadline", gvr)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// seedRestActionItem adds an unstructured RESTAction CR (ns/name only —
// the enumerator reads ns/name via meta.Accessor) to the boot-anchored
// RESTActions informer indexer.
func seedRestActionItem(t *testing.T, rw *ResourceWatcher, ns, name string) {
	t.Helper()
	rw.mu.RLock()
	gi, ok := rw.informers[restActionGVR]
	rw.mu.RUnlock()
	if !ok {
		t.Fatalf("precondition: restActionGVR informer not registered")
	}
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": restActionGVR.Group + "/" + restActionGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"namespace": ns, "name": name},
	}}
	if err := gi.Informer().GetIndexer().Add(u); err != nil {
		t.Fatalf("seed RESTAction %s/%s: %v", ns, name, err)
	}
}

func refKeys(refs []RestActionRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.Namespace+"/"+r.Name)
	}
	sort.Strings(out)
	return out
}

// TestProactiveRASeed_F_intersection — the airtight cache-side falsifier.
// When SOME published binding grants get on restactions, the enumerator
// returns ALL RESTAction CRs resident in the boot informer (Option A:
// resource-level RBAC → the whole RA set is reachable by that binding's
// subjects). De-dup is by {ns,name}.
func TestProactiveRASeed_F_intersection(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetBindingsByGVRIndexForTest()

	// A binding granting get/list on restactions (templates.krateo.io).
	crb, cr := crbRuleUID("ra-reader", "uid-ra-reader",
		rbacv1.Subject{Kind: "User", Name: "alice"},
		getListRules(restActionGVR.Group, restActionGVR.Resource))
	snap := buildSnap([]*rbacv1.ClusterRoleBinding{crb}, []*rbacv1.ClusterRole{cr})
	PublishRBACSnapshotForTest(snap)

	// Index must be built over a navigated set INCLUDING restActionGVR so
	// the get-restactions binding is enrolled.
	if n := SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{restActionGVR}); n != 1 {
		t.Fatalf("expected 1 binding enrolled, got %d", n)
	}
	// Precondition: the gate the enumerator reads is non-empty.
	if got := EnumeratePrewarmTargetsForGVR(restActionGVR, "get"); len(got) != 1 {
		t.Fatalf("precondition: expected 1 get-restactions target, got %d", len(got))
	}

	rw := newSyntheticRemoveWatcher(t, restActionGVR)
	t.Cleanup(func() { rw.Stop() })
	rw.EnsureResourceType(restActionGVR)
	// Wait for the informer's initial (empty) LIST+Replace to land BEFORE
	// seeding, or the async Replace races the manual Adds below and wipes
	// them — the CI-order flake this test tripped.
	waitInformerSynced(t, rw, restActionGVR)

	// Seed three composition-detail-style RESTActions (the per-composition
	// RAs the nav walk never reaches). One duplicate add to prove dedup.
	seedRestActionItem(t, rw, "comp-a", "composition-resources")
	seedRestActionItem(t, rw, "comp-b", "composition-resources")
	seedRestActionItem(t, rw, "krateo-system", "sidebar-nav-menu")
	seedRestActionItem(t, rw, "comp-a", "composition-resources") // dup

	got := refKeys(RBACReachableRestActionRefs(rw))
	want := []string{"comp-a/composition-resources", "comp-b/composition-resources", "krateo-system/sidebar-nav-menu"}
	if len(got) != len(want) {
		t.Fatalf("expected %d reachable refs, got %d: %v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ref[%d]: want %q got %q (full: %v)", i, want[i], got[i], got)
		}
	}
}

// TestProactiveRASeed_F6_transparent_no_binding — F-6 cache layer. When NO
// binding grants get on restactions, the enumerator returns nil EVEN IF
// RA CRs exist (the RBAC gate fails closed → nothing reachable → the seed
// source is unchanged → serving is transparent). This is the RBAC-leak
// guard: we never seed under identities that cannot authorise the GVR.
func TestProactiveRASeed_F6_transparent_no_binding(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetBindingsByGVRIndexForTest()

	// A binding granting ONLY compositions — NOT restactions.
	crb, cr := crbRuleUID("comp-reader", "uid-comp-reader",
		rbacv1.Subject{Kind: "Group", Name: "devs"},
		getListRules("composition.krateo.io", "compositions"))
	snap := buildSnap([]*rbacv1.ClusterRoleBinding{crb}, []*rbacv1.ClusterRole{cr})
	PublishRBACSnapshotForTest(snap)
	SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{restActionGVR, gr("composition.krateo.io", "compositions")})

	if got := EnumeratePrewarmTargetsForGVR(restActionGVR, "get"); len(got) != 0 {
		t.Fatalf("precondition: expected 0 get-restactions targets, got %d", len(got))
	}

	rw := newSyntheticRemoveWatcher(t, restActionGVR)
	t.Cleanup(func() { rw.Stop() })
	rw.EnsureResourceType(restActionGVR)
	seedRestActionItem(t, rw, "comp-a", "composition-resources")

	if got := RBACReachableRestActionRefs(rw); got != nil {
		t.Fatalf("F-6 FAIL: expected nil (no binding grants get-restactions), got %v", refKeys(got))
	}
}

// TestProactiveRASeed_F6_transparent_empty_informer — F-6 cache layer.
// Even with a get-restactions binding, an EMPTY RESTActions informer
// yields nil (nothing to seed) — the seed source is unchanged.
func TestProactiveRASeed_F6_transparent_empty_informer(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetBindingsByGVRIndexForTest()

	crb, cr := crbRuleUID("ra-reader", "uid-ra-reader",
		rbacv1.Subject{Kind: "User", Name: "alice"},
		getListRules(restActionGVR.Group, restActionGVR.Resource))
	snap := buildSnap([]*rbacv1.ClusterRoleBinding{crb}, []*rbacv1.ClusterRole{cr})
	PublishRBACSnapshotForTest(snap)
	SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{restActionGVR})

	rw := newSyntheticRemoveWatcher(t, restActionGVR)
	t.Cleanup(func() { rw.Stop() })
	rw.EnsureResourceType(restActionGVR)
	// No items seeded.

	if got := RBACReachableRestActionRefs(rw); got != nil {
		t.Fatalf("expected nil (empty informer), got %v", refKeys(got))
	}
}

// TestProactiveRASeed_F6_nil_passthrough — F-6 cache layer. A nil watcher
// is a no-op (defensive — the dispatcher always passes a live rw, but the
// helper must be safe).
func TestProactiveRASeed_F6_nil_passthrough(t *testing.T) {
	if got := RBACReachableRestActionRefs(nil); got != nil {
		t.Fatalf("expected nil for nil watcher, got %v", refKeys(got))
	}
}
