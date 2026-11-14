package dispatchers

// issue1328_gated_view_test.go — #1328, the tracker on its own (the wire arms
// are internal/handlers refreshes_gated_view_1328_test.go):
//
//	T1  a grant that changes what alice is served from a gated key → the key,
//	    once; a second class move before any frame was written, that changes
//	    nothing more, → nothing (GatedMoved records the view it announced).
//	T2  bob, on the same shared key, is unaffected by alice's grant.

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
)

func TestIssue1328_T1_TheGatedViewMovesOnceAndOnlyForItsIdentity(t *testing.T) {
	resetRBAC := func() {
		cache.ResetBindingsByGVRIndexForTest()
		cache.ResetRBACSubGenForTest()
	}
	resetRBAC()
	t.Cleanup(resetRBAC)
	dyn := h1BuildWatcherWithRA(t)
	store := cache.ResolvedCache()
	const key = "wc-1328-shared"
	store.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: d1324Shell(true),
		Inputs: &cache.ResolvedKeyInputs{CacheEntryClass: cache.CacheEntryClassWidgetContent, Name: "shared"}})
	alice, bob := d1324Ctx("alice-1328"), d1324Ctx("bob-1328")
	tracker := func(ctx context.Context) *SubscriptionRotationTracker {
		tr := NewSubscriptionRotationTracker(cache.WithInformerOnlyReads(ctx))
		tr.trackGated(key, cache.CacheEntryClassWidgetContent)
		tr.ArmGated()
		return tr
	}
	ta, tb := tracker(alice), tracker(bob)
	if !ta.Tracking() {
		t.Fatal("PRE: a gated key does not make the connection track the class signal")
	}

	rbacGVR := func(r string) schema.GroupVersionResource {
		return schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: r}
	}
	mk := func(gvr schema.GroupVersionResource, obj map[string]any) {
		t.Helper()
		if _, err := dyn.Resource(gvr).Create(context.Background(), &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	grant := func(role, binding, group, resource, verb string) {
		t.Helper()
		mk(rbacGVR("clusterroles"), map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
			"metadata": map[string]any{"name": role},
			"rules":    []any{map[string]any{"apiGroups": []any{group}, "resources": []any{resource}, "verbs": []any{verb}}}})
		mk(rbacGVR("clusterrolebindings"), map[string]any{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRoleBinding",
			"metadata": map[string]any{"name": binding, "uid": "uid-" + binding},
			"subjects": []any{map[string]any{"kind": "User", "name": "alice-1328"}},
			"roleRef":  map[string]any{"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role}})
	}
	awaitClassMove := func(before cache.IdentityClass) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for rbac.IdentityClassOf(ta.ui) == before {
			if time.Now().After(deadline) {
				t.Fatal("PRE: the grant never moved alice's class")
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	c0 := rbac.IdentityClassOf(ta.ui)
	grant("panel-deleter-1328", "alice-deletes-1328", h1WidgetGVR.Group, h1WidgetGVR.Resource, "delete")
	awaitClassMove(c0)
	time.Sleep(50 * time.Millisecond) // the #703 second publish (sub-gen) may follow the first
	if got := ta.GatedMoved(); len(got) != 1 || got[0] != key {
		t.Fatalf("T1 RED: GatedMoved=%v after a grant that changes what alice is served, want [%s]", got, key)
	}
	if got := tb.GatedMoved(); len(got) != 0 {
		t.Fatalf("T2 RED: bob's served view did not change but GatedMoved=%v", got)
	}

	// No frame written yet; alice's class moves again with no view change.
	c1 := rbac.IdentityClassOf(ta.ui)
	grant("cm-reader-1328", "alice-cm-1328", "", "configmaps", "get")
	awaitClassMove(c1)
	if got := ta.GatedMoved(); len(got) != 0 {
		t.Fatalf("T1 RED: GatedMoved=%v at a class move that changed nothing more — the view already announced must be "+
			"recorded, or every later class move re-announces it", got)
	}
}
