// phase1_epoch_settle_819_test.go — #819 part 1 through the REAL readiness
// code: phase1WarmupWith's seed-deadline release (one of the paths that does
// not wait for the engine's boot pass) with a ClusterRole ADD that lands after
// the first RBAC publish and before the bindings index is ready — delivered by
// the real informer, so the real #682 handler takes the note. The pod must not
// be Ready before the index is, and the epoch must be bumped by then.
//
// Until #1458 the index was BUILT from the snapshot and the readiness flip ran
// that build (RED on main before #819: Ready flipped with the index unbuilt
// and epoch 0; the bump landed whenever the requeued boot pass got to its
// build — bench run 10: 6.5 min after Ready, rotating every armed tab's keys).
// Since #1458 the index is marked ready by the watcher's initial-publish
// goroutine, once the RBAC handlers have been delivered their initial lists,
// and readiness waits for that goroutine (#1231). The arm holds it between its
// publish and the ready mark, which is the window.

package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func TestIssue819_SeedDeadlineRelease_EpochSettledBeforeReady(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		crGVR: "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		// Step 1's meta-query seed.
		{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}: "RESTActionList",
	})
	cache.ResetPhase1DoneForTest()
	cache.ResetRBACSubGenForTest()

	// Hold the watcher's initial publish between its complete publish and the
	// ready mark.
	gate := make(chan struct{})
	restoreHook := cache.SetInitialPublishBeforeIndexHookForTest(func() { <-gate })
	released := false
	release := func() {
		if !released {
			released = true
			close(gate)
			restoreHook()
		}
	}
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		release()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	t.Cleanup(release) // runs before Stop
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		cache.SetGlobal(prev)
		cache.ResetPhase1DoneForTest()
		cache.ResetBindingsByGVRIndexForTest()
		cache.ResetRBACSubGenForTest()
	})
	wait := func(what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	wait("the first complete RBAC publish", func() bool { s := cache.RBACSnapshotForTest(); return s != nil && s.Synced })

	// The window edit: a ClusterRole ADD that is not part of the initial list.
	cr := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
		"metadata": map[string]any{"name": "added-in-window-819"},
		"rules":    []any{map[string]any{"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"get"}}},
	}}
	if _, err := dyn.Resource(crGVR).Create(context.Background(), cr, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	// The handler notes before it schedules the rebuild, so a published
	// snapshot holding the role means the note is taken. The fake apiserver
	// drops an event sent before the informer's watch is open (it opens after
	// its LIST): a rules edit is re-sent until one arrives.
	edits := 0
	wait("the window ClusterRole in a published snapshot", func() bool {
		if s := cache.RBACSnapshotForTest(); s != nil && s.ClusterRolesByName["added-in-window-819"] != nil {
			return true
		}
		edits++
		if edits%40 == 0 { // every 200 ms
			_ = unstructured.SetNestedField(cr.Object, time.Now().String(), "metadata", "annotations", "resent")
			_, _ = dyn.Resource(crGVR).Update(context.Background(), cr, metav1.UpdateOptions{})
		}
		return false
	})
	if cache.BindingsByGVRIndexBuilt() || cache.RBACSubGenEpoch() != 0 {
		t.Fatal("precondition: index not ready and epoch 0 in the window")
	}

	// The engine never reaches its pass: the seed runs out its deadline.
	lister := func(ctx context.Context) ([]navigationRoot, error) { return nil, nil }
	resolver := func(ctx context.Context, root navigationRoot) error { return nil }
	seed := pipSeedFn(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = phase1WarmupWith(ctx, rw, lister, resolver, nil, nil, seed, nil)

	if !cache.IsPhase1Done() {
		t.Fatal("precondition: the deadline release must flip phase 1")
	}
	// Phase 1 is done, but the pod is not Ready: readiness also needs the
	// initial publish to have returned, and it is still before its ready mark.
	if cache.RBACReadiness().Complete {
		t.Fatal("#819 RED: readiness is complete with the bindings index not ready — the #682 rotation is still owed after Ready")
	}
	if cache.BindingsByGVRIndexBuilt() || cache.RBACSubGenEpoch() != 0 {
		t.Fatalf("the phase-1 flip made the index ready (%v) or rotated the epoch (%d): only the delivery of the RBAC handlers' "+
			"initial lists may (#1458)", cache.BindingsByGVRIndexBuilt(), cache.RBACSubGenEpoch())
	}

	release()
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	// rbacInitialPublishReturned is what readiness reads; by then:
	if !cache.BindingsByGVRIndexBuilt() {
		t.Fatal("#819 RED: the initial publish returned (readiness may flip) with the bindings index not ready")
	}
	if got := cache.RBACSubGenEpoch(); got != 1 {
		t.Fatalf("#819 RED: epoch=%d when readiness could flip, want 1 — the window edit's rotation must land before Ready", got)
	}
	if !cache.RBACReadiness().Complete {
		t.Fatal("readiness is not complete after the initial publish returned")
	}
}
