// phase1_epoch_settle_819_test.go — #819 part 1 through the REAL readiness
// code: phase1WarmupWith's seed-deadline release (one of the paths that does
// not wait for the engine's boot pass, which is where the bindings index is
// built) with a ClusterRole ADD that lands after the first RBAC publish and
// before any build — delivered by the real informer, so the real #682 handler
// takes the note. When /readyz flips, the index must be built and the epoch
// bumped. RED on main: Ready flipped with the index unbuilt and epoch 0; the
// bump then landed whenever the requeued boot pass got to its build (bench
// run 10: 6.5 min after Ready, rotating every armed tab's keys).

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
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	if err := rw.WaitInitialRBACPublishForTest(time.Minute); err != nil {
		t.Fatal(err)
	}
	prev := cache.Global()
	cache.SetGlobal(rw)
	reset := func() {
		cache.ResetPhase1DoneForTest()
		cache.ResetBindingsByGVRIndexForTest()
		cache.ResetRBACSubGenForTest()
	}
	reset()
	t.Cleanup(func() {
		cache.SetGlobal(prev)
		reset()
	})

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
	// snapshot holding the role means the note is taken.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if s := cache.RBACSnapshotForTest(); s != nil && s.ClusterRolesByName["added-in-window-819"] != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the window ClusterRole never reached a published snapshot")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if cache.BindingsByGVRIndexBuilt() || cache.RBACSubGenEpoch() != 0 {
		t.Fatal("precondition: index unbuilt and epoch 0 before readiness")
	}

	// The engine never reaches its build: the seed runs out its deadline.
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
		t.Fatal("precondition: the deadline release must flip readiness")
	}
	if !cache.BindingsByGVRIndexBuilt() {
		t.Fatal("#819 RED: /readyz flipped with the bindings index unbuilt — the #682 rotation is still owed after Ready")
	}
	if got := cache.RBACSubGenEpoch(); got != 1 {
		t.Fatalf("#819 RED: epoch=%d when /readyz flipped, want 1 — the window edit's rotation must land before Ready", got)
	}
}
