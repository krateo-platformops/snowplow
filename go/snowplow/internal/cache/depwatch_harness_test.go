// depwatch_harness_test.go — 1.12.6 C1: a REAL synced watcher for the bridge
// arms.
//
// Pre-1.12.6 the #187 arms drove the handler closures off a bare
// ResourceWatcher with a hand-closed syncCh: enough for a bridge whose action
// was fixed at the handler. Under C1 the worker derives the action from the
// informer's CURRENT STATE, and a bare watcher has no informer — every probe
// would read objUnknown, which is honest (and E2/the C2 arm pin that path) but
// is not the shape the ADD/DELETE arms mean to exercise. So the arms now run
// over a fake dynamic client whose informer really lists, syncs and indexes:
// an object the arm never created on the client is genuinely ABSENT from the
// indexer; one it created is genuinely PRESENT. Nothing about the probe is
// simulated.

package cache

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// harnessWaitBound bounds every harness wait (informer sync, watch
// establishment, indexer convergence). Generous on purpose: the full serial
// -race suite runs this package at ~70 s of wall-clock load, and a bound that
// is comfortable in isolation (5 s) was observed to be crossed there
// (fullsuite-1126-c1.log: D4 at 18:04:56). The bound only turns a pass into a
// fail; the arms' RED-on-main claims are behavioural (served body), never a
// timeout, so a wide bound cannot mask a defect.
const harnessWaitBound = 30 * time.Second

// watchGate records, per resource, when the fake client has REGISTERED a
// watch on its tracker. This closes the one real gap in the fake-client
// harness: the tracker cannot resume a watch by resourceVersion, so an object
// created after the reflector's initial LIST but before its Watch is
// registered is never delivered and the indexer never sees it — no wait,
// however long, can recover it. realWatcher therefore waits for the watch
// to be registered before it hands the client to an arm.
type watchGate struct {
	mu   sync.Mutex
	seen map[schema.GroupVersionResource]int
	cond *sync.Cond
}

func newWatchGate() *watchGate {
	g := &watchGate{seen: map[schema.GroupVersionResource]int{}}
	g.cond = sync.NewCond(&g.mu)
	return g
}

// reactor registers the watch on the tracker ITSELF (so the registration
// happens-before the signal), then signals.
func (g *watchGate) reactor(dyn *dynamicfake.FakeDynamicClient) k8stesting.WatchReactionFunc {
	return func(action k8stesting.Action) (bool, watch.Interface, error) {
		gvr := action.GetResource()
		w, err := dyn.Tracker().Watch(gvr, action.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		g.mu.Lock()
		g.seen[gvr]++
		g.cond.Broadcast()
		g.mu.Unlock()
		return true, w, nil
	}
}

func (g *watchGate) wait(t *testing.T, gvr schema.GroupVersionResource, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	g.mu.Lock()
	defer g.mu.Unlock()
	for g.seen[gvr] == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("watch for %s was not registered on the fake tracker within %s", gvr, within)
		}
		// cond has no timed wait; poll coarsely under the lock release.
		g.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		g.mu.Lock()
	}
}

// realWatcher builds a ResourceWatcher over a fake dynamic client, registers
// every gvr and waits for each informer to sync. The returned client is the
// "cluster": create/delete objects on it to change what the indexer holds.
func realWatcher(t *testing.T, gvrs ...schema.GroupVersionResource) (*ResourceWatcher, dynamic.Interface) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")

	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	_ = corev1.AddToScheme(sch)
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	for _, gvr := range gvrs {
		listKinds[gvr] = listKindFor(gvr)
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds)
	gate := newWatchGate()
	dyn.PrependWatchReactor("*", gate.reactor(dyn))

	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("NewResourceWatcher returned nil with CACHE_ENABLED=true")
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond) // let the informer goroutines exit under -race
	})
	for _, gvr := range gvrs {
		_, syncCh := rw.EnsureResourceType(gvr)
		select {
		case <-syncCh:
		case <-time.After(harnessWaitBound):
			t.Fatalf("informer for %s did not sync within %s", gvr, harnessWaitBound)
		}
		if !rw.IsServable(gvr) {
			t.Fatalf("precondition: %s registered+synced but not servable", gvr)
		}
		// The reflector registers its Watch AFTER the initial LIST that
		// flipped HasSynced; only once the watch is on the tracker will a
		// Create/Delete on the client reach the indexer (see watchGate).
		gate.wait(t, gvr, harnessWaitBound)
	}
	return rw, dyn
}

// syncedWatcher is the #187 arms' helper, now backed by a real synced
// informer for gvr (see file comment).
func syncedWatcher(t *testing.T, gvr schema.GroupVersionResource) *ResourceWatcher {
	t.Helper()
	rw, _ := realWatcher(t, gvr)
	return rw
}

// listKindFor derives the fake client's list kind from the plural resource:
// "flexes" → "FlexList", "compositions" → "CompositionList".
func listKindFor(gvr schema.GroupVersionResource) string {
	r := gvr.Resource
	switch {
	case strings.HasSuffix(r, "ies"):
		r = strings.TrimSuffix(r, "ies") + "y"
	case strings.HasSuffix(r, "xes"):
		r = strings.TrimSuffix(r, "es")
	case strings.HasSuffix(r, "s"):
		r = strings.TrimSuffix(r, "s")
	}
	return strings.ToUpper(r[:1]) + r[1:] + "List"
}

// createObj creates a minimal object on the fake cluster and waits until the
// watcher's indexer holds it, so a subsequent probe reads objExists.
func createObj(t *testing.T, rw *ResourceWatcher, dyn dynamic.Interface, gvr schema.GroupVersionResource, ns, name, body string) {
	t.Helper()
	kind := strings.TrimSuffix(listKindFor(gvr), "List")
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvr.GroupVersion().String(),
		"kind":       kind,
		"metadata":   map[string]any{"namespace": ns, "name": name},
		"spec":       map[string]any{"body": body},
	}}
	if _, err := dyn.Resource(gvr).Namespace(ns).Create(context.Background(), u, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create %s %s/%s: %v", gvr.Resource, ns, name, err)
	}
	waitIndexer(t, rw, gvr, ns, name, true)
}

// deleteObj deletes the object on the fake cluster and waits until the
// watcher's indexer no longer holds it.
func deleteObj(t *testing.T, rw *ResourceWatcher, dyn dynamic.Interface, gvr schema.GroupVersionResource, ns, name string) {
	t.Helper()
	if err := dyn.Resource(gvr).Namespace(ns).Delete(context.Background(), name, metav1.DeleteOptions{}); err != nil {
		t.Fatalf("delete %s %s/%s: %v", gvr.Resource, ns, name, err)
	}
	waitIndexer(t, rw, gvr, ns, name, false)
}

// waitIndexer polls the real probe until the indexer agrees with `present`.
func waitIndexer(t *testing.T, rw *ResourceWatcher, gvr schema.GroupVersionResource, ns, name string, present bool) {
	t.Helper()
	want := objAbsent
	if present {
		want = objExists
	}
	deadline := time.Now().Add(harnessWaitBound)
	for time.Now().Before(deadline) {
		if rw.probeObjectState(gvr, ns, name) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("indexer for %s never reached %v for %s/%s within %s (got %v)", gvr, want, ns, name, harnessWaitBound, rw.probeObjectState(gvr, ns, name))
}

// waitQueueIdle waits until the dep-event queue has drained.
func waitQueueIdle(t *testing.T, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if DepWatchStatsSnapshot().DeleteQueueDepth == 0 {
			time.Sleep(20 * time.Millisecond) // let the in-flight item finish
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("dep-event queue did not drain within %s (depth=%d)", within, DepWatchStatsSnapshot().DeleteQueueDepth)
}
