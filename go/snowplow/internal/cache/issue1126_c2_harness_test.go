// issue1126_c2_harness_test.go — 1.12.6 C2/C3 harness: a REAL synced watcher
// over a fake dynamic client whose watch stream can be told to SWALLOW
// specific Deleted events (modelling a lost informer event: the tracker
// still deletes the object, so the next LIST omits it, but the running
// informer never hears about it), and whose LIST can be HELD (modelling a
// relisted informer that never syncs). The CRD meta-GVR sync channel is
// pre-closed so the real CRD lifecycle handlers pass the post-sync gate and
// the real schema relist fires. Nothing about the relist, the bridge, the
// worker or the probe is simulated.
//
// Deliberately uses ONLY API that exists on main (plus the C1 harness), so
// the RED-on-main probe file can compile against main with this harness.

package cache

import (
	"context"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
)

// c2Faults is the fault injector shared by the harness pieces.
type c2Faults struct {
	mu             sync.Mutex
	swallowDeletes map[string]bool // object name → drop its Deleted watch events
	holdList       chan struct{}   // when non-nil, LIST blocks until it is closed
}

func (f *c2Faults) swallow(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.swallowDeletes == nil {
		f.swallowDeletes = map[string]bool{}
	}
	f.swallowDeletes[name] = true
}

func (f *c2Faults) shouldSwallow(ev watch.Event) bool {
	if ev.Type != watch.Deleted {
		return false
	}
	u, ok := ev.Object.(*unstructured.Unstructured)
	if !ok {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.swallowDeletes[u.GetName()]
}

// holdLists makes every subsequent LIST on the fake client block until the
// returned release func is called (the relisted informer can never sync).
func (f *c2Faults) holdLists() (release func()) {
	f.mu.Lock()
	ch := make(chan struct{})
	f.holdList = ch
	f.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			f.mu.Lock()
			f.holdList = nil
			f.mu.Unlock()
			close(ch)
		})
	}
}

func (f *c2Faults) listGate() chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.holdList
}

// filteringWatch proxies a tracker watch, dropping the events the faults say
// to swallow. Stop is forwarded; the output channel closes when the inner
// channel closes.
type filteringWatch struct {
	inner watch.Interface
	out   chan watch.Event
	f     *c2Faults
}

func newFilteringWatch(inner watch.Interface, f *c2Faults) *filteringWatch {
	fw := &filteringWatch{inner: inner, out: make(chan watch.Event, 64), f: f}
	go func() {
		defer close(fw.out)
		for ev := range inner.ResultChan() {
			if f.shouldSwallow(ev) {
				continue
			}
			fw.out <- ev
		}
	}()
	return fw
}

func (fw *filteringWatch) Stop()                          { fw.inner.Stop() }
func (fw *filteringWatch) ResultChan() <-chan watch.Event { return fw.out }

// c2Watcher builds the real watcher over a faultable fake cluster, registers
// gvr and waits for its informer to sync AND for its watch to be registered
// on the tracker (the C1 harness gap), pre-closes the CRD meta-GVR syncCh so
// the CRD lifecycle handlers fire, and installs rw as Global() so the relist
// (which reads Global()) targets it.
func c2Watcher(t *testing.T, gvr schema.GroupVersionResource) (*ResourceWatcher, dynamic.Interface, *c2Faults) {
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
		gvr: listKindFor(gvr),
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds)
	faults := &c2Faults{}
	gate := newWatchGate()
	dyn.PrependWatchReactor("*", func(action k8stesting.Action) (bool, watch.Interface, error) {
		agvr := action.GetResource()
		w, err := dyn.Tracker().Watch(agvr, action.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		gate.mu.Lock()
		gate.seen[agvr]++
		gate.cond.Broadcast()
		gate.mu.Unlock()
		return true, newFilteringWatch(w, faults), nil
	})
	dyn.PrependReactor("list", "*", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		if ch := faults.listGate(); ch != nil {
			<-ch
		}
		return false, nil, nil // not handled → the tracker serves the LIST
	})

	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("NewResourceWatcher returned nil with CACHE_ENABLED=true")
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})

	_, syncCh := rw.EnsureResourceType(gvr)
	select {
	case <-syncCh:
	case <-time.After(harnessWaitBound):
		t.Fatalf("informer for %s did not sync within %s", gvr, harnessWaitBound)
	}
	if !rw.IsServable(gvr) {
		t.Fatalf("precondition: %s registered+synced but not servable", gvr)
	}
	gate.wait(t, gvr, harnessWaitBound)

	// The CRD meta-GVR handlers are invoked directly by the arms; only a
	// CLOSED syncCh is needed so addEventPostSync lets the ADD through.
	crdSync := make(chan struct{})
	close(crdSync)
	rw.mu.Lock()
	if rw.syncCh == nil {
		rw.syncCh = map[schema.GroupVersionResource]chan struct{}{}
	}
	rw.syncCh[CRDGVRForTest()] = crdSync
	rw.mu.Unlock()

	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })
	return rw, dyn, faults
}

// c2WaitGone polls until the store no longer holds key, or the bound lapses.
func c2WaitGone(store *ResolvedCacheStore, key string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if _, alive := store.Get(key); !alive {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	_, alive := store.Get(key)
	return !alive
}
