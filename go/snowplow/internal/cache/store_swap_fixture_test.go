package cache

// store_swap_fixture_test.go — #870: the fixture the build-and-swap arms run
// on. A REAL ResourceWatcher whose informers really list, sync and deliver,
// over a fake dynamic client where:
//
//   - every WATCH of the arm's GVR is a separate FakeWatcher the arm can drive
//     (index 0 is the live copy's, index 1 the first candidate's, …), and none
//     delivers anything unless the arm pushes it — a store can be made to
//     diverge from the cluster exactly as #237 measured it;
//   - the cluster's truth is the fake's tracker, which every LIST reads;
//   - the n-th LIST of the GVR can be HELD outside the fake's reactor lock;
//   - a recording consumer handler (a registered handler extension) sees every
//     delivery the adapter makes, with its kind, key, resourceVersion and the
//     object's spec value, and can BLOCK a delivery (to hold a listener).
//
// The GVR's group is navigation-discovered, so its informer is OWNED (the
// standalone path, the production streaming path's stand-in without a
// *rest.Config).

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	metadatafake "k8s.io/client-go/metadata/fake"
	k8stesting "k8s.io/client-go/testing"
	clientcache "k8s.io/client-go/tools/cache"
)

const swapNS = "swap"

// swapEvent is one delivery the recording consumer saw.
type swapEvent struct {
	kind  string // add | update | delete
	key   string
	rv    string
	v     string // spec.v of the delivered object ("" for a delete)
	oldRV string // the old side's resourceVersion (update, delete)
	inst  string // filled by the fixture: never compared
}

func (e swapEvent) String() string {
	return fmt.Sprintf("%s(%s rv=%s v=%s old=%s)", e.kind, e.key, e.rv, e.v, e.oldRV)
}

// swapRecorder is the recording consumer. block, when set, is called for every
// delivery BEFORE it is recorded; it may block (to hold the listener).
type swapRecorder struct {
	mu     sync.Mutex
	events []swapEvent
	block  func(e swapEvent)
}

func (r *swapRecorder) record(e swapEvent) {
	r.mu.Lock()
	b := r.block
	r.mu.Unlock()
	if b != nil {
		b(e)
	}
	r.mu.Lock()
	r.events = append(r.events, e)
	r.mu.Unlock()
}

func (r *swapRecorder) setBlock(b func(e swapEvent)) {
	r.mu.Lock()
	r.block = b
	r.mu.Unlock()
}

func (r *swapRecorder) all() []swapEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]swapEvent(nil), r.events...)
}

func (r *swapRecorder) forKey(key string) []swapEvent {
	var out []swapEvent
	for _, e := range r.all() {
		if e.key == key {
			out = append(out, e)
		}
	}
	return out
}

func (r *swapRecorder) since(n int) []swapEvent {
	all := r.all()
	if n > len(all) {
		return nil
	}
	return all[n:]
}

func specV(obj any) string {
	b, ok := itemBytes(obj)
	if !ok {
		return ""
	}
	var m struct {
		Spec struct {
			V string `json:"v"`
		} `json:"spec"`
	}
	_ = json.Unmarshal(b, &m)
	return m.Spec.V
}

func rvOf(obj any) string { return stateOf(obj, obj != nil).rv }

func keyOf(obj any) string {
	k, _ := clientcache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	return k
}

func (r *swapRecorder) handler() clientcache.ResourceEventHandler {
	return clientcache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			r.record(swapEvent{kind: "add", key: keyOf(obj), rv: rvOf(obj), v: specV(obj)})
		},
		UpdateFunc: func(oldObj, newObj any) {
			r.record(swapEvent{kind: "update", key: keyOf(newObj), rv: rvOf(newObj), v: specV(newObj), oldRV: rvOf(oldObj)})
		},
		DeleteFunc: func(obj any) {
			if t, ok := obj.(clientcache.DeletedFinalStateUnknown); ok {
				obj = t.Obj
			}
			r.record(swapEvent{kind: "delete", key: keyOf(obj), oldRV: rvOf(obj)})
		},
	}
}

// swapFix is one arm's world.
type swapFix struct {
	t    *testing.T
	gvr  schema.GroupVersionResource
	fake *dynamicfake.FakeDynamicClient
	rw   *ResourceWatcher
	rec  *swapRecorder

	mu       sync.Mutex
	watchers []*watch.FakeWatcher
	holds    map[int64]chan struct{}
	lists    int64
	listErr  func(n int64) error
	extra    []schema.GroupVersionResource
	meta     *metadatafake.FakeMetadataClient // the metadata-only variant's cluster
	hdyn     holdListDynamic                  // counts every LIST of the GVR as it is ISSUED
	factory  bool                             // the GVR's informer comes from the shared factory (not owned)
	watchErr atomic.Bool                      // every new WATCH of the GVR fails
}

type swapFixOpt func(*swapFix)

// withListErr makes the n-th LIST of the GVR fail with the returned error.
func withListErr(f func(n int64) error) swapFixOpt { return func(s *swapFix) { s.listErr = f } }

// withFactoryBuilt leaves the GVR's group off the navigation-discovered set,
// so its informer comes from the shared dynamic factory (not owned).
func withFactoryBuilt() swapFixOpt { return func(s *swapFix) { s.factory = true } }

// withExtraGVR makes the fake cluster serve another GVR too (silent watch,
// tracker LIST).
func withExtraGVR(g schema.GroupVersionResource) swapFixOpt {
	return func(s *swapFix) { s.extra = append(s.extra, g) }
}

// swapObj is one object of the fixture GVR.
func swapObj(gvr schema.GroupVersionResource, name, rv, v string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvr.GroupVersion().String(),
		"kind":       "Panel",
		"metadata": map[string]any{
			"namespace": swapNS, "name": name, "resourceVersion": rv, "uid": "uid-" + name,
		},
		"spec": map[string]any{"v": v},
	}}
}

// newSwapFix builds the fixture with objs seeded and the GVR registered and
// servable. The recording consumer is attached to every instance of the GVR.
func newSwapFix(t *testing.T, gvr schema.GroupVersionResource, objs []*unstructured.Unstructured, opts ...swapFixOpt) *swapFix {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	resetVerificationForArm(t)
	ResetListCellsForTest()
	t.Cleanup(ResetListCellsForTest)
	f := &swapFix{t: t, gvr: gvr, rec: &swapRecorder{}, holds: map[int64]chan struct{}{}}
	for _, o := range opts {
		o(f)
	}

	// The recording consumer, through the real extension registry (so it is
	// wrapped by the real adapter on every instance).
	saved := HandlerExtensionsSnapshot()
	ResetHandlerExtensionsForTest()
	for _, e := range saved {
		RegisterHandlerExtension(e)
	}
	RegisterHandlerExtension(HandlerExtension{
		Name:      "swap-recorder-" + t.Name(),
		Predicate: func(g schema.GroupVersionResource) bool { return g == gvr },
		Handlers: func(*ResourceWatcher, schema.GroupVersionResource) clientcache.ResourceEventHandler {
			return f.rec.handler()
		},
	})
	t.Cleanup(func() {
		ResetHandlerExtensionsForTest()
		for _, e := range saved {
			RegisterHandlerExtension(e)
		}
	})

	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	kinds := rbacListKinds()
	kinds[gvr] = "PanelList"
	for _, g := range f.extra {
		kinds[g] = "PanelList"
	}
	f.fake = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, kinds)
	for _, o := range objs {
		if err := f.fake.Tracker().Create(gvr, o.DeepCopy(), o.GetNamespace()); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	f.fake.PrependWatchReactor("*", func(a k8stesting.Action) (bool, watch.Interface, error) {
		if a.GetResource() == gvr && f.watchErr.Load() {
			return true, nil, apierrors.NewInternalError(errors.New("watch refused"))
		}
		w := watch.NewFakeWithChanSize(256, false)
		t.Cleanup(w.Stop)
		if a.GetResource() == gvr {
			f.mu.Lock()
			f.watchers = append(f.watchers, w)
			f.mu.Unlock()
		}
		return true, w, nil
	})
	f.fake.PrependReactor("list", gvr.Resource, func(a k8stesting.Action) (bool, k8sruntime.Object, error) {
		f.mu.Lock()
		f.lists++
		n := f.lists
		le := f.listErr
		f.mu.Unlock()
		if le != nil {
			if err := le(n); err != nil {
				return true, nil, err
			}
		}
		obj, err := f.fake.Tracker().List(gvr, gvr.GroupVersion().WithKind("Panel"), a.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		if l, ok := obj.(*unstructured.UnstructuredList); ok {
			l.SetResourceVersion(clusterListRV)
		}
		return true, obj, nil
	})

	if !f.factory {
		AddNavigationDiscoveredGroup(gvr.Group)
	}
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)

	f.hdyn = newHoldListDynamic(f.fake, gvr, func(n int64) <-chan struct{} {
		f.mu.Lock()
		defer f.mu.Unlock()
		if ch, ok := f.holds[n]; ok {
			return ch
		}
		return nil
	})
	rw, err := NewResourceWatcher(context.Background(), f.hdyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	f.rw = rw
	t.Cleanup(rw.Stop)
	prev := Global()
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(prev) })

	_, syncCh := rw.EnsureResourceType(gvr)
	select {
	case <-syncCh:
	case <-time.After(30 * time.Second):
		t.Fatal("the GVR's informer never synced")
	}
	f.waitFor("the GVR to be servable", func() bool { return rw.IsServable(gvr) })
	f.waitFor("the live copy's watch", func() bool { return f.watcherCount() >= 1 })
	f.waitFor("the initial list to reach the consumer", func() bool { return len(f.rec.all()) >= len(objs) })
	return f
}

// newSwapFixCluster is newSwapFix for objects of any namespace (a
// cluster-scoped GVR's objects have none).
func newSwapFixCluster(t *testing.T, gvr schema.GroupVersionResource, objs ...*unstructured.Unstructured) *swapFix {
	t.Helper()
	return newSwapFix(t, gvr, objs)
}

// holdList holds the n-th LIST of the GVR (1-based: 1 is the live copy's).
// Returns the release func.
func (f *swapFix) holdList(n int64) func() {
	ch := make(chan struct{})
	f.mu.Lock()
	f.holds[n] = ch
	f.mu.Unlock()
	var once sync.Once
	rel := func() { once.Do(func() { close(ch) }) }
	f.t.Cleanup(rel)
	return rel
}

// listCount is how many LISTs of the GVR have been ISSUED (a held one
// counts as soon as it is issued).
func (f *swapFix) listCount() int64 {
	if f.hdyn.lists == nil {
		return 0
	}
	return f.hdyn.lists.Load()
}

func (f *swapFix) watcherCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.watchers)
}

// watcher returns the i-th watch of the GVR (0 = the live copy's).
func (f *swapFix) watcher(i int) *watch.FakeWatcher {
	f.waitFor(fmt.Sprintf("watch #%d", i), func() bool { return f.watcherCount() > i })
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.watchers[i]
}

// clusterSet writes the cluster's truth (the tracker) without any watch
// delivering it: the store diverges.
func (f *swapFix) clusterSet(o *unstructured.Unstructured) {
	f.t.Helper()
	if f.meta != nil {
		pm := &metav1.PartialObjectMetadata{
			TypeMeta:   metav1.TypeMeta{APIVersion: f.gvr.GroupVersion().String(), Kind: "Panel"},
			ObjectMeta: metav1.ObjectMeta{Namespace: o.GetNamespace(), Name: o.GetName(), ResourceVersion: o.GetResourceVersion(), UID: "u"},
		}
		if err := f.meta.Tracker().Update(f.gvr, pm, o.GetNamespace()); err != nil {
			f.t.Fatalf("metadata cluster set: %v", err)
		}
		return
	}
	if err := f.fake.Tracker().Update(f.gvr, o.DeepCopy(), o.GetNamespace()); err != nil {
		if err := f.fake.Tracker().Create(f.gvr, o.DeepCopy(), o.GetNamespace()); err != nil {
			f.t.Fatalf("cluster set: %v", err)
		}
	}
}

func (f *swapFix) clusterDelete(name string) {
	f.t.Helper()
	if err := f.fake.Tracker().Delete(f.gvr, swapNS, name); err != nil {
		f.t.Fatalf("cluster delete: %v", err)
	}
}

// live is the GVR's live instance.
func (f *swapFix) live() *storeInstance { return f.rw.liveInstance(f.gvr) }

// storeObj reads the live store's object for key.
func (f *swapFix) storeObj(key string) (any, bool) {
	in := f.live()
	if in == nil {
		return nil, false
	}
	return in.storeGet(key)
}

func (f *swapFix) waitFor(what string, cond func() bool) {
	f.t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !cond() {
		f.t.Fatalf("timed out waiting for %s", what)
	}
}

// request asks for a build with reason and keys.
func (f *swapFix) request(reason string, keys ...string) {
	f.t.Helper()
	if !f.rw.requestStoreBuild(f.gvr, reason, keys) {
		f.t.Fatalf("the %s build request was refused", reason)
	}
}

// awaitSwaps waits until n swaps have completed their delivery (step 6), read
// as "the build slot is empty, the live instance has passed step 3 and every
// key of M has been offered".
func (f *swapFix) awaitSwaps(n uint64) {
	f.t.Helper()
	f.waitFor(fmt.Sprintf("%d swap(s)", n), func() bool {
		if StoreSwapStatsSnapshot().Swaps < n {
			return false
		}
		f.rw.mu.RLock()
		defer f.rw.mu.RUnlock()
		in := f.rw.instances[f.gvr]
		return in != nil && in.passedSt3 && f.rw.builds[f.gvr] == nil && len(f.rw.retiring[f.gvr]) == 0
	})
}

// settle waits until the consumer has received n events in total.
func (f *swapFix) settle(n int) {
	f.t.Helper()
	f.waitFor(fmt.Sprintf("%d consumer events (have %d)", n, len(f.rec.all())), func() bool { return len(f.rec.all()) >= n })
}

func keysOf(evs []swapEvent) string {
	var out []string
	for _, e := range evs {
		out = append(out, e.String())
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

// swapGVR returns a fresh GVR per arm, so no package-global per-GVR state
// (epochs, list cells, the comparator record) leaks between arms.
func swapGVR(t *testing.T) schema.GroupVersionResource {
	name := strings.ToLower(strings.NewReplacer("/", "", "_", "", "-", "").Replace(t.Name()))
	if len(name) > 40 {
		name = name[len(name)-40:]
	}
	return schema.GroupVersionResource{Group: "swap870.krateo.io", Version: "v1", Resource: "p" + name}
}

func swapKey(name string) string { return swapNS + "/" + name }

// unused guard for imports kept for arms in sibling files.
var _ = metav1.ListOptions{}
