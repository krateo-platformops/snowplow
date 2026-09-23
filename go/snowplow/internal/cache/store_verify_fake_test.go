package cache

// store_verify_fake_test.go — the test infrastructure for #237 deliverable B,
// budgeted deliberately rather than as a footnote.
//
// # WHY A NEW FAKE WAS UNAVOIDABLE
//
// featuregate_watchlist_test.go disables WatchListClient for the WHOLE package,
// and its reason is correct: the existing fakes never emit the
// k8s.io/initial-events-end bookmark, so a watch-list reflector blocks in
// WaitForCacheSync until the go-test timeout. That TestMain is NOT edited here
// — clientfeaturestesting.SetFeatureDuringTest goes through Set, and Enabled
// consults the set-method map BEFORE the env map, so a per-test override wins
// over the package-wide env even though the env was read once at first use.
//
// fakeAuthoritativeSource is the fake that makes the opt-in usable: it honours
// SendInitialEvents by streaming one synthetic Added per object and then the
// bookmark, exactly as the apiserver's cacher does. Without it B could ship
// with five green arms having never executed the branch production takes.
//
// # WHAT THESE ARMS DRIVE, AND WHAT THEY DELIBERATELY DO NOT
//
// Every arm builds a REAL SharedIndexInformer over a REAL Reflector, with the
// decorator in the position production puts it. Nothing installs a crossed
// store by hand and nothing dispatches through a seam: a divergence exists here
// only because the authoritative set moved while the watch delivered no event,
// which is the actual boundary the defect crosses. Each arm performs at least
// two real reflector invocations — an initial sync, then a real
// re-establishment.

import (
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	clientcache "k8s.io/client-go/tools/cache"
)

// fakeObj is one object in the authoritative set.
type fakeObj struct {
	ns, name string
	rv, uid  string
}

func (o fakeObj) key() string {
	if o.ns == "" {
		return o.name
	}
	return o.ns + "/" + o.name
}

func (o fakeObj) unstructured(gvr schema.GroupVersionResource) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvr.GroupVersion().String(),
		"kind":       "Widget",
		"metadata": map[string]any{
			"name":            o.name,
			"resourceVersion": o.rv,
			"uid":             o.uid,
		},
	}}
	if o.ns != "" {
		_ = unstructured.SetNestedField(u.Object, o.ns, "metadata", "namespace")
	}
	return u
}

// fakeAuthoritativeSource is a ListerWatcher over a mutable authoritative
// object set. It is the apiserver in these arms.
//
// The whole point is that the set can be MUTATED WITHOUT EMITTING AN EVENT —
// that is what a lost watch event is, and it is why these arms test a real hole
// rather than a straw one.
type fakeAuthoritativeSource struct {
	gvr schema.GroupVersionResource

	mu      sync.Mutex
	objs    map[string]fakeObj
	rv      string
	watches []*fakeWatch

	// Observability for the arms.
	listCalls   int
	watchCalls  int
	listOpts    []metav1.ListOptions
	watchOpts   []metav1.ListOptions
	watchListed int // watches that carried SendInitialEvents
}

var _ clientcache.ListerWatcher = (*fakeAuthoritativeSource)(nil)

func newFakeSource(gvr schema.GroupVersionResource, objs ...fakeObj) *fakeAuthoritativeSource {
	s := &fakeAuthoritativeSource{gvr: gvr, objs: map[string]fakeObj{}, rv: "100"}
	for _, o := range objs {
		s.objs[o.key()] = o
	}
	return s
}

// setSilently replaces the authoritative set WITHOUT emitting watch events.
// This is the lost-event injection, and it is the only mutation these arms use.
func (s *fakeAuthoritativeSource) setSilently(objs ...fakeObj) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objs = map[string]fakeObj{}
	for _, o := range objs {
		s.objs[o.key()] = o
	}
	s.rv = bumpRV(s.rv)
}

func bumpRV(rv string) string {
	n := 0
	for _, c := range rv {
		if c < '0' || c > '9' {
			return rv + "1"
		}
		n = n*10 + int(c-'0')
	}
	n++
	return verifyItoa(n)
}

func verifyItoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func (s *fakeAuthoritativeSource) List(opts metav1.ListOptions) (runtime.Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls++
	s.listOpts = append(s.listOpts, opts)
	list := &unstructured.UnstructuredList{Object: map[string]any{
		"apiVersion": s.gvr.GroupVersion().String(),
		"kind":       "WidgetList",
		"metadata":   map[string]any{"resourceVersion": s.rv},
	}}
	for _, o := range s.objs {
		list.Items = append(list.Items, *o.unstructured(s.gvr))
	}
	return list, nil
}

func (s *fakeAuthoritativeSource) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	s.mu.Lock()
	s.watchCalls++
	s.watchOpts = append(s.watchOpts, opts)
	sendInitial := opts.SendInitialEvents != nil && *opts.SendInitialEvents
	if sendInitial {
		s.watchListed++
	}
	w := newFakeWatch()
	s.watches = append(s.watches, w)
	snapshot := make([]fakeObj, 0, len(s.objs))
	for _, o := range s.objs {
		snapshot = append(snapshot, o)
	}
	rv := s.rv
	s.mu.Unlock()

	if sendInitial {
		// Watch-list semantics, exactly as the apiserver's cacher does it: one
		// synthetic ADDED per object, then a Bookmark carrying
		// k8s.io/initial-events-end. The reflector accumulates the ADDEDs into
		// its temporaryStore and Replaces only when the bookmark arrives.
		go func() {
			for _, o := range snapshot {
				if !w.send(watch.Event{Type: watch.Added, Object: o.unstructured(s.gvr)}) {
					return
				}
			}
			bookmark := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": s.gvr.GroupVersion().String(),
				"kind":       "Widget",
				"metadata": map[string]any{
					"resourceVersion": rv,
					"annotations":     map[string]any{metav1.InitialEventsAnnotationKey: "true"},
				},
			}}
			w.send(watch.Event{Type: watch.Bookmark, Object: bookmark})
		}()
	}
	return w, nil
}

// breakWatches ends every open watch, which makes the reflector's
// ListAndWatchWithContext return and BackoffUntil re-invoke it — a REAL
// re-establishment, and the second reflector invocation every arm needs.
func (s *fakeAuthoritativeSource) breakWatches() {
	s.mu.Lock()
	ws := s.watches
	s.watches = nil
	s.mu.Unlock()
	for _, w := range ws {
		w.Stop()
	}
}

func (s *fakeAuthoritativeSource) counts() (listCalls, watchCalls, watchListed int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listCalls, s.watchCalls, s.watchListed
}

// recordedListOptions copies the LIST options the source was called with, for
// wire-shape assertions.
func (s *fakeAuthoritativeSource) recordedListOptions() []metav1.ListOptions {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]metav1.ListOptions(nil), s.listOpts...)
}

// fakeWatch is a watch.Interface whose Stop closes the result channel, which is
// what makes the reflector treat it as an ended watch.
type fakeWatch struct {
	ch   chan watch.Event
	stop chan struct{}
	once sync.Once
}

func newFakeWatch() *fakeWatch {
	return &fakeWatch{ch: make(chan watch.Event), stop: make(chan struct{})}
}

func (w *fakeWatch) ResultChan() <-chan watch.Event { return w.ch }

func (w *fakeWatch) Stop() {
	w.once.Do(func() {
		close(w.stop)
		close(w.ch)
	})
}

func (w *fakeWatch) send(ev watch.Event) bool {
	select {
	case <-w.stop:
		return false
	case w.ch <- ev:
		return true
	}
}

// verifiedInformer builds the production shape: the verifying decorator over
// the source, a real SharedIndexInformer over the decorator, bound and running.
func verifiedInformer(t *testing.T, src *fakeAuthoritativeSource) (clientcache.SharedIndexInformer, *verifyingListerWatcher) {
	t.Helper()
	v := newVerifyingListerWatcher(src, src.gvr)
	inf := clientcache.NewSharedIndexInformerWithOptions(
		v,
		&unstructured.Unstructured{},
		clientcache.SharedIndexInformerOptions{
			ResyncPeriod:      0,
			Indexers:          clientcache.Indexers{clientcache.NamespaceIndex: clientcache.MetaNamespaceIndexFunc},
			ObjectDescription: src.gvr.String(),
		},
	)
	v.bind(inf)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		inf.Run(stop)
	}()
	t.Cleanup(func() {
		close(stop)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("informer did not stop")
		}
	})
	if !clientcache.WaitForCacheSync(stop, inf.HasSynced) {
		t.Fatal("informer never synced")
	}
	return inf, v
}

// waitFor polls until cond holds or the bound expires. Used instead of a fixed
// sleep so an arm fails on the assertion it is about rather than on timing.
func waitForVerify(t *testing.T, what string, bound time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(bound)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", bound, what)
}

// verifyBound is the wait bound for a reflector re-establishment. The
// reflector's BackoffUntil starts at ~800 ms, so this must comfortably exceed
// one backoff step.
const verifyBound = 15 * time.Second

// storeRV reads one object's resourceVersion straight out of the informer's
// indexer — the assertion that matters, as opposed to a counter.
func storeRV(t *testing.T, inf clientcache.SharedIndexInformer, key string) (string, bool) {
	t.Helper()
	obj, ok, err := inf.GetIndexer().GetByKey(key)
	if err != nil || !ok || obj == nil {
		return "", false
	}
	u, castOK := obj.(*unstructured.Unstructured)
	if !castOK {
		t.Fatalf("indexer holds %T, want *unstructured.Unstructured", obj)
	}
	return u.GetResourceVersion(), true
}

// divergenceCounts reads the four confirmed classes for one site.
func divergenceCounts(site string) (lostUpdate, lostDelete, lostAdd, uidMismatch uint64) {
	return storeDivergentLostUpdate[site].Load(),
		storeDivergentLostDelete[site].Load(),
		storeDivergentLostAdd[site].Load(),
		storeDivergentUIDMismatch[site].Load()
}

// resetVerificationForArm gives an arm a clean instrument and restores it after.
func resetVerificationForArm(t *testing.T) {
	t.Helper()
	ResetStoreVerificationStatsForTest()
	t.Cleanup(ResetStoreVerificationStatsForTest)
}
