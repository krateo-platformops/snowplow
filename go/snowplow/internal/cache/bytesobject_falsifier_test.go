// bytesobject_falsifier_test.go — Ship H1 pre-flight falsifier for
// FINDING 1 / AC-H1.2: the five indexer-read cast sites in watcher.go.
//
// Team rule feedback_falsifier_first_before_ship: this file is the
// silent-drop falsifier. Each of the five watcher read methods
// (GetObject, ListObjects, ListObjectsServable, GetTypedObject,
// ListTypedObjects) historically did a `, ok := obj.(*unstructured.
// Unstructured)` / `obj.(runtime.Object)` assert behind an `if ok`
// guard. A *bytesObject FAILS that bare assert — and because the guard
// just skips a failed cast, the object is SILENTLY DROPPED: no crash,
// an empty/short result. That is a correctness defect.
//
// THE FALSIFIER PROPERTY: with the indexer populated by N bytesObjects,
// every one of the five methods must return N non-nil objects. If ANY
// cast site is reverted to a bare `*unstructured.Unstructured` assert,
// that method returns 0 (or a short count) and its sub-test FAILS.
// When all five are converted to decode-on-access (decodeBytesObject /
// asRuntimeObject), all five pass.
//
// TestFalsifier_AllFiveCastSites_NoSilentDrop walks the production
// methods directly — it is not a unit test of a helper, it is the
// N-in-N-out gate the PM acceptance contract demands.
package cache

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	clientcache "k8s.io/client-go/tools/cache"
)

// fakeListWatch is an empty ListerWatcher — enough to construct a real
// SharedIndexInformer whose GetIndexer() we populate by hand. The
// informer is never Run(), so List/Watch are never invoked.
type fakeListWatch struct{}

func (fakeListWatch) List(metav1.ListOptions) (runtime.Object, error) {
	return &metav1.List{}, nil
}
func (fakeListWatch) Watch(metav1.ListOptions) (watch.Interface, error) {
	return watch.NewFake(), nil
}

// genericInformerForTest wraps a real SharedIndexInformer so it
// satisfies informers.GenericInformer (the type rw.informers stores).
type genericInformerForTest struct {
	inf clientcache.SharedIndexInformer
}

func (g *genericInformerForTest) Informer() clientcache.SharedIndexInformer { return g.inf }
func (g *genericInformerForTest) Lister() clientcache.GenericLister         { return nil }

var _ informers.GenericInformer = (*genericInformerForTest)(nil)

// newIndexerBackedWatcher builds a modeInformer ResourceWatcher whose
// single registered GVR is served by a real SharedIndexInformer with
// MetaNamespaceKeyFunc + the NamespaceIndex — exactly the informer's
// production indexer wiring.
//
// The informer is Run()-ed against an empty fake ListWatch and waited
// to HasSynced — so the four-conjunct servable gate passes (disco is
// nil, watchBroken empty, HasSynced true). The initial LIST is empty,
// so the test then populates the returned indexer by hand; the fake
// WATCH never emits, so the reflector never re-Replaces the store.
//
// Returns the watcher, its indexer, and a stop func the caller defers.
func newIndexerBackedWatcher(t *testing.T, gvr schema.GroupVersionResource) (*ResourceWatcher, clientcache.Indexer, func()) {
	t.Helper()

	inf := clientcache.NewSharedIndexInformer(
		fakeListWatch{},
		&metav1.PartialObjectMetadata{},
		0,
		clientcache.Indexers{clientcache.NamespaceIndex: clientcache.MetaNamespaceIndexFunc},
	)

	stopCh := make(chan struct{})
	go inf.Run(stopCh)
	if !clientcache.WaitForCacheSync(stopCh, inf.HasSynced) {
		close(stopCh)
		t.Fatal("informer failed to sync")
	}

	rw := &ResourceWatcher{
		mode:      modeInformer,
		informers: map[schema.GroupVersionResource]informers.GenericInformer{},
	}
	rw.informers[gvr] = &genericInformerForTest{inf: inf}

	return rw, inf.GetIndexer(), func() { close(stopCh) }
}

// TestFalsifier_AllFiveCastSites_NoSilentDrop — AC-H1.2 / FINDING 1.
//
// Populates the indexer with N bytesObjects and asserts each of the
// five watcher read methods returns N non-nil objects. A reverted cast
// site fails its sub-test with a count mismatch.
func TestFalsifier_AllFiveCastSites_NoSilentDrop(t *testing.T) {
	gvr := compositionGVR
	rw, idx, stop := newIndexerBackedWatcher(t, gvr)
	defer stop()

	const n = 25
	const ns = "bench-ns-falsifier"
	names := make([]string, 0, n)
	for i := 0; i < n; i++ {
		name := "comp-"
		for k := 0; k <= i; k++ {
			name += "z"
		}
		names = append(names, name)
		src := makeComposition(ns, name, "GithubScaffoldingWithCompositionPage")
		bo, err := newBytesObject(src)
		if err != nil {
			t.Fatalf("newBytesObject(%s): %v", name, err)
		}
		if err := idx.Add(bo); err != nil {
			t.Fatalf("idx.Add(%s) — bytesObject not indexable (FINDING 2): %v", name, err)
		}
	}

	// --- cast site 1/5: GetObject (watcher.go:1413) ---
	t.Run("GetObject", func(t *testing.T) {
		got := 0
		for _, name := range names {
			uns, ok := rw.GetObject(context.Background(), gvr, ns, name)
			if !ok || uns == nil {
				t.Errorf("GetObject(%s) silently dropped the bytesObject", name)
				continue
			}
			if uns.GetName() != name || uns.GetNamespace() != ns {
				t.Errorf("GetObject(%s) decoded wrong object: %s/%s", name, uns.GetNamespace(), uns.GetName())
			}
			got++
		}
		if got != n {
			t.Fatalf("GetObject: %d/%d objects returned — cast site 1/5 silently drops bytesObject", got, n)
		}
	})

	// --- cast site 2/5: ListObjects -> listFromIndexer (watcher.go:1467) ---
	t.Run("ListObjects", func(t *testing.T) {
		out := rw.ListObjects(gvr, ns)
		if len(out) != n {
			t.Fatalf("ListObjects: %d/%d objects — cast site 2/5 silently drops bytesObject", len(out), n)
		}
		for _, uns := range out {
			if uns == nil {
				t.Fatal("ListObjects returned a nil object")
			}
		}
	})

	// --- cast site 2/5 (shared): ListObjectsServable -> listFromIndexer ---
	t.Run("ListObjectsServable", func(t *testing.T) {
		out, served := rw.ListObjectsServable(context.Background(), gvr, ns)
		if !served {
			t.Fatal("ListObjectsServable not served — informer not vouched")
		}
		if len(out) != n {
			t.Fatalf("ListObjectsServable: %d/%d objects — cast site 2/5 silently drops bytesObject", len(out), n)
		}
	})

	// --- cast site 3/5: filterByNamespace (watcher.go:1647) ---
	// filterByNamespace runs only when the NamespaceIndex is absent.
	// Drive it directly with the indexer's full List to exercise the
	// namespace-filter cast site in isolation.
	t.Run("filterByNamespace", func(t *testing.T) {
		filtered := filterByNamespace(idx.List(), ns)
		if len(filtered) != n {
			t.Fatalf("filterByNamespace: %d/%d objects — cast site 3/5 silently drops bytesObject", len(filtered), n)
		}
		// The filtered items must still be the original bytesObjects,
		// not decoded — listFromIndexer decodes them afterwards.
		for _, it := range filtered {
			if _, ok := it.(*bytesObject); !ok {
				t.Fatalf("filterByNamespace returned %T, want *bytesObject preserved", it)
			}
		}
	})

	// --- cast site 4/5: GetTypedObject (watcher.go:1705) ---
	t.Run("GetTypedObject", func(t *testing.T) {
		got := 0
		for _, name := range names {
			obj, ok := rw.GetTypedObject(gvr, ns, name)
			if !ok || obj == nil {
				t.Errorf("GetTypedObject(%s) silently dropped the bytesObject", name)
				continue
			}
			got++
		}
		if got != n {
			t.Fatalf("GetTypedObject: %d/%d objects — cast site 4/5 silently drops bytesObject", got, n)
		}
	})

	// --- cast site 5/5: ListTypedObjects (watcher.go:1758) ---
	t.Run("ListTypedObjects", func(t *testing.T) {
		out := rw.ListTypedObjects(gvr, ns)
		if len(out) != n {
			t.Fatalf("ListTypedObjects: %d/%d objects — cast site 5/5 silently drops bytesObject", len(out), n)
		}
	})
}

// TestFalsifier_CastSites_MixedStore confirms the five methods are also
// correct when the indexer holds a MIX of *bytesObject and plain
// *unstructured.Unstructured (the realistic state — bytes routing is
// per-group, and a mid-rollout indexer could carry both). All objects,
// of either shape, must survive every read path.
func TestFalsifier_CastSites_MixedStore(t *testing.T) {
	gvr := compositionGVR
	rw, idx, stop := newIndexerBackedWatcher(t, gvr)
	defer stop()

	const ns = "bench-ns-mixed"
	const bytesCount = 12
	const plainCount = 8

	for i := 0; i < bytesCount; i++ {
		name := "b-" + string(rune('a'+i))
		bo, err := newBytesObject(makeComposition(ns, name, "K"))
		if err != nil {
			t.Fatalf("newBytesObject: %v", err)
		}
		if err := idx.Add(bo); err != nil {
			t.Fatalf("idx.Add bytes: %v", err)
		}
	}
	for i := 0; i < plainCount; i++ {
		name := "p-" + string(rune('a'+i))
		if err := idx.Add(makeComposition(ns, name, "K")); err != nil {
			t.Fatalf("idx.Add plain: %v", err)
		}
	}
	total := bytesCount + plainCount

	if out := rw.ListObjects(gvr, ns); len(out) != total {
		t.Fatalf("ListObjects mixed store: %d/%d", len(out), total)
	}
	if out := rw.ListTypedObjects(gvr, ns); len(out) != total {
		t.Fatalf("ListTypedObjects mixed store: %d/%d", len(out), total)
	}
	if out, served := rw.ListObjectsServable(context.Background(), gvr, ns); !served || len(out) != total {
		t.Fatalf("ListObjectsServable mixed store: served=%v len=%d/%d", served, len(out), total)
	}
}
