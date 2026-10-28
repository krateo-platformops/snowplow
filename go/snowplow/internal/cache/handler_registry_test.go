// handler_registry_test.go — Ship 0 / 0.30.222: -race-covered unit tests
// for the declarative handler-extension registry. Package internal so
// tests can reach RegisterHandlerExtension + ResetHandlerExtensionsForTest
// + the unexported attach helper.

package cache

import (
	"sync"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
	clientcache "k8s.io/client-go/tools/cache"
)

// fakeAttacher implements eventHandlerAttacher just enough to record an
// AddEventHandler invocation and optionally fail the first call. The
// narrow eventHandlerAttacher interface (handler_registry.go) is the
// reason we don't need to implement the much larger SharedIndexInformer
// shape here.
type fakeAttacher struct {
	addCount atomic.Int32
	failNext atomic.Bool
}

func (f *fakeAttacher) AddEventHandler(_ clientcache.ResourceEventHandler) (clientcache.ResourceEventHandlerRegistration, error) {
	if f.failNext.Swap(false) {
		return nil, errFakeAddFailed{}
	}
	f.addCount.Add(1)
	return fakeRegistration{}, nil
}

type fakeRegistration struct{}

func (fakeRegistration) HasSynced() bool { return true }

// HasSyncedChecker completes the client-go v0.36 registration interface: the
// fake is synced from the start, so its checker is already done.
func (fakeRegistration) HasSyncedChecker() clientcache.DoneChecker { return fakeDoneChecker{} }

// syncedDone is the closed channel every fakeDoneChecker reports.
var syncedDone = func() chan struct{} { c := make(chan struct{}); close(c); return c }()

type fakeDoneChecker struct{}

func (fakeDoneChecker) Name() string          { return "fakeRegistration" }
func (fakeDoneChecker) Done() <-chan struct{} { return syncedDone }

type errFakeAddFailed struct{}

func (errFakeAddFailed) Error() string { return "fake add failed" }

// isolateHandlerExtensionsForTest empties the registry for the calling arm
// and RESTORES the package's init()-registered extensions afterwards. The
// arms used to clean up with ResetHandlerExtensionsForTest, which left the
// registry EMPTY for every later test in the package: every watcher built
// after this file ran had no typed-RBAC snapshot handlers, so RBAC events
// never scheduled a rebuild or recorded a sub-gen bump (found by #703's arms,
// which drive exactly that path).
func isolateHandlerExtensionsForTest(t *testing.T) {
	t.Helper()
	saved := HandlerExtensionsSnapshot()
	ResetHandlerExtensionsForTest()
	t.Cleanup(func() {
		ResetHandlerExtensionsForTest()
		for _, ext := range saved {
			RegisterHandlerExtension(ext)
		}
	})
}

// dummyHandlers returns a stub event-handler set the fake informer can
// accept.
func dummyHandlers(_ *ResourceWatcher, _ schema.GroupVersionResource) clientcache.ResourceEventHandler {
	return clientcache.ResourceEventHandlerFuncs{}
}

func TestRegisterHandlerExtension_Registers(t *testing.T) {
	isolateHandlerExtensionsForTest(t)

	gvrA := schema.GroupVersionResource{Group: "a", Version: "v1", Resource: "as"}

	RegisterHandlerExtension(HandlerExtension{
		Name:      "test.extension_a",
		Predicate: func(g schema.GroupVersionResource) bool { return g == gvrA },
		Handlers:  dummyHandlers,
	})

	snap := HandlerExtensionsSnapshot()
	if len(snap) != 1 {
		t.Fatalf("snapshot len = %d, want 1", len(snap))
	}
	if snap[0].Name != "test.extension_a" {
		t.Fatalf("name = %q, want test.extension_a", snap[0].Name)
	}
}

func TestRegisterHandlerExtension_DuplicateNamePanics(t *testing.T) {
	isolateHandlerExtensionsForTest(t)

	gvr := schema.GroupVersionResource{Group: "a", Version: "v1", Resource: "as"}

	RegisterHandlerExtension(HandlerExtension{
		Name:      "test.dup",
		Predicate: func(g schema.GroupVersionResource) bool { return g == gvr },
		Handlers:  dummyHandlers,
	})

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("duplicate name must panic")
		}
	}()
	RegisterHandlerExtension(HandlerExtension{
		Name:      "test.dup",
		Predicate: func(g schema.GroupVersionResource) bool { return g == gvr },
		Handlers:  dummyHandlers,
	})
}

func TestRegisterHandlerExtension_EmptyNamePanics(t *testing.T) {
	isolateHandlerExtensionsForTest(t)

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("empty name must panic")
		}
	}()
	RegisterHandlerExtension(HandlerExtension{
		Name:      "",
		Predicate: func(_ schema.GroupVersionResource) bool { return true },
		Handlers:  dummyHandlers,
	})
}

func TestRegisterHandlerExtension_NilPredicatePanics(t *testing.T) {
	isolateHandlerExtensionsForTest(t)

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("nil predicate must panic")
		}
	}()
	RegisterHandlerExtension(HandlerExtension{
		Name:      "nil.pred",
		Predicate: nil,
		Handlers:  dummyHandlers,
	})
}

func TestRegisterHandlerExtension_NilHandlersPanics(t *testing.T) {
	isolateHandlerExtensionsForTest(t)

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("nil handlers must panic")
		}
	}()
	RegisterHandlerExtension(HandlerExtension{
		Name:      "nil.handlers",
		Predicate: func(_ schema.GroupVersionResource) bool { return true },
		Handlers:  nil,
	})
}

// TestAttachMatchingHandlerExtensions_PredicateMatching asserts that only
// extensions whose predicate matches the GVR fire, and that the per-name
// counter increments by exactly 1 per attach.
func TestAttachMatchingHandlerExtensions_PredicateMatching(t *testing.T) {
	isolateHandlerExtensionsForTest(t)

	gvrA := schema.GroupVersionResource{Group: "a", Version: "v1", Resource: "as"}
	gvrB := schema.GroupVersionResource{Group: "b", Version: "v1", Resource: "bs"}

	RegisterHandlerExtension(HandlerExtension{
		Name:      "ext.a_only",
		Predicate: func(g schema.GroupVersionResource) bool { return g == gvrA },
		Handlers:  dummyHandlers,
	})
	RegisterHandlerExtension(HandlerExtension{
		Name:      "ext.b_only",
		Predicate: func(g schema.GroupVersionResource) bool { return g == gvrB },
		Handlers:  dummyHandlers,
	})
	RegisterHandlerExtension(HandlerExtension{
		Name:      "ext.both",
		Predicate: func(g schema.GroupVersionResource) bool { return g == gvrA || g == gvrB },
		Handlers:  dummyHandlers,
	})

	// Attach for gvrA — ext.a_only + ext.both should fire (2), ext.b_only must not.
	attA := &fakeAttacher{}
	got := attachMatchingHandlerExtensions(nil, gvrA, attA)
	if got != 2 {
		t.Fatalf("attached = %d, want 2 (ext.a_only + ext.both for gvrA)", got)
	}
	if v := attA.addCount.Load(); v != 2 {
		t.Fatalf("AddEventHandler invocations = %d, want 2", v)
	}
	if HandlerExtensionsAttachedCount("ext.a_only") != 1 {
		t.Fatalf("counter ext.a_only = %d, want 1", HandlerExtensionsAttachedCount("ext.a_only"))
	}
	if HandlerExtensionsAttachedCount("ext.b_only") != 0 {
		t.Fatalf("counter ext.b_only = %d, want 0 (predicate didn't match gvrA)", HandlerExtensionsAttachedCount("ext.b_only"))
	}
	if HandlerExtensionsAttachedCount("ext.both") != 1 {
		t.Fatalf("counter ext.both = %d, want 1", HandlerExtensionsAttachedCount("ext.both"))
	}

	// Attach for gvrB — ext.b_only + ext.both should fire.
	attB := &fakeAttacher{}
	got = attachMatchingHandlerExtensions(nil, gvrB, attB)
	if got != 2 {
		t.Fatalf("attached for gvrB = %d, want 2", got)
	}
	if HandlerExtensionsAttachedCount("ext.b_only") != 1 {
		t.Fatalf("counter ext.b_only post-gvrB = %d, want 1", HandlerExtensionsAttachedCount("ext.b_only"))
	}
	if HandlerExtensionsAttachedCount("ext.both") != 2 {
		t.Fatalf("counter ext.both post-gvrB = %d, want 2 (gvrA + gvrB)", HandlerExtensionsAttachedCount("ext.both"))
	}
}

// TestAttachMatchingHandlerExtensions_EmptyRegistry is the no-op contract
// — an attach call against an empty registry must succeed with zero
// attachments and no panic.
func TestAttachMatchingHandlerExtensions_EmptyRegistry(t *testing.T) {
	isolateHandlerExtensionsForTest(t)

	gvr := schema.GroupVersionResource{Group: "a", Version: "v1", Resource: "as"}
	att := &fakeAttacher{}
	got := attachMatchingHandlerExtensions(nil, gvr, att)
	if got != 0 {
		t.Fatalf("empty-registry attached = %d, want 0", got)
	}
}

// TestAttachMatchingHandlerExtensions_AttachFailureDoesNotIncrementCounter
// verifies the contract: only successful AddEventHandler returns tick the
// counter.
func TestAttachMatchingHandlerExtensions_AttachFailureDoesNotIncrementCounter(t *testing.T) {
	isolateHandlerExtensionsForTest(t)

	gvr := schema.GroupVersionResource{Group: "a", Version: "v1", Resource: "as"}

	RegisterHandlerExtension(HandlerExtension{
		Name:      "ext.fail_first",
		Predicate: func(_ schema.GroupVersionResource) bool { return true },
		Handlers:  dummyHandlers,
	})

	att := &fakeAttacher{}
	att.failNext.Store(true) // first AddEventHandler returns an error
	got := attachMatchingHandlerExtensions(nil, gvr, att)
	if got != 0 {
		t.Fatalf("attached on failed-add = %d, want 0", got)
	}
	if HandlerExtensionsAttachedCount("ext.fail_first") != 0 {
		t.Fatalf("counter ext.fail_first = %d, want 0 (add failed)", HandlerExtensionsAttachedCount("ext.fail_first"))
	}
}

// TestRegisterHandlerExtension_ConcurrentAttach is the -race smoke test:
// many goroutines call attachMatchingHandlerExtensions against a stable
// registry simultaneously. The counter sum must equal the call count
// (no lost updates).
func TestRegisterHandlerExtension_ConcurrentAttach(t *testing.T) {
	isolateHandlerExtensionsForTest(t)

	gvr := schema.GroupVersionResource{Group: "race", Version: "v1", Resource: "rs"}
	RegisterHandlerExtension(HandlerExtension{
		Name:      "ext.race",
		Predicate: func(_ schema.GroupVersionResource) bool { return true },
		Handlers:  dummyHandlers,
	})

	const goroutines = 50
	const callsPer = 20
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < callsPer; j++ {
				att := &fakeAttacher{}
				_ = attachMatchingHandlerExtensions(nil, gvr, att)
			}
		}()
	}
	wg.Wait()

	want := uint64(goroutines * callsPer)
	if got := HandlerExtensionsAttachedCount("ext.race"); got != want {
		t.Fatalf("concurrent attach counter = %d, want %d", got, want)
	}
}
