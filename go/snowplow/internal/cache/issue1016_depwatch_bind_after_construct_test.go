// issue1016_depwatch_bind_after_construct_test.go — #1016: the dep-watch
// bridge binds a ResourceWatcher only once NewResourceWatcher has built it.
//
// Pre-fix, building the dep handler for a GVR (depEventHandler) also stored
// the watcher into the process-wide bridge. NewResourceWatcher builds one dep
// handler per RBAC GVR inside its registration loop, so the bridge held the
// watcher while the constructor was still writing its maps (rw.informers,
// rw.syncCh, rw.instances) without rw.mu. The dep worker loads the bound
// watcher and probes it under rw.mu.RLock: a worker still draining another
// watcher's event read the half-built watcher's maps while the constructor
// wrote them (the -race report in rbac/evaltest).
//
// The seam is a handler extension: wireInstanceLocked runs its Handlers
// factory on the constructor's goroutine, right after the dep handler for the
// same GVR was built and before the constructor registers the next GVR. No
// sleeps decide any ordering.

package cache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
	clientcache "k8s.io/client-go/tools/cache"
)

// issue1016ConstructionSeam registers a handler extension (on top of the
// package's own, which the constructor asserts are wired) whose Handlers
// factory calls onConstruct while armed. The registry is restored afterwards.
func issue1016ConstructionSeam(t *testing.T, armed *atomic.Bool, onConstruct func(rw *ResourceWatcher, gvr schema.GroupVersionResource)) {
	t.Helper()
	saved := HandlerExtensionsSnapshot()
	isolateHandlerExtensionsForTest(t)
	for _, ext := range saved {
		RegisterHandlerExtension(ext)
	}
	RegisterHandlerExtension(HandlerExtension{
		Name:      "test.issue1016.construction_seam",
		Predicate: func(schema.GroupVersionResource) bool { return true },
		Handlers: func(rw *ResourceWatcher, gvr schema.GroupVersionResource) clientcache.ResourceEventHandler {
			if armed.Load() {
				onConstruct(rw, gvr)
			}
			return clientcache.ResourceEventHandlerFuncs{}
		},
	})
}

// A1 — deterministic, no -race needed. While NewResourceWatcher is still
// registering its GVRs, the bridge must not hold the watcher being built.
// Once it has returned, the bridge holds it: the test reset
// (stopBoundDepWatcherForTest) stops the bound watcher, so a watcher whose
// handlers feed the bridge must be the one bound even before its first event.
//
// RED on 5dbb71c4: the bridge holds the watcher at every one of the four RBAC
// registrations.
func TestIssue1016_BridgeNeverBindsAWatcherStillUnderConstruction(t *testing.T) {
	defer withCleanDepWatch(t)()

	var armed atomic.Bool
	var mu sync.Mutex
	var seen, boundMid int
	issue1016ConstructionSeam(t, &armed, func(rw *ResourceWatcher, gvr schema.GroupVersionResource) {
		mu.Lock()
		defer mu.Unlock()
		seen++
		if depWatchSingleton().watcher.Load() == rw {
			boundMid++
		}
	})

	armed.Store(true)
	rw, _ := realWatcher(t) // no extra GVRs: only the constructor registers
	armed.Store(false)

	mu.Lock()
	defer mu.Unlock()
	if seen != len(RBACResourceTypes) {
		t.Fatalf("precondition: the construction seam ran %d times, want one per RBAC GVR (%d)", seen, len(RBACResourceTypes))
	}
	if boundMid != 0 {
		t.Fatalf("the dep-watch bridge held the watcher at %d/%d of its constructor's registrations: "+
			"a half-built ResourceWatcher was published to the dep worker (#1016)", boundMid, seen)
	}
	if got := depWatchSingleton().watcher.Load(); got != rw {
		t.Fatalf("after NewResourceWatcher returned the bridge holds %p, want the new watcher %p: "+
			"the test reset could no longer stop the watcher whose handlers feed the bridge", got, rw)
	}
}

// A2 — the reader. Watcher A's event is in the dep worker, held after
// takeMask until watcher B's constructor reaches its FIRST registration (one
// of four RBAC GVRs). The seam then lets the worker go and blocks the
// constructor until the worker has probed the event, so the probe runs while
// B is provably half-built (B.informers holds one GVR of four, none started).
//
// What the worker probed is read off its outcome: A's informer for the GVR is
// synced and does not hold the object, so a probe of A reads ABSENT; B's is
// not started, so a probe of B reads UNKNOWN. RED on 5dbb71c4: UNKNOWN — the
// worker probed the watcher under construction.
//
// The -race report the issue saw is NOT deterministic, and this arm does not
// rely on it: every later registration's bind is an atomic Store that orders
// the constructor's earlier map writes before a worker Load that comes after
// it, so the race fires only when the worker loads between two registrations
// and reads after the next one (rbac/evaltest: run 1 red, run 2 green). The
// seam orders the probe inside the window instead, so the observation is
// deterministic and -race stays quiet here by construction.
func TestIssue1016_DepWorkerNeverProbesAWatcherUnderConstruction(t *testing.T) {
	defer withCleanDepWatch(t)()

	gvr := RBACResourceTypes[0]
	a, _ := realWatcher(t, gvr) // A's informer for gvr is synced and empty

	held := depEventKey{gvr: gvr, namespace: "issue1016", name: "probed-during-construction"}
	next := depEventKey{gvr: gvr, namespace: "issue1016", name: "after-the-held-event"}
	released := make(chan struct{})
	heldHandled := make(chan struct{})
	counted := make(chan struct{})
	var countedOnce sync.Once
	markCounted := func() { countedOnce.Do(func() { close(counted) }) }
	defer markCounted() // never leave the worker parked
	restore := SetDepWorkerTakenHookForTest(func(g schema.GroupVersionResource, namespace, name string) {
		switch (depEventKey{gvr: g, namespace: namespace, name: name}) {
		case held:
			<-released
		case next:
			// One worker, FIFO: picking up `next` means `held` was probed and
			// handled (an UNKNOWN requeue of `held` waits out a backoff).
			// `next` itself waits until the seam has read the counters.
			close(heldHandled)
			<-counted
		}
	})
	defer restore()

	w := depWatchSingleton()
	w.submitDepEvent(a, held, dmSourceWatch)
	w.submitDepEvent(a, next, dmSourceWatch)

	var armed atomic.Bool
	var once sync.Once
	var absent, unknown uint64
	issue1016ConstructionSeam(t, &armed, func(*ResourceWatcher, schema.GroupVersionResource) {
		once.Do(func() {
			absent0, unknown0 := w.counters.probeAbsent.Load(), w.counters.probeUnknown.Load()
			close(released)
			select {
			case <-heldHandled:
			case <-time.After(harnessWaitBound):
				t.Errorf("the dep worker did not handle the held event within %s", harnessWaitBound)
				return
			}
			absent = w.counters.probeAbsent.Load() - absent0
			unknown = w.counters.probeUnknown.Load() - unknown0
			markCounted()
		})
	})

	armed.Store(true)
	_, _ = realWatcher(t) // watcher B: the seam runs inside its constructor
	armed.Store(false)
	markCounted()
	w.stopWorker() // join: the worker drains what is queued, then exits

	if absent+unknown != 1 {
		t.Fatalf("precondition: the held event was probed %d times inside B's construction, want 1 (absent=%d unknown=%d)",
			absent+unknown, absent, unknown)
	}
	if unknown != 0 {
		t.Fatalf("the dep worker probed watcher B while B's constructor was still registering its informers " +
			"(probe UNKNOWN: B's informer is not even started); it must probe A, the fully built watcher " +
			"that submitted the event (#1016)")
	}
}
