package cache

// informer_lock_order_1146_review_test.go — the arms the #1147 review asked
// for (issuecomment-6630752103). Each drives the real link — the real watch
// error handler, the real install, the real confirm passes, the real
// registration — and lands its event INSIDE the off-lock position read, the
// window the fix opened.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/informers"
	metadatafake "k8s.io/client-go/metadata/fake"
	clientcache "k8s.io/client-go/tools/cache"
)

// posInformer1146 is an informer whose LastSyncResourceVersion is a script:
// read runs on every call (and may act while the caller reads). Every other
// informer method panics (the embedded interface is nil): the arms touch none.
type posInformer1146 struct {
	clientcache.SharedIndexInformer
	read func() string
}

func (*posInformer1146) HasSynced() bool                   { return true }
func (f *posInformer1146) LastSyncResourceVersion() string { return f.read() }

type posGI1146 struct{ inf *posInformer1146 }

func (g posGI1146) Informer() clientcache.SharedIndexInformer { return g.inf }
func (g posGI1146) Lister() clientcache.GenericLister         { return nil }

func newPosGI1146(read func() string) posGI1146 {
	return posGI1146{&posInformer1146{read: read}}
}

// TestIssue1146_R1R2_AnErrorDuringTheInstallsPositionReadIsNotHealed — the
// candidate's real watch-error handler fires WHILE installBuild reads the
// candidate's position (the reflector moved on and its watch dropped during
// the read). The read returns the position from before the error, "5"; the
// error is recorded at "9". That error is the newer event: the install must
// copy the broken watch and take the error's position as the recovery
// baseline. RED if the install reads the position before the error count (R1)
// or the handler does not count the error (R2): either way the install judges
// "5" ≠ "9" as a heal.
func TestIssue1146_R1R2_AnErrorDuringTheInstallsPositionReadIsNotHealed(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "r1146.krateo.io", Version: "v1", Resource: "things"}
	rw := &ResourceWatcher{}
	live := newStoreInstance(gvr, newPosGI1146(func() string { return "1" }), shapeStreaming, make(chan struct{}))
	var c *storeInstance
	var once sync.Once
	inRead := false
	c = newStoreInstance(gvr, newPosGI1146(func() string {
		if inRead {
			return "9" // the handler's own read: the position at the error
		}
		once.Do(func() {
			inRead = true
			rw.watchErrorHandlerForInstance(gvr, c)(nil, apierrors.NewInternalError(errors.New("watch dropped during the read")))
			inRead = false
		})
		return "5" // the position the install's read took, before the error
	}), shapeStreaming, make(chan struct{}))
	rw.mu.Lock()
	rw.ensureSwapMapsLocked()
	rw.informers = map[schema.GroupVersionResource]informers.GenericInformer{gvr: live.gi}
	rw.instances[gvr] = live
	rw.builds[gvr] = &storeBuild{inst: c, account: newBuildAccount()}
	rw.mu.Unlock()
	// Stop after step 1: what is under test is the state the install wrote.
	setSwapStepHook(t, func(in *storeInstance, step string) {
		if in == c && step == "installed" {
			rw.mu.Lock()
			rw.instances[gvr] = newStoreInstance(gvr, live.gi, shapeStreaming, make(chan struct{}))
			rw.mu.Unlock()
		}
	})
	rw.installBuild(c)

	rw.mu.RLock()
	_, broken := rw.watchBroken[gvr]
	baseline := rw.lastSyncRV[gvr]
	installed := rw.informers[gvr] == c.gi
	rw.mu.RUnlock()
	if !installed {
		t.Fatal("PRECONDITION: the install did not run step 1")
	}
	if !c.broken || c.brokenAt != "9" {
		t.Fatalf("PRECONDITION: the error handler did not record the error (broken=%v at %q)", c.broken, c.brokenAt)
	}
	if !broken {
		t.Fatal("#1146 R1/R2 RED: an error recorded during the install's position read was healed by the older " +
			"position — the new copy serves with its watch down")
	}
	if baseline != "9" {
		t.Fatalf("#1146 R1/R2 RED: the recovery baseline is %q, want the error's position (9)", baseline)
	}
}

// TestIssue1146_R5_AConfirmPassDropsAReadingOfAnInformerReplacedDuringIt —
// each confirm pass reads the position off the lock. Here the GVR's informer
// is replaced DURING that read: the successor's watch is broken at "B0". The
// pass's reading ("A9", of the replaced informer) must not heal the successor
// nor become its baseline. RED for a pass that hands its reading to
// applyConfirmLocked without positionIfCurrent (R5; RefreshDiscovery on main).
func TestIssue1146_R5_AConfirmPassDropsAReadingOfAnInformerReplacedDuringIt(t *testing.T) {
	passes := map[string]func(rw *ResourceWatcher, gvr schema.GroupVersionResource){
		"RefreshDiscovery": func(rw *ResourceWatcher, _ schema.GroupVersionResource) { rw.RefreshDiscovery(context.Background()) },
		"ConfirmResourceType": func(rw *ResourceWatcher, gvr schema.GroupVersionResource) {
			rw.ConfirmResourceType(context.Background(), gvr)
		},
		"ConfirmResourceTypes": func(rw *ResourceWatcher, gvr schema.GroupVersionResource) {
			rw.ConfirmResourceTypes(context.Background(), []schema.GroupVersionResource{gvr})
		},
	}
	for name, pass := range passes {
		t.Run(name, func(t *testing.T) {
			gvr := schema.GroupVersionResource{Group: "r5.krateo.io", Version: "v1", Resource: "things"}
			rw := &ResourceWatcher{}
			succ := newPosGI1146(func() string { return "B0" })
			var once sync.Once
			read := 0
			first := newPosGI1146(func() string {
				read++
				once.Do(func() {
					// The swap lands while the pass reads.
					rw.mu.Lock()
					rw.informers[gvr] = succ
					rw.watchBroken = map[schema.GroupVersionResource]struct{}{gvr: {}}
					rw.lastSyncRV = map[schema.GroupVersionResource]string{gvr: "B0"}
					rw.mu.Unlock()
				})
				return "A9"
			})
			rw.informers = map[schema.GroupVersionResource]informers.GenericInformer{gvr: first}
			pass(rw, gvr)
			if read == 0 {
				t.Fatal("PRECONDITION: the pass never read the replaced informer's position")
			}
			rw.mu.RLock()
			_, broken := rw.watchBroken[gvr]
			baseline := rw.lastSyncRV[gvr]
			rw.mu.RUnlock()
			if !broken {
				t.Fatalf("#1146 R5 RED (%s): the replaced informer's position healed its successor's broken watch", name)
			}
			if baseline != "B0" {
				t.Fatalf("#1146 R5 RED (%s): the successor's baseline was overwritten with the replaced informer's position (%q)", name, baseline)
			}
		})
	}
}

// TestIssue1146_RefreshDiscoverySkipsAGVRTornDownDuringIt — the GVR is torn
// down while the pass reads its position: nothing may be written back for it
// (main wrote its baseline, and with discovery its confirmation, for a GVR no
// longer registered).
func TestIssue1146_RefreshDiscoverySkipsAGVRTornDownDuringIt(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "r5gone.krateo.io", Version: "v1", Resource: "things"}
	rw := &ResourceWatcher{}
	var once sync.Once
	first := newPosGI1146(func() string {
		once.Do(func() {
			rw.mu.Lock()
			delete(rw.informers, gvr)
			rw.mu.Unlock()
		})
		return "A9"
	})
	rw.informers = map[schema.GroupVersionResource]informers.GenericInformer{gvr: first}
	rw.RefreshDiscovery(context.Background())
	rw.mu.RLock()
	_, wrote := rw.lastSyncRV[gvr]
	rw.mu.RUnlock()
	if wrote {
		t.Fatal("#1146 RED: RefreshDiscovery wrote a recovery baseline for a GVR torn down during the pass")
	}
}

// TestIssue1146_N1_AFactoryGVRReRegisteredDuringItsDetachDoesNotDeadlock —
// the #1147 reviewer's residual repro, adopted. c854GVR takes the shared
// dynamic factory here (no REST config, group not navigation-discovered).
//   - its dep ADD is parked; the GVR is torn down; the teardown's off-lock
//     detach holds the informer's startedLock while it waits for the parked
//     listener (the processor is draining);
//   - EnsureResourceType re-registers the GVR. On main and on the first #1147
//     head the factory handed back the SAME informer, and its wiring
//     (SetTransform) waited for startedLock with rw.mu held;
//   - the parked listener is let go, towards rw.mu.RLock: the cycle.
//
// With the fix the factory issues a GVR's informer once (factoryIssued): the
// re-registration builds a standalone informer, nobody else holds its locks,
// and everything completes.
func TestIssue1146_N1_AFactoryGVRReRegisteredDuringItsDetachDoesNotDeadlock(t *testing.T) {
	rw, dyn, wedged := newWatcher1146(t)
	rw.mu.RLock()
	shape := rw.instances[c854GVR].shape
	firstInf := rw.informers[c854GVR].Informer()
	rw.mu.RUnlock()
	if shape != shapeFactoryDynamic {
		t.Fatalf("PRECONDITION: c854GVR is not on the shared-factory path here (shape %v)", shape)
	}

	g3, release := parkDepAdd1146(t, dyn, "t-n1")
	var before map[string]bool
	drained := make(chan bool, 1)
	tdHook := func(gvr schema.GroupVersionResource, step string) {
		if gvr == c854GVR && step == "stop_closed" {
			drained <- waitFor1146(func(st map[string]string) bool { return newDrainingProcessor1146(st, before) })
		}
	}
	teardownStepHookForTest.Store(&tdHook)
	defer teardownStepHookForTest.Store(nil)

	before = drainingProcessors1146(goroutineStacks1146())
	*wedged = true // until every goroutine of the interleaving has returned
	g1, done1 := goTracked1146(func() { rw.RemoveResourceType(c854GVR) })
	if !<-drained {
		t.Fatal("PRECONDITION: the processor was never seen draining")
	}
	if !waitFor1146(func(st map[string]string) bool { return stackHas1146(st[g1], "RemoveEventHandler", "unlockAndDetach") }) {
		t.Fatal("PRECONDITION: the teardown never reached its off-lock detach")
	}
	// G4 — the re-registration, while the detach waits for the parked
	// listener. It either returns, or is seen waiting on an informer lock
	// under rw.mu.
	g4, done4 := goTracked1146(func() { rw.EnsureResourceType(c854GVR) })
	waitFor1146(func(st map[string]string) bool {
		select {
		case <-done4:
			return true
		default:
		}
		return stackHas1146(st[g4], "addResourceTypeLocked", "sync.(*Mutex).Lock")
	})
	close(release) // G3 heads for rw.mu.RLock
	outcome := awaitOrCycle1146(func(st map[string]string) bool {
		return stackHas1146(st[g1], "RemoveEventHandler") &&
			stackHas1146(st[g3()], "syncChState", "RWMutex).RLock") &&
			stackHas1146(st[g4], "addResourceTypeLocked", "sync.(*Mutex).Lock")
	}, done1, done4)
	if outcome == "" {
		*wedged = false
	} else {
		t.Fatalf("#1146 N1 RED: %s — the re-registration wired the torn-down GVR's factory informer under rw.mu "+
			"while the teardown's handler removal held its startedLock", outcome)
	}
	rw.mu.RLock()
	gi := rw.informers[c854GVR]
	shape = rw.instances[c854GVR].shape
	rw.mu.RUnlock()
	if gi == nil || gi.Informer() == firstInf {
		t.Fatal("#1146 N1 RED: the re-registration got the torn-down factory informer back (stopped and frozen)")
	}
	if shape != shapeStandaloneDynamic {
		t.Fatalf("#1146 N1: the re-registered GVR's shape is %v, want a standalone informer", shape)
	}
}

// TestIssue1146_ATornDownFactoryGVRIsReRegisteredStandalone — a factory issues
// a GVR's informer once: after a teardown, the re-registration builds a fresh
// standalone informer (not the factory's stopped one, which took no transform
// and no handler and froze the store), and it syncs with a recording delivery
// log. RED without factoryIssued.
func TestIssue1146_ATornDownFactoryGVRIsReRegisteredStandalone(t *testing.T) {
	rw, _, _ := newWatcher1146(t)
	rw.mu.RLock()
	first := rw.informers[c854GVR].Informer()
	shape := rw.instances[c854GVR].shape
	rw.mu.RUnlock()
	if shape != shapeFactoryDynamic {
		t.Fatalf("PRECONDITION: c854GVR is not on the shared-factory path here (shape %v)", shape)
	}
	rw.RemoveResourceType(c854GVR)
	_, synced := rw.EnsureResourceType(c854GVR)
	select {
	case <-synced:
	case <-time.After(verifyBound):
		t.Fatal("the re-registered informer never synced")
	}
	rw.mu.RLock()
	gi := rw.informers[c854GVR]
	shape = rw.instances[c854GVR].shape
	dl := rw.deliveries[c854GVR]
	rw.mu.RUnlock()
	if gi.Informer() == first {
		t.Fatal("#1146 RED: the re-registration got the torn-down factory informer back")
	}
	if shape != shapeStandaloneDynamic {
		t.Fatalf("#1146 RED: the re-registered GVR's shape is %v, want standalone", shape)
	}
	if dl == nil || !dl.recording {
		t.Fatalf("#1146 RED: the re-registered GVR has no recording delivery log (%v)", dl)
	}
	if !gi.Informer().HasSynced() {
		t.Fatal("the re-registered informer is not synced")
	}
}

// TestIssue1146_ATornDownMetadataFactoryGVRIsReRegisteredStandalone — the same
// for the metadata factory: a GVR the metadata factory issued an informer for
// is re-registered, after a teardown, with a standalone metadata informer.
func TestIssue1146_ATornDownMetadataFactoryGVRIsReRegisteredStandalone(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	gvr := schema.GroupVersionResource{Group: "meta1146.krateo.io", Version: "v1", Resource: "things"}
	rw := newSyntheticRemoveWatcher(t)
	t.Cleanup(rw.Stop)
	rw.SetMetadataClient(metadatafake.NewSimpleMetadataClient(metadatafake.NewTestScheme()))
	shapeAndInformer := func() (instanceShape, clientcache.SharedIndexInformer) {
		rw.mu.RLock()
		defer rw.mu.RUnlock()
		return rw.instances[gvr].shape, rw.informers[gvr].Informer()
	}
	if added, _ := rw.EnsureResourceTypeMetadataOnly(gvr); !added {
		t.Fatal("PRECONDITION: the metadata-only registration did not run")
	}
	shape, first := shapeAndInformer()
	if shape != shapeFactoryMetadata {
		t.Fatalf("PRECONDITION: the GVR is not on the metadata factory path (shape %v)", shape)
	}
	rw.RemoveResourceType(gvr)
	if added, _ := rw.EnsureResourceTypeMetadataOnly(gvr); !added {
		t.Fatal("the metadata-only re-registration did not run")
	}
	shape, again := shapeAndInformer()
	if again == first {
		t.Fatal("#1146 RED: the re-registration got the torn-down metadata factory informer back")
	}
	if shape != shapeStandaloneMetadata {
		t.Fatalf("#1146 RED: the re-registered GVR's shape is %v, want standalone metadata", shape)
	}
}

// TestIssue1146_ATeardownRetiresTheGVRsPendingKeys — the #1147 review's R6: a
// teardown drops the GVR's pending keys and stops its resolver (#918), and
// removes its log from the live set.
func TestIssue1146_ATeardownRetiresTheGVRsPendingKeys(t *testing.T) {
	rw, _, _ := newWatcher1146(t)
	rw.mu.RLock()
	dl := rw.deliveries[c854GVR]
	rw.mu.RUnlock()
	if dl == nil {
		t.Fatal("PRECONDITION: the GVR has no delivery log")
	}
	n0 := storePendingKeys.Load()
	dl.setPending(map[string]readState{"c854/a": {}, "c854/b": {}})
	if dl.pendingCount() != 2 || storePendingKeys.Load() != n0+2 {
		t.Fatalf("PRECONDITION: the keys are not pending (%d, gauge +%d)", dl.pendingCount(), storePendingKeys.Load()-n0)
	}
	rw.RemoveResourceType(c854GVR)
	if dl.pendingCount() != 0 || storePendingKeys.Load() != n0 {
		t.Fatalf("#1146 R6 RED: the teardown left the GVR's pending keys (%d, gauge +%d)", dl.pendingCount(), storePendingKeys.Load()-n0)
	}
	select {
	case <-dl.stop:
	default:
		t.Fatal("#1146 R6 RED: the teardown did not stop the GVR's resolver")
	}
	if _, live := liveDeliveryLogs.Load(dl); live {
		t.Fatal("#1146 R6 RED: the torn-down GVR's log is still counted live")
	}
}

// ── shared harness ────────────────────────────────────────────────────────────

// newWatcher1146 is a watcher with c854GVR registered and synced. A test that
// wedges it sets *wedged: the cleanup then skips Stop (it would wait on the
// wedged rw.mu forever) and unbinds it from every process-wide singleton, so
// the rest of the package never touches its rw.mu — a RED stays a fast RED.
func newWatcher1146(t *testing.T) (*ResourceWatcher, *dynamicfake.FakeDynamicClient, *bool) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	kinds := rbacListKinds()
	kinds[c854GVR] = "ThingList"
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(rbacScheme(t), kinds, []kruntime.Object{}...)
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatal(err)
	}
	wedged := new(bool)
	t.Cleanup(func() {
		if *wedged {
			abandonWedgedWatcher1146(rw)
			return
		}
		rw.Stop()
	})
	_, syncCh := rw.EnsureResourceType(c854GVR)
	select {
	case <-syncCh:
	case <-time.After(verifyBound):
		t.Fatal("PRECONDITION: the informer did not sync")
	}
	return rw, dyn, wedged
}

// abandonWedgedWatcher1146 unbinds a deadlocked watcher from the process-wide
// state that would reach its rw.mu: the dep-watch singleton (every
// withCleanDepWatch stops the bound watcher, under its rw.mu) and Global().
// Its goroutines stay blocked; nothing else in the process can reach them.
func abandonWedgedWatcher1146(rw *ResourceWatcher) {
	if w := depWatchInstance; w != nil {
		w.watcher.CompareAndSwap(rw, nil)
	}
	if Global() == rw {
		SetGlobal(nil)
	}
}

// parkDepAdd1146 creates an object and parks its dep ADD on the listener
// goroutine, before it reaches rw.mu. It returns that goroutine's id (once
// parked) and the channel that lets it go.
func parkDepAdd1146(t *testing.T, dyn *dynamicfake.FakeDynamicClient, name string) (func() string, chan struct{}) {
	t.Helper()
	parked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var id string
	h := func(gvr schema.GroupVersionResource) func() {
		if gvr == c854GVR {
			once.Do(func() {
				id = currentGoroutineID1146()
				close(parked)
				<-release
			})
		}
		return func() {}
	}
	depAddDeliveryHookForTest.Store(&h)
	t.Cleanup(func() { depAddDeliveryHookForTest.Store(nil) })
	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "c854.krateo.io/v1", "kind": "Thing",
		"metadata": map[string]any{"namespace": "c854", "name": name},
	}}
	if _, err := dyn.Resource(c854GVR).Namespace("c854").Create(context.Background(), obj, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-parked:
	case <-time.After(verifyBound):
		t.Fatal("PRECONDITION: the ADD never reached the dep handler")
	}
	return func() string { return id }, release
}

// goTracked1146 runs f on a new goroutine; it returns that goroutine's id and
// a channel closed when f returns.
func goTracked1146(f func()) (string, chan struct{}) {
	idc := make(chan string, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		idc <- currentGoroutineID1146()
		f()
	}()
	return <-idc, done
}

// waitFor1146 polls the goroutine stacks until cond holds (true) or the
// bound passes (false).
func waitFor1146(cond func(map[string]string) bool) bool {
	for end := time.Now().Add(verifyBound); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
		if cond(goroutineStacks1146()) {
			return true
		}
	}
	return false
}

// awaitOrCycle1146 waits until every done channel is closed ("") or cycle is
// seen in three consecutive samples (a description of it). The bound only
// stops a run where neither ever shows.
func awaitOrCycle1146(cycle func(map[string]string) bool, done ...chan struct{}) string {
	seen := 0
	for end := time.Now().Add(2 * verifyBound); time.Now().Before(end); {
		all := true
		for _, d := range done {
			select {
			case <-d:
			default:
				all = false
			}
		}
		if all {
			return ""
		}
		time.Sleep(20 * time.Millisecond)
		if cycle(goroutineStacks1146()) {
			seen++
		} else {
			seen = 0
		}
		if seen >= 3 {
			return "DEADLOCK"
		}
	}
	return "neither completed nor deadlocked"
}
