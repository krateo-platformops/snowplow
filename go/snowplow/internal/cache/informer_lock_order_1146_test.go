package cache

// informer_lock_order_1146_test.go — #1146: a GVR torn down while its informer
// is delivering an event must not deadlock rw.mu.
//
// The cycle, as the #1140 reviewer's goroutine dump showed it:
//   - G1, the teardown, holds rw.mu and removes the delivery log's handler:
//     RemoveEventHandler waits for the informer's listenersLock;
//   - G2, the informer's sharedProcessor.run, holds listenersLock once the
//     stop channel G1 closed has stopped the controller, and waits for every
//     listener to return;
//   - G3, the dep handler's listener, is in the middle of an ADD and waits for
//     rw.mu.RLock (syncChState).
//
// The arm builds exactly that interleaving, with no timing in it: G3 is parked
// at the top of the dep ADD (depAddDeliveryHookForTest), G1 is held after it
// closed the stop channel (teardownStepHookForTest) until G2 is seen waiting in
// sharedProcessor.run, and only then is G3 let go towards rw.mu. From there the
// teardown either completes (the fix: the handler is removed after rw.mu is
// released) or the three goroutines are seen in the cycle, which nothing can
// break: that is reported as the deadlock.

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	kruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	clientcache "k8s.io/client-go/tools/cache"
)

// goroutineStacks1146 returns every goroutine's stack, by goroutine id.
func goroutineStacks1146() map[string]string {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			out := map[string]string{}
			for _, s := range strings.Split(string(buf[:n]), "\n\n") {
				out[goroutineID1146(s)] = s
			}
			return out
		}
		buf = make([]byte, 2*len(buf))
	}
}

// goroutineID1146 is the id in a stack's "goroutine N [state]:" header.
func goroutineID1146(stack string) string {
	f := strings.Fields(stack)
	if len(f) < 2 || f[0] != "goroutine" {
		return ""
	}
	return f[1]
}

// currentGoroutineID1146 is the calling goroutine's id.
func currentGoroutineID1146() string {
	buf := make([]byte, 128)
	return goroutineID1146(string(buf[:runtime.Stack(buf, false)]))
}

func stackHas1146(stack string, all ...string) bool {
	if stack == "" {
		return false
	}
	for _, w := range all {
		if !strings.Contains(stack, w) {
			return false
		}
	}
	return true
}

// drainingProcessors1146 are the goroutines of informer processors that hold
// listenersLock and wait for their listeners (shared_informer.go:1143-1159).
func drainingProcessors1146(stacks map[string]string) map[string]bool {
	out := map[string]bool{}
	for id, s := range stacks {
		if stackHas1146(s, "(*sharedProcessor).run", "WaitGroup).Wait") {
			out[id] = true
		}
	}
	return out
}

// newDrainingProcessor1146 reports a draining processor that was not draining
// before the teardown (another test's wedged informer is not this one).
func newDrainingProcessor1146(stacks map[string]string, before map[string]bool) bool {
	for id := range drainingProcessors1146(stacks) {
		if !before[id] {
			return true
		}
	}
	return false
}

// cycle1146 is the #1146 deadlock, on the arm's own goroutines: the teardown
// (g1) removing a handler from inside its rw.mu section, the parked dep
// listener (g3) waiting for rw.mu, and this informer's processor waiting for
// its listeners.
func cycle1146(stacks map[string]string, g1, g3 string, before map[string]bool) bool {
	return stackHas1146(stacks[g1], "RemoveEventHandler", "deletePerGVRStateLocked") &&
		stackHas1146(stacks[g3], "syncChState", "RWMutex).RLock") &&
		newDrainingProcessor1146(stacks, before)
}

func TestIssue1146_TeardownDuringADeliveryWaitingOnRWMuCompletes(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	kinds := rbacListKinds()
	kinds[c854GVR] = "ThingList"
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(rbacScheme(t), kinds, []kruntime.Object{}...)
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatal(err)
	}
	// rw.Stop runs only when the teardown completed: after a deadlock it would
	// wait on rw.mu forever and hang the package.
	wedged := false
	defer func() {
		if !wedged {
			rw.Stop()
		}
	}()
	_, syncCh := rw.EnsureResourceType(c854GVR)
	select {
	case <-syncCh:
	case <-time.After(verifyBound):
		t.Fatal("PRECONDITION: the informer did not sync")
	}

	// G3 — the dep handler's ADD of the object created below parks here, on
	// its listener goroutine, before it reaches rw.mu.
	parked := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var g3 string
	depHook := func(gvr schema.GroupVersionResource) func() {
		if gvr == c854GVR {
			once.Do(func() {
				g3 = currentGoroutineID1146()
				close(parked)
				<-release
			})
		}
		return func() {}
	}
	depAddDeliveryHookForTest.Store(&depHook)
	defer depAddDeliveryHookForTest.Store(nil)

	// G1 — held after it closed the stop channel, under rw.mu, until G2 is
	// waiting for its listeners; then G3 is let go towards rw.mu.
	drained := make(chan bool, 1)
	var before map[string]bool // processors already draining: not this informer's
	tdHook := func(gvr schema.GroupVersionResource, step string) {
		if gvr != c854GVR || step != "stop_closed" {
			return
		}
		ok := false
		for end := time.Now().Add(verifyBound); time.Now().Before(end); time.Sleep(5 * time.Millisecond) {
			if newDrainingProcessor1146(goroutineStacks1146(), before) {
				ok = true
				break
			}
		}
		drained <- ok
		close(release)
	}
	teardownStepHookForTest.Store(&tdHook)
	defer teardownStepHookForTest.Store(nil)

	obj := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "c854.krateo.io/v1", "kind": "Thing",
		"metadata": map[string]any{"namespace": "c854", "name": "t-1146"},
	}}
	if _, err := dyn.Resource(c854GVR).Namespace("c854").Create(context.Background(), obj, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-parked:
	case <-time.After(verifyBound):
		t.Fatal("PRECONDITION: the ADD never reached the dep handler")
	}

	before = drainingProcessors1146(goroutineStacks1146())
	done := make(chan struct{})
	g1c := make(chan string, 1)
	go func() {
		defer close(done)
		g1c <- currentGoroutineID1146()
		rw.RemoveResourceType(c854GVR)
	}()
	g1 := <-g1c
	select {
	case ok := <-drained:
		if !ok {
			t.Fatal("PRECONDITION: the informer's processor was never seen waiting for its listeners")
		}
	case <-time.After(2 * verifyBound):
		t.Fatal("PRECONDITION: the teardown never closed the informer's stop channel")
	}

	// Either the teardown returns, or the cycle forms. Each is decided by
	// what the goroutines are doing, not by how long they take; the bound
	// only stops a run where neither ever shows.
	seen := 0
	for end := time.Now().Add(2 * verifyBound); time.Now().Before(end); {
		select {
		case <-done:
			return
		case <-time.After(20 * time.Millisecond):
		}
		if cycle1146(goroutineStacks1146(), g1, g3, before) {
			seen++
		} else {
			seen = 0
		}
		if seen >= 3 {
			wedged = true
			t.Fatalf("#1146 RED: DEADLOCK — the teardown holds rw.mu inside RemoveEventHandler (deletePerGVRStateLocked), " +
				"the informer's processor holds listenersLock waiting for its listeners, and the dep listener waits for " +
				"rw.mu.RLock (syncChState). Every cache read now waits on rw.mu forever.")
		}
	}
	wedged = true
	t.Fatal("the teardown neither completed nor formed the #1146 cycle")
}

// fakeRemoval1146 is an informer whose RemoveEventHandler records the call and
// whether rw.mu was free at that moment (the arm is single-goroutine, so a
// failed TryLock means the caller holds it).
type fakeRemoval1146 struct {
	clientcache.SharedIndexInformer
	rw      *ResourceWatcher
	calls   int
	underMu bool
}

func (f *fakeRemoval1146) RemoveEventHandler(clientcache.ResourceEventHandlerRegistration) error {
	f.calls++
	if f.rw.mu.TryLock() {
		f.rw.mu.Unlock()
	} else {
		f.underMu = true
	}
	return nil
}

type fakeReg1146 struct{}

func (fakeReg1146) HasSynced() bool                           { return true }
func (fakeReg1146) HasSyncedChecker() clientcache.DoneChecker { return nil }

// TestIssue1146_ARetiredLogLosesItsPendingNowAndItsHandlerAfterTheUnlock — the
// #918 state goes at the retire, under rw.mu; the handler goes after the
// unlock, exactly once, and never with rw.mu held.
func TestIssue1146_ARetiredLogLosesItsPendingNowAndItsHandlerAfterTheUnlock(t *testing.T) {
	rw := &ResourceWatcher{}
	inf := &fakeRemoval1146{rw: rw}
	dl := newDeliveryLog(c854GVR)
	dl.inf, dl.reg = inf, fakeReg1146{}
	dl.mu.Lock()
	dl.pending = map[string]pendingKey{"a/b": {}}
	dl.mu.Unlock()
	storePendingKeys.Add(1)
	liveDeliveryLogs.Store(dl, struct{}{})

	rw.mu.Lock()
	rw.retireDeliveryLogLocked(dl)
	rw.retireDeliveryLogLocked(dl) // idempotent: queued once
	if dl.pendingCount() != 0 {
		rw.mu.Unlock()
		t.Fatal("#918 RED: the retired log still holds its pending keys")
	}
	select {
	case <-dl.stop:
	default:
		rw.mu.Unlock()
		t.Fatal("#918 RED: the retired log's resolver was not stopped")
	}
	if _, live := liveDeliveryLogs.Load(dl); live {
		rw.mu.Unlock()
		t.Fatal("#918 RED: the retired log is still counted live")
	}
	if inf.calls != 0 {
		rw.mu.Unlock()
		t.Fatalf("#1146 RED: the handler was removed inside the rw.mu section (%d calls)", inf.calls)
	}
	rw.unlockAndDetach()
	if inf.calls != 1 || inf.underMu {
		t.Fatalf("#1146 RED: after the unlock the handler was removed %d times (want 1), underMu=%v", inf.calls, inf.underMu)
	}
	if len(rw.detachQueue) != 0 {
		t.Fatalf("the detach queue was not drained: %d", len(rw.detachQueue))
	}
	dl.close() // a later close is a no-op
	if inf.calls != 1 {
		t.Fatalf("a second close removed the handler again (%d calls)", inf.calls)
	}
}

// TestIssue1146_TheInstallJudgesAWatchErrorNewerThanItsReadAsBroken — the
// install reads the candidate's position before rw.mu; an error the candidate
// records after that read is the newer event, so it is not healed by a
// position that predates it, and the recovery baseline is the error's own
// position.
func TestIssue1146_TheInstallJudgesAWatchErrorNewerThanItsReadAsBroken(t *testing.T) {
	synced := &fakeSyncedInformer1146{}
	in := newStoreInstance(c854GVR, stubGenericInformer1146{synced}, shapeStreaming, make(chan struct{}))
	in.broken, in.brokenAt, in.refused = true, "5", true
	in.watchErrs.Store(1)
	in.installAt = instancePosition{rv: "7", errs: 1} // read after the error
	if broken, _ := in.watchStateNow(); broken {
		t.Fatal("a position read after the error that advanced past it must heal (watchHealed)")
	}
	if got := in.installBaseline(); got != "7" {
		t.Fatalf("baseline = %q, want the position read (7)", got)
	}
	in.installAt = instancePosition{rv: "7", errs: 0} // read before the error
	if broken, refused := in.watchStateNow(); !broken || !refused {
		t.Fatalf("#1146 RED: an error newer than the install's read was healed by the older position (broken=%v refused=%v)", broken, refused)
	}
	if got := in.installBaseline(); got != "5" {
		t.Fatalf("#1146 RED: baseline = %q, want the newer error's position (5)", got)
	}
}

type fakeSyncedInformer1146 struct {
	clientcache.SharedIndexInformer
}

func (*fakeSyncedInformer1146) HasSynced() bool { return true }

type stubGenericInformer1146 struct {
	inf clientcache.SharedIndexInformer
}

func (g stubGenericInformer1146) Informer() clientcache.SharedIndexInformer { return g.inf }
func (g stubGenericInformer1146) Lister() clientcache.GenericLister         { return nil }

// TestIssue1146_AConfirmReadingOfAReplacedInformerIsDropped — N2 with the
// position read off the lock: a reading counts only for the informer it was
// read from.
func TestIssue1146_AConfirmReadingOfAReplacedInformerIsDropped(t *testing.T) {
	a := stubGenericInformer1146{&fakeSyncedInformer1146{}}
	b := stubGenericInformer1146{&fakeSyncedInformer1146{}}
	if got := positionIfCurrent(a, a, "9"); got != "9" {
		t.Fatalf("same informer: %q, want 9", got)
	}
	if got := positionIfCurrent(b, a, "9"); got != "" {
		t.Fatalf("#1146 RED: a reading of a replaced informer was applied to its successor (%q)", got)
	}
	if got := positionIfCurrent(nil, a, "9"); got != "" {
		t.Fatalf("unregistered: %q, want empty", got)
	}
}
