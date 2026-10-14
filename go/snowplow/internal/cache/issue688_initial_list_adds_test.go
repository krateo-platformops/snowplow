// issue688_initial_list_adds_test.go — #688: an initial-LIST ADD that the
// handler receives AFTER the store-level syncCh has closed must not be
// treated as a live event.
//
// THE MECHANISM. syncCh[gvr] is closed by waitInformerSync polling the
// INFORMER's HasSynced, which flips once the initial LIST has been popped
// into the store. client-go hands each registered handler its notifications
// through that handler's own processorListener goroutine, so the handler can
// still be working through the initial replay after the store is synced.
// addEventPostSync reads "syncCh closed" as "post-sync", so such a replayed
// ADD used to be propagated as live: it stamped the per-GVR freshness clock
// (#655's "lastEventAgeSeconds = 8e-06 with no event ever delivered") and
// dirty-marked the dependents of an object that had not changed — at boot
// and again on every relist.
//
// WHAT THIS ARM DRIVES. A real ResourceWatcher over a fake dynamic client
// seeded with initialObjects objects; the real informer lists, syncs and
// delivers. The only thing the arm controls is TIMING: depAddDeliveryHookForTest
// holds every ADD delivery until syncCh has closed — the processorListener
// lag that CPU contention and a large initial LIST produce in production,
// made certain instead of probable. Nothing about the gate is simulated.
//
// Assertions are on the side effects themselves: the freshness stamp, the
// propagated-ADD and submitted-event counters, and the dep tracker's
// dirty-marks on a registered list dependent. A live ADD after the replay
// must still produce all of them, so the arm cannot pass by dropping
// everything.

package cache

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const (
	issue688NS           = "krateo-system"
	issue688InitialCount = 6
)

func issue688GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "paragraphs"}
}

func issue688Obj(gvr schema.GroupVersionResource, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvr.GroupVersion().String(),
		"kind":       "Paragraph",
		"metadata":   map[string]any{"namespace": issue688NS, "name": name},
		"spec":       map[string]any{"body": name},
	}}
}

// issue688Deliveries holds every dep ADD delivery until the GVR's syncCh has
// closed, and counts the deliveries that have been fully handled.
type issue688Deliveries struct {
	rw   *ResourceWatcher
	mu   sync.Mutex
	done map[schema.GroupVersionResource]int
	late int // deliveries that started only after syncCh had closed
}

func installIssue688Barrier(t *testing.T, rw *ResourceWatcher) *issue688Deliveries {
	t.Helper()
	d := &issue688Deliveries{rw: rw, done: map[schema.GroupVersionResource]int{}}
	hook := func(gvr schema.GroupVersionResource) func() {
		deadline := time.Now().Add(harnessWaitBound)
		for time.Now().Before(deadline) {
			rw.mu.RLock()
			ch := rw.syncCh[gvr]
			rw.mu.RUnlock()
			closed := false
			if ch != nil {
				select {
				case <-ch:
					closed = true
				default:
				}
			}
			if closed {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		d.mu.Lock()
		d.late++
		d.mu.Unlock()
		return func() {
			d.mu.Lock()
			d.done[gvr]++
			d.mu.Unlock()
		}
	}
	depAddDeliveryHookForTest.Store(&hook)
	t.Cleanup(func() { depAddDeliveryHookForTest.Store(nil) })
	return d
}

func (d *issue688Deliveries) handled(gvr schema.GroupVersionResource) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.done[gvr]
}

func (d *issue688Deliveries) waitHandled(t *testing.T, gvr schema.GroupVersionResource, want int) {
	t.Helper()
	deadline := time.Now().Add(harnessWaitBound)
	for d.handled(gvr) < want {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d ADD deliveries for %s were handled within %s", d.handled(gvr), want, gvr, harnessWaitBound)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// issue688Drain waits for the dep-event queue to empty, then a quiet period,
// so an asynchronous dirty-mark has had every chance to land before an
// absence is asserted.
func issue688Drain() {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && DepWatchStatsSnapshot().DeleteQueueDepth > 0 {
		time.Sleep(5 * time.Millisecond)
	}
	waitQuiet()
}

type issue688Reading struct {
	addPropagated   uint64
	eventsSubmitted uint64
	dirtyMarks      uint64
	stamp           int64
}

func issue688Read(rw *ResourceWatcher, gvr schema.GroupVersionResource) issue688Reading {
	s := DepWatchStatsSnapshot()
	return issue688Reading{
		addPropagated:   s.AddPropagated,
		eventsSubmitted: s.EventsSubmitted,
		dirtyMarks:      Deps().Stats().DirtyMarkTotal,
		stamp:           rw.lastEventUnixNano(gvr),
	}
}

// issue688Watcher builds a watcher over a fake cluster already holding
// issue688InitialCount objects of gvr. The GVR is NOT registered yet: the arm
// installs its barrier first, then registers.
func issue688Watcher(t *testing.T, gvr schema.GroupVersionResource) (*ResourceWatcher, *dynamicfake.FakeDynamicClient) {
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
		gvr: "ParagraphList",
	}
	seed := make([]k8sruntime.Object, 0, issue688InitialCount)
	for i := 0; i < issue688InitialCount; i++ {
		seed = append(seed, issue688Obj(gvr, fmt.Sprintf("p-%d", i)))
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds, seed...)
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
		time.Sleep(50 * time.Millisecond)
	})
	return rw, dyn
}

func issue688Register(t *testing.T, rw *ResourceWatcher, gvr schema.GroupVersionResource) {
	t.Helper()
	_, syncCh := rw.EnsureResourceType(gvr)
	select {
	case <-syncCh:
	case <-time.After(harnessWaitBound):
		t.Fatalf("informer for %s did not sync within %s", gvr, harnessWaitBound)
	}
}

// TestIssue688_InitialListAddsDeliveredPastSyncAreNotLiveEvents covers both
// the boot registration and the relist (RemoveResourceType +
// EnsureResourceType builds a fresh informer whose LIST is again an initial
// list), then proves the gate still admits a genuinely live ADD.
func TestIssue688_InitialListAddsDeliveredPastSyncAreNotLiveEvents(t *testing.T) {
	// standalone: the group is navigation-discovered, so the GVR gets its own
	// re-creatable informer, the only shape production relists.
	// factory: the shared dynamic-factory informer that a boot registration of
	// an undiscovered group takes. No relist leg: production never relists it.
	for _, standalone := range []bool{true, false} {
		name := "factory"
		if standalone {
			name = "standalone"
		}
		t.Run(name, func(t *testing.T) { issue688Arm(t, standalone) })
	}
}

func issue688Arm(t *testing.T, standalone bool) {
	defer withCleanDepWatch(t)()
	gvr := issue688GVR()
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	if standalone {
		AddNavigationDiscoveredGroup(gvr.Group)
	}

	// A list dependent of the GVR's namespace: what a served LIST of
	// paragraphs records. An ADD treated as live dirty-marks it.
	store := newResolvedCache(100, 1<<20, time.Hour)
	d := Deps()
	d.SetStore(store)
	const l1Key = "L1_issue688_paragraph_list"
	store.Put(l1Key, &ResolvedEntry{RawJSON: []byte(`{"items":[]}`), Inputs: widgetInputs(gvr, issue688NS, "")})
	d.RecordList(context.Background(), l1Key, gvr, issue688NS)

	rw, dyn := issue688Watcher(t, gvr)
	deliveries := installIssue688Barrier(t, rw)

	// --- boot: the initial LIST replay, every delivery held past syncCh ---
	before := issue688Read(rw, gvr)
	issue688Register(t, rw, gvr)
	deliveries.waitHandled(t, gvr, issue688InitialCount)
	issue688Drain()
	after := issue688Read(rw, gvr)
	if got := after.addPropagated - before.addPropagated; got != 0 {
		t.Errorf("#688 boot: %d of %d initial-LIST ADDs delivered past syncCh were propagated as live ADDs, want 0", got, issue688InitialCount)
	}
	if got := after.eventsSubmitted - before.eventsSubmitted; got != 0 {
		t.Errorf("#688 boot: %d dep events submitted for the initial LIST, want 0", got)
	}
	if got := after.dirtyMarks - before.dirtyMarks; got != 0 {
		t.Errorf("#688 boot: %d dirty-marks on dependents of objects that did not change, want 0", got)
	}
	if after.stamp != 0 {
		t.Errorf("#688 boot: freshness clock stamped %s ago with no live event ever delivered, want never (0)",
			time.Since(time.Unix(0, after.stamp)))
	}

	// --- a genuinely live ADD still propagates (the arm is not vacuous) ---
	beforeLive := issue688Read(rw, gvr)
	createObj(t, rw, dyn, gvr, issue688NS, "p-live", "p-live")
	deliveries.waitHandled(t, gvr, issue688InitialCount+1)
	issue688Drain()
	afterLive := issue688Read(rw, gvr)
	if got := afterLive.addPropagated - beforeLive.addPropagated; got != 1 {
		t.Fatalf("live ADD: propagated %d, want 1", got)
	}
	if afterLive.stamp == 0 {
		t.Fatalf("live ADD did not stamp the freshness clock")
	}
	if afterLive.dirtyMarks <= beforeLive.dirtyMarks {
		t.Fatalf("live ADD did not dirty-mark the list dependent (dirty_mark_total %d → %d)", beforeLive.dirtyMarks, afterLive.dirtyMarks)
	}

	if !standalone {
		return
	}

	// --- relist: a fresh informer re-LISTs; its replay is initial again ---
	// The dependent was re-marked by the live ADD; re-Put it so a mark on the
	// relist is observable as a fresh mark.
	store.Put(l1Key, &ResolvedEntry{RawJSON: []byte(`{"items":[]}`), Inputs: widgetInputs(gvr, issue688NS, "")})
	d.RecordList(context.Background(), l1Key, gvr, issue688NS)
	rw.RemoveResourceType(gvr)
	beforeRelist := issue688Read(rw, gvr)
	issue688Register(t, rw, gvr)
	relisted := issue688InitialCount + 1
	deliveries.waitHandled(t, gvr, issue688InitialCount+1+relisted)
	issue688Drain()
	afterRelist := issue688Read(rw, gvr)
	if got := afterRelist.addPropagated - beforeRelist.addPropagated; got != 0 {
		t.Errorf("#688 relist: %d of %d re-LISTed ADDs delivered past syncCh were propagated as live ADDs, want 0", got, relisted)
	}
	if got := afterRelist.eventsSubmitted - beforeRelist.eventsSubmitted; got != 0 {
		t.Errorf("#688 relist: %d dep events submitted for the re-LIST, want 0", got)
	}
	if got := afterRelist.dirtyMarks - beforeRelist.dirtyMarks; got != 0 {
		t.Errorf("#688 relist: %d dirty-marks from the re-LIST replay, want 0", got)
	}
	if afterRelist.stamp != beforeRelist.stamp {
		t.Errorf("#688 relist: the re-LIST replay moved the freshness clock (%d → %d); only a live event may", beforeRelist.stamp, afterRelist.stamp)
	}
	deliveries.mu.Lock()
	late := deliveries.late
	deliveries.mu.Unlock()
	t.Logf("deliveries held past syncCh: %d (boot %d + live 1 + relist %d)", late, issue688InitialCount, relisted)
}
