// issue187_relist_window_test.go — #187 arm B5, re-pointed by #870.
//
// #187 was a TEARDOWN window: a CRD schema relist removed the informer and
// built a fresh one, so a delete landing inside the window was dropped with no
// handler run and the fresh informer synthesised no Deleted delta. 1.12.5 / 1.12.6
// covered it with a post-sync dirty-mark re-fire and a relist delta bridge.
//
// #870 removed the window itself: a schema change BUILDS the replacement beside
// the live informer and swaps it in (store_swap.go). The live copy keeps
// watching for the whole build, so a delete during the build is delivered by it
// like any other event, and the swap delivers only the keys whose bytes changed
// (M). These arms drive the real CRD lifecycle handlers and the real
// triggerCRDSchemaRelist against a real ResourceWatcher whose informers really
// sync over a fake dynamic client; the assertion is on the SERVED ENTRY.
//
//   - B5:  an object deleted WHILE the replacement is being built (its LIST held
//          open) is evicted through the live copy's own DELETE.
//   - B5b: the over-eviction guard. A live, unchanged object survives the swap
//          and is NOT re-resolved: a widen that changes no bytes marks nothing.

package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const (
	b5Group   = "widgets.templates.krateo.io"
	b5Version = "v1beta1"
	b5Plural  = "buttons"
	b5CRDName = "buttons.widgets.templates.krateo.io"
	b5NS      = "demo-system"
	b5Object  = "button-x"
)

func b5GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: b5Group, Version: b5Version, Resource: b5Plural}
}

// b5Watcher builds a ResourceWatcher over a fake dynamic client whose
// informers actually Run and sync, registers the target GVR, and wires the CRD
// meta-GVR syncCh so the lifecycle handlers pass the R1 post-sync gate.
func b5Watcher(t *testing.T, holdRelistUntil <-chan struct{}) *ResourceWatcher {
	t.Helper()

	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	_ = corev1.AddToScheme(sch)
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		b5GVR(): "ButtonList",
	}
	fake := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds)
	var dyn dynamic.Interface = fake
	if holdRelistUntil != nil {
		// #870 — the replacement's LIST (the second LIST of the GVR) is held
		// OUTSIDE the fake's reactor lock, so the live copy's watch and the
		// arm's writes keep flowing while the build is open.
		dyn = newHoldListDynamic(fake, b5GVR(), func(n int64) <-chan struct{} {
			if n > 1 {
				return holdRelistUntil
			}
			return nil
		})
	}

	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("NewResourceWatcher returned nil with CACHE_ENABLED=true")
	}
	t.Cleanup(rw.Stop)

	// Register the "running" informer. #870: it must be OWNED (production gets
	// that from the streaming constructor; with no *rest.Config here the
	// standalone path provides it), or the schema build is refused (bound 6).
	AddNavigationDiscoveredGroup(b5Group)
	_, syncCh := rw.EnsureResourceType(b5GVR())
	select {
	case <-syncCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("target informer did not sync")
	}
	if !rw.IsRegistered(b5GVR()) {
		t.Fatalf("precondition: target GVR not registered after EnsureResourceType")
	}

	// The CRD meta-GVR handlers are invoked directly, so the informer itself is
	// not needed — only a CLOSED syncCh so addEventPostSync lets the ADD
	// through and the lifecycle side-effect fires.
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
	return rw
}

// b5DriveRelist fires the real CRD lifecycle handlers: first an ADD at the
// narrow schema to seed the fingerprint, then an UPDATE that widens it — the
// load-bearing event that calls triggerCRDSchemaRelist.
func b5DriveRelist(t *testing.T, rw *ResourceWatcher) {
	t.Helper()
	handlers := rw.depEventHandlers(CRDGVRForTest())

	narrow := crdBytesObjWithSchema(t, b5CRDName, b5Group, b5Plural, b5Version, narrowButtonSchema, "")
	handlers.AddFunc(narrow)
	if !WaitCRDDiscoveryProcessedForTest(1, 5000) {
		t.Fatalf("worker did not process CRD ADD: %s", crdDiscoveryStatsString())
	}

	widened := crdBytesObjWithSchema(t, b5CRDName, b5Group, b5Plural, b5Version, widenedButtonSchema, "")
	handlers.UpdateFunc(narrow, widened)
	if !WaitCRDDiscoveryProcessedForTest(2, 5000) {
		t.Fatalf("worker did not process CRD UPDATE: %s", crdDiscoveryStatsString())
	}
	if s := CRDDiscoveryStatsSnapshot(); s.SchemaRelistsFired != 1 {
		t.Fatalf("precondition: SchemaRelistsFired=%d want 1 — the relist did not run, so the "+
			"arm is not driving the real teardown window (%s)",
			s.SchemaRelistsFired, crdDiscoveryStatsString())
	}
}

func b5RefresherEnv(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "1")
}

// --- B5 — an object deleted INSIDE the teardown window must not survive ------

func TestIssue187_B5_RelistTeardownWindowStrandsSelfEntry(t *testing.T) {
	b5RefresherEnv(t)
	withCleanCRDDiscovery(t)
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	SetProcessSARestConfig(nil)
	resetRefresherForTest()
	t.Cleanup(resetRefresherForTest)

	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	Deps().SetStore(store)

	firstResolved := make(chan struct{})
	rw := b5Watcher(t, firstResolved)
	// The object exists on the cluster, and the live informer holds it.
	if err := createB5Object(rw, b5Object); err != nil {
		t.Fatalf("create: %v", err)
	}
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if rw.StoreObjectState(b5GVR(), b5NS, b5Object).Found {
			break
		}
	}
	if !rw.StoreObjectState(b5GVR(), b5NS, b5Object).Found {
		t.Fatalf("precondition: the live informer never received the object")
	}

	inputs := &ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           b5Group,
		Version:         b5Version,
		Resource:        b5Plural,
		Namespace:       b5NS,
		Name:            b5Object,
	}
	key := ComputeKey(*inputs)
	store.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"pre-delete"}`), Inputs: inputs})
	Deps().Record(context.Background(), key, b5GVR(), b5NS, b5Object) // the self dep edge

	// The refresher is wired so a wrong whole-type mark would be visible as a
	// re-resolve; the eviction itself comes from the live DELETE.
	var attempts atomic.Int64
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		attempts.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	beforeEvictDelete := Deps().Stats().EvictDeleteTotal
	swapsBefore := StoreSwapStatsSnapshot().Swaps

	b5DriveRelist(t, rw)

	// The replacement's LIST is held (b5HoldRelistList): the build is open and
	// the GVR is still servable from the live copy, which keeps watching.
	if !rw.IsServable(b5GVR()) {
		t.Fatalf("#870 RED: the GVR left servable during a schema build — the build must happen beside the live copy")
	}
	// The delete lands DURING the build. The fake client's watch delivers it to
	// the LIVE informer.
	if err := deleteB5Object(rw, b5Object); err != nil {
		t.Fatalf("delete: %v", err)
	}
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		if _, alive := store.Get(key); !alive {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, alive := store.Get(key); alive {
		t.Fatalf("#187 B5 RED: an object deleted DURING the schema build left its self entry resident "+
			"(%d refresh attempts) — the live copy must keep delivering while its replacement is built", attempts.Load())
	}
	if got := Deps().Stats().EvictDeleteTotal; got == beforeEvictDelete {
		t.Fatalf("#187 B5: the entry went away without an informer DELETE — the live copy's watch is not what removed it")
	}
	close(firstResolved) // release the replacement's LIST: the build completes
	for time.Now().Before(deadline) && StoreSwapStatsSnapshot().Swaps == swapsBefore {
		time.Sleep(20 * time.Millisecond)
	}
	if StoreSwapStatsSnapshot().Swaps == swapsBefore {
		t.Fatalf("#870: the schema build never swapped in after its LIST was released")
	}
}

func createB5Object(rw *ResourceWatcher, name string) error {
	o := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": b5Group + "/" + b5Version,
		"kind":       "Button",
		"metadata":   map[string]any{"name": name, "namespace": b5NS},
	}}
	_, err := rw.dyn.Resource(b5GVR()).Namespace(b5NS).Create(context.Background(), o, metav1.CreateOptions{})
	return err
}

func deleteB5Object(rw *ResourceWatcher, name string) error {
	return rw.dyn.Resource(b5GVR()).Namespace(b5NS).Delete(context.Background(), name, metav1.DeleteOptions{})
}

// --- B5b — the over-eviction guard: a LIVE object must survive the relist ----
//
// Without this, the fix passes while evicting the whole GVR on every relist.
// Same real relist path, same post-sync re-fire — but the object never goes
// away, so the re-resolve keeps succeeding and the entry must stay.

func TestIssue187_B5b_RelistDoesNotEvictALiveObject(t *testing.T) {
	b5RefresherEnv(t)
	withCleanCRDDiscovery(t)
	ResetDepsForTest()
	t.Cleanup(ResetDepsForTest)
	ResetNavigationDiscoveredGroupsForTest()
	t.Cleanup(ResetNavigationDiscoveredGroupsForTest)
	SetProcessSARestConfig(nil)
	resetRefresherForTest()
	t.Cleanup(resetRefresherForTest)

	store := ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	Deps().SetStore(store)

	rw := b5Watcher(t, nil)

	inputs := &ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           b5Group,
		Version:         b5Version,
		Resource:        b5Plural,
		Namespace:       b5NS,
		Name:            "button-live",
	}
	key := ComputeKey(*inputs)
	store.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"old"}`), Inputs: inputs})
	Deps().Record(context.Background(), key, b5GVR(), b5NS, "button-live")

	var attempts atomic.Int64
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		attempts.Add(1)
		store.put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"fresh"}`), Inputs: inputs})
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	swapsBefore := StoreSwapStatsSnapshot().Swaps
	b5DriveRelist(t, rw)

	// Wait for the swap: event-driven, the deadline is a backstop.
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) && StoreSwapStatsSnapshot().Swaps == swapsBefore {
		time.Sleep(20 * time.Millisecond)
	}
	if StoreSwapStatsSnapshot().Swaps == swapsBefore {
		t.Fatalf("#870: the schema build never swapped in")
	}
	time.Sleep(300 * time.Millisecond) // room for any (wrong) mark to be refreshed

	e, alive := store.Get(key)
	if !alive {
		t.Fatalf("#187 B5b RED: the relist EVICTED an entry whose object is still live, after "+
			"%d refresh attempts — evicting a whole GVR on every CRD schema change would be a "+
			"cold-cache storm.", attempts.Load())
	}
	// #870 — the widen changed no bytes (the fake apiserver prunes nothing), so
	// M is empty and nothing is re-resolved: only what changed is marked.
	if got := string(e.RawJSON); got != `{"v":"old"}` || attempts.Load() != 0 {
		t.Fatalf("#870 RED (B5b): a swap that changed no bytes re-resolved the entry (%d attempts, body %s) — "+
			"a whole-type mark is back", attempts.Load(), got)
	}
	if got := Deps().Stats().EvictSelfGoneTotal; got != 0 {
		t.Fatalf("#187 B5b: evict_self_gone_total = %d, want 0 for a live object", got)
	}
}
