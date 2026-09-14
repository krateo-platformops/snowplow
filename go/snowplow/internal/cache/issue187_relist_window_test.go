// issue187_relist_window_test.go — #187 arm B5: the CRD schema-relist
// teardown window strands self entries, and the post-sync dirty-mark re-fire
// is their only trigger.
//
// THE MECHANISM (trace addendum 4 §1). triggerCRDSchemaRelist tears the
// per-GVR informer down and builds a fresh one:
//
//   - RemoveResourceType closes the old informer's per-GVR stop channel and
//     purges its state, so anything still in its DeltaFIFO or in flight on its
//     watch is dropped with NO handler run;
//   - the replacement informer is freshly constructed, so its knownObjects
//     indexer is EMPTY. DeltaFIFO.Replace() synthesises Deleted deltas only
//     for keys present in knownObjects and absent from the new list, so for a
//     fresh informer there is nothing to diff: an object that vanished before
//     the new LIST simply never appears, and NO DELETE IS EVER GENERATED.
//
// OnResourceTypeSchemaRelisted does dirty-mark from the forward dep index
// (not the indexer), so it covers entries whose objects are already gone. But
// it ran ONCE, at the START of the window. An entry dirty-marked at that
// instant re-resolves SUCCESSFULLY while its object still exists, survives,
// and is then stranded when the delete lands a moment later. That is the #187
// burst shape exactly: relist ~09:21, deletes 09:21-09:24.
//
// WHAT THIS ARM DRIVES. The real triggerCRDSchemaRelist, via the real CRD
// lifecycle handlers, against a real ResourceWatcher whose informers really
// sync over a fake dynamic client. The relist's RemoveResourceType /
// EnsureResourceType / dirty-mark sequence is production code; nothing about
// the window is simulated. The object's deletion is modelled by the resolve
// handler's view flipping between the two dirty-marks, which is what a delete
// landing inside the window looks like from the refresher's side.
//
// The assertion is on the SERVED ENTRY — store.Get must MISS — never on a
// counter.
//
// RED on main: no second dirty-mark exists, so nothing ever re-resolves the
// stranded entry and it survives to TTL.
// GREEN with the post-sync re-fire + the drop-point self-404 eviction. The
// re-fire is worthless without the eviction, and vice versa for this window.

package cache

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
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
func b5Watcher(t *testing.T) *ResourceWatcher {
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
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds)

	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("NewResourceWatcher returned nil with CACHE_ENABLED=true")
	}
	t.Cleanup(rw.Stop)

	// Register the "running" informer — the one the relist will tear down.
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

	rw := b5Watcher(t)

	inputs := &ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           b5Group,
		Version:         b5Version,
		Resource:        b5Plural,
		Namespace:       b5NS,
		Name:            b5Object,
	}
	key := ComputeKey(*inputs)
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"pre-delete"}`), Inputs: inputs})
	Deps().Record(key, b5GVR(), b5NS, b5Object) // the self dep edge

	// objectGone models the delete landing INSIDE the teardown window: the
	// first re-resolve (the relist's pre-sync dirty-mark) still finds the
	// object and succeeds; every later one does not. That is precisely the
	// stranding condition — and no DELETE is delivered for it, because a fresh
	// informer synthesises none.
	var objectGone atomic.Bool
	var attempts atomic.Int64
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, in ResolvedKeyInputs) error {
		n := attempts.Add(1)
		if n == 1 {
			// Pre-sync mark: object still live, re-resolve succeeds, entry
			// survives with a fresh body. THE DELETE LANDS NOW.
			store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"still-here"}`), Inputs: inputs})
			objectGone.Store(true)
			return nil
		}
		if objectGone.Load() {
			return fmt.Errorf("resolveAndPopulateL1 %s/%s: re-fetch %s/%s: %s.%s %q not found: %w",
				in.CacheEntryClass, in.Name, in.Resource, in.Name, in.Resource, in.Group, in.Name,
				ErrSelfObjectGone)
		}
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	beforeEvictDelete := Deps().Stats().EvictDeleteTotal

	b5DriveRelist(t, rw)

	// The whole point: no DELETE is ever delivered for the object, so the
	// post-sync dirty-mark is the only trigger that can reach this entry.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if _, alive := store.Get(key); !alive {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if _, alive := store.Get(key); alive {
		s := CRDDiscoveryStatsSnapshot()
		t.Fatalf("#187 B5 RED: an object deleted INSIDE the relist teardown window left its "+
			"self entry RESIDENT after %d refresh attempts. The old informer was torn down "+
			"(its in-flight deltas dropped), the fresh informer synthesises no Deleted delta "+
			"for an object it never saw, and the single pre-sync dirty-mark re-resolved while "+
			"the object still existed — so nothing else will ever touch this entry before its "+
			"TTL. postsync_refires=%d postsync_timeouts=%d",
			attempts.Load(), s.RelistDirtyMarkPostSync, s.RelistPostSyncTimeout)
	}
	if got := CRDDiscoveryStatsSnapshot().RelistDirtyMarkPostSync; got != 1 {
		t.Fatalf("#187 B5: relist_dirtymark_postsync_total = %d, want 1 — the eviction must "+
			"have come from the post-sync re-fire, not from some other trigger", got)
	}
	// No informer DELETE was involved: the H1 counter must not have moved.
	if got := Deps().Stats().EvictDeleteTotal; got != beforeEvictDelete {
		t.Fatalf("#187 B5: evict_delete_total moved (%d -> %d) — a DELETE event WAS delivered, "+
			"so the arm is not exercising the stranding window it claims to",
			beforeEvictDelete, got)
	}
	if got := Deps().Stats().EvictSelfGoneTotal; got == 0 {
		t.Fatalf("#187 B5: the entry went away without a self-gone eviction — unexpected route")
	}
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

	rw := b5Watcher(t)

	inputs := &ResolvedKeyInputs{
		CacheEntryClass: "widgets",
		Group:           b5Group,
		Version:         b5Version,
		Resource:        b5Plural,
		Namespace:       b5NS,
		Name:            "button-live",
	}
	key := ComputeKey(*inputs)
	store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"old"}`), Inputs: inputs})
	Deps().Record(key, b5GVR(), b5NS, "button-live")

	var attempts atomic.Int64
	RegisterRefreshFunc("widgets", func(_ context.Context, _ string, _ ResolvedKeyInputs) error {
		attempts.Add(1)
		store.Put(key, &ResolvedEntry{RawJSON: []byte(`{"v":"fresh"}`), Inputs: inputs})
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	StartRefresher(ctx)

	b5DriveRelist(t, rw)

	// Wait for the post-sync re-fire to have happened, then give the refresher
	// room to do whatever it is going to do.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if CRDDiscoveryStatsSnapshot().RelistDirtyMarkPostSync >= 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(500 * time.Millisecond)

	e, alive := store.Get(key)
	if !alive {
		t.Fatalf("#187 B5b RED: the relist EVICTED an entry whose object is still live, after "+
			"%d refresh attempts. The post-sync dirty-mark must re-RESOLVE, never evict by "+
			"itself — evicting a whole GVR on every CRD schema change would be a cold-cache "+
			"storm dressed up as a fix.", attempts.Load())
	}
	if got := string(e.RawJSON); got != `{"v":"fresh"}` {
		t.Fatalf("#187 B5b: entry survived but was never re-resolved (%s) — the post-sync "+
			"dirty-mark did not reach it", got)
	}
	if got := Deps().Stats().EvictSelfGoneTotal; got != 0 {
		t.Fatalf("#187 B5b: evict_self_gone_total = %d, want 0 for a live object", got)
	}
}
