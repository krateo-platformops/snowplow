package cache

// store_verify_repair_falsifier_test.go — #237 deliverable B, the deadline
// pass and the repair verb.
//
// These arms drive a REAL ResourceWatcher over a fake cluster whose WATCH
// DELIVERS NOTHING. That is the whole fixture, and it is the defect's own
// shape rather than a model of it: the object changes on the cluster, no watch
// event arrives, and the indexer keeps serving the old copy — which is exactly
// "present but stale", the state every existing mechanism in snowplow is blind
// to. Nothing here installs a crossed store by hand.

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/metadata"
	metadatafake "k8s.io/client-go/metadata/fake"
	k8stesting "k8s.io/client-go/testing"
)

// --- the recording metadata client ----------------------------------------

// recordedList is one forced LIST as it left the process.
type recordedList struct {
	gvr  schema.GroupVersionResource
	opts metav1.ListOptions
}

// recordingMetadataClient captures the ListOptions of every metadata LIST.
//
// It exists because the fake metadata client folds ListOptions into
// ListRestrictions (labels and fields only), so a k8stesting reactor CANNOT
// see resourceVersion, resourceVersionMatch or limit — and those three ARE the
// assertion: a LIST carrying a limit together with a non-zero resourceVersion
// is delegated to etcd and skips the watch cache, which is the one thing this
// pass must never do.
type recordingMetadataClient struct {
	inner metadata.Interface
	mu    sync.Mutex
	lists []recordedList
}

func (c *recordingMetadataClient) Resource(gvr schema.GroupVersionResource) metadata.Getter {
	return &recordingGetter{Getter: c.inner.Resource(gvr), c: c, gvr: gvr}
}

func (c *recordingMetadataClient) record(gvr schema.GroupVersionResource, opts metav1.ListOptions) {
	c.mu.Lock()
	c.lists = append(c.lists, recordedList{gvr: gvr, opts: opts})
	c.mu.Unlock()
}

func (c *recordingMetadataClient) recorded() []recordedList {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]recordedList(nil), c.lists...)
}

type recordingGetter struct {
	metadata.Getter
	c   *recordingMetadataClient
	gvr schema.GroupVersionResource
}

func (g *recordingGetter) Namespace(ns string) metadata.ResourceInterface {
	return &recordingResource{ResourceInterface: g.Getter.Namespace(ns), c: g.c, gvr: g.gvr}
}

func (g *recordingGetter) List(ctx context.Context, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	g.c.record(g.gvr, opts)
	return g.Getter.List(ctx, opts)
}

type recordingResource struct {
	metadata.ResourceInterface
	c   *recordingMetadataClient
	gvr schema.GroupVersionResource
}

func (r *recordingResource) List(ctx context.Context, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	r.c.record(r.gvr, opts)
	return r.ResourceInterface.List(ctx, opts)
}

// --- the stale-store fixture ----------------------------------------------

var repairGVR = schema.GroupVersionResource{Group: "widgets.krateo.io", Version: "v1beta1", Resource: "panels"}

// silentWatchCluster builds a ResourceWatcher over a fake dynamic client whose
// WATCH never delivers an event.
//
// This is the lost-event regime itself. The initial LIST populates the
// indexer; every later change to the cluster is invisible to the informer, so
// the store holds an object that is present, correct-looking, and WRONG. It is
// the only fixture in which a repair has anything to repair.
func silentWatchCluster(t *testing.T, objs ...*unstructured.Unstructured) (*ResourceWatcher, dynamic.Interface) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")

	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	// NewResourceWatcher registers the four typed-RBAC informers eagerly, so
	// their list kinds must be present or the fake client panics on the first
	// LIST — the same set realWatcher registers, for the same reason.
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch,
		map[schema.GroupVersionResource]string{
			repairGVR: "PanelList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		})
	for _, o := range objs {
		if _, err := dyn.Resource(repairGVR).Namespace(o.GetNamespace()).
			Create(context.Background(), o, metav1.CreateOptions{}); err != nil {
			t.Fatalf("seeding the fake cluster: %v", err)
		}
	}
	// The silent watch. Returning a never-written FakeWatcher is what makes
	// the informer deaf: the reflector establishes a watch, blocks on it, and
	// receives nothing, which is what a dropped event stream looks like from
	// inside the process.
	dyn.PrependWatchReactor("*", func(k8stesting.Action) (bool, watch.Interface, error) {
		w := watch.NewFake()
		t.Cleanup(w.Stop)
		return true, w, nil
	})
	// A LIST that carries a collection resourceVersion, as every real
	// apiserver does. The stock fake returns none, which would leave
	// lastSyncRV empty and silently push the forced pass onto its RV="0"
	// fallback — so the arm asserting "the pass carries OUR sync position"
	// would be asserting the fallback instead, and would stay green if the
	// real branch broke.
	dyn.PrependReactor("list", repairGVR.Resource, func(a k8stesting.Action) (bool, k8sruntime.Object, error) {
		obj, err := dyn.Tracker().List(repairGVR,
			repairGVR.GroupVersion().WithKind("Panel"), a.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		if l, ok := obj.(*unstructured.UnstructuredList); ok {
			l.SetResourceVersion(clusterListRV)
		}
		return true, obj, nil
	})

	// The repair verb needs a GVR that OWNS its informer: a shared-factory
	// informer torn down by RemoveResourceType is handed straight back,
	// stopped. Production gets that from the streaming constructor; with no
	// *rest.Config here the standalone dynamic path provides it, and that path
	// is selected by the group being navigation-discovered.
	//
	// IT MUST BE MARKED BEFORE REGISTRATION. ownsInformer is recorded by the
	// branch that CONSTRUCTS the informer, so marking the group afterwards
	// would leave the GVR recorded as factory-built and every repair refused —
	// the arms would then fail for a fixture reason that looks exactly like
	// the defect they exist to catch.
	AddNavigationDiscoveredGroup(repairGVR.Group)

	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v (rw=%v)", err, rw)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	_, syncCh := rw.EnsureResourceType(repairGVR)
	select {
	case <-syncCh:
	case <-time.After(verifyBound):
		t.Fatalf("informer for %s did not sync", repairGVR)
	}
	// Populates lastSyncRV, which is the resourceVersion the forced LIST must
	// carry. With no discovery client, conjunct 4 is degraded-true and the
	// refresh is purely the conjunct-3 / lastSyncRV update.
	rw.RefreshDiscovery(context.Background())
	if !rw.IsServable(repairGVR) {
		t.Fatalf("precondition: %s is registered and synced but not servable", repairGVR)
	}
	SetGlobal(rw)
	t.Cleanup(func() { SetGlobal(nil) })
	return rw, dyn
}

// clusterListRV is the collection resourceVersion the fake cluster reports on
// every LIST. Fixed rather than incrementing: the arms assert that the forced
// pass carries the watcher's OWN recorded sync position, and a moving target
// would make that assertion about timing instead.
const clusterListRV = "4242"

func panelObj(ns, name, rv, uid string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": repairGVR.GroupVersion().String(),
		"kind":       "Panel",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       ns,
			"resourceVersion": rv,
			"uid":             uid,
		},
	}}
	return u
}

func partialMeta(ns, name, rv, uid string) *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{
		TypeMeta: metav1.TypeMeta{
			APIVersion: repairGVR.GroupVersion().String(),
			Kind:       "Panel",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       ns,
			Name:            name,
			ResourceVersion: rv,
			UID:             k8stypes.UID(uid),
		},
	}
}

// attachMetaClient installs a recording metadata client holding objs as the
// apiserver's view.
func attachMetaClient(t *testing.T, rw *ResourceWatcher, objs ...*metav1.PartialObjectMetadata) *recordingMetadataClient {
	t.Helper()
	sch := metadatafake.NewTestScheme()
	if err := metav1.AddMetaToScheme(sch); err != nil {
		t.Fatalf("AddMetaToScheme: %v", err)
	}
	runtimeObjs := make([]k8sruntime.Object, 0, len(objs))
	for _, o := range objs {
		runtimeObjs = append(runtimeObjs, o)
	}
	rec := &recordingMetadataClient{inner: metadatafake.NewSimpleMetadataClient(sch, runtimeObjs...)}
	rw.SetMetadataClient(rec)
	return rec
}

// indexerRV reads one object's resourceVersion out of the watcher's own
// indexer — the assertion that matters, as opposed to a counter.
func indexerRV(t *testing.T, rw *ResourceWatcher, ns, name string) (string, bool) {
	t.Helper()
	st := rw.StoreObjectState(repairGVR, ns, name)
	return st.ResourceVersion, st.Found
}

// --- B-2 — the standing record of what the existing audit cannot see -------

// TestReconcileAudit_IsBlindToPresentButStale — GREEN TODAY AND MUST STAY
// GREEN. It is the control that proves the other arms test a real hole.
//
// The reconcile audit acts only on ABSENCE (deps_reconcile.go: `if st !=
// objAbsent { continue }`), so a present-but-stale object is skipped before
// any field is compared. This arm makes that executable by running the audit
// over TWO DIFFERENT STORE CONTENTS and asserting its report is IDENTICAL. An
// audit whose output cannot distinguish two different stores cannot, a
// fortiori, distinguish a stale one from a fresh one — which is why the live
// #237 capture read divergent:0 with the defect active, and why raising
// DEPS_RECONCILE_SAMPLE would have changed nothing.
func TestReconcileAudit_IsBlindToPresentButStale(t *testing.T) {
	c3Setup(t)
	resetResolvedCacheForTest()
	t.Cleanup(resetResolvedCacheForTest)
	resetVerificationForArm(t)

	rw, dyn := silentWatchCluster(t, panelObj("demo", "panel-a", "1", "uid-a"))
	store := ResolvedCache()
	Deps().SetStore(store)
	Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {})
	c3Put(store, "L1_panel-a", repairGVR, "demo", "panel-a")

	if rv, ok := indexerRV(t, rw, "demo", "panel-a"); !ok || rv != "1" {
		t.Fatalf("precondition: store holds rv=%q ok=%v, want rv=1", rv, ok)
	}
	first, ok := ReconcileFull()
	if !ok {
		t.Fatal("ReconcileFull did not run")
	}
	if first.Probed == 0 {
		t.Fatalf("the audit probed nothing (%+v) — a report of 0 divergences over 0 probes says "+
			"nothing about blindness", first)
	}
	if first.Divergent != 0 {
		t.Fatalf("divergent=%d over a present object, want 0", first.Divergent)
	}

	// Move the cluster on and let the store follow, so the two passes see
	// GENUINELY DIFFERENT store contents. (The watch is silent, so the change
	// reaches the indexer only through a relist — which is precisely the point
	// being made: without one, nothing notices at all.)
	updated := panelObj("demo", "panel-a", "2", "uid-a")
	if _, err := dyn.Resource(repairGVR).Namespace("demo").
		Update(context.Background(), updated, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("updating the fake cluster: %v", err)
	}
	if rv, _ := indexerRV(t, rw, "demo", "panel-a"); rv != "1" {
		t.Fatalf("the silent watch leaked an event: store already reads rv=%q", rv)
	}
	// Force the store to catch up through a real relist, so the SECOND audit
	// pass runs over rv=2.
	rw.removeResourceTypeWithReason(repairGVR, confirmRetractUnspecified)
	_, syncCh := rw.EnsureResourceType(repairGVR)
	select {
	case <-syncCh:
	case <-time.After(verifyBound):
		t.Fatal("the relisted informer did not sync")
	}
	rw.RefreshDiscovery(context.Background())
	waitForVerify(t, "the store to hold rv=2", verifyBound, func() bool {
		rv, ok := indexerRV(t, rw, "demo", "panel-a")
		return ok && rv == "2"
	})

	second, ok := ReconcileFull()
	if !ok {
		t.Fatal("the second ReconcileFull did not run")
	}
	// THE BLINDNESS, stated as an equality. Two different stores, one report.
	if first.Probed != second.Probed || first.Divergent != second.Divergent || first.Sampled != second.Sampled {
		t.Fatalf("the audit distinguished rv=1 from rv=2: first=%+v second=%+v — if this ever "+
			"fails, the audit has gained content awareness and deliverable B's premise "+
			"needs re-examining, which is exactly what this arm is here to tell you", first, second)
	}
	if second.Divergent != 0 {
		t.Fatalf("divergent=%d, want 0", second.Divergent)
	}
	t.Logf("the audit reports the identical %+v over a store at rv=1 and a store at rv=2", second)
}

// --- B-5 — the deadline forces a full, watch-cache-served pass -------------

// TestStoreVerification_DeadlineForcesFullGVRPass — B-5.
//
// The wire-shape assertions are made POSITIVELY first (gate condition B-4): a
// test that recorded zero requests would satisfy "no limit was sent"
// vacuously, and would be green on a binary where the pass never ran at all.
func TestStoreVerification_DeadlineForcesFullGVRPass(t *testing.T) {
	c3Setup(t)
	resetVerificationForArm(t)

	rw, _ := silentWatchCluster(t,
		panelObj("demo", "panel-a", "1", "uid-a"),
		panelObj("demo", "panel-b", "1", "uid-b"),
	)
	rec := attachMetaClient(t,
		rw,
		partialMeta("demo", "panel-a", "1", "uid-a"),
		partialMeta("demo", "panel-b", "1", "uid-b"),
	)

	// B-6: a GVR that has never been verified must read its age since
	// REGISTRATION, never 0. A zero here would read as perfect health while
	// nothing had been verified at all.
	_, _, _, maxAgeBefore, _ := storeVerificationGauges()
	if maxAgeBefore <= 0 {
		t.Fatalf("store_verification_max_age_seconds = %v before any verification; a never-verified "+
			"GVR must report age-since-registration, never 0", maxAgeBefore)
	}

	before := storeObjectsVerifiedTotal.Load()
	forcedVerify(context.Background(), repairGVR)

	// VACUITY GUARD FIRST: exactly one forced LIST for this GVR.
	lists := rec.recorded()
	if len(lists) != 1 {
		t.Fatalf("recorded %d forced LISTs, want exactly 1 — every assertion below is vacuous "+
			"without this", len(lists))
	}
	got := lists[0]
	if got.gvr != repairGVR {
		t.Errorf("forced LIST addressed %s, want %s", got.gvr, repairGVR)
	}
	wantRV := lastSyncRVForTest(rw, repairGVR)
	if wantRV == "" {
		t.Fatal("precondition: lastSyncRV is empty, so the arm cannot assert the pass carries it")
	}
	if got.opts.ResourceVersion != wantRV {
		t.Errorf("forced LIST carried resourceVersion=%q, want the store's own lastSyncRV %q — "+
			"any other value breaks the guarantee that the result is at least as fresh as our store",
			got.opts.ResourceVersion, wantRV)
	}
	if got.opts.ResourceVersionMatch != metav1.ResourceVersionMatchNotOlderThan {
		t.Errorf("forced LIST carried resourceVersionMatch=%q, want NotOlderThan — without it the "+
			"cacher may answer from behind our own position and invent false positives",
			got.opts.ResourceVersionMatch)
	}
	// THE ONE THAT MUST NEVER REGRESS: a limit together with a non-zero
	// resourceVersion is delegated to etcd and skips the watch cache.
	if got.opts.Limit != 0 {
		t.Errorf("forced LIST carried limit=%d with resourceVersion=%q — that shape is delegated to "+
			"etcd by client-go's own documented rule, and this pass must never be an etcd read",
			got.opts.Limit, got.opts.ResourceVersion)
	}

	// R4 — COVERAGE, NOT SAMPLING. The deadline chooses WHEN, never WHAT.
	delta := storeObjectsVerifiedTotal.Load() - before
	indexerCount := rw.StoreObjectState(repairGVR, "demo", "panel-a").IndexerCount
	if int(delta) != indexerCount {
		t.Errorf("the pass compared %d objects but the GVR holds %d — a shortfall means sampling "+
			"crept in and the coverage guarantee is gone", delta, indexerCount)
	}

	// No divergence in this fixture, and the GVR is no longer overdue.
	if lu, ld, la, um := divergenceCounts(verifySiteForced); lu+ld+la+um != 0 {
		t.Errorf("a matching store reported divergences: %d/%d/%d/%d", lu, ld, la, um)
	}
	unverified, unverifiable, _, maxAge, _ := storeVerificationGauges()
	if unverified != 0 {
		t.Errorf("store_gvrs_unverified = %d after a completed pass, want 0", unverified)
	}
	if unverifiable != 0 {
		t.Errorf("store_gvrs_unverifiable = %d, want 0", unverifiable)
	}
	if maxAge <= 0 || time.Duration(maxAge)*time.Second > maxVerificationAge() {
		t.Errorf("store_verification_max_age_seconds = %v; want >0 and within the deadline", maxAge)
	}

	// WORST CASE (feedback_design_claim_worst_case_falsifier): with the pass
	// unable to run, the age rises past the deadline and the gauge SAYS SO
	// rather than reading 0.
	storeVerify.mu.Lock()
	storeVerify.gvrs[repairGVR].epoch = time.Now().Add(-2 * maxVerificationAge())
	storeVerify.mu.Unlock()
	unverifiedLate, _, _, maxAgeLate, _ := storeVerificationGauges()
	if unverifiedLate != 1 {
		t.Errorf("store_gvrs_unverified = %d with the GVR two deadlines overdue, want 1", unverifiedLate)
	}
	if time.Duration(maxAgeLate)*time.Second <= maxVerificationAge() {
		t.Errorf("store_verification_max_age_seconds = %v, want above the %s deadline", maxAgeLate, maxVerificationAge())
	}
}

// TestStoreVerification_NoSyncPosition_SkipsRatherThanWeakensTheOracle.
//
// The architect's REQUIRED fix, as an executable guard. With no recorded sync
// position the pass must issue NO LIST AT ALL and count a skip — it must not
// fall back to resourceVersion="0".
//
// Why a fallback is not a lesser evil: the forced pass's soundness rests
// entirely on NotOlderThan at OUR lastSyncRV making the returned set provably
// no older than the store, which is what licenses an OPAQUE resourceVersion
// inequality to mean "the store is behind". Under RV="0" the cacher may answer
// from BEHIND our store, and then every class inverts — objects created after
// its snapshot read as lost DELETEs, objects deleted after it read as lost
// ADDs. The chain ends with a repair that was never needed, a real
// multi-minute non-servable window, and the breaker latching
// store_repair_ineffective_total, which this family's own description defines
// as "our detector is wrong". The instrument would accuse itself of a fault it
// caused.
func TestStoreVerification_NoSyncPosition_SkipsRatherThanWeakensTheOracle(t *testing.T) {
	c3Setup(t)
	resetVerificationForArm(t)
	rw, _ := silentWatchCluster(t, panelObj("demo", "panel-a", "1", "uid-a"))
	rec := attachMetaClient(t, rw, partialMeta("demo", "panel-a", "1", "uid-a"))

	// Clear the recorded sync position, reproducing the reachable window the
	// architect traced: applyConfirmLocked writes `confirmed` and writes
	// lastSyncRV only when the informer reports a non-empty
	// LastSyncResourceVersion, AFTER and independently — so a confirm pass
	// landing before the informer syncs leaves servable && synced && rv == "".
	rw.mu.Lock()
	delete(rw.lastSyncRV, repairGVR)
	rw.mu.Unlock()
	if got := lastSyncRVForTest(rw, repairGVR); got != "" {
		t.Fatalf("precondition: lastSyncRV is %q, want empty", got)
	}

	forcedVerify(context.Background(), repairGVR)

	if n := len(rec.recorded()); n != 0 {
		t.Errorf("the pass issued %d LIST(s) with no sync position; it must issue NONE rather than "+
			"downgrade to resourceVersion=0, which can return a set OLDER than our store and "+
			"invert every divergence class", n)
	}
	if got := storeVerifyForcedListsTotal.Load(); got != 0 {
		t.Errorf("store_verification_forced_lists_total = %d, want 0", got)
	}
	if got := VerifySkippedByReasonSnapshot()[verifySkipNoSyncPosition]; got != 1 {
		t.Errorf("skipped_by_reason[%s] = %d, want 1 — a skip must be VISIBLE, because "+
			"'I could not run a sound comparison' and 'I compared and found nothing' are "+
			"different statements that would otherwise share one spelling",
			verifySkipNoSyncPosition, got)
	}
	if lu, ld, la, um := divergenceCounts(verifySiteForced); lu+ld+la+um != 0 {
		t.Errorf("divergences reported from a pass that never ran: %d/%d/%d/%d", lu, ld, la, um)
	}
	if got := storeRepairQueueDepth(); got != 0 {
		t.Errorf("store_repairs_pending = %d — a skipped pass must never enqueue a repair, which "+
			"would open a real non-servable window on the strength of a comparison that did "+
			"not happen", got)
	}
}

// --- B-7 — the pass repairs the STORE, not just a counter -----------------

// TestForcedPass_RepairsTheStore_NotJustL1 — B-7, the arm a gate should look at
// first.
//
// The assertion is NOT that a counter moved and NOT that a dep event was
// submitted. It is that THE INDEXER HOLDS THE NEW OBJECT. #237 measured the
// alternative from outside: eight consecutive /call requests over twelve
// minutes returning a byte-identical stale body while every counter looked
// fine.
func TestForcedPass_RepairsTheStore_NotJustL1(t *testing.T) {
	t.Run("repair_enabled_store_is_repaired", func(t *testing.T) {
		c3Setup(t)
		resetVerificationForArm(t)
		logs := captureCacheEvents(t)
		resetResolvedCacheForTest()
		t.Cleanup(resetResolvedCacheForTest)
		rw, dyn := silentWatchCluster(t, panelObj("demo", "panel-a", "1", "uid-a"))
		// A resident L1 cell that DEPENDS on the object. Without a dependent
		// there is nothing for the repair to dirty-mark, and the cause label
		// this arm asserts would never be emitted — the arm would pass or fail
		// on the absence of an edge rather than on the attribution.
		store := ResolvedCache()
		Deps().SetStore(store)
		Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {})
		c3Put(store, "L1_panel-a", repairGVR, "demo", "panel-a")

		// The cluster moves on; the silent watch delivers nothing.
		if _, err := dyn.Resource(repairGVR).Namespace("demo").
			Update(context.Background(), panelObj("demo", "panel-a", "2", "uid-a"), metav1.UpdateOptions{}); err != nil {
			t.Fatalf("updating the fake cluster: %v", err)
		}
		attachMetaClient(t, rw, partialMeta("demo", "panel-a", "2", "uid-a"))
		if rv, _ := indexerRV(t, rw, "demo", "panel-a"); rv != "1" {
			t.Fatalf("precondition: the store should still read rv=1, reads %q", rv)
		}

		// 1. DETECTION.
		forcedVerify(context.Background(), repairGVR)
		if lu, _, _, _ := divergenceCounts(verifySiteForced); lu != 1 {
			t.Fatalf("lost_update_forced = %d, want 1", lu)
		}
		if lu, _, _, _ := divergenceCounts(verifySiteSnapshot); lu != 0 {
			t.Errorf("snapshot-site counter moved (%d) with no snapshot taken — the site split is not holding", lu)
		}

		// 2. THE REPAIR FIRED, and it actually re-registered the informer.
		if storeRepairQueueDepth() != 1 {
			t.Fatalf("store_repairs_pending = %d after a confirmed divergence, want 1", storeRepairQueueDepth())
		}
		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go runStoreRepairQueue(ctx)
		waitForVerify(t, "the repair to fire", verifyBound, func() bool {
			return storeRepairsFiredTotal.Load() == 1
		})
		// The repair must NAME ITSELF in the event log rather than reusing the
		// schema-relist label. #237 is an issue about instruments that named
		// the wrong cause, so a store repair logged as SCHEMA_RELIST would be
		// the same defect shipped inside its own fix.
		//
		// Asserted on the dep-tracker cause rather than on
		// confirm_retracted_by_reason, because that counter only ticks for a
		// GVR that was actually CONFIRMED — and with no discovery client
		// wired, conjunct 4 is degraded-true and rw.confirmed is never
		// populated. Counting a retraction that did not happen would be the
		// very thing informer_watch_stats.go warns against, so the fixture
		// cannot assert it and says so instead of asserting something weaker
		// under the same name.
		if !sawEventType(logs, "STORE_REPAIR") {
			t.Errorf("no cache_event.consumed carried type=STORE_REPAIR; the repair's dirty-mark did "+
				"not name its own cause. Records seen: %v", eventTypes(logs))
		}
		if sawEventType(logs, "SCHEMA_RELIST") {
			t.Errorf("the store repair logged itself as SCHEMA_RELIST — an instrument naming the " +
				"wrong cause, which is the defect class #237 is about")
		}

		// 3. THE STORE. This is the assertion B-1 was about.
		waitForVerify(t, "the indexer to hold the repaired object", verifyBound, func() bool {
			rv, ok := indexerRV(t, rw, "demo", "panel-a")
			return ok && rv == "2"
		})

		// 4. And the queue drained — the gauge counts the in-flight repair, so
		// reaching 0 means the work finished, not that it started.
		waitForVerify(t, "the repair queue to drain", verifyBound, func() bool {
			return storeRepairQueueDepth() == 0
		})
	})

	t.Run("worst_case_repair_disabled_store_stays_stale", func(t *testing.T) {
		// THE DISCRIMINATING HALF. Same fixture, repair never run. Detection
		// alone produces a green-looking divergence counter OVER A WRONG
		// STORE, and without this arm a future refactor could drop the repair
		// while every other arm stayed green.
		c3Setup(t)
		resetVerificationForArm(t)
		rw, dyn := silentWatchCluster(t, panelObj("demo", "panel-a", "1", "uid-a"))
		Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {})

		if _, err := dyn.Resource(repairGVR).Namespace("demo").
			Update(context.Background(), panelObj("demo", "panel-a", "2", "uid-a"), metav1.UpdateOptions{}); err != nil {
			t.Fatalf("updating the fake cluster: %v", err)
		}
		attachMetaClient(t, rw, partialMeta("demo", "panel-a", "2", "uid-a"))

		forcedVerify(context.Background(), repairGVR)

		if lu, _, _, _ := divergenceCounts(verifySiteForced); lu != 1 {
			t.Fatalf("lost_update_forced = %d, want 1", lu)
		}
		if storeRepairsFiredTotal.Load() != 0 {
			t.Fatalf("a repair fired with no queue worker running (%d)", storeRepairsFiredTotal.Load())
		}
		// The whole point: the counter is green-looking and the store is WRONG.
		time.Sleep(200 * time.Millisecond)
		rv, ok := indexerRV(t, rw, "demo", "panel-a")
		if !ok || rv != "1" {
			t.Fatalf("the store read rv=%q ok=%v without a repair; this arm must observe the STALE "+
				"store, or it is not demonstrating that detection alone is insufficient", rv, ok)
		}
		// And the queue says so out loud, which is what makes the state readable.
		if depth := storeRepairQueueDepth(); depth != 1 {
			t.Errorf("store_repairs_pending = %d, want 1 — an outstanding repair must be visible", depth)
		}
		if age := storeRepairQueueAgeSeconds(); age <= 0 {
			t.Errorf("store_repair_queue_age_seconds = %v, want >0 for a waiting repair", age)
		}
	})
}

// --- B-8 — the bounds -----------------------------------------------------

// TestForcedPass_RepairBounds — B-8. Without this arm the design's largest cost
// (a rebuild window per repair, during which the GVR is non-servable and every
// resolve for it falls through to the apiserver) has no guard that a test would
// notice being removed.
func TestForcedPass_RepairBounds(t *testing.T) {
	t.Run("circuit_breaker_latches_after_one_ineffective_repair", func(t *testing.T) {
		c3Setup(t)
		resetVerificationForArm(t)
		// The cluster and the apiserver's metadata view DISAGREE PERMANENTLY:
		// the metadata client reports an object the dynamic cluster does not
		// have, so no relist can ever reconcile them. That is the
		// "I relisted and it is still divergent" state, which does not mean a
		// lost event — it means the comparison is wrong.
		rw, _ := silentWatchCluster(t, panelObj("demo", "panel-a", "1", "uid-a"))
		Deps().SetRefreshHook(func(string, schema.GroupVersionResource) {})
		attachMetaClient(t, rw,
			partialMeta("demo", "panel-a", "1", "uid-a"),
			partialMeta("demo", "panel-phantom", "1", "uid-p"),
		)

		ctx, cancel := context.WithCancel(context.Background())
		t.Cleanup(cancel)
		go runStoreRepairQueue(ctx)

		// Pass 1: divergence, repair enqueued and fired.
		forcedVerify(context.Background(), repairGVR)
		if _, _, la, _ := divergenceCounts(verifySiteForced); la != 1 {
			t.Fatalf("lost_add_forced = %d, want 1", la)
		}
		waitForVerify(t, "the first repair to fire", verifyBound, func() bool {
			return storeRepairsFiredTotal.Load() == 1
		})
		waitForVerify(t, "the repair queue to drain", verifyBound, func() bool {
			return storeRepairQueueDepth() == 0
		})
		rw.RefreshDiscovery(context.Background())

		// Pass 2: STILL divergent. The breaker latches.
		forcedVerify(context.Background(), repairGVR)
		if got := storeRepairIneffectiveTotal.Load(); got != 1 {
			t.Fatalf("store_repair_ineffective_total = %d, want 1 — a relist that did not clear the "+
				"divergence must be named as OUR bug, not the store's", got)
		}
		if got := storeRepairSuppressedTotal.Load(); got != 1 {
			t.Fatalf("store_repair_suppressed_total = %d, want 1", got)
		}

		// Pass 3: detection CONTINUES, repair does NOT. Silence must never be
		// produced by the breaker.
		_, _, addBefore, _ := divergenceCounts(verifySiteForced)
		rw.RefreshDiscovery(context.Background())
		forcedVerify(context.Background(), repairGVR)
		_, _, addAfter, _ := divergenceCounts(verifySiteForced)
		if addAfter <= addBefore {
			t.Errorf("divergence stopped being reported under suppression (%d -> %d) — the breaker "+
				"must stop REPAIRING, never stop DETECTING", addBefore, addAfter)
		}
		time.Sleep(200 * time.Millisecond)
		if got := storeRepairsFiredTotal.Load(); got != 1 {
			t.Errorf("store_repairs_fired_total = %d after suppression, want it to stay at 1 — "+
				"hammering a GVR whose divergence a relist cannot clear is self-harm", got)
		}
	})

	t.Run("pending_repairs_are_deduped_per_gvr", func(t *testing.T) {
		// RG-2. Bound 1 forbids CONCURRENT relists of one GVR; it does not by
		// itself forbid a second queue entry for one already waiting, and a
		// GVR re-detected at the next deadline while still pending would
		// otherwise be enqueued twice — doubling the gauge and the work.
		c3Setup(t)
		resetVerificationForArm(t)
		rememberStoreVerification(repairGVR, true, true)

		if !enqueueStoreRepair(repairGVR, "first") {
			t.Fatal("the first enqueue was refused")
		}
		if enqueueStoreRepair(repairGVR, "second") {
			t.Error("a second repair was queued for a GVR already pending — store_repairs_pending " +
				"would double and the GVR would be relisted twice")
		}
		if got := storeRepairQueueDepth(); got != 1 {
			t.Errorf("store_repairs_pending = %d after a duplicate enqueue, want 1", got)
		}
	})

	t.Run("repair_is_refused_on_ownership_not_on_group_name", func(t *testing.T) {
		// THE DRIFT ARM (PM condition C-B1). The property bound 6 needs is
		// "does this GVR own its informer" — can it be torn down and rebuilt.
		// An earlier draft derived that from the GVR's GROUP, which answers
		// the right question only by coincidence of today's routing.
		//
		// This arm constructs the exact state a group-derived check would
		// misjudge: a GVR whose informer is FACTORY-BUILT (not owned) sitting
		// in a group the walker HAS navigation-discovered. Today's routing
		// cannot produce that pair, which is precisely why no other arm in the
		// matrix would fail if the predicate regressed to the group name —
		// they agree on every GVR that currently exists. H5 has already
		// re-routed informers once; the next change makes this pair real, and
		// the GVR would then be killed by its own repair.
		c3Setup(t)
		resetVerificationForArm(t)
		drifted := schema.GroupVersionResource{
			Group: "widgets.krateo.io", Version: "v1beta1", Resource: "driftedpanels",
		}
		AddNavigationDiscoveredGroup(drifted.Group)
		if !IsNavigationDiscoveredGroup(drifted.Group) {
			t.Fatal("precondition: the group must be navigation-discovered, or this arm proves nothing")
		}
		// Factory-built: NOT owned, and not decorated either.
		rememberStoreVerification(drifted, false, false)

		if enqueueStoreRepair(drifted, "drift") {
			t.Fatal("a repair was queued for a GVR that does NOT own its informer. The teardown would " +
				"stop the factory's cached informer and the re-register would hand the STOPPED one " +
				"back, killing the cache for this GVR while store_repairs_fired_total counted a success")
		}
		if got := storeRepairUnsupportedTotal.Load(); got != 1 {
			t.Errorf("store_repair_unsupported_total = %d, want 1 — the refusal must be counted, or "+
				"a GVR that is detected-but-never-repaired looks identical to one nobody acted on", got)
		}
		if got := storeRepairQueueDepth(); got != 0 {
			t.Errorf("store_repairs_pending = %d after a refused repair, want 0", got)
		}

		// The mirror image: OWNED but in a group that is NOT
		// navigation-discovered. A group-derived check would wrongly REFUSE
		// this one, silently withdrawing repair from a GVR that can be
		// rebuilt perfectly well — the same proxy failing in the other
		// direction, and just as invisible.
		owned := schema.GroupVersionResource{
			Group: "undiscovered.krateo.io", Version: "v1beta1", Resource: "ownedpanels",
		}
		if IsNavigationDiscoveredGroup(owned.Group) {
			t.Fatal("precondition: this group must NOT be navigation-discovered")
		}
		rememberStoreVerification(owned, true, true)
		if !enqueueStoreRepair(owned, "owned") {
			t.Fatal("a repair was refused for a GVR that DOES own its informer — the predicate is " +
				"still reading the group name, not the recorded ownership")
		}
		if got := storeRepairUnsupportedTotal.Load(); got != 1 {
			t.Errorf("store_repair_unsupported_total = %d after one refusal and one acceptance, want 1", got)
		}
	})

	t.Run("the_unrepairable_class_is_exactly_the_typed_rbac_four", func(t *testing.T) {
		// THE DRIFT GUARD on the refused SET, not just on the predicate.
		//
		// `store_repair_unsupported_total` going non-zero cannot, on its own,
		// distinguish "the known unrepairable GVRs, as designed" from "a new
		// GVR has quietly joined the unrepairable set". That is two regimes in
		// one number — the shape this whole investigation exists to remove —
		// and the counter alone cannot fix it, because the number is the same
		// either way. What fixes it is pinning the SET.
		//
		// Under production routing a GVR is unrepairable iff it takes the
		// shared factory, which is iff it is a streaming exception AND its
		// group is not navigation-discovered. isStreamingException is true
		// exactly for the typed-RBAC overrides, which exist because
		// stripAndType requires *unstructured.Unstructured — their
		// construction is load-bearing for RBAC correctness, which is why the
		// right answer is to leave them unrepairable rather than to re-route
		// them for a diagnostic's benefit.
		//
		// So: if a fifth typed override is ever added, this arm fails and
		// NAMES it, instead of the operator discovering a silently widened
		// unrepairable class from a counter that merely ticked higher.
		for _, gvr := range RBACResourceTypes {
			if !isStreamingException(gvr) {
				t.Errorf("%s is no longer a streaming exception — it would now take the streaming "+
					"path and become repairable, which changes the unrepairable class", gvr)
			}
		}
		exceptions := map[schema.GroupVersionResource]bool{}
		for gvr := range typedResourceOverrides {
			exceptions[gvr] = true
		}
		if len(exceptions) != len(RBACResourceTypes) {
			t.Fatalf("the typed-override set has %d members, the RBAC set has %d — the unrepairable "+
				"class has changed size. Every member of the typed-override set is factory-built and "+
				"therefore DETECTED BUT NEVER REPAIRED, so a new member silently widens the class "+
				"whose staleness is unbounded. Members: %v",
				len(exceptions), len(RBACResourceTypes), exceptions)
		}
		for _, gvr := range RBACResourceTypes {
			if !exceptions[gvr] {
				t.Errorf("%s is in RBACResourceTypes but has no typed override — the two sets have "+
					"drifted apart and the unrepairable class is no longer either of them", gvr)
			}
		}

		// And the other half: a GVR OUTSIDE that set must be repairable, so
		// the arm fails if the exception predicate ever widens to swallow
		// ordinary widget GVRs.
		ordinary := schema.GroupVersionResource{
			Group: "widgets.krateo.io", Version: "v1beta1", Resource: "pageheaders",
		}
		if isStreamingException(ordinary) {
			t.Errorf("%s is a streaming exception — an ordinary widget GVR has joined the "+
				"factory-built, never-repaired class", ordinary)
		}
	})

	t.Run("queue_is_fifo_by_detection_and_counts_the_inflight_repair", func(t *testing.T) {
		// FIFO BY DETECTION TIME, not by GVR size: a 50K GVR at the head must
		// not starve everything behind it by being re-detected first each
		// round.
		c3Setup(t)
		resetVerificationForArm(t)
		gvrA := repairGVR
		gvrB := schema.GroupVersionResource{Group: "widgets.krateo.io", Version: "v1beta1", Resource: "cards"}
		rememberStoreVerification(gvrA, true, true)
		rememberStoreVerification(gvrB, true, true)

		if !enqueueStoreRepair(gvrA, "a") {
			t.Fatal("enqueue A refused")
		}
		time.Sleep(5 * time.Millisecond)
		if !enqueueStoreRepair(gvrB, "b") {
			t.Fatal("enqueue B refused")
		}
		if got := storeRepairQueueDepth(); got != 2 {
			t.Fatalf("store_repairs_pending = %d, want 2", got)
		}

		first, ok := dequeueStoreRepair()
		if !ok || first.gvr != gvrA {
			t.Fatalf("dequeued %v (ok=%v), want A first — the queue is not FIFO by detection time", first.gvr, ok)
		}
		// GLOBAL SERIALISATION, as the gauge sees it: A is in flight and B is
		// still queued, so the gauge must read 2 and NOT 1. A gauge that drops
		// as soon as work STARTS reads 0 for the whole rebuild window, which
		// is precisely when an operator is looking at it.
		if got := storeRepairQueueDepth(); got != 2 {
			t.Errorf("store_repairs_pending = %d with one in flight and one queued, want 2", got)
		}
		second, ok := dequeueStoreRepair()
		if !ok || second.gvr != gvrB {
			t.Fatalf("dequeued %v (ok=%v), want B second", second.gvr, ok)
		}
		if !second.detected.After(first.detected) && !second.detected.Equal(first.detected) {
			t.Errorf("B was detected before A (%v vs %v); the ordering key is not detection time",
				second.detected, first.detected)
		}
	})
}

// --- the cause-label capture ----------------------------------------------

// cacheEventLog collects cache_event.consumed records so an arm can assert
// WHICH CAUSE a dirty-mark was attributed to. The attribution is the point:
// the relist machinery is shared between the CRD schema-relist path and the
// store-repair path, and a repair that reused the schema label would put a
// wrong cause in the event log — the defect class #237 exists to remove.
type cacheEventLog struct {
	mu    sync.Mutex
	types []string
}

type cacheEventHandler struct {
	slog.Handler
	log *cacheEventLog
}

func (h *cacheEventHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Message == "cache_event.consumed" {
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "type" {
				h.log.mu.Lock()
				h.log.types = append(h.log.types, a.Value.String())
				h.log.mu.Unlock()
				return false
			}
			return true
		})
	}
	return nil
}

func (h *cacheEventHandler) Enabled(context.Context, slog.Level) bool { return true }

// captureCacheEvents installs the handler for the duration of the test and
// restores the previous default afterwards.
func captureCacheEvents(t *testing.T) *cacheEventLog {
	t.Helper()
	prev := slog.Default()
	log := &cacheEventLog{}
	slog.SetDefault(slog.New(&cacheEventHandler{Handler: prev.Handler(), log: log}))
	t.Cleanup(func() { slog.SetDefault(prev) })
	return log
}

func sawEventType(l *cacheEventLog, want string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, got := range l.types {
		if got == want {
			return true
		}
	}
	return false
}

func eventTypes(l *cacheEventLog) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.types...)
}
