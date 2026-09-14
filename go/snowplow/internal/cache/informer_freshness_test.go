// informer_freshness_test.go — 1.12.5 / #187 acceptance for the per-GVR
// informer freshness fields.
//
// WHY THEY EXIST. The four servability conjuncts say whether an informer is
// ALLOWED to serve. None of them says whether what it holds is CURRENT, and
// HasSynced in particular latches true the moment the initial LIST completes
// and never goes back. During #187 the central question — "does this indexer
// still hold the object the apiserver says is deleted?" — had no field
// anywhere, on any surface, so the whole inference chain had to be built from
// the client's subsequent child fetches.
//
// The arms are therefore not "the struct has three more fields". They are:
//
//	1. IndexerCount reports what the informer actually holds, so it can be
//	   compared against the cluster;
//	2. LastEventAgeSeconds MOVES when the bridge delivers an event and reads
//	   -1 (never), not 0 (just now), before the first one — a freshness field
//	   that reports "fresh" for a GVR nothing has ever happened to would be
//	   worse than absent;
//	3. the aggregate the OTLP mirror publishes agrees with the per-GVR rows.

package cache

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// newUnstructuredForGVR builds a minimal object the fake dynamic client can
// serve for gvr. Kind is derived from the registered list kind so the fake's
// tracker files it under the right resource.
func newUnstructuredForGVR(gvr schema.GroupVersionResource, ns, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": gvr.GroupVersion().String(),
		"kind":       "Button",
		"metadata": map[string]any{
			"namespace": ns,
			"name":      name,
		},
	}}
}

func freshnessGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: b5Group, Version: b5Version, Resource: b5Plural}
}

func freshnessRow(t *testing.T, rw *ResourceWatcher, gvr schema.GroupVersionResource) ServableGVRStatus {
	t.Helper()
	for _, row := range rw.ServableSnapshot() {
		if row.GVR == gvr.String() {
			return row
		}
	}
	t.Fatalf("no /debug/servable row for %s", gvr)
	return ServableGVRStatus{}
}

func TestInformerFreshness_FieldsTrackTheIndexerAndTheEventClock(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	// Stops the DELETE-eviction worker deterministically before any cleanup
	// touches the Deps() singleton — this test drives a real DeleteFunc, which
	// spawns that worker.
	defer withCleanDepWatch(t)()

	gvr := freshnessGVR()
	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	_ = corev1.AddToScheme(sch)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch,
		map[schema.GroupVersionResource]string{
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
			gvr: "ButtonList",
		})

	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)

	_, syncCh := rw.EnsureResourceType(gvr)
	select {
	case <-syncCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("informer did not sync")
	}

	// (1) Before any event: the age must read NEVER, not "0 seconds ago".
	row := freshnessRow(t, rw, gvr)
	if row.LastEventAgeSeconds != -1 {
		t.Fatalf("LastEventAgeSeconds = %v before any event, want -1 (never). A freshness "+
			"field that reports \"just now\" for a GVR nothing has happened to is worse than "+
			"no field at all — it reads as healthy.", row.LastEventAgeSeconds)
	}
	if row.IndexerCount != 0 {
		t.Fatalf("IndexerCount = %d for an empty fake cluster, want 0", row.IndexerCount)
	}

	// (2) Drive a REAL bridge event through the production handler set.
	h := rw.depEventHandlers(gvr)
	h.UpdateFunc(unstructuredObj(gvr, "ns", "b1"), unstructuredObj(gvr, "ns", "b1"))

	row = freshnessRow(t, rw, gvr)
	if row.LastEventAgeSeconds < 0 {
		t.Fatalf("LastEventAgeSeconds = %v after a delivered UPDATE, want >= 0 — the freshness "+
			"clock is not wired to the informer bridge", row.LastEventAgeSeconds)
	}
	if row.LastEventAgeSeconds > 60 {
		t.Fatalf("LastEventAgeSeconds = %v immediately after an event", row.LastEventAgeSeconds)
	}

	// (3) A DELETE must stamp it too — DELETE is the event class #187 is about.
	before := rw.lastEventUnixNano(gvr)
	time.Sleep(2 * time.Millisecond)
	h.DeleteFunc(unstructuredObj(gvr, "ns", "b1"))
	if after := rw.lastEventUnixNano(gvr); after <= before {
		t.Fatalf("the freshness clock did not advance on a DELETE (%d -> %d). DELETE is the "+
			"event class #187 is about; a clock blind to it would have said \"fresh\" through "+
			"the whole incident.", before, after)
	}

	// (4) The aggregate the OTLP mirror publishes must agree with the rows.
	fr := rw.InformerFreshnessSnapshot()
	var rowObjects int64
	var never int64
	for _, r := range rw.ServableSnapshot() {
		rowObjects += int64(r.IndexerCount)
		if r.LastEventAgeSeconds < 0 {
			never++
		}
	}
	if fr.IndexerObjects != rowObjects {
		t.Fatalf("aggregate IndexerObjects = %d, per-GVR rows sum to %d — the two surfaces "+
			"disagree", fr.IndexerObjects, rowObjects)
	}
	if fr.GVRsNeverEvent != never {
		t.Fatalf("aggregate GVRsNeverEvent = %d, rows say %d", fr.GVRsNeverEvent, never)
	}
}

// TestInformerFreshness_IndexerCountReportsRealObjects pins the field that
// would have answered #187 in one read: how many objects does this informer
// actually hold?
func TestInformerFreshness_IndexerCountReportsRealObjects(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	// Stops the DELETE-eviction worker deterministically before any cleanup
	// touches the Deps() singleton — this test drives a real DeleteFunc, which
	// spawns that worker.
	defer withCleanDepWatch(t)()

	gvr := freshnessGVR()
	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	_ = corev1.AddToScheme(sch)

	seed := []k8sruntime.Object{
		newUnstructuredForGVR(gvr, "demo-system", "button-a"),
		newUnstructuredForGVR(gvr, "demo-system", "button-b"),
		newUnstructuredForGVR(gvr, "demo-system", "button-c"),
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch,
		map[schema.GroupVersionResource]string{
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
			gvr: "ButtonList",
		}, seed...)

	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)

	_, syncCh := rw.EnsureResourceType(gvr)
	select {
	case <-syncCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("informer did not sync")
	}

	row := freshnessRow(t, rw, gvr)
	if row.IndexerCount != len(seed) {
		t.Fatalf("IndexerCount = %d, want %d. This is the field that makes indexer staleness a "+
			"single read — compare it against the cluster and a divergence IS the staleness.",
			row.IndexerCount, len(seed))
	}
	if !row.HasSynced {
		t.Fatalf("precondition: informer should be synced")
	}
	if row.LastSyncResourceVersion != "" && row.IndexerCount == 0 {
		t.Fatalf("inconsistent row: %+v", row)
	}
}
