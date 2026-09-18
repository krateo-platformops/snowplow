// skip_unwatchable_events_a2_test.go — audit A2 falsifier item 4: the
// END-TO-END arm at the real boundary.
//
// Arms 1-11 (skip_unwatchable_resource_a2_test.go) assert the REGISTRATION
// decision. This file asserts the CONSEQUENCE that the whole fix is for: an
// unwatchable GVR must submit ZERO dep events, because that is the head of the
// chain the audit traced —
//
//	no informer -> no depEventHandlers wired -> no submitDepEvent -> no probe
//	-> no objUnknown -> no degrade -> no OnObjectEvent(objUnknownDegraded)
//	-> no dirty-mark -> no refresher enqueue
//
// which is what turns +236,300 dirty-marks and +236,329 refresher enqueues per
// 14 minutes into zero.
//
// ASSERTED ON THE COUNTER, NOT ON LOG ABSENCE. An absent log line is also what
// an unreached code path produces, so a log-based arm cannot tell "the event
// was never submitted" from "the test never got that far". events_submitted_total
// (DepWatchStatsSnapshot().EventsSubmitted) is the coordinate-enqueue counter
// at deps_expvar.go:153.
//
// THE ANTI-TRIVIAL HALF IS LOAD-BEARING. A zero delta is ALSO what a broken
// counter, a watcher that never started, or a fake client whose watch never
// registered would produce. So the arm first drives a WATCHABLE GVR through the
// identical path and requires the counter to MOVE. Only then is a zero delta on
// the unwatchable GVR evidence of anything.

package cache

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// a2EventsWatchable is a normal, fully-watchable resource — the control.
var a2EventsWatchable = schema.GroupVersionResource{
	Group: "served.krateo.io", Version: "v1", Resource: "watchables",
}

// a2EventsUnwatchable carries the REAL metrics.k8s.io wire shape: get+list,
// no watch.
var a2EventsUnwatchable = schema.GroupVersionResource{
	Group: "metrics.k8s.io", Version: "v1beta1", Resource: "pods",
}

// a2EventsDisco reports every group as served (so the #119 group gate is inert)
// and carries per-resource verbs — the axis under test.
type a2EventsDisco struct{}

func (d *a2EventsDisco) ServerGroups() (*metav1.APIGroupList, error) {
	return &metav1.APIGroupList{Groups: []metav1.APIGroup{
		{Name: "served.krateo.io"},
		{Name: "metrics.k8s.io"},
	}}, nil
}

func (d *a2EventsDisco) ServerResourcesForGroupVersion(gv string) (*metav1.APIResourceList, error) {
	list := &metav1.APIResourceList{GroupVersion: gv}
	switch gv {
	case "served.krateo.io/v1":
		list.APIResources = []metav1.APIResource{{
			Name: "watchables", Namespaced: true, Kind: "Watchable",
			Verbs: metav1.Verbs{"get", "list", "watch"},
		}}
	case "metrics.k8s.io/v1beta1":
		// The live shape, verbatim from `kubectl get --raw`.
		list.APIResources = []metav1.APIResource{{
			Name: "pods", Namespaced: true, Kind: "PodMetrics",
			Verbs: metav1.Verbs{"get", "list"},
		}}
	}
	return list, nil
}

// TestA2_Arm12_UnregisteredGVRSubmitsZeroDepEvents is the audit's falsifier
// item 4.
func TestA2_Arm12_UnregisteredGVRSubmitsZeroDepEvents(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	resetDepWatchForTest()
	t.Cleanup(resetDepWatchForTest)
	ResetServedGroupsMemoForTest()
	ResetResourceVerbsMemoForTest()
	t.Cleanup(ResetServedGroupsMemoForTest)
	t.Cleanup(ResetResourceVerbsMemoForTest)

	// The watcher registers its fixed boot RBACResourceTypes set at
	// construction, so the fake client needs their list kinds too or the
	// reflector panics on the initial LIST (same set realWatcher installs).
	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	_ = corev1.AddToScheme(sch)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch,
		map[schema.GroupVersionResource]string{
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
			a2EventsWatchable:   "WatchableList",
			a2EventsUnwatchable: "PodMetricsList",
		})
	gate := newWatchGate()
	dyn.PrependWatchReactor("*", gate.reactor(dyn))

	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(func() {
		rw.Stop()
		time.Sleep(50 * time.Millisecond)
	})
	rw.SetDiscoveryClient(&a2EventsDisco{})

	// --- the watchable control: registers, syncs, and its events flow. ---
	added, syncCh := rw.EnsureResourceType(a2EventsWatchable)
	if !added {
		t.Fatalf("ARM12 setup FAIL: the watchable GVR must register")
	}
	select {
	case <-syncCh:
	case <-time.After(harnessWaitBound):
		t.Fatalf("ARM12 setup FAIL: watchable informer did not sync within %s", harnessWaitBound)
	}
	gate.wait(t, a2EventsWatchable, harnessWaitBound)

	// --- the unwatchable GVR: skipped, so no handler is ever wired. ---
	if unwAdded, _ := rw.EnsureResourceType(a2EventsUnwatchable); unwAdded {
		t.Fatalf("ARM12 setup FAIL: the [get list] GVR must NOT register")
	}
	if rw.IsRegistered(a2EventsUnwatchable) {
		t.Fatalf("ARM12 setup FAIL: the unwatchable GVR must not be registered")
	}

	// === CONTROL: the instrument works. ===
	//
	// Without this half, the zero-delta assertion below is satisfied by a
	// counter that never moves at all.
	before := DepWatchStatsSnapshot().EventsSubmitted
	createObj(t, rw, dyn, a2EventsWatchable, "ns1", "control-1", "x")
	waitQueueIdle(t, harnessWaitBound)
	afterControl := DepWatchStatsSnapshot().EventsSubmitted
	if afterControl <= before {
		t.Fatalf("ARM12 CONTROL FAIL: events_submitted_total did not move for a WATCHABLE GVR "+
			"(%d -> %d). The instrument is not measuring anything, so a zero delta below would "+
			"prove nothing", before, afterControl)
	}

	// === THE ARM: an ADD on the unregistered GVR submits nothing. ===
	//
	// Create straight on the fake client — there is no indexer to wait on,
	// which is precisely the point.
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": a2EventsUnwatchable.GroupVersion().String(),
		"kind":       "PodMetrics",
		"metadata":   map[string]any{"namespace": "ns1", "name": "metrics-1"},
	}}
	if _, cerr := dyn.Resource(a2EventsUnwatchable).Namespace("ns1").
		Create(context.Background(), u, metav1.CreateOptions{}); cerr != nil {
		t.Fatalf("ARM12: create on the unwatchable GVR: %v", cerr)
	}

	// Give any (wrongly-wired) handler ample opportunity to fire, then drain.
	time.Sleep(250 * time.Millisecond)
	waitQueueIdle(t, harnessWaitBound)

	afterUnwatchable := DepWatchStatsSnapshot().EventsSubmitted
	if afterUnwatchable != afterControl {
		t.Fatalf("ARM12 FAIL: an ADD on the UNREGISTERED %s submitted %d dep event(s) "+
			"(%d -> %d). An unwatchable GVR must submit ZERO — that is the head of the chain "+
			"that produced +236,300 dirty-marks per 14 min on 057",
			a2EventsUnwatchable, afterUnwatchable-afterControl, afterControl, afterUnwatchable)
	}

	// And the control still holds at the end: a second watchable ADD still
	// moves the counter, so the zero above is not "the bridge died halfway".
	createObj(t, rw, dyn, a2EventsWatchable, "ns1", "control-2", "y")
	waitQueueIdle(t, harnessWaitBound)
	if final := DepWatchStatsSnapshot().EventsSubmitted; final <= afterUnwatchable {
		t.Fatalf("ARM12 TRAILING-CONTROL FAIL: the bridge stopped submitting for the watchable "+
			"GVR too (%d -> %d) — the zero delta above was the bridge dying, not the gate working",
			afterUnwatchable, final)
	}
}
