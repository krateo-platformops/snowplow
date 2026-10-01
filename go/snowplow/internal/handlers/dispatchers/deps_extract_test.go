// deps_extract_test.go — Tag 0.30.8 tests for the widget dep-edge
// extractor. Both positive (render ref recorded) and negative (action
// ref filtered) cases per Revision 14 BINDING.
//
// Tag 0.30.9 Sub-scope B extension: assert that every recorded
// dep-edge GVR triggers EnsureResourceType on the watcher singleton
// — the wire-up that makes the 0.30.8 dep tracker actually fire.

package dispatchers

import (
	"context"
	"log/slog"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	corev1 "k8s.io/api/core/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func widgetForTest() *unstructured.Unstructured {
	w := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Panel",
		"metadata": map[string]any{
			"name":      "panel-1",
			"namespace": "demo-ns",
		},
		"spec": map[string]any{
			"apiRef": map[string]any{
				"name":      "compositions-list",
				"namespace": "krateo-system",
			},
			"resourcesRefs": map[string]any{
				"items": []any{
					// Render ref — should be recorded.
					map[string]any{
						"id":         "render-1",
						"apiVersion": "composition.krateo.io/v1",
						"resource":   "compositions",
						"namespace":  "bench-ns-01",
						"name":       "bench-app-01-01",
					},
					// Action-only ref — should be filtered out.
					map[string]any{
						"id":         "action-1",
						"apiVersion": "v1",
						"resource":   "pods",
						"namespace":  "bench-ns-01",
						"name":       "viewlogs-pod",
					},
				},
			},
		},
		"status": map[string]any{
			"widgetData": map[string]any{
				"actions": map[string]any{
					"navigate": []any{
						map[string]any{"resourceRefId": "action-1"},
					},
				},
			},
		},
	}}
	return w
}

func TestRecordWidgetDeps_RenderRefsRecorded_ActionRefsFiltered(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetDepsForTest()

	w := widgetForTest()
	l1Key := "L1_panel_demo"
	widgetGVR := schema.GroupVersionResource{
		Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels",
	}

	recordWidgetDeps(context.Background(), slog.Default(), l1Key, widgetGVR, w)

	deps := cache.Deps()
	// Expected edges:
	//  1. self-dep:    (widgetGVR, "demo-ns", "panel-1") -> L1
	//  2. apiRef:      (restactions, "krateo-system", "compositions-list") -> L1
	//  3. render ref:  (compositions, "bench-ns-01", "bench-app-01-01") -> L1
	// Action ref ("v1/pods/bench-ns-01/viewlogs-pod") must NOT appear.
	wantPos := []struct {
		gvr  schema.GroupVersionResource
		ns   string
		name string
	}{
		{widgetGVR, "demo-ns", "panel-1"},
		{schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}, "krateo-system", "compositions-list"},
		{schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "compositions"}, "bench-ns-01", "bench-app-01-01"},
	}
	for _, c := range wantPos {
		matched := deps.CollectMatchesForTest(c.gvr, c.ns, c.name)
		if _, ok := matched[l1Key]; !ok {
			t.Errorf("expected positive dep %v/%s/%s but L1 key not found; got matched=%v",
				c.gvr, c.ns, c.name, matched)
		}
	}

	// Action ref must NOT be recorded.
	actionGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	negMatched := deps.CollectMatchesForTest(actionGVR, "bench-ns-01", "viewlogs-pod")
	if _, ok := negMatched[l1Key]; ok {
		t.Fatalf("action-only ref WAS recorded as a render dep — Revision 14 filter broken")
	}
}

func TestParseGVR(t *testing.T) {
	cases := []struct {
		apiVer, resource string
		wantGroup, wantVer string
		wantOK bool
	}{
		{"v1", "pods", "", "v1", true},
		{"apps/v1", "deployments", "apps", "v1", true},
		{"templates.krateo.io/v1", "restactions", "templates.krateo.io", "v1", true},
		{"", "pods", "", "", false},
		{"v1", "", "", "", false},
	}
	for _, c := range cases {
		gvr, ok := parseGVR(c.apiVer, c.resource)
		if ok != c.wantOK {
			t.Errorf("parseGVR(%q,%q) ok=%v want %v", c.apiVer, c.resource, ok, c.wantOK)
			continue
		}
		if !ok {
			continue
		}
		if gvr.Group != c.wantGroup || gvr.Version != c.wantVer {
			t.Errorf("parseGVR(%q,%q) = %v, want group=%q version=%q",
				c.apiVer, c.resource, gvr, c.wantGroup, c.wantVer)
		}
	}
}

func TestExtractActionRefIDs_BothShapes(t *testing.T) {
	// Slice shape.
	w1 := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"widgetData": map[string]any{"actions": map[string]any{
			"navigate": []any{
				map[string]any{"resourceRefId": "a1"},
				map[string]any{"resourceRefId": "a2"},
			},
		}}},
	}}
	got := extractActionRefIDs(w1)
	if !got["a1"] || !got["a2"] {
		t.Errorf("slice-shape action ids missing: %v", got)
	}

	// Map shape.
	w2 := &unstructured.Unstructured{Object: map[string]any{
		"status": map[string]any{"widgetData": map[string]any{"actions": map[string]any{
			"openDrawer": map[string]any{"resourceRefId": "d1"},
		}}},
	}}
	got2 := extractActionRefIDs(w2)
	if !got2["d1"] {
		t.Errorf("map-shape action id missing: %v", got2)
	}

	// Empty status.
	w3 := &unstructured.Unstructured{Object: map[string]any{}}
	if got3 := extractActionRefIDs(w3); len(got3) != 0 {
		t.Errorf("empty status should yield empty set, got %v", got3)
	}
}

// ----------------------------------------------------------------------
// Tag 0.30.9 Sub-scope B — lazy-register-on-resolver-touch wiring
// ----------------------------------------------------------------------

// dispatcherListKindsForLazyRegister extends the RBAC list-kinds map
// with the GVRs the recordWidgetDeps test below records dep edges
// for (compositions, panels, restactions, namespaces) so the fake
// dynamic client's lazy-registered informers don't panic on initial
// LIST.
func dispatcherListKindsForLazyRegister() map[schema.GroupVersionResource]string {
	return map[schema.GroupVersionResource]string{
		// RBAC (eager-registered by NewResourceWatcher).
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:                "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:         "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:         "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		// Lazy-registered via recordWidgetDeps's ensureWatcherInformerForGVR
		// calls — the falsifier targets for Sub-scope B.
		{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}: "PanelList",
		{Group: "composition.krateo.io", Version: "v1", Resource: "compositions"}:     "CompositionList",
		{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}:         "RESTActionList",
		// pods/namespaces for the action-ref skip-set negative case.
		{Group: "", Version: "v1", Resource: "pods"}:       "PodList",
		{Group: "", Version: "v1", Resource: "namespaces"}: "NamespaceList",
	}
}

// newSchemeForLazyRegister returns a scheme registered with the GVRs
// the fake dynamic client serves in TestRecordWidgetDeps_TriggersEnsureResourceType.
func newSchemeForLazyRegister() *k8sruntime.Scheme {
	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	_ = corev1.AddToScheme(sch)
	_ = appsv1.AddToScheme(sch)
	return sch
}

// TestRecordWidgetDeps_TriggersEnsureResourceType is the Sub-scope B
// dispatcher-level falsifier: every dep edge recordWidgetDeps records
// MUST first trigger cache.Global().EnsureResourceType for its GVR.
// Without this wire-up, the 0.30.8 dep tracker DELETE-evict mechanism
// is dormant (no informer registered → no UpdateFunc/DeleteFunc fires
// → record_total>0 but evict_delete_total=0, the 0.30.8 production
// bug per Revision 17 finding 1).
//
// We verify the wiring via ListObjects: a registered GVR returns
// a non-nil slice (possibly empty); an unregistered GVR returns nil.
// This is the same probe TestNewResourceWatcher_RBACTypesEagerlyRegistered
// uses to assert eager registration.
func TestRecordWidgetDeps_TriggersEnsureResourceType(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetDepsForTest()

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		newSchemeForLazyRegister(), dispatcherListKindsForLazyRegister())

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher")
	}
	defer rw.Stop()

	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	w := widgetForTest()
	l1Key := "L1_panel_demo_0309"
	widgetGVR := schema.GroupVersionResource{
		Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels",
	}

	recordWidgetDeps(context.Background(), slog.Default(), l1Key, widgetGVR, w)

	// Every GVR the dep tracker recorded an edge for MUST be
	// registered on the watcher (i.e., ListObjects returns non-nil).
	mustBeRegistered := []schema.GroupVersionResource{
		widgetGVR, // self-dep
		{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}, // apiRef edge
		{Group: "composition.krateo.io", Version: "v1", Resource: "compositions"}, // resourcesRefs render edge
	}
	for _, gvr := range mustBeRegistered {
		got := rw.ListObjects(gvr, "")
		if got == nil {
			t.Fatalf("Sub-scope B WIRE-UP BROKEN: GVR %s was NOT registered on watcher; recordWidgetDeps did not call EnsureResourceType for it",
				gvr.String())
		}
	}

	// Action-only ref (v1/pods/viewlogs-pod) was filtered by Revision
	// 14 — pods GVR MUST NOT be registered.
	podsGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	if got := rw.ListObjects(podsGVR, ""); got != nil {
		t.Fatalf("Revision 14 filter regression: pods GVR was registered, but the action-only ref should have been filtered before EnsureResourceType")
	}
}

// TestRecordWidgetDeps_CacheOffSkipsEnsure covers the production
// cache=off path (cache.Global() == nil): ensureWatcherInformerForGVR
// must return silently without panicking. Functionally redundant
// with the nil-watcher unit test in watcher_test.go but exercises
// the dispatcher-side adapter path.
func TestRecordWidgetDeps_CacheOffSkipsEnsure(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("RESOLVED_CACHE_ENABLED", "false")
	cache.ResetDepsForTest()
	cache.SetGlobal(nil)

	w := widgetForTest()
	widgetGVR := schema.GroupVersionResource{
		Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels",
	}

	// Must not panic; must not record any dep edges (dep tracker is
	// a no-op in cache=off mode at the moment of test since we are
	// not exercising L1 lookup, but recordWidgetDeps still emits
	// deps.Record calls — the tracker handles them but no informer
	// exists to fire events).
	recordWidgetDeps(context.Background(), slog.Default(), "L1_panel_off", widgetGVR, w)
}
