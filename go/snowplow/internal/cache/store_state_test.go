// store_state_test.go — #237 deliverable A, the cache-layer properties.
//
// The HTTP-level arms live in internal/handlers/debug_store_falsifier_test.go
// (the blindness arm + the discriminator). These are the two properties that
// belong to the accessor itself and cannot be asserted through the route:
// that it reads a NON-SERVABLE GVR where probeObjectState deliberately goes
// blind, and that its type cannot carry a body.

package cache

import (
	"context"
	"reflect"
	"testing"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var storeStateGVR = schema.GroupVersionResource{
	Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "pageheaders",
}

// storeStateWatcher builds a cache-on watcher whose informer has synced one
// pageheader at rv from a fake apiserver.
func storeStateWatcher(t *testing.T, rv string) *ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")

	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch,
		map[schema.GroupVersionResource]string{
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
			{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
			storeStateGVR: "PageHeaderList",
		},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": storeStateGVR.Group + "/" + storeStateGVR.Version,
			"kind":       "PageHeader",
			"metadata": map[string]any{
				"name": "portal-builder-page-header", "namespace": "krateo-system",
				"uid": "11111111-2222-3333-4444-555555555555", "resourceVersion": rv,
				"generation": int64(1), "creationTimestamp": "2026-09-22T10:00:00Z",
			},
			"spec": map[string]any{"widgetData": map[string]any{"title": "Portal builder"}},
		}})

	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	_, syncCh := rw.EnsureResourceType(storeStateGVR)
	select {
	case <-syncCh:
	case <-time.After(10 * time.Second):
		t.Fatalf("informer for %s never synced", storeStateGVR)
	}
	return rw
}

// TestStoreObjectState_ReadsANonServableGVR_WhereProbeGoesBlind is the
// non-negotiable that separates this accessor from the audit's oracle.
//
// probeObjectState returns objUnknown for a non-servable GVR BY DESIGN — right
// for an eviction decision, useless for a diagnostic, because "this GVR is
// retracted / its watch is broken" is one of the answers #237 needed (that
// capture read confirm_retracted_total: 10158, discovery_refresh: 10011). A
// diagnostic that goes blind in the same state as the mechanism it is
// diagnosing cannot diagnose it.
//
// The arm drives BOTH readers over the SAME watcher in the SAME state, so it
// asserts a difference rather than a property in isolation.
func TestStoreObjectState_ReadsANonServableGVR_WhereProbeGoesBlind(t *testing.T) {
	rw := storeStateWatcher(t, "1")
	const ns, name = "krateo-system", "portal-builder-page-header"

	// Servable first: both readers agree the object is there.
	if st := rw.probeObjectState(storeStateGVR, ns, name); st != objExists {
		t.Fatalf("precondition: probeObjectState = %v, want objExists while the GVR is servable", st)
	}
	if got := rw.StoreObjectState(storeStateGVR, ns, name); !got.Found || got.ResourceVersion != "1" {
		t.Fatalf("precondition: StoreObjectState = %+v, want found with resourceVersion 1", got)
	}

	// Now retract servability the way production does: a broken WATCH
	// (conjunct 3) plus an un-confirmed resource type (conjunct 4).
	rw.mu.Lock()
	rw.watchBroken[storeStateGVR] = struct{}{}
	delete(rw.confirmed, storeStateGVR)
	rw.mu.Unlock()

	if st := rw.probeObjectState(storeStateGVR, ns, name); st != objUnknown {
		t.Fatalf("fixture: probeObjectState = %v on a non-servable GVR, want objUnknown — the "+
			"arm compares this reader's blindness against StoreObjectState's read, and the "+
			"comparison is vacuous if the probe still answers", st)
	}

	got := rw.StoreObjectState(storeStateGVR, ns, name)
	if !got.Found || got.ResourceVersion != "1" {
		t.Fatalf("StoreObjectState went blind on a non-servable GVR: %+v. It must NOT gate on "+
			"servableLocked — a retracted or watch-broken GVR is exactly when an operator "+
			"needs to read the store", got)
	}
	if got.Servable || !got.WatchBroken {
		t.Fatalf("the conjuncts must be REPORTED as fields: got servable=%v watchBroken=%v, "+
			"want false/true", got.Servable, got.WatchBroken)
	}
	if got.UID != "11111111-2222-3333-4444-555555555555" || got.Generation != 1 {
		t.Fatalf("object metadata not reported: %+v", got)
	}
	if got.Representation != "bytesObject" {
		t.Fatalf("representation = %q, want bytesObject (the H1 storage shape every "+
			"non-typed-RBAC GVR takes in production)", got.Representation)
	}
	if len(got.BodySHA256) != 64 {
		t.Fatalf("bodySHA256 = %q, want 64 hex chars", got.BodySHA256)
	}
	if got.IndexerCount != 1 {
		t.Fatalf("indexerCount = %d, want 1", got.IndexerCount)
	}
}

// TestStoreObjectState_UnregisteredAndAbsentAreDistinct pins the three
// answers apart. "snowplow has no informer for this GVR" and "the informer has
// no such object" are different diagnoses — #237's phantom-DELETE class is the
// second, and collapsing them would make the route answer the wrong question.
func TestStoreObjectState_UnregisteredAndAbsentAreDistinct(t *testing.T) {
	rw := storeStateWatcher(t, "1")

	unreg := rw.StoreObjectState(
		schema.GroupVersionResource{Group: "nope.example.io", Version: "v1", Resource: "widgets"},
		"krateo-system", "portal-builder-page-header")
	if unreg.Registered || unreg.Found {
		t.Fatalf("unregistered GVR: want registered=false found=false; got %+v", unreg)
	}

	absent := rw.StoreObjectState(storeStateGVR, "krateo-system", "no-such-object")
	if !absent.Registered || absent.Found {
		t.Fatalf("absent object in a registered GVR: want registered=true found=false; got %+v", absent)
	}
	if absent.ResourceVersion != "" || absent.BodySHA256 != "" {
		t.Fatalf("absent object must carry no object metadata; got %+v", absent)
	}
	// The GVR-level context still has to be there — it is what says whether
	// the GVR is receiving events at all, which is the difference between
	// "deleted" and "this informer is dead".
	if absent.IndexerCount != 1 {
		t.Fatalf("indexerCount = %d for an absent object in a GVR holding one object, want 1",
			absent.IndexerCount)
	}
	if absent.GVRLastEventAgeSeconds != -1 {
		t.Fatalf("gvrLastEventAgeSeconds = %v with no event ever delivered, want -1 (never). "+
			"A freshness field that reports \"just now\" for a GVR nothing has happened to "+
			"reads as healthy", absent.GVRLastEventAgeSeconds)
	}
}

// TestStoreObjectState_StructurallyCannotLeakContent is the leak guard, and it
// is the TYPE that enforces it rather than the handler. Resolved output and
// object bodies are per-identity RBAC-sensitive; the /debug/* gate admits any
// valid Krateo JWT, so a body field on this struct would be a cross-identity
// read waiting for a future edit to expose it.
//
// Same guard shape as TestRangeMetadata_StructurallyCannotLeakContent for
// /debug/apistage.
func TestStoreObjectState_StructurallyCannotLeakContent(t *testing.T) {
	typ := reflect.TypeOf(StoreObjectState{})
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		switch f.Type.Kind() {
		case reflect.String, reflect.Bool,
			reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			// Scalars cannot carry an object tree.
		default:
			t.Fatalf("StoreObjectState.%s is a %s. The structural leak guard is that this type "+
				"has no []byte, map, slice or interface field — it must be INCAPABLE of "+
				"carrying an object body, not merely not carrying one today", f.Name, f.Type)
		}
	}
}

// TestDescribeStoredObject_CoversEveryStoredShape drives the shape dispatch
// over the three representations the store can hold. bytesObject is covered
// end-to-end by the arms above (it is what a real informer produces); these
// are the two the metadata/typed paths produce, asserted here because a wrong
// branch would silently report an empty resourceVersion — which reads exactly
// like "the object has no resourceVersion" rather than like a bug.
func TestDescribeStoredObject_CoversEveryStoredShape(t *testing.T) {
	cases := []struct {
		name   string
		obj    any
		repr   string
		wantRV string
	}{
		{
			name: "unstructured",
			obj: &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "widgets.templates.krateo.io/v1beta1", "kind": "PageHeader",
				"metadata": map[string]any{"name": "w", "namespace": "n", "resourceVersion": "7", "uid": "u-7"},
			}},
			repr: "unstructured", wantRV: "7",
		},
		{
			name: "partialObjectMetadata",
			obj: &metav1.PartialObjectMetadata{
				ObjectMeta: metav1.ObjectMeta{Name: "w", Namespace: "n", ResourceVersion: "8", UID: types.UID("u-8")},
			},
			repr: "partialObjectMetadata", wantRV: "8",
		},
		{
			name: "typed rbac object reports its concrete type",
			obj: &rbacv1.Role{
				ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "n", ResourceVersion: "9", UID: types.UID("u-9")},
			},
			repr: "*v1.Role", wantRV: "9",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out StoreObjectState
			describeStoredObject(&out, tc.obj)
			if out.Representation != tc.repr {
				t.Fatalf("representation = %q, want %q", out.Representation, tc.repr)
			}
			if out.ResourceVersion != tc.wantRV {
				t.Fatalf("resourceVersion = %q, want %q — a shape whose metadata is not read "+
					"reports an empty rv, which reads like a property of the object rather "+
					"than like a missing branch", out.ResourceVersion, tc.wantRV)
			}
			if len(out.BodySHA256) != 64 {
				t.Fatalf("bodySHA256 = %q, want 64 hex chars", out.BodySHA256)
			}
		})
	}
}
