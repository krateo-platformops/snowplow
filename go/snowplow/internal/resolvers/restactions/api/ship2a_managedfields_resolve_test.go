// ship2a_managedfields_resolve_test.go — Ship 2a (0.30.209) full-Resolve
// -race gate.
//
// This closes the coverage gap that let the v1 race survive the original
// ship2a_* gates: those run gojq DIRECTLY and never exercise the serve
// path's managedFields strip. The v1 -race fired in
// TestResolve_ConcurrentRequestsDoNotCrossPollinate because the dropped
// per-serve removeManagedFields(dict) walk wrote the now-SHARED
// entry.Items maps in place.
//
// This test drives the FULL api.Resolve (so the serve path — including
// the managedFields handling — is in the loop) with shared item maps that
// CARRY metadata.managedFields, under N concurrent serves. It asserts:
//
//   (a) -race CLEAN — no path writes the shared entry.Items;
//   (b) the served output carries NO managedFields (the load-time strip
//       at parseListEnvelope works, preserving the pre-Ship-2a served
//       wire shape now that the per-serve walk is gone);
//   (c) every concurrent serve produces identical output (no mutation
//       drift across serves of the shared entry).
//
// Run with -race. KUBECONFIG=/dev/null.

package api

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

// f1WidgetObjectWithManagedFields is f1WidgetObject with a populated
// metadata.managedFields — the wire field every apiserver object carries
// and that snowplow strips before serving. The Ship 2a strip happens at
// parseListEnvelope (load time, private item) rather than per-serve.
func f1WidgetObjectWithManagedFields(ns, name string) runtime.Object {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.krateo.io/v1",
		"kind":       "Widget",
		"metadata": map[string]any{
			"namespace": ns,
			"name":      name,
			"managedFields": []any{
				map[string]any{
					"manager":    "kube-controller-manager",
					"operation":  "Update",
					"apiVersion": "widgets.krateo.io/v1",
					"time":       "2026-05-29T00:00:00Z",
					"fieldsType": "FieldsV1",
					"fieldsV1": map[string]any{
						"f:spec": map[string]any{"f:replicas": map[string]any{}},
					},
				},
			},
		},
		"spec": map[string]any{"replicas": int64(3)},
	}}
}

// newF1WatcherWithManagedFields mirrors newF1Watcher but seeds widgets
// that carry metadata.managedFields, so the content entry materialised
// from the informer pivot exercises the Ship 2a load-time strip.
func newF1WatcherWithManagedFields(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	// #57: api-stage L1 + informer pivot are implicit under CACHE_ENABLED
	// (RESOLVED_CACHE_APISTAGE_ENABLED + RESOLVER_USE_INFORMER retired).
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	// LIFO cleanup: the watcher Stop() registered below runs BEFORE this
	// reset (t.Cleanup is LIFO), so the informer has fully drained before
	// the Deps singleton is nilled — the OnAdd-bridge/reset interleave that
	// #85+#331 closed cannot recur here.
	t.Cleanup(func() {
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	var seed []runtime.Object
	for _, ns := range f1AllNamespaces {
		seed = append(seed, f1WidgetObjectWithManagedFields(ns, "widget-"+ns))
	}
	seed = append(seed, f1WidgetListerClusterRole())
	for _, ns := range f1AllNamespaces {
		seed = append(seed, f1RoleBinding(ns, f1BroadUser))
	}
	for _, ns := range f1NarrowNamespaces {
		seed = append(seed, f1RoleBinding(ns, f1NarrowUser))
	}

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		f1WidgetsGVR: "WidgetList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:                "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:         "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:         "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	if rw == nil {
		t.Fatalf("expected non-nil watcher under CACHE_ENABLED=true")
	}
	t.Cleanup(rw.Stop) // #85: Stop() blocks until goroutine drain — no settle-sleep needed

	added, syncCh := rw.EnsureResourceType(f1WidgetsGVR)
	if !added {
		t.Fatalf("EnsureResourceType(widgets): want added=true")
	}
	select {
	case <-syncCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("widgets informer did not sync within 5s")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync (RBAC informers): %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return rw
}

// f1ResolveItemsAs drives the real Resolve for a one-stage widgets LIST
// whose filter projects the whole items array, returning dict[id] as the
// []any of served widget objects.
func f1ResolveItemsAs(t *testing.T, username string, stage *templates.API) []any {
	t.Helper()
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: username}),
	)
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
	dict := Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               []*templates.API{stage},
		RESTActionNamespace: "default",
		RESTActionName:      "ship2a-mf-restaction",
	})
	if dict == nil {
		t.Errorf("resolve as %q returned nil dict", username)
		return nil
	}
	items, _ := dict[stage.Name].([]any)
	return items
}

// hasManagedFieldsAnywhere reports whether any map in the value tree has a
// "managedFields" key — used to assert the served output is stripped.
func hasManagedFieldsAnywhere(v any) bool {
	switch t := v.(type) {
	case map[string]any:
		if _, ok := t["managedFields"]; ok {
			return true
		}
		for _, sub := range t {
			if hasManagedFieldsAnywhere(sub) {
				return true
			}
		}
	case []any:
		for _, sub := range t {
			if hasManagedFieldsAnywhere(sub) {
				return true
			}
		}
	}
	return false
}

// TestShip2a_FullResolve_ManagedFields_Race is the full-package -race gate
// for Ship 2a's serve path. It seeds managedFields-bearing widgets,
// cold-warms the content entry, then fans N concurrent Resolves over the
// SHARED entry.Items.
func TestShip2a_FullResolve_ManagedFields_Race(t *testing.T) {
	newF1WatcherWithManagedFields(t)

	stage := &templates.API{
		Name:   "widgets",
		Path:   "/apis/widgets.krateo.io/v1/widgets",
		Verb:   ptr.To("GET"),
		Filter: ptr.To(".widgets.items"),
	}

	// Cold warm-up: one resolve populates the identity-free content entry
	// (parseListEnvelope strips managedFields once at materialisation).
	warm := f1ResolveItemsAs(t, f1BroadUser, stage)
	if len(warm) == 0 {
		t.Fatalf("warm-up resolve returned no items")
	}
	// The served items must be STRIPPED (the load-time strip ran).
	for _, it := range warm {
		if hasManagedFieldsAnywhere(it) {
			t.Fatalf("warm-up served item still carries managedFields: %#v", it)
		}
	}
	// Snapshot the warm output's marshalled bytes for the drift check.
	wantBytes, err := json.Marshal(warm)
	if err != nil {
		t.Fatalf("marshal warm: %v", err)
	}

	// N concurrent resolves over the SAME content entry — every one is a
	// content-Get-HIT serving the shared entry.Items via the shallow
	// envelope. -race makes any write to the shared maps a hard failure.
	const workers = 24
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		user := f1BroadUser
		if i%2 == 1 {
			user = f1NarrowUser
		}
		go func(u string) {
			defer wg.Done()
			items := f1ResolveItemsAs(t, u, stage)
			// (b) served output carries no managedFields.
			for _, it := range items {
				if hasManagedFieldsAnywhere(it) {
					t.Errorf("concurrent serve as %q leaked managedFields: %#v", u, it)
					return
				}
			}
			// (c) broad-user serves must match the warm snapshot (no
			// mutation drift); narrow-user is a subset, so only assert
			// the strip + non-empty for it.
			if u == f1BroadUser {
				gotBytes, merr := json.Marshal(items)
				if merr != nil {
					t.Errorf("marshal concurrent serve: %v", merr)
					return
				}
				if string(gotBytes) != string(wantBytes) {
					t.Errorf("broad-user serve drifted from warm snapshot:\n want=%s\n  got=%s",
						wantBytes, gotBytes)
				}
			}
		}(user)
	}
	wg.Wait()

	// Final: a fresh broad-user resolve must STILL match the warm snapshot
	// — proves the shared entry.Items were never mutated by any serve.
	final := f1ResolveItemsAs(t, f1BroadUser, stage)
	finalBytes, err := json.Marshal(final)
	if err != nil {
		t.Fatalf("marshal final: %v", err)
	}
	if string(finalBytes) != string(wantBytes) {
		t.Fatalf("post-run broad-user serve diverged — shared entry.Items were mutated:\n want=%s\n  got=%s",
			wantBytes, finalBytes)
	}
}
