// refresh_subscription_key_parity_test.go — #64: the subscription key MUST
// fold the SAME inline-extras union the emit key folds, so an armed /refreshes
// connection matches the key the resolver Puts + PublishRefreshes.
//
// Root cause (1.5.5–1.5.10): the emit key folds
// unionForKey(spec.apiRef.extras, spec.resourcesRefsTemplateExtras, request)
// (widgets.go genuine-Put), but DeriveSubscriptionKey folded ONLY the request
// extras → a composition-DETAIL widget with INLINE author extras derived a
// different subscription key than its cell publishes → delivered:0. The fix
// reconstructs the union server-side from the widget CR (subscriptionKeyExtras).
//
// Hermetic: a fake dynamic client seeds BOTH the RBAC snapshot (for the
// BindingUID fold) AND the widget CR (so objects.Get serves it from the
// informer — the C64-6 informer-served read). NO remote go-test, NO apiserver.
//
//	ARM 1 (C64-4 DISCRIMINATOR): widget CR has inline spec.apiRef.extras=
//	       {region:eu}; coords.Extras={name:demo-vpc} — DISJOINT (coords does
//	       NOT pre-contain the inline; that disjointness is what the old golden
//	       lacked). DeriveSubscriptionKey(coords) MUST == the emit key folding
//	       unionForKey(inline,{name}) = {region,name}. RED on the old code
//	       (folds {name} only), GREEN on the fix.
//	ARM 2 (C64-5 backward-compat): empty inline → DeriveSubscriptionKey == the
//	       request-only emit key (no regression for inline-free widgets).
//	ARM 3: widgetContent class, same disjoint inline shape — RED/GREEN.
//	ARM 4: restactions class — request-only on BOTH sides (branch UNCHANGED).
//	ARM 5 (C64-1 fail-closed): the widget CR is NOT gettable (absent) →
//	       DeriveSubscriptionKey returns ("", false) — the coord is SKIPPED,
//	       NOT armed with a request-only key.
package dispatchers

import (
	"context"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var widgetParityGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "panels"}

// buildWidgetParityWatcher seeds an RBAC snapshot granting userA get/list on
// the widget GVR + a widget CR (panel-1 in ns demo) carrying the supplied
// inline spec.apiRef.extras, published as cache.Global() for the test. When
// inlineApiRefExtras is nil the widget is seeded with no inline block. When
// seedWidget is false the widget CR is omitted (ARM 5 fail-closed: objects.Get
// finds nothing).
func buildWidgetParityWatcher(t *testing.T, seedWidget bool, inlineApiRefExtras map[string]any) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")

	// #327: reset the process-lived RBAC sub-gen counters so cross-test
	// username reuse can't leak sub-gen into this parity harness (same class
	// as newTestWatcher / buildF3AdminsParityWatcher). No t.Parallel here, so
	// the process-global reset is sequential-safe.
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		crbGVR: "ClusterRoleBindingList",
		crGVR:  "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
		widgetParityGVR: "PanelList",
	}
	rule := []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{"widgets.templates.krateo.io"}, Resources: []string{"panels"}}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "panel-reader"}, Rules: rule},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "a-bind", UID: types.UID("uid-A")},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: "userA"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "panel-reader"},
		},
	}
	if seedWidget {
		spec := map[string]any{}
		if inlineApiRefExtras != nil {
			spec["apiRef"] = map[string]any{
				"name":      "some-ra",
				"namespace": "demo",
				"extras":    inlineApiRefExtras,
			}
		}
		w := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "widgets.templates.krateo.io/v1beta1",
			"kind":       "Panel",
			"metadata":   map[string]any{"name": "panel-1", "namespace": "demo"},
			"spec":       spec,
		}}
		seed = append(seed, w)
	}

	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	// Make the widget GVR servable so objects.Get serves it from the informer
	// (C64-6) rather than falling through to a (non-existent) apiserver.
	_, _ = rw.EnsureResourceType(widgetParityGVR)
	_ = rw.WaitForCacheSync(ctx, 5*time.Second)
	cache.RebuildRBACSnapshotForTest(rw)
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
}

func ctxUserA() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "userA"}))
}

func panelCoords(class string, extras map[string]any) SubscriptionCoordinates {
	return SubscriptionCoordinates{
		Class:     class,
		Group:     widgetParityGVR.Group,
		Version:   widgetParityGVR.Version,
		Resource:  widgetParityGVR.Resource,
		Namespace: "demo",
		Name:      "panel-1",
		Extras:    extras,
	}
}

// ARM 1 — C64-4 DISCRIMINATOR. Inline {region:eu} in the CR; coords {name:demo-vpc}
// disjoint. The subscription key must == the emit key folding the union.
func TestFalsifier64_ARM1_WidgetsInlineExtrasUnion(t *testing.T) {
	inline := map[string]any{"region": "eu"}
	buildWidgetParityWatcher(t, true, inline)
	ctx := ctxUserA()
	coords := panelCoords(classWidgets, map[string]any{"name": "demo-vpc"})
	// #64 pagination: the emit path normalizes page/perPage (paginationInfo →
	// normalizePagination), and DeriveSubscriptionKey now does too. Mirror it on
	// the hand-built emit keys below so this extras golden compares like-for-like
	// (0,0 → -1,-1) instead of tripping on the pagination fold.
	coords.PerPage, coords.Page = normalizePagination(coords.PerPage, coords.Page)

	subKey, ok := DeriveSubscriptionKey(ctx, coords)
	if !ok || subKey == "" {
		t.Fatalf("DeriveSubscriptionKey failed (ok=%v key=%q) — RBAC/objects.Get setup wrong?", ok, subKey)
	}

	// The emit key: driven through the REAL prod fold effectiveKeyExtras (NOT a
	// hand-built unionForKey). F6 (docs/f6-chrome-route-key-design-2026-07-12.md,
	// arch-ruled 2026-07-13): the prod emit path (widgets.go:152) AND the
	// subscription path (subscriptionKeyExtras) BOTH call effectiveKeyExtras, so
	// building the golden's emit key any other way is a SHADOW that can drift (the
	// #66 lesson). The inline {region:eu} (apiRef.extras) is CR-fixed → F6 does NOT
	// filter it; the request {name:demo-vpc} is UNDECLARED → F6 drops it from the
	// key on BOTH sides identically. So the sub key still == the emit key.
	got := objects.Get(ctx, templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: "panel-1", Namespace: "demo"},
		APIVersion: "widgets.templates.krateo.io/v1beta1",
		Resource:   "panels",
	})
	if got.Err != nil || got.Unstructured == nil {
		t.Fatalf("objects.Get(panel-1) failed in setup: err=%v", got.Err)
	}
	emitExtras := effectiveKeyExtras(ctx, got.Unstructured.Object, map[string]any{"name": "demo-vpc"})
	emitKey, handle, _ := dispatchCacheLookupKey(ctx, classWidgets,
		coords.Group, coords.Version, coords.Resource, coords.Namespace, coords.Name,
		coords.PerPage, coords.Page, emitExtras)
	if handle == nil || emitKey == "" {
		t.Fatalf("emit key derivation failed")
	}

	// Sanity: the inline {region:eu} actually changed the emit key vs a widget with
	// NO inline (else the test can't discriminate — the inline must be load-bearing).
	// (Comparing against request-only would be a no-op under F6: the undeclared
	// {name} request extra drops from the key, so request-only == no-extras. The
	// discriminant is the CR-fixed INLINE map, which F6 preserves.)
	noInline := map[string]any{"spec": map[string]any{}}
	bareExtras := effectiveKeyExtras(ctx, noInline, map[string]any{"name": "demo-vpc"})
	bareKey, _, _ := dispatchCacheLookupKey(ctx, classWidgets,
		coords.Group, coords.Version, coords.Resource, coords.Namespace, coords.Name,
		coords.PerPage, coords.Page, bareExtras)
	if emitKey == bareKey {
		t.Fatalf("ARM1 setup: inline {region:eu} did not change the emit key vs a no-inline widget — not discriminating")
	}

	if subKey != emitKey {
		t.Fatalf("ARM1 C64-4 RED: subscription key %q != emit key %q. DeriveSubscriptionKey must fold the inline "+
			"extras union (subscriptionKeyExtras), not request-only — else an inline-extras widget never matches "+
			"its published cell (the 1.5.5–1.5.10 zero-delivery).", subKey, emitKey)
	}
}

// ARM 2 — C64-5 backward-compat: inline-free widget → subscription key == the
// request-only emit key (no regression).
func TestFalsifier64_ARM2_WidgetsNoInlineBackwardCompat(t *testing.T) {
	buildWidgetParityWatcher(t, true, nil) // no inline block
	ctx := ctxUserA()
	coords := panelCoords(classWidgets, map[string]any{"name": "demo-vpc"})
	coords.PerPage, coords.Page = normalizePagination(coords.PerPage, coords.Page) // #64 pagination parity

	subKey, ok := DeriveSubscriptionKey(ctx, coords)
	if !ok {
		t.Fatalf("DeriveSubscriptionKey failed for inline-free widget")
	}
	// F6 (arch-ruled 2026-07-13): the emit-side comparison must go through the REAL
	// effectiveKeyExtras (not a raw {name} fold) — for an inline-free UNDECLARED
	// widget the request {name} drops from the key on BOTH sides, so the sub key
	// equals the emit key with an EMPTY effective fold. (Hand-building {name} would
	// be a pre-F6 shadow that no longer matches the filtered prod path.)
	got := objects.Get(ctx, templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: "panel-1", Namespace: "demo"},
		APIVersion: "widgets.templates.krateo.io/v1beta1", Resource: "panels",
	})
	if got.Err != nil || got.Unstructured == nil {
		t.Fatalf("objects.Get(panel-1) failed in setup: err=%v", got.Err)
	}
	emitExtras := effectiveKeyExtras(ctx, got.Unstructured.Object, map[string]any{"name": "demo-vpc"})
	emitKey, _, _ := dispatchCacheLookupKey(ctx, classWidgets,
		coords.Group, coords.Version, coords.Resource, coords.Namespace, coords.Name,
		coords.PerPage, coords.Page, emitExtras)
	if subKey != emitKey {
		t.Fatalf("ARM2 C64-5: inline-free widget subscription key %q != emit key %q — backward-compat/parity regression",
			subKey, emitKey)
	}
}

// ARM 3 — widgetContent class, same disjoint inline shape.
func TestFalsifier64_ARM3_WidgetContentInlineExtrasUnion(t *testing.T) {
	inline := map[string]any{"region": "eu"}
	buildWidgetParityWatcher(t, true, inline)
	ctx := ctxUserA()
	coords := panelCoords(cache.CacheEntryClassWidgetContent, map[string]any{"name": "demo-vpc"})
	coords.PerPage, coords.Page = normalizePagination(coords.PerPage, coords.Page) // #64 pagination parity

	subKey, ok := DeriveSubscriptionKey(ctx, coords)
	if !ok || subKey == "" {
		t.Fatalf("DeriveSubscriptionKey(widgetContent) failed (ok=%v)", ok)
	}
	got := objects.Get(ctx, templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: "panel-1", Namespace: "demo"},
		APIVersion: "widgets.templates.krateo.io/v1beta1", Resource: "panels",
	})
	// F6 (arch-ruled 2026-07-13): emit key via the REAL effectiveKeyExtras (not a
	// hand-built unionForKey shadow). Inline {region:eu} folds (CR-fixed); the
	// undeclared {name} request extra drops on both sides → sub key == emit key.
	emitExtras := effectiveKeyExtras(ctx, got.Unstructured.Object, map[string]any{"name": "demo-vpc"})
	emitKey, _, _ := dispatchWidgetContentKey(ctx,
		coords.Group, coords.Version, coords.Resource, coords.Namespace, coords.Name,
		coords.PerPage, coords.Page, emitExtras)
	if subKey != emitKey {
		t.Fatalf("ARM3: widgetContent subscription key %q != emit key %q (inline-extras union parity broken)", subKey, emitKey)
	}
}

// ARM 4 — restactions class: request-only on BOTH sides (branch UNCHANGED). The
// subscription key must equal the request-only key, and must NOT fetch/fold the
// widget CR's inline extras (a RESTAction is not a widget).
func TestFalsifier64_ARM4_RestActionsRequestOnlyUnchanged(t *testing.T) {
	inline := map[string]any{"region": "eu"} // present in the CR, but restactions must ignore it
	buildWidgetParityWatcher(t, true, inline)
	ctx := ctxUserA()
	coords := panelCoords(classRestActions, map[string]any{"name": "demo-vpc"})
	coords.PerPage, coords.Page = normalizePagination(coords.PerPage, coords.Page) // #64 pagination parity

	subKey, ok := DeriveSubscriptionKey(ctx, coords)
	if !ok {
		t.Fatalf("DeriveSubscriptionKey(restactions) failed")
	}
	reqOnlyKey, _, _ := dispatchCacheLookupKey(ctx, classRestActions,
		coords.Group, coords.Version, coords.Resource, coords.Namespace, coords.Name,
		coords.PerPage, coords.Page, map[string]any{"name": "demo-vpc"})
	if subKey != reqOnlyKey {
		t.Fatalf("ARM4: restactions subscription key %q != request-only key %q — the restactions branch must "+
			"stay request-only (no inline-extras fold)", subKey, reqOnlyKey)
	}
}

// ARM 5 — C64-1 fail-closed: the widget CR is NOT gettable → DeriveSubscriptionKey
// returns ("", false). The coord is SKIPPED, never armed with a request-only key
// (which would re-introduce the mismatch for an inline-extras widget).
func TestFalsifier64_ARM5_FailClosedOnMissingCR(t *testing.T) {
	buildWidgetParityWatcher(t, false, nil) // widget CR absent
	ctx := ctxUserA()
	coords := panelCoords(classWidgets, map[string]any{"name": "demo-vpc"})

	subKey, ok := DeriveSubscriptionKey(ctx, coords)
	if ok || subKey != "" {
		t.Fatalf("ARM5 C64-1: widget CR absent but DeriveSubscriptionKey returned (%q, %v) — must fail-closed "+
			"(\"\", false) and SKIP the coord, never arm a request-only key", subKey, ok)
	}
}
