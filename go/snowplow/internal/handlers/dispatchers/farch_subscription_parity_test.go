// farch_subscription_parity_test.go — A3 subscription parity falsifiers F-ARCH-4
// (definitive-cache-identity-architecture-2026-07-07.md §8.1 A3). A3 is TEST-ONLY:
// the subscription arming path (subscriptionKeyExtras, refresh_subscription.go:137)
// ALREADY routes through the shared effectiveKeyExtras (extracted in A1), which A2
// wired to fold declaredIdentityForKey → widgets.DeclaredIdentity. So the /refreshes
// arming key inherits A2's declared-identity injection AUTOMATICALLY, and the EMIT
// path (widgets.go:152 dispatch → effectiveKeyExtras) folds the SAME material through
// the SAME helper. Arming↔serve identity parity for declared widgets therefore holds
// BY CONSTRUCTION — these arms PIN it so it can never regress silently (the #64/#67
// permanent-invariant discipline, extended to the declared-identity dimension).
//
// F-ARCH-4: for a DECLARED widget, DeriveSubscriptionKey(coords, JWT-identity) ==
// ComputeKey(the /call emit that renders it), per subscribable class, with REAL
// derivation + pre-hash ResolvedKeyInputs field-equality (never a hand-fed digest;
// feedback_key_parity_golden_real_inputs_prehash_diff). This is the arming↔serve
// key-parity that keeps /refreshes forgery-proof for declared widgets.
//
// u2-cannot-arm-u1: user u2's subscription derivation for a DECLARED widget yields a
// DIFFERENT key than u1's cell → u2 cannot arm/receive u1's refreshes. RED mutation:
// drop the declaredIdentity fold from the shared effectiveKeyExtras (the same helper
// the subscription path calls) → u1 and u2 derive the same key → u2 arms u1's key.
//
// GATE AUTHN-1: identity flows ONLY from the connection ctx's xcontext.UserInfo (the
// JWT), never the SubscriptionCoordinates wire — the forgery-proof property.
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

// buildDeclaredWidgetParityWatcher seeds a widget CR that DECLARES
// spec.identityContext=declaredKeys (the A2 identity-dependence field) in ns demo,
// and grants BOTH userA and userB get/list on the widget GVR (so the u2 forgery arm
// can drive DeriveSubscriptionKey under each identity — both can GET the CR, the
// key difference is purely the folded identity, not an RBAC skip). Reuses the
// widgetParityGVR shape from refresh_subscription_key_parity_test.go.
//
// sharedBinding controls the RBAC shape: when true, userA and userB are subjects of
// ONE ClusterRoleBinding → they share a BindingUID → the declared-identity fold is
// the SOLE key discriminant (so the forgery arm's key-collapse under the fold-drop
// mutation is provable). When false, each user has its own binding (distinct
// BindingUID) — the realistic per-tenant shape used by the parity arm.
func buildDeclaredWidgetParityWatcher(t *testing.T, sharedBinding bool, declaredKeys ...string) {
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
	}
	if sharedBinding {
		// ONE binding lists BOTH users → identical BindingUID → the ONLY key
		// discriminant is the declared-identity fold (the forgery-arm shape).
		seed = append(seed, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "ab-bind", UID: types.UID("uid-AB")},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: "userA"}, {Kind: "User", Name: "userB"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "panel-reader"},
		})
	} else {
		// Two DISTINCT bindings (distinct UIDs) → distinct BindingUID folds.
		seed = append(seed,
			&rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "a-bind", UID: types.UID("uid-A")},
				Subjects:   []rbacv1.Subject{{Kind: "User", Name: "userA"}},
				RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "panel-reader"},
			},
			&rbacv1.ClusterRoleBinding{
				ObjectMeta: metav1.ObjectMeta{Name: "b-bind", UID: types.UID("uid-B")},
				Subjects:   []rbacv1.Subject{{Kind: "User", Name: "userB"}},
				RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "panel-reader"},
			},
		)
	}
	declared := make([]any, len(declaredKeys))
	for i, k := range declaredKeys {
		declared[i] = k
	}
	w := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.templates.krateo.io/v1beta1",
		"kind":       "Panel",
		"metadata":   map[string]any{"name": "panel-1", "namespace": "demo"},
		"spec":       map[string]any{"identityContext": declared},
	}}
	seed = append(seed, w)

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

func ctxUserB() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "userB"}))
}

// declaredPanelCoords is panelCoords for a DECLARED widget under the two
// subscribable widget classes; identity is NOT in coords (forgery-proof: it
// comes from the connection ctx).
func declaredPanelCoords(class string) SubscriptionCoordinates {
	return panelCoords(class, map[string]any{"name": "demo-vpc"})
}

// emitInputsDeclared computes the EMIT-side ResolvedKeyInputs the /call dispatcher
// would Put for a DECLARED widget under ctx's identity, driving the SAME production
// derivation the dispatch emit path uses: effectiveKeyExtras (which folds the
// declaredIdentity via A2) → dispatch*Key. This is the emit half of the arming↔serve
// parity — reconstructed through the real helper, not hand-built extras.
func emitInputsDeclared(t *testing.T, ctx context.Context, class string, coords SubscriptionCoordinates) *cache.ResolvedKeyInputs {
	t.Helper()
	got := objects.Get(ctx, templatesv1.ObjectReference{
		Reference:  templatesv1.Reference{Name: coords.Name, Namespace: coords.Namespace},
		APIVersion: coords.Group + "/" + coords.Version,
		Resource:   coords.Resource,
	})
	if got.Err != nil || got.Unstructured == nil {
		t.Fatalf("emit setup: objects.Get(%s/%s) failed: %v", coords.Namespace, coords.Name, got.Err)
	}
	// The REAL emit key extras: the SAME effectiveKeyExtras the widgets.go
	// genuine-Put folds (widgets.go:152), which under A2 injects the declared
	// identity from ctx. Pass it through dispatch*Key exactly as emit does.
	emitExtras := effectiveKeyExtras(ctx, got.Unstructured.Object, coords.Extras)
	pp, pg := normalizePagination(coords.PerPage, coords.Page)
	if class == cache.CacheEntryClassWidgetContent {
		_, _, inputs := dispatchWidgetContentKey(ctx,
			coords.Group, coords.Version, coords.Resource, coords.Namespace, coords.Name, pp, pg, emitExtras)
		return inputs
	}
	_, _, inputs := dispatchCacheLookupKey(ctx, class,
		coords.Group, coords.Version, coords.Resource, coords.Namespace, coords.Name, pp, pg, emitExtras)
	return inputs
}

// TestFARCH4_DeclaredWidget_SubscriptionEmitParity — the A3 arming↔serve invariant
// for a DECLARED widget, per subscribable class. For a widget declaring
// identityContext:[username,groups], the SUBSCRIPTION key (production
// DeriveSubscriptionKey under userA's JWT) MUST equal the EMIT key (production
// effectiveKeyExtras → dispatch*Key under the same JWT), with field-by-field
// pre-hash equality as the BindingUID-independent proof. This is the parity that
// keeps declared-widget /refreshes forgery-proof; it holds because both sides fold
// the declared identity through the SAME effectiveKeyExtras (A2). RED mutation: drop
// the declaredIdentity fold from effectiveKeyExtras → the emit key stops folding the
// declared identity while the sub key derivation is the same call, so the parity
// itself does NOT break here (both sides move together) — the DISCRIMINATING arm for
// the fold-drop is the u2-forgery arm below. This arm guards against a FUTURE change
// that folds identity on ONE side only (the #64 desync class at the identity dimension).
func TestFARCH4_DeclaredWidget_SubscriptionEmitParity(t *testing.T) {
	classes := []string{classWidgets, cache.CacheEntryClassWidgetContent}
	for _, class := range classes {
		class := class
		t.Run(class, func(t *testing.T) {
			buildDeclaredWidgetParityWatcher(t, false, "username", "groups")
			ctx := ctxUserA()
			coords := declaredPanelCoords(class)

			subKey, ok := DeriveSubscriptionKey(ctx, coords)
			if !ok || subKey == "" {
				t.Fatalf("%s: DeriveSubscriptionKey failed (ok=%v key=%q)", class, ok, subKey)
			}
			subIn, okIn := deriveSubscriptionKeyInputsForTest(ctx, coords)
			if !okIn || subIn == nil {
				t.Fatalf("%s: sub inputs failed", class)
			}
			emitIn := emitInputsDeclared(t, ctx, class, coords)
			if emitIn == nil {
				t.Fatalf("%s: emit inputs nil", class)
			}

			// Pre-hash: the declared identity (userA) must be folded into the key
			// extras on BOTH sides — this is the load-bearing dimension A3 adds.
			if subIn.Extras["username"] != "userA" {
				t.Fatalf("%s F-ARCH-4 pre-hash: subscription key extras must carry the declared JWT username (userA); got %#v", class, subIn.Extras)
			}
			if emitIn.Extras["username"] != "userA" {
				t.Fatalf("%s F-ARCH-4 pre-hash: emit key extras must carry the declared JWT username (userA); got %#v", class, emitIn.Extras)
			}

			// THE INVARIANT: digest equality (what the broadcaster matches on).
			if got := cache.ComputeKey(*emitIn); subKey != got {
				t.Fatalf("%s F-ARCH-4 INVARIANT BROKEN: DeriveSubscriptionKey %q != emit ComputeKey %q — the declared-widget "+
					"armed key would never match the published key (arming↔serve identity desync).", class, subKey, got)
			}
			assertKeyInputsFieldEqual(t, "farch4/"+class, subIn, emitIn)
		})
	}
}

// TestFARCH4_U2CannotArmU1_DeclaredWidget — the forgery arm. For a DECLARED widget
// where u1 and u2 SHARE ONE binding (identical BindingUID, so the declared-identity
// fold is the SOLE key discriminant), user u2's subscription derivation yields a
// DIFFERENT key than u1's → u2 cannot arm (and thus cannot receive) u1's refreshes.
// RED mutation: drop the declaredIdentity fold from the shared effectiveKeyExtras →
// u1 and u2 key extras lose the username → with the shared binding the keys COLLAPSE
// to equal → u2 arms u1's key (the cross-user leak on the arming path). This arm
// goes RED under that mutation on BOTH the pre-hash extras assertion AND the
// key-collapse assertion (the shared binding is what makes the collapse observable).
func TestFARCH4_U2CannotArmU1_DeclaredWidget(t *testing.T) {
	classes := []string{classWidgets, cache.CacheEntryClassWidgetContent}
	for _, class := range classes {
		class := class
		t.Run(class, func(t *testing.T) {
			buildDeclaredWidgetParityWatcher(t, true, "username", "groups") // SHARED binding
			coords := declaredPanelCoords(class)

			k1, ok1 := DeriveSubscriptionKey(ctxUserA(), coords)
			k2, ok2 := DeriveSubscriptionKey(ctxUserB(), coords)
			if !ok1 || !ok2 || k1 == "" || k2 == "" {
				t.Fatalf("%s: DeriveSubscriptionKey failed (u1 ok=%v key=%q; u2 ok=%v key=%q)", class, ok1, k1, ok2, k2)
			}

			in1, _ := deriveSubscriptionKeyInputsForTest(ctxUserA(), coords)
			in2, _ := deriveSubscriptionKeyInputsForTest(ctxUserB(), coords)
			if in1 == nil || in2 == nil {
				t.Fatalf("%s: sub inputs nil (in1=%v in2=%v)", class, in1, in2)
			}
			// Pre-hash: each user's folded identity must be their OWN username — the
			// declared-identity dimension is what the forgery guard hinges on.
			if in1.Extras["username"] != "userA" || in2.Extras["username"] != "userB" {
				t.Fatalf("%s FORGERY pre-hash: each subscription's folded identity must be its own JWT username; "+
					"u1=%#v u2=%#v — the RED mutation (drop the declaredIdentity fold from effectiveKeyExtras) makes both empty", class, in1.Extras, in2.Extras)
			}

			// THE FORGERY GUARD: distinct keys → u2 cannot arm u1's cell.
			if k1 == k2 {
				t.Fatalf("%s FORGERY: u2's declared-widget subscription key %q == u1's %q — u2 could arm/receive u1's "+
					"per-user refreshes (cross-user leak on the arming path). RED mutation (drop declaredIdentity fold) collapses them.", class, k2, k1)
			}
		})
	}
}
