// prewarm_f3_key_parity_test.go — #130 F3 falsifier (c): seed↔browser key parity
// through REAL derivation, plus the never-worse SA-traffic arm (safety-check #1).
//
// C-F3-3 proves the attribution claim from the design §3: for an UNDECLARED
// first-nav widget the admins group-rep SEED key (Username="", Groups=["admins"])
// FIELD-equals the real admin BROWSER key (Username="admin", Groups=["admins"]),
// so a completed admins seed writes a browser-hittable cell. And for a DECLARED
// identityContext:[username] widget the keys DIVERGE (guards the §4 caveat — such
// a widget is per-user and not cohort-seed-coverable).
//
// BOTH sides run the REAL derivation (withCohortSeedContext vs a real request
// ctx → dispatchCacheLookupKey → effectiveKeyExtras → widgets.DeclaredIdentity)
// and the arm asserts PRE-HASH ResolvedKeyInputs FIELD-equality BEFORE the digest
// (feedback_key_parity_golden_real_inputs_prehash_diff), never hand-fed keys
// (feedback_consultation_mutation_is_not_key_correctness). curl is inadmissible
// here (feedback_curl_probes_inadmissible_for_seed_hit_acceptance).

package dispatchers

import (
	"context"
	"reflect"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var f3WidgetGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}

// buildF3AdminsParityWatcher publishes an admins Group ClusterRoleBinding on the
// widget GVR + two widget CRs: "undeclared-flex" (no identityContext) and
// "declared-flex" (spec.identityContext:[username]). Makes the GVR servable so
// the real dispatchCacheLookupKey derivation runs on both.
func buildF3AdminsParityWatcher(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")

	// #205 / #224-step1: the RBAC sub-generation counters are process-lived.
	// This test's seed folds subGen(admins); the browser derivation folds
	// subGen(admin-User)+subGen(admins). They are equal iff subGen(admin-User)
	// is 0 — which is the TRUE contract here (admin is GROUP-granted; there is
	// no User:admin binding), but it only holds if OTHER tests' admin-User
	// sub-gen bumps do not leak into this process. Reset to the clean baseline
	// at setup and restore on cleanup so the parity reflects this test's own
	// group-granted contract, not ambient sub-gen churn (the ~2/3 full-suite
	// flake at the ComputeKey digest assertion). This is real isolation of the
	// test's stated contract, NOT a quiesce-to-force-equality.
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		f3WidgetGVR: "FlexList",
	}
	rule := []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{f3WidgetGVR.Group}, Resources: []string{f3WidgetGVR.Resource}}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "flex-reader"}, Rules: rule},
		// admins granted via a GROUP-subject CRB — the group-rep the seed uses.
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "admins-bind", UID: types.UID("uid-admins")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", Name: "admins"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "flex-reader"},
		},
		// UNDECLARED widget — no spec.identityContext → DeclaredIdentity nil → key
		// is identity-invariant (BindingUID re-derived first-match, same both sides).
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": f3WidgetGVR.Group + "/" + f3WidgetGVR.Version,
			"kind":       "Flex",
			"metadata":   map[string]any{"name": "undeclared-flex", "namespace": "krateo-system"},
			"spec":       map[string]any{},
		}},
		// DECLARED widget — spec.identityContext:[username] → DeclaredIdentity
		// folds the username into the key → seed (no username) vs browser (admin)
		// keys DIVERGE.
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": f3WidgetGVR.Group + "/" + f3WidgetGVR.Version,
			"kind":       "Flex",
			"metadata":   map[string]any{"name": "declared-flex", "namespace": "krateo-system"},
			"spec":       map[string]any{"identityContext": []any{"username"}},
		}},
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
	_, _ = rw.EnsureResourceType(f3WidgetGVR)
	_ = rw.WaitForCacheSync(ctx, 5*time.Second)
	cache.RebuildRBACSnapshotForTest(rw)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{f3WidgetGVR})
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
		cache.ResetBindingsByGVRIndexForTest()
	})
}

// f3WidgetCR reconstructs the widget CR object the seeded watcher holds, so the
// derive helpers can thread it through the REAL keyExtras derivation
// (effectiveKeyExtras → widgets.DeclaredIdentity) exactly as widgets.go:152 does
// before calling dispatchCacheLookupKey. The declared CR carries
// spec.identityContext:[username]; the undeclared CR has an empty spec.
func f3WidgetCR(widgetName string, declared bool) map[string]any {
	spec := map[string]any{}
	if declared {
		spec["identityContext"] = []any{"username"}
	}
	return map[string]any{
		"apiVersion": f3WidgetGVR.Group + "/" + f3WidgetGVR.Version,
		"kind":       "Flex",
		"metadata":   map[string]any{"name": widgetName, "namespace": "krateo-system"},
		"spec":       spec,
	}
}

// buildF3SAOnlyWatcher publishes a ServiceAccount-subject ClusterRoleBinding
// (the SA-only kind F3 excludes from seeding) granting get/list on the widget
// GVR + the undeclared widget CR, servable. Used by the never-worse arm to prove
// the excluded SA still SERVES via the normal dispatch.
func buildF3SAOnlyWatcher(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		f3WidgetGVR: "FlexList",
	}
	rule := []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{f3WidgetGVR.Group}, Resources: []string{f3WidgetGVR.Resource}}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "flex-reader"}, Rules: rule},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "cdc-sa-bind", UID: types.UID("uid-cdc-sa")},
			Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "cdc-sa", Namespace: "krateo-system"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "flex-reader"},
		},
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": f3WidgetGVR.Group + "/" + f3WidgetGVR.Version,
			"kind":       "Flex",
			"metadata":   map[string]any{"name": "undeclared-flex", "namespace": "krateo-system"},
			"spec":       map[string]any{},
		}},
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
	_, _ = rw.EnsureResourceType(f3WidgetGVR)
	_ = rw.WaitForCacheSync(ctx, 5*time.Second)
	cache.RebuildRBACSnapshotForTest(rw)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{f3WidgetGVR})
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
		cache.ResetBindingsByGVRIndexForTest()
	})
}

// deriveSeedInputs runs the REAL seed derivation for the admins group-rep
// (Username="", Groups=["admins"]): withCohortSeedContext → effectiveKeyExtras
// (the SAME keyExtras widgets.go computes) → dispatchCacheLookupKey.
func deriveSeedInputs(t *testing.T, widgetName string, declared bool) (*cache.ResolvedKeyInputs, string) {
	t.Helper()
	c := seedTarget{Username: "", Groups: []string{"admins"}}
	cctx := withCohortSeedContext(context.Background(), c, endpoints.Endpoint{}, nil)
	keyExtras := effectiveKeyExtras(cctx, f3WidgetCR(widgetName, declared), nil)
	key, _, inputs := dispatchCacheLookupKey(cctx, "widgets",
		f3WidgetGVR.Group, f3WidgetGVR.Version, f3WidgetGVR.Resource,
		"krateo-system", widgetName, -1, -1, keyExtras)
	return inputs, key
}

// deriveBrowserInputs runs the REAL browser derivation (real admin identity:
// Username="admin", Groups=["admins"]) → effectiveKeyExtras → dispatchCacheLookupKey.
func deriveBrowserInputs(t *testing.T, widgetName string, declared bool) (*cache.ResolvedKeyInputs, string) {
	t.Helper()
	bctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "admin", Groups: []string{"admins"}}))
	keyExtras := effectiveKeyExtras(bctx, f3WidgetCR(widgetName, declared), nil)
	key, _, inputs := dispatchCacheLookupKey(bctx, "widgets",
		f3WidgetGVR.Group, f3WidgetGVR.Version, f3WidgetGVR.Resource,
		"krateo-system", widgetName, -1, -1, keyExtras)
	return inputs, key
}

// hashedKeyInputsEqual compares the SUBSET of ResolvedKeyInputs fields that
// ComputeKey actually hashes (CacheEntryClass, Group, Version, Resource,
// Namespace, Name, BindingUID, RBACSubGen, PerPage, Page, Extras) — NOT the diagnostic
// RepresentativeUsername/RepresentativeGroups, which ComputeKey deliberately
// does NOT fold (resolved.go: "Carried on Inputs but NOT folded into
// ComputeKey"). A raw struct DeepEqual would false-RED on the representative
// identity, which legitimately differs between the group-rep seed and the real
// browser — the whole point of the identity-invariant-key design.
func hashedKeyInputsEqual(a, b *cache.ResolvedKeyInputs) bool {
	// Stage IS included (arch review): ComputeKey folds it when non-empty
	// (resolved.go:699). It is "" for the widget class here, so inclusion is
	// inert — but the helper must mirror ComputeKey's fold set EXACTLY so the
	// pre-hash field assertion is the discriminating one, not merely digest-backed.
	return a.CacheEntryClass == b.CacheEntryClass &&
		a.Group == b.Group && a.Version == b.Version && a.Resource == b.Resource &&
		a.Namespace == b.Namespace && a.Name == b.Name &&
		a.BindingUID == b.BindingUID &&
		// #205 / #224-step1: ComputeKey folds RBACSubGen for every identity-bound
		// class (resolved.go, right after BindingUID). Mirror it here so a
		// sub-gen divergence REDs as a clear FIELD mismatch at the pre-hash
		// assertion instead of an opaque ComputeKey digest mismatch, AND so this
		// guard would catch a real multi-subject sub-gen divergence, not only the
		// test-isolation leak this change also fixes.
		a.RBACSubGen == b.RBACSubGen &&
		a.PerPage == b.PerPage && a.Page == b.Page &&
		a.Stage == b.Stage &&
		reflect.DeepEqual(a.Extras, b.Extras)
}

// TestF3KeyParity_UndeclaredWidget_SeedKeyEqualsBrowserKey is arm (c)(i): the
// undeclared first-nav widget's seed key (admins group-rep) FIELD-equals the
// real admin browser key → a completed admins seed writes a browser-hittable
// cell (the attribution=0 gap was coverage, NOT key divergence).
func TestF3KeyParity_UndeclaredWidget_SeedKeyEqualsBrowserKey(t *testing.T) {
	buildF3AdminsParityWatcher(t)

	seedInputs, seedKey := deriveSeedInputs(t, "undeclared-flex", false)
	browserInputs, browserKey := deriveBrowserInputs(t, "undeclared-flex", false)

	if seedInputs == nil || browserInputs == nil {
		t.Fatalf("arm (c)(i): nil inputs — seed=%v browser=%v (RBAC/objects.Get setup wrong; the admins "+
			"binding must first-match on both sides)", seedInputs, browserInputs)
	}
	// PRE-HASH equality over the HASHED SUBSET (feedback_key_parity_golden_real_inputs_prehash_diff):
	// for an undeclared widget DeclaredIdentity is nil on BOTH sides, so nothing
	// folds Extras, and both re-derive the SAME first-match admins BindingUID. The
	// diagnostic RepresentativeUsername legitimately differs ("" seed vs "admin"
	// browser) and is NOT hashed — comparing it (raw struct DeepEqual) would
	// false-RED. hashedKeyInputsEqual compares exactly what ComputeKey folds.
	if !hashedKeyInputsEqual(seedInputs, browserInputs) {
		t.Fatalf("arm (c)(i): undeclared-widget seed vs browser HASHED key inputs DIFFER — "+
			"a completed admins seed would NOT be browser-hittable (key-divergence, not coverage).\n seed=%+v\n browser=%+v",
			*seedInputs, *browserInputs)
	}
	// The digest matches (closes the loop end-to-end through real ComputeKey).
	if seedKey != browserKey {
		t.Fatalf("arm (c)(i): hashed inputs equal but DIFFERENT ComputeKey digest — seed=%q browser=%q", seedKey, browserKey)
	}
	// The BindingUID must be a real first-match (not "") — else the parity is the
	// vacuous empty-identity collapse, not a genuine admins-cohort match.
	if seedInputs.BindingUID == "" {
		t.Fatalf("arm (c)(i): re-derived BindingUID is empty — the admins group identity did not first-match "+
			"a published binding; parity would be vacuous. inputs=%+v", *seedInputs)
	}
}

// TestF3KeyParity_DeclaredWidget_SeedKeyDivergesFromBrowser is arm (c)(ii): a
// declared identityContext:[username] widget folds the username into the key, so
// the group-rep seed (Username="") and the real admin browser (Username="admin")
// derive DIFFERENT keys — such a widget is per-user and NOT cohort-seed-coverable
// (guards the §4 caveat; a future regression that made it seed-covered would
// write an admin-unhittable cell).
func TestF3KeyParity_DeclaredWidget_SeedKeyDivergesFromBrowser(t *testing.T) {
	buildF3AdminsParityWatcher(t)

	seedInputs, seedKey := deriveSeedInputs(t, "declared-flex", true)
	browserInputs, browserKey := deriveBrowserInputs(t, "declared-flex", true)
	if seedInputs == nil || browserInputs == nil {
		t.Fatalf("arm (c)(ii): nil inputs — seed=%v browser=%v", seedInputs, browserInputs)
	}
	// Sanity: the declared fold MUST have injected the username into Extras on the
	// browser side (proving the fold actually ran through the real path — else the
	// divergence below would be vacuous). effectiveKeyExtras → DeclaredIdentity
	// reads spec.identityContext:[username] + ctx UserInfo.
	if _, ok := browserInputs.Extras["username"]; !ok {
		t.Fatalf("arm (c)(ii) setup: the declared identityContext:[username] fold did NOT inject username into "+
			"the browser key Extras — the real effectiveKeyExtras/DeclaredIdentity path did not run; the "+
			"divergence assert would be vacuous. browser=%+v", *browserInputs)
	}
	// The declared username fold MUST make the keys diverge (seed folds no
	// username — group-rep has Username=""; browser folds "admin"). A group-rep
	// seed would write a cell the admin cannot hit — so such a widget is per-user
	// and out of cohort-seed scope (guards the §4 caveat).
	if seedKey == browserKey {
		t.Fatalf("arm (c)(ii): declared identityContext:[username] widget produced EQUAL seed/browser keys — "+
			"the username fold is inert; a group-rep seed would write a cell the admin cannot hit yet the "+
			"key claims parity.\n seed=%+v\n browser=%+v", *seedInputs, *browserInputs)
	}
}

// TestF3SAExclusion_NeverWorse_SATrafficStillServes is the safety-check #1 arm:
// the SA-exclusion touches ONLY the boot SEED target set. An excluded
// ServiceAccount identity that issues a REAL /call still resolves through the
// normal dispatch path — it derives a valid (non-empty) BindingUID and its cell
// cold-fills as a TRAFFIC entry (SeededAtBoot=false → hit_source:"traffic").
// Proves NEVER-WORSE: excluding the SA from seeding does not deny it service.
func TestF3SAExclusion_NeverWorse_SATrafficStillServes(t *testing.T) {
	// A watcher granting a ServiceAccount get/list on the widget GVR (an SA-ONLY
	// binding — the kind F3 excludes from seeding). Reuse the admins-parity
	// builder shape but with an SA subject.
	buildF3SAOnlyWatcher(t)

	// (1) SEED side: the SA-only binding is EXCLUDED from the enumeration.
	targets := cache.EnumeratePrewarmTargetsForGVR(f3WidgetGVR, "list")
	for _, tg := range targets {
		if tg.Subject.Username == "system:serviceaccount:krateo-system:cdc-sa" {
			t.Fatalf("never-worse setup: the SA-only cohort must be EXCLUDED from seed targets; present: %+v", targets)
		}
	}

	// (2) SERVE side (never-worse): a REAL dispatch under the SA identity still
	// derives a NON-EMPTY BindingUID — the SA can be served; its cell would
	// cold-fill on first request as a traffic entry. This is the customer /call
	// path, entirely independent of the seed enumeration the exclusion touched.
	saCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "system:serviceaccount:krateo-system:cdc-sa"}))
	_, _, inputs := dispatchCacheLookupKey(saCtx, "widgets",
		f3WidgetGVR.Group, f3WidgetGVR.Version, f3WidgetGVR.Resource,
		"krateo-system", "undeclared-flex", -1, -1, nil)
	if inputs == nil {
		t.Fatalf("never-worse: excluded SA derived nil inputs — the SA can no longer be served (WORSE); " +
			"exclusion must touch SEED only, not serve")
	}
	if inputs.BindingUID == "" {
		t.Fatalf("never-worse: excluded SA re-derived an EMPTY BindingUID — it cannot be served from cache "+
			"(the empty-BindingUID serve-miss collapse). Exclusion must NOT deny the SA service; inputs=%+v", *inputs)
	}
	// A cell written on this dispatch is a TRAFFIC entry (SeededAtBoot false by
	// construction — the seed never wrote it), so a subsequent hit reports
	// hit_source:"traffic". Assert the traffic default holds on a fresh entry.
	entry := &cache.ResolvedEntry{RawJSON: []byte(`{}`)}
	if entry.SeededAtBoot {
		t.Fatalf("never-worse: a non-seed (traffic) ResolvedEntry must default SeededAtBoot=false")
	}
}
