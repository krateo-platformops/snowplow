// prewarm_dedup_key_parity_test.go — #42 ARM-KEY-PARITY (design §3,
// feedback_key_parity_golden_real_inputs_prehash_diff).
//
// The dedup (prewarm_enumeration.go) collapses N bindings that project to the
// same representative (Username, sorted-Groups) tuple to ONE seed dispatch. It
// is LOSSLESS iff every deduped-away binding would have derived the SAME cell
// as the surviving representative. This arm PROVES that through the REAL
// derivation path — withCohortSeedContext → dispatchCacheLookupKey (which reads
// xcontext.UserInfo and re-derives the cell-key BindingUID via
// rbac.EvaluateRBAC first-match, Path B) — and asserts FIELD-EQUALITY of the
// pre-hash ResolvedKeyInputs (NOT a digest compare) across the collapsed group.
//
// Real RBAC snapshot in the wildcard-binding shape: K bindings, all Group/devs,
// all granting get/list on the SAME widget GVR (the per-composition-RoleBinding
// floor). Each pre-dedup seedTarget carries a DISTINCT enumerated BindingUID but
// the SAME representative identity. Driving each through the real derivation
// must yield BYTE-IDENTICAL ResolvedKeyInputs — the enumerated BindingUID never
// reaches the key; the re-derived first-match one does, and it is the same for
// all of them. That is exactly why deduping them is safe.
//
// Hermetic: dynamicfake watcher + published RBAC snapshot; never touches the
// rbac package's destructive TestMain (AC-D6).

package dispatchers

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var dedupWidgetGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}

// buildDedupParityWatcher publishes an RBAC snapshot with K ClusterRoleBindings
// ALL granting Group/devs get/list on the widget GVR, plus the widget CR, and
// makes the widget GVR servable so objects.Get + dispatchCacheLookupKey run the
// real derivation. Returns the K distinct enumerated BindingUIDs (in the index)
// for the caller to construct pre-dedup seedTargets.
func buildDedupParityWatcher(t *testing.T, k int) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")

	// #327: reset the process-lived RBAC sub-gen counters so cross-test
	// username reuse can't leak sub-gen into this parity harness (same class
	// as newTestWatcher / buildF3AdminsParityWatcher). No t.Parallel here, so
	// the process-global reset is sequential-safe.
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		dedupWidgetGVR: "FlexList",
	}
	rule := []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{dedupWidgetGVR.Group}, Resources: []string{dedupWidgetGVR.Resource}}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "flex-reader"}, Rules: rule},
	}
	// K distinct bindings, ALL Group/devs → all project to the SAME
	// representative tuple; the per-composition-RoleBinding floor shape.
	for i := 0; i < k; i++ {
		seed = append(seed, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "devs-bind-" + string(rune('a'+i)), UID: types.UID("uid-devs-" + string(rune('a'+i)))},
			Subjects:   []rbacv1.Subject{{Kind: "Group", Name: "devs"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "flex-reader"},
		})
	}
	// The widget CR so objects.Get serves it from the informer.
	seed = append(seed, &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": dedupWidgetGVR.Group + "/" + dedupWidgetGVR.Version,
		"kind":       "Flex",
		"metadata":   map[string]any{"name": "dashboard-flex", "namespace": "krateo-system"},
		"spec":       map[string]any{},
	}})

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
	_, _ = rw.EnsureResourceType(dedupWidgetGVR)
	_ = rw.WaitForCacheSync(ctx, 5*time.Second)
	cache.RebuildRBACSnapshotForTest(rw)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{dedupWidgetGVR})
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

// deriveInputsForTuple runs the REAL withCohortSeedContext → dispatchCacheLookupKey
// for a seedTarget (representative identity), returning the pre-hash
// ResolvedKeyInputs the seed Put would fold. saEP/saRC are inert here (no I/O
// beyond objects.Get from the informer + rbac.EvaluateRBAC over the published
// snapshot).
func deriveInputsForTuple(ctx context.Context, c seedTarget) *cache.ResolvedKeyInputs {
	cctx := withCohortSeedContext(ctx, c, endpoints.Endpoint{}, nil)
	_, _, inputs := dispatchCacheLookupKey(cctx, "widgets",
		dedupWidgetGVR.Group, dedupWidgetGVR.Version, dedupWidgetGVR.Resource,
		"krateo-system", "dashboard-flex", -1, -1, nil)
	return inputs
}

// TestPrewarmDedup_ARM_KEY_PARITY: K same-tuple pre-dedup targets all derive
// FIELD-EQUAL ResolvedKeyInputs through the real path (so collapsing them is
// lossless), AND the deduped enumerator returns exactly ONE target whose
// derived inputs equal them.
func TestPrewarmDedup_ARM_KEY_PARITY(t *testing.T) {
	const k = 5
	buildDedupParityWatcher(t, k)

	base := context.Background()

	// Pre-dedup targets: all Group/devs (same representative), DISTINCT
	// enumerated BindingUID (what the pre-dedup enumeration would have emitted).
	var preDedup []seedTarget
	for i := 0; i < k; i++ {
		preDedup = append(preDedup, seedTarget{
			BindingUID: "C:uid-devs-" + string(rune('a'+i)), // distinct, DIAGNOSTIC only
			Username:   "",
			Groups:     []string{"devs"},
		})
	}

	// Derive the representative's inputs (first target).
	repInputs := deriveInputsForTuple(base, preDedup[0])
	if repInputs == nil {
		t.Fatal("ARM-KEY-PARITY: representative derived nil ResolvedKeyInputs — RBAC/objects.Get setup wrong (EvaluateRBAC must first-match a devs binding)")
	}
	if repInputs.BindingUID == "" {
		t.Fatalf("ARM-KEY-PARITY: representative re-derived an EMPTY BindingUID — the Group/devs identity must first-match a published binding; inputs=%+v", repInputs)
	}

	// Every deduped-away target must derive FIELD-EQUAL inputs (pre-hash) — the
	// enumerated BindingUID differs but never reaches the key; the re-derived
	// first-match BindingUID is identical for all of them.
	for i := 1; i < k; i++ {
		got := deriveInputsForTuple(base, preDedup[i])
		if got == nil {
			t.Fatalf("ARM-KEY-PARITY: deduped-away target %d derived nil inputs", i)
		}
		if !reflect.DeepEqual(*got, *repInputs) {
			t.Fatalf("ARM-KEY-PARITY: deduped-away target %d (enumerated BindingUID %q) derived DIFFERENT pre-hash inputs than the representative — dedup would be LOSSY.\n rep=%+v\n got=%+v",
				i, preDedup[i].BindingUID, *repInputs, *got)
		}
	}
	t.Logf("ARM-KEY-PARITY: all %d same-tuple pre-dedup targets derive FIELD-EQUAL ResolvedKeyInputs (re-derived BindingUID=%q) — collapsing them is lossless", k, repInputs.BindingUID)

	// And the deduped enumerator returns EXACTLY ONE target for this tuple,
	// whose derived inputs equal the representative's.
	targets := cache.EnumeratePrewarmTargetsForGVR(dedupWidgetGVR, "list")
	if len(targets) != 1 {
		t.Fatalf("ARM-KEY-PARITY: enumerator returned %d targets for %d same-tuple bindings; want 1: %+v", len(targets), k, targets)
	}
	oneInputs := deriveInputsForTuple(base, seedTarget{
		BindingUID: targets[0].BindingUID,
		Username:   targets[0].Subject.Username,
		Groups:     targets[0].Subject.Groups,
	})
	if oneInputs == nil || !reflect.DeepEqual(*oneInputs, *repInputs) {
		t.Fatalf("ARM-KEY-PARITY: the single deduped target derives inputs != the pre-dedup representative's.\n rep=%+v\n one=%v", *repInputs, oneInputs)
	}
}
