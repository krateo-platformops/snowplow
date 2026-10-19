// prewarm_seed_empty_binding_skip_test.go — #42 FIX-C: the seed primitives skip
// the Put when the cohort re-derives a first-match BindingUID of "" (design §A4
// security finding, populate side).
//
// A4: some seed cohorts (system:kube-controller-manager, resourcequota-controller
// SA, …) re-derive BindingUID="" (EvaluateRBAC deny/err → fail-closed collapse).
// Every "" cohort folds the SAME shared empty-identity cell, resolved under a
// possibly-broad identity; any real request that also derives "" would HIT it.
// FIX-C: seedOneRestaction/seedOneWidget skip the Put (one skip log) when the
// re-derived BindingUID is "". Non-empty cohorts still seed.
//
// FALSIFIER SHAPE (team-lead ruling (a), guard-level with the REAL trigger
// derivation; forge-the-split rejected as fake-production scaffolding):
//   ARM-1 (trigger): a deny cohort derives BindingUID=="" through the REAL
//     dispatchCacheLookupKey; a granted cohort derives non-empty. Real
//     derivation of the exact FIX-C trigger.
//   ARM-2 (wiring, REAL function): the deny cohort driven through the REAL
//     seedOneWidget emits phase1.seed.skip.empty_binding + returns nil (no
//     Put); the granted cohort does NOT emit the skip. seedOneWidget is chosen
//     as the wire-site deliberately (see the ORDERING note below).
//   MUTATION: remove the guard in seedOneWidget → ARM-2 loses the skip log
//     (RED), observed via the real function's emission.
//
// ORDERING NOTE — WHY seedOneWidget, and WHY end-to-end is OC-covered, NOT a
// forged seam (do NOT "fix" this by adding an objects.Get double that serves
// the RA while EvaluateRBAC denies — that is a fake-production split,
// feedback_no_fake_production_scenarios):
//   - seedOneRestaction does objects.Get(RA) BEFORE dispatchCacheLookupKey. The
//     RA fetch's filterGetByRBAC and the key's EvaluateRBAC use the SAME
//     (verb=get, RA gvr, name) inputs, so a cohort that would derive ""
//     ALSO fails the informer-serve RBAC filter → objects.Get falls through to
//     the apiserver → errors before the guard. A "cleanly denied" cohort thus
//     never reaches seedOneRestaction's guard hermetically.
//   - seedOneWidget takes the widget CR already in hand (navWidgetEntry.W) and
//     reaches dispatchCacheLookupKey with NO objects.Get precondition, so the
//     ""-trigger IS reachable there through the real function — this is the
//     wire-site ARM-2 + the mutation use.
//   - The A4 case for seedOneRestaction (KCM/CCM/resourcequota-controller) is a
//     fail-closed inconsistency (informer-serve admits the RA fetch but
//     EvaluateRBAC first-match returns "") — its END-TO-END proof is the
//     ON-CLUSTER re-eval: the boot log must show phase1.seed.skip.empty_binding
//     for those cohorts AND zero seed-diag lines with an empty buid. That
//     observes the real split instead of forging it.
//
// Hermetic: dynamicfake watcher; never touches ./internal/rbac.

package dispatchers

import (
	"bytes"
	"context"
	"log/slog"
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

var fixCWidgetGVR = schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "flexes"}

// buildFixCWatcher publishes a watcher granting userGranted get on the widget
// GVR (→ non-empty first-match BindingUID) and NOT userDenied (→ EvaluateRBAC
// denies → BindingUID ""). The widget GVR is servable so dispatchCacheLookupKey
// runs the real derivation.
func buildFixCWatcher(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		fixCWidgetGVR: "FlexList",
		crbGVR:        "ClusterRoleBindingList",
		crGVR:         "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
	}
	flexRule := []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{fixCWidgetGVR.Group}, Resources: []string{fixCWidgetGVR.Resource}}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "flex-reader"}, Rules: flexRule},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "granted-bind", UID: types.UID("uid-granted")},
			Subjects:   []rbacv1.Subject{{Kind: "User", Name: "userGranted"}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "flex-reader"},
		},
		// The widget CR (so a resolve past the guard would find it; the granted
		// arm proceeds past the guard, the denied arm skips before it).
		&unstructured.Unstructured{Object: map[string]any{
			"apiVersion": fixCWidgetGVR.Group + "/" + fixCWidgetGVR.Version,
			"kind":       "Flex",
			"metadata":   map[string]any{"name": "dashboard-flex", "namespace": "krateo-system"},
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
	rw.EnsureResourceType(fixCWidgetGVR)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
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

// fixCCohortCtx mirrors withCohortSeedContext's identity install so
// dispatchCacheLookupKey re-derives the first-match BindingUID for the cohort.
// saEP/saRC inert — the FIX-C guard returns before any resolve/transport use.
func fixCCohortCtx(username string) context.Context {
	return withCohortSeedContext(context.Background(),
		seedTarget{Username: username}, endpoints.Endpoint{}, nil)
}

func fixCWidgetEntry() navWidgetEntry {
	w := &unstructured.Unstructured{}
	w.SetNamespace("krateo-system")
	w.SetName("dashboard-flex")
	w.SetGroupVersionKind(schema.GroupVersionKind{
		Group: fixCWidgetGVR.Group, Version: fixCWidgetGVR.Version, Kind: "Flex",
	})
	return navWidgetEntry{W: w, GVR: fixCWidgetGVR}
}

// ARM-1 — the REAL trigger derivation: deny cohort → BindingUID ""; granted →
// non-empty. This is the exact condition the FIX-C guard branches on.
func TestFixC_Trigger_EmptyBindingForDenyCohort(t *testing.T) {
	buildFixCWatcher(t)

	denied := fixCCohortCtx("userDenied")
	_, _, denyInputs := dispatchCacheLookupKey(denied, "widgets",
		fixCWidgetGVR.Group, fixCWidgetGVR.Version, fixCWidgetGVR.Resource,
		"krateo-system", "dashboard-flex", -1, -1, nil)
	if denyInputs == nil {
		t.Fatal("ARM-1: deny cohort got nil inputs (cache must be on)")
	}
	if denyInputs.BindingUID != "" {
		t.Fatalf("ARM-1: a cohort with NO get-grant on the widget GVR must re-derive BindingUID==\"\" (the FIX-C trigger); got %q", denyInputs.BindingUID)
	}

	granted := fixCCohortCtx("userGranted")
	_, _, grantInputs := dispatchCacheLookupKey(granted, "widgets",
		fixCWidgetGVR.Group, fixCWidgetGVR.Version, fixCWidgetGVR.Resource,
		"krateo-system", "dashboard-flex", -1, -1, nil)
	if grantInputs == nil || grantInputs.BindingUID == "" {
		t.Fatalf("ARM-1 control: a GRANTED cohort must re-derive a NON-empty BindingUID; got %+v", grantInputs)
	}
}

// ARM-2 — WIRING through the REAL seedOneWidget (no objects.Get precondition,
// see the ORDERING note): the deny cohort takes the FIX-C skip (log + no Put);
// the granted cohort does not skip.
func TestFixC_SeedOneWidget_SkipsEmptyBinding(t *testing.T) {
	buildFixCWatcher(t)
	e := fixCWidgetEntry()

	var buf syncBuffer
	prevDefault := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(prevDefault) })

	// GREEN: deny cohort → "" → skip log, returns nil (non-fatal).
	buf.Reset()
	if err := seedOneWidget(fixCCohortCtx("userDenied"), e, "authn-ns", seedModeBoot); err != nil {
		t.Fatalf("FIX-C: seedOneWidget for a \"\"-binding cohort returned %v; want nil", err)
	}
	if !bytes.Contains(buf.Bytes(), []byte("phase1.seed.skip.empty_binding")) {
		t.Fatalf("FIX-C ARM-2: the REAL seedOneWidget must emit phase1.seed.skip.empty_binding for a \"\"-binding cohort; logs:\n%s", buf.String())
	}

	// CONTROL: granted cohort → non-empty → NO skip log (proceeds past the
	// guard; downstream resolve may fail on inert transport, but that is AFTER
	// the FIX-C decision — the ABSENCE of the skip log is the signal FIX-C did
	// not fire for a non-empty identity).
	buf.Reset()
	_ = seedOneWidget(fixCCohortCtx("userGranted"), e, "authn-ns", seedModeBoot)
	if bytes.Contains(buf.Bytes(), []byte("phase1.seed.skip.empty_binding")) {
		t.Fatalf("FIX-C ARM-2 control: a GRANTED cohort (non-empty BindingUID) must NOT emit the empty_binding skip; logs:\n%s", buf.String())
	}
}
