// discovery_lookup_test.go — Ship 2 Stage 2 / 0.30.247 unit tests for
// the discovery→re-prewarm wiring added to discovery_lookup.go's
// `if added` branch. Package cache so the tests can drive the
// unexported notifyGVRDiscoveredForReprewarm + AddNavigatedGVR via
// their adjacent surfaces.
//
// CORE PROPERTY UNDER TEST (RC-1 falsifier): EnumeratePrewarmTargetsForGVR
// for a post-boot-discovered GVR returns the FULL cohort union
// (wildcard ∪ narrow per-binding) AFTER AddNavigatedGVR widens the
// index. Pre-Ship-2-Stage-2 (no AddNavigatedGVR call) returns ONLY the
// wildcard bucket — narrow cohorts silently skipped.
//
// NON-DESTRUCTIVE — synthetic in-process RBAC snapshots; resets the
// bindings-by-gvr index + GVR-discovered hook registry around each
// test. Safe under `go test ./internal/cache/...`.

package cache

import (
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestDiscoveryHook_AddNavigatedGVRFlow_NarrowCohortEnumeratedPostWiden
// reproduces RC-1: the structural defect where a narrow-RBAC cohort's
// per-binding entry is NOT enumerated for a GVR discovered after the
// index has been built.
//
// SHAPE:
//   - Build the BindingsByGVR index over an EMPTY navigated set (the
//     boot-time state before composition.krateo.io exists).
//   - Verify that EnumeratePrewarmTargetsForGVR(<new GVR>, "list") returns
//     ONLY the wildcard cohort (admin) — narrow cohorts are silently
//     dropped because idx.byGVR[<new GR>] is nil.
//   - Call AddNavigatedGVR(<new GVR>) — the Ship 2 Stage 2 call.
//   - Verify EnumeratePrewarmTargetsForGVR now returns BOTH cohorts
//     (admin via wildcard + devs via per-binding).
//
// This is the FALSIFIER for the production-scale narrow-cohort silent-
// skip scenario (1000 users, most with namespaced/list-only RBAC on
// compositions).
//
// GVR + RBAC alignment: the narrow cohort's roleRef MUST grant get/list
// on the SAME {group,resource} the discovery hook fires for. The
// empirical scenario per task-194-ship-2-stage-2-empirical-trace-2026-06-04.md
// is composition.krateo.io/compositions — that is what the customer
// RoleBindings name, and what the apiserver registers when the
// CompositionDefinition CRD ships.
func TestDiscoveryHook_AddNavigatedGVRFlow_NarrowCohortEnumeratedPostWiden(t *testing.T) {
	ResetBindingsByGVRIndexForTest()
	defer ResetBindingsByGVRIndexForTest()

	// Synthetic RBAC: one cluster-admin (wildcard) and one narrow
	// cohort (devs group with get/list on composition.krateo.io/compositions).
	adminCRB, adminCR := crbRuleUID("admin-binding", "uid-admin",
		rbacv1.Subject{Kind: "User", Name: "admin"}, wildcardRules())
	devsCRB, devsCR := crbRuleUID("devs-binding", "uid-devs",
		rbacv1.Subject{Kind: "Group", Name: "devs"},
		getListRules("composition.krateo.io", "compositions"))
	snap := buildSnap(
		[]*rbacv1.ClusterRoleBinding{adminCRB, devsCRB},
		[]*rbacv1.ClusterRole{adminCR, devsCR},
	)
	PublishRBACSnapshotForTest(snap)

	// EMPTY navigated set — composition.krateo.io is NOT in
	// RegisteredGVRs() at boot (the empirical scenario).
	SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{})

	compGVR := schema.GroupVersionResource{
		Group:    "composition.krateo.io",
		Version:  "v1",
		Resource: "compositions",
	}

	// PRE-FIX: only wildcard (admin) is enumerated. Narrow cohort
	// (devs) is silently dropped because idx.byGVR[<compositions GR>]
	// is nil — RC-1 P0 defect.
	pre := EnumeratePrewarmTargetsForGVR(compGVR, "list")
	if len(pre) != 1 {
		t.Fatalf("pre-widen: expected 1 target (wildcard admin only), got %d: %+v", len(pre), pre)
	}
	if pre[0].Subject.Username != "admin" {
		t.Fatalf("pre-widen: expected admin (wildcard), got %+v", pre[0].Subject)
	}

	// SHIP 2 STAGE 2: widen the navigated set. This is the call
	// added to discovery_lookup.go inside the `if added` branch.
	AddNavigatedGVR(compGVR)

	// POST-FIX: BOTH cohorts (admin via wildcard + devs via narrow
	// per-binding) MUST be enumerated. Narrow-RBAC cells are now
	// reachable by the re-prewarm.
	post := EnumeratePrewarmTargetsForGVR(compGVR, "list")
	if len(post) != 2 {
		t.Fatalf("post-widen: expected 2 targets (admin + devs), got %d: %+v", len(post), post)
	}
	gotAdmin := false
	gotDevs := false
	for _, target := range post {
		if target.Subject.Username == "admin" {
			gotAdmin = true
		}
		for _, g := range target.Subject.Groups {
			if g == "devs" {
				gotDevs = true
			}
		}
	}
	if !gotAdmin {
		t.Fatalf("post-widen: missing admin (wildcard) target: %+v", post)
	}
	if !gotDevs {
		t.Fatalf("post-widen: missing devs (narrow) target — RC-1 silent skip still active: %+v", post)
	}
}

// TestDiscoveryHook_AddNavigatedGVRIdempotent asserts that calling
// AddNavigatedGVR twice for the same GVR is a no-op (the function's
// own idempotency guard at bindings_by_gvr.go:461-463). This is
// load-bearing for the dedup behaviour at the discovery site — repeated
// CRD discovery events (e.g. via the per-group singleflight retry path)
// must NOT re-enrol the same bindings.
func TestDiscoveryHook_AddNavigatedGVRIdempotent(t *testing.T) {
	ResetBindingsByGVRIndexForTest()
	defer ResetBindingsByGVRIndexForTest()

	devsCRB, devsCR := crbRuleUID("devs-binding", "uid-devs",
		rbacv1.Subject{Kind: "Group", Name: "devs"},
		getListRules("composition.krateo.io", "compositions"))
	snap := buildSnap(
		[]*rbacv1.ClusterRoleBinding{devsCRB},
		[]*rbacv1.ClusterRole{devsCR},
	)
	PublishRBACSnapshotForTest(snap)
	SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{})

	compGVR := schema.GroupVersionResource{
		Group:    "composition.krateo.io",
		Version:  "v1",
		Resource: "compositions",
	}

	AddNavigatedGVR(compGVR)
	first := EnumeratePrewarmTargetsForGVR(compGVR, "list")

	// Second call must be a no-op (same set of targets).
	AddNavigatedGVR(compGVR)
	second := EnumeratePrewarmTargetsForGVR(compGVR, "list")

	if len(first) != len(second) {
		t.Fatalf("idempotency: first=%d second=%d targets — expected identical", len(first), len(second))
	}
}

// TestDiscoveryHook_NotifyOrderingIsLoadBearing asserts the design
// invariant: a hook subscriber that reads EnumeratePrewarmTargetsForGVR
// inside its callback observes the WIDENED index (AddNavigatedGVR ran
// before the hook fired). This is the production wire shape — the
// dispatchers-side hook handler enqueues a scope into the engine, and
// the engine's rePrewarm later reads EnumeratePrewarmTargetsForGVR. If
// the order were reversed (notify-then-widen), the engine would see
// the stale index and skip narrow cohorts.
//
// Verifies the same property at the call-site level: the test fires
// AddNavigatedGVR first, then the hook, and the hook's callback
// observes the widened state.
func TestDiscoveryHook_NotifyOrderingIsLoadBearing(t *testing.T) {
	ResetBindingsByGVRIndexForTest()
	defer ResetBindingsByGVRIndexForTest()
	ResetGVRDiscoveredHooksForTest()
	defer ResetGVRDiscoveredHooksForTest()

	adminCRB, adminCR := crbRuleUID("admin-binding", "uid-admin",
		rbacv1.Subject{Kind: "User", Name: "admin"}, wildcardRules())
	devsCRB, devsCR := crbRuleUID("devs-binding", "uid-devs",
		rbacv1.Subject{Kind: "Group", Name: "devs"},
		getListRules("composition.krateo.io", "compositions"))
	snap := buildSnap(
		[]*rbacv1.ClusterRoleBinding{adminCRB, devsCRB},
		[]*rbacv1.ClusterRole{adminCR, devsCR},
	)
	PublishRBACSnapshotForTest(snap)
	SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{})

	compGVR := schema.GroupVersionResource{
		Group:    "composition.krateo.io",
		Version:  "v1",
		Resource: "compositions",
	}

	// Register a hook that reads the widened index. If notify fires
	// BEFORE widen, the read would return only wildcard.
	var observedTargets int
	RegisterGVRDiscoveredHook(func(gvr schema.GroupVersionResource) {
		observedTargets = len(EnumeratePrewarmTargetsForGVR(gvr, "list"))
	})

	// PRODUCTION ORDER: widen FIRST, then notify.
	AddNavigatedGVR(compGVR)
	notifyGVRDiscoveredForReprewarm(compGVR)

	if observedTargets != 2 {
		t.Fatalf("hook observed %d targets — expected 2 (admin + devs). Index ordering broken.", observedTargets)
	}
}
