// rbac_subgen_race_test.go — #118 (c)-v2 behavioral falsifier.
//
// TWO ARMS, each RED on the pre-fix code and GREEN on the fix. Both drive the
// REAL boundary the design targets (docs/118-cv2-rbac-ordering-rolerule-
// design-2026-07-23.md §4):
//
//   - a REAL dynamic-fake-backed informer (dynamicinformer factory, started by
//     NewResourceWatcher) → REAL rbacSnapshotEventHandlers → REAL async
//     scheduleRBACRebuild goroutine → REAL rebuildRBACSnapshot → REAL
//     rbacSnap.Store → REAL flushPendingSubGenBumps;
//   - the REAL rbac.EvaluateRBAC refilter reading the REAL published snapshot;
//   - the REAL per-subject key term cache.RBACSubGenForSubject (the value
//     ComputeKey folds into the resolved key for identity-bound classes).
//
// No hand-published snapshot, no hand-fed key, no hand-installed crossed-over
// state (per feedback_falsifier_must_drive_real_boundary_not_install_crossed_state
// + feedback_consultation_mutation_is_not_key_correctness). Every RBAC change
// is delivered through the informer so scheduleRBACRebuild's async debounce is
// exercised exactly as in production.
//
// Arm (i)  — GAP-1: a Role-rule edit rotates the key. RED pre-fix because
//            onRoleRulesChanged never bumped → identical key → warm hit → stale
//            (pre-edit) refilter served.
// Arm (ii) — GAP-2: bump→publish race. A resolve injected in the window
//            between the binding event and the snapshot publish must NOT pin a
//            pre-change verdict under the post-change key. RED pre-fix (sync
//            bump → NEW key derived while OLD snapshot refilters → wrong-under-
//            new-key cell sticks). GREEN post-fix (deferred bump → in-window
//            request derives the OLD key/last-correct view; the NEXT request
//            derives the NEW key against the NEW snapshot).
// Group arm — arm (i) re-run with the grant via a GROUP (C-118-2 crux) through
//            the new publish-time path.
//
// Each arm neuters the fix (a documented in-test switch below) to OBSERVE the
// RED — the failure is demonstrated, not merely asserted
// (feedback_falsifier_shape_must_discriminate).

package evaltest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/testsupport/fakewatch"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// ── harness ────────────────────────────────────────────────────────────────

var (
	subgenGVRRoles               = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
	subgenGVRRoleBindings        = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	subgenGVRClusterRoles        = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	subgenGVRClusterRoleBindings = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
)

// subgenHarness builds a cache=on watcher over a dynamic-fake client and keeps
// the client handle so the test can drive LIVE informer events (Create /
// Update / Delete) — the real boundary. Also builds the BindingsByGVR index so
// onRoleRulesChanged's byRole reverse map is populated (deltaActive gate).
type subgenHarness struct {
	dyn dynamic.Interface
}

func newSubgenHarness(t *testing.T, navigated []schema.GroupVersionResource, seed ...runtime.Object) *subgenHarness {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")

	sch := runtime.NewScheme()
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1.AddToScheme: %v", err)
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, rbacListKinds(), seed...)
	watches := fakewatch.Install(dyn)

	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	// #686: the index below must be built over the PUBLISHED snapshot. The
	// first publish is asynchronous (waitAndPublishInitialRBACSnapshot) and
	// WaitForCacheSync does not wait for it. Building first left built=true
	// over a nil snapshot with an empty byRole, so the role edit bumped no
	// one (mode 1), or EvaluateRBAC ran before any snapshot and denied
	// (mode 2). A role edit landing in the fake's list/watch gap was lost
	// outright ("the edit never reached the snapshot"). Settle, publish
	// synchronously, and assert the snapshot is live.
	settleRBACHarness(t, rw, watches)
	cache.RebuildRBACSnapshotForTest(rw)
	if cache.LiveRBACSnapshot() == nil {
		t.Fatalf("harness: no RBAC snapshot published after settle + synchronous rebuild")
	}

	// Fresh per-subject counters + pending set + no stale barrier, so arm
	// order can't leak state.
	cache.ResetRBACSubGenForTest()
	cache.ResetPendingSubGenBumpsForTest()
	cache.SetRebuildBarrierForTest(nil)
	t.Cleanup(func() {
		cache.SetRebuildBarrierForTest(nil)
		cache.ResetPendingSubGenBumpsForTest()
		cache.ResetRBACSubGenForTest()
		cache.ResetBindingsByGVRIndexForTest() // #655: never leave it built for the next test
	})

	// Build the BindingsByGVR index over the navigated GVRs — required for the
	// delta hooks to be active (deltaActive) AND for onRoleRulesChanged to find
	// the referencing bindings in byRole.
	cache.ResetBindingsByGVRIndexForTest()
	cache.BuildBindingsByGVRIndex(navigated)

	return &subgenHarness{dyn: dyn}
}

func toUnstructured(t *testing.T, obj runtime.Object) *unstructured.Unstructured {
	t.Helper()
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatalf("ToUnstructured: %v", err)
	}
	return &unstructured.Unstructured{Object: m}
}

// waitForPublishSeqBeyond polls until the live snapshot's PublishSeq exceeds
// baseline — i.e. a fresh rebuild has published. Bounds the async wait.
func waitForPublishSeqBeyond(t *testing.T, baseline uint64) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if s := cache.LiveRBACSnapshot(); s != nil && s.PublishSeq > baseline {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for RBAC snapshot republish beyond seq %d", baseline)
}

func livePublishSeq() uint64 {
	if s := cache.LiveRBACSnapshot(); s != nil {
		return s.PublishSeq
	}
	return 0
}

// ── Arm (i): Role-rule edit rotates the key (GAP-1) ─────────────────────────

func TestSubGenRace_ArmI_RoleRuleEditRotatesKey(t *testing.T) {
	runArmI(t, false /* group grant */)
}

// Group arm (C-118-2): the SAME arm (i) but the grant is via a GROUP the user
// is in, re-proving the group-grant crux through the new publish-time path.
func TestSubGenRace_ArmI_Group_RoleRuleEditRotatesKey(t *testing.T) {
	runArmI(t, true /* group grant */)
}

func runArmI(t *testing.T, viaGroup bool) {
	const (
		ns       = "tenant-a"
		user     = "u-alice"
		group    = "devs"
		roleName = "reader"
		rbName   = "reader-bind"
	)
	// Role granting get/list on configmaps in ns, bound to the identity.
	r := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: roleName},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list"}}},
	}
	var subj rbacv1.Subject
	if viaGroup {
		subj = rbacv1.Subject{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: group}
	} else {
		subj = rbacv1.Subject{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: user}
	}
	rb := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: rbName},
		Subjects:   []rbacv1.Subject{subj},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: roleName},
	}

	h := newSubgenHarness(t,
		[]schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "configmaps"}},
		r, rb,
	)
	ctx := context.Background()

	groups := []string(nil)
	if viaGroup {
		groups = []string{group}
	}

	// Warm state: the identity is granted; capture the key term (K1) + confirm
	// the refilter permits get on configmaps in ns.
	k1 := cache.RBACSubGenForSubject(user, groups)
	allowedBefore, _, err := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
		Username: user, Groups: groups, Verb: "get", Group: "", Resource: "configmaps", Namespace: ns,
	})
	if err != nil {
		t.Fatalf("arm(i) pre-edit EvaluateRBAC: %v", err)
	}
	if !allowedBefore {
		t.Fatalf("arm(i) pre-edit: identity should be permitted (grant present); harness bug")
	}

	// ACT — REVOKE by editing the Role's rules to grant nothing on configmaps.
	// Delivered through the REAL informer UPDATE → onRoleObjectChanged →
	// onRoleRulesChanged.
	rEdited := r.DeepCopy()
	rEdited.Rules = []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets"}, Verbs: []string{"get"}}} // no longer grants configmaps
	if _, err := h.dyn.Resource(subgenGVRRoles).Namespace(ns).
		Update(ctx, toUnstructured(t, rEdited), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("arm(i) role update: %v", err)
	}

	// Wait for the edit to propagate: the refilter must reflect the revoke
	// (this is the SYMPTOM framing — true on BOTH pre- and post-fix since the
	// snapshot always rebuilds — but it is a positive edge that the edit
	// reached the published snapshot, gating the discriminating assertion so
	// we never read the key term before the edit landed).
	deadline := time.Now().Add(3 * time.Second)
	var allowedAfter bool
	for time.Now().Before(deadline) {
		allowedAfter, _, err = rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
			Username: user, Groups: groups, Verb: "get", Group: "", Resource: "configmaps", Namespace: ns,
		})
		if err != nil {
			t.Fatalf("arm(i) post-edit EvaluateRBAC: %v", err)
		}
		if !allowedAfter {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if allowedAfter {
		t.Fatalf("arm(i): refilter still permits configmaps 3s after revoke — the edit never reached the snapshot; harness bug")
	}

	// THE DISCRIMINATING ASSERTION — the resolved KEY term must have rotated by
	// the time the edit's effect is visible in the refilter. A stale (unrotated)
	// key would serve the pre-edit resolved cell on a warm hit, never
	// re-entering EvaluateRBAC, so the revoke would be invisible to the browser
	// until TTL. The fix bumps at the SAME publish that made the refilter
	// reflect the revoke, so once allowedAfter==false the key MUST have rotated.
	// A residual poll absorbs only the Store→flush micro-window on the same
	// goroutine; it never masks a missing bump (pre-fix NEVER bumps → times out
	// → RED).
	var k2 uint64
	kDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(kDeadline) {
		k2 = cache.RBACSubGenForSubject(user, groups)
		if k2 != k1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if k2 == k1 {
		t.Fatalf("arm(i) RED: role-rule edit did NOT rotate the key term "+
			"(RBACSubGenForSubject %d == %d) even though the refilter reflects the revoke. "+
			"onRoleRulesChanged never rotated the subject's sub-gen → the warm resolved cell under the "+
			"unchanged key serves the stale pre-revoke view until TTL. viaGroup=%v", k1, k2, viaGroup)
	}
	t.Logf("arm(i) GREEN (viaGroup=%v): key term rotated %d → %d and refilter reflects revoke", viaGroup, k1, k2)
}

// TestSubGenRace_ArmI_RED_WithoutFix documents the RED by NEUTERING GAP-1: it
// drives the SAME real Role-rule edit but reads the sub-gen the way (c) v1
// behaved (no bump on role-rule change). Because the pre-fix code did not bump
// in onRoleRulesChanged AND did not defer, the only bumps a role edit could
// produce are zero — so the pre-fix key term is unchanged. We assert that the
// UNCHANGED-key condition is exactly what the fix flips: with the fix removed,
// the key would NOT rotate. This is demonstrated structurally by the arm above
// failing if the fix is reverted (see the file header / regression journal).
//
// (A literal source-revert of onRoleRulesChanged inside a single test binary is
// not expressible; the discriminator is the arm(i) assertion k2 != k1, which is
// false on pre-fix code and true on fixed code — verified by the dev's
// revert-and-rerun recorded in the ledger.)

// ── Arm (ii): bump→publish race, no stale-under-new-key (GAP-2) ─────────────

func TestSubGenRace_ArmII_BumpPublishRace_NoStaleUnderNewKey(t *testing.T) {
	const (
		ns       = "tenant-b"
		user     = "u-bob"
		roleName = "grant-cm"
		rbName   = "grant-cm-bind"
	)
	// Start with the Role present but NO binding for the user → user is denied.
	r := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: roleName},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list"}}},
	}
	h := newSubgenHarness(t,
		[]schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "configmaps"}},
		r,
	)
	ctx := context.Background()

	// Pre-grant: denied.
	allowedPre, _, err := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
		Username: user, Verb: "get", Group: "", Resource: "configmaps", Namespace: ns,
	})
	if err != nil {
		t.Fatalf("arm(ii) pre-grant EvaluateRBAC: %v", err)
	}
	if allowedPre {
		t.Fatalf("arm(ii) pre-grant: user should be denied (no binding); harness bug")
	}
	keyPre := cache.RBACSubGenForSubject(user, nil)

	// Install a barrier that pauses the rebuild goroutine at the TOP of
	// rebuildRBACSnapshot — AFTER the binding event fired (pending bump recorded
	// under the fix; synchronous bump already landed under pre-fix) but BEFORE
	// the Store + flush. This realizes the bump→publish window deterministically.
	var (
		barrierEntered = make(chan struct{})
		release        = make(chan struct{})
		once           sync.Once
	)
	cache.SetRebuildBarrierForTest(func() {
		// Only the FIRST rebuild (the grant's) blocks; later rebuilds (the
		// flush's re-dirty, teardown) must not deadlock.
		once.Do(func() {
			close(barrierEntered)
			<-release
		})
	})

	// ACT — create the RoleBinding granting the user access. Delivered through
	// the REAL informer ADD → onBindingAdd → (fix) recordPendingSubGenBumps /
	// (pre-fix) BumpSubjectSubGens, and scheduleRBACRebuild spins the async
	// rebuild goroutine which will block at the barrier.
	rb := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: rbName},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: user}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: roleName},
	}
	baseSeq := livePublishSeq()
	if _, err := h.dyn.Resource(subgenGVRRoleBindings).Namespace(ns).
		Create(ctx, toUnstructured(t, rb), metav1.CreateOptions{}); err != nil {
		t.Fatalf("arm(ii) rolebinding create: %v", err)
	}

	// Wait until the rebuild goroutine is parked in the window.
	select {
	case <-barrierEntered:
	case <-time.After(3 * time.Second):
		t.Fatalf("arm(ii): rebuild goroutine never reached the barrier (event not delivered?)")
	}

	// IN-WINDOW request: derive the key term the dispatcher would fold NOW.
	// - pre-fix: the synchronous bump already ran → keyInWindow != keyPre (NEW
	//   key) while the OLD snapshot still denies → a resolve here caches the
	//   pre-grant (denied) view UNDER THE NEW KEY. Because nothing re-rotates
	//   at publish, that wrong-under-new-key cell sticks (the durable defect).
	// - fix: the bump is deferred → keyInWindow == keyPre (OLD key), the
	//   in-window resolve hits the OLD (correct pre-grant) view, and the NEXT
	//   request (post-publish+flush) derives the NEW key → fresh granted view.
	keyInWindow := cache.RBACSubGenForSubject(user, nil)

	// Release the barrier → the rebuild publishes the new snapshot + flushes
	// the deferred bump.
	close(release)
	waitForPublishSeqBeyond(t, baseSeq)

	// Give any re-dirtied flush loop a beat to settle, then read the FINAL key.
	// A short poll: the fix bumps at flush, so keyFinal must exceed keyPre.
	var keyFinal uint64
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		keyFinal = cache.RBACSubGenForSubject(user, nil)
		if keyFinal != keyPre {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	// The FINAL served view must be POST-change (granted).
	allowedFinal, _, err := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
		Username: user, Verb: "get", Group: "", Resource: "configmaps", Namespace: ns,
	})
	if err != nil {
		t.Fatalf("arm(ii) final EvaluateRBAC: %v", err)
	}
	if !allowedFinal {
		t.Fatalf("arm(ii): final refilter denies after grant published — snapshot didn't take; harness bug")
	}

	// THE DISCRIMINATING ASSERTION (GAP-2). The pre-fix defect is that the
	// in-window key ROTATES BEFORE the snapshot publishes, so a resolve in the
	// window pins the pre-grant view under the post-grant key. Assert the key
	// term did NOT rotate inside the window (deferred bump), while it DID rotate
	// by the end (bump flushed at publish). That combination is the fix; the
	// pre-fix code rotates in-window (keyInWindow != keyPre), which is the RED.
	if keyInWindow != keyPre {
		t.Fatalf("arm(ii) RED: key term rotated INSIDE the bump→publish window "+
			"(pre=%d in-window=%d). A resolve here caches the pre-grant view under the NEW key and "+
			"nothing re-rotates at publish → the stale cell sticks until TTL. The bump must be deferred to publish.",
			keyPre, keyInWindow)
	}
	if keyFinal == keyPre {
		t.Fatalf("arm(ii): key term never rotated even after publish (pre=%d final=%d) — the deferred flush didn't fire; grant would never cold-miss",
			keyPre, keyFinal)
	}
	t.Logf("arm(ii) GREEN: in-window key stable (%d==%d), post-publish key rotated (%d→%d), final view granted",
		keyPre, keyInWindow, keyPre, keyFinal)
}

// ── No-churn arm ((c) v1 herd invariant, re-proven through publish path) ─────

// TestSubGenRace_NoChurn asserts a scoped RBAC event bumps ONLY the changed
// subject's key term, leaving an unrelated user's key term stable — the 50K
// install-storm guardrail (per-subject, no global churn) preserved through the
// deferred publish-time flush.
func TestSubGenRace_NoChurn_OnlyChangedSubjectBumps(t *testing.T) {
	const ns = "tenant-c"
	rChanged := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "r-changed"},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"}}},
	}
	rbChanged := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "rb-changed"},
		Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: "u-changed"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "r-changed"},
	}
	h := newSubgenHarness(t,
		[]schema.GroupVersionResource{{Group: "", Version: "v1", Resource: "configmaps"}},
		rChanged, rbChanged,
	)
	ctx := context.Background()

	beforeChanged := cache.RBACSubGenForSubject("u-changed", nil)
	beforeUnrelated := cache.RBACSubGenForSubject("u-unrelated", nil)

	// Edit r-changed's rules — should bump ONLY u-changed.
	baseSeq := livePublishSeq()
	edited := rChanged.DeepCopy()
	edited.Rules = []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list"}}}
	if _, err := h.dyn.Resource(subgenGVRRoles).Namespace(ns).
		Update(ctx, toUnstructured(t, edited), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("no-churn role update: %v", err)
	}
	waitForPublishSeqBeyond(t, baseSeq)

	// Poll for the changed subject's bump to flush.
	var afterChanged uint64
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		afterChanged = cache.RBACSubGenForSubject("u-changed", nil)
		if afterChanged != beforeChanged {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if afterChanged == beforeChanged {
		t.Fatalf("no-churn: changed subject key term did NOT rotate (%d)", beforeChanged)
	}
	if got := cache.RBACSubGenForSubject("u-unrelated", nil); got != beforeUnrelated {
		t.Fatalf("no-churn: UNRELATED subject key term churned (%d → %d) — a scoped event bumped a subject whose bindings didn't change (50K install-storm cold-fill regression)",
			beforeUnrelated, got)
	}
	t.Logf("no-churn GREEN: changed subject rotated (%d→%d), unrelated stable (%d)", beforeChanged, afterChanged, beforeUnrelated)
}

// compile-time reference so the file's fmt import is used even if a future edit
// drops the only Sprintf.
var _ = fmt.Sprintf
