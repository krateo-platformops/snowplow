// implicit_group_detector_261_test.go — #261 case-2 serve-time detector
// falsifier (implicit-group first-match missed-rotation carrier).
//
// #261 is a SERVE-TIME DETECTOR (observability only; the keying FIX is
// v7-deferred). The bounded self-stale-leak class: a BindingUID identity key
// can rotate with NO sub-gen bump. Case-2 carrier: the serve-time
// first-permitting binding's WINNING subject matched only through an IMPLICIT
// group (system:authenticated, or a synthetic system:serviceaccounts[:ns]
// group). RBACSubGenForSubject sums only the requester's PRESENTED groups, so a
// grant/revoke through such a binding rotates the serve-time first-match
// BindingUID with NO sub-gen bump. The counter measures the AT-RISK SERVE
// POPULATION (a structural proxy); off-zero => reassess, NOT a confirmed-leak
// count.
//
// WHY THIS ARM DRIVES THE REAL BOUNDARY
// (feedback_falsifier_must_drive_real_boundary_not_install_crossed_state):
// the "implicit-group" class is NOT installed. It is PRODUCED by the REAL
// rbac.EvaluateRBAC walking a REAL published snapshot whose ONLY binding
// granting the layer's GET is a ClusterRoleBinding whose subject is
// Group=system:authenticated — so a requester carrying an UNRELATED group
// ("devs") first-permits ONLY through the implicit system:authenticated match.
// The winning-subject class is computed by the evaluator (the only component
// that knows which subject won), surfaced via the WinningSubjectClassOut
// out-param at KEY-MINT, marked on cacheInputs, and READ at the hit-site — the
// counter's 0->+1 is driven by a REAL dispatch SERVE that HITS the cell.
//
// MEMO-HIT PATH EXERCISED: the populate serve is a cold RBAC walk (memo miss →
// stores the class on snapshotAuthzVerdict); the HIT serve re-derives the key
// via a memo HIT, which must READ the class back off the verdict — so this arm
// also proves the snapshot-authz memo thread carries the class (else a memo-hit
// serve would lose it and the counter would stay 0).
//
// HIT-GATED: the increment must land ON THE HIT, never on the populate/miss (a
// miss re-resolves under fresh perms — no stale). Arm A populates via a REAL
// resolve (a miss, asserting the counter stays 0) and only THEN drives the HIT.
//
// CONTROL: an identical serve whose winning subject is a REGULAR User (the
// h1-bind CRB's User=alice subject) HITS — the implicit_group detector does NOT
// move. This discriminates: the only difference is HOW the winning subject
// matched.
//
// T2 (hard): the increment is asserted through the OTLP/RegisterCallback path
// (cache.ResolvedCacheStatsByStat()[...]), not merely the raw store field.
//
// RED (before the case-2 bump is wired, or with it stubbed): the counter stays
// 0 on the hit. GREEN on this branch: the hit bumps it exactly once, observable
// via the OTLP stat map.

package dispatchers

import (
	"context"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

const c2ImplicitGroupStat = "serve_missed_rotation_atrisk_implicit_group"

// c2ImplicitGroupCount reads the #261 case-2 counter through the OTLP /
// RegisterCallback path (ResolvedCacheStatsByStat — the same map metrics.go
// ranges), so the assertion exercises the OTLP-observed value, not just the raw
// atomic. Fails the test if the store is unpublished (a setup error).
func c2ImplicitGroupCount(t *testing.T) int64 {
	t.Helper()
	m := cache.ResolvedCacheStatsByStat()
	if len(m) == 0 {
		t.Fatalf("ResolvedCacheStatsByStat() is empty — the resolved cache store is not published; " +
			"the detector cannot be observed via the OTLP path")
	}
	v, ok := m[c2ImplicitGroupStat]
	if !ok {
		t.Fatalf("ResolvedCacheStatsByStat() has no %q key — the OTLP stat is not wired", c2ImplicitGroupStat)
	}
	return v
}

// c2BuildWatcherImplicitGroup wires a cache=on ResourceWatcher whose RBAC
// snapshot's ONLY binding granting get/list on the RA + widget GVRs is a
// ClusterRoleBinding whose subject is Group=system:authenticated. A requester
// carrying an unrelated group therefore first-permits ONLY through the implicit
// system:authenticated match — the case-2 carrier. Mirrors h1BuildWatcher's
// wiring (the same reset + watcher + snapshot-publish discipline) but swaps the
// CRB subject from User=alice to Group=system:authenticated.
func c2BuildWatcherImplicitGroup(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		h1RAGVR:     "RESTActionList",
		h1WidgetGVR: "PanelList",
		crbGVR:      "ClusterRoleBindingList",
		crGVR:       "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}: "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:        "RoleList",
	}
	rule := []rbacv1.PolicyRule{{
		Verbs:     []string{"get", "list"},
		APIGroups: []string{h1RAGVR.Group, h1WidgetGVR.Group},
		Resources: []string{h1RAGVR.Resource, h1WidgetGVR.Resource},
	}}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "c2-reader"}, Rules: rule},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "c2-bind", UID: types.UID("uid-c2")},
			// WINNING subject is an IMPLICIT group. alice (Groups=["devs"]) does
			// NOT match by User or by "devs" — she first-permits ONLY via the
			// implicit system:authenticated branch (anySubjectMatches evaluate.go).
			Subjects: []rbacv1.Subject{{Kind: "Group", Name: "system:authenticated"}},
			RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "c2-reader"},
		},
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

// TestC2_ImplicitGroup_HitBumpsDetector — Arm A + T2.
func TestC2_ImplicitGroup_HitBumpsDetector(t *testing.T) {
	c2BuildWatcherImplicitGroup(t)
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)

	// Build the index against the LIVE (full) snapshot — the c2-reader ClusterRole
	// IS present, so the binding resolves normally and is NOT in the case-3
	// roleref-unresolved skip-set. This isolates the case-2 (implicit-group)
	// class: any bump here comes from the RBAC winning-subject fact, not case-3.
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR})

	reqCtx := h1ReqCtx(h1User) // alice, Groups=["devs"]
	key, handle, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: expected a live, cacheable key with a non-empty first-match BindingUID; "+
			"key=%q handle=%v inputs=%+v", key, handle != nil, inputs)
	}
	// PRECONDITION: the MARK-AT-MINT classified this cell as case-2 implicit_group
	// — produced by the REAL evaluator's winning-subject determination, not
	// asserted into existence. (It must NOT be the case-3 class: the role is
	// present, so IsRoleRefUnresolvedAtBuild is false.)
	if inputs.AtRiskClass != cache.AtRiskImplicitGroup {
		t.Fatalf("PRECONDITION FAILED: the mint-time at-risk class for the serve-time first-match "+
			"BindingUID %q must be AtRiskImplicitGroup (the winning subject matched via "+
			"system:authenticated); got %v. Without it the arm cannot exercise the case-2 detector.",
			inputs.BindingUID, inputs.AtRiskClass)
	}
	if cache.IsRoleRefUnresolvedAtBuild(inputs.BindingUID) {
		t.Fatalf("PRECONDITION: the case-2 binding %q must NOT also read case-3 at-risk (role is present "+
			"at build) — else the arm would not isolate the implicit-group class", inputs.BindingUID)
	}
	if _, ok := handle.Get(key); ok {
		t.Fatalf("PRECONDITION: the derived key must be cold before the miss-populate")
	}
	if got := c2ImplicitGroupCount(t); got != 0 {
		t.Fatalf("PRECONDITION: the detector must start at 0; got %d", got)
	}

	// --- POPULATE via a REAL resolve (a MISS). The detector must NOT move on a
	// miss (HIT-GATED). This serve is a cold RBAC walk (authz-memo miss). ---------
	c3Serve(t, reqCtx)
	if _, ok := handle.Get(key); !ok {
		t.Fatalf("SETUP CHECK: the non-UAF miss must have POPULATED the cell under key %q "+
			"(else there is nothing to HIT)", key)
	}
	if got := c2ImplicitGroupCount(t); got != 0 {
		t.Fatalf("HIT-GATE VIOLATED: the detector moved on the POPULATE/MISS (got %d, want 0). "+
			"A miss re-resolves under fresh perms — only a HIT of the at-risk-keyed cell serves "+
			"potentially-stale bytes, so the bump must be hit-gated.", got)
	}

	// --- Drive the SERVE that HITS the cell. Its key derivation re-runs
	// EvaluateRBAC, which now takes the authz-memo HIT path — so the class must be
	// READ back off snapshotAuthzVerdict (the memo thread) for the mark to survive.
	// The detector goes 0 -> +1 ON THE HIT. ---------------------------------------
	c3Serve(t, reqCtx)

	// T2 (hard): observe the increment through the OTLP / RegisterCallback path.
	if got := c2ImplicitGroupCount(t); got != 1 {
		t.Fatalf("RED (#261 case-2 detector not wired / memo thread lost the class): a HIT of an "+
			"at-risk cell whose winning subject matched via an implicit group (binding %q) must bump "+
			"%s via the OTLP stat map to exactly 1; got %d", inputs.BindingUID, c2ImplicitGroupStat, got)
	}
	// And the raw store field agrees (belt-and-suspenders; the OTLP path above is
	// the load-bearing assertion).
	if raw := cache.ResolvedCache().Stats().ServeMissedRotationAtriskImplicitGroup; raw != 1 {
		t.Fatalf("the raw store field must also read 1; got %d", raw)
	}
	// Cross-class isolation: the case-3 counter must NOT have moved (this cell is
	// NOT roleref-unresolved).
	if raw := cache.ResolvedCache().Stats().ServeMissedRotationAtriskRolerefUnresolved; raw != 0 {
		t.Fatalf("cross-class leak: the case-3 roleref_unresolved counter moved to %d on a case-2 "+
			"implicit-group serve; the two classes must be independent", raw)
	}
}

// TestC2_RegularSubject_ControlDoesNotBump — CONTROL. A serve whose winning
// subject is a REGULAR User (h1-bind's User=alice) HITS; the implicit_group
// detector does NOT move. Discriminates the implicit-group class from a regular
// User match.
func TestC2_RegularSubject_ControlDoesNotBump(t *testing.T) {
	h1BuildWatcher(t) // h1-bind CRB subject is User=alice — a REGULAR match.
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR})

	reqCtx := h1ReqCtx(h1User)
	key, handle, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: expected a live cacheable key; key=%q handle=%v inputs=%+v", key, handle != nil, inputs)
	}
	// PRECONDITION: the control cell is genuinely NOT case-2 (winning subject is a
	// regular User) — else this arm would pass vacuously.
	if inputs.AtRiskClass == cache.AtRiskImplicitGroup {
		t.Fatalf("PRECONDITION FAILED: the control binding %q must NOT read implicit-group at-risk "+
			"(its winning subject is User=alice, a regular match). A control that is itself at-risk "+
			"cannot discriminate.", inputs.BindingUID)
	}

	c3Serve(t, reqCtx) // miss/populate
	if _, ok := handle.Get(key); !ok {
		t.Fatalf("SETUP CHECK: the control miss must have populated the cell")
	}
	c3Serve(t, reqCtx) // HIT

	if got := c2ImplicitGroupCount(t); got != 0 {
		t.Fatalf("CONTROL BROKE: a HIT of a cell whose winning subject is a REGULAR User must NOT move "+
			"the implicit_group detector; got %d, want 0. The detector fires for the implicit-group "+
			"class ONLY.", got)
	}
}
