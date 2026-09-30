// roleref_unresolved_detector_261_test.go — #261 case-3 serve-time detector
// falsifier (roleRef-unresolved-at-build missed-rotation carrier).
//
// #261 is a SERVE-TIME DETECTOR (observability only; the keying FIX is
// v7-deferred). The bounded self-stale-leak class: a BindingUID identity key
// can rotate with NO sub-gen bump. Case-3 carrier: the serve-time
// first-permitting binding's roleRef was UNRESOLVED at the last
// bindings_by_gvr index build (so its grant is not reflected in the keying)
// but PERMITS now at serve — a later role create/edit shifted the grant with
// no bump. The counter measures the AT-RISK SERVE POPULATION (a structural
// proxy); off-zero => reassess, NOT a confirmed-leak count.
//
// WHY THESE ARMS DRIVE THE REAL BOUNDARY
// (feedback_falsifier_must_drive_real_boundary_not_install_crossed_state):
// the "at-risk" flag is NOT installed. It is PRODUCED by building the REAL
// bindings_by_gvr index while the binding's ClusterRole is genuinely absent
// from the published snapshot (the rulesForRoleRef !ok skip site), then
// restoring the full snapshot so the REAL rbac.EvaluateRBAC first-permits the
// requester through that very binding at serve. The counter's 0->+1 is driven
// by a REAL dispatch SERVE that HITS the cell — never a hand-set counter.
//
// HIT-GATED: the increment must land ON THE HIT, never on the populate/miss
// (a miss re-resolves under fresh perms — no stale). Arm B populates via a
// REAL resolve (a miss, asserting the counter stays 0) and only THEN drives
// the HIT.
//
// CONTROL: an identical cell whose roleRef IS resolved at index build (nothing
// skipped) is served and HITS — the detector does NOT move. This discriminates:
// the ONLY difference from Arm B is whether the role was present at build.
//
// T2 (hard): the increment is asserted through the OTLP/RegisterCallback path
// (cache.ResolvedCacheStatsByStat()[...]), not merely the raw store field.
//
// RED on origin/main (and before the hit-site bump is wired): the counter
// stays 0 on the hit — the detector does not exist. GREEN on this branch: the
// hit bumps it exactly once, observable via the OTLP stat map.

package dispatchers

import (
	"context"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"net/http/httptest"
)

const c3RolerefUnresolvedStat = "serve_missed_rotation_atrisk_roleref_unresolved"

// c3RolerefUnresolvedCount reads the #261 case-3 counter through the OTLP /
// RegisterCallback path (ResolvedCacheStatsByStat — the same map metrics.go
// ranges), so the assertion exercises the OTLP-observed value, not just the
// raw atomic. Returns -1 if the store is unpublished (a setup error).
func c3RolerefUnresolvedCount(t *testing.T) int64 {
	t.Helper()
	m := cache.ResolvedCacheStatsByStat()
	if len(m) == 0 {
		t.Fatalf("ResolvedCacheStatsByStat() is empty — the resolved cache store is not published; " +
			"the detector cannot be observed via the OTLP path")
	}
	v, ok := m[c3RolerefUnresolvedStat]
	if !ok {
		t.Fatalf("ResolvedCacheStatsByStat() has no %q key — the OTLP stat is not wired", c3RolerefUnresolvedStat)
	}
	return v
}

// c3PartialSnapshotRoleAbsent returns a snapshot carrying the full snapshot's
// ClusterRoleBindings but with an EMPTY ClusterRolesByName map — so the CRB's
// roleRef is UNRESOLVABLE at BuildBindingsByGVRIndex (rulesForRoleRef !ok),
// which drives the real skip site. Reusing the full snapshot's CRB pointers
// keeps the binding UID byte-identical to what the serve path folds.
func c3PartialSnapshotRoleAbsent(full *cache.RBACSnapshot) *cache.RBACSnapshot {
	return &cache.RBACSnapshot{
		ClusterRoleBindings: full.ClusterRoleBindings,
		RoleBindingsByNS:    map[string][]*rbacv1.RoleBinding{},
		ClusterRolesByName:  map[string]*rbacv1.ClusterRole{}, // role ABSENT → roleRef unresolved at build
		RolesByNSName:       map[string]*rbacv1.Role{},
	}
}

// c3NonUAFResolve is a resolver seam that returns a plain (NON-UAF) RESTAction,
// so the serve path's Put is ACCEPTED and a later serve can HIT the cell.
func c3NonUAFResolve() func(context.Context, restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
	return func(_ context.Context, _ restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
		out := &templatesv1.RESTAction{}
		out.SetName(h1RAName)
		out.SetNamespace(h1NS)
		return out, nil
	}
}

// c3Serve drives one full dispatch through the REAL RESTAction handler and
// returns the response recorder. cr is the fetched CR (non-UAF).
func c3Serve(t *testing.T, ctx context.Context) *httptest.ResponseRecorder {
	t.Helper()
	restore := installRAFakes(t, a1RAUnstructured(false), func() bool { return true }, c3NonUAFResolve())
	defer restore()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(ctx)
	RESTAction().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("dispatch must serve 200; got %d body=%s", rec.Code, rec.Body.String())
	}
	return rec
}

// TestC3_RolerefUnresolvedAtBuild_HitBumpsDetector — Arm B + T2.
func TestC3_RolerefUnresolvedAtBuild_HitBumpsDetector(t *testing.T) {
	h1BuildWatcher(t) // full snapshot (h1-bind CRB uid-h1 + ClusterRole h1-reader), cache on
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)

	full := cache.LiveRBACSnapshot()
	if full == nil || len(full.ClusterRoleBindings) == 0 {
		t.Fatalf("setup: expected a published full snapshot with the h1 CRB; got %+v", full)
	}

	// --- Build the index while the CRB's ClusterRole is GENUINELY ABSENT — the
	// real rulesForRoleRef !ok skip site records the binding's UID. --------------
	cache.PublishRBACSnapshotForTest(c3PartialSnapshotRoleAbsent(full))
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR})
	// Restore the FULL snapshot so the serve-time EvaluateRBAC permits through
	// that same binding (its role now exists — the "later role create" shift).
	cache.PublishRBACSnapshotForTest(full)

	reqCtx := h1ReqCtx(h1User)
	key, handle, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: expected a live, cacheable key with a non-empty first-match BindingUID; "+
			"key=%q handle=%v inputs=%+v", key, handle != nil, inputs)
	}
	// PRECONDITION: the serve-time first-match binding is exactly the one skipped
	// at build — the case-3 carrier. This is produced by the REAL derivations, not
	// asserted into existence.
	if !cache.IsRoleRefUnresolvedAtBuild(inputs.BindingUID) {
		t.Fatalf("PRECONDITION FAILED: the serve-time first-match BindingUID %q must read AT-RISK "+
			"(roleRef unresolved at the last index build). Without it the arm cannot exercise the detector.",
			inputs.BindingUID)
	}
	if _, ok := handle.Get(key); ok {
		t.Fatalf("PRECONDITION: the derived key must be cold before the miss-populate")
	}
	if got := c3RolerefUnresolvedCount(t); got != 0 {
		t.Fatalf("PRECONDITION: the detector must start at 0; got %d", got)
	}

	// --- POPULATE via a REAL resolve (a MISS). The detector must NOT move on a
	// miss (HIT-GATED). ----------------------------------------------------------
	c3Serve(t, reqCtx)
	if _, ok := handle.Get(key); !ok {
		t.Fatalf("SETUP CHECK: the non-UAF miss must have POPULATED the cell under key %q "+
			"(else there is nothing to HIT)", key)
	}
	if got := c3RolerefUnresolvedCount(t); got != 0 {
		t.Fatalf("HIT-GATE VIOLATED: the detector moved on the POPULATE/MISS (got %d, want 0). "+
			"A miss re-resolves under fresh perms — only a HIT of the at-risk-keyed cell serves "+
			"potentially-stale bytes, so the bump must be hit-gated.", got)
	}

	// --- Drive the SERVE that HITS the cell. The detector goes 0 -> +1 ON THE
	// HIT. ------------------------------------------------------------------------
	c3Serve(t, reqCtx)

	// T2 (hard): observe the increment through the OTLP / RegisterCallback path.
	if got := c3RolerefUnresolvedCount(t); got != 1 {
		t.Fatalf("RED (#261 case-3 detector not wired): a HIT of an at-risk cell (roleRef unresolved "+
			"at build, first-permitting binding %q) must bump %s via the OTLP stat map to exactly 1; got %d",
			inputs.BindingUID, c3RolerefUnresolvedStat, got)
	}
	// And the raw store field agrees (belt-and-suspenders; the OTLP path above is
	// the load-bearing assertion).
	if raw := cache.ResolvedCache().Stats().ServeMissedRotationAtriskRolerefUnresolved; raw != 1 {
		t.Fatalf("the raw store field must also read 1; got %d", raw)
	}
}

// TestC3_ResolvedRoleRef_ControlDoesNotBump — CONTROL. A normal cell whose
// roleRef is RESOLVED at index build (nothing skipped) is served and HITS; the
// detector does NOT move. Discriminates the at-risk class.
func TestC3_ResolvedRoleRef_ControlDoesNotBump(t *testing.T) {
	h1BuildWatcher(t)
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)

	// Build the index against the FULL snapshot (the ClusterRole IS present) —
	// the binding resolves and enrols normally, so it is NOT in the skipped-set.
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR})

	reqCtx := h1ReqCtx(h1User)
	key, handle, inputs := dispatchCacheLookupKey(reqCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, h1RAName, -1, -1, nil)
	if handle == nil || key == "" || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRECONDITION: expected a live cacheable key; key=%q handle=%v inputs=%+v", key, handle != nil, inputs)
	}
	// PRECONDITION: the control binding is genuinely NOT at-risk (its roleRef
	// resolved at build) — else this arm would pass vacuously.
	if cache.IsRoleRefUnresolvedAtBuild(inputs.BindingUID) {
		t.Fatalf("PRECONDITION FAILED: the control binding %q must NOT read at-risk (its roleRef resolved "+
			"at build). A control that is itself at-risk cannot discriminate.", inputs.BindingUID)
	}

	c3Serve(t, reqCtx) // miss/populate
	if _, ok := handle.Get(key); !ok {
		t.Fatalf("SETUP CHECK: the control miss must have populated the cell")
	}
	c3Serve(t, reqCtx) // HIT

	if got := c3RolerefUnresolvedCount(t); got != 0 {
		t.Fatalf("CONTROL BROKE: a HIT of a NON-at-risk cell (resolved roleRef) must NOT move the detector; "+
			"got %d, want 0. The detector fires for the at-risk class ONLY.", got)
	}
}
