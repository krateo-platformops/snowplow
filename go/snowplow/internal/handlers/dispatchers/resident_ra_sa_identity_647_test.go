package dispatchers

// resident_ra_sa_identity_647_test.go — #647: the resident enumeration
// (enumerateResident, behind the RBAC-shift reseed, the learned-class seed and
// the #258 rotated-target filter) resolves every RESTAction's target GVR through
// the REAL restActionTargetGVR → objects.Get → informer → filterGetByRBAC. On a
// raw engine ctx (no SA UserInfo, no UserConfig) the filter fails closed, the
// apiserver fall-through has no endpoint, and every RA unit was dropped with a
// bare `continue` — so a rotation or a new learned class reseeded widgets only
// and left every RESTAction cell of the affected identities cold.
//
// The arms run on the #262 harness (real RBAC informer, binding index, learned
// registry, seed key derivation and terminal Put). Only the resolver edges are
// stubbed (widget resolve, RESTAction fetch for the SEED); the target-GVR seam is
// put back to production so the lookup under test is the real one. K=2 cohorts ×
// M=2 RESTActions throughout.

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

const (
	s647RAs      = 2
	s647CohortA  = "s647-cohort-a"
	s647CohortB  = "s647-cohort-b"
	s647SAName   = "snowplow"
	s647SANS     = "krateo-system"
	s647SAServer = "https://kubernetes.default.svc"
)

// s647RAObject is a RESTAction whose userAccessFilter names l262TargetGVR — the
// static target the resident enumeration must derive.
func s647RAObject(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": name, "namespace": h1NS},
		"spec": map[string]any{
			"api": []any{map[string]any{
				"name": "items",
				"path": "/apis/" + l262TargetGVR.Group + "/" + l262TargetGVR.Version + "/" + l262TargetGVR.Resource,
				"userAccessFilter": map[string]any{
					"verb": "list", "group": l262TargetGVR.Group, "resource": l262TargetGVR.Resource,
				},
			}},
		},
	}}
}

// s647ExtraObjs: the M RESTAction objects the harness's nav widgets apiRef, and
// the snowplow SA's binding to the reader role (its token's `sub` is the only
// way the code under test learns the SA name — phase1RealSAToken).
func s647ExtraObjs() []runtime.Object {
	out := []runtime.Object{&rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "s647-sa", UID: types.UID("uid-s647-sa")},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: s647SAName, Namespace: s647SANS}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "l262-reader"},
	}}
	for i := 0; i < s647RAs; i++ {
		out = append(out, s647RAObject("l262-ra-"+itoaLine(i)))
	}
	return out
}

// s647Env builds the harness with the RA objects served from a synced informer,
// the production deps (SA endpoint carrying the projected-token shape) and the
// production target-GVR lookup.
func s647Env(t *testing.T, o l262Opts) *l262Env {
	t.Helper()
	o.ras = s647RAs
	o.extraObjs = append(o.extraObjs, s647ExtraObjs()...)
	env := l262Setup(t, o)
	l262Stubs(t, l262StubOpts{})
	// The lookup under test is the production one.
	restActionTargetGVRFn = restActionTargetGVR

	env.rw.EnsureResourceType(h1RAGVR)
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := env.rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync(restactions): %v", err)
	}
	cache.RebuildRBACSnapshotForTest(env.rw)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1WidgetGVR, h1RAGVR, l262TargetGVR})

	env.deps.saEP = endpoints.Endpoint{ServerURL: s647SAServer, Token: phase1RealSAToken}
	if len(env.ras) != s647RAs {
		t.Fatalf("fixture: %d harvested RESTActions, want %d", len(env.ras), s647RAs)
	}
	// PRECONDITION: the fixture is servable to the SA — the lookup CAN succeed
	// when it carries the SA identity (so a RED below is the missing identity,
	// not a broken fixture).
	saCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserConfig(env.deps.saEP),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "system:serviceaccount:" + s647SANS + ":" + s647SAName}))
	for _, ref := range env.ras {
		got := objects.Get(saCtx, ref)
		if got.Err != nil || got.Unstructured == nil {
			t.Fatalf("PRECONDITION: the SA-identity GET of %s/%s is not served (err=%v)", ref.Namespace, ref.Name, got.Err)
		}
	}
	return env
}

// s647RotatedGroups fires the production hook chain with the K cohort groups
// rotated and returns the set the #258 handler receives.
func s647RotatedGroups(t *testing.T, groups ...string) cache.RotatedSubjectSet {
	t.Helper()
	cache.ResetRBACShiftHooksForTest()
	t.Cleanup(cache.ResetRBACShiftHooksForTest)
	var rs cache.RotatedSubjectSet
	captured := false
	cache.RegisterRBACShiftHook(func(r cache.RotatedSubjectSet) {
		if !captured {
			rs, captured = r, true
		}
	})
	subs := make([]cache.RotatedSubject, 0, len(groups))
	for _, g := range groups {
		subs = append(subs, cache.RotatedSubject{Kind: "Group", Name: g})
	}
	cache.NotifyRBACShiftForTest(subs)
	if !captured || rs.Len() != len(groups) {
		t.Fatalf("PRECONDITION: the hook did not deliver the %d-group rotated set", len(groups))
	}
	return rs
}

func s647DropsByReason() map[string]uint64 { return residentUnitsDroppedSnapshot() }

// Arm A — the REAL #258 handler: every (RA × rotated cohort) is reseeded.
func TestS647_RBACShiftReseedEnumeratesAndSeedsEveryRA(t *testing.T) {
	env := s647Env(t, l262Opts{groupCohorts: []string{s647CohortA, s647CohortB}})
	knowEveryRepresentative(t, h1WidgetGVR, h1RAGVR, l262TargetGVR) // #841: a known requester per representative
	rotated := s647RotatedGroups(t, s647CohortA, s647CohortB)
	before := s647DropsByReason()

	for _, g := range []string{s647CohortA, s647CohortB} {
		if _, _, rw, rn := l262WarmByKind(t, env, l262CustomerCtx("", []string{g})); rw != 0 || rn != s647RAs {
			t.Fatalf("PRECONDITION: cohort %s holds %d/%d warm RESTAction cells before the reseed, want 0/%d", g, rw, rn, s647RAs)
		}
	}

	if err := rePrewarmRBACShift(context.Background(), env.deps, rotated); err != nil {
		t.Fatalf("rePrewarmRBACShift: %v", err)
	}

	for _, g := range []string{s647CohortA, s647CohortB} {
		ww, wn, rw, rn := l262WarmByKind(t, env, l262CustomerCtx("", []string{g}))
		if ww != wn {
			t.Fatalf("cohort %s: %d/%d widget cells warm after the RBAC-shift reseed", g, ww, wn)
		}
		if rw != rn {
			t.Fatalf("#647 RED: cohort %s finds %d/%d RESTAction cells warm after the RBAC-shift reseed — the "+
				"resident enumeration dropped the RA units (target-GVR lookup on a ctx with no identity)", g, rw, rn)
		}
	}
	if d := s647DropsByReason()[residentDropGetFailed] - before[residentDropGetFailed]; d != 0 {
		t.Fatalf("resident enumeration recorded %d get_failed drops on a fixture where every RA is servable to the SA", d)
	}
}

// Arm B — the #258 rotated-target filter (reseed_engine.go) yields M × K RA
// requests.
func TestS647_RotatedResidentTargetsIncludeEveryRA(t *testing.T) {
	env := s647Env(t, l262Opts{groupCohorts: []string{s647CohortA, s647CohortB}})
	knowEveryRepresentative(t, h1WidgetGVR, h1RAGVR, l262TargetGVR) // #841: a known requester per representative
	rotated := s647RotatedGroups(t, s647CohortA, s647CohortB)

	reqs := enumerateRotatedResidentTargets(context.Background(), env.deps, rotated)
	pairs := map[string]bool{}
	for _, r := range reqs {
		if r.isWidget {
			continue
		}
		for _, g := range r.identity.Groups {
			pairs[r.ra.Name+"|"+g] = true
		}
	}
	for _, ref := range env.ras {
		for _, g := range []string{s647CohortA, s647CohortB} {
			if !pairs[ref.Name+"|"+g] {
				t.Fatalf("#647 RED: the rotated resident targets have no (%s × %s) request (RA requests: %d) — "+
					"the RA unit was dropped at enumeration", ref.Name, g, len(pairs))
			}
		}
	}
}

// Arm C — the REAL learned-class handler. K=2 users log in with a new group
// set (the F3 shape: a clientconfig Secret UPDATE), which makes each a NEW
// learned class; the handler must seed each across all M RAs.
func TestS647_LearnedClassSeedEnumeratesAndSeedsEveryRA(t *testing.T) {
	users := map[string]string{"s647-dave": s647CohortA, "s647-frank": s647CohortB}
	secrets := make([]*corev1.Secret, 0, len(users))
	for u := range users {
		secrets = append(secrets, l262ClientconfigSecret(t, u+"-clientconfig", u, []string{l262Group}, time.Now().Add(-time.Hour), "1"))
	}
	env := s647Env(t, l262Opts{groupCohorts: []string{s647CohortA, s647CohortB}, secrets: secrets})
	l262WaitLearned(t, len(users))
	// The boot pass (under the SA, as production) measures the unit costs the
	// learned bound admits against; the group-only users are not distinct yet.
	if err := seedScopeYielding(withPhase1SAContext(context.Background(), env.deps.saEP, env.deps.saRC), env.ras, env.widgets,
		env.deps.saEP, nil, l262AuthnNS, seedModeBoot); err != nil {
		t.Fatalf("boot seed: %v", err)
	}
	_ = cache.DrainPendingLearnedClasses()

	// THE LOGINS: each user is re-issued a clientconfig with an extra group.
	for u, g := range users {
		upd := l262ClientconfigSecret(t, u+"-clientconfig", u, []string{l262Group, g}, time.Now(), "2")
		if _, err := env.kube.CoreV1().Secrets(l262AuthnNS).Update(context.Background(), upd, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(l262LearnedTargets(l262TargetGVR)) < len(users) {
		if time.Now().After(deadline) {
			t.Fatalf("the logins never produced %d learned classes (got %v)", len(users), l262LearnedTargets(l262TargetGVR))
		}
		time.Sleep(5 * time.Millisecond)
	}
	before := s647DropsByReason()
	for u, g := range users {
		if _, _, rw, _ := l262WarmByKind(t, env, l262CustomerCtx(u, []string{l262Group, g})); rw != 0 {
			t.Fatalf("PRECONDITION: %s holds %d warm RESTAction cells under the new group set before the learned seed", u, rw)
		}
	}

	if err := rePrewarmLearnedClasses(context.Background(), env.deps); err != nil {
		t.Fatalf("rePrewarmLearnedClasses: %v", err)
	}

	for u, g := range users {
		ww, wn, rw, rn := l262WarmByKind(t, env, l262CustomerCtx(u, []string{l262Group, g}))
		if ww != wn {
			t.Fatalf("%s: %d/%d widget cells warm after the learned-class seed", u, ww, wn)
		}
		if rw != rn || rn != s647RAs {
			t.Fatalf("#647 RED: learned class %s finds %d/%d RESTAction cells warm after the learned-class seed — "+
				"the resident enumeration dropped the RA units", u, rw, rn)
		}
	}
	if d := s647DropsByReason()[residentDropGetFailed] - before[residentDropGetFailed]; d != 0 {
		t.Fatalf("resident enumeration recorded %d get_failed drops on a fixture where every RA is servable to the SA", d)
	}
}

// Arm D — the fix did NOT loosen the informer serve: a request-scoped ctx with
// no identity (and one with an identity holding no grant) is never served the
// informer object, while the SA identity is.
func TestS647_ObjectsGetStillFailsClosedWithoutIdentity(t *testing.T) {
	env := s647Env(t, l262Opts{})
	ref := env.ras[0]

	req := httptest.NewRequest("GET", "/call", nil)
	noIdentity := xcontext.BuildContext(req.Context())
	if _, err := xcontext.UserInfo(noIdentity); err == nil {
		t.Fatalf("PRECONDITION: the request-scoped ctx must carry no identity")
	}
	if got := objects.Get(noIdentity, ref); got.Err == nil || got.Unstructured != nil {
		t.Fatalf("FAIL-OPEN: objects.Get served %s/%s to a request ctx with NO identity (err=%v)", ref.Namespace, ref.Name, got.Err)
	}
	if _, miss := restActionTargetGVR(noIdentity, ref); miss != residentDropGetFailed {
		t.Fatalf("restActionTargetGVR on a no-identity ctx: miss=%q, want %q", miss, residentDropGetFailed)
	}

	stranger := xcontext.BuildContext(req.Context(), xcontext.WithUserInfo(jwtutil.UserInfo{Username: "s647-stranger"}))
	if got := objects.Get(stranger, ref); got.Err == nil || got.Unstructured != nil {
		t.Fatalf("FAIL-OPEN: objects.Get served %s/%s to an identity with no grant (err=%v)", ref.Namespace, ref.Name, got.Err)
	}

	// The enumeration's identity hop is the SA's, built from the deps alone.
	saCtx := withSAIdentityContext(context.Background(), env.deps.saEP, env.deps.saRC)
	gvr, miss := restActionTargetGVR(saCtx, ref)
	if miss != raTargetFound || gvr.Group != l262TargetGVR.Group || gvr.Resource != l262TargetGVR.Resource {
		t.Fatalf("SA identity ctx: target=%v miss=%q, want %v found", gvr, miss, l262TargetGVR)
	}
}

// Arm E — the detector: on a raw identity the lookup fails; a drop is counted
// by reason, and an RA with no static userAccessFilter target is counted under
// its own reason.
func TestS647_ResidentDropCounterByReason(t *testing.T) {
	env := s647Env(t, l262Opts{})
	before := s647DropsByReason()

	// get_failed: the SA token carries no canonical SA subject, so the hop has
	// no identity to install and the fail-closed filter refuses every RA.
	deps := env.deps
	deps.saEP = endpoints.Endpoint{ServerURL: s647SAServer}
	units := enumerateResident(context.Background(), deps, false)
	for _, u := range units {
		if !u.isWidget {
			t.Fatalf("an RA unit was enumerated with no identity to look it up under: %s", u.ra.Name)
		}
	}
	if d := s647DropsByReason()[residentDropGetFailed] - before[residentDropGetFailed]; d != s647RAs {
		t.Fatalf("get_failed drops = %d, want %d (one per RA)", d, s647RAs)
	}

	// no_static_target: an RA without a userAccessFilter.
	noTarget := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version, "kind": "RESTAction",
		"metadata": map[string]any{"name": "l262-ra-0", "namespace": h1NS},
		"spec":     map[string]any{"api": []any{map[string]any{"name": "x", "path": "/api/v1/namespaces"}}},
	}}
	// #660: the RA the poll watches must be the one Updated. env.ras is the
	// harvested set in NO fixed order, so env.ras[0] was l262-ra-1 in about 1
	// run in 5 under load: the poll then waited on an object nobody changed,
	// and no deadline could save it (measured: observed within ms, or never
	// within 90s). Also wait for the RESTAction watch to be established on
	// the fake, so the Update cannot fall into its list/watch gap.
	targetIdx := -1
	for i, ref := range env.ras {
		if ref.Namespace == h1NS && ref.Name == "l262-ra-0" {
			targetIdx = i
		}
	}
	if targetIdx < 0 {
		t.Fatalf("fixture: no harvested RESTAction %s/l262-ra-0 in %v", h1NS, env.ras)
	}
	target := env.ras[targetIdx]
	if err := env.watches.WaitFor(time.Minute, h1RAGVR.Resource); err != nil {
		t.Fatal(err)
	}
	if _, err := env.dyn.Resource(h1RAGVR).Namespace(h1NS).Update(context.Background(), noTarget, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Minute) // event-driven; generous only for a starved informer
	for {
		saCtx := withSAIdentityContext(context.Background(), env.deps.saEP, env.deps.saRC)
		if _, miss := restActionTargetGVR(saCtx, target); miss == residentDropNoStaticTarget {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the informer never observed the target-less RESTAction")
		}
		time.Sleep(10 * time.Millisecond)
	}
	before = s647DropsByReason()
	_ = enumerateResident(context.Background(), env.deps, false)
	after := s647DropsByReason()
	if d := after[residentDropNoStaticTarget] - before[residentDropNoStaticTarget]; d != 1 {
		t.Fatalf("no_static_target drops = %d, want 1", d)
	}
	if d := after[residentDropGetFailed] - before[residentDropGetFailed]; d != 0 {
		t.Fatalf("get_failed drops = %d under the SA identity, want 0", d)
	}
}
