// shadow_parity_hook_test.go — v7 Step 2D falsifiers for the DARK shadow-parity
// hook wired onto the LIVE EvaluateRBAC path.
//
// These drive the REAL rbac.EvaluateRBAC over a REAL published RBAC snapshot
// (dynamicfake + cache.NewResourceWatcher — no cluster, no network), with the
// dispatchers hook registered via this package's init(). The toggle is turned on
// per-test and reset in cleanup; production/other tests run with it OFF (dark).
//
// RED-first control matrix (each arm goes RED under exactly its one defect,
// GREEN otherwise; single-defect controls between arms):
//
//	F-D6(a)  — the hook fires on exactly the right returns (memo permit, walk
//	           permit, walk deny) and NOT on cache-off / nil-snap / error / no-ctx
//	           / toggle-off.
//	F-D6(b)  — the named-return refactor + hook-on are verdict+UID byte-identical
//	           to hook-off (the dark proof).
//	F-verdict— a divergent R makes verdict_mismatch_total fire; the real R keeps
//	           it zero.
//	F-coverage— a check whose coordinate is in NO class of D (the ${._getpath}
//	           external-classified deriver output) makes coverage_miss_total fire;
//	           a covering D keeps it zero.
//	F-proj   — the live agents get-UAF cell is classified NOT shareable, so the
//	           name-ambiguous leak counter stays zero; forcing the cell shareable
//	           makes it fire.
//	F-panic  — a panic in the hook body AND in the inline D-derivation bumps
//	           dark_panic_total, leaves the live return unchanged, no crash; a ctx
//	           type-collision comma-oks cleanly.
//	F-2mutant— two defects in one run fire two counters independently; single-
//	           defect controls fire exactly one.
//	F-H5     — checks_total == the INDEPENDENT rbac.EvaluateRBACCallCount() over a
//	           served-only hermetic window.
//	F-seed   — the seed/refresher shape (no shadow context) no-ops the hook —
//	           the design's deliberate exclusion of the populate path (§4.3).

package dispatchers

import (
	"context"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	authv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// ───────────────────────────── harness ─────────────────────────────

const (
	spGroup = "portal" // the group the test users present
	spKAG   = "kagent.dev"
)

// shadowWatcher publishes an RBAC snapshot: a ClusterRole granting get/list on
// core pods and kagent.dev agents to Group:portal, so any portal user is
// permitted those reads cluster-wide. Cache-on. t.Cleanup restores globals.
func shadowWatcher(t testing.TB) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	rbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	rGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		crbGVR: "ClusterRoleBindingList",
		crGVR:  "ClusterRoleList",
		rbGVR:  "RoleBindingList",
		rGVR:   "RoleList",
	}

	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "reader"},
			Rules: []rbacv1.PolicyRule{
				{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{"pods"}},
				{Verbs: []string{"get", "list"}, APIGroups: []string{spKAG}, Resources: []string{"agents"}},
			},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "reader-bind"},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: spGroup}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "reader"},
		},
	}

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	wctx, wcancel := context.WithCancel(context.Background())
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

	rbac.ResetRequesterProfileMemoForTest()
	resetShadowCounters()
	rbac.SetShadowParityEnabled(true)

	t.Cleanup(func() {
		rbac.SetShadowParityEnabled(false)
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
		rbac.ResetRequesterProfileMemoForTest()
	})
}

func resetShadowCounters() {
	shadowChecksTotal.Store(0)
	shadowVerdictMismatchTotal.Store(0)
	shadowCoverageMissTotal.Store(0)
	shadowProjectionNameAmbiguousLeakTotal.Store(0)
	shadowDarkPanicTotal.Store(0)
}

type shadowCounters struct{ checks, mismatch, coverage, leak, panic uint64 }

func snapCounters() shadowCounters {
	return shadowCounters{
		checks:   shadowChecksTotal.Load(),
		mismatch: shadowVerdictMismatchTotal.Load(),
		coverage: shadowCoverageMissTotal.Load(),
		leak:     shadowProjectionNameAmbiguousLeakTotal.Load(),
		panic:    shadowDarkPanicTotal.Load(),
	}
}

// idCtx installs the requester identity (UserInfo) on a fresh ctx.
func idCtx(user string, groups ...string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: groups}))
}

// driveCheck installs a hand-built shadow context whose identity MATCHES the
// request identity (the production invariant — same request), then calls the
// real EvaluateRBAC. Returns the verdict and the shadow context (to read its
// untrusted flag). opts.Username/Groups are forced to the given identity.
func driveCheck(t *testing.T, user string, groups []string, d AccessDomain, opts rbac.EvaluateOptions) (bool, error, *shadowContext) {
	t.Helper()
	sc := &shadowContext{
		domain:    d,
		identity:  rbac.EvaluateOptions{Username: user, Groups: groups},
		shareable: Shareable(d),
	}
	ctx := context.WithValue(idCtx(user, groups...), shadowCtxKey, sc)
	opts.Username = user
	opts.Groups = groups
	allowed, _, err := rbac.EvaluateRBAC(ctx, opts)
	return allowed, err, sc
}

// coverGetAgents is a domain that covers `get kagent.dev/agents` (the live UAF
// get shape) as a name-free namespace-set class.
func coverGetAgents() AccessDomain {
	return domain([]AccessClass{{Kind: ClassNamespaceSet, Verb: "get", Group: spKAG, Resource: "agents"}}, nil)
}

// coverGetPods covers `get core/pods` as a name-pinned exact class (shareable).
func coverGetPods(name string) AccessDomain {
	return domain([]AccessClass{{Kind: ClassExact, Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: name}}, nil)
}

// ───────────────────────── F-D6(a) — fires on the right returns ─────────────────────────

func TestFD6a_HookFiresOnMemoPermitWalkPermitAndDeny(t *testing.T) {
	shadowWatcher(t)
	d := coverGetPods("p1")
	opts := rbac.EvaluateOptions{Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1"}

	// (1) permit via walk — first call, memo cold.
	resetShadowCounters()
	allowed, err, _ := driveCheck(t, "alice", []string{spGroup}, d, opts)
	if err != nil || !allowed {
		t.Fatalf("walk permit: allowed=%v err=%v (alice should be permitted)", allowed, err)
	}
	if c := snapCounters(); c.checks != 1 || c.mismatch != 0 || c.coverage != 0 || c.leak != 0 {
		t.Fatalf("walk permit counters = %+v, want checks=1 rest=0", c)
	}

	// (2) permit via memo — same opts again → authz memo hit, hook still fires.
	resetShadowCounters()
	allowed, err, _ = driveCheck(t, "alice", []string{spGroup}, d, opts)
	if err != nil || !allowed {
		t.Fatalf("memo permit: allowed=%v err=%v", allowed, err)
	}
	if c := snapCounters(); c.checks != 1 {
		t.Fatalf("memo permit counters = %+v, want checks=1 (hook must fire on the memo-hit return)", c)
	}

	// (3) deny via walk — a user with no groups is denied; R(nobody) also denies,
	// so verdict matches (no mismatch) but the hook STILL fires.
	resetShadowCounters()
	allowed, err, _ = driveCheck(t, "nobody", nil, d, opts)
	if err != nil || allowed {
		t.Fatalf("walk deny: allowed=%v err=%v (nobody should be denied)", allowed, err)
	}
	if c := snapCounters(); c.checks != 1 || c.mismatch != 0 {
		t.Fatalf("walk deny counters = %+v, want checks=1 mismatch=0", c)
	}
}

func TestFD6a_HookDoesNotFire_ToggleOff_NoCtx_ErrPath(t *testing.T) {
	shadowWatcher(t)
	d := coverGetPods("p1")
	opts := rbac.EvaluateOptions{Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1"}

	// toggle OFF (default-dark): even with a shadow ctx + a permit, no fire.
	rbac.SetShadowParityEnabled(false)
	resetShadowCounters()
	if _, err, _ := driveCheck(t, "alice", []string{spGroup}, d, opts); err != nil {
		t.Fatalf("toggle-off drive: %v", err)
	}
	if c := snapCounters(); c.checks != 0 {
		t.Fatalf("toggle-off must not fire: %+v", c)
	}
	rbac.SetShadowParityEnabled(true)

	// no shadow ctx on the check (the dispatch-gate / seed shape) → no fire.
	resetShadowCounters()
	o := opts
	o.Username = "alice"
	o.Groups = []string{spGroup}
	if _, _, err := rbac.EvaluateRBAC(idCtx("alice", spGroup), o); err != nil {
		t.Fatalf("no-ctx drive: %v", err)
	}
	if c := snapCounters(); c.checks != 0 {
		t.Fatalf("no shadow ctx must not fire: %+v", c)
	}

	// error path — cache=on but Global()==nil → EvaluateRBAC returns err → no fire.
	resetShadowCounters()
	prev := cache.Global()
	cache.SetGlobal(nil)
	sc := &shadowContext{domain: d, identity: rbac.EvaluateOptions{Username: "alice", Groups: []string{spGroup}}, shareable: Shareable(d)}
	ctx := context.WithValue(idCtx("alice", spGroup), shadowCtxKey, sc)
	if _, _, err := rbac.EvaluateRBAC(ctx, o); err == nil {
		t.Fatalf("expected an error with Global()==nil")
	}
	cache.SetGlobal(prev)
	if c := snapCounters(); c.checks != 0 {
		t.Fatalf("error path must not fire: %+v", c)
	}
}

// TestFD6a_HookDoesNotFire_CacheOff isolates the snap==nil (cache-off) return
// (err==nil, served via the SAR baseline). The hook must no-op on snap==nil.
func TestFD6a_HookDoesNotFire_CacheOff(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("TEST_MODE", "true")
	rbac.SetShadowParityEnabled(true)
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })

	cs := k8sfake.NewSimpleClientset()
	cs.PrependReactor("create", "selfsubjectaccessreviews",
		func(ktesting.Action) (bool, runtime.Object, error) {
			return true, &authv1.SelfSubjectAccessReview{
				Status: authv1.SubjectAccessReviewStatus{Allowed: true},
			}, nil
		})
	restore := rbac.SetSARClientsetForTest(func(context.Context, endpoints.Endpoint) (kubernetes.Interface, error) {
		return cs, nil
	})
	t.Cleanup(restore)

	resetShadowCounters()
	d := coverGetPods("p1")
	sc := &shadowContext{domain: d, identity: rbac.EvaluateOptions{Username: "alice"}, shareable: Shareable(d)}
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice"}),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: "https://sar.test"}))
	ctx = context.WithValue(ctx, shadowCtxKey, sc)
	allowed, _, err := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
		Username: "alice", Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1",
	})
	if err != nil || !allowed {
		t.Fatalf("cache-off SAR permit: allowed=%v err=%v", allowed, err)
	}
	if c := snapCounters(); c.checks != 0 {
		t.Fatalf("cache-off (snap==nil) must not fire: %+v", c)
	}
}

// ───────────────────────── F-verdict (RED-first) ─────────────────────────

func TestFVerdict_MismatchFiresOnDivergentR(t *testing.T) {
	shadowWatcher(t)
	d := coverGetPods("p1")
	opts := rbac.EvaluateOptions{Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1"}

	// GREEN: the real R agrees with EvaluateRBAC → no mismatch.
	resetShadowCounters()
	driveCheck(t, "alice", []string{spGroup}, d, opts)
	if c := snapCounters(); c.mismatch != 0 {
		t.Fatalf("real R must not mismatch: %+v", c)
	}

	// RED: a divergent R that denies everything, while EvaluateRBAC permits →
	// verdict_mismatch fires, and ONLY that counter (coverage covers, no leak).
	prev := shadowProfileFor
	shadowProfileFor = func(*cache.RBACSnapshot, rbac.EvaluateOptions) *rbac.RequesterProfile {
		return &rbac.RequesterProfile{NamespacedRules: map[string][]rbacv1.PolicyRule{}}
	}
	t.Cleanup(func() { shadowProfileFor = prev })
	resetShadowCounters()
	_, _, sc := driveCheck(t, "alice", []string{spGroup}, d, opts)
	if c := snapCounters(); c.mismatch != 1 || c.coverage != 0 || c.leak != 0 {
		t.Fatalf("divergent R counters = %+v, want mismatch=1 rest=0 (single-defect)", c)
	}
	if !sc.untrusted.Load() {
		t.Fatalf("a verdict mismatch must mark the resolve digest untrusted")
	}
}

// ───────────────────────── F-coverage (RED-first) incl ${._getpath} ─────────────────────────

func TestFCoverage_MissFiresOnGetpathExternalDomain(t *testing.T) {
	shadowWatcher(t)
	opts := rbac.EvaluateOptions{Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1"}

	// GREEN: a domain covering the check → no coverage miss.
	resetShadowCounters()
	driveCheck(t, "alice", []string{spGroup}, coverGetPods("p1"), opts)
	if c := snapCounters(); c.coverage != 0 {
		t.Fatalf("covered check must not miss: %+v", c)
	}

	// RED: the ${._getpath} shape — an api-step whose WHOLE path is produced
	// inside ${…}. The REAL deriver classifies it external → D has NO class →
	// any real check registers a coverage miss (the #265 runtime blind-spot
	// catch), while the verdict still matches (no mismatch).
	getpath := "${.getpath}"
	ra := &templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{API: []*templatesv1.API{
		{Name: "s1", Path: getpath, Verb: strptr("GET")},
	}}}
	d := DeriveRESTActionAccessDomain(ra, NilChainResolver)
	if len(d.Classes) != 0 {
		t.Fatalf("F-coverage precondition: ${._getpath} deriver must yield NO class, got %d: %v", len(d.Classes), d)
	}
	resetShadowCounters()
	_, _, sc := driveCheck(t, "alice", []string{spGroup}, d, opts)
	if c := snapCounters(); c.coverage != 1 || c.mismatch != 0 || c.leak != 0 {
		t.Fatalf("getpath-external counters = %+v, want coverage=1 rest=0 (single-defect)", c)
	}
	if !sc.untrusted.Load() {
		t.Fatalf("a coverage miss must mark the resolve digest untrusted")
	}
}

// ───────────────────────── F-proj (name-ambiguous leak) ─────────────────────────

func TestFProj_AgentsGetUAFCellIsNotShareableAndLeakStaysZero(t *testing.T) {
	shadowWatcher(t)

	// The live agents get-UAF cell: one UAF stanza {get, kagent.dev, agents}.
	ra := &templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{API: []*templatesv1.API{
		{Name: "list-agents", Path: "/apis/kagent.dev/v1/agents", Verb: strptr("GET"),
			UserAccessFilter: &templatesv1.UserAccessFilterSpec{Verb: "get", Group: spKAG, Resource: "agents"}},
	}}}
	d := DeriveRESTActionAccessDomain(ra, NilChainResolver)

	// Primary F-proj assertion: the cell is structurally NOT shareable (§2.4).
	if Shareable(d) {
		t.Fatalf("F-proj: the agents get-UAF cell must be classified NOT shareable, got shareable; D=%v", d)
	}

	// A name-carrying refilter check (the per-object get with a real Name).
	opts := rbac.EvaluateOptions{Verb: "get", Group: spKAG, Resource: "agents", Namespace: "t-a", Name: "agent-x"}

	// GREEN: not-shareable cell → the runtime leak counter stays zero for BOTH
	// identities (their would-be digests are never treated as shareable).
	resetShadowCounters()
	driveCheck(t, "alice", []string{spGroup}, d, opts)
	driveCheck(t, "bob", []string{spGroup}, d, opts)
	if c := snapCounters(); c.leak != 0 {
		t.Fatalf("not-shareable agents cell must keep the leak counter zero: %+v", c)
	}

	// RED: a broken classification that marks this name-ambiguous cell shareable.
	// The name-carrying check is then covered ONLY by the name-free nsSet class →
	// the runtime leak counter FIRES (the empirical proof §2.4 must hold).
	resetShadowCounters()
	sc := &shadowContext{domain: d, identity: rbac.EvaluateOptions{Username: "alice", Groups: []string{spGroup}}, shareable: true}
	ctx := context.WithValue(idCtx("alice", spGroup), shadowCtxKey, sc)
	o := opts
	o.Username = "alice"
	o.Groups = []string{spGroup}
	if _, _, err := rbac.EvaluateRBAC(ctx, o); err != nil {
		t.Fatalf("F-proj RED drive: %v", err)
	}
	if c := snapCounters(); c.leak != 1 {
		t.Fatalf("forced-shareable name-ambiguous cell must fire the leak counter once: %+v", c)
	}
	if !sc.untrusted.Load() {
		t.Fatalf("a projection leak must mark the resolve digest untrusted")
	}
}

// ───────────────────────── F-panic (both carriers + ctx collision) ─────────────────────────

func TestFPanic_HookBodyPanicIsRecoveredAndVerdictUnchanged(t *testing.T) {
	shadowWatcher(t)
	d := coverGetPods("p1")
	o := rbac.EvaluateOptions{Username: "alice", Groups: []string{spGroup}, Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1"}

	// Baseline verdict with the toggle OFF (hook never runs).
	rbac.SetShadowParityEnabled(false)
	a0, u0, e0 := rbac.EvaluateRBAC(idCtx("alice", spGroup), o)
	rbac.SetShadowParityEnabled(true)

	// Inject a panic in the hook BODY. It must be recovered (dark_panic_total++),
	// the digest marked untrusted, and the live verdict/UID byte-identical.
	shadowHookPanicForTest.Store(true)
	t.Cleanup(func() { shadowHookPanicForTest.Store(false) })
	resetShadowCounters()
	sc := &shadowContext{domain: d, identity: rbac.EvaluateOptions{Username: "alice", Groups: []string{spGroup}}, shareable: Shareable(d)}
	ctx := context.WithValue(idCtx("alice", spGroup), shadowCtxKey, sc)
	a1, u1, e1 := rbac.EvaluateRBAC(ctx, o)
	if a1 != a0 || u1 != u0 || (e1 == nil) != (e0 == nil) {
		t.Fatalf("hook-body panic changed the live return: (%v,%q,%v) != baseline (%v,%q,%v)", a1, u1, e1, a0, u0, e0)
	}
	if c := snapCounters(); c.panic != 1 {
		t.Fatalf("hook-body panic must bump dark_panic_total exactly once: %+v", c)
	}
	if !sc.untrusted.Load() {
		t.Fatalf("a recovered hook panic must mark the digest untrusted")
	}
	shadowHookPanicForTest.Store(false)
}

func TestFPanic_InlineDDerivationPanicIsRecovered(t *testing.T) {
	shadowWatcher(t)
	shadowDerivePanicForTest.Store(true)
	t.Cleanup(func() { shadowDerivePanicForTest.Store(false) })
	resetShadowCounters()

	base := idCtx("alice", spGroup)
	ra := &templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{API: []*templatesv1.API{
		{Name: "s1", Path: "/api/v1/pods", Verb: strptr("GET")},
	}}}
	// Must NOT panic; must return the ORIGINAL ctx (no shadow ctx installed).
	out := installShadowParityRESTAction(base, ra)
	if v := out.Value(shadowCtxKey); v != nil {
		t.Fatalf("a panicking D-derivation must leave NO shadow context on ctx, got %v", v)
	}
	if c := snapCounters(); c.panic != 1 {
		t.Fatalf("inline D-derivation panic must bump dark_panic_total once: %+v", c)
	}
	shadowDerivePanicForTest.Store(false)
}

func TestFPanic_CtxTypeCollisionCommaOksCleanly(t *testing.T) {
	shadowWatcher(t)
	resetShadowCounters()
	// A FOREIGN value under the shadow key: the comma-ok read must return cleanly
	// (no panic, no fire) — never a bare assertion.
	ctx := context.WithValue(idCtx("alice", spGroup), shadowCtxKey, "not-a-shadow-context")
	o := rbac.EvaluateOptions{Username: "alice", Groups: []string{spGroup}, Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1"}
	if _, _, err := rbac.EvaluateRBAC(ctx, o); err != nil {
		t.Fatalf("ctx-collision drive: %v", err)
	}
	if c := snapCounters(); c.checks != 0 || c.panic != 0 {
		t.Fatalf("ctx type-collision must comma-ok cleanly (no fire, no panic): %+v", c)
	}
}

// ───────────────────────── F-2mutant (two-mutant control) ─────────────────────────

func TestF2Mutant_TwoDefectsFireTwoCountersIndependently(t *testing.T) {
	shadowWatcher(t)
	covered := coverGetPods("p1")
	// The ${._getpath} external domain (no class).
	empty := DeriveRESTActionAccessDomain(&templatesv1.RESTAction{Spec: templatesv1.RESTActionSpec{
		API: []*templatesv1.API{{Name: "s1", Path: "${.getpath}", Verb: strptr("GET")}},
	}}, NilChainResolver)
	opts := rbac.EvaluateOptions{Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1"}

	denyAll := func(*cache.RBACSnapshot, rbac.EvaluateOptions) *rbac.RequesterProfile {
		return &rbac.RequesterProfile{NamespacedRules: map[string][]rbacv1.PolicyRule{}}
	}
	orig := shadowProfileFor

	// Control A — only the R defect (covered domain): mismatch only.
	shadowProfileFor = denyAll
	resetShadowCounters()
	driveCheck(t, "alice", []string{spGroup}, covered, opts)
	if c := snapCounters(); c.mismatch != 1 || c.coverage != 0 {
		t.Fatalf("control-A (R defect only) = %+v, want mismatch=1 coverage=0", c)
	}
	shadowProfileFor = orig

	// Control B — only the coverage defect (correct R): coverage only.
	resetShadowCounters()
	driveCheck(t, "alice", []string{spGroup}, empty, opts)
	if c := snapCounters(); c.coverage != 1 || c.mismatch != 0 {
		t.Fatalf("control-B (coverage defect only) = %+v, want coverage=1 mismatch=0", c)
	}

	// Both defects in one run: BOTH counters fire, independently.
	shadowProfileFor = denyAll
	t.Cleanup(func() { shadowProfileFor = orig })
	resetShadowCounters()
	driveCheck(t, "alice", []string{spGroup}, empty, opts)
	if c := snapCounters(); c.mismatch != 1 || c.coverage != 1 {
		t.Fatalf("two-mutant run = %+v, want mismatch=1 AND coverage=1", c)
	}
	shadowProfileFor = orig
}

// ───────────────────────── F-H5 (independent denominator) ─────────────────────────

func TestFH5_ChecksTotalEqualsIndependentEvaluateRBACCount(t *testing.T) {
	shadowWatcher(t)
	d := coverGetPods("") // name-free exact get pods (covers any name at (get,core,pods))
	// A served-only hermetic window: every EvaluateRBAC call below carries a
	// shadow context, and nothing else calls EvaluateRBAC. The RHS instrument
	// (rbac.EvaluateRBACCallCount, bumped at the TOP of EvaluateRBAC — a
	// different site, package, and derivation than checks_total) is therefore the
	// served-path count.
	rbac.ResetEvaluateRBACCallCount()
	resetShadowCounters()

	const n = 25
	for i := 0; i < n; i++ {
		opts := rbac.EvaluateOptions{Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p"}
		if _, err, _ := driveCheck(t, "alice", []string{spGroup}, d, opts); err != nil {
			t.Fatalf("drive %d: %v", i, err)
		}
	}
	checks := shadowChecksTotal.Load()
	independent := rbac.EvaluateRBACCallCount()
	if checks != uint64(n) {
		t.Fatalf("checks_total=%d, want %d", checks, n)
	}
	if checks != independent {
		t.Fatalf("F-H5: checks_total=%d != independent EvaluateRBACCallCount=%d", checks, independent)
	}
}

// ───────────────────────── F-D6(b) / dark proof ─────────────────────────

func TestFD6b_VerdictAndUIDByteIdenticalHookOnVsOff(t *testing.T) {
	shadowWatcher(t)
	type probe struct {
		user   string
		groups []string
		opts   rbac.EvaluateOptions
	}
	probes := []probe{
		{"alice", []string{spGroup}, rbac.EvaluateOptions{Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1"}},
		{"alice", []string{spGroup}, rbac.EvaluateOptions{Verb: "list", Group: "", Resource: "pods", Namespace: "default"}},
		{"alice", []string{spGroup}, rbac.EvaluateOptions{Verb: "delete", Group: "", Resource: "pods", Namespace: "default", Name: "p1"}}, // deny
		{"nobody", nil, rbac.EvaluateOptions{Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1"}},                 // deny
		{"alice", []string{spGroup}, rbac.EvaluateOptions{Verb: "get", Group: spKAG, Resource: "agents", Namespace: "t-a", Name: "a1"}},
	}
	d := domain([]AccessClass{
		{Kind: ClassExact, Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1"},
		{Kind: ClassNamespaceSet, Verb: "list", Group: "", Resource: "pods"},
		{Kind: ClassNamespaceSet, Verb: "get", Group: spKAG, Resource: "agents"},
	}, nil)

	for i, p := range probes {
		o := p.opts
		o.Username = p.user
		o.Groups = p.groups

		// hook OFF.
		rbac.SetShadowParityEnabled(false)
		rbac.ResetRequesterProfileMemoForTest()
		a0, u0, e0 := rbac.EvaluateRBAC(idCtx(p.user, p.groups...), o)

		// hook ON with a shadow ctx.
		rbac.SetShadowParityEnabled(true)
		rbac.ResetRequesterProfileMemoForTest()
		sc := &shadowContext{domain: d, identity: rbac.EvaluateOptions{Username: p.user, Groups: p.groups}, shareable: Shareable(d)}
		ctx := context.WithValue(idCtx(p.user, p.groups...), shadowCtxKey, sc)
		a1, u1, e1 := rbac.EvaluateRBAC(ctx, o)

		if a1 != a0 || u1 != u0 || (e1 == nil) != (e0 == nil) {
			t.Fatalf("probe %d: hook-on changed the verdict/UID: (%v,%q,%v) != off (%v,%q,%v)", i, a1, u1, e1, a0, u0, e0)
		}
	}
}

// ───────────────────────── F-seed (populate path is out of scope) ─────────────────────────

func TestFSeed_SeedPathHasNoShadowCtxSoHookNoOps(t *testing.T) {
	shadowWatcher(t)
	resetShadowCounters()
	// The seed / prewarm / refresher populate path carries NO shadow context
	// (design §4.3 deliberately excludes it — it POPULATES the cache; it does not
	// determine a served byte, and it is the ~160M-hit CPU-dominant path). A
	// check on that shape must no-op the hook.
	o := rbac.EvaluateOptions{Username: "alice", Groups: []string{spGroup}, Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1"}
	if _, _, err := rbac.EvaluateRBAC(idCtx("alice", spGroup), o); err != nil {
		t.Fatalf("seed-shape drive: %v", err)
	}
	if c := snapCounters(); c.checks != 0 {
		t.Fatalf("a check with no shadow context (seed/refresher shape) must not fire: %+v", c)
	}
}

func strptr(s string) *string { return &s }

// ───────────────────────── PM condition 3 — unconditional defer overhead ─────────────────────────
//
// The dark defer fires on EVERY EvaluateRBAC call even when the toggle is OFF
// (it does one atomic.Bool.Load then returns). This benchmark measures the
// hot-path (authz-memo-hit) cost of EvaluateRBAC with the toggle OFF — the
// production posture — so a regression against the pre-refactor hot path is
// visible. It is an ABSOLUTE number (the pre-refactor binary is not available
// in-worktree for a delta); Step E runs the production A/B (toggle off, fresh
// pods) and a regression triggers the explicit-call fallback (two hook calls at
// the return sites instead of a defer).
func BenchmarkEvaluateRBAC_HotPath_ShadowToggleOff(b *testing.B) {
	shadowWatcher(b)
	rbac.SetShadowParityEnabled(false) // production posture: dark toggle OFF.

	// Quiet logger — EvaluateRBAC emits a per-call Debug line whose cost would
	// otherwise dominate and mask the defer overhead this benchmark isolates.
	quiet := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{spGroup}}),
		xcontext.WithLogger(quiet))
	o := rbac.EvaluateOptions{Username: "alice", Groups: []string{spGroup}, Verb: "get", Group: "", Resource: "pods", Namespace: "default", Name: "p1"}
	// Prime the authz memo so every timed iteration is a memo hit (the hot path).
	if _, _, err := rbac.EvaluateRBAC(ctx, o); err != nil {
		b.Fatalf("prime: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = rbac.EvaluateRBAC(ctx, o)
	}
}

// benchDeferToggle models EvaluateRBAC's dark defer in isolation (its exact
// toggle-off shape: one atomic.Bool.Load then return). The pair below measures
// the MARGINAL cost of the unconditional defer vs an identical function without
// it — the delta PM condition 3 asks for, which the pre/post-refactor A/B cannot
// give in-worktree.
var benchDeferToggle atomic.Bool

//go:noinline
func benchReturnWithDarkDefer() (a bool, s string, e error) {
	defer func() {
		if !benchDeferToggle.Load() {
			return
		}
	}()
	return true, "", nil
}

//go:noinline
func benchReturnNoDefer() (a bool, s string, e error) { return true, "", nil }

func BenchmarkDarkDefer_ToggleOff(b *testing.B) {
	benchDeferToggle.Store(false)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = benchReturnWithDarkDefer()
	}
}

func BenchmarkDarkDefer_NoDefer_Baseline(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = benchReturnNoDefer()
	}
}
