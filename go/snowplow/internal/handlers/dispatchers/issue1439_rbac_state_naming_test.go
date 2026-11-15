// issue1439_rbac_state_naming_test.go — #1439: an identity-bound key must name
// ONE RBAC state, across identities AND across time.
//
// The identity class is (binding-set digest, sub-generation). The digest covers
// binding ids, never role contents; a rules edit is carried only by the
// sub-generation. Two identities holding the same binding set share a cell at
// one instant, which is sound: one RBAC state, one set of verdicts. The key is
// sound over time only if a class value never names two different RBAC states.
//
// Before #1439 the sub-generation was a SUM of per-subject counters that each
// went up by 1, so two identities with one binding set and different histories
// reached the same value at different times. Since #1439 a bump writes the next
// value of one process-wide state clock and the sub-generation is the LATEST
// such value over the identity's subjects (cache/rbac_subgen.go).
//
// THE ARMS (TestIssue1439_ClassValueNamesOneRBACState). Two members of one
// group hold the same binding set. One of them (the "earlier" identity) has an
// RBAC history the other lacks: a binding naming one of its subjects was created
// and later deleted. It resolves a LIST and its body is stored. Then the rules
// of the role behind the shared group binding are edited, the `list` grant being
// removed. The "later" identity then makes the same request.
//
//	user-binding-granted-then-revoked   history on the earlier USER; then two
//	extra-group-with-revoked-binding    rules edits. Under "+1 per bump" both
//	                                    read 2 (RED on main 74606e45, and under
//	                                    the "+1, latest" mutation).
//	shared-edit-before-the-history      one shared rules edit, THEN the history,
//	                                    then one rules edit. Under "sum of clock
//	                                    values" the earlier reads 1+3 and the
//	                                    later 4 (RED under the "clock, sum"
//	                                    mutation; green on main by accident).
//
// REAL BOUNDARY (the #423 ps harness): a real ResourceWatcher over a fake
// apiserver, the real RBAC snapshot rebuild and its sub-generation flush, the
// real bindings index, the real dispatchCacheLookupKey, the real
// restActionHandler.ServeHTTP (Put and hit path), the real resolver over the
// informer pivot, and the real seedOneRestaction in the RBAC-shift reseed mode
// under the production seed context. The only seams are the reads of the
// RESTAction CR itself (fetchObjectFn / seedObjectsGetFn).
//
// ASSERTED ON THE BYTES SERVED: the later identity, denied `list` by the real
// evaluator, must not be served the row the earlier identity was allowed to see
// before the edit.
package dispatchers

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const i1439HistoryGroup = "i1439-former-team"

// The rules the arms give target-reader, the role behind the SHARED group
// binding. Each is a real rules change from the one before it.
var (
	// i1439RulesKeep keeps `list` on the target and adds an unrelated grant.
	i1439RulesKeep = func(a psArm) []any {
		return []any{
			map[string]any{"verbs": []any{"get", "list"}, "apiGroups": []any{""}, "resources": []any{a.target.Resource}},
			map[string]any{"verbs": []any{"get"}, "apiGroups": []any{""}, "resources": []any{"pods"}},
		}
	}
	// i1439RulesRevoke removes every grant on the target.
	i1439RulesRevoke = []any{map[string]any{"verbs": []any{"get"}, "apiGroups": []any{""}, "resources": []any{"pods"}}}
	// i1439RulesRevokeAgain is one more rules change that grants nothing on the target.
	i1439RulesRevokeAgain = []any{map[string]any{"verbs": []any{"get"}, "apiGroups": []any{""}, "resources": []any{"pods", "services"}}}
)

type i1439Arm struct {
	name string
	// The earlier identity's groups, and the subject its history binding names.
	earlierGroups  []string
	historySubject rbacv1.Subject
	// editBeforeHistory: one shared rules edit (keeping the grant) lands before
	// the earlier identity's history.
	editBeforeHistory bool
	// revokeEdits: the shared rules edits after the earlier identity cached its
	// answer; the first removes the grant.
	revokeEdits [][]any
	// The sub-generations the state clock gives this history, which the arm
	// controls from a zeroed clock: every flush is one state.
	wantEarlierPreEdit, wantLaterPostEdit uint64
}

func i1439UserSubject() rbacv1.Subject {
	return rbacv1.Subject{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psCarol}
}

type i1439Env struct {
	t        *testing.T
	a        psArm
	dyn      *dynamicfake.FakeDynamicClient
	srvURL   string
	counters map[string]*atomic.Int64
	cr       *unstructured.Unstructured

	earlierGroups, laterGroups []string
	earlierCtx, laterCtx       context.Context
}

func i1439Setup(t *testing.T, earlierGroups []string) *i1439Env {
	t.Helper()
	k423Env(t)
	a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a, k423PortalReaders(a)...)
	// Steady state after boot: the bindings index is built, so a rules edit
	// reaches the sub-generation of every bound subject. The state clock and
	// every counter start at zero.
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR, a.target})
	cache.ResetPendingSubGenBumpsForTest()
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetPendingSubGenBumpsForTest)

	counters := k423Counters()
	srv := psFakeAPIServer(t, a, counters)
	psSeedClientconfigs(t, srv.URL)
	e := &i1439Env{t: t, a: a, dyn: dyn, srvURL: srv.URL, counters: counters, cr: k423ListCR(a),
		earlierGroups: earlierGroups, laterGroups: []string{psGroup}}
	e.earlierCtx = i1439Ctx(psCarol, e.earlierGroups)
	e.laterCtx = i1439Ctx(psDave, e.laterGroups)
	if e.earlier() != e.later() {
		t.Fatalf("PRE: before any history both identities must be in one class")
	}
	return e
}

func i1439Ctx(user string, groups []string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: groups}))
}

func i1439Class(user string, groups []string) cache.IdentityClass {
	return rbac.IdentityClassOf(jwtutil.UserInfo{Username: user, Groups: groups})
}

func (e *i1439Env) earlier() cache.IdentityClass { return i1439Class(psCarol, e.earlierGroups) }
func (e *i1439Env) later() cache.IdentityClass   { return i1439Class(psDave, e.laterGroups) }

func (e *i1439Env) canList(user string, groups []string) bool {
	e.t.Helper()
	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: user, Groups: groups, Verb: "list", Resource: e.a.target.Resource, Namespace: psTargetNS,
	})
	if err != nil {
		e.t.Fatalf("EvaluateRBAC: %v", err)
	}
	return ok
}

// settle waits for cond, then for the RBAC rebuild loop to go quiet, so every
// later read sees one published snapshot and its flushed sub-generations.
func (e *i1439Env) settle(what string, cond func() bool) {
	e.t.Helper()
	if !i444Wait(10*time.Second, cond) {
		e.t.Fatalf("SETUP: timed out waiting for %s", what)
	}
	if err := cache.WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		e.t.Fatalf("SETUP: RBAC rebuild did not quiesce after %s: %v", what, err)
	}
}

// history grants, then revokes, a binding naming subj — two RBAC events, each
// published (and flushed) on its own. The binding set is the same before and
// after.
func (e *i1439Env) history(subj rbacv1.Subject) {
	e.t.Helper()
	rb := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: psOtherNS, Name: "i1439-history", UID: types.UID("uid-rb-1439-history")},
		Subjects:   []rbacv1.Subject{subj},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "pod-reader"},
	}
	m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
	if _, err := e.dyn.Resource(k424RBGVR).Namespace(psOtherNS).Create(context.Background(),
		&unstructured.Unstructured{Object: m}, metav1.CreateOptions{}); err != nil {
		e.t.Fatalf("history grant: %v", err)
	}
	e.settle("the history grant", func() bool { return e.earlier().SubjectBindingSet != e.later().SubjectBindingSet })
	if err := e.dyn.Resource(k424RBGVR).Namespace(psOtherNS).Delete(context.Background(), rb.Name, metav1.DeleteOptions{}); err != nil {
		e.t.Fatalf("history revoke: %v", err)
	}
	e.settle("the history revoke", func() bool { return e.earlier().SubjectBindingSet == e.later().SubjectBindingSet })
}

// editSharedRole replaces the rules of target-reader, the role behind the
// shared group binding, through the fake apiserver, and waits for the later
// identity's class to move.
func (e *i1439Env) editSharedRole(what string, rules []any) {
	e.t.Helper()
	before := e.later().RBACSubGen
	u, err := e.dyn.Resource(icsRoleGVR).Namespace(psTargetNS).Get(context.Background(), "target-reader", metav1.GetOptions{})
	if err != nil {
		e.t.Fatalf("get role: %v", err)
	}
	if err := unstructured.SetNestedSlice(u.Object, rules, "rules"); err != nil {
		e.t.Fatalf("edit role: %v", err)
	}
	if _, err := e.dyn.Resource(icsRoleGVR).Namespace(psTargetNS).Update(context.Background(), u, metav1.UpdateOptions{}); err != nil {
		e.t.Fatalf("update role: %v", err)
	}
	e.settle(what, func() bool { return e.later().RBACSubGen != before })
}

func TestIssue1439_ClassValueNamesOneRBACState(t *testing.T) {
	for _, arm := range []i1439Arm{
		{
			// A binding naming the earlier USER was granted and later revoked.
			// States: 1 grant, 2 revoke | 3 rules edit, 4 rules edit.
			name:               "user-binding-granted-then-revoked",
			earlierGroups:      []string{psGroup},
			historySubject:     i1439UserSubject(),
			revokeEdits:        [][]any{i1439RulesRevoke, i1439RulesRevokeAgain},
			wantEarlierPreEdit: 2, wantLaterPostEdit: 4,
		},
		{
			// The earlier identity presents one more GROUP that holds no binding
			// now but had one granted and later revoked. Same states.
			name:               "extra-group-with-revoked-binding",
			earlierGroups:      []string{psGroup, i1439HistoryGroup},
			historySubject:     rbacv1.Subject{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: i1439HistoryGroup},
			revokeEdits:        [][]any{i1439RulesRevoke, i1439RulesRevokeAgain},
			wantEarlierPreEdit: 2, wantLaterPostEdit: 4,
		},
		{
			// The shared role is edited once BEFORE the history.
			// States: 1 rules edit | 2 grant, 3 revoke | 4 rules edit.
			name:               "shared-edit-before-the-history",
			earlierGroups:      []string{psGroup},
			historySubject:     i1439UserSubject(),
			editBeforeHistory:  true,
			revokeEdits:        [][]any{i1439RulesRevoke},
			wantEarlierPreEdit: 3, wantLaterPostEdit: 4,
		},
	} {
		arm := arm
		t.Run(arm.name, func(t *testing.T) {
			e := i1439Setup(t, arm.earlierGroups)
			a := e.a

			if arm.editBeforeHistory {
				e.editSharedRole("the shared rules edit before the history", i1439RulesKeep(a))
			}
			e.history(arm.historySubject)

			ec, lc := e.earlier(), e.later()
			t.Logf("after the history: earlier class sub-gen=%d, later class sub-gen=%d, binding sets equal=%v",
				ec.RBACSubGen, lc.RBACSubGen, ec.SubjectBindingSet == lc.SubjectBindingSet)
			if ec.SubjectBindingSet != lc.SubjectBindingSet || ec.RBACSubGen == lc.RBACSubGen {
				t.Fatalf("PRE: the two identities must hold ONE binding set with DIFFERENT sub-generations")
			}
			if !e.canList(psCarol, e.earlierGroups) || !e.canList(psDave, e.laterGroups) {
				t.Fatalf("PRE: both identities must be allowed `list %s` in %s before the edit", a.target.Resource, psTargetNS)
			}

			// --- the earlier identity caches its answer under the pre-edit state.
			earlierKeyOld, _ := k423RAKey(t, e.earlierCtx)
			laterKeyBefore, _ := k423RAKey(t, e.laterCtx)
			if earlierKeyOld == laterKeyBefore {
				t.Fatalf("PRE: different sub-generations must give different keys before the edit")
			}
			if rec := psServeCR(t, e.cr, e.srvURL, e.earlierCtx); !psHasSentinel(rec.Body.Bytes()) {
				t.Fatalf("SETUP: the earlier identity's pre-edit body must carry row %s/%s; body=%s",
					psTargetNS, psTargetObj, psTrunc(rec.Body.String(), 300))
			}
			preEdit, ok := cache.ResolvedCache().GetNoTouch(earlierKeyOld)
			if !ok || !psHasSentinel(preEdit.RawJSON) {
				t.Fatalf("SETUP: the earlier identity's pre-edit body must be stored under its key")
			}

			// --- the rules edits on the role behind the SHARED group binding; the
			// first removes `list` on the target.
			for i, rules := range arm.revokeEdits {
				e.editSharedRole(fmt.Sprintf("rules edit %d", i+1), rules)
			}
			if e.canList(psDave, e.laterGroups) {
				t.Fatalf("PRE: after the edit the later identity must be DENIED `list %s` in %s", a.target.Resource, psTargetNS)
			}

			laterKeyNow, laterIn := k423RAKey(t, e.laterCtx)
			keysCoincide := laterKeyNow == earlierKeyOld
			_, residentBefore := cache.ResolvedCache().GetNoTouch(laterKeyNow)
			t.Logf("after %d rules edit(s): later class sub-gen=%d (earlier's pre-edit sub-gen was %d); "+
				"later's post-edit key == earlier's pre-edit key: %v; a cell is resident under the later identity's key: %v",
				len(arm.revokeEdits), laterIn.RBACSubGen, ec.RBACSubGen, keysCoincide, residentBefore)

			// --- the #258 reseed for the rotated group, as the flush enqueues it:
			// the real seed primitive, RBAC-shift mode, production seed context.
			var rep *cache.PrewarmTarget
			for _, tg := range cache.EnumeratePrewarmTargetsForGVR(h1RAGVR, "list") {
				tg := tg
				if tg.Subject.Username == "" && len(tg.Subject.Groups) == 1 && tg.Subject.Groups[0] == psGroup {
					rep = &tg
				}
			}
			if rep == nil {
				t.Fatalf("SETUP: the enumerator must yield the Group:%s representative", psGroup)
			}
			saEP := endpoints.Endpoint{ServerURL: e.srvURL, Token: "tok-sa"}
			seedCtx := withCohortSeedContext(context.Background(), seedTarget{
				BindingUID: rep.BindingUID, Username: rep.Subject.Username, Groups: rep.Subject.Groups,
			}, saEP, &rest.Config{Host: e.srvURL})
			seedKey, _ := k423RAKey(t, seedCtx)
			if seedKey != laterKeyNow {
				t.Fatalf("SETUP: the reseed's representative must derive the later identity's key (seed/customer parity)")
			}
			_, handle, _ := dispatchCacheLookupKey(seedCtx, "restactions",
				h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, psRAName, -1, -1, nil)
			skip := seedSkipDecision(seedCtx, seedModeRBACShift, handle, seedKey, "restactions", h1NS+"/"+psRAName)
			origGet := seedObjectsGetFn
			seedObjectsGetFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
				return objects.Result{GVR: h1RAGVR, Unstructured: e.cr.DeepCopy()}
			}
			t.Cleanup(func() { seedObjectsGetFn = origGet })
			ref := templatesv1.ObjectReference{
				Reference:  templatesv1.Reference{Name: psRAName, Namespace: h1NS},
				APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version,
				Resource:   h1RAGVR.Resource,
			}
			if err := seedOneRestaction(seedCtx, ref, psAuthnNS, seedModeRBACShift); err != nil {
				t.Fatalf("seedOneRestaction(rbac-shift): %v", err)
			}
			afterSeed, residentAfterSeed := cache.ResolvedCache().GetNoTouch(laterKeyNow)
			seedLeftPreEdit := residentAfterSeed && afterSeed == preEdit
			t.Logf("RBAC-shift reseed for the rotated group: skip decision=%v; cell under the later identity's key after it: "+
				"resident=%v, still the earlier identity's pre-edit entry=%v", skip, residentAfterSeed, seedLeftPreEdit)

			// --- the later identity's request.
			dialsBefore := e.counters[psDave].Load()
			hitsBefore := cache.ResolvedCache().Stats().HitTotal
			rec := psServeCR(t, e.cr, e.srvURL, e.laterCtx)
			dials := e.counters[psDave].Load() - dialsBefore
			hits := cache.ResolvedCache().Stats().HitTotal - hitsBefore
			served := psHasSentinel(rec.Body.Bytes())
			t.Logf("later identity's /call: code=%d apiserver dials as it=%d L1 hit delta=%d carries row %s/%s=%v body=%s",
				rec.Code, dials, hits, psTargetNS, psTargetObj, served, psTrunc(rec.Body.String(), 300))

			// --- control: the same request with nothing cached is the denied view.
			cache.ResetResolvedCacheForTest()
			cache.ResetDepsForTest()
			ctl := psServeCR(t, e.cr, e.srvURL, e.laterCtx)
			if psHasSentinel(ctl.Body.Bytes()) {
				t.Fatalf("CONTROL FAILED: the later identity's own post-edit resolve carries the row — the revoke is not real; body=%s",
					psTrunc(ctl.Body.String(), 300))
			}
			t.Logf("CONTROL later identity, cold cache: code=%d body=%s", ctl.Code, psTrunc(ctl.Body.String(), 300))

			if keysCoincide {
				t.Errorf("ONE CLASS VALUE, TWO RBAC STATES: the later identity's post-edit key equals the earlier identity's "+
					"pre-edit key (binding set equal, sub-generation %d before the edit and %d after it)", ec.RBACSubGen, laterIn.RBACSubGen)
			}
			if seedLeftPreEdit {
				t.Errorf("RESEED LEFT THE PRE-EDIT BODY: the RBAC-shift reseed skipped the rotated key (skip=%v) because a "+
					"cell was already live under it — the earlier identity's pre-edit entry", skip)
			}
			if served {
				t.Errorf("STALE GRANT SERVED: the later identity — denied `list %s` in %s by the real evaluator — was served "+
					"the earlier identity's pre-edit body carrying row %s/%s (apiserver dials=%d, L1 hits=%d)",
					a.target.Resource, psTargetNS, psTargetNS, psTargetObj, dials, hits)
			}
			// The values themselves: the arm drives the whole history from a
			// zeroed clock, so each sub-generation is the number of the state
			// that last touched the identity.
			if ec.RBACSubGen != arm.wantEarlierPreEdit || laterIn.RBACSubGen != arm.wantLaterPostEdit {
				t.Errorf("STATE NUMBERS: earlier pre-edit sub-gen=%d (want %d: the state of its last history event), "+
					"later post-edit sub-gen=%d (want %d: the state of the last rules edit)",
					ec.RBACSubGen, arm.wantEarlierPreEdit, laterIn.RBACSubGen, arm.wantLaterPostEdit)
			}
		})
	}
}

// TestIssue1439_SharedEditRotatesExactlyItsHoldersAndReunitesThem — the blast
// radius of the #1439 fold, on the same harness.
//
//   - a rules edit of the shared role rotates the key of BOTH holders and of
//     nobody else (an identity bound only to another role keeps its class);
//   - after it the two holders, separated until then by one's own history,
//     derive ONE key again and the second is an L1 hit on the first's cell.
//     With a sum they stayed apart for the life of the process.
func TestIssue1439_SharedEditRotatesExactlyItsHoldersAndReunitesThem(t *testing.T) {
	e := i1439Setup(t, []string{psGroup})
	e.history(i1439UserSubject())

	earlierBefore, _ := k423RAKey(t, e.earlierCtx)
	laterBefore, _ := k423RAKey(t, e.laterCtx)
	if earlierBefore == laterBefore {
		t.Fatalf("PRE: the earlier identity's own history must separate the two keys")
	}
	// Bound only to pod-reader in another namespace, presenting no group.
	outsider := i1439Class(psBob, nil)

	// States: 1 grant, 2 revoke | 3 the shared edit.
	e.editSharedRole("the shared rules edit", i1439RulesKeep(e.a))

	earlierAfter, earlierIn := k423RAKey(t, e.earlierCtx)
	laterAfter, laterIn := k423RAKey(t, e.laterCtx)
	if earlierAfter == earlierBefore || laterAfter == laterBefore {
		t.Fatalf("ROTATION MISSING: a rules edit of the shared role must rotate both holders' keys; earlier rotated=%v later rotated=%v",
			earlierAfter != earlierBefore, laterAfter != laterBefore)
	}
	if earlierIn.RBACSubGen != 3 || laterIn.RBACSubGen != 3 {
		t.Fatalf("STATE NUMBERS: both holders must read state 3 (the shared edit); earlier=%d later=%d", earlierIn.RBACSubGen, laterIn.RBACSubGen)
	}
	if got := i1439Class(psBob, nil); got != outsider {
		t.Fatalf("BLAST RADIUS: the shared role's edit moved the class of an identity no binding of which references it")
	}
	if earlierAfter != laterAfter {
		t.Fatalf("SHARING NOT RESTORED: after a change both hold, the two same-binding-set identities must derive one key")
	}

	if rec := psServeCR(t, e.cr, e.srvURL, e.laterCtx); !psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("SETUP: the later identity (still allowed) must be served the row; body=%s", psTrunc(rec.Body.String(), 300))
	}
	dialsBefore := e.counters[psCarol].Load()
	hitsBefore := cache.ResolvedCache().Stats().HitTotal
	rec := psServeCR(t, e.cr, e.srvURL, e.earlierCtx)
	if d, h := e.counters[psCarol].Load()-dialsBefore, cache.ResolvedCache().Stats().HitTotal-hitsBefore; d != 0 || h != 1 {
		t.Fatalf("SHARING NOT RESTORED: the earlier identity must be an L1 hit on the later identity's cell; apiserver dials=%d L1 hits=%d", d, h)
	}
	if !psHasSentinel(rec.Body.Bytes()) {
		t.Fatalf("the earlier identity must be served the shared cell's body; got %s", psTrunc(rec.Body.String(), 300))
	}
}

// TestIssue1439_NoKeyNamesTwoVerdictVectors — the property the arms above are
// instances of, over generated worlds and histories: an identity-bound key, once
// derived by ANY identity at ANY step, names one verdict vector for good. Every
// key each identity derives after a mutation is compared against the first
// derivation of that same key by any identity (itself included) at an earlier
// step.
//
// The #449 properties (identity_class_property_test.go) compare identities at
// one instant (sufficiency) and one identity across one step (rotation). Neither
// compares one identity's key now against ANOTHER identity's key earlier.
// RED on main 74606e45: 246 of 2,526 cross-step re-derivations, in 9 worlds.
func TestIssue1439_NoKeyNamesTwoVerdictVectors(t *testing.T) {
	const worlds, steps = 20, 8
	type derivation struct {
		who      string
		step     int
		verdicts string
	}
	aliases, compared := 0, 0
	for seed := int64(1); seed <= worlds; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("world-%02d", seed), func(t *testing.T) {
			objs := icsWorld(seed)
			dyn := icsPublish(t, objs)
			r := rand.New(rand.NewSource(1439000 + seed))
			first := map[string]derivation{} // "<kind>/<key>" → its first derivation
			record := func(step int) {
				for _, id := range icsIdentities() {
					keys := icsKeys(t, id)
					if keys == nil {
						continue
					}
					v := icsVerdicts(t, id)
					for kind, k := range keys {
						name := kind + "/" + k
						prior, seen := first[name]
						if !seen {
							first[name] = derivation{who: id.String(), step: step, verdicts: v}
							continue
						}
						if prior.step == step {
							continue // same instant: the #449 sufficiency property
						}
						compared++
						if prior.verdicts != v {
							aliases++
							t.Errorf("ONE KEY, TWO RBAC STATES: %s key %.12s… was derived by %s at step %d with verdicts [%s] "+
								"and is derived by %s at step %d with verdicts [%s]",
								kind, k, prior.who, prior.step, prior.verdicts, id, step, v)
						}
					}
				}
			}
			record(0)
			for step := 1; step <= steps; step++ {
				seq := cache.RBACSnapshotForTest().PublishSeq
				what := icsMutate(t, dyn, objs, r, step)
				icsAwaitRepublish(t, seq, what)
				record(step)
			}
		})
	}
	t.Logf("worlds=%d steps=%d cross-step re-derivations of an earlier key=%d of which name a different verdict vector=%d",
		worlds, steps, compared, aliases)
	if compared == 0 {
		t.Fatalf("VACUOUS: no key was ever derived at two different steps; the property was never exercised")
	}
}
