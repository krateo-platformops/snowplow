// issue1439_rbac_state_naming_test.go — #1439: an identity-bound key must name
// ONE RBAC state, across identities AND across time.
//
// The identity class is (binding-set digest, sub-generation). The digest covers
// binding ids, never role contents; a rules edit is carried only by the
// sub-generation. Two identities holding the same binding set share a cell at
// one instant, which is sound: one RBAC state, one set of verdicts. The key is
// sound over time only if a class value never names two different RBAC states.
//
// THE ARM. Two members of one group hold the same binding set. One of them (the
// "earlier" identity) has an RBAC history the other lacks: a binding naming one
// of its subjects was created and later deleted. It resolves a LIST and its body
// is stored. Then the rules of the role behind the shared group binding are
// edited: the first edit removes the `list` grant, and the role is edited as
// many times as the earlier identity's history had steps. The "later" identity
// then makes the same request.
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
	"k8s.io/client-go/rest"
)

const i1439HistoryGroup = "i1439-former-team"

type i1439Arm struct {
	name string
	// The earlier identity's groups, and the subject its history binding names.
	earlierGroups  []string
	historySubject rbacv1.Subject
}

func i1439Ctx(user string, groups []string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: groups}))
}

func i1439Class(user string, groups []string) cache.IdentityClass {
	return rbac.IdentityClassOf(jwtutil.UserInfo{Username: user, Groups: groups})
}

// i1439Settle waits for cond, then for the RBAC rebuild loop to go quiet, so
// every later read sees one published snapshot and its flushed sub-generations.
func i1439Settle(t *testing.T, what string, cond func() bool) {
	t.Helper()
	if !i444Wait(10*time.Second, cond) {
		t.Fatalf("SETUP: timed out waiting for %s", what)
	}
	if err := cache.WaitRBACRebuildQuiesceForTest(30 * time.Second); err != nil {
		t.Fatalf("SETUP: RBAC rebuild did not quiesce after %s: %v", what, err)
	}
}

func TestIssue1439_ClassValueNamesOneRBACState(t *testing.T) {
	for _, arm := range []i1439Arm{
		{
			// A binding naming the earlier USER was granted and later revoked.
			name:           "user-binding-granted-then-revoked",
			earlierGroups:  []string{psGroup},
			historySubject: rbacv1.Subject{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psCarol},
		},
		{
			// The earlier identity presents one more GROUP that holds no binding
			// now but had one granted and later revoked.
			name:           "extra-group-with-revoked-binding",
			earlierGroups:  []string{psGroup, i1439HistoryGroup},
			historySubject: rbacv1.Subject{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: i1439HistoryGroup},
		},
	} {
		arm := arm
		t.Run(arm.name, func(t *testing.T) {
			k423Env(t)
			a := psArm{name: "configmaps/group/watch", target: psConfigmapsGVR, watchShape: true}
			dyn := psBuildWatcher(t, a, k423PortalReaders(a)...)
			// Steady state after boot: the bindings index is built, so a rules
			// edit reaches the sub-generation of every bound subject.
			cache.ResetBindingsByGVRIndexForTest()
			t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
			cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR, a.target})
			cache.ResetPendingSubGenBumpsForTest()
			cache.ResetRBACSubGenForTest()
			t.Cleanup(cache.ResetPendingSubGenBumpsForTest)

			counters := k423Counters()
			srv := psFakeAPIServer(t, a, counters)
			psSeedClientconfigs(t, srv.URL)

			laterGroups := []string{psGroup}
			earlierCtx := i1439Ctx(psCarol, arm.earlierGroups)
			laterCtx := i1439Ctx(psDave, laterGroups)
			earlierClass := func() cache.IdentityClass { return i1439Class(psCarol, arm.earlierGroups) }
			laterClass := func() cache.IdentityClass { return i1439Class(psDave, laterGroups) }
			canList := func(user string, groups []string) bool {
				ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
					Username: user, Groups: groups, Verb: "list", Resource: a.target.Resource, Namespace: psTargetNS,
				})
				if err != nil {
					t.Fatalf("EvaluateRBAC: %v", err)
				}
				return ok
			}

			if earlierClass() != laterClass() {
				t.Fatalf("PRE: before any history both identities must be in one class")
			}

			// --- the earlier identity's history: a binding granted, then revoked.
			rb := &rbacv1.RoleBinding{
				TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
				ObjectMeta: metav1.ObjectMeta{Namespace: psOtherNS, Name: "i1439-history", UID: types.UID("uid-rb-1439-history")},
				Subjects:   []rbacv1.Subject{arm.historySubject},
				RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "pod-reader"},
			}
			m, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(rb)
			historySteps := 0
			last := earlierClass().RBACSubGen
			step := func() {
				if now := earlierClass().RBACSubGen; now != last {
					historySteps++
					last = now
				}
			}
			if _, err := dyn.Resource(k424RBGVR).Namespace(psOtherNS).Create(context.Background(),
				&unstructured.Unstructured{Object: m}, metav1.CreateOptions{}); err != nil {
				t.Fatalf("history grant: %v", err)
			}
			i1439Settle(t, "the history grant", func() bool {
				return earlierClass().SubjectBindingSet != laterClass().SubjectBindingSet
			})
			step()
			if err := dyn.Resource(k424RBGVR).Namespace(psOtherNS).Delete(context.Background(), rb.Name, metav1.DeleteOptions{}); err != nil {
				t.Fatalf("history revoke: %v", err)
			}
			i1439Settle(t, "the history revoke", func() bool {
				return earlierClass().SubjectBindingSet == laterClass().SubjectBindingSet
			})
			step()

			ec, lc := earlierClass(), laterClass()
			t.Logf("after the history (%d sub-generation steps): earlier class sub-gen=%d, later class sub-gen=%d, binding sets equal=%v",
				historySteps, ec.RBACSubGen, lc.RBACSubGen, ec.SubjectBindingSet == lc.SubjectBindingSet)
			if historySteps == 0 || ec.SubjectBindingSet != lc.SubjectBindingSet || ec.RBACSubGen == lc.RBACSubGen {
				t.Fatalf("PRE: the two identities must hold ONE binding set with DIFFERENT sub-generations")
			}
			if !canList(psCarol, arm.earlierGroups) || !canList(psDave, laterGroups) {
				t.Fatalf("PRE: both identities must be allowed `list %s` in %s before the edit", a.target.Resource, psTargetNS)
			}

			// --- the earlier identity caches its answer under the pre-edit state.
			cr := k423ListCR(a)
			earlierKeyOld, _ := k423RAKey(t, earlierCtx)
			laterKeyBefore, _ := k423RAKey(t, laterCtx)
			if earlierKeyOld == laterKeyBefore {
				t.Fatalf("PRE: different sub-generations must give different keys before the edit")
			}
			if rec := psServeCR(t, cr, srv.URL, earlierCtx); !psHasSentinel(rec.Body.Bytes()) {
				t.Fatalf("SETUP: the earlier identity's pre-edit body must carry row %s/%s; body=%s",
					psTargetNS, psTargetObj, psTrunc(rec.Body.String(), 300))
			}
			preEdit, ok := cache.ResolvedCache().GetNoTouch(earlierKeyOld)
			if !ok || !psHasSentinel(preEdit.RawJSON) {
				t.Fatalf("SETUP: the earlier identity's pre-edit body must be stored under its key")
			}

			// --- the rules edits on the role behind the SHARED group binding. The
			// first removes `list` on the target; the rest are further real rules
			// changes. One edit per step of the earlier identity's history.
			editRules := [][]any{
				{map[string]any{"verbs": []any{"get"}, "apiGroups": []any{""}, "resources": []any{"pods"}}},
				{map[string]any{"verbs": []any{"get"}, "apiGroups": []any{""}, "resources": []any{"pods", "services"}}},
				{map[string]any{"verbs": []any{"get"}, "apiGroups": []any{""}, "resources": []any{"pods", "endpoints"}}},
			}
			if historySteps > len(editRules) {
				t.Fatalf("SETUP: the history took %d steps; the arm has %d rules edits", historySteps, len(editRules))
			}
			for i := 0; i < historySteps; i++ {
				before := laterClass().RBACSubGen
				u, err := dyn.Resource(icsRoleGVR).Namespace(psTargetNS).Get(context.Background(), "target-reader", metav1.GetOptions{})
				if err != nil {
					t.Fatalf("get role: %v", err)
				}
				if err := unstructured.SetNestedSlice(u.Object, editRules[i], "rules"); err != nil {
					t.Fatalf("edit role: %v", err)
				}
				if _, err := dyn.Resource(icsRoleGVR).Namespace(psTargetNS).Update(context.Background(), u, metav1.UpdateOptions{}); err != nil {
					t.Fatalf("update role: %v", err)
				}
				i1439Settle(t, fmt.Sprintf("rules edit %d", i+1), func() bool { return laterClass().RBACSubGen != before })
			}
			if canList(psDave, laterGroups) {
				t.Fatalf("PRE: after the edit the later identity must be DENIED `list %s` in %s", a.target.Resource, psTargetNS)
			}

			laterKeyNow, laterIn := k423RAKey(t, laterCtx)
			keysCoincide := laterKeyNow == earlierKeyOld
			_, residentBefore := cache.ResolvedCache().GetNoTouch(laterKeyNow)
			t.Logf("after %d rules edit(s): later class sub-gen=%d (earlier's pre-edit sub-gen was %d); "+
				"later's post-edit key == earlier's pre-edit key: %v; a cell is resident under the later identity's key: %v",
				historySteps, laterIn.RBACSubGen, ec.RBACSubGen, keysCoincide, residentBefore)

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
			saEP := endpoints.Endpoint{ServerURL: srv.URL, Token: "tok-sa"}
			seedCtx := withCohortSeedContext(context.Background(), seedTarget{
				BindingUID: rep.BindingUID, Username: rep.Subject.Username, Groups: rep.Subject.Groups,
			}, saEP, &rest.Config{Host: srv.URL})
			seedKey, _ := k423RAKey(t, seedCtx)
			if seedKey != laterKeyNow {
				t.Fatalf("SETUP: the reseed's representative must derive the later identity's key (seed/customer parity)")
			}
			_, handle, _ := dispatchCacheLookupKey(seedCtx, "restactions",
				h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, psRAName, -1, -1, nil)
			skip := seedSkipDecision(seedCtx, seedModeRBACShift, handle, seedKey, "restactions", h1NS+"/"+psRAName)
			origGet := seedObjectsGetFn
			seedObjectsGetFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
				return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
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
			dialsBefore := counters[psDave].Load()
			hitsBefore := cache.ResolvedCache().Stats().HitTotal
			rec := psServeCR(t, cr, srv.URL, laterCtx)
			dials := counters[psDave].Load() - dialsBefore
			hits := cache.ResolvedCache().Stats().HitTotal - hitsBefore
			served := psHasSentinel(rec.Body.Bytes())
			t.Logf("later identity's /call: code=%d apiserver dials as it=%d L1 hit delta=%d carries row %s/%s=%v body=%s",
				rec.Code, dials, hits, psTargetNS, psTargetObj, served, psTrunc(rec.Body.String(), 300))

			// --- control: the same request with nothing cached is the denied view.
			cache.ResetResolvedCacheForTest()
			cache.ResetDepsForTest()
			ctl := psServeCR(t, cr, srv.URL, laterCtx)
			if psHasSentinel(ctl.Body.Bytes()) {
				t.Fatalf("CONTROL FAILED: the later identity's own post-edit resolve carries the row — the revoke is not real; body=%s",
					psTrunc(ctl.Body.String(), 300))
			}
			t.Logf("CONTROL later identity, cold cache: code=%d body=%s", ctl.Code, psTrunc(ctl.Body.String(), 300))

			if keysCoincide {
				t.Errorf("ONE CLASS VALUE, TWO RBAC STATES: the later identity's post-edit key equals the earlier identity's "+
					"pre-edit key (binding set equal, sub-generation %d both times)", ec.RBACSubGen)
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
		})
	}
}

// TestIssue1439_NoKeyNamesTwoVerdictVectors — the property the arm above is one
// instance of, over generated worlds and histories: an identity-bound key, once
// derived by ANY identity at ANY step, names one verdict vector for good. Every
// key each identity derives after a mutation is compared against the first
// derivation of that same key by any identity (itself included) at an earlier
// step.
//
// The #449 properties (identity_class_property_test.go) compare identities at
// one instant (sufficiency) and one identity across one step (rotation). Neither
// compares one identity's key now against ANOTHER identity's key earlier.
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
