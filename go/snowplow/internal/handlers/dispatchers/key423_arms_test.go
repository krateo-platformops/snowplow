// key423_arms_test.go — #423 carrier, sharing, parity and rotation arms.
//
// All arms reuse the TestFalsifier_L1PerStepRBAC_CrossUser harness
// (l1_perstep_rbac_cross_user_falsifier_test.go): a real ResourceWatcher + RBAC
// snapshot, the real dispatch RBAC gate, the real key derivation and L1 store,
// and a fake apiserver that authorises each per-user token with the real
// rbac.EvaluateRBAC. Acceptance is always asserted on the BYTES served.
//
//   TestKey423_ListStep_CrossUser            — a LIST step (per-item filterListByRBAC
//                                              on the informer pivot) must not leak.
//   TestKey423_WidgetsCarrier_CrossUser      — the widgets cell (the hot carrier)
//                                              must not leak a per-step read.
//   TestKey423_CohortSharing_GroupOnlyMembers — two group-only members with one
//                                              binding set SHARE one cell (L1 hit,
//                                              zero apiserver dials) — prewarm cost
//                                              and cohort-count independence unchanged.
//   TestKey423_SeedKeyParity                 — the prewarm seed, minting under its
//                                              enumerated representative through the
//                                              production seed context, derives the
//                                              SAME key a group-only customer derives,
//                                              and the customer hits the seeded cell.
//   TestKey423_RoleEditRotatesKey            — editing a role referenced by a
//                                              requester's binding rotates that
//                                              requester's key (via RBACSubGen) and
//                                              leaves an unaffected requester's key
//                                              alone — role contents need no fold.

package dispatchers

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
)

func k423Env(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "1ms")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
}

func k423Counters() map[string]*atomic.Int64 {
	m := map[string]*atomic.Int64{}
	for _, u := range psUsers {
		m[u] = &atomic.Int64{}
	}
	return m
}

func k423RAKey(t *testing.T, ctx context.Context) (string, *cache.ResolvedKeyInputs) {
	t.Helper()
	k, h, in := dispatchCacheLookupKey(ctx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, psRAName, -1, -1, nil)
	if h == nil || in == nil || in.BindingUID == "" {
		t.Fatalf("PRE: need a live cache handle and a non-empty BindingUID; handle=%v in=%v", h != nil, in)
	}
	return k, in
}

func k423ListCR(a psArm) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": psRAName, "namespace": h1NS},
		"spec": map[string]any{"api": []any{
			map[string]any{"name": "items", "path": "/api/v1/namespaces/" + psTargetNS + "/" + a.target.Resource, "continueOnError": true},
		}},
	}}
}

// TestKey423_ListStep_CrossUser — the LIST twin of the GET falsifier. On the
// informer pivot a LIST is per-item filtered by filterListByRBAC (bob's view is
// EMPTY, not an error — so bob's own resolve is cacheable too). The per-item
// narrowing is invisible to the pre-#423 key; RED on c20cc763 in every shape
// (the GET partial-shape guard does not apply to LIST envelopes).
func TestKey423_ListStep_CrossUser(t *testing.T) {
	k423Env(t)
	for _, a := range []psArm{
		{name: "secrets/group-cobound/watch-shape", target: psSecretsGVR, watchShape: true},
		{name: "configmaps/group-cobound/watch-shape", target: psConfigmapsGVR, watchShape: true},
		{name: "configmaps/user-cobound/watch-shape", target: psConfigmapsGVR, userBound: true, watchShape: true},
		{name: "secrets/group-cobound/list-shape", target: psSecretsGVR},
		{name: "configmaps/user-cobound/list-shape", target: psConfigmapsGVR, userBound: true},
	} {
		a := a
		t.Run(a.name, func(t *testing.T) {
			psBuildWatcher(t, a)
			srv := psFakeAPIServer(t, a, k423Counters())
			psSeedClientconfigs(t, srv.URL)
			aliceCtx, bobCtx := psUserCtx(a, psAlice), psUserCtx(a, psBob)

			canList := func(user string) bool {
				ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
					Username: user, Groups: psGroups(a), Verb: "list", Resource: a.target.Resource, Namespace: psTargetNS,
				})
				if err != nil {
					t.Fatalf("PRE (i): %v", err)
				}
				return ok
			}
			if !canList(psAlice) || canList(psBob) {
				t.Fatalf("PRE (i): alice must be allowed and bob denied `list %s` in %s", a.target.Resource, psTargetNS)
			}
			aKey, aIn := k423RAKey(t, aliceCtx)
			_, bIn := k423RAKey(t, bobCtx)
			if aIn.BindingUID != bIn.BindingUID || keyWithoutSBS423(*aIn) != keyWithoutSBS423(*bIn) {
				t.Fatalf("PRE (ii): alice and bob must be co-bound (one cell pre-#423)")
			}

			cr := k423ListCR(a)
			aliceRec := psServeCR(t, cr, srv.URL, aliceCtx)
			if !psHasSentinel(aliceRec.Body.Bytes()) {
				t.Fatalf("SETUP: alice's own LIST body must carry the sentinel; body=%s", aliceRec.Body.String())
			}
			_, cached := cache.ResolvedCache().Get(aKey)
			switch {
			case isSensitive398(a.target) && cached:
				// #398: a Secret-bearing body is never persisted in any L1 cell.
				t.Fatalf("#398: alice's Secret-bearing LIST body was cached under her resolved-output key")
			case !isSensitive398(a.target) && !cached:
				t.Fatalf("SETUP: alice's LIST body must be cached (else the arm cannot detect a cross-user serve)")
			}
			bobRec := psServeCR(t, cr, srv.URL, bobCtx)
			if psHasSentinel(bobRec.Body.Bytes()) {
				t.Fatalf("#423 LIST-STEP LEAK (%s): bob — denied `list %s` — received alice's LIST item from the shared "+
					"restactions cell. body=%s", a.name, a.target.Resource, psTrunc(bobRec.Body.String(), 400))
			}
		})
	}
}

// k423WidgetExtras: a CR + CRB that lets Group:portal (and both Users, for the
// user-cobound shape) `get` the widget GVR, so the REAL checkDispatchRBAC admits
// the widget dispatch and derives a shared widgets-layer BindingUID.
func k423WidgetExtras(a psArm) []runtime.Object {
	return []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "widget-reader"},
			Rules: []rbacv1.PolicyRule{{Verbs: []string{"get", "list"},
				APIGroups: []string{h1WidgetGVR.Group}, Resources: []string{h1WidgetGVR.Resource}}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "portal-widget-bind", UID: types.UID("uid-portal-widgets")},
			Subjects:   psSubjects(a),
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "widget-reader"},
		},
	}
}

// k423WidgetSeam stands in for widgets.Resolve → apiRef RA → a NON-UAF step that
// reads the target as the requester: the body carries the sentinel iff the REAL
// evaluator lets THIS requester `get` the target. Nothing is keyed on a name.
func k423WidgetSeam(t *testing.T, a psArm) func(context.Context, widgets.ResolveOptions) (*widgets.Widget, error) {
	return func(ctx context.Context, _ widgets.ResolveOptions) (*widgets.Widget, error) {
		out := h1WidgetUnstructured(map[string]any{})
		ui, err := xcontext.UserInfo(ctx)
		if err != nil {
			t.Fatalf("seam: no identity on ctx: %v", err)
		}
		ok, _, err := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
			Username: ui.Username, Groups: ui.Groups, Verb: "get",
			Resource: a.target.Resource, Namespace: psTargetNS, Name: psTargetObj,
		})
		if err != nil {
			t.Fatalf("seam: %v", err)
		}
		v := "denied"
		if ok {
			v = psSentinel
		}
		if err := unstructured.SetNestedField(out.Object, v, "status", "widgetData", "password"); err != nil {
			t.Fatalf("seam: %v", err)
		}
		return out, nil
	}
}

func TestKey423_WidgetsCarrier_CrossUser(t *testing.T) {
	for _, a := range []psArm{
		{name: "secrets/group-cobound", target: psSecretsGVR, watchShape: true},
		{name: "configmaps/user-cobound", target: psConfigmapsGVR, userBound: true, watchShape: true},
	} {
		a := a
		t.Run(a.name, func(t *testing.T) {
			psBuildWatcher(t, a, k423WidgetExtras(a)...)
			aliceCtx, bobCtx := psUserCtx(a, psAlice), psUserCtx(a, psBob)
			cr := h1WidgetUnstructured(map[string]any{})

			wKey := func(ctx context.Context) (string, *cache.ResolvedKeyInputs) {
				k, _, in := dispatchCacheLookupKey(ctx, "widgets", h1WidgetGVR.Group, h1WidgetGVR.Version,
					h1WidgetGVR.Resource, h1NS, h1WName, -1, -1, effectiveKeyExtras(ctx, cr.Object, nil))
				if in == nil || in.BindingUID == "" {
					t.Fatalf("PRE: widgets key needs a non-empty BindingUID")
				}
				return k, in
			}
			aKey, aIn := wKey(aliceCtx)
			_, bIn := wKey(bobCtx)
			if aIn.BindingUID != bIn.BindingUID || keyWithoutSBS423(*aIn) != keyWithoutSBS423(*bIn) {
				t.Fatalf("PRE (ii): alice and bob must share one widgets cell pre-#423")
			}

			serve := func(ctx context.Context) *httptest.ResponseRecorder {
				r1 := setFetchObjectForTest(func(*http.Request) objects.Result {
					return objects.Result{GVR: h1WidgetGVR, Unstructured: cr.DeepCopy()}
				})
				r2 := setWidgetsResolveForTest(k423WidgetSeam(t, a))
				defer func() { r2(); r1() }()
				rec := httptest.NewRecorder()
				h := &widgetsHandler{authnNS: psAuthnNS, saRC: &rest.Config{}}
				h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(ctx))
				if rec.Code != 200 {
					t.Fatalf("widget dispatch must serve 200; got %d body=%s", rec.Code, rec.Body.String())
				}
				return rec
			}
			if !psHasSentinel(serve(aliceCtx).Body.Bytes()) {
				t.Fatalf("SETUP: alice's own widget body must carry the sentinel")
			}
			if _, ok := cache.ResolvedCache().Get(aKey); !ok {
				t.Fatalf("SETUP: alice's widget body must be cached under her widgets key")
			}
			bobRec := serve(bobCtx)
			if psHasSentinel(bobRec.Body.Bytes()) {
				t.Fatalf("#423 WIDGETS-CARRIER LEAK (%s): bob was served alice's per-step data from the shared widgets "+
					"cell; body=%s", a.name, psTrunc(bobRec.Body.String(), 400))
			}
		})
	}
}

// k423PortalReaders: Group:portal may `get`/`list` the target in ns x — so every
// group-only member holds the SAME binding set and is allowed the step.
func k423PortalReaders(a psArm) []runtime.Object {
	return []runtime.Object{&rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Namespace: psTargetNS, Name: "portal-target", UID: types.UID("uid-rb-portal-target")},
		Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: psGroup}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "target-reader"},
	}, &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "basic-user"},
		Rules:      []rbacv1.PolicyRule{{Verbs: []string{"create"}, APIGroups: []string{"authorization.k8s.io"}, Resources: []string{"selfsubjectaccessreviews"}}},
	}, &rbacv1.ClusterRoleBinding{
		// The cluster's ever-present system:authenticated grant (system:basic-user).
		ObjectMeta: metav1.ObjectMeta{Name: "system:basic-user", UID: types.UID("uid-crb-basic-user")},
		Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: "system:authenticated"}},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "basic-user"},
	}}
}

func TestKey423_CohortSharing_GroupOnlyMembers(t *testing.T) {
	k423Env(t)
	a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
	psBuildWatcher(t, a, k423PortalReaders(a)...)
	counters := k423Counters()
	srv := psFakeAPIServer(t, a, counters)
	psSeedClientconfigs(t, srv.URL)
	carolCtx, daveCtx := psUserCtx(a, psCarol), psUserCtx(a, psDave)

	cKey, _ := k423RAKey(t, carolCtx)
	dKey, _ := k423RAKey(t, daveCtx)
	if cKey != dKey {
		t.Fatalf("COHORT SHARING BROKEN: two group-only members with one binding set must derive ONE key "+
			"(else every user is its own cohort: prewarm cost and cell count scale with users); carol=%s dave=%s", cKey, dKey)
	}
	if !psHasSentinel(psServe(t, a, srv.URL, carolCtx).Body.Bytes()) {
		t.Fatalf("SETUP: carol (allowed via the group RB) must see the sentinel")
	}
	if _, ok := cache.ResolvedCache().Get(cKey); !ok {
		t.Fatalf("SETUP: carol's body must be cached")
	}
	before := counters[psDave].Load()
	daveRec := psServe(t, a, srv.URL, daveCtx)
	if counters[psDave].Load() != before {
		t.Fatalf("COHORT SHARING BROKEN: dave's /call dialled the apiserver — he must be an L1 hit on the shared cell")
	}
	if !psHasSentinel(daveRec.Body.Bytes()) {
		t.Fatalf("dave (same binding set, allowed) must be served the shared cell's body; got %s", daveRec.Body.String())
	}
}

// TestKey423_SeedKeyParity — the prewarm engine enumerates its representative
// identity from the BindingsByGVR index (cache.EnumeratePrewarmTargetsForGVR →
// pickRepresentativeFromSubjects → {Username:"", Groups:[portal]}) and mints the
// cell key via dispatchCacheLookupKey under withCohortSeedContext. That key must
// equal the key a real group-only member derives — including the
// system:authenticated binding the member holds and the representative (no
// username) would not match under the evaluator's username gate — and the
// member's /call must HIT a cell Put under the seed key.
func TestKey423_SeedKeyParity(t *testing.T) {
	k423Env(t)
	a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
	psBuildWatcher(t, a, k423PortalReaders(a)...)
	counters := k423Counters()
	srv := psFakeAPIServer(t, a, counters)
	psSeedClientconfigs(t, srv.URL)

	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR})
	var rep *cache.PrewarmTarget
	for _, tg := range cache.EnumeratePrewarmTargetsForGVR(h1RAGVR, "list") {
		tg := tg
		if tg.Subject.Username == "" && len(tg.Subject.Groups) == 1 && tg.Subject.Groups[0] == psGroup {
			rep = &tg
		}
	}
	if rep == nil {
		t.Fatalf("PRE: the enumerator must yield the Group:%s representative for %s", psGroup, h1RAGVR)
	}
	seedCtx := withCohortSeedContext(context.Background(), seedTarget{
		BindingUID: rep.BindingUID, Username: rep.Subject.Username, Groups: rep.Subject.Groups,
	}, endpoints.Endpoint{}, nil)
	seedKey, seedIn := k423RAKey(t, seedCtx)
	carolKey, carolIn := k423RAKey(t, psUserCtx(a, psCarol))
	if seedKey != carolKey {
		t.Fatalf("SEED/CUSTOMER KEY PARITY BROKEN: the seed mints %s (set %s) but group-only carol derives %s (set %s) — "+
			"every prewarmed cell would be unreachable and every first navigation cold",
			seedKey, sbsOf423(*seedIn), carolKey, sbsOf423(*carolIn))
	}

	// The cell the seed would Put is the cell carol's /call reads.
	marker := []byte(`{"kind":"RESTAction","status":{"seeded":"` + psSentinel + `"}}`)
	cache.ResolvedCache().Put(seedKey, &cache.ResolvedEntry{RawJSON: marker, Inputs: seedIn})
	before := counters[psCarol].Load()
	rec := psServe(t, a, srv.URL, psUserCtx(a, psCarol))
	if counters[psCarol].Load() != before || !bytes.Contains(rec.Body.Bytes(), marker) {
		t.Fatalf("carol's /call must HIT the seed-keyed cell (0 dials, seeded body); dials=%d body=%s",
			counters[psCarol].Load()-before, psTrunc(rec.Body.String(), 300))
	}

	// A requester with an EXTRA User-subject binding (alice) is a different RBAC
	// class and must NOT land on the group representative's cell.
	aliceKey, _ := k423RAKey(t, psUserCtx(a, psAlice))
	if aliceKey == seedKey {
		t.Fatalf("alice holds an extra RoleBinding yet derives the group representative's key")
	}
}

func TestKey423_RoleEditRotatesKey(t *testing.T) {
	a := psArm{name: "configmaps/group", target: psConfigmapsGVR, watchShape: true}
	dyn := psBuildWatcher(t, a) // target-reader is referenced ONLY by alice's RoleBinding
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.BuildBindingsByGVRIndex([]schema.GroupVersionResource{h1RAGVR, a.target})

	aliceCtx := psUserCtx(a, psAlice)
	// Bob is bound only to pod-reader in ns z — the edited role does not touch him.
	bobCtx := psUserCtx(a, psBob)
	aBefore, aIn := k423RAKey(t, aliceCtx)
	bBefore, _ := k423RAKey(t, bobCtx)

	rGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
	u, err := dyn.Resource(rGVR).Namespace(psTargetNS).Get(context.Background(), "target-reader", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get role: %v", err)
	}
	if err := unstructured.SetNestedSlice(u.Object, []any{map[string]any{
		"verbs": []any{"get"}, "apiGroups": []any{""}, "resources": []any{a.target.Resource}, // list REVOKED
	}}, "rules"); err != nil {
		t.Fatalf("edit role: %v", err)
	}
	if _, err := dyn.Resource(rGVR).Namespace(psTargetNS).Update(context.Background(), u, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("update role: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	var aAfter string
	for time.Now().Before(deadline) {
		if aAfter, _ = k423RAKey(t, aliceCtx); aAfter != aBefore {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if aAfter == aBefore {
		t.Fatalf("ROLE-EDIT ROTATION MISSING: a rules change on target-reader (referenced by alice's RoleBinding) did "+
			"not rotate alice's key within 5s (subgen still %d) — her cell would keep serving the pre-edit view", aIn.RBACSubGen)
	}
	if bAfter, _ := k423RAKey(t, bobCtx); bAfter != bBefore {
		t.Fatalf("BLAST RADIUS: the role edit rotated bob's key though no binding of his references target-reader")
	}
}
