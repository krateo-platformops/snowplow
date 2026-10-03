// l1_perstep_rbac_cross_user_falsifier_test.go — #423 acceptance falsifier.
//
// QUESTION: on origin/main c20cc763 the restactions-class L1 key folds only the RESTAction's own
// first-match dispatch BindingUID + the requester's RBACSubGen sum
// (helpers.go dispatchCacheLookupKey, resolved.go ComputeKey). A NON-UAF stage
// inside the RESTAction is narrowed PER REQUESTER by the apiserver (the
// requester's own <user>-clientconfig) or by the informer pivot's
// filterGetByRBAC — a dependency the key never sees. Does a co-bound user who
// LACKS the per-step grant get served the first user's resolved body?
//
// REAL BOUNDARY:
//   - the real restActionHandler.ServeHTTP (constructed in-package only to hand
//     it a non-nil SA *rest.Config — RESTAction() returns nil out-of-cluster and
//     api.Resolve then short-circuits to an empty dict);
//   - the real checkDispatchRBAC (NOT stubbed) over a real published RBAC
//     snapshot built by a real ResourceWatcher;
//   - the real dispatchCacheLookupKey / ResolvedCacheStore;
//   - the REAL resolver (restactions.Resolve → api.Resolve → per-user
//     clientconfig endpoint resolution → informer pivot / apiserver dial). The
//     resolver seam is NOT stubbed.
//   - a fake apiserver that authorises every request with the REAL
//     rbac.EvaluateRBAC for the identity its bearer token names — so A's and
//     B's bodies differ ONLY because their RBAC differs.
//
// The only seam is fetchObjectFn (the read of the RESTAction CR itself), which
// is not on the path under test.
//
// Arms (each a subtest): {secrets, configmaps} × {co-bound via ONE Group CRB,
// co-bound via ONE CRB naming both Users}. For each:
//   PRE (i)  alice may `get` the target, bob may NOT (real EvaluateRBAC).
//   PRE (ii) both derive a NON-EMPTY, EQUAL BindingUID and EQUAL RBACSubGen —
//            the two dimensions the pre-#423 key folded, so on c20cc763 their
//            keys collide (that collision IS the defect). Post-#423 the keys
//            differ only through SubjectBindingSet; the arm asserts on BOB'S
//            BODY, never on the keys (feedback: key-side-only is inadmissible).
//   CONTROL  bob alone (cold cache) never sees the sentinel (the narrowing is
//            real; the fake apiserver 403s him).
//   ARM      alice /call (cold → resolve → Put), then bob /call. bob's response
//            bytes must NOT contain alice's sentinel.
//
// OBSERVED on origin/main c20cc763 (pre-#423):
//   - watch-shape arms (secrets AND configmaps, Group AND User co-binding): RED.
//     alice's read is served by the informer pivot (apistage content, gated
//     allowed for her), her resolved body is Put under the shared restactions
//     key, and bob's /call is an L1 hit (zero apiserver dials as bob) whose
//     body carries the sentinel. The defect is the resolved-output key, not
//     anything Secret-specific.
//   - list-shape arms: GREEN, but only incidentally — the apistage GET
//     partial-shape guard refuses a TypeMeta-less object, the step falls
//     through to the per-user apiserver dial, which the dispatch site classes
//     as "external", and the external sink declines the Put. Nothing is
//     cached, so nothing leaks. That is not an isolation property.
//
// POST-#423: all 8 subtests GREEN — alice and bob hold different matching-binding
// sets (alice's extra RoleBinding), so SubjectBindingSet separates their keys;
// bob MISSES, resolves as himself, and is 403'd by the apiserver.

package dispatchers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const (
	psAuthnNS   = "krateo-system"
	psTargetNS  = "x"
	psTargetObj = "y"
	psOtherNS   = "z"
	psGroup     = "portal"
	psAlice     = "ps423-alice"
	psBob       = "ps423-bob"
	psCarol     = "ps423-carol" // #423 arms: group-only member (no User-subject binding)
	psDave      = "ps423-dave"  // #423 arms: group-only member (no User-subject binding)
	psRAName    = "perstep-reader"
	psSentinel  = "PERSTEP-SENTINEL-6b1f0c"
)

var psUsers = []string{psAlice, psBob, psCarol, psDave}

// psSAUser is the fake apiserver's identity for the "tok-sa" token.
const psSAUser = "system:serviceaccount:krateo-system:snowplow"

var (
	psSecretsGVR    = schema.GroupVersionResource{Version: "v1", Resource: "secrets"}
	psConfigmapsGVR = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
)

type psArm struct {
	name      string
	target    schema.GroupVersionResource
	userBound bool // true: ONE CRB naming User:alice + User:bob; false: ONE CRB naming Group:portal
	// watchShape: the informer-held object carries apiVersion/kind — the shape a
	// WATCH event delivers (an object created/updated after the informer
	// started). false: TypeMeta elided — the shape a core-group LIST page
	// delivers (initial sync / relist), which the apistage GET partial-shape
	// guard (apistage.go gateGetEnvelope) refuses to serve.
	watchShape bool
	// noPrereg: do not pre-register the target informer (the #398 arms observe
	// whether the REQUEST path registers it).
	noPrereg bool
}

func psGroups(a psArm) []string {
	if a.userBound {
		return nil
	}
	return []string{psGroup}
}

func psSubjects(a psArm) []rbacv1.Subject {
	if a.userBound {
		return []rbacv1.Subject{
			{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psAlice},
			{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psBob},
		}
	}
	return []rbacv1.Subject{{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: psGroup}}
}

// psBuildWatcher publishes: one CRB (the shared RESTAction grant); alice's RB in
// ns x granting get on the target resource; bob's SYMMETRIC but unrelated RB in
// ns z (get pods) so the per-subject RBACSubGen sums come out equal (asserted,
// not assumed). The target object (Secret or ConfigMap x/y) carries the
// sentinel and is also present in the fake dynamic client so the lazily
// registered informer can serve it.
func psBuildWatcher(t *testing.T, a psArm, extra ...runtime.Object) *dynamicfake.FakeDynamicClient {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	// The per-subject sub-gen counters are process-global; another test's
	// binding events must not skew this arm's (symmetric) sub-gens.
	cache.ResetRBACSubGenForTest()
	cache.ResetPendingSubGenBumpsForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)

	crbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	rbGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	rGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		h1RAGVR:         "RESTActionList",
		h1WidgetGVR:     "PanelList",
		crbGVR:          "ClusterRoleBindingList",
		crGVR:           "ClusterRoleList",
		rbGVR:           "RoleBindingList",
		rGVR:            "RoleList",
		psSecretsGVR:    "SecretList",
		psConfigmapsGVR: "ConfigMapList",
	}

	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "ra-reader"},
			Rules: []rbacv1.PolicyRule{{
				Verbs:     []string{"get", "list"},
				APIGroups: []string{h1RAGVR.Group},
				Resources: []string{h1RAGVR.Resource},
			}},
		},
		&rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "portal-ra-bind", UID: types.UID("uid-portal-shared")},
			Subjects:   psSubjects(a),
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "ra-reader"},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: psTargetNS, Name: "target-reader"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{a.target.Resource}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: psTargetNS, Name: "alice-target", UID: types.UID("uid-rb-alice")},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psAlice}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "target-reader"},
		},
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: psOtherNS, Name: "pod-reader"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"pods"}}},
		},
		&rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: psOtherNS, Name: "bob-pods", UID: types.UID("uid-rb-bob")},
			Subjects:   []rbacv1.Subject{{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: psBob}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "pod-reader"},
		},
	}
	if a.target == psSecretsGVR {
		seed = append(seed, &corev1.Secret{
			TypeMeta:   psTypeMeta(a, "Secret"),
			ObjectMeta: metav1.ObjectMeta{Namespace: psTargetNS, Name: psTargetObj},
			Data:       map[string][]byte{"password": []byte(psSentinel)},
		})
	} else {
		seed = append(seed, &corev1.ConfigMap{
			TypeMeta:   psTypeMeta(a, "ConfigMap"),
			ObjectMeta: metav1.ObjectMeta{Namespace: psTargetNS, Name: psTargetObj},
			Data:       map[string]string{"password": psSentinel},
		})
	}

	seed = append(seed, extra...)
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
	// Steady state: the target GVR's informer is registered and servable — the
	// state #398 shows ANY prior read of the path produces (Gate 6 lazy
	// register). A per-user apiserver fall-through is classified "external" by
	// the dispatch site (resolve.go external branch) and declines the Put, so
	// the shared cell is written only when the informer pivot serves the read.
	//
	// #398: a SENSITIVE target (core v1/secrets) is never informed — that is the
	// steady state now — so it is not pre-registered and must stay unregistered.
	if a.noPrereg || isSensitive398(a.target) {
		if rw.IsRegistered(a.target) {
			t.Fatalf("PRE: %s must not be registered before the arm runs", a.target)
		}
	} else {
		if _, ch := rw.EnsureResourceType(a.target); ch != nil {
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Fatalf("target informer %s did not sync", a.target)
			}
		}
		if !rw.IsServable(a.target) {
			t.Fatalf("PRE: target informer %s must be servable (steady state)", a.target)
		}
	}
	// Settle the snapshot, then zero the sub-gens: initial-sync ADD ordering
	// (a Role arriving before vs after its binding) makes the boot-time bump
	// count nondeterministic, and the pre-#423 key folds it. Zeroing it AFTER
	// the last publish models the steady state (no RBAC change since boot →
	// 0 for every subject) and makes the pre-#423 collision deterministic.
	cache.RebuildRBACSnapshotForTest(rw)
	cache.ResetPendingSubGenBumpsForTest()
	cache.ResetRBACSubGenForTest()
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})
	return dyn
}

// psFakeAPIServer authorises each GET with the REAL rbac.EvaluateRBAC for the
// user its bearer token names, then serves the sentinel-bearing object or a
// 403 Status — the apiserver's own contract for a per-user credential.
func psFakeAPIServer(t *testing.T, a psArm, perUser map[string]*atomic.Int64) *httptest.Server {
	t.Helper()
	tokens := map[string]string{}
	for _, u := range psUsers {
		tokens["tok-"+u] = u
	}
	// snowplow's own ServiceAccount (the seed / refresher transport): the
	// apiserver lets it read everything (the chart's */* get/list/watch).
	tokens["tok-sa"] = psSAUser
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		user, ok := tokens[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if c := perUser[user]; c != nil {
			c.Add(1)
		}
		kind := "ConfigMap"
		if a.target == psSecretsGVR {
			kind = "Secret"
		}
		obj := map[string]any{
			"apiVersion": "v1", "kind": kind,
			"metadata": map[string]any{"name": psTargetObj, "namespace": psTargetNS},
			"data":     map[string]any{"password": psWireValue(a)},
		}
		// LIST (the LIST-step arm): authorised with the real evaluator on `list`.
		if ok && r.URL.Path == "/api/v1/namespaces/"+psTargetNS+"/"+a.target.Resource {
			allowed, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
				Username: user, Groups: psGroups(a), Verb: "list",
				Resource: a.target.Resource, Namespace: psTargetNS,
			})
			if user == psSAUser {
				allowed, err = true, nil
			}
			if err != nil || !allowed {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
				return
			}
			body, _ := json.Marshal(map[string]any{"apiVersion": "v1", "kind": kind + "List", "items": []any{obj}})
			_, _ = w.Write(body)
			return
		}
		want := "/api/v1/namespaces/" + psTargetNS + "/" + a.target.Resource + "/" + psTargetObj
		if !ok || r.URL.Path != want {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`))
			return
		}
		allowed, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
			Username: user, Groups: psGroups(a), Verb: "get",
			Resource: a.target.Resource, Namespace: psTargetNS, Name: psTargetObj,
		})
		if user == psSAUser {
			allowed, err = true, nil
		}
		if err != nil || !allowed {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
			return
		}
		// The apiserver's own content negotiation: a metadata-only request
		// (#398 arm) gets a PartialObjectMetadata — never the object's data.
		if strings.Contains(r.Header.Get("Accept"), "as=PartialObjectMetadata") {
			body, _ := json.Marshal(map[string]any{
				"apiVersion": "meta.k8s.io/v1", "kind": "PartialObjectMetadata",
				"metadata": map[string]any{"name": psTargetObj, "namespace": psTargetNS},
			})
			_, _ = w.Write(body)
			return
		}
		body, _ := json.Marshal(obj)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func psSeedClientconfigs(t *testing.T, srvURL string) {
	t.Helper()
	prevTM := env.TestMode()
	env.SetTestMode(true) // keep the seeded server-url (no kubernetes.default.svc rewrite)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	mk := func(user string) *corev1.Secret {
		return &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: psAuthnNS, Name: user + "-clientconfig"},
			Data: map[string][]byte{
				"server-url": []byte(srvURL),
				"token":      []byte("tok-" + user),
			},
		}
	}
	byName := map[string]*corev1.Secret{}
	for _, u := range psUsers {
		byName[u+"-clientconfig"] = mk(u)
	}
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: byName})
	cache.ForceSecretsCacheReadyForTest(psAuthnNS)
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})
}

func psRACR(a psArm) *unstructured.Unstructured {
	path := "/api/v1/namespaces/" + psTargetNS + "/" + a.target.Resource + "/" + psTargetObj
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": psRAName, "namespace": h1NS},
		"spec": map[string]any{"api": []any{
			// NON-UAF, nil endpointRef → the requester's own clientconfig.
			map[string]any{"name": "target", "path": path, "continueOnError": true},
		}},
	}}
}

func psUserCtx(a psArm, user string) context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: user, Groups: psGroups(a)}))
}

func psServe(t *testing.T, a psArm, srvURL string, ctx context.Context) *httptest.ResponseRecorder {
	t.Helper()
	return psServeCR(t, psRACR(a), srvURL, ctx)
}

// psServeCR drives the real restActionHandler.ServeHTTP for an arbitrary
// RESTAction CR (the LIST-step arm reuses the harness with a LIST path).
func psServeCR(t *testing.T, cr *unstructured.Unstructured, srvURL string, ctx context.Context) *httptest.ResponseRecorder {
	t.Helper()
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
	})
	defer restore()
	h := &restActionHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: srvURL}}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(ctx)
	h.ServeHTTP(rec, req)
	return rec
}

func TestFalsifier_L1PerStepRBAC_CrossUser(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "1ms")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")

	var arms []psArm
	for _, ws := range []bool{true, false} {
		shape := "watch-shape"
		if !ws {
			shape = "list-shape"
		}
		arms = append(arms,
			psArm{name: "secrets/group-cobound/" + shape, target: psSecretsGVR, watchShape: ws},
			psArm{name: "configmaps/group-cobound/" + shape, target: psConfigmapsGVR, watchShape: ws},
			psArm{name: "secrets/user-cobound/" + shape, target: psSecretsGVR, userBound: true, watchShape: ws},
			psArm{name: "configmaps/user-cobound/" + shape, target: psConfigmapsGVR, userBound: true, watchShape: ws},
		)
	}
	for _, a := range arms {
		a := a
		t.Run(a.name, func(t *testing.T) {
			psBuildWatcher(t, a)
			perUser := map[string]*atomic.Int64{psAlice: {}, psBob: {}}
			srv := psFakeAPIServer(t, a, perUser)
			psSeedClientconfigs(t, srv.URL)
			aliceCtx, bobCtx := psUserCtx(a, psAlice), psUserCtx(a, psBob)

			// PRE (i): divergent per-step verdicts, from the real evaluator.
			can := func(ctx context.Context, user string) bool {
				ok, _, err := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
					Username: user, Groups: psGroups(a), Verb: "get",
					Resource: a.target.Resource, Namespace: psTargetNS, Name: psTargetObj,
				})
				if err != nil {
					t.Fatalf("PRE (i): EvaluateRBAC(%s): %v", user, err)
				}
				return ok
			}
			if !can(aliceCtx, psAlice) || can(bobCtx, psBob) {
				t.Fatalf("PRE (i) FAILED: alice must be allowed and bob denied `get %s %s/%s`; got alice=%v bob=%v",
					a.target.Resource, psTargetNS, psTargetObj, can(aliceCtx, psAlice), can(bobCtx, psBob))
			}

			// PRE (ii): one shared cell, by the production derivation.
			aKey, handle, aIn := dispatchCacheLookupKey(aliceCtx, "restactions",
				h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, psRAName, -1, -1, nil)
			bKey, _, bIn := dispatchCacheLookupKey(bobCtx, "restactions",
				h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, psRAName, -1, -1, nil)
			if handle == nil || aIn == nil || bIn == nil {
				t.Fatalf("PRE (ii): need a live cache handle + inputs; handle=%v a=%v b=%v", handle != nil, aIn != nil, bIn != nil)
			}
			t.Logf("keys: alice BindingUID=%q subgen=%d key=%s… | bob BindingUID=%q subgen=%d key=%s…",
				aIn.BindingUID, aIn.RBACSubGen, aKey[:12], bIn.BindingUID, bIn.RBACSubGen, bKey[:12])
			if aIn.BindingUID == "" || aIn.BindingUID != bIn.BindingUID || aIn.RBACSubGen != bIn.RBACSubGen {
				t.Fatalf("PRE (ii) FAILED: the users must be co-bound — one non-empty BindingUID and equal RBACSubGen "+
					"(the pre-#423 key dimensions); alice=%q/%d bob=%q/%d",
					aIn.BindingUID, aIn.RBACSubGen, bIn.BindingUID, bIn.RBACSubGen)
			}
			t.Logf("keys equal=%v (pre-#423: true — the shared cell; post-#423: false via SubjectBindingSet)", aKey == bKey)

			// CONTROL: bob alone, cold cache — his own resolve never carries the sentinel.
			bobAlone := psServe(t, a, srv.URL, bobCtx)
			if psHasSentinel(bobAlone.Body.Bytes()) {
				t.Fatalf("CONTROL FAILED: bob's OWN cold resolve contains the sentinel — the per-step narrowing is not real; body=%s",
					bobAlone.Body.String())
			}
			t.Logf("CONTROL bob-alone: code=%d body=%s", bobAlone.Code, psTrunc(bobAlone.Body.String(), 300))
			// Clear whatever bob's cold resolve may have stored so alice is the first writer.
			cache.ResetResolvedCacheForTest()
			cache.ResetDepsForTest()
			_, handle, _ = dispatchCacheLookupKey(aliceCtx, "restactions",
				h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, psRAName, -1, -1, nil)
			if _, ok := handle.Get(aKey); ok {
				t.Fatalf("PRE: shared key must be cold before alice's request")
			}

			// ARM: alice first (writer), then bob (reader).
			aliceRec := psServe(t, a, srv.URL, aliceCtx)
			if !psHasSentinel(aliceRec.Body.Bytes()) {
				t.Fatalf("SETUP: alice's own body must contain the sentinel (else nothing to leak); code=%d body=%s",
					aliceRec.Code, aliceRec.Body.String())
			}
			_, stored := handle.Get(aKey)
			t.Logf("after alice: code=%d storedUnderSharedKey=%v", aliceRec.Code, stored)

			bobBefore := perUser[psBob].Load()
			hitsBefore := cache.ResolvedCache().Stats().HitTotal
			bobRec := psServe(t, a, srv.URL, bobCtx)
			bobDials := perUser[psBob].Load() - bobBefore
			hits := cache.ResolvedCache().Stats().HitTotal - hitsBefore
			// bob is denied by filterGetByRBAC on the informer pivot, so ANY resolve
			// of his must dial the apiserver as him. Zero dials ⇒ no resolve ran ⇒
			// his body came from the resolved-output L1.
			hit := bobDials == 0
			t.Logf("bob: code=%d apiserverDialsAsBob=%d l1HitDelta=%d (servedFromL1=%v) body=%s",
				bobRec.Code, bobDials, hits, hit, psTrunc(bobRec.Body.String(), 300))
			if psHasSentinel(bobRec.Body.Bytes()) {
				t.Fatalf("CROSS-IDENTITY LEAK (%s): bob — denied `get %s %s/%s` by his own RBAC — received alice's "+
					"resolved body carrying the sentinel from the shared restactions cell (BindingUID=%q subgen=%d). "+
					"storedAfterAlice=%v servedFromL1=%v",
					a.name, a.target.Resource, psTargetNS, psTargetObj, aIn.BindingUID, aIn.RBACSubGen, stored, hit)
			}
		})
	}
}

func psTrunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func psTypeMeta(a psArm, kind string) metav1.TypeMeta {
	if !a.watchShape {
		return metav1.TypeMeta{}
	}
	return metav1.TypeMeta{APIVersion: "v1", Kind: kind}
}

// psWireValue is the sentinel as it appears on the wire: Secret .data is
// base64 (corev1.Secret.Data []byte), ConfigMap .data is plain.
func psWireValue(a psArm) string {
	if a.target == psSecretsGVR {
		return base64.StdEncoding.EncodeToString([]byte(psSentinel))
	}
	return psSentinel
}

func psHasSentinel(b []byte) bool {
	return bytes.Contains(b, []byte(psSentinel)) ||
		bytes.Contains(b, []byte(base64.StdEncoding.EncodeToString([]byte(psSentinel))))
}
