// seed_serves_unnarrowed_425_test.go — #425 (SECURITY) acceptance arms.
//
// DEFECT (c20cc763): withCohortSeedContext attaches a ServeWatcher, and
// rbac.ServesUnnarrowed returned true for ANY ctx carrying one. The cohort seed
// resolves as the cohort REPRESENTATIVE, not as snowplow's ServiceAccount, so a
// step the representative may not read fell to branch C
// (api.dispatchViaInternalRESTConfig), was served un-narrowed from the SA fetch,
// and the seed Put it under the cohort's key: a group-only member denied the read
// was then served it from L1 with zero apiserver calls.
//
// REAL BOUNDARY: the real seedOneRestaction under the real withCohortSeedContext
// (the only seam is seedObjectsGetFn, the read of the RESTAction CR itself), the
// real resolver (informer pivot / apistage gate / branch C with the SA
// *rest.Config), the real RBAC snapshot and EvaluateRBAC, the real L1 store, and
// the real restActionHandler.ServeHTTP for the customer. Two fake apiservers: one
// that answers ONLY the SA token (it can read everything, like the snowplow SA),
// and one that authorises each per-user token with the real EvaluateRBAC.
//
// Two informer states for the target GVR (configmaps):
//   - servable: the informer holds the objects (the reviewer's probe shape). A
//     GET-by-name the representative may not read is refused by the apistage
//     gate and falls to branch C.
//   - unservable: the informer's LIST is refused, so it never syncs and every
//     GET and LIST step falls to branch C — the only place a LIST is served
//     from the SA fetch, so the only place a LIST narrowing can be lost.
//
// Arms, and the mutation of the ServesUnnarrowed ServeWatcher clause (a) that
// turns each one RED:
//
//	TestSeed425_DeniedRepresentative_NotCachedForGroup        RED under M1 (revert:
//	    any ServeWatcher ctx serves un-narrowed).
//	TestSeed425_SnowplowSAIdentity_StillServesUnnarrowed     RED under M2 (clause
//	    (a) never exempts) — the walk / content-prewarm / SA cohort keep the SA view.
//	TestSeed425_AllowedRepresentative_GetsObject_ListNarrowed RED under M1 (the
//	    denied LIST namespace leaks) and under any over-narrowing that drops the
//	    allowed object.
//	TestSeed425_OtherServiceAccountRepresentative_Narrowed    RED under M1 and
//	    under M4 (clause (a) exempts ANY canonical SA username, not snowplow's own).

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
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
)

const (
	s425AuthnNS   = "krateo-system"
	s425NSAllowed = "x" // portal may read here only in the "allowed" arms
	s425NSDenied  = "z" // portal never may read here
	s425ObjY      = "y"
	s425ObjW      = "w"
	s425Group     = "portal"
	s425Carol     = "s425-carol" // group-only member of portal
	s425RAName    = "s425-reader"
	s425SentinelY = "S425-SENTINEL-Y-7d2e"
	s425SentinelW = "S425-SENTINEL-W-91ab"
	// snowplow's own SA and an unrelated tenant SA. The snowplow SA username is
	// never named to the code under test: it reaches it only as the `sub` claim
	// of the SA token (s425SAToken), exactly as in-cluster.
	s425SnowplowSA = "system:serviceaccount:krateo-system:snowplow-s425"
	s425TenantSA   = "system:serviceaccount:tenant-a:ci"
)

var s425CMs = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}

// s425SAToken is an unsigned JWT whose `sub` is the snowplow SA username — the
// shape of the projected SA token (phase1SAUsername decodes `sub` from it).
func s425SAToken() string {
	enc := func(v any) string {
		b, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(b)
	}
	return enc(map[string]any{"alg": "none", "typ": "JWT"}) + "." +
		enc(map[string]any{"sub": s425SnowplowSA}) + ".sig"
}

type s425Opts struct {
	servable      bool // target informer synced and holding the objects
	portalAllowed bool // RoleBinding Group:portal → get/list configmaps in ns x
}

func s425Env(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("CLIENT_BASE_BACKOFF", "1ms")
	t.Setenv("CLIENT_MAX_BACKOFF", "1ms")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
}

func s425CM(ns, name, sentinel string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Data:       map[string]string{"password": sentinel},
	}
}

func s425CMMap(ns, name, sentinel string) map[string]any {
	return map[string]any{"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"name": name, "namespace": ns},
		"data":     map[string]any{"password": sentinel}}
}

// s425BuildWatcher publishes the RBAC fixture and installs a real
// ResourceWatcher as cache.Global():
//   - ClusterRole ra-reader (get/list restactions) bound to Group:portal, to the
//     snowplow SA and to the tenant SA in THREE separate CRBs (three cohorts);
//   - Role cm-reader (get/list configmaps) in ns x, bound to Group:portal only
//     when o.portalAllowed. Nothing grants configmaps in ns z to anyone, and
//     nothing grants configmaps to either SA.
func s425BuildWatcher(t *testing.T, o s425Opts) {
	t.Helper()
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
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
		h1RAGVR: "RESTActionList", h1WidgetGVR: "PanelList",
		crbGVR: "ClusterRoleBindingList", crGVR: "ClusterRoleList",
		rbGVR: "RoleBindingList", rGVR: "RoleList", s425CMs: "ConfigMapList",
	}
	const rbacAPI = "rbac.authorization.k8s.io"
	crb := func(name, uid string, s rbacv1.Subject) *rbacv1.ClusterRoleBinding {
		return &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(uid)},
			Subjects:   []rbacv1.Subject{s},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacAPI, Kind: "ClusterRole", Name: "ra-reader"},
		}
	}
	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "ra-reader"},
			Rules: []rbacv1.PolicyRule{{Verbs: []string{"get", "list"},
				APIGroups: []string{h1RAGVR.Group}, Resources: []string{h1RAGVR.Resource}}},
		},
		crb("s425-portal-ra", "uid-s425-portal", rbacv1.Subject{Kind: "Group", APIGroup: rbacAPI, Name: s425Group}),
		crb("s425-snowplow-ra", "uid-s425-snowplow", rbacv1.Subject{Kind: "ServiceAccount", Namespace: "krateo-system", Name: "snowplow-s425"}),
		crb("s425-tenant-ra", "uid-s425-tenant", rbacv1.Subject{Kind: "ServiceAccount", Namespace: "tenant-a", Name: "ci"}),
		&rbacv1.Role{
			ObjectMeta: metav1.ObjectMeta{Namespace: s425NSAllowed, Name: "cm-reader"},
			Rules:      []rbacv1.PolicyRule{{Verbs: []string{"get", "list"}, APIGroups: []string{""}, Resources: []string{"configmaps"}}},
		},
		s425CM(s425NSAllowed, s425ObjY, s425SentinelY),
		s425CM(s425NSDenied, s425ObjW, s425SentinelW),
	}
	if o.portalAllowed {
		seed = append(seed, &rbacv1.RoleBinding{
			ObjectMeta: metav1.ObjectMeta{Namespace: s425NSAllowed, Name: "portal-cm", UID: types.UID("uid-s425-rb-portal")},
			Subjects:   []rbacv1.Subject{{Kind: "Group", APIGroup: rbacAPI, Name: s425Group}},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacAPI, Kind: "Role", Name: "cm-reader"},
		})
	}
	wctx, wcancel := context.WithCancel(context.Background())
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	if !o.servable {
		// The informer's LIST is refused: it never syncs, so the informer pivot and
		// the apistage content layer decline every configmaps call and branch C
		// (the SA *rest.Config) serves it.
		dyn.PrependReactor("list", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(s425CMs.GroupResource(), "", nil)
		})
	}
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		rw.Stop()
		wcancel()
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	if o.servable {
		if _, ch := rw.EnsureResourceType(s425CMs); ch != nil {
			select {
			case <-ch:
			case <-time.After(5 * time.Second):
				t.Fatalf("configmaps informer did not sync")
			}
		}
		if !rw.IsServable(s425CMs) {
			t.Fatalf("PRE: configmaps informer must be servable in the servable shape")
		}
	}
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
	if !o.servable && rw.IsServable(s425CMs) {
		t.Fatalf("PRE: configmaps informer must NOT be servable in the unservable shape")
	}
}

// s425Objects answers a configmaps GET/LIST path from the fixture objects, or
// returns ok=false for any other path.
func s425Objects(path string) (any, bool) {
	objs := map[string]map[string]any{
		s425NSAllowed + "/" + s425ObjY: s425CMMap(s425NSAllowed, s425ObjY, s425SentinelY),
		s425NSDenied + "/" + s425ObjW:  s425CMMap(s425NSDenied, s425ObjW, s425SentinelW),
	}
	for _, ns := range []string{s425NSAllowed, s425NSDenied} {
		base := "/api/v1/namespaces/" + ns + "/configmaps"
		if path == base {
			var items []any
			for k, v := range objs {
				if strings.HasPrefix(k, ns+"/") {
					items = append(items, v)
				}
			}
			return map[string]any{"apiVersion": "v1", "kind": "ConfigMapList", "metadata": map[string]any{}, "items": items}, true
		}
		if name, ok := strings.CutPrefix(path, base+"/"); ok {
			o, found := objs[ns+"/"+name]
			return o, found
		}
	}
	return nil, false
}

// s425SAServer answers ONLY the SA token, and lets it read everything (the
// snowplow SA's */* grant). dials counts the SA fetches (branch C ran).
func s425SAServer(t *testing.T, dials *atomic.Int64) *httptest.Server {
	t.Helper()
	tok := s425SAToken()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") != tok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		dials.Add(1)
		body, ok := s425Objects(r.URL.Path)
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","code":404}`))
			return
		}
		b, _ := json.Marshal(body)
		_, _ = w.Write(b)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// s425UserServer authorises carol's per-user token with the real EvaluateRBAC.
func s425UserServer(t *testing.T, dials *atomic.Int64) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") != "tok-"+s425Carol {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		dials.Add(1)
		gvr, ns, name, ok := cache.ParseAPIServerPathToDep(r.URL.Path)
		verb := "get"
		if name == "" {
			verb = "list"
		}
		allowed := false
		if ok {
			allowed, _, _ = rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
				Username: s425Carol, Groups: []string{s425Group}, Verb: verb,
				Group: gvr.Group, Resource: gvr.Resource, Namespace: ns, Name: name})
		}
		if !allowed {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403}`))
			return
		}
		body, found := s425Objects(r.URL.Path)
		if !found {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		b, _ := json.Marshal(body)
		_, _ = w.Write(b)
	}))
	t.Cleanup(srv.Close)
	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		s425Carol + "-clientconfig": {
			ObjectMeta: metav1.ObjectMeta{Namespace: s425AuthnNS, Name: s425Carol + "-clientconfig"},
			Data:       map[string][]byte{"server-url": []byte(srv.URL), "token": []byte("tok-" + s425Carol)},
		},
	}})
	cache.ForceSecretsCacheReadyForTest(s425AuthnNS)
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})
	return srv
}

// s425RA builds a RESTAction whose NON-UAF steps read the given apiserver paths.
func s425RA(paths ...string) *unstructured.Unstructured {
	var api []any
	for i, p := range paths {
		api = append(api, map[string]any{"name": "s" + string(rune('a'+i)), "path": p, "continueOnError": true})
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": s425RAName, "namespace": h1NS},
		"spec":       map[string]any{"api": api},
	}}
}

func s425PathGetY() string  { return "/api/v1/namespaces/" + s425NSAllowed + "/configmaps/" + s425ObjY }
func s425PathListX() string { return "/api/v1/namespaces/" + s425NSAllowed + "/configmaps" }
func s425PathListZ() string { return "/api/v1/namespaces/" + s425NSDenied + "/configmaps" }

func s425Key(t *testing.T, ctx context.Context) string {
	t.Helper()
	k, h, in := dispatchCacheLookupKey(ctx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, s425RAName, -1, -1, nil)
	if h == nil || in == nil || in.BindingUID == "" {
		t.Fatalf("PRE: need a live cache handle and a non-empty BindingUID; handle=%v in=%v", h != nil, in)
	}
	return k
}

// s425Seed runs the real seedOneRestaction for cr under the real
// withCohortSeedContext for cohort, against the SA apiserver, and returns the
// seed ctx and the seeded cell (nil when the seed declined the Put).
func s425Seed(t *testing.T, cohort seedTarget, cr *unstructured.Unstructured, saURL string) (context.Context, []byte) {
	t.Helper()
	prev := seedObjectsGetFn
	seedObjectsGetFn = func(context.Context, templatesv1.ObjectReference) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
	}
	t.Cleanup(func() { seedObjectsGetFn = prev })
	tok := s425SAToken()
	saEP := endpoints.Endpoint{ServerURL: saURL, Token: tok}
	saRC := &rest.Config{Host: saURL, BearerToken: tok}
	seedCtx := withCohortSeedContext(context.Background(), cohort, saEP, saRC)
	ref := templatesv1.ObjectReference{Reference: templatesv1.Reference{Name: s425RAName, Namespace: h1NS},
		APIVersion: h1RAGVR.Group + "/" + h1RAGVR.Version, Resource: h1RAGVR.Resource}
	if err := seedOneRestaction(seedCtx, "s425", ref, s425AuthnNS, seedModeBoot); err != nil {
		t.Fatalf("seedOneRestaction: %v", err)
	}
	if e, ok := cache.ResolvedCache().Get(s425Key(t, seedCtx)); ok {
		return seedCtx, e.RawJSON
	}
	return seedCtx, nil
}

func s425CarolCtx() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: s425Carol, Groups: []string{s425Group}}))
}

func s425ServeCarol(t *testing.T, cr *unstructured.Unstructured, userURL string) []byte {
	t.Helper()
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
	})
	defer restore()
	h := &restActionHandler{authnNS: s425AuthnNS, saRC: &rest.Config{Host: userURL}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(s425CarolCtx()))
	return rec.Body.Bytes()
}

func s425Has(b []byte, sentinel string) bool { return bytes.Contains(b, []byte(sentinel)) }

func s425Trunc(b []byte) string {
	if len(b) > 400 {
		return string(b[:400]) + "..."
	}
	return string(b)
}

func s425CanCarol(t *testing.T, verb, ns, name string) bool {
	t.Helper()
	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: s425Carol, Groups: []string{s425Group}, Verb: verb, Resource: "configmaps", Namespace: ns, Name: name})
	if err != nil {
		t.Fatalf("EvaluateRBAC: %v", err)
	}
	return ok
}

// TestSeed425_DeniedRepresentative_NotCachedForGroup is the reviewer's probe
// (#424 review, zz_review424_seedcontent_test.go) generalised over both informer
// states: the seed runs as {"", [portal]}, portal may NOT read configmaps in ns x,
// and group-only carol must never be served the object from the seeded cell.
func TestSeed425_DeniedRepresentative_NotCachedForGroup(t *testing.T) {
	s425Env(t)
	for _, tc := range []struct {
		name     string
		servable bool
		path     string
	}{
		{"servable/get", true, s425PathGetY()}, // the reviewer's shape
		{"unservable/get", false, s425PathGetY()},
		{"unservable/list", false, s425PathListX()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s425BuildWatcher(t, s425Opts{servable: tc.servable})
			var saDials, carolDials atomic.Int64
			saSrv := s425SAServer(t, &saDials)
			userSrv := s425UserServer(t, &carolDials)
			if s425CanCarol(t, "get", s425NSAllowed, s425ObjY) || s425CanCarol(t, "list", s425NSAllowed, "") {
				t.Fatalf("PRE: group-only carol must be denied configmaps in ns %s", s425NSAllowed)
			}
			cr := s425RA(tc.path)
			_, cell := s425Seed(t, seedTarget{Groups: []string{s425Group}}, cr, saSrv.URL)
			if saDials.Load() == 0 {
				t.Fatalf("PRE (non-vacuity): the seed never fetched through the SA credential (branch C did not run)")
			}
			t.Logf("seed: saDials=%d cellStored=%v cellHasSentinel=%v", saDials.Load(), cell != nil, s425Has(cell, s425SentinelY))
			if s425Has(cell, s425SentinelY) {
				t.Fatalf("#425 SEED LEAK (%s): the cohort seed cached the SA-fetched object under the group key; cell=%s",
					tc.name, s425Trunc(cell))
			}
			body := s425ServeCarol(t, cr, userSrv.URL)
			if s425Has(body, s425SentinelY) {
				t.Fatalf("#425 SEED LEAK (%s): group-only carol (denied) was served the object; carolDials=%d body=%s",
					tc.name, carolDials.Load(), s425Trunc(body))
			}
		})
	}
}

// TestSeed425_SnowplowSAIdentity_StillServesUnnarrowed — non-regression: a
// ServeWatcher ctx whose identity IS snowplow's SA (the Phase-1 walk, the content
// prewarm, and a seed cohort that is the snowplow SA's own binding) keeps serving
// the SA view un-narrowed, even though the RBAC snapshot grants the SA nothing on
// configmaps (snowplow's real */* grant is not modelled, so a narrowing would
// deny — which is what makes the arm discriminate).
func TestSeed425_SnowplowSAIdentity_StillServesUnnarrowed(t *testing.T) {
	s425Env(t)
	s425BuildWatcher(t, s425Opts{servable: false})
	var saDials atomic.Int64
	saSrv := s425SAServer(t, &saDials)

	tok := s425SAToken()
	saEP := endpoints.Endpoint{ServerURL: saSrv.URL, Token: tok}
	saRC := &rest.Config{Host: saSrv.URL, BearerToken: tok}
	for name, ctx := range map[string]context.Context{
		"withPhase1SAContext":         withPhase1SAContext(context.Background(), saEP, saRC),
		"withContentPrewarmSAContext": withContentPrewarmSAContext(context.Background(), saEP, saRC),
	} {
		ui, err := xcontext.UserInfo(ctx)
		if err != nil || ui.Username != s425SnowplowSA {
			t.Fatalf("PRE: %s must install the SA identity decoded from the token; got %q err=%v", name, ui.Username, err)
		}
		if _, ok := cache.ServeWatcherFromContext(ctx); !ok {
			t.Fatalf("PRE: %s must carry a ServeWatcher (cache-on)", name)
		}
		if !rbac.ServesUnnarrowed(ctx) {
			t.Fatalf("REGRESSION: %s (ServeWatcher + snowplow SA identity) is no longer ServesUnnarrowed", name)
		}
	}

	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: s425SnowplowSA, Verb: "get", Resource: "configmaps", Namespace: s425NSAllowed, Name: s425ObjY})
	if err != nil || ok {
		t.Fatalf("PRE: the fixture must NOT grant the SA `get configmaps` (else narrowing is a no-op); ok=%v err=%v", ok, err)
	}
	_, cell := s425Seed(t, seedTarget{Username: s425SnowplowSA}, s425RA(s425PathGetY(), s425PathListZ()), saSrv.URL)
	t.Logf("SA cohort seed: saDials=%d cellStored=%v", saDials.Load(), cell != nil)
	if !s425Has(cell, s425SentinelY) || !s425Has(cell, s425SentinelW) {
		t.Fatalf("REGRESSION: the snowplow-SA identity under a ServeWatcher no longer serves un-narrowed "+
			"(GET y and LIST z must both be in the seeded cell); cell=%s", s425Trunc(cell))
	}
}

// TestSeed425_AllowedRepresentative_GetsObject_ListNarrowed — the representative
// IS allowed configmaps in ns x and NOT in ns z. Every step goes through branch C
// (unservable informer). The seeded cell must hold the allowed object (no
// over-narrowing), must NOT hold ns z's object (the LIST stays narrowed), and
// carol must be served that cell from L1 with zero apiserver calls.
func TestSeed425_AllowedRepresentative_GetsObject_ListNarrowed(t *testing.T) {
	s425Env(t)
	s425BuildWatcher(t, s425Opts{servable: false, portalAllowed: true})
	var saDials, carolDials atomic.Int64
	saSrv := s425SAServer(t, &saDials)
	userSrv := s425UserServer(t, &carolDials)
	if !s425CanCarol(t, "get", s425NSAllowed, s425ObjY) || s425CanCarol(t, "list", s425NSDenied, "") {
		t.Fatalf("PRE: portal must be allowed configmaps in ns %s and denied in ns %s", s425NSAllowed, s425NSDenied)
	}
	cr := s425RA(s425PathGetY(), s425PathListZ())
	_, cell := s425Seed(t, seedTarget{Groups: []string{s425Group}}, cr, saSrv.URL)
	if saDials.Load() < 2 {
		t.Fatalf("PRE (non-vacuity): both steps must be fetched through the SA credential (branch C); saDials=%d", saDials.Load())
	}
	if cell == nil {
		t.Fatalf("UNDER-SERVE: the seed declined the Put for an allowed representative (cold first navigation)")
	}
	if !s425Has(cell, s425SentinelY) {
		t.Fatalf("UNDER-SERVE: the allowed representative's seeded cell lacks the object it may read; cell=%s", s425Trunc(cell))
	}
	if s425Has(cell, s425SentinelW) {
		t.Fatalf("#425 LIST LEAK: the seeded cell holds ns %s's object from an un-narrowed SA LIST; cell=%s", s425NSDenied, s425Trunc(cell))
	}
	before := carolDials.Load()
	body := s425ServeCarol(t, cr, userSrv.URL)
	if carolDials.Load() != before {
		t.Fatalf("carol must be an L1 hit on the seeded cell (zero apiserver calls); dials=%d", carolDials.Load()-before)
	}
	if !s425Has(body, s425SentinelY) || s425Has(body, s425SentinelW) {
		t.Fatalf("carol must get the allowed object and not ns %s's; body=%s", s425NSDenied, s425Trunc(body))
	}
}

// TestSeed425_OtherServiceAccountRepresentative_Narrowed — a cohort whose
// representative is a DIFFERENT ServiceAccount (cache.pickRepresentativeFromSubjects
// emits system:serviceaccount:<ns>:<name> for a ServiceAccount-kind subject) is a
// real narrowing subject. Only snowplow's own SA identity is exempt under a
// ServeWatcher; a canonical-SA-form username is not enough.
func TestSeed425_OtherServiceAccountRepresentative_Narrowed(t *testing.T) {
	s425Env(t)
	s425BuildWatcher(t, s425Opts{servable: false})
	var saDials atomic.Int64
	saSrv := s425SAServer(t, &saDials)
	ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: s425TenantSA, Verb: "get", Resource: "configmaps", Namespace: s425NSAllowed, Name: s425ObjY})
	if err != nil || ok {
		t.Fatalf("PRE: the tenant SA must be denied `get configmaps`; ok=%v err=%v", ok, err)
	}
	seedCtx, cell := s425Seed(t, seedTarget{Username: s425TenantSA}, s425RA(s425PathGetY(), s425PathListZ()), saSrv.URL)
	if rbac.ServesUnnarrowed(seedCtx) {
		t.Fatalf("#425: the tenant-SA cohort seed ctx is ServesUnnarrowed — any canonical SA username is being exempted, not only snowplow's own")
	}
	if saDials.Load() == 0 {
		t.Fatalf("PRE (non-vacuity): branch C never fetched through the SA credential")
	}
	if s425Has(cell, s425SentinelY) || s425Has(cell, s425SentinelW) {
		t.Fatalf("#425 SEED LEAK: the tenant-SA cohort's seeded cell holds SA-fetched objects it may not read; cell=%s", s425Trunc(cell))
	}
}

// s425Token mints an unsigned JWT with the given `sub`.
func s425Token(sub string) string {
	enc := func(v any) string {
		b, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(b)
	}
	return enc(map[string]any{"alg": "none"}) + "." + enc(map[string]any{"sub": sub}) + ".sig"
}

// TestRefresher427_TenantSARepresentative_Narrowed — #427, the reviewer's probe
// (#428 review, zz_review428_refresher_tenantsa_test.go) on this file's harness.
// OUTSIDE a ServeWatcher, the canonical ServiceAccount FORM of a username used to
// exempt (clause d). A restactions cell whose representative is a TENANT SA (a
// seed representative picked from a ServiceAccount-kind subject, or an
// SA-identity /call) is re-resolved by the REAL refresher (resolveAndPopulateL1)
// under that username with snowplow's SA transport; the GET the tenant SA is
// denied fell through the apistage gate to branch C and was served un-gated, and
// the refresher re-Put it under the tenant SA's key. RED on fd5df128 and c20cc763.
func TestRefresher427_TenantSARepresentative_Narrowed(t *testing.T) {
	s425Env(t)
	s425BuildWatcher(t, s425Opts{servable: true})
	var saDials, userDials atomic.Int64
	saSrv := s425SAServer(t, &saDials)
	userSrv := s425UserServer(t, &userDials)
	if ok, _, _ := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
		Username: s425TenantSA, Verb: "get", Resource: "configmaps", Namespace: s425NSAllowed, Name: s425ObjY}); ok {
		t.Fatalf("PRE: the tenant SA must be denied get configmaps %s/%s", s425NSAllowed, s425ObjY)
	}
	tok := s425SAToken()
	saEP := &endpoints.Endpoint{ServerURL: saSrv.URL, Token: tok}
	saRC := &rest.Config{Host: saSrv.URL, BearerToken: tok}

	botCtx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: s425TenantSA}))
	key, h, inputs := dispatchCacheLookupKey(botCtx, "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, s425RAName, -1, -1, nil)
	if h == nil || inputs == nil || inputs.BindingUID == "" {
		t.Fatalf("PRE: the tenant SA must derive a live cell key; inputs=%v", inputs)
	}
	// A narrowed body already sits in the cell (what the #425-fixed seed or a
	// narrowed resolve produces). The arm drives the REAL refresher over it.
	cache.ResolvedCache().Put(key, &cache.ResolvedEntry{
		RawJSON: []byte(`{"kind":"RESTAction","status":{"sa":"narrowed"}}`), Inputs: inputs})

	cr := s425RA(s425PathGetY())
	restore := setResolveOnceForTest(func(ctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		ctx = cache.WithBackgroundResolve(ctx) // as resolveOnceProd
		if rbac.ServesUnnarrowed(ctx) {
			t.Errorf("#427: the refresher ctx for a tenant-SA representative is ServesUnnarrowed")
		}
		return resolveRestActionForRefresh(ctx, objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}, in, s425AuthnNS)
	})
	defer restore()
	if err := resolveAndPopulateL1(context.Background(), *inputs, saEP, saRC); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if saDials.Load() == 0 {
		t.Fatalf("PRE (non-vacuity): the refresh never fetched through the SA credential (branch C did not run)")
	}
	if e, ok := cache.ResolvedCache().Get(key); ok && s425Has(e.RawJSON, s425SentinelY) {
		t.Fatalf("#427 LEAK: the refresher re-resolved the tenant-SA cell un-narrowed and stored data the tenant SA "+
			"is denied under its key; cell=%s", s425Trunc(e.RawJSON))
	}
	restoreFetch := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
	})
	defer restoreFetch()
	rec := httptest.NewRecorder()
	(&restActionHandler{authnNS: s425AuthnNS, saRC: &rest.Config{Host: userSrv.URL}}).
		ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(botCtx))
	if s425Has(rec.Body.Bytes(), s425SentinelY) {
		t.Fatalf("#427 LEAK at serve: the tenant SA was served SA-fetched data; body=%s", s425Trunc(rec.Body.Bytes()))
	}
}

// TestSAIdentity427_FailsClosedWithoutMatchingCredential — #428 C2. The exemption
// is "the identity IS the subject of the SA credential on the ctx", never "the
// username has the canonical SA form". With a ServeWatcher (walk/seed shape) and
// without one (refresher shape), an SA username whose ctx carries NO SA token, an
// opaque (non-JWT) token, or a token for a DIFFERENT subject must narrow (and be
// re-gated). RED under M6 (`return !ok || sa == username`) and under any
// mutation that exempts on the SA form alone.
func TestSAIdentity427_FailsClosedWithoutMatchingCredential(t *testing.T) {
	s425Env(t)
	s425BuildWatcher(t, s425Opts{servable: true})
	rc := &rest.Config{Host: "https://kubernetes.default.svc"}
	build := func(watch bool, ep *endpoints.Endpoint) context.Context {
		ctx := xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: s425SnowplowSA}))
		ctx = cache.WithInternalRESTConfig(ctx, rc)
		if ep != nil {
			ctx = cache.WithInternalEndpoint(ctx, ep)
		}
		if watch {
			ctx = cache.WithServeWatcher(ctx, cache.Global())
		}
		return cache.WithBackgroundResolve(ctx)
	}
	for _, watch := range []bool{true, false} {
		if _, ok := cache.ServeWatcherFromContext(build(watch, nil)); ok != watch {
			t.Fatalf("PRE: ServeWatcher presence must be %v", watch)
		}
		for _, tc := range []struct {
			name string
			ep   *endpoints.Endpoint
			want bool
		}{
			{"no-endpoint", nil, false},
			{"endpoint-without-token", &endpoints.Endpoint{ServerURL: rc.Host}, false},
			{"opaque-token", &endpoints.Endpoint{ServerURL: rc.Host, Token: "opaque-not-a-jwt"}, false},
			{"token-for-other-sa", &endpoints.Endpoint{ServerURL: rc.Host, Token: s425Token(s425TenantSA)}, false},
			{"token-for-this-sa (control)", &endpoints.Endpoint{ServerURL: rc.Host, Token: s425SAToken()}, true},
		} {
			ctx := build(watch, tc.ep)
			if got := rbac.ServesUnnarrowed(ctx); got != tc.want {
				t.Errorf("serveWatcher=%v %s: ServesUnnarrowed=%v, want %v", watch, tc.name, got, tc.want)
			}
			if got := rbac.MustRegateSADial(ctx); got != !tc.want {
				t.Errorf("serveWatcher=%v %s: MustRegateSADial=%v, want %v", watch, tc.name, got, !tc.want)
			}
		}
	}
}
