package dispatchers

// issue681_denial_only_call_test.go — #681 on the customer /call path, through
// the REAL restActionHandler.ServeHTTP, the REAL key derivation and the REAL
// L1, against a per-user httptest apiserver that authorizes every dial with the
// real EvaluateRBAC and answers a deny with an apiserver-style 403 whose
// message NAMES the requester (username + groups — sentinel strings).
//
// Identities:
//   A  u-alice-SNT681, groups [all681]              — may NOT get x/y
//   B  u-bob-SNT681,   groups [all681, readers681]   — may get x/y (another binding set)
//   C  u-carol-SNT681, groups [all681]              — A's exact binding set
//
// ARMS
//   H1   A's denial-only result is cached and served warm (2nd /call: 0 dials).
//   H5   cross-identity isolation: B (another set) never gets A's cell and
//        gets the fuller body; C (A's set) is served A's cell by design.
//   H5b  no identity text in any body: A's cached body, C's served body and the
//        body the cohort SEED computes (F1) contain none of the sentinels; C's
//        served body == C's live (cold) body == the seeded body, byte for byte.
//   H2   a denial plus another error (404) is NOT cached.
//   H4   a ClusterRole RULE edit granting x/y rotates A's key: the next /call
//        serves the fuller result.
//   H4g  every grant path rotates A's key: binding add, binding update,
//        binding delete, Role/ClusterRole rule edit, aggregated ClusterRole
//        update, group membership (the clientconfig / learned-class groups).

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/kubeutil"
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
	k8sdynamic "k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const (
	c681AuthnNS  = "krateo-system"
	c681All      = "all681"
	c681Readers  = "readers681"
	c681RA       = "ra681"
	c681Sentinel = "SNT681"
	c681SecretY  = "SECRETY-681"
)

var (
	c681A = ident681{"u-alice-SNT681", []string{c681All}}
	c681B = ident681{"u-bob-SNT681", []string{c681All, c681Readers}}
	c681C = ident681{"u-carol-SNT681", []string{c681All}}

	c681CMs    = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	c681CRGVR  = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	c681CRBGVR = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	c681RBGVR  = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	c681RGVR   = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
)

type ident681 struct {
	user   string
	groups []string
}

func (i ident681) token() string { return "tok-" + i.user }

func (i ident681) ctx() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: i.user, Groups: i.groups}))
}

type c681Env struct {
	dyn   k8sdynamic.Interface
	rw    *cache.ResourceWatcher
	user  *httptest.Server
	dials atomic.Int64
	ra    *unstructured.Unstructured
}

const c681RBAC = "rbac.authorization.k8s.io"

func c681Group(name string) rbacv1.Subject {
	return rbacv1.Subject{Kind: "Group", APIGroup: c681RBAC, Name: name}
}

func c681Seed() []runtime.Object {
	cr := func(name string, rules ...rbacv1.PolicyRule) *rbacv1.ClusterRole {
		return &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules}
	}
	crb := func(name, role string, s ...rbacv1.Subject) *rbacv1.ClusterRoleBinding {
		return &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID("uid-" + name)},
			Subjects: s, RoleRef: rbacv1.RoleRef{APIGroup: c681RBAC, Kind: "ClusterRole", Name: role}}
	}
	rb := func(ns, name, role string, s ...rbacv1.Subject) *rbacv1.RoleBinding {
		return &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, UID: types.UID("uid-" + ns + "-" + name)},
			Subjects: s, RoleRef: rbacv1.RoleRef{APIGroup: c681RBAC, Kind: "Role", Name: role}}
	}
	getCM := rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list"}}
	none := rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"secrets-none"}, Verbs: []string{"get"}}
	return []runtime.Object{
		cr("ra-reader681", rbacv1.PolicyRule{APIGroups: []string{h1RAGVR.Group}, Resources: []string{h1RAGVR.Resource}, Verbs: []string{"get", "list"}}),
		crb("ra-reader681", "ra-reader681", c681Group(c681All)),
		// all681 may read configmaps in pub; readers681 in x.
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: "pub", Name: "cm"}, Rules: []rbacv1.PolicyRule{getCM}},
		rb("pub", "all-cm", "cm", c681Group(c681All)),
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "cm"}, Rules: []rbacv1.PolicyRule{getCM}},
		rb("x", "readers-cm", "cm", c681Group(c681Readers)),
		// grantable681: bound to all681, grants nothing until a RULE edit (H4).
		cr("grantable681", none),
		crb("grantable681", "grantable681", c681Group(c681All)),
		// aggregated681: an aggregated ClusterRole bound to all681 (H4g).
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "aggregated681"},
			AggregationRule: &rbacv1.AggregationRule{ClusterRoleSelectors: []metav1.LabelSelector{{MatchLabels: map[string]string{"agg681": "true"}}}},
			Rules:           []rbacv1.PolicyRule{none}},
		crb("aggregated681", "aggregated681", c681Group(c681All)),
	}
}

// c681Objects answers the per-user apiserver's object reads.
func c681Objects(path string) (map[string]any, bool) {
	objs := map[string]string{
		"/api/v1/namespaces/pub/configmaps/p": "PUBDATA-681",
		"/api/v1/namespaces/x/configmaps/y":   c681SecretY,
	}
	v, ok := objs[path]
	if !ok {
		return nil, false
	}
	seg := strings.Split(strings.Trim(path, "/"), "/")
	return map[string]any{"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]any{"namespace": seg[3], "name": seg[5]}, "data": map[string]any{"v": v}}, true
}

func c681Setup(t *testing.T) *c681Env {
	t.Helper()
	s425Env(t)
	e := &c681Env{}
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.ResetDenialOnlyPutForTest()

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		h1RAGVR: "RESTActionList", h1WidgetGVR: "PanelList",
		c681CRBGVR: "ClusterRoleBindingList", c681CRGVR: "ClusterRoleList",
		c681RBGVR: "RoleBindingList", c681RGVR: "RoleList", c681CMs: "ConfigMapList",
	}
	objs := append(c681Seed(), c681CMObj("pub", "p", "PUBDATA-681"), c681CMObj("x", "y", c681SecretY))
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, objs...)
	// The production shape: configmaps is informer-servable, so an ALLOWED read
	// is served in-process (no dial) and a DENIED one falls through to the
	// requester's own apiserver dial, which 403s (F2).
	e.dyn = dyn
	wctx, wcancel := context.WithCancel(context.Background())
	rw, err := cache.NewResourceWatcher(wctx, dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	if _, ch := rw.EnsureResourceType(c681CMs); ch != nil {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("configmaps informer did not sync")
		}
	}
	if !rw.IsServable(c681CMs) {
		t.Fatal("PRE: configmaps must be servable")
	}
	cache.RebuildRBACSnapshotForTest(rw)
	// The prewarm engine builds the delta index once; the #257 role-edit
	// rotation runs only on a built index.
	cache.BuildBindingsByGVRIndex(rw.RegisteredGVRs())
	cache.ResetPendingSubGenBumpsForTest()
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)
	prev := cache.Global()
	cache.SetGlobal(rw)
	e.rw = rw
	t.Cleanup(func() {
		rw.Stop()
		wcancel()
		cache.SetGlobal(prev)
		cache.PublishRBACSnapshotForTest(nil)
	})

	byToken := map[string]ident681{}
	for _, i := range []ident681{c681A, c681B, c681C} {
		byToken[i.token()] = i
	}
	e.user = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		id, ok := byToken[strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")]
		if !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		e.dials.Add(1)
		gvr, ns, name, pok := cache.ParseAPIServerPathToDep(r.URL.Path)
		allowed := false
		if pok {
			allowed, _, _ = rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
				Username: id.user, Groups: append([]string{"system:authenticated"}, id.groups...), Verb: "get",
				Group: gvr.Group, Resource: gvr.Resource, Namespace: ns, Name: name})
		}
		if !allowed {
			// The apiserver's own wording: it names the requester and its groups.
			w.WriteHeader(http.StatusForbidden)
			gb, _ := json.Marshal(id.groups)
			_, _ = fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,`+
				`"message":"configmaps \"%s\" is forbidden: User \"%s\" cannot get resource \"configmaps\" in the namespace \"%s\" (groups %s)"}`,
				name, id.user, ns, strings.ReplaceAll(string(gb), `"`, `'`))
			return
		}
		body, found := c681Objects(r.URL.Path)
		if !found {
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
			return
		}
		b, _ := json.Marshal(body)
		_, _ = w.Write(b)
	}))
	t.Cleanup(e.user.Close)

	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	secrets := map[string]*corev1.Secret{}
	for _, i := range []ident681{c681A, c681B, c681C} {
		n := kubeutil.MakeDNS1123Compatible(i.user) + "-clientconfig"
		secrets[n] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: c681AuthnNS, Name: n},
			Data: map[string][]byte{"server-url": []byte(e.user.URL), "token": []byte(i.token())}}
	}
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: secrets})
	cache.ForceSecretsCacheReadyForTest(c681AuthnNS)
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})
	e.ra = c681RAObj("/api/v1/namespaces/x/configmaps/y", "/api/v1/namespaces/pub/configmaps/p")
	return e
}

func c681CMObj(ns, name, v string) *corev1.ConfigMap {
	return &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Data: map[string]string{"v": v}}
}

func c681RAObj(paths ...string) *unstructured.Unstructured {
	var api []any
	for i, p := range paths {
		id := "s" + string(rune('a'+i))
		api = append(api, map[string]any{"name": id, "path": p, "continueOnError": true, "errorKey": id + "Err"})
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": h1RAGVR.Group + "/" + h1RAGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": c681RA, "namespace": h1NS},
		"spec":       map[string]any{"api": api},
	}}
}

// call serves one /call for id and returns the body and the dials it made.
func (e *c681Env) call(t *testing.T, id ident681) ([]byte, int64) {
	t.Helper()
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: e.ra.DeepCopy()}
	})
	defer restore()
	d0 := e.dials.Load()
	h := &restActionHandler{authnNS: c681AuthnNS, saRC: &rest.Config{Host: e.user.URL}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call", nil).WithContext(id.ctx()))
	if rec.Code != http.StatusOK {
		t.Fatalf("/call for %s: status %d body=%s", id.user, rec.Code, rec.Body.String())
	}
	return rec.Body.Bytes(), e.dials.Load() - d0
}

func (e *c681Env) key(t *testing.T, id ident681) string {
	t.Helper()
	k, h, in := dispatchCacheLookupKey(id.ctx(), "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, c681RA, -1, -1, nil)
	if h == nil || in == nil || in.BindingUID == "" {
		t.Fatalf("PRE: %s must derive a live key (in=%v)", id.user, in)
	}
	return k
}

func (e *c681Env) cell(id ident681, t *testing.T) ([]byte, bool) {
	t.Helper()
	got, ok := cache.ResolvedCache().GetNoTouch(e.key(t, id))
	if !ok {
		return nil, false
	}
	return got.RawJSON, true
}

func c681NoIdentity(t *testing.T, what string, b []byte) {
	t.Helper()
	low := bytes.ToLower(b)
	for _, s := range []string{c681Sentinel, "alice", "bob", "carol", c681All, c681Readers} {
		if bytes.Contains(low, bytes.ToLower([]byte(s))) {
			t.Fatalf("S1/H5b RED: %s carries identity text %q: %s", what, s, s425Trunc(b))
		}
	}
}

func c681DenialPuts(site string) uint64 {
	for _, c := range cache.DenialOnlyPutSnapshot() {
		if c.Site == site {
			return c.Count
		}
	}
	return 0
}

func TestIssue681_H1_DenialOnlyCachedAndServedWarm(t *testing.T) {
	e := c681Setup(t)
	body1, d1 := e.call(t, c681A)
	if d1 != 1 {
		t.Fatalf("PRE: A's first /call made %d own dials, want 1 (the 403; the allowed read is in-process)", d1)
	}
	if bytes.Contains(body1, []byte(c681SecretY)) || !bytes.Contains(body1, []byte("PUBDATA-681")) {
		t.Fatalf("PRE: A's body must lack x/y and carry pub/p: %s", s425Trunc(body1))
	}
	if _, ok := e.cell(c681A, t); !ok {
		t.Fatal("H1 RED: A's denial-only result was not cached — every visit is a cold navigation")
	}
	if got := c681DenialPuts(cache.DenialOnlyPutSiteCallRA); got != 1 {
		t.Fatalf("denial_only_put_total{call_ra} = %d, want 1", got)
	}
	body2, d2 := e.call(t, c681A)
	if d2 != 0 {
		t.Fatalf("H1 RED: A's second /call dialled %d times — not served warm", d2)
	}
	if !bytes.Equal(body1, body2) {
		t.Fatalf("H1: the warm body differs from the cold one")
	}
}

func TestIssue681_H5_H5b_CrossIdentityIsolation_AndNoIdentityText(t *testing.T) {
	e := c681Setup(t)
	if e.key(t, c681A) != e.key(t, c681C) {
		t.Fatal("PRE: C must share A's binding set (and key)")
	}
	if e.key(t, c681A) == e.key(t, c681B) {
		t.Fatal("PRE: B must derive a different key from A")
	}
	bodyA, _ := e.call(t, c681A)
	cellA, ok := e.cell(c681A, t)
	if !ok {
		t.Fatal("PRE: A's cell was not cached")
	}
	c681NoIdentity(t, "A's cached cell", cellA)
	c681NoIdentity(t, "A's served body", bodyA)

	// C is served A's cell — by design, same set ⇒ same verdicts.
	bodyC, dC := e.call(t, c681C)
	if dC != 0 {
		t.Fatalf("PRE: C should be served A's cell (0 dials); dialled %d", dC)
	}
	c681NoIdentity(t, "C's served body", bodyC)

	// B (another set) never gets A's cell, and gets the fuller body.
	bodyB, _ := e.call(t, c681B)
	if !bytes.Contains(bodyB, []byte(c681SecretY)) || bytes.Equal(bodyB, bodyA) {
		t.Fatalf("H5 RED: B was served A's partial cell (or lacks x/y): %s", s425Trunc(bodyB))
	}

	// C's live (cold) body equals the cached body C was served.
	cache.ResetResolvedCacheForTest()
	liveC, dLive := e.call(t, c681C)
	if dLive == 0 {
		t.Fatal("PRE: C's live /call was not cold")
	}
	if !bytes.Equal(liveC, bodyC) {
		t.Fatalf("H5b RED: C's served (A-cached) body differs from C's own live body:\nserved %s\n  live %s", s425Trunc(bodyC), s425Trunc(liveC))
	}
}

func TestIssue681_H2_MixedDenialAndOtherError_NotCached(t *testing.T) {
	e := c681Setup(t)
	e.ra = c681RAObj("/api/v1/namespaces/x/configmaps/y", "/api/v1/namespaces/pub/configmaps/ghost")
	e.call(t, c681A)
	if _, ok := e.cell(c681A, t); ok {
		t.Fatal("H2 RED: a denial + 404 result was cached")
	}
	if _, d := e.call(t, c681A); d == 0 {
		t.Fatal("H2: the second /call was served from a cell")
	}
}

// waitKeyMoves polls until id's key differs from before (RBAC events are
// published asynchronously).
func (e *c681Env) waitKeyMoves(t *testing.T, id ident681, before, what string) string {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if k := e.key(t, id); k != before {
			return k
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("H4g RED (%s): A's key never rotated — a grant through this path would keep serving the old cell", what)
	return ""
}

func (e *c681Env) update(t *testing.T, gvr schema.GroupVersionResource, obj runtime.Object) {
	t.Helper()
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	uo := &unstructured.Unstructured{Object: u}
	ri := e.dyn.Resource(gvr)
	var rerr error
	if ns := uo.GetNamespace(); ns != "" {
		_, rerr = ri.Namespace(ns).Update(context.Background(), uo, metav1.UpdateOptions{})
	} else {
		_, rerr = ri.Update(context.Background(), uo, metav1.UpdateOptions{})
	}
	if rerr != nil {
		t.Fatalf("update %s: %v", gvr.Resource, rerr)
	}
}

func (e *c681Env) create(t *testing.T, gvr schema.GroupVersionResource, obj runtime.Object) {
	t.Helper()
	u, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		t.Fatal(err)
	}
	uo := &unstructured.Unstructured{Object: u}
	if _, err := e.dyn.Resource(gvr).Namespace(uo.GetNamespace()).Create(context.Background(), uo, metav1.CreateOptions{}); err != nil {
		t.Fatalf("create %s: %v", gvr.Resource, err)
	}
}

func c681CR(name string, rules ...rbacv1.PolicyRule) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{TypeMeta: metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: name}, Rules: rules}
}

var c681GetCM = rbacv1.PolicyRule{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get", "list"}}

// H4 — the end-to-end grant: a ClusterRole RULE edit (no binding change)
// grants x/y; A's key rotates (#257) and the next /call serves the fuller body.
func TestIssue681_H4_RuleEditGrant_NextCallServesFuller(t *testing.T) {
	e := c681Setup(t)
	e.call(t, c681A)
	k0 := e.key(t, c681A)
	if _, ok := e.cell(c681A, t); !ok {
		t.Fatal("PRE: A's denial-only cell is not cached")
	}
	e.update(t, c681CRGVR, c681CR("grantable681", c681GetCM))
	e.waitKeyMoves(t, c681A, k0, "ClusterRole rule edit")
	body, _ := e.call(t, c681A)
	if !bytes.Contains(body, []byte(c681SecretY)) {
		t.Fatalf("H4 RED: after the grant A was served the old denial-only cell: %s", s425Trunc(body))
	}
}

// H4g — every grant path rotates A's key.
func TestIssue681_H4g_EveryGrantPathRotates(t *testing.T) {
	e := c681Setup(t)
	k := e.key(t, c681A)
	alice := rbacv1.Subject{Kind: "User", APIGroup: c681RBAC, Name: c681A.user}

	// binding ADD — a RoleBinding granting A directly.
	rb := &rbacv1.RoleBinding{TypeMeta: metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "x", Name: "alice-cm", UID: types.UID("uid-alice-cm")},
		Subjects:   []rbacv1.Subject{alice}, RoleRef: rbacv1.RoleRef{APIGroup: c681RBAC, Kind: "Role", Name: "cm"}}
	e.create(t, c681RBGVR, rb)
	k = e.waitKeyMoves(t, c681A, k, "binding add")

	// binding UPDATE — the same binding's subjects edited (A removed).
	rb2 := rb.DeepCopy()
	rb2.Subjects = []rbacv1.Subject{c681Group("someone-else681")}
	e.update(t, c681RBGVR, rb2)
	k = e.waitKeyMoves(t, c681A, k, "binding update")

	// binding DELETE — a binding A holds is deleted. (Created here, unstructured:
	// the dynamic fake replays a TYPED-seeded object on its delete watch event,
	// which the informer rejects — a fixture artifact, not a product path.)
	rb3 := rb.DeepCopy()
	rb3.Name, rb3.UID = "alice-cm3", types.UID("uid-alice-cm3")
	e.create(t, c681RBGVR, rb3)
	k = e.waitKeyMoves(t, c681A, k, "binding add (for the delete arm)")
	if err := e.dyn.Resource(c681RBGVR).Namespace("x").Delete(context.Background(), "alice-cm3", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	k = e.waitKeyMoves(t, c681A, k, "binding delete")

	// Role / ClusterRole RULE edit.
	e.update(t, c681CRGVR, c681CR("grantable681", c681GetCM))
	k = e.waitKeyMoves(t, c681A, k, "ClusterRole rule edit")

	// AGGREGATED ClusterRole update — the aggregation controller rewrites the
	// aggregate's rules.
	agg := c681CR("aggregated681", c681GetCM)
	agg.AggregationRule = &rbacv1.AggregationRule{ClusterRoleSelectors: []metav1.LabelSelector{{MatchLabels: map[string]string{"agg681": "true"}}}}
	e.update(t, c681CRGVR, agg)
	k = e.waitKeyMoves(t, c681A, k, "aggregated ClusterRole update")

	// GROUP MEMBERSHIP — the clientconfig / learned class carries another group
	// set: another identity input, another key.
	more := ident681{c681A.user, append([]string{c681Readers}, c681A.groups...)}
	if e.key(t, more) == k {
		t.Fatal("H4g RED (group membership): A with an extra group derives the same key")
	}
}
