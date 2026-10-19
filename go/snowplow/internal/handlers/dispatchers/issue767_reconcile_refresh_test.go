package dispatchers

// issue767_reconcile_refresh_test.go — #767 on the customer /call path and the
// REAL refresher, for the risk the null skip introduces.
//
// With a null resourcesFrom element skipped (refilter.go
// resolveRefilterUAFResources), a user who opens the form of a blueprint whose
// CompositionDefinition is not reconciled yet gets a fail-closed EMPTY namespace
// picker, and — unlike #761's stage error — that empty answer is now CACHED.
// It is only correct if the cell refreshes by itself when the compdef gains
// status.resource; otherwise the user is served a stale deny-all.
//
// The RA is blueprint-formdef's api exactly (057, 2026-10-19): compdef (GET by
// name off extras) → jsonschema / crd (iterators over .compdef) → namespaces
// (no iterator, dependsOn crd, userAccessFilter create on
// [.compdef.status.resource]). The apiserver answers every read from the live
// fake client, so an Update is what the next read sees.
//
// ARMS
//   E1  /call (named, compdef NOT reconciled) serves namespaces=[] and CACHES
//       it; then an Update gives the compdef status.resource, and — with no
//       further /call — the informer event dirty-marks the cell through the
//       compdef GET's dependency edge, the refresher re-resolves it, and the
//       cell holds ["ns-a"] within the 10s refresh bound. The next /call is
//       served that body warm.
//   E2  fail-closed: in E1's not-reconciled state the requester HOLDS create on
//       fireworksapps in ns-a, and still gets [] (an empty resource set denies
//       every item) — the skip never grants.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/kubeutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sdynamic "k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const (
	e767RA    = "blueprint-formdef"
	e767User  = "u-767"
	e767Group = "g767"
	e767BP    = "bp-767"
)

var (
	e767CompdefGVR = schema.GroupVersionResource{Group: "core.krateo.io", Version: "v1alpha1", Resource: "compositiondefinitions"}
	e767CRDGVR     = schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}
	e767NSGVR      = schema.GroupVersionResource{Version: "v1", Resource: "namespaces"}
	e767CMGVR      = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	e767Extras     = map[string]any{"namespace": h1NS, "name": e767BP}
)

type e767Env struct {
	rw    *cache.ResourceWatcher
	dyn   k8sdynamic.Interface
	srv   *httptest.Server
	dials atomic.Int64
	ra    *unstructured.Unstructured
}

func e767Ctx() context.Context {
	return xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: e767User, Groups: []string{e767Group}}))
}

func e767Obj(js string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	if err := json.Unmarshal([]byte(js), &u.Object); err != nil {
		panic(err)
	}
	return u
}

func e767Compdef(reconciled bool) *unstructured.Unstructured {
	status := `{"conditions":[{"type":"Ready","status":"False"}]}`
	if reconciled {
		status = `{"resource":"fireworksapps","apiVersion":"composition.krateo.io/v1-2-0","kind":"FireworksApp"}`
	}
	return e767Obj(fmt.Sprintf(`{"apiVersion":"core.krateo.io/v1alpha1","kind":"CompositionDefinition",`+
		`"metadata":{"name":%q,"namespace":%q},"status":%s}`, e767BP, h1NS, status))
}

// e767RAObj is blueprint-formdef's spec.api as it is on 057.
func e767RAObj() *unstructured.Unstructured {
	return e767Obj(`{"apiVersion":"templates.krateo.io/v1","kind":"RESTAction",
	"metadata":{"name":"` + e767RA + `","namespace":"` + h1NS + `"},
	"spec":{"api":[
	 {"name":"compdef","verb":"GET","headers":["Accept: application/json"],
	  "dependsOn":{"name":"","iterator":"[ { namespace: (.namespace // \"\"), name: (.name // \"\") } | select(.namespace != \"\" and .name != \"\") ]"},
	  "path":"${ \"/apis/core.krateo.io/v1alpha1/namespaces/\" + .namespace + \"/compositiondefinitions/\" + .name }"},
	 {"name":"jsonschema","verb":"GET","headers":["Accept: application/json"],"continueOnError":true,"errorKey":"jsonschemaError",
	  "dependsOn":{"name":"compdef","iterator":"[ .compdef | objects | { namespace: (.metadata.namespace // \"krateo-system\"), resource: (.status.resource // \"\"), version: (((.status.apiVersion // \"\") | split(\"/\"))[1] // \"\") } | select(.resource != \"\" and .version != \"\") ]"},
	  "path":"${ \"/api/v1/namespaces/\" + .namespace + \"/configmaps/\" + .resource + \"-\" + .version + \"-jsonschema-configmap\" }"},
	 {"name":"crd","verb":"GET","headers":["Accept: application/json"],"continueOnError":true,"errorKey":"crdError",
	  "dependsOn":{"name":"compdef","iterator":"[ .compdef | objects | { resource: (.status.resource // \"\"), group: (((.status.apiVersion // \"\") | split(\"/\"))[0] // \"\") } | select(.resource != \"\" and .group != \"\") ]"},
	  "path":"${ \"/apis/apiextensions.k8s.io/v1/customresourcedefinitions/\" + .resource + \".\" + .group }"},
	 {"name":"namespaces","verb":"GET","headers":["Accept: application/json"],
	  "dependsOn":{"name":"crd"},
	  "filter":"[.namespaces.items[] | .metadata.name]",
	  "path":"/api/v1/namespaces",
	  "userAccessFilter":{"verb":"create","group":"composition.krateo.io","resourcesFrom":"[.compdef.status.resource]",
	   "namespaceFrom":".metadata.name","nameFrom":".metadata.name"}}
	]}}`)
}

func e767RBAC() []runtime.Object {
	g := rbacv1.Subject{Kind: "Group", APIGroup: c681RBAC, Name: e767Group}
	return []runtime.Object{
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: "reader767"}, Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{h1RAGVR.Group}, Resources: []string{h1RAGVR.Resource}, Verbs: []string{"get", "list"}},
			{APIGroups: []string{"", "core.krateo.io", "apiextensions.k8s.io"},
				Resources: []string{"namespaces", "configmaps", "compositiondefinitions", "customresourcedefinitions"},
				Verbs:     []string{"get", "list"}},
		}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: "reader767", UID: "uid-reader767"},
			Subjects: []rbacv1.Subject{g}, RoleRef: rbacv1.RoleRef{APIGroup: c681RBAC, Kind: "ClusterRole", Name: "reader767"}},
		// The requester may create fireworksapps in ns-a only.
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Namespace: "ns-a", Name: "creator767"}, Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"composition.krateo.io"}, Resources: []string{"fireworksapps"}, Verbs: []string{"create"}}}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Namespace: "ns-a", Name: "creator767", UID: "uid-creator767"},
			Subjects: []rbacv1.Subject{g}, RoleRef: rbacv1.RoleRef{APIGroup: c681RBAC, Kind: "Role", Name: "creator767"}},
	}
}

// serve answers a GET from the live fake client: by name, or a list.
func (e *e767Env) serve(w http.ResponseWriter, r *http.Request) {
	e.dials.Add(1)
	w.Header().Set("Content-Type", "application/json")
	gvr, ns, name, ok := cache.ParseAPIServerPathToDep(r.URL.Path)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
		return
	}
	var ri k8sdynamic.ResourceInterface = e.dyn.Resource(gvr)
	if ns != "" {
		ri = e.dyn.Resource(gvr).Namespace(ns)
	}
	var out any
	var err error
	if name == "" {
		var l *unstructured.UnstructuredList
		if l, err = ri.List(context.Background(), metav1.ListOptions{}); err == nil {
			out = l
		}
	} else {
		var o *unstructured.Unstructured
		if o, err = ri.Get(context.Background(), name, metav1.GetOptions{}); err == nil {
			out = o
		}
	}
	if err != nil {
		code := http.StatusInternalServerError
		if apierrors.IsNotFound(err) {
			code = http.StatusNotFound
		}
		w.WriteHeader(code)
		_, _ = fmt.Fprintf(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":%d}`, code)
		return
	}
	b, _ := json.Marshal(out)
	_, _ = w.Write(b)
}

func e767Setup(t *testing.T) *e767Env {
	t.Helper()
	s425Env(t)
	i187RefresherEnv(t)
	cache.ResetResolvedCacheForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	cache.ResetDepsForTest()
	t.Cleanup(cache.ResetDepsForTest)
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetRefresherForTest)
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)

	e := &e767Env{ra: e767RAObj()}
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		h1RAGVR: "RESTActionList", h1WidgetGVR: "PanelList",
		c681CRBGVR: "ClusterRoleBindingList", c681CRGVR: "ClusterRoleList",
		c681RBGVR: "RoleBindingList", c681RGVR: "RoleList",
		e767CompdefGVR: "CompositionDefinitionList", e767CRDGVR: "CustomResourceDefinitionList",
		e767NSGVR: "NamespaceList", e767CMGVR: "ConfigMapList",
	}
	objs := append(e767RBAC(),
		&corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: "ns-a"}},
		&corev1.Namespace{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"}, ObjectMeta: metav1.ObjectMeta{Name: "ns-b"}},
		e767Obj(`{"apiVersion":"apiextensions.k8s.io/v1","kind":"CustomResourceDefinition","metadata":{"name":"fireworksapps.composition.krateo.io"},"spec":{"group":"composition.krateo.io"}}`),
		c681CMObj(h1NS, "fireworksapps-v1-2-0-jsonschema-configmap", "{}"),
		e767Compdef(false),
		e.ra.DeepCopy(),
	)
	e.dyn = dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, objs...)
	wctx, wcancel := context.WithCancel(context.Background())
	rw, err := cache.NewResourceWatcher(wctx, e.dyn)
	if err != nil {
		wcancel()
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
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

	e.srv = httptest.NewServer(http.HandlerFunc(e.serve))
	t.Cleanup(e.srv.Close)
	t.Cleanup(api.SetServiceAccountEndpointForTest(func() (*endpoints.Endpoint, error) {
		return &endpoints.Endpoint{ServerURL: e.srv.URL, Token: "sa-767"}, nil
	}))

	prevTM := env.TestMode()
	env.SetTestMode(true)
	t.Cleanup(func() { env.SetTestMode(prevTM) })
	n := kubeutil.MakeDNS1123Compatible(e767User) + "-clientconfig"
	cache.ResetSecretsInformerForTest()
	cache.ResetSecretsSnapshotForTest()
	cache.PublishSecretsSnapshotForTest(&cache.SecretsSnapshot{ByName: map[string]*corev1.Secret{
		n: {ObjectMeta: metav1.ObjectMeta{Namespace: c681AuthnNS, Name: n},
			Data: map[string][]byte{"server-url": []byte(e.srv.URL), "token": []byte("tok-767")}},
	}})
	cache.ForceSecretsCacheReadyForTest(c681AuthnNS)
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})

	_, stop := i1126Loop(t, "restactions", &endpoints.Endpoint{ServerURL: e.srv.URL, Token: "sa-767"}, &rest.Config{Host: e.srv.URL})
	t.Cleanup(stop)
	return e
}

// call serves one /call with blueprint-formdef's extras and returns the
// namespaces the body carries.
func (e *e767Env) call(t *testing.T) ([]any, []byte) {
	t.Helper()
	restore := setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: e.ra.DeepCopy()}
	})
	defer restore()
	xb, _ := json.Marshal(e767Extras)
	h := &restActionHandler{authnNS: c681AuthnNS, saRC: &rest.Config{Host: e.srv.URL}}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/call?extras="+url.QueryEscape(string(xb)), nil).WithContext(e767Ctx()))
	if rec.Code != http.StatusOK {
		t.Fatalf("/call: status %d body=%s", rec.Code, rec.Body.String())
	}
	return e767Namespaces(t, rec.Body.Bytes()), rec.Body.Bytes()
}

func e767Namespaces(t *testing.T, body []byte) []any {
	t.Helper()
	ns, raw, ok := e767NamesOf(body)
	if !ok {
		t.Fatalf("body carries no readable status.namespaces: %s", raw)
	}
	return ns
}

// e767NamesOf reads the namespace names a body carries, in either shape the
// stage can leave: the stage filter's ["ns-a", ...] or a list envelope
// {"items":[{"metadata":{"name":"ns-a"}}, ...]}. raw is status.namespaces as
// served, for the log.
func e767NamesOf(body []byte) ([]any, string, bool) {
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, "<not JSON>", false
	}
	st, _ := doc["status"].(map[string]any)
	rb, _ := json.Marshal(st["namespaces"])
	switch v := st["namespaces"].(type) {
	case []any:
		return v, string(rb), true
	case map[string]any:
		items, ok := v["items"].([]any)
		if !ok {
			return nil, string(rb), false
		}
		out := []any{}
		for _, it := range items {
			m, _ := it.(map[string]any)
			md, _ := m["metadata"].(map[string]any)
			out = append(out, md["name"])
		}
		return out, string(rb), true
	}
	return nil, string(rb), false
}

func (e *e767Env) key(t *testing.T) string {
	t.Helper()
	k, h, in := dispatchCacheLookupKey(e767Ctx(), "restactions",
		h1RAGVR.Group, h1RAGVR.Version, h1RAGVR.Resource, h1NS, e767RA, -1, -1, e767Extras)
	if h == nil || in == nil || in.BindingUID == "" {
		t.Fatalf("PRE: the requester must derive a live key (in=%v)", in)
	}
	return k
}

func e767NSString(ns []any) string {
	b, _ := json.Marshal(ns)
	return string(b)
}

func TestIssue767_E1_E2_NotReconciledIsCachedFailClosed_AndRefreshesOnReconcile(t *testing.T) {
	e := e767Setup(t)

	// A running pod: the first resolver touch registers the compdef and CRD
	// informers (cache.lazy_register); that cold /call dials them and is not
	// cached (external touch). Wait until both are servable, as on a warm pod.
	e.call(t)
	i1126WaitFor(t, 10*time.Second, "compdef + CRD informers servable", func() bool {
		return e.rw.IsServable(e767CompdefGVR) && e.rw.IsServable(e767CRDGVR)
	})

	// E2 — not reconciled: the requester holds create on fireworksapps in
	// ns-a, and is still denied every namespace (an empty resource set).
	ns, body := e.call(t)
	_, raw0, _ := e767NamesOf(body)
	t.Logf("E2: not-reconciled /call status.namespaces = %s", raw0)
	if len(ns) != 0 {
		t.Fatalf("E2 RED: a null resource set GRANTED namespaces %s — the skip must stay fail-closed", e767NSString(ns))
	}
	if strings.Contains(string(body), e767User) || strings.Contains(string(body), e767Group) {
		t.Fatalf("the body names the requester: %s", s425Trunc(body))
	}
	key := e.key(t)
	// The /call Put lands after the response is written.
	var got *cache.ResolvedEntry
	putBy := time.Now().Add(5 * time.Second)
	for {
		if c, ok := cache.ResolvedCache().GetNoTouch(key); ok {
			got = c
			break
		}
		if time.Now().After(putBy) {
			t.Fatal("E1 RED: the not-reconciled fail-closed answer was not cached — a null element is still a stage error")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n := e767Namespaces(t, got.RawJSON); len(n) != 0 {
		t.Fatalf("E1 PRE: the cached cell holds %s, want []", e767NSString(n))
	}

	// The compdef reconciles. No /call follows.
	if _, err := e.dyn.Resource(e767CompdefGVR).Namespace(h1NS).Update(context.Background(), e767Compdef(true), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("compdef update: %v", err)
	}
	start := time.Now()
	var last string
	deadline := start.Add(10 * time.Second)
	for {
		if c, ok := cache.ResolvedCache().GetNoTouch(key); ok {
			if n, raw, ok := e767NamesOf(c.RawJSON); ok {
				last = raw
				if e767NSString(n) == `["ns-a"]` {
					t.Logf("E1: refreshed cell status.namespaces = %s", raw)
					break
				}
			}
		} else {
			last = "<evicted>"
		}
		if time.Now().After(deadline) {
			t.Fatalf("E1 RED: 10s after the compdef reconciled the cached cell still holds %s, want [\"ns-a\"] — "+
				"the null skip would serve a stale deny-all (refresher stats %v)", last, cache.RefresherStatsByStat())
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("E1: the cell refreshed to [\"ns-a\"] %v after the compdef update, with no /call", time.Since(start).Round(time.Millisecond))

	// The next /call is served the refreshed body warm.
	d0 := e.dials.Load()
	ns, _ = e.call(t)
	if e767NSString(ns) != `["ns-a"]` {
		t.Fatalf("E1 RED: the /call after the refresh served %s, want [\"ns-a\"]", e767NSString(ns))
	}
	if d := e.dials.Load() - d0; d != 0 {
		t.Fatalf("E1: the /call after the refresh dialled %d time(s) — it was not served from the refreshed cell", d)
	}
}
