// issue608_crd_schema_sa_transport_test.go — #608 falsifiers F1–F5.
//
// THE DEFECT. Since #574/#597 the refresher dials the apiserver as the cell's
// cohort (cache.ImpersonatingRESTConfig). The widget refresh handed that SAME
// impersonated *rest.Config to the CRD-schema validator (RC: rcFromCtx(ctx)),
// so on a memo miss crdschema.ValidateObjectStatus rebuilt the "SA" discovery
// singleton around the cohort's config (it is keyed by pointer and every
// refresh mints a fresh rest.CopyConfig) and asked GET /api as the cohort.
// krateo-057 grants `impersonate` on users only, so every cohort that has a
// group — and every identity-free cell, which impersonated snowplow's OWN
// ServiceAccount — was refused with a 403. client-go cannot read a Status
// body on that request (.Raw()), reports the literal "unknown", and
// widgets.Resolve relabelled it as a 400. ~50K refused /api calls on 057.
//
// HOW REAL IS REAL. Every frame between the refresher queue and the wire is
// production code: cache.EnqueueRefresh -> the real refresher pool ->
// resolveAndPopulateL1 -> resolveOnceProd -> objects.Get (informer, RBAC
// filtered) -> widgets.Resolve -> crdschema.ValidateObjectStatus ->
// cache.GVRFor / dynamic.SharedSADiscoveryClient -> client-go -> an httptest
// apiserver that behaves like 057: an impersonated request whose group is not
// allowlisted, or that impersonates a ServiceAccount, is answered with
// `403 application/json Status` on EVERY path, /api included.
//
// ARMS
//
//	F1  K=3 group cohorts x M=4 widgets over 2 kinds, memo invalidated, all 12
//	    dirty-marked: 12 accepted re-Puts, refresher.failed == 0, at most one
//	    SA-discovery build, ZERO impersonated discovery/CRD requests, at most
//	    one CRD GET per kind.
//	F2  an identity-free (widgetContent) cell refreshed under snowplow's own
//	    SA identity sends no Impersonate-User header at all.
//	F3  with the validator's CRD hop forbidden even to the SA, the refresh
//	    error carries Code 403 (not 400), names the stage and the GVK, and
//	    the refresher.refresh_failed record says err_class=forbidden
//	    stage=validate.
//	F4  SharedSADiscoveryClient refuses an impersonating config without
//	    replacing the singleton or paying a build.
//	F5  the DATA read (the object fall-through) still carries the cohort's
//	    Impersonate-User — the fix must not quietly undo #574.
package dispatchers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	idynamic "github.com/krateo-platformops/snowplow/internal/dynamic"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	crdschema "github.com/krateo-platformops/snowplow/internal/resolvers/crds/schema"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const (
	f608Group   = "widgets.templates.krateo.io"
	f608Version = "v1beta1"
	f608NS      = "krateo-system"
	f608SAUser  = "system:serviceaccount:krateo-system:snowplow"
)

// f608Kind is one widget kind the fixture serves: two kinds, so two CRDs and
// two memo slots, matching the production shape (the 057 burst spanned many).
type f608Kind struct{ kind, resource, singular string }

var f608Kinds = []f608Kind{
	// Regular plurals on purpose: the fake dynamic client's tracker guesses the
	// resource from the kind (Flex -> "flexs"), so an irregular plural would
	// silently empty the informer and every read would fall through.
	{kind: "Button", resource: "buttons", singular: "button"},
	{kind: "Panel", resource: "panels", singular: "panel"},
}

func (k f608Kind) gvr() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: f608Group, Version: f608Version, Resource: k.resource}
}

func (k f608Kind) crdName() string { return k.resource + "." + f608Group }

// f608Req is one request the fake apiserver saw.
type f608Req struct {
	path   string
	user   string
	groups []string
}

func (r f608Req) impersonated() bool { return r.user != "" || len(r.groups) > 0 }

func (r f608Req) isCRD() bool {
	return strings.HasPrefix(r.path, "/apis/apiextensions.k8s.io/v1/customresourcedefinitions")
}

func (r f608Req) isDiscovery() bool {
	return r.path == "/api" || r.path == "/apis" || r.path == "/api/v1" ||
		r.path == "/apis/"+f608Group+"/"+f608Version || r.path == "/apis/apiextensions.k8s.io/v1"
}

// f608APIServer answers like krateo-057's apiserver for snowplow's SA:
//
//   - no Impersonate-* header (snowplow itself): everything is served —
//     discovery, the CRDs, the widget objects. forbidCRDForSA flips the CRD
//     GET to a 403 for F3.
//   - Impersonate-User system:serviceaccount:* or any Impersonate-Group not in
//     allowGroups: 403 application/json Status on EVERY path (057 grants
//     `impersonate` on users only — the refusal precedes routing).
//   - any other impersonation (a username-only cohort): accepted; the cohort
//     may read discovery and the widget objects but NOT the CRDs, exactly a
//     tenant's RBAC.
type f608APIServer struct {
	*httptest.Server
	allowGroups    map[string]bool
	forbidCRDForSA atomic.Bool

	mu      sync.Mutex
	reqs    []f608Req
	objects map[string]map[string]any // object path -> body
}

func newF608APIServer(t *testing.T) *f608APIServer {
	t.Helper()
	s := &f608APIServer{allowGroups: map[string]bool{}, objects: map[string]map[string]any{}}

	writeJSON := func(w http.ResponseWriter, code int, v any) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(v)
	}
	forbid := func(w http.ResponseWriter, msg string) {
		writeJSON(w, http.StatusForbidden, &metav1.Status{
			TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
			Status:   metav1.StatusFailure,
			Code:     http.StatusForbidden,
			Reason:   metav1.StatusReasonForbidden,
			Message:  msg,
		})
	}

	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := f608Req{
			path:   r.URL.Path,
			user:   r.Header.Get("Impersonate-User"),
			groups: r.Header.Values("Impersonate-Group"),
		}
		s.mu.Lock()
		s.reqs = append(s.reqs, req)
		obj, haveObj := s.objects[r.URL.Path]
		s.mu.Unlock()

		if req.impersonated() {
			refused := strings.HasPrefix(req.user, "system:serviceaccount:")
			for _, g := range req.groups {
				if !s.allowGroups[g] {
					refused = true
				}
			}
			if refused {
				forbid(w, fmt.Sprintf("users %q is forbidden: User %q cannot impersonate resource "+
					"in API group \"\" at the cluster scope", req.user, f608SAUser))
				return
			}
			if req.isCRD() {
				forbid(w, fmt.Sprintf("customresourcedefinitions.apiextensions.k8s.io is forbidden: "+
					"User %q cannot get resource \"customresourcedefinitions\"", req.user))
				return
			}
		} else if req.isCRD() && s.forbidCRDForSA.Load() {
			forbid(w, fmt.Sprintf("customresourcedefinitions.apiextensions.k8s.io is forbidden: "+
				"User %q cannot get resource \"customresourcedefinitions\"", f608SAUser))
			return
		}

		switch {
		case haveObj:
			writeJSON(w, http.StatusOK, obj)
		case r.URL.Path == "/api":
			writeJSON(w, http.StatusOK, &metav1.APIVersions{
				TypeMeta: metav1.TypeMeta{Kind: "APIVersions"},
				Versions: []string{"v1"},
			})
		case r.URL.Path == "/api/v1":
			writeJSON(w, http.StatusOK, &metav1.APIResourceList{
				TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: "v1",
			})
		case r.URL.Path == "/apis":
			gvd := func(g, v string) metav1.GroupVersionForDiscovery {
				return metav1.GroupVersionForDiscovery{GroupVersion: g + "/" + v, Version: v}
			}
			writeJSON(w, http.StatusOK, &metav1.APIGroupList{
				TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
				Groups: []metav1.APIGroup{
					{Name: f608Group, Versions: []metav1.GroupVersionForDiscovery{gvd(f608Group, f608Version)},
						PreferredVersion: gvd(f608Group, f608Version)},
					{Name: "apiextensions.k8s.io", Versions: []metav1.GroupVersionForDiscovery{gvd("apiextensions.k8s.io", "v1")},
						PreferredVersion: gvd("apiextensions.k8s.io", "v1")},
				},
			})
		case r.URL.Path == "/apis/"+f608Group+"/"+f608Version:
			var res []metav1.APIResource
			for _, k := range f608Kinds {
				res = append(res, metav1.APIResource{Name: k.resource, SingularName: k.singular,
					Namespaced: true, Kind: k.kind, Verbs: metav1.Verbs{"get", "list", "watch"}})
			}
			writeJSON(w, http.StatusOK, &metav1.APIResourceList{
				TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
				GroupVersion: f608Group + "/" + f608Version, APIResources: res,
			})
		case r.URL.Path == "/apis/apiextensions.k8s.io/v1":
			writeJSON(w, http.StatusOK, &metav1.APIResourceList{
				TypeMeta:     metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"},
				GroupVersion: "apiextensions.k8s.io/v1",
				APIResources: []metav1.APIResource{{Name: "customresourcedefinitions",
					SingularName: "customresourcedefinition", Kind: "CustomResourceDefinition",
					Verbs: metav1.Verbs{"get", "list", "watch"}}},
			})
		default:
			for _, k := range f608Kinds {
				if r.URL.Path == "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/"+k.crdName() {
					writeJSON(w, http.StatusOK, f608CRD(k))
					return
				}
			}
			writeJSON(w, http.StatusNotFound, &metav1.Status{
				TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
				Status:   metav1.StatusFailure, Code: http.StatusNotFound,
				Reason: metav1.StatusReasonNotFound, Message: "not found " + r.URL.Path,
			})
		}
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *f608APIServer) addObject(obj map[string]any, k f608Kind, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[fmt.Sprintf("/apis/%s/%s/namespaces/%s/%s/%s", f608Group, f608Version, f608NS, k.resource, name)] = obj
}

func (s *f608APIServer) requests() []f608Req {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]f608Req(nil), s.reqs...)
}

func (s *f608APIServer) resetRequests() {
	s.mu.Lock()
	s.reqs = nil
	s.mu.Unlock()
}

// f608CRD is a CRD whose widgetData schema accepts the fixture's widgets.
func f608CRD(k f608Kind) map[string]any {
	return map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1",
		"kind":       "CustomResourceDefinition",
		"metadata":   map[string]any{"name": k.crdName()},
		"spec": map[string]any{
			"group": f608Group,
			"names": map[string]any{"kind": k.kind, "plural": k.resource, "singular": k.singular},
			"scope": "Namespaced",
			"versions": []any{map[string]any{
				"name": f608Version, "served": true, "storage": true,
				"schema": map[string]any{"openAPIV3Schema": map[string]any{
					"type": "object",
					"properties": map[string]any{"spec": map[string]any{
						"type": "object",
						"properties": map[string]any{"widgetData": map[string]any{
							"type":       "object",
							"properties": map[string]any{"title": map[string]any{"type": "string"}},
						}},
					}},
				}},
			}},
		},
	}
}

func f608Widget(k f608Kind, name string) map[string]any {
	return map[string]any{
		"apiVersion": f608Group + "/" + f608Version,
		"kind":       k.kind,
		"metadata":   map[string]any{"name": name, "namespace": f608NS, "uid": "uid-" + name, "resourceVersion": "1"},
		"spec":       map[string]any{"widgetData": map[string]any{"title": name}},
	}
}

// f608SAToken is an (unsigned) ServiceAccount JWT whose `sub` is snowplow's
// own SA — the shape the projected token has; only `sub` is read.
func f608SAToken() string {
	enc := base64.RawURLEncoding
	hdr := enc.EncodeToString([]byte(`{"alg":"RS256","typ":"JWT"}`))
	body := enc.EncodeToString([]byte(`{"sub":"` + f608SAUser + `","iss":"kubernetes/serviceaccount"}`))
	return hdr + "." + body + "." + enc.EncodeToString([]byte("sig"))
}

type f608Env struct {
	srv   *f608APIServer
	store *cache.ResolvedCacheStore
	saEP  *endpoints.Endpoint
	saRC  *rest.Config
}

// f608Setup builds the 057-shaped world: a real ResourceWatcher over a fake
// dynamic client carrying the RBAC (each cohort group may get/list both
// widget kinds) and the informer-served widgets, a process SA rc installed the
// way main.go installs it, and impersonation granted the way the #574 probe
// records it.
func f608Setup(t *testing.T, cohortGroups []string, informerWidgets map[f608Kind][]string) *f608Env {
	t.Helper()
	i187RefresherEnv(t)
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	t.Cleanup(cache.ResetDepsForTest)
	t.Cleanup(cache.ResetRefresherForTest)

	prevImp := cache.ImpersonationAvailable()
	cache.SetImpersonationAvailable(true)
	t.Cleanup(func() { cache.SetImpersonationAvailable(prevImp) })

	store := cache.ResolvedCache()
	if store == nil {
		t.Skip("resolved cache disabled in this environment")
	}
	cache.Deps().SetStore(store)

	srv := newF608APIServer(t)

	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	_ = corev1.AddToScheme(sch)
	listKinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	for _, k := range f608Kinds {
		listKinds[k.gvr()] = k.kind + "List"
	}
	objs := []k8sruntime.Object{
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "f608-widget-reader"},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{f608Group},
				Resources: []string{"buttons", "panels"},
				Verbs:     []string{"get", "list", "watch"},
			}},
		},
	}
	// One binding PER cohort group, so each cohort is its own RBAC class and
	// mints its own cell key (a shared binding would collapse K cohorts onto
	// one key and the arm would measure K=1).
	for _, g := range cohortGroups {
		objs = append(objs, &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "f608-widget-reader-" + g},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "f608-widget-reader"},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: g}},
		})
	}
	for k, names := range informerWidgets {
		for _, n := range names {
			objs = append(objs, &unstructured.Unstructured{Object: f608Widget(k, n)})
		}
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, listKinds, objs...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: rw=%v err=%v", rw, err)
	}
	t.Cleanup(rw.Stop)
	for _, k := range f608Kinds {
		_, syncCh := rw.EnsureResourceType(k.gvr())
		select {
		case <-syncCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("EnsureResourceType(%s): informer did not sync", k.gvr())
		}
	}
	cache.RebuildRBACSnapshotForTest(rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	tok := f608SAToken()
	env := &f608Env{
		srv:   srv,
		store: store,
		saEP:  &endpoints.Endpoint{ServerURL: srv.URL, Token: tok},
		saRC:  &rest.Config{Host: srv.URL, BearerToken: tok},
	}
	// main.go installs the SA rc once at startup (cache.SetProcessSARestConfig)
	// and hands the SAME pointer to RegisterRefreshHandlers.
	cache.SetProcessSARestConfig(env.saRC)
	t.Cleanup(cache.ResetProcessSARestConfigForTest)

	// The plural store is warm, as on 057 (99.47% hit): it is permanent per
	// process and was filled by an SA-transport read long before.
	cache.ResetPluralsStoreForTest()
	t.Cleanup(cache.ResetPluralsStoreForTest)
	for _, k := range f608Kinds {
		if _, err := cache.GVRFor(context.Background(),
			schema.GroupVersionKind{Group: f608Group, Version: f608Version, Kind: k.kind}, env.saRC); err != nil {
			t.Fatalf("setup: warm plurals for %s: %v", k.kind, err)
		}
	}
	// The defect window: a CRD lifecycle event just emptied the schema memo.
	crdschema.InvalidateCRDSchemaMemo()
	srv.resetRequests()
	return env
}

const f608StaleBody = `{"stale":true}`

func (e *f608Env) seedCell(t *testing.T, in cache.ResolvedKeyInputs) string {
	t.Helper()
	key := cache.ComputeKey(in)
	cp := in
	e.store.Put(key, &cache.ResolvedEntry{RawJSON: []byte(f608StaleBody), Inputs: &cp})
	return key
}

// settled reports whether every key has either been accepted or left L1
// (dropped and evicted) — the two ends a refresh can reach.
func (e *f608Env) settled(keys []string) bool {
	for _, k := range keys {
		if _, ok := e.store.Get(k); ok && !e.accepted(k) {
			return false
		}
	}
	return true
}

func (e *f608Env) accepted(key string) bool {
	got, ok := e.store.Get(key)
	return ok && string(got.RawJSON) != f608StaleBody && strings.Contains(string(got.RawJSON), "widgetData")
}

func f608CohortInputs(k f608Kind, name, user string, groups []string) cache.ResolvedKeyInputs {
	return cache.ResolvedKeyInputs{
		CacheEntryClass:        "widgets",
		Group:                  f608Group,
		Version:                f608Version,
		Resource:               k.resource,
		Namespace:              f608NS,
		Name:                   name,
		BindingUID:             "uid-608",
		RepresentativeUsername: user,
		RepresentativeGroups:   groups,
		SubjectBindingSet:      rbac.SubjectBindingSetDigest(user, groups),
	}
}

// f608Run drives the real refresher for `class` until every key is settled
// (or a generous deadline) and returns how many handler invocations and
// refresher.failed increments the run produced. The counters are read BEFORE
// the pool is torn down: ResetRefresherForTest zeroes them.
func f608Run(t *testing.T, e *f608Env, class string, keys []string) (invocations, failed int64) {
	t.Helper()
	inv, stop := i1126Loop(t, class, e.saEP, e.saRC)
	for _, k := range keys {
		cache.EnqueueRefresh(k)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && !e.settled(keys) {
		time.Sleep(20 * time.Millisecond)
	}
	// Let any in-flight requeue settle before the pool is torn down.
	time.Sleep(200 * time.Millisecond)
	failed = int64(f608Failed())
	invocations = inv.Load()
	stop()
	return invocations, failed
}

func f608Failed() uint64 {
	_, _, failed, _, _, _, _, _, _, _, _, _ := cache.RefresherSnapshot()
	return failed
}

// --- F1 + F5 -----------------------------------------------------------------

func TestIssue608_F1_GroupCohortRefreshValidatesOverTheSATransport(t *testing.T) {
	cohorts := []struct {
		user  string
		group string
	}{{"cohort-a", "team-a"}, {"cohort-b", "team-b"}, {"cohort-c", "team-c"}}
	widgets := map[f608Kind][]string{
		f608Kinds[0]: {"f1-button-1", "f1-button-2"},
		f608Kinds[1]: {"f1-panel-1", "f1-panel-2"},
	}
	var groups []string
	for _, c := range cohorts {
		groups = append(groups, c.group)
	}
	e := f608Setup(t, groups, widgets)

	var keys []string
	for _, c := range cohorts {
		for k, names := range widgets {
			for _, n := range names {
				keys = append(keys, e.seedCell(t, f608CohortInputs(k, n, c.user, []string{c.group})))
			}
		}
	}
	if len(keys) != 12 {
		t.Fatalf("setup: %d cells, want K=3 x M=4 = 12", len(keys))
	}

	buildsBefore := idynamic.SADiscoveryStatsSnapshot().Builds
	invocations, failed := f608Run(t, e, "widgets", keys)

	n := 0
	for _, k := range keys {
		if e.accepted(k) {
			n++
		}
	}
	builds := idynamic.SADiscoveryStatsSnapshot().Builds - buildsBefore
	var impMeta, crdGets int
	for _, r := range e.srv.requests() {
		if r.isCRD() {
			crdGets++
		}
		if r.impersonated() && (r.isCRD() || r.isDiscovery()) {
			impMeta++
		}
	}
	t.Logf("F1: accepted=%d/12 invocations=%d refresher.failed=+%d sa_discovery.builds=+%d impersonated_meta_requests=%d crd_gets=%d",
		n, invocations, failed, builds, impMeta, crdGets)

	if n != len(keys) {
		t.Errorf("F1 RED: %d/%d cells accepted a re-Put. A refused cohort impersonation must never "+
			"reach CRD-schema validation: the schema is identity-free cluster metadata (#608)", n, len(keys))
	}
	if failed != 0 {
		t.Errorf("F1 RED: refresher.failed rose by %d, want 0", failed)
	}
	if builds > 1 {
		t.Errorf("F1 RED: the shared SA-discovery singleton was built %d times, want <= 1 — "+
			"it was rebuilt around per-refresh impersonated configs (keyed by pointer)", builds)
	}
	if impMeta != 0 {
		t.Errorf("F1 RED: %d discovery/CRD requests were sent IMPERSONATED, want 0", impMeta)
	}
	if crdGets > len(f608Kinds) {
		t.Errorf("F1: %d CRD GETs, want <= %d (one per kind; the memo serves the rest)", crdGets, len(f608Kinds))
	}
}

func TestIssue608_F5_DataReadsStayCohortScoped(t *testing.T) {
	// A username-only cohort is impersonable on 057 (`impersonate users`). Its
	// widget is NOT in the informer, so objects.Get falls through to the
	// apiserver — the data read #574 moved onto the cohort's identity.
	const user = "alice-f5"
	k := f608Kinds[0]
	e := f608Setup(t, nil, nil)
	e.srv.addObject(f608Widget(k, "f5-button"), k, "f5-button")
	key := e.seedCell(t, f608CohortInputs(k, "f5-button", user, nil))

	f608Run(t, e, "widgets", []string{key})

	objPath := fmt.Sprintf("/apis/%s/%s/namespaces/%s/%s/f5-button", f608Group, f608Version, f608NS, k.resource)
	var dataAsCohort, dataAsSA, metaImp int
	for _, r := range e.srv.requests() {
		switch {
		case r.path == objPath && r.user == user:
			dataAsCohort++
		case r.path == objPath:
			dataAsSA++
		case r.impersonated() && (r.isCRD() || r.isDiscovery()):
			metaImp++
		}
	}
	t.Logf("F5: accepted=%v data_as_cohort=%d data_as_sa=%d impersonated_meta=%d", e.accepted(key), dataAsCohort, dataAsSA, metaImp)
	if dataAsCohort == 0 || dataAsSA != 0 {
		t.Errorf("F5: the object fall-through must carry Impersonate-User=%s (got %d as cohort, %d as SA) — "+
			"#574's guarantee that background DATA reads are cohort-scoped", user, dataAsCohort, dataAsSA)
	}
	if metaImp != 0 {
		t.Errorf("F5 RED: %d discovery/CRD requests were impersonated, want 0", metaImp)
	}
	if !e.accepted(key) {
		t.Errorf("F5 RED: the cohort's refresh was not accepted — its CRD-schema check ran as the cohort, " +
			"which (like any tenant) may not read CRDs")
	}
}

// --- F2 ----------------------------------------------------------------------

func TestIssue608_F2_IdentityFreeCellNeverImpersonatesSnowplowItself(t *testing.T) {
	k := f608Kinds[0]
	e := f608Setup(t, nil, nil)
	e.srv.addObject(f608Widget(k, "f2-button"), k, "f2-button")
	key := e.seedCell(t, cache.ResolvedKeyInputs{
		CacheEntryClass: cache.CacheEntryClassWidgetContent,
		Group:           f608Group, Version: f608Version, Resource: k.resource,
		Namespace: f608NS, Name: "f2-button", PerPage: -1, Page: -1,
	})

	f608Run(t, e, cache.CacheEntryClassWidgetContent, []string{key})

	self := 0
	for _, r := range e.srv.requests() {
		if r.user == f608SAUser {
			self++
		}
	}
	t.Logf("F2: accepted=%v self_impersonated_requests=%d", e.accepted(key), self)
	if self != 0 {
		t.Errorf("F2 RED: %d requests carried Impersonate-User=%s. Impersonating your own ServiceAccount "+
			"can only lose rights; 057 refuses it outright", self, f608SAUser)
	}
	if !e.accepted(key) {
		t.Errorf("F2 RED: the identity-free cell was not refreshed")
	}
}

// --- F3 ----------------------------------------------------------------------

// f608LogCapture records every slog record at Warn+ while installed.
type f608LogCapture struct {
	mu   sync.Mutex
	recs []map[string]string
}

func (c *f608LogCapture) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelWarn }
func (c *f608LogCapture) Handle(_ context.Context, r slog.Record) error {
	m := map[string]string{"msg": r.Message}
	r.Attrs(func(a slog.Attr) bool { m[a.Key] = a.Value.String(); return true })
	c.mu.Lock()
	c.recs = append(c.recs, m)
	c.mu.Unlock()
	return nil
}
func (c *f608LogCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *f608LogCapture) WithGroup(string) slog.Handler      { return c }

func (c *f608LogCapture) find(msg string) []map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []map[string]string
	for _, r := range c.recs {
		if r["msg"] == msg {
			out = append(out, r)
		}
	}
	return out
}

func TestIssue608_F3_ForbiddenValidateIsClassifiedNotRelabelled(t *testing.T) {
	const user, group = "cohort-f3", "team-f3"
	k := f608Kinds[1]
	e := f608Setup(t, []string{group}, map[f608Kind][]string{k: {"f3-panel"}})
	e.srv.forbidCRDForSA.Store(true)
	in := f608CohortInputs(k, "f3-panel", user, []string{group})
	key := e.seedCell(t, in)

	// The classification, at the site that produces it.
	err := resolveAndPopulateL1(context.Background(), in, e.saEP, e.saRC)
	if err == nil {
		t.Fatalf("F3: want an error from a forbidden CRD-schema read, got nil")
	}
	var se *apierrors.StatusError
	if !errors.As(err, &se) {
		t.Fatalf("F3 RED: no *StatusError in the chain: %v", err)
	}
	gvk := schema.GroupVersionKind{Group: f608Group, Version: f608Version, Kind: k.kind}.String()
	t.Logf("F3: code=%d reason=%s err=%q", se.Status().Code, se.Status().Reason, err.Error())
	if se.Status().Code != http.StatusForbidden || !apierrors.IsForbidden(err) {
		t.Errorf("F3 RED: the validator's 403 surfaced as Code %d (%s) — widgets.Resolve relabelled it",
			se.Status().Code, se.Status().Reason)
	}
	if !strings.Contains(err.Error(), "validate status") || !strings.Contains(err.Error(), gvk) {
		t.Errorf("F3 RED: the error must name the stage and the GVK (%q): %q", gvk, err.Error())
	}

	// The refresher record: err_class + stage, closed set.
	capt := &f608LogCapture{}
	prev := slog.Default()
	slog.SetDefault(slog.New(capt))
	t.Cleanup(func() { slog.SetDefault(prev) })
	f608Run(t, e, "widgets", []string{key})
	slog.SetDefault(prev)

	recs := capt.find("refresher.refresh_failed")
	if len(recs) == 0 {
		t.Fatalf("F3: no refresher.refresh_failed record")
	}
	var buf bytes.Buffer
	for _, r := range recs {
		fmt.Fprintf(&buf, "%v; ", r)
	}
	t.Logf("F3: refresh_failed records: %s", buf.String())
	if recs[0]["err_class"] != "forbidden" || recs[0]["stage"] != "validate" {
		t.Errorf("F3 RED: refresh_failed err_class=%q stage=%q, want forbidden/validate",
			recs[0]["err_class"], recs[0]["stage"])
	}
	// Q2 ruling: a deterministic 403 is not retried through the 5-requeue budget.
	if len(recs) != 1 {
		t.Errorf("F3 RED: a forbidden refresh was attempted %d times, want 1 (non-retryable, Q2)", len(recs))
	}
}

// --- F4 ----------------------------------------------------------------------

func TestIssue608_F4_SharedSADiscoveryRefusesAnImpersonatingConfig(t *testing.T) {
	srv := newF608APIServer(t)
	base := &rest.Config{Host: srv.URL}
	first, err := idynamic.SharedSADiscoveryClient(base)
	if err != nil {
		t.Fatalf("F4: SA build: %v", err)
	}
	builds := idynamic.SADiscoveryStatsSnapshot().Builds

	imp := rest.CopyConfig(base)
	imp.Impersonate = rest.ImpersonationConfig{UserName: "cohort-a", Groups: []string{"team-a"}}
	if _, err := idynamic.SharedSADiscoveryClient(imp); err == nil {
		t.Errorf("F4 RED: SharedSADiscoveryClient accepted an impersonating config — its singleton " +
			"identity invariant (rc is ALWAYS the SA rest.Config) is not enforced")
	}
	if got := idynamic.SADiscoveryStatsSnapshot().Builds; got != builds {
		t.Errorf("F4 RED: an impersonating config paid a discovery build (%d -> %d)", builds, got)
	}
	again, err := idynamic.SharedSADiscoveryClient(base)
	if err != nil || again != first {
		t.Errorf("F4 RED: the SA singleton was replaced by the impersonating call (err=%v)", err)
	}
}
