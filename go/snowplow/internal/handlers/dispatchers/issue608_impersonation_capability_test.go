// issue608_impersonation_capability_test.go — #608 (b): the refresher must ASK
// before it impersonates a cohort's groups, and fall back to the re-gated
// ServiceAccount transport when the answer is no.
//
// THE PRODUCTION SHAPE (057, 1.12.43). The chart grants `impersonate users` to
// every name and `impersonate groups` to an EMPTY allowlist, i.e. not at all. The
// apiserver then refuses ANY request carrying an Impersonate-Group it does not
// permit, on every path. The startup probe asked only about users, so the
// refresher impersonated every grouped cohort, every such refresh was refused,
// and the cell stayed stale until TTL (26,462 refused stage reads).
//
// HOW REAL IS REAL. The fake apiserver below enforces exactly that policy on
// the Impersonate-* headers client-go puts on the wire, and answers
// SelfSubjectAccessReviews from the same policy table — so the production
// reviewer (cache.NewSSARImpersonationReviewer) asks a real HTTP question and
// the production dial is refused or accepted by a real HTTP answer. Every frame
// between the refresher queue and the wire is production code:
//
//	cache.EnqueueRefresh -> the real refresher pool -> resolveAndPopulateL1 ->
//	(the #608 capability check) -> resolveOnceProd -> objects.Get ->
//	getFromAPIServer -> client-go dynamic -> the httptest apiserver.
//
// ARMS
//
//	FALLBACK   K=3 grouped cohorts x M=2 RESTActions, groups NOT permitted:
//	           all 6 cells are re-Put fresh over the SA transport, no request
//	           carries Impersonate-Group, and the review count is bounded by the
//	           distinct principals (not by the cell count). RED on 9c233de9: every
//	           dial carries the refused group, 403s, and all 6 cells stay stale.
//	PERMITTED  same shape, groups permitted: every RA read carries the cohort's
//	           Impersonate-User + Impersonate-Group (the #574 guarantee), and no
//	           read goes out as the bare SA.
//	REGATE     on the fallback dial the ctx carries NO impersonated-dial marker,
//	           rbac.MustRegateSADial is TRUE, and a non-UAF branch-E read
//	           (a pod subresource) is refused by the re-gate BEFORE the SA dial:
//	           the refusal is recorded and the path never reaches the wire.
//	MEMO       an answer is reused until the RBAC snapshot generation moves; a
//	           grant added on the apiserver takes effect on the first refresh
//	           after the next snapshot publish, and not before.
package dispatchers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	authzv1 "k8s.io/api/authorization/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8sscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

const (
	i608NS    = "ns1"
	i608Token = "sa-token-608"
	i608Mark  = "I608-FRESH"
)

var i608RAGVR = schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}

// i608Req is one request as it arrived on the wire.
type i608Req struct {
	method, path, auth string
	impUser            string
	impGroups          []string
	refused            bool
}

// i608APIServer enforces 057's impersonation policy: users are permitted for
// every name; a group or a ServiceAccount is permitted only when listed. A
// request carrying a principal that is not permitted is refused with a 403 on
// EVERY path, discovery included — exactly what the apiserver's impersonation
// filter does before any handler runs.
type i608APIServer struct {
	srv *httptest.Server

	mu            sync.Mutex
	allowedGroups map[string]bool
	reqs          []i608Req
	objects       map[string]map[string]any

	ssar atomic.Int64
}

func (s *i608APIServer) allowGroups(groups ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range groups {
		s.allowedGroups[g] = true
	}
}

func (s *i608APIServer) permitted(resource, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch resource {
	case "users":
		return true // the shipped chart: `impersonate users`, no resourceNames
	case "groups":
		return name != "" && s.allowedGroups[name]
	default:
		return false // serviceaccounts: empty allowlist, as on 057
	}
}

func newI608APIServer(t *testing.T, objects map[string]map[string]any) *i608APIServer {
	t.Helper()
	s := &i608APIServer{allowedGroups: map[string]bool{}, objects: objects}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := i608Req{
			method: r.Method, path: r.URL.Path, auth: r.Header.Get("Authorization"),
			impUser: r.Header.Get("Impersonate-User"), impGroups: r.Header.Values("Impersonate-Group"),
		}
		if rec.impUser != "" || len(rec.impGroups) > 0 {
			ok := s.permitted("users", rec.impUser)
			if strings.HasPrefix(rec.impUser, "system:serviceaccount:") {
				ok = s.permitted("serviceaccounts", rec.impUser)
			}
			for _, g := range rec.impGroups {
				ok = ok && s.permitted("groups", g)
			}
			if !ok {
				rec.refused = true
				s.record(rec)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure",`+
					`"reason":"Forbidden","code":403,"message":"users \"x\" is forbidden: cannot impersonate"}`)
				return
			}
		}
		s.record(rec)

		if r.Method == http.MethodPost && r.URL.Path == "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews" {
			s.ssar.Add(1)
			// client-go sends built-in kinds as protobuf; decode whatever it sent.
			var review authzv1.SelfSubjectAccessReview
			body, _ := io.ReadAll(r.Body)
			if _, _, err := k8sscheme.Codecs.UniversalDeserializer().Decode(body, nil, &review); err != nil {
				t.Errorf("fake apiserver: undecodable SelfSubjectAccessReview (Content-Type %q): %v",
					r.Header.Get("Content-Type"), err)
			}
			ra := review.Spec.ResourceAttributes
			allowed := ra != nil && ra.Verb == "impersonate" && s.permitted(ra.Resource, ra.Name)
			review.APIVersion, review.Kind = "authorization.k8s.io/v1", "SelfSubjectAccessReview"
			review.Status.Allowed = allowed
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(&review)
			return
		}
		if doc, ok := i608DiscoveryDoc(r.URL.Path); ok {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, doc)
			return
		}
		if o, ok := s.objects[r.URL.Path]; ok {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(o)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *i608APIServer) record(r i608Req) {
	s.mu.Lock()
	s.reqs = append(s.reqs, r)
	s.mu.Unlock()
}

func (s *i608APIServer) requests(pathSuffix string) []i608Req {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []i608Req
	for _, r := range s.reqs {
		if strings.HasSuffix(r.path, pathSuffix) {
			out = append(out, r)
		}
	}
	return out
}

func (s *i608APIServer) reset() {
	s.mu.Lock()
	s.reqs = nil
	s.mu.Unlock()
}

func i608RA(name string, api []any) map[string]any {
	return map[string]any{
		"apiVersion": "templates.krateo.io/v1",
		"kind":       "RestAction",
		"metadata":   map[string]any{"namespace": i608NS, "name": name},
		"spec":       map[string]any{"api": api, "filter": `{"fresh":"` + i608Mark + `"}`},
	}
}

func i608RAPath(name string) string {
	return "/apis/templates.krateo.io/v1/namespaces/" + i608NS + "/restactions/" + name
}

// i608Cohort is one cell representative: a user and one group.
type i608Cohort struct{ user, group string }

// i608Watcher publishes a synced RBAC snapshot in which every cohort's GROUP is
// granted `get restactions` (so the SA fallback's re-gate PASSES the RA read and
// the cell can be refreshed) but NOT pods (so a pod-subresource step is a read
// the re-gate must refuse). restactions holds no objects, so objects.Get misses
// the informer and falls through to the apiserver dial under test.
func i608Watcher(t *testing.T, cohorts []i608Cohort) *cache.ResourceWatcher {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		i608RAGVR: "RestActionList",
		{Group: "", Version: "v1", Resource: "pods"}:                                         "PodList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	// One binding PER cohort, so each cohort is its own RBAC class and therefore
	// its own L1 key: cohorts sharing a binding set share a cell by design, and
	// would collapse K x M back to M.
	seed := []runtime.Object{
		&rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: "i608-ra-getter"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"templates.krateo.io"}, Resources: []string{"restactions"}, Verbs: []string{"get"}}},
		},
	}
	for _, c := range cohorts {
		seed = append(seed, &rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: "i608-ra-getter-" + c.group},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.GroupKind, APIGroup: rbacv1.GroupName, Name: c.group}},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: "i608-ra-getter"},
		})
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: rw=%v err=%v", rw, err)
	}
	t.Cleanup(rw.Stop)
	sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(sctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() {
		cache.SetGlobal(nil)
		cache.PublishRBACSnapshotForTest(nil)
	})
	return rw
}

// i608Env resets every process-global the arms touch and turns impersonation
// on, as the startup probe does on 057 (users granted).
func i608Env(t *testing.T) {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_REFRESHER_BASE_DELAY_MS", "1")
	t.Setenv("RESOLVED_CACHE_REFRESHER_MAX_DELAY_MS", "2")
	t.Setenv("RESOLVED_CACHE_REFRESHER_RATE_FLOOR_SECONDS", "0")
	t.Setenv("RESOLVED_CACHE_REFRESHER_PARALLELISM", "4")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefresherForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	t.Cleanup(cache.ResetDepsForTest)
	t.Cleanup(cache.ResetRefresherForTest)
	t.Cleanup(cache.SetImpersonationAvailableForTest(true))
	i608ResetCapability(t)
}

// i608Cell seeds one stale restactions cell for (cohort, ra), minted against
// the live snapshot exactly as a cold dispatch mints it.
func i608Cell(t *testing.T, store *cache.ResolvedCacheStore, c i608Cohort, ra string) (string, cache.ResolvedKeyInputs) {
	t.Helper()
	cls := rbac.IdentityClassOf(jwtutil.UserInfo{Username: c.user, Groups: []string{c.group}})
	in := cache.ResolvedKeyInputs{
		CacheEntryClass:        "restactions",
		Group:                  i608RAGVR.Group,
		Version:                i608RAGVR.Version,
		Resource:               i608RAGVR.Resource,
		Namespace:              i608NS,
		Name:                   ra,
		BindingUID:             "uid-608-" + ra,
		RepresentativeUsername: c.user,
		RepresentativeGroups:   []string{c.group},
		SubjectBindingSet:      cls.SubjectBindingSet,
		RBACSubGen:             cls.RBACSubGen,
	}
	key := cache.ComputeKey(in)
	store.PutResolved(fixtureStoreVerdict, key, &cache.ResolvedEntry{RawJSON: []byte(`{"stale":"prior"}`), Inputs: &in})
	return key, in
}

func i608Fresh(store *cache.ResolvedCacheStore, key string) bool {
	e, ok := store.Get(key)
	return ok && strings.Contains(string(e.RawJSON), i608Mark)
}

// i608RunRefresher drives the REAL refresher pool over keys and waits until
// every cell is fresh or every key has exhausted its requeue budget.
func i608RunRefresher(t *testing.T, store *cache.ResolvedCacheStore, keys []string, saEP *endpoints.Endpoint, saRC *rest.Config) {
	t.Helper()
	var invocations atomic.Int64
	cache.RegisterRefreshFunc("restactions", func(ctx context.Context, _ string, in cache.ResolvedKeyInputs) error {
		invocations.Add(1)
		return resolveAndPopulateL1(ctx, in, saEP, saRC)
	})
	ctx, cancel := context.WithCancel(context.Background())
	cache.StartRefresher(ctx)
	for _, k := range keys {
		cache.EnqueueRefresh(k)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, k := range keys {
			if !i608Fresh(store, k) {
				all = false
			}
		}
		if all || invocations.Load() >= int64(len(keys)*(i187MaxRequeues+1)) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond) // let a just-finished worker's Put land
	cancel()
	cache.ResetRefresherForTest()
}

func i608Shape() ([]i608Cohort, []string) {
	return []i608Cohort{{"u608-a", "g608-a"}, {"u608-b", "g608-b"}, {"u608-c", "g608-c"}},
		[]string{"ra608-one", "ra608-two"}
}

func i608Fixture(t *testing.T) (*i608APIServer, *cache.ResolvedCacheStore, []string, []i608Cohort, *endpoints.Endpoint, *rest.Config) {
	t.Helper()
	i608Env(t)
	cohorts, ras := i608Shape()
	objs := map[string]map[string]any{}
	for _, ra := range ras {
		objs[i608RAPath(ra)] = i608RA(ra, []any{})
	}
	srv := newI608APIServer(t, objs)
	i608Watcher(t, cohorts)
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatal("resolved cache nil under RESOLVED_CACHE_ENABLED=true")
	}
	var keys []string
	for _, c := range cohorts {
		for _, ra := range ras {
			k, _ := i608Cell(t, store, c, ra)
			keys = append(keys, k)
		}
	}
	distinct := map[string]bool{}
	for _, k := range keys {
		distinct[k] = true
	}
	if len(distinct) != len(cohorts)*len(ras) {
		t.Fatalf("setup: %d distinct cells, want K x M = %d — the cohorts collapsed into one RBAC class",
			len(distinct), len(cohorts)*len(ras))
	}
	saRC := &rest.Config{Host: srv.srv.URL, BearerToken: i608Token}
	i608InstallCapabilityCheck(t, saRC)
	return srv, store, keys, cohorts, &endpoints.Endpoint{ServerURL: srv.srv.URL, Token: i608Token}, saRC
}

// --- FALLBACK — refused groups -> re-gated SA, and the cells are FRESH --------

func TestIssue608_RefusedGroupImpersonation_FallsBackToSAAndRefreshes(t *testing.T) {
	srv, store, keys, cohorts, saEP, saRC := i608Fixture(t)

	i608RunRefresher(t, store, keys, saEP, saRC)

	var stale []string
	for _, k := range keys {
		if !i608Fresh(store, k) {
			stale = append(stale, k)
		}
	}
	raReqs := srv.requests("/restactions/ra608-one")
	raReqs = append(raReqs, srv.requests("/restactions/ra608-two")...)
	refused := 0
	for _, r := range srv.requests("") {
		if r.refused {
			refused++
		}
	}
	if len(stale) > 0 {
		t.Fatalf("#608 RED: %d/%d cells (K=%d grouped cohorts x M=2 RESTActions) are still STALE after the "+
			"refresher ran. The apiserver refuses every impersonated group here (057's empty allowlist), and "+
			"%d requests were refused at the wire (the first impersonated hop is discovery, so the RA GET is "+
			"never reached: %d RA reads). The refresher must ask BEFORE impersonating and fall back to the "+
			"re-gated SA transport, so the refresh runs instead of dialling a certain 403.",
			len(stale), len(keys), len(cohorts), refused, len(raReqs))
	}
	for _, r := range raReqs {
		if len(r.impGroups) > 0 || r.impUser != "" {
			t.Fatalf("#608: an RA read went out impersonating %q/%v although the capability check had "+
				"answered no — the fallback must dial as the bare SA", r.impUser, r.impGroups)
		}
	}
	st := cache.ImpersonationCapabilityStats()
	if st["impersonation_fallback_sa"] < uint64(len(keys)) || st["impersonation_permitted"] != 0 {
		t.Fatalf("#608: counters do not describe the fallback: %v (want fallback_sa >= %d, permitted 0)", st, len(keys))
	}
	// Bounded by PRINCIPALS, not cells: users kind-wide (1) + groups kind-wide
	// (1) + one per distinct group (K). M does not multiply it.
	if got, want := srv.ssar.Load(), int64(2+len(cohorts)); got != want {
		t.Fatalf("#608: %d SelfSubjectAccessReviews for %d cells, want exactly %d (one per distinct principal "+
			"plus the two kind-wide questions) — the memo is not deduplicating", got, len(keys), want)
	}
}

// --- PERMITTED — the #574 guarantee survives ---------------------------------

func TestIssue608_PermittedGroupImpersonation_StillImpersonates(t *testing.T) {
	srv, store, keys, cohorts, saEP, saRC := i608Fixture(t)
	for _, c := range cohorts {
		srv.allowGroups(c.group)
	}

	i608RunRefresher(t, store, keys, saEP, saRC)

	for _, k := range keys {
		if !i608Fresh(store, k) {
			t.Fatalf("#608/#574: a cell is stale although its cohort's impersonation is permitted")
		}
	}
	byUser := map[string]string{}
	for _, c := range cohorts {
		byUser[c.user] = c.group
	}
	raReqs := append(srv.requests("/restactions/ra608-one"), srv.requests("/restactions/ra608-two")...)
	if len(raReqs) == 0 {
		t.Fatal("no RA read reached the apiserver — the arm observes nothing")
	}
	for _, r := range raReqs {
		g, ok := byUser[r.impUser]
		if !ok || len(r.impGroups) != 1 || r.impGroups[0] != g {
			t.Fatalf("#574 REGRESSION: an RA read went out as %q/%v; with the grant present every refresh "+
				"must read AS its cohort (Impersonate-User + that cohort's Impersonate-Group)", r.impUser, r.impGroups)
		}
	}
	st := cache.ImpersonationCapabilityStats()
	if st["impersonation_fallback_sa"] != 0 || st["impersonation_permitted"] < uint64(len(keys)) {
		t.Fatalf("#608: counters %v, want permitted >= %d and fallback_sa 0", st, len(keys))
	}
}

// --- REGATE — the fallback dial is the pre-#574 re-gated SA dial --------------

func TestIssue608_FallbackDialIsRegated(t *testing.T) {
	i608Env(t)
	cohort := i608Cohort{"u608-r", "g608-r"}
	const podPath = "/api/v1/namespaces/" + i608NS + "/pods/p608/status"
	srv := newI608APIServer(t, map[string]map[string]any{
		i608RAPath("ra608-sub"): i608RA("ra608-sub", []any{map[string]any{
			"name": "podstatus", "path": podPath, "verb": "GET", "filter": ".podstatus",
		}}),
		podPath: {"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{"namespace": i608NS, "name": "p608"},
			"status":   map[string]any{"phase": "Running"}},
	})
	i608Watcher(t, []i608Cohort{cohort})
	store := cache.ResolvedCache()
	_, in := i608Cell(t, store, cohort, "ra608-sub")
	saRC := &rest.Config{Host: srv.srv.URL, BearerToken: i608Token}
	saEP := &endpoints.Endpoint{ServerURL: srv.srv.URL, Token: i608Token}
	i608InstallCapabilityCheck(t, saRC)

	var regated, marked, seen atomic.Int32
	var stageErr atomic.Value
	t.Cleanup(setResolveOnceForTest(func(ctx context.Context, in cache.ResolvedKeyInputs) ([]byte, error) {
		seen.Add(1)
		if cache.ImpersonatedDialFromContext(ctx) {
			marked.Add(1)
		}
		if rbac.MustRegateSADial(cache.WithBackgroundResolve(ctx)) {
			regated.Add(1)
		}
		out, err := resolveOnceProd(ctx, in)
		if sink := cache.StageErrorSinkFromContext(ctx); sink != nil && sink.Count() > 0 {
			_, msg := sink.Sample()
			stageErr.Store(msg)
		}
		return out, err
	}))

	_ = resolveAndPopulateL1(context.Background(), in, saEP, saRC)

	if seen.Load() == 0 {
		t.Fatal("resolve seam never ran — the arm observes nothing")
	}
	if marked.Load() != 0 {
		t.Fatalf("#608/#615: the refresh ctx carries the impersonated-dial marker although the cohort's group is "+
			"refused; the marker exempts it from MustRegateSADial (%d of %d resolves)", marked.Load(), seen.Load())
	}
	if regated.Load() != seen.Load() {
		t.Fatalf("#608: MustRegateSADial is false on %d of %d fallback resolves — the SA fallback must be "+
			"re-gated exactly as pre-#574", seen.Load()-regated.Load(), seen.Load())
	}
	for _, r := range srv.requests("/pods/p608/status") {
		t.Fatalf("#608: the fallback dialled a non-UAF branch-E read (%s %s auth=%q imp=%q/%v). The re-gate "+
			"must refuse it BEFORE the SA dial.", r.method, r.path, r.auth, r.impUser, r.impGroups)
	}
	// Positive evidence, so the wire assertion above cannot pass merely because
	// the step was never reached: the step WAS reached and the re-gate refused it.
	msg, _ := stageErr.Load().(string)
	if !strings.Contains(msg, "branch-E SA-endpoint dial re-gated") {
		t.Fatalf("#608: no branch-E re-gate refusal was recorded for the pod-subresource step (stage error %q); "+
			"the arm cannot tell a refusal from a step that never ran", msg)
	}
}

// --- MEMO — reuse until the RBAC generation moves, then re-ask ----------------

func TestIssue608_CapabilityMemoInvalidatedByRBACChange(t *testing.T) {
	i608Env(t)
	cohort := i608Cohort{"u608-m", "g608-m"}
	srv := newI608APIServer(t, map[string]map[string]any{i608RAPath("ra608-m"): i608RA("ra608-m", []any{})})
	rw := i608Watcher(t, []i608Cohort{cohort})
	store := cache.ResolvedCache()
	saRC := &rest.Config{Host: srv.srv.URL, BearerToken: i608Token}
	saEP := &endpoints.Endpoint{ServerURL: srv.srv.URL, Token: i608Token}
	i608InstallCapabilityCheck(t, saRC)

	refresh := func() i608Req {
		t.Helper()
		key, in := i608Cell(t, store, cohort, "ra608-m")
		srv.reset()
		if err := resolveAndPopulateL1(context.Background(), in, saEP, saRC); err != nil {
			t.Fatalf("refresh: %v", err)
		}
		if !i608Fresh(store, key) {
			t.Fatalf("cell not fresh after refresh")
		}
		rs := srv.requests("/restactions/ra608-m")
		if len(rs) == 0 {
			t.Fatalf("no RA read on the wire")
		}
		return rs[len(rs)-1]
	}

	if r := refresh(); r.impUser != "" {
		t.Fatalf("setup: group not allowlisted yet, but the read impersonated %q/%v", r.impUser, r.impGroups)
	}
	reviews := srv.ssar.Load()

	// The chart rolls and the ClusterRole now allowlists the group. Until the
	// RBAC snapshot publishes, the memo still holds the old answer.
	srv.allowGroups(cohort.group)
	if r := refresh(); r.impUser != "" {
		t.Fatalf("#608: the memo was bypassed — the read impersonated before any RBAC generation change")
	}
	if srv.ssar.Load() != reviews {
		t.Fatalf("#608: %d new reviews with the RBAC generation unchanged; the memo must answer", srv.ssar.Load()-reviews)
	}

	// The ClusterRole change publishes a new snapshot: the next refresh re-asks
	// and reads AS the cohort.
	cache.RebuildRBACSnapshotForTest(rw)
	r := refresh()
	if r.impUser != cohort.user || len(r.impGroups) != 1 || r.impGroups[0] != cohort.group {
		t.Fatalf("#608: after the RBAC generation moved, the read went out as %q/%v; want %q/[%s]. The memo "+
			"must be invalidated by the RBAC snapshot generation", r.impUser, r.impGroups, cohort.user, cohort.group)
	}
	if cache.ImpersonationCapabilityStats()["memo_invalidations"] == 0 {
		t.Fatalf("#608: memo_invalidations did not move")
	}
}

// i608ResetCapability clears the capability memo, reviewer and counters.
func i608ResetCapability(t *testing.T) {
	t.Helper()
	cache.ResetImpersonationCapabilityForTest()
	t.Cleanup(cache.ResetImpersonationCapabilityForTest)
}

// i608InstallCapabilityCheck installs the PRODUCTION reviewer against the fake
// apiserver, as main does against the real one.
func i608InstallCapabilityCheck(t *testing.T, saRC *rest.Config) {
	t.Helper()
	fn, err := cache.NewSSARImpersonationReviewer(saRC)
	if err != nil {
		t.Fatalf("NewSSARImpersonationReviewer: %v", err)
	}
	cache.SetImpersonationReviewer(fn)
}

// i608DiscoveryDoc is the canned discovery client-go's RESTMapper needs to
// resolve restactions and pods (the same documents as the tagged #268 harness,
// which this untagged file cannot reach).
func i608DiscoveryDoc(path string) (string, bool) {
	switch path {
	case "/api":
		return `{"kind":"APIVersions","versions":["v1"],"serverAddressByClientCIDRs":[]}`, true
	case "/api/v1":
		return `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"v1","resources":[` +
			`{"name":"pods","singularName":"pod","namespaced":true,"kind":"Pod","verbs":["get","list"]},` +
			`{"name":"pods/status","singularName":"","namespaced":true,"kind":"Pod","verbs":["get"]}` +
			`]}`, true
	case "/apis":
		return `{"kind":"APIGroupList","apiVersion":"v1","groups":[` +
			`{"name":"templates.krateo.io","versions":[{"groupVersion":"templates.krateo.io/v1","version":"v1"}],` +
			`"preferredVersion":{"groupVersion":"templates.krateo.io/v1","version":"v1"}}` +
			`]}`, true
	case "/apis/templates.krateo.io/v1":
		return `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"templates.krateo.io/v1","resources":[` +
			`{"name":"restactions","singularName":"restaction","namespaced":true,"kind":"RestAction","verbs":["get","list"]}` +
			`]}`, true
	}
	return "", false
}
