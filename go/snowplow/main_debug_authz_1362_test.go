// main_debug_authz_1362_test.go — #1362: the /debug/* surface is AUTHORIZED
// through Kubernetes RBAC, not just authenticated.
//
// THE DEFECT THIS PINS. Before #1362 every /debug route passed any valid
// Krateo JWT (debug_routes.go: RefreshAuth only). /debug/apistage, among
// others, reports the coordinates and item counts of EVERY identity's resident
// cache entries, so any logged-in user could learn that objects exist, and
// what they are called, even where their own RBAC denies them.
//
// THE ARMS (hermetic: httptest, a test RS256 key, and a cache=on
// ResourceWatcher over a dynamic fake seeded with real typed RBAC objects, so
// the snapshot the gate reads is built by production code):
//
//	B1  every registered route, driven with its DECLARED method: an
//	    authenticated identity WITHOUT the grant gets 403 (several shapes,
//	    including one holding a wildcard RESOURCE reader and the owner of a
//	    cache entry); one WITH the chart's grant gets 200; and cluster-admin,
//	    which carries nonResourceURLs ["*"], gets 200.
//	B2  the verb is the HTTP method: a get-only grant reads the toggle and
//	    cannot flip it.
//	B3  an undecidable authorization is 503 with a named reason, never 200: no
//	    snapshot yet, no watcher, cache off.
//	B4  an authorized operator sees on /debug/apistage an entry created by a
//	    DIFFERENT identity, so the grant is meaningful, and that identity cannot
//	    read its own entry's metadata without the grant.
//	B5  no /debug route can be mounted without the gate. At runtime,
//	    routeTable panics on a /debug pattern outside the gated lane, whatever
//	    form the pattern takes. Structurally, by type, nothing in package main
//	    registers on an *http.ServeMux except routeTable, the one /debug lane is
//	    registerDebugRoutes' mount, and `gated` contains DebugAuthz.
//
// The grant the arms bind is PARSED FROM THE CHART
// (helm/snowplow/templates/clusterrole-debug.yaml), so B1 also proves the
// shipped ClusterRole opens every route.

package main

import (
	"context"
	"encoding/json"
	"go/ast"
	"go/token"
	"go/types"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/testsupport/fakewatch"

	"golang.org/x/tools/go/packages"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"sigs.k8s.io/yaml"
)

// i1362ChartRole is the path of the ClusterRole the chart ships for /debug.
const i1362ChartRole = "../../helm/snowplow/templates/clusterrole-debug.yaml"

// i1362ChartRules parses the rules of the chart's /debug ClusterRole. The
// template's only Helm expressions are in metadata, so the `rules:` block is
// plain YAML.
func i1362ChartRules(t *testing.T) []rbacv1.PolicyRule {
	t.Helper()
	raw, err := os.ReadFile(i1362ChartRole)
	if err != nil {
		t.Fatalf("read the chart's /debug ClusterRole: %v", err)
	}
	_, block, ok := strings.Cut(string(raw), "\nrules:\n")
	if !ok {
		t.Fatalf("%s has no top-level rules: block", i1362ChartRole)
	}
	if strings.Contains(block, "{{") {
		t.Fatalf("%s: the rules block is templated; this parser reads it as plain YAML", i1362ChartRole)
	}
	var doc struct {
		Rules []rbacv1.PolicyRule `json:"rules"`
	}
	if err := yaml.Unmarshal([]byte("rules:\n"+block), &doc); err != nil {
		t.Fatalf("parse %s rules: %v", i1362ChartRole, err)
	}
	if len(doc.Rules) == 0 || len(doc.Rules[0].NonResourceURLs) == 0 {
		t.Fatalf("PRE: the chart's /debug ClusterRole grants no nonResourceURLs: %+v", doc.Rules)
	}
	return doc.Rules
}

func i1362ClusterRole(name string, rules ...rbacv1.PolicyRule) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Rules:      rules,
	}
}

func i1362CRB(name, role string, subject rbacv1.Subject) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Subjects:   []rbacv1.Subject{subject},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role},
	}
}

func i1362RB(ns, name, role string, subject rbacv1.Subject) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Subjects:   []rbacv1.Subject{subject},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: role},
	}
}

func i1362Group(name string) rbacv1.Subject {
	return rbacv1.Subject{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: name}
}

func i1362User(name string) rbacv1.Subject {
	return rbacv1.Subject{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: name}
}

// i1362Identity is one caller: the JWT it presents carries user and groups.
type i1362Identity struct {
	user   string
	groups []string
}

var (
	// operator holds the chart's grant through a group binding.
	i1362Operator = i1362Identity{"op-1362", []string{"operators"}}
	// admin is the shape of the bench's `admin` user: a group bound to the
	// built-in cluster-admin ClusterRole.
	i1362Admin = i1362Identity{"admin-1362", []string{"admins"}}
	// reader holds a get-only variant of the grant.
	i1362Reader = i1362Identity{"reader-1362", nil}
	// visitor is authenticated and holds a cluster-wide wildcard RESOURCE
	// reader, which must not open a single /debug path.
	i1362Visitor = i1362Identity{"visitor-1362", []string{"devs"}}
	// alice owns the cache entry B4 reads, and holds no /debug grant.
	i1362Alice = i1362Identity{"alice-1362", []string{"alice-team"}}
)

// i1362World is the RBAC the arms run against.
func i1362World(t *testing.T) []runtime.Object {
	t.Helper()
	clusterAdmin := i1362ClusterRole("cluster-admin-1362",
		rbacv1.PolicyRule{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"*"}},
		rbacv1.PolicyRule{NonResourceURLs: []string{"*"}, Verbs: []string{"*"}},
	)
	return []runtime.Object{
		i1362ClusterRole("chart-debug-1362", i1362ChartRules(t)...),
		i1362CRB("operators-debug", "chart-debug-1362", i1362Group("operators")),
		clusterAdmin,
		i1362CRB("admins-cluster-admin", "cluster-admin-1362", i1362Group("admins")),
		i1362ClusterRole("debug-get-only-1362",
			rbacv1.PolicyRule{NonResourceURLs: []string{"/debug/*"}, Verbs: []string{"get"}}),
		i1362CRB("reader-debug-get", "debug-get-only-1362", i1362User("reader-1362")),
		i1362ClusterRole("wildcard-reader-1362",
			rbacv1.PolicyRule{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"get", "list", "watch"}}),
		i1362CRB("devs-wildcard-reader", "wildcard-reader-1362", i1362Group("devs")),
		// alice may read her own namespace, which RoleBindings can grant; no
		// RoleBinding can grant a non-resource URL.
		i1362RB("alice-ns", "alice-admin", "cluster-admin-1362", i1362Group("alice-team")),
	}
}

func i1362ListKinds() map[schema.GroupVersionResource]string {
	return map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
}

// i1362Watcher starts a cache=on ResourceWatcher over the seeded RBAC, waits
// until it is quiet, publishes its snapshot synchronously, and installs it as
// the global watcher the evaluator reads (the internal/rbac/evaltest harness,
// in this package).
func i1362Watcher(t *testing.T, seed ...runtime.Object) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	rbac.ResetAuthzMemoForTest()
	t.Cleanup(rbac.ResetAuthzMemoForTest)
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)

	sch := runtime.NewScheme()
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1.AddToScheme: %v", err)
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, i1362ListKinds(), seed...)
	watches := fakewatch.Install(dyn)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v, %v", rw, err)
	}
	t.Cleanup(rw.Stop)
	if err := rw.WaitForCacheSync(context.Background(), 10*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	prev := cache.Global()
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(prev) })
	if err := watches.WaitFor(time.Minute, "roles", "rolebindings", "clusterroles", "clusterrolebindings"); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if err := rw.WaitRBACQuiescentForTest(time.Minute); err != nil {
		t.Fatalf("settle: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	if cache.RBACSnapshotForTest() == nil {
		t.Fatal("PRE: no RBAC snapshot published")
	}
	return rw
}

// i1362Token mints the JWT the identity presents.
func i1362Token(t *testing.T, id i1362Identity) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username:   id.user,
		Groups:     id.groups,
		Duration:   time.Hour,
		KeyID:      debugAuthTestKeyID,
		PrivateKey: debugAuthTestPrivateKey,
	})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	return tok
}

// i1362Drive drives one request and returns the recorder.
func i1362Drive(t *testing.T, h http.Handler, method, path, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "http://snowplow.example"+path, nil)
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// i1362Status decodes the plumbing Status every non-2xx route answers with.
type i1362Status struct {
	Kind    string `json:"kind"`
	Status  string `json:"status"`
	Message string `json:"message"`
	Reason  string `json:"reason"`
	Code    int    `json:"code"`
}

func i1362DecodeStatus(t *testing.T, rec *httptest.ResponseRecorder) i1362Status {
	t.Helper()
	var s i1362Status
	if err := json.Unmarshal(rec.Body.Bytes(), &s); err != nil {
		t.Fatalf("body is not a Status (%v): %q", err, rec.Body.String())
	}
	return s
}

// i1362AuthorizedPaths is debugProbePaths with the two long-running
// collectors bounded, so the authorized arm can drive them to their 200.
// pprof.Profile's floor is one second; pprof.Trace takes fractions.
func i1362AuthorizedPaths() map[string]string {
	out := map[string]string{}
	for p, path := range debugProbePaths {
		out[p] = path
	}
	out["GET /debug/pprof/profile"] = "/debug/pprof/profile?seconds=1"
	out["GET /debug/pprof/trace"] = "/debug/pprof/trace?seconds=0.05"
	return out
}

// ─── B1 — every route: no grant → 403, the grant → 200 ──────────────────────

func TestIssue1362_EveryRoute_DeniesWithoutTheGrantAdmitsWithIt(t *testing.T) {
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	i1362Watcher(t, i1362World(t)...)
	_, h := buildProdDebugMux(t)
	paths := i1362AuthorizedPaths()

	for _, pattern := range debugRoutePatterns {
		method := methodOf(t, pattern)
		path, ok := paths[pattern]
		if !ok {
			t.Fatalf("pattern %q has no probe path", pattern)
		}
		for _, id := range []i1362Identity{i1362Visitor, i1362Alice} {
			rec := i1362Drive(t, h, method, path, i1362Token(t, id))
			if rec.Code != http.StatusForbidden {
				t.Errorf("%s as %s (authenticated, no grant) = %d, want 403: %s",
					pattern, id.user, rec.Code, rec.Body.String())
				continue
			}
			// The same Status shape every other route's 403 has (plumbing
			// response.Forbidden), naming the verb and path that were refused.
			s := i1362DecodeStatus(t, rec)
			wantMsg := `cannot ` + strings.ToLower(method) + ` path "` + strings.SplitN(path, "?", 2)[0] + `"`
			if s.Kind != "Status" || s.Status != "Failure" || s.Reason != "Forbidden" || s.Code != 403 ||
				!strings.Contains(s.Message, wantMsg) {
				t.Errorf("%s as %s: 403 body %+v, want a Forbidden Status containing %q", pattern, id.user, s, wantMsg)
			}
		}
		for _, id := range []i1362Identity{i1362Operator, i1362Admin} {
			if rec := i1362Drive(t, h, method, path, i1362Token(t, id)); rec.Code != http.StatusOK {
				t.Errorf("%s as %s (granted) = %d, want 200: %s", pattern, id.user, rec.Code, rec.Body.String())
			}
		}
	}
}

// ─── B2 — the verb is the method ────────────────────────────────────────────

func TestIssue1362_VerbIsTheHTTPMethod(t *testing.T) {
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	i1362Watcher(t, i1362World(t)...)
	_, h := buildProdDebugMux(t)
	tok := i1362Token(t, i1362Reader)

	if rec := i1362Drive(t, h, http.MethodGet, "/debug/shadow-parity", tok); rec.Code != http.StatusOK {
		t.Errorf("GET /debug/shadow-parity with a get-only grant = %d, want 200", rec.Code)
	}
	if rec := i1362Drive(t, h, http.MethodPost, "/debug/shadow-parity?enabled=true", tok); rec.Code != http.StatusForbidden {
		t.Errorf("POST /debug/shadow-parity with a get-only grant = %d, want 403", rec.Code)
	}
	if rbac.ShadowParityEnabled() {
		t.Error("the refused POST flipped the toggle")
	}
}

// ─── B3 — undecidable → 503 with a named reason, never 200 ──────────────────

func TestIssue1362_UndecidableAuthorizationIs503(t *testing.T) {
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })

	assert503 := func(t *testing.T, h http.Handler, reason string) {
		t.Helper()
		tok := i1362Token(t, i1362Operator)
		for _, pattern := range debugRoutePatterns {
			path := debugProbePaths[pattern]
			rec := i1362Drive(t, h, methodOf(t, pattern), path, tok)
			if rec.Code != http.StatusServiceUnavailable {
				t.Errorf("%s = %d, want 503 (%s): %s", pattern, rec.Code, reason, rec.Body.String())
				continue
			}
			s := i1362DecodeStatus(t, rec)
			// The named reason ONLY: the evaluator's or the review's own
			// error text stays in the server log (#1362 review finding 5).
			want := "debug authorization unavailable (" + reason + ")"
			if s.Code != 503 || s.Reason != "ServiceUnavailable" || s.Message != want {
				t.Errorf("%s: 503 body %+v, want a ServiceUnavailable Status with message exactly %q", pattern, s, want)
			}
		}
	}

	t.Run("cache-on/snapshot-not-built", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "true")
		prevW, prevS := cache.Global(), cache.RBACSnapshotForTest()
		cache.SetGlobal(&cache.ResourceWatcher{})
		cache.PublishRBACSnapshotForTest(nil)
		t.Cleanup(func() { cache.SetGlobal(prevW); cache.PublishRBACSnapshotForTest(prevS) })
		_, h := buildProdDebugMux(t)
		assert503(t, h, middleware.DebugAuthzReasonSnapshotNotBuilt)
	})

	t.Run("cache-on/watcher-not-wired", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "true")
		prev := cache.Global()
		cache.SetGlobal(nil)
		t.Cleanup(func() { cache.SetGlobal(prev) })
		_, h := buildProdDebugMux(t)
		assert503(t, h, middleware.DebugAuthzReasonWatcherNotWired)
	})

	// With the cache off the authorizer is the requester's own SSAR, which
	// needs the clientconfig endpoint this route deliberately never reads.
	t.Run("cache-off/review-unavailable", func(t *testing.T) {
		t.Setenv("CACHE_ENABLED", "false")
		_, h := buildProdDebugMux(t)
		assert503(t, h, middleware.DebugAuthzReasonReviewUnavailable)
	})
}

// ─── B4 — the operator sees another identity's entry ────────────────────────

func TestIssue1362_ApistageShowsTheOperatorAnotherIdentitysEntry(t *testing.T) {
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	i1362Watcher(t, i1362World(t)...)
	cache.CompleteRBACSnapshotForTest(t)
	store := cache.ResolvedCache()
	if store == nil {
		t.Fatal("PRE: ResolvedCache() is nil with CACHE_ENABLED=true")
	}

	// The entry alice's request would have written: her identity in the key
	// inputs, and an object only her RoleBinding lets her read.
	in := cache.ResolvedKeyInputs{
		CacheEntryClass:        "restactions",
		Group:                  "",
		Version:                "v1",
		Resource:               "secrets",
		Namespace:              "alice-ns",
		Name:                   "alice-private-1362",
		BindingUID:             "R:alice-ns/alice-admin",
		RepresentativeUsername: i1362Alice.user,
		RepresentativeGroups:   i1362Alice.groups,
	}
	key := cache.ComputeKey(in)
	stamped := in
	_, g := cache.WithChildPutGateSinks(context.Background())
	store.PutResolved(cache.PutGateVerdict(g, "restactions"), key, &cache.ResolvedEntry{RawJSON: []byte(`{"secret":"x"}`), Inputs: &stamped})
	t.Cleanup(func() { store.DeleteForTest(key) })

	_, h := buildProdDebugMux(t)

	// The operator's RBAC cannot read alice-ns at all: the grant alone is what
	// shows the entry.
	if ok, _, _ := rbac.EvaluateRBAC(context.Background(), rbac.ObjectCheck(i1362Operator.user, i1362Operator.groups,
		"get", rbac.ObjectTarget{Resource: "secrets", Namespace: "alice-ns", Name: "alice-private-1362"})); ok {
		t.Fatal("PRE: the operator may read alice's secret, so this arm would not show a cross-identity read")
	}

	var body struct {
		Count   int `json:"count"`
		Entries []struct {
			KeyHash   string `json:"keyHash"`
			Namespace string `json:"namespace"`
			Name      string `json:"name"`
		} `json:"entries"`
	}
	for _, path := range []string{"/debug/apistage", "/debug/apistage?key_hash=" + key} {
		rec := i1362Drive(t, h, http.MethodGet, path, i1362Token(t, i1362Operator))
		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s as the operator = %d, want 200: %s", path, rec.Code, rec.Body.String())
		}
		body.Entries = nil
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		found := false
		for _, e := range body.Entries {
			if e.KeyHash == key && e.Namespace == "alice-ns" && e.Name == "alice-private-1362" {
				found = true
			}
		}
		if !found {
			t.Errorf("GET %s as the operator does not show alice's entry %s: %s", path, key, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), `"secret":"x"`) {
			t.Errorf("GET %s returned the entry's BODY", path)
		}

		// alice, the entry's creator, is authenticated but not granted: 403.
		if rec := i1362Drive(t, h, http.MethodGet, path, i1362Token(t, i1362Alice)); rec.Code != http.StatusForbidden {
			t.Errorf("GET %s as alice (no grant) = %d, want 403", path, rec.Code)
		}
	}
}

// ─── B5 — no /debug route can be mounted without the gate ───────────────────
//
// Two halves, neither of which reads pattern TEXT, so a constant, a computed
// pattern or a table entry cannot slip past (#1362 review finding 4):
//
//   - RUNTIME: routeTable, main's only registrar, panics on a /debug pattern
//     given to Handle, and on a non-debug one given to handleDebug.
//   - STRUCTURE, by TYPE (go/types): no code in package main calls
//     Handle/HandleFunc on an *http.ServeMux, or http.Handle/HandleFunc, except
//     routeTable's own two delegations. The only handleDebug call is
//     registerDebugRoutes' mount, whose handler is gated.Then(...). gated holds
//     both RefreshAuth and DebugAuthz, and mount is called once per
//     debugRoutePatterns entry.

func TestIssue1362_RouteTableRefusesDebugOutsideTheGate(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mustPanic := func(name string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Errorf("%s: did not panic", name)
			}
		}()
		f()
	}
	method := "GET"
	computed := method + " " + "/de" + "bug/new" // not a constant anywhere
	for _, p := range []string{
		"GET /debug/new", "/debug/new", "/debug", "/debug/", "POST /debug/x",
		"example.com/debug/x", "GET /%64ebug/x", "GET /debug/{rest...}", computed,
	} {
		p := p
		mustPanic("Handle("+p+")", func() { newRouteTable(http.NewServeMux()).Handle(p, ok) })
	}
	for _, p := range []string{"GET /call", "/debugger", "GET /debugx/y", "GET /api/debug"} {
		p := p
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Handle(%q) panicked on a non-debug pattern: %v", p, r)
				}
			}()
			newRouteTable(http.NewServeMux()).Handle(p, ok)
		}()
		mustPanic("handleDebug("+p+")", func() { newRouteTable(http.NewServeMux()).handleDebug(p, ok) })
	}

	// The production registrar builds, through the lane it must use, without
	// panicking, and serves what it mounted.
	mux := http.NewServeMux()
	registerDebugRoutes(newRouteTable(mux), use.NewChain(), debugAuthTestKeys)
	if _, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, "/debug/vars", nil)); pattern != "GET /debug/vars" {
		t.Errorf("registerDebugRoutes via routeTable did not mount GET /debug/vars (got %q)", pattern)
	}
}

func TestIssue1362_NoDebugRouteMountedOutsideTheGate(t *testing.T) {
	pkgs, err := packages.Load(&packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:  ".",
	}, ".")
	if err != nil {
		t.Fatal(err)
	}
	if len(pkgs) != 1 || len(pkgs[0].Errors) > 0 || pkgs[0].Name != "main" {
		t.Fatalf("load package main: %d packages, errors %v", len(pkgs), pkgs[0].Errors)
	}
	pkg := pkgs[0]
	info, fset := pkg.TypesInfo, pkg.Fset

	isServeMux := func(tp types.Type) bool {
		if p, ok := tp.(*types.Pointer); ok {
			tp = p.Elem()
		}
		n, ok := tp.(*types.Named)
		return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == "net/http" && n.Obj().Name() == "ServeMux"
	}
	recvName := func(fn *ast.FuncDecl) string {
		if fn.Recv == nil || len(fn.Recv.List) == 0 {
			return ""
		}
		e := fn.Recv.List[0].Type
		if st, ok := e.(*ast.StarExpr); ok {
			e = st.X
		}
		if id, ok := e.(*ast.Ident); ok {
			return id.Name
		}
		return ""
	}

	rawMuxInTable, handleDebugCalls, mountCalls, tableHandleCalls := 0, 0, 0, 0
	sawGatedDef := false
	for _, f := range pkg.Syntax {
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			inTable := recvName(fn) == "routeTable"
			inRegistrar := fn.Name.Name == "registerDebugRoutes" && fn.Recv == nil
			ast.Inspect(fn, func(n ast.Node) bool {
				if as, ok := n.(*ast.AssignStmt); ok && inRegistrar && len(as.Lhs) == 1 {
					if id, ok := as.Lhs[0].(*ast.Ident); ok && id.Name == "gated" {
						src := i1362Src(fset, as.Rhs[0])
						sawGatedDef = strings.Contains(src, "middleware.RefreshAuth(") &&
							strings.Contains(src, "middleware.DebugAuthz()")
					}
				}
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				pos := fset.Position(call.Pos())
				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					name := fun.Sel.Name
					// http.Handle / http.HandleFunc: the DefaultServeMux.
					if obj, ok := info.Uses[fun.Sel].(*types.Func); ok && obj.Pkg() != nil &&
						obj.Pkg().Path() == "net/http" && (name == "Handle" || name == "HandleFunc") &&
						info.Selections[fun] == nil {
						t.Errorf("%s: http.%s registers on the DefaultServeMux; mount through routeTable", pos, name)
						return true
					}
					sel := info.Selections[fun]
					if sel == nil {
						return true
					}
					switch {
					case (name == "Handle" || name == "HandleFunc") && isServeMux(sel.Recv()):
						if !inTable {
							t.Errorf("%s: %s on an *http.ServeMux outside routeTable (%s): it bypasses the /debug "+
								"lane check; mount through routeTable", pos, name, i1362Src(fset, call))
						} else {
							rawMuxInTable++
						}
					case name == "handleDebug":
						if inTable {
							return true
						}
						handleDebugCalls++
						if !inRegistrar {
							t.Errorf("%s: handleDebug called outside registerDebugRoutes' mount", pos)
						} else if len(call.Args) != 2 || !strings.HasPrefix(i1362Src(fset, call.Args[1]), "gated.Then(") {
							t.Errorf("%s: handleDebug's handler is not gated.Then(...): %s", pos, i1362Src(fset, call))
						}
					case name == "Handle":
						if n, ok := sel.Recv().(*types.Pointer); ok {
							if nn, ok := n.Elem().(*types.Named); ok && nn.Obj().Name() == "routeTable" {
								tableHandleCalls++
							}
						}
					}
				case *ast.Ident:
					if fun.Name == "mount" && inRegistrar {
						mountCalls++
					}
				}
				return true
			})
		}
	}
	if !sawGatedDef {
		t.Error("registerDebugRoutes does not define gated with both middleware.RefreshAuth and middleware.DebugAuthz()")
	}
	if handleDebugCalls != 1 {
		t.Errorf("handleDebug is called %d times outside routeTable; want exactly 1 (mount's)", handleDebugCalls)
	}
	if mountCalls != len(debugRoutePatterns) {
		t.Errorf("registerDebugRoutes calls mount %d times; debugRoutePatterns lists %d", mountCalls, len(debugRoutePatterns))
	}
	// Non-vacuity: the scan sees routeTable's own two delegations and main's
	// route mounts, so a broken type resolution cannot read as a clean bill.
	if rawMuxInTable != 2 {
		t.Errorf("NON-VACUITY: saw %d ServeMux.Handle calls inside routeTable, want 2", rawMuxInTable)
	}
	if tableHandleCalls < 10 {
		t.Errorf("NON-VACUITY: saw only %d routeTable.Handle calls in main", tableHandleCalls)
	}
}

// i1362Src renders an expression back to source for a structural check.
func i1362Src(fset *token.FileSet, e ast.Expr) string {
	start, end := fset.Position(e.Pos()), fset.Position(e.End())
	raw, err := os.ReadFile(start.Filename)
	if err != nil {
		return ""
	}
	return string(raw[start.Offset:end.Offset])
}
