package dispatchers

// issue681_denial_only_refresher_test.go — #681 on the REFRESHER path, driven
// through the REAL refresher pool -> resolveAndPopulateL1 -> resolveOnceProd ->
// restactions.Resolve -> branch C against an httptest apiserver that counts
// every SA dial, with the RBAC verdict from a REAL in-process RBAC snapshot.
// (The harness was #677's; #681 obsoletes its known-declined mark.)
//
// The cell: a "restactions" cell for RESTAction ra677 whose stages GET a
// ConfigMap the representative may NOT read (team-b/catalog) and one it may
// (team-a/other) — the 057 shape.
//
// ARMS
//   R1  (H1/H8) a refresh whose ONLY stage error is the per-identity re-gate
//       deny (F1) is RE-PUT: the body changes, the #547 window closes FRESH (not
//       known-stale), denial_only_put_total{refresher} moves, and the denied
//       object is never SA-dialled (#677 (1)).
//   R2  (H4) GRANT: a Role RULE edit grants the ConfigMap; the next dirty mark
//       re-resolves and Puts the fuller body.
//   R5  (H2/H8) mixed: a denial plus a 404 is NOT Put and the window ends
//       KNOWN-STALE(declined), exactly as before #681.
//   H10 census: every dispatchers Put gate that reads a StageErrorSink count
//       goes through cache.DenialOnly (no gate site can be missed).

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8sdynamic "k8s.io/client-go/dynamic"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const (
	i677NS   = "krateo-system"
	i677RA   = "ra677"
	i677Rep  = "rep677"
	i677Role = "cm-team-b"
)

var (
	i677CMGVR   = schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}
	i677RoleGVR = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
)

type i677Env struct {
	dials   atomic.Int64
	byPath  map[string]*atomic.Int64
	dyn     k8sdynamic.Interface
	saEP    *endpoints.Endpoint
	saRC    *rest.Config
	store   *cache.ResolvedCacheStore
	key     string
	inputs  cache.ResolvedKeyInputs
	missing map[string]bool
}

func (e *i677Env) pathDials(p string) int64 {
	if c, ok := e.byPath[p]; ok {
		return c.Load()
	}
	return 0
}

func i677Stage(name, path string) map[string]any {
	return map[string]any{"name": name, "path": path, "verb": "GET", "continueOnError": true}
}

func i677RAObject(stages ...map[string]any) *unstructured.Unstructured {
	api := make([]any, 0, len(stages))
	for _, s := range stages {
		api = append(api, s)
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": restActionGVR.Group + "/" + restActionGVR.Version,
		"kind":       "RESTAction",
		"metadata":   map[string]any{"name": i677RA, "namespace": i677NS, "uid": "ra677-uid", "resourceVersion": "1"},
		"spec":       map[string]any{"api": api},
	}}
}

func i677RBAC() []k8sruntime.Object {
	subj := rbacv1.Subject{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: i677Rep}
	return []k8sruntime.Object{
		&rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: "ra-get"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{restActionGVR.Group}, Resources: []string{"restactions"}, Verbs: []string{"get", "list"}}},
		},
		&rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: "ra-get"},
			Subjects:   []rbacv1.Subject{subj},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "ra-get"},
		},
		&rbacv1.Role{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "cm-team-a"},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"}}},
		},
		&rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "cm-team-a"},
			Subjects:   []rbacv1.Subject{subj},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "cm-team-a"},
		},
		// team-b: bound, but the Role grants nothing yet. R2 edits its RULES.
		&rbacv1.Role{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: i677Role},
			Rules:      []rbacv1.PolicyRule{{APIGroups: []string{""}, Resources: []string{"secrets-none"}, Verbs: []string{"get"}}},
		},
		&rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-b", Name: i677Role},
			Subjects:   []rbacv1.Subject{subj},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: i677Role},
		},
	}
}

func i677Setup(t *testing.T, ra *unstructured.Unstructured) *i677Env {
	t.Helper()
	i187RefresherEnv(t)
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	cache.ResetRefresherForTest()
	cache.ResetDenialOnlyPutForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	t.Cleanup(cache.ResetDepsForTest)
	t.Cleanup(cache.ResetRefresherForTest)

	e := &i677Env{byPath: map[string]*atomic.Int64{}, missing: map[string]bool{}}
	for _, p := range []string{
		"/api/v1/namespaces/team-b/configmaps/catalog",
		"/api/v1/namespaces/team-a/configmaps/other",
		"/api/v1/namespaces/team-a/configmaps/ghost",
	} {
		e.byPath[p] = &atomic.Int64{}
	}
	e.missing["ghost"] = true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.dials.Add(1)
		if c, ok := e.byPath[r.URL.Path]; ok {
			c.Add(1)
		}
		w.Header().Set("Content-Type", "application/json")
		seg := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(seg) == 6 && seg[0] == "api" && seg[4] == "configmaps" && !e.missing[seg[5]] {
			_, _ = fmt.Fprintf(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"namespace":%q,"name":%q},"data":{"k":"v"}}`, seg[3], seg[5])
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,"message":"not found"}`)
	}))
	t.Cleanup(srv.Close)
	e.saEP = &endpoints.Endpoint{ServerURL: srv.URL}
	e.saRC = &rest.Config{Host: srv.URL}

	sch := k8sruntime.NewScheme()
	_ = rbacv1.AddToScheme(sch)
	kinds := map[schema.GroupVersionResource]string{
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		restActionGVR: "RESTActionList",
		i677CMGVR:     "ConfigMapList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, kinds, append(i677RBAC(), ra)...)
	e.dyn = dyn
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	_, syncCh := rw.EnsureResourceType(restActionGVR)
	select {
	case <-syncCh:
	case <-time.After(10 * time.Second):
		t.Fatal("restactions informer did not sync")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		t.Fatalf("publish: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })

	e.store = cache.ResolvedCache()
	if e.store == nil {
		t.Skip("resolved cache disabled")
	}
	cache.Deps().SetStore(e.store)
	e.inputs = cache.ResolvedKeyInputs{
		CacheEntryClass:        "restactions",
		Group:                  restActionGVR.Group,
		Version:                restActionGVR.Version,
		Resource:               restActionGVR.Resource,
		Namespace:              i677NS,
		Name:                   i677RA,
		BindingUID:             "uid-677",
		RepresentativeUsername: i677Rep,
		SubjectBindingSet:      rbac.SubjectBindingSetDigest(i677Rep, nil),
	}
	e.key = cache.ComputeKey(e.inputs)
	e.store.PutResolved(fixtureStoreVerdict, e.key, &cache.ResolvedEntry{RawJSON: []byte(`{"prior":true}`), Inputs: &e.inputs})
	// ONE refresher pool for the whole arm: its stop resets the refresher
	// (including the #547 dirty windows the arms read).
	_, stop := i1126Loop(t, "restactions", e.saEP, e.saRC)
	t.Cleanup(stop)
	return e
}

// cycle enqueues the key once through the REAL refresher pool and waits until
// the handler ran and the dequeue settled.
func (e *i677Env) cycle(t *testing.T) {
	t.Helper()
	completed0 := cache.RefresherStatsByStat()["completed"]
	cache.DirtyMarkForTest(e.key)
	i1126WaitFor(t, 15*time.Second, "refresh dequeue settled", func() bool {
		return cache.RefresherStatsByStat()["completed"] > completed0
	})
}

func (e *i677Env) body(t *testing.T) string {
	t.Helper()
	got, ok := e.store.GetNoTouch(e.key)
	if !ok {
		t.Fatal("the cell left the store")
	}
	return string(got.RawJSON)
}

const (
	i677Denied  = "/api/v1/namespaces/team-b/configmaps/catalog"
	i677Allowed = "/api/v1/namespaces/team-a/configmaps/other"
	i677Ghost   = "/api/v1/namespaces/team-a/configmaps/ghost"
)

func i677DefaultRA() *unstructured.Unstructured {
	return i677RAObject(i677Stage("catalog", i677Denied), i677Stage("ok", i677Allowed))
}

// grant edits the team-b Role's RULES (not a binding) to grant the ConfigMap,
// and waits for the RBAC snapshot to republish.
func (e *i677Env) grant(t *testing.T) {
	t.Helper()
	gen0 := cache.RBACGen()
	role := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "Role",
		"metadata": map[string]any{"name": i677Role, "namespace": "team-b"},
		"rules": []any{map[string]any{
			"apiGroups": []any{""}, "resources": []any{"configmaps"}, "verbs": []any{"get"},
		}},
	}}
	if _, err := e.dyn.Resource(i677RoleGVR).Namespace("team-b").Update(context.Background(), role, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("role update: %v", err)
	}
	i1126WaitFor(t, 10*time.Second, "RBAC republish with the grant", func() bool {
		if cache.RBACGen() == gen0 {
			return false
		}
		ok, _, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
			Username: i677Rep, Verb: "get", Resource: "configmaps", Namespace: "team-b", Name: "catalog",
		})
		return err == nil && ok
	})
}


func i681DenialPuts(site string) uint64 {
	for _, c := range cache.DenialOnlyPutSnapshot() {
		if c.Site == site {
			return c.Count
		}
	}
	return 0
}

func TestIssue681_R1_DenialOnlyRefresh_IsPut_WindowClosesFresh(t *testing.T) {
	e := i677Setup(t, i677DefaultRA())
	e.cycle(t)
	if d := e.pathDials(i677Denied); d != 0 {
		t.Fatalf("R1: the denied ConfigMap was SA-dialled %d time(s)", d)
	}
	if d := e.pathDials(i677Allowed); d != 1 {
		t.Fatalf("R1 precondition: the allowed stage dialled %d, want 1", d)
	}
	if e.body(t) == `{"prior":true}` {
		t.Fatal("R1 RED: a refresh whose only stage error was a per-identity re-gate deny was declined — the cell never refreshes")
	}
	if strings.Contains(e.body(t), i677Rep) {
		t.Fatalf("R1: the refreshed body names the representative: %s", e.body(t))
	}
	if _, _, stale := cache.KnownStale(e.key); stale {
		t.Fatal("R1 (H8): the accepted re-Put left the window KNOWN-STALE")
	}
	if got := i681DenialPuts(cache.DenialOnlyPutSiteRefresher); got != 1 {
		t.Fatalf("denial_only_put_total{refresher} = %d, want 1", got)
	}
}

func TestIssue681_R2_GrantRotates_NextRefreshPutsFuller(t *testing.T) {
	e := i677Setup(t, i677DefaultRA())
	e.cycle(t)
	denied := e.body(t)
	e.grant(t)
	d0 := e.pathDials(i677Denied)
	e.cycle(t)
	if e.pathDials(i677Denied)-d0 != 1 || e.body(t) == denied {
		t.Fatalf("R2 RED: after the grant the next refresh did not read the ConfigMap and re-Put the fuller body (dials %d)",
			e.pathDials(i677Denied)-d0)
	}
}

func TestIssue681_R5_MixedDenialAndOtherError_Declined_KnownStale(t *testing.T) {
	e := i677Setup(t, i677RAObject(i677Stage("catalog", i677Denied), i677Stage("ghost", i677Ghost)))
	e.cycle(t)
	if e.pathDials(i677Ghost) != 1 {
		t.Fatalf("precondition: the 404 stage dialled %d", e.pathDials(i677Ghost))
	}
	if e.body(t) != `{"prior":true}` {
		t.Fatalf("R5 RED: a denial + 404 refresh was Put: %s", e.body(t))
	}
	if _, reason, ok := cache.KnownStale(e.key); !ok || reason != "declined" {
		t.Fatalf("R5 (H8): the declined refresh must end KNOWN-STALE(declined); got ok=%v reason=%q", ok, reason)
	}
}

// H10 — every function in this package (non-test) that reads a stage-error
// sink count as a Put gate decides through the shared Put-gate verdict
// (cache.PutGateVerdict, which applies cache.DenialOnly). #912 re-review moved
// the four gates from calling DenialOnly by hand to the verdict.
func TestIssue681_H10_EveryStageErrorPutGateUsesDenialOnly(t *testing.T) {
	ents, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	var offenders []string
	checked := 0
	for _, de := range ents {
		n := de.Name()
		if de.IsDir() || !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(token.NewFileSet(), n, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			readsStage, usesDenialOnly := false, false
			// A gate handed the verdict its caller took (a cache.PutVerdict
			// parameter, e.g. declineSeedPutOnError) decides through it too.
			for _, p := range fd.Type.Params.List {
				if sel, ok := p.Type.(*ast.SelectorExpr); ok && sel.Sel.Name == "PutVerdict" {
					usesDenialOnly = true
				}
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				ce, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := ce.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if id, ok := sel.X.(*ast.Ident); ok {
					if id.Name == "stageErrSink" && sel.Sel.Name == "Count" {
						readsStage = true
					}
					if id.Name == "cache" && sel.Sel.Name == "PutGateVerdict" {
						usesDenialOnly = true
					}
				}
				return true
			})
			if readsStage {
				checked++
				if !usesDenialOnly {
					offenders = append(offenders, n+":"+fd.Name.Name)
				}
			}
		}
	}
	if checked < 4 {
		t.Fatalf("census found %d gate functions, want >= 4 (restactions, widgets, refresher, seed) — the scan is not seeing them", checked)
	}
	if len(offenders) > 0 {
		t.Fatalf("H10: stage-error Put gates that bypass the shared verdict (cache.PutGateVerdict): %v", offenders)
	}
}
