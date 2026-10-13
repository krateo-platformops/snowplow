package dispatchers

// issue677_regate_declined_test.go — #677 (2): the refresher's KNOWN-DECLINED
// mark, driven through the REAL refresher pool -> resolveAndPopulateL1 ->
// resolveOnceProd -> restactions.Resolve -> branch C against an httptest
// apiserver that counts every SA dial, with the RBAC verdict from a REAL
// in-process RBAC snapshot.
//
// The cell: a "restactions" cell for RESTAction ra677 whose two stages GET a
// ConfigMap the representative may NOT read (team-b/catalog) and one it may
// (team-a/other) — the 057 shape: one correctly denied step in an RA whose other
// steps keep it dirty.
//
// ARMS
//   R1  mark + skip: the first dequeue resolves (0 SA dials for the denied
//       ConfigMap — #677 (1) — 1 for the allowed one), declines, marks, and ends
//       KNOWN-STALE. The next dequeue does NOT re-resolve (0 dials, skip +1) and
//       the cell is still known-stale (#547 bookkeeping intact).
//   R2  GRANT: a Role RULE edit (no binding change, so no subject sub-gen move)
//       grants the ConfigMap. The next dequeue re-resolves and PUTS; the mark
//       retires as rbac_published; the cell is no longer known-stale.
//   R2m mutation: without the publish-seq retire the same grant leaves the cell
//       skipped and stale (RED).
//   R3  definition change: an edit of the RESTAction retires the mark
//       (definition_changed) and the next dequeue re-resolves.
//   R4  never consulted outside the refresher: the #547 demand revalidation
//       claim is not blocked by a mark; and a static census pins the consult's
//       only call site to resolveAndPopulateL1.
//   R5  mixed: a denied step plus a step that fails for another reason never
//       marks; the predicate refuses an external touch.

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
	"path/filepath"
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
	cache.ResetRegateDeclinedForTest()
	t.Cleanup(cache.ResetResolvedCacheForTest)
	t.Cleanup(cache.ResetDepsForTest)
	t.Cleanup(cache.ResetRefresherForTest)
	t.Cleanup(cache.ResetRegateDeclinedForTest)

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
	e.store.Put(e.key, &cache.ResolvedEntry{RawJSON: []byte(`{"prior":true}`), Inputs: &e.inputs})
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

func i677Retired(reason string) uint64 {
	for _, c := range cache.RegateDeclinedRetiredSnapshot() {
		if c.Reason == reason {
			return c.Count
		}
	}
	return 0
}

const (
	i677Denied  = "/api/v1/namespaces/team-b/configmaps/catalog"
	i677Allowed = "/api/v1/namespaces/team-a/configmaps/other"
	i677Ghost   = "/api/v1/namespaces/team-a/configmaps/ghost"
)

func i677DefaultRA() *unstructured.Unstructured {
	return i677RAObject(i677Stage("catalog", i677Denied), i677Stage("ok", i677Allowed))
}

// markedAndSkipping runs R1 and returns the env with the cell marked.
func i677MarkedAndSkipping(t *testing.T) *i677Env {
	t.Helper()
	e := i677Setup(t, i677DefaultRA())

	e.cycle(t)
	if d := e.pathDials(i677Denied); d != 0 {
		t.Fatalf("R1: the denied ConfigMap was SA-dialled %d time(s) — #677 (1) must decide the deny before the dial", d)
	}
	if d := e.pathDials(i677Allowed); d != 1 {
		t.Fatalf("R1 precondition: the allowed stage dialled %d, want 1 (the resolve did not run?)", d)
	}
	if e.body(t) != `{"prior":true}` {
		t.Fatalf("R1: a re-gate-denied refresh overwrote the prior body: %s", e.body(t))
	}
	if _, ok := cache.LookupRegateDeclined(e.key); !ok {
		t.Fatal("R1 RED: a refresh whose only stage error was a re-gate deny did not mark the key known-declined")
	}
	if _, reason, ok := cache.KnownStale(e.key); !ok || reason != "declined" {
		t.Fatalf("R1: after the declined refresh the cell must be KNOWN-STALE(declined); got ok=%v reason=%q", ok, reason)
	}

	dials0 := e.dials.Load()
	skip0 := cache.RegateDeclinedSkipTotal()
	e.cycle(t)
	if d := e.dials.Load() - dials0; d != 0 {
		t.Fatalf("R1 RED: the marked cell was re-resolved (%d SA dials) on the next dirty mark — the deterministic re-dial is back", d)
	}
	if got := cache.RegateDeclinedSkipTotal() - skip0; got != 1 {
		t.Fatalf("R1: skip_total moved by %d, want 1", got)
	}
	if _, reason, ok := cache.KnownStale(e.key); !ok || reason != "declined" {
		t.Fatalf("R1: a skipped dequeue must still end KNOWN-STALE (#547); got ok=%v reason=%q", ok, reason)
	}
	return e
}

func TestIssue677_R1_MarkAndSkip_StaysKnownStale(t *testing.T) {
	i677MarkedAndSkipping(t)
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

func TestIssue677_R2_GrantRetiresTheMark_NextCyclePuts(t *testing.T) {
	e := i677MarkedAndSkipping(t)
	e.grant(t)

	d0 := e.pathDials(i677Denied)
	e.cycle(t)
	if e.pathDials(i677Denied)-d0 != 1 {
		t.Fatalf("R2 RED: after the grant the next cycle did not re-resolve (catalog dials %d) — a mark outlived a permission change", e.pathDials(i677Denied)-d0)
	}
	if e.body(t) == `{"prior":true}` {
		t.Fatal("R2 RED: after the grant the next cycle did not Put the fresh body")
	}
	if _, ok := cache.LookupRegateDeclined(e.key); ok {
		t.Fatal("R2: the mark is still held after the grant")
	}
	if got := i677Retired(cache.RegateDeclinedRetiredRBACPublished); got != 1 {
		t.Fatalf("R2: retired_total{rbac_published} = %d, want 1", got)
	}
	if _, _, stale := cache.KnownStale(e.key); stale {
		t.Fatal("R2: the accepted Put did not close the known-stale window")
	}
}

func TestIssue677_R2m_Mutation_NoPublishSeqRetire_StaysStale(t *testing.T) {
	prev := regateDeclinedRBACMovedFn
	regateDeclinedRBACMovedFn = func(*cache.RegateDeclinedMark) bool { return false }
	t.Cleanup(func() { regateDeclinedRBACMovedFn = prev })

	e := i677MarkedAndSkipping(t)
	e.grant(t)
	d0 := e.dials.Load()
	e.cycle(t)
	if e.dials.Load() != d0 || e.body(t) != `{"prior":true}` {
		t.Fatalf("mutation: without the publish-seq retire the grant was still picked up (dials %d, body %s) — R2 cannot see its defect",
			e.dials.Load()-d0, e.body(t))
	}
	t.Log("R2m RED as required: without the RBAC publish-seq retire the cell stays skipped and stale after a grant")
}

func TestIssue677_R3_DefinitionChangeRetires(t *testing.T) {
	e := i677MarkedAndSkipping(t)
	// Baseline BEFORE the edit: the edit is itself a dep event, so the
	// refresher may retire + re-resolve (and re-mark under the new version)
	// before the explicit cycle below runs.
	d0 := e.pathDials(i677Allowed)
	ra := i677DefaultRA()
	ra.SetResourceVersion("2")
	ra.Object["spec"].(map[string]any)["marker"] = "edited"
	if _, err := e.dyn.Resource(restActionGVR).Namespace(i677NS).Update(context.Background(), ra, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("ra update: %v", err)
	}
	i1126WaitFor(t, 10*time.Second, "informer sees the RA edit", func() bool {
		obj, ok := cache.Global().GetObject(restActionGVR, i677NS, i677RA)
		return ok && obj.GetResourceVersion() != "1"
	})
	e.cycle(t)
	if e.pathDials(i677Allowed)-d0 < 1 {
		t.Fatal("R3 RED: after the RESTAction changed the cell was never re-resolved — the mark outlived its definition")
	}
	if got := i677Retired(cache.RegateDeclinedRetiredDefinitionChanged); got != 1 {
		t.Fatalf("R3: retired_total{definition_changed} = %d, want 1", got)
	}
}

// R4 — the demand revalidation (#547, under the REQUESTER) is not blocked by a
// mark: a mark gates only the refresher's own re-resolve.
func TestIssue677_R4_DemandRevalidationNotBlockedByMark(t *testing.T) {
	e := i677MarkedAndSkipping(t)
	t0, _, ok := cache.KnownStale(e.key)
	if !ok {
		t.Fatal("precondition: cell not known-stale")
	}
	if !cache.ClaimDemandRevalidation(e.key, t0) {
		t.Fatal("R4: a known-declined mark blocked the requester's demand revalidation")
	}
}

// R4 census — the consult has exactly ONE production call site, inside
// resolveAndPopulateL1 (the refresher handler), and the cache-side lookup is
// reached only through it. A new caller (a /call path, the seed) fails here.
func TestIssue677_R4_ConsultCensus_RefresherOnly(t *testing.T) {
	root := "../.."
	type site struct{ file, fn string }
	var consult, lookup []site
	err := filepath.Walk(root, func(p string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return err
		}
		fs := token.NewFileSet()
		f, perr := parser.ParseFile(fs, p, nil, 0)
		if perr != nil {
			return perr
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				ce, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fn := ce.Fun.(type) {
				case *ast.Ident:
					if fn.Name == "regateDeclinedSkip" {
						consult = append(consult, site{filepath.Base(p), fd.Name.Name})
					}
				case *ast.SelectorExpr:
					if fn.Sel.Name == "LookupRegateDeclined" {
						lookup = append(lookup, site{filepath.Base(p), fd.Name.Name})
					}
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(consult) != 1 || consult[0] != (site{"resolve_populate.go", "resolveAndPopulateL1"}) {
		t.Fatalf("regateDeclinedSkip call sites = %v, want exactly resolve_populate.go:resolveAndPopulateL1", consult)
	}
	if len(lookup) != 1 || lookup[0] != (site{"regate_declined_mark.go", "regateDeclinedSkip"}) {
		t.Fatalf("cache.LookupRegateDeclined call sites = %v, want exactly regate_declined_mark.go:regateDeclinedSkip", lookup)
	}
}

func TestIssue677_R5_MixedErrorsNeverMark_ExternalNeverMarks(t *testing.T) {
	e := i677Setup(t, i677RAObject(i677Stage("catalog", i677Denied), i677Stage("ghost", i677Ghost)))
	e.cycle(t)
	if e.pathDials(i677Ghost) != 1 {
		t.Fatalf("precondition: the 404 stage dialled %d", e.pathDials(i677Ghost))
	}
	if _, ok := cache.LookupRegateDeclined(e.key); ok {
		t.Fatal("R5 RED: a refresh with a re-gate deny AND another stage error was marked — only an all-deny decline is deterministic")
	}
	if regateDeclinedMarkIfAllDenies(e.key, &e.inputs, cache.RBACGen(), 1, 1, 1) {
		t.Fatal("R5: the mark predicate accepted a resolve that touched an external endpoint")
	}
	if regateDeclinedMarkIfAllDenies(e.key, &e.inputs, cache.RBACGen(), 0, 0, 0) {
		t.Fatal("R5: the mark predicate accepted a resolve with no stage error")
	}
}
