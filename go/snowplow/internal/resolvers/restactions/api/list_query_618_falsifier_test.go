// list_query_618_falsifier_test.go — #618 / #602 parity arms.
//
// THE DEFECT (RUN on 9c233de9): every informer-served path keyed and served a
// LIST on the parsed (gvr, ns, name) alone — the query string was discarded.
// `?labelSelector=`, `?fieldSelector=` and `?limit=` all returned the
// UNSELECTED list, and a selector stage shared ONE identity-free apistage
// content cell with the unselected stage on the same GVR.
//
// THE ORACLE. Every expected item set below is WRITTEN BY HAND from the seed,
// never computed with labels.Parse / fields.ParseSelector (the code under test
// uses those, so an oracle built from them would agree with any bug in how they
// are wired). The real-apiserver oracle is the integration-tagged kind arm in
// list_query_618_kind_test.go.
//
// SEED (K=3 namespaces × M=3 widgets, the K>1 × M>1 shape):
//
//	<ns>-w1  env=prod  tier=web
//	<ns>-w2  env=dev   tier=db
//	<ns>-w3  env=test  (no tier)
//
// for ns in team-a, team-b, team-c.
//
// PATHS DRIVEN, each against the same table:
//
//	A  dispatchViaInformer, decoded + per-item RBAC branch (customer ctx)
//	B  dispatchViaInformer, #578 zero-decode raw branch (content-resolve ctx)
//	C  apistageContentServe (identity-free content cell, gate at serve)
//	D  the REAL api.Resolve stage loop (selector + unselected stages on one GVR,
//	   both orders, and the unselected cell filled FIRST in-process)
//	E  the cluster-list collapse (#602) through the real Resolve
//	F  fall-through reaches the apiserver with the query INTACT (Resolve +
//	   a recording httptest apiserver)
//
// Every arm that drives only pre-#618 entry points (dispatchViaInformer,
// apistageContentServe, Resolve) is RED on 9c233de9.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
)

const q618User = "q618-broad-user"

var q618Namespaces = []string{"team-a", "team-b", "team-c"}

const q618ClusterPath = "/apis/widgets.krateo.io/v1/widgets"

func q618NSPath(ns string) string { return "/apis/widgets.krateo.io/v1/namespaces/" + ns + "/widgets" }

func q618Widget(ns, name string, labels map[string]any) runtime.Object {
	md := map[string]any{"namespace": ns, "name": name}
	if len(labels) > 0 {
		md["labels"] = labels
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "widgets.krateo.io/v1",
		"kind":       "Widget",
		"metadata":   md,
	}}
}

// newQ618Watcher seeds the K×M labelled widgets and grants q618User `list`
// and `get` on widgets CLUSTER-WIDE (a ClusterRoleBinding — the collapse's gate
// 5 needs a cluster-scope grant; the per-item gate passes every namespace).
func newQ618Watcher(t *testing.T) *cache.ResourceWatcher {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	cache.ResetResolvedCacheForTest()
	cache.ResetDepsForTest()
	ResetClusterListAsyncStateForTest()
	t.Cleanup(func() {
		ResetClusterListAsyncStateForTest()
		cache.ResetResolvedCacheForTest()
		cache.ResetDepsForTest()
	})

	var seed []runtime.Object
	for _, ns := range q618Namespaces {
		seed = append(seed,
			q618Widget(ns, ns+"-w1", map[string]any{"env": "prod", "tier": "web"}),
			q618Widget(ns, ns+"-w2", map[string]any{"env": "dev", "tier": "db"}),
			q618Widget(ns, ns+"-w3", map[string]any{"env": "test"}),
		)
	}
	seed = append(seed,
		&rbacv1.ClusterRole{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
			ObjectMeta: metav1.ObjectMeta{Name: "q618-widget-reader"},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{f1WidgetsGVR.Group}, Resources: []string{f1WidgetsGVR.Resource},
				Verbs: []string{"list", "get"},
			}},
		},
		&rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: "q618-widget-reader"},
			Subjects:   []rbacv1.Subject{{Kind: rbacv1.UserKind, APIGroup: "rbac.authorization.k8s.io", Name: q618User}},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "q618-widget-reader"},
		},
	)

	scheme := runtime.NewScheme()
	_ = rbacv1.AddToScheme(scheme)
	listKinds := map[schema.GroupVersionResource]string{
		f1WidgetsGVR: "WidgetList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
	}
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds, seed...)
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil || rw == nil {
		t.Fatalf("NewResourceWatcher: rw=%v err=%v", rw, err)
	}
	t.Cleanup(rw.Stop)
	if _, syncCh := rw.EnsureResourceType(f1WidgetsGVR); syncCh != nil {
		select {
		case <-syncCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("widgets informer did not sync within 5s")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rw.WaitForCacheSync(ctx, 5*time.Second); err != nil {
		t.Fatalf("WaitForCacheSync: %v", err)
	}
	waitOwnRBACPublish(t, rw)
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	return rw
}

// names builds a hand-written expected set from "<ns>-wN" shorthand.
func names(ns []string, ws ...string) []string {
	var out []string
	for _, n := range ns {
		for _, w := range ws {
			out = append(out, n+"-"+w)
		}
	}
	sort.Strings(out)
	return out
}

func lit(xs ...string) []string { sort.Strings(xs); return xs }

var all3 = q618Namespaces

// q618Served is the served-path table: query → exact expected item names.
var q618Served = []struct {
	name  string
	path  string
	want  []string
	label string // human note
}{
	{"unselected", q618ClusterPath, names(all3, "w1", "w2", "w3"), ""},
	{"empty_label_selector", q618ClusterPath + "?labelSelector=", names(all3, "w1", "w2", "w3"), "an empty selector selects everything"},
	{"label_eq_encoded", q618ClusterPath + "?labelSelector=env%3Dprod", names(all3, "w1"), ""},
	{"label_eq_raw", q618ClusterPath + "?labelSelector=env=prod", names(all3, "w1"), ""},
	{"label_double_eq", q618ClusterPath + "?labelSelector=env%3D%3Ddev", names(all3, "w2"), ""},
	{"label_neq", q618ClusterPath + "?labelSelector=env!%3Dprod", names(all3, "w2", "w3"), ""},
	{"label_set_in", q618ClusterPath + "?labelSelector=" + url.QueryEscape("env in (prod,dev)"), names(all3, "w1", "w2"), "set-based"},
	{"label_set_notin", q618ClusterPath + "?labelSelector=" + url.QueryEscape("env notin (prod)"), names(all3, "w2", "w3"), "set-based"},
	{"label_exists", q618ClusterPath + "?labelSelector=tier", names(all3, "w1", "w2"), "set-based"},
	{"label_not_exists", q618ClusterPath + "?labelSelector=" + url.QueryEscape("!tier"), names(all3, "w3"), "set-based"},
	{"label_conjunction", q618ClusterPath + "?labelSelector=" + url.QueryEscape("env!=prod,tier"), names(all3, "w2"), ""},
	{"label_matches_nothing", q618ClusterPath + "?labelSelector=env%3Dstaging", nil, "an empty result is a real answer"},
	{"field_name_eq", q618ClusterPath + "?fieldSelector=metadata.name%3Dteam-b-w2", lit("team-b-w2"), ""},
	{"field_name_neq", q618ClusterPath + "?fieldSelector=metadata.name!%3Dteam-a-w1",
		lit("team-a-w2", "team-a-w3", "team-b-w1", "team-b-w2", "team-b-w3", "team-c-w1", "team-c-w2", "team-c-w3"), ""},
	{"label_and_field", q618ClusterPath + "?labelSelector=env%3Ddev&fieldSelector=metadata.name%3Dteam-c-w2", lit("team-c-w2"), ""},
	{"label_and_field_disjoint", q618ClusterPath + "?labelSelector=env%3Dprod&fieldSelector=metadata.name%3Dteam-c-w2", nil, ""},
	{"ns_path_unselected", q618NSPath("team-b"), names([]string{"team-b"}, "w1", "w2", "w3"), ""},
	{"ns_path_label", q618NSPath("team-b") + "?labelSelector=env%3Dprod", lit("team-b-w1"), ""},
	{"ns_path_field_namespace_eq", q618NSPath("team-b") + "?fieldSelector=metadata.namespace%3Dteam-b", names([]string{"team-b"}, "w1", "w2", "w3"), ""},
	{"ns_path_field_namespace_neq", q618NSPath("team-b") + "?fieldSelector=metadata.namespace!%3Dteam-b", nil, ""},
	{"ns_path_field_namespace_other", q618NSPath("team-b") + "?fieldSelector=metadata.namespace%3Dteam-a", nil, ""},
	{"ns_path_field_ns_and_name", q618NSPath("team-b") + "?fieldSelector=" + url.QueryEscape("metadata.namespace=team-b,metadata.name=team-b-w3"), lit("team-b-w3"), ""},
}

// q618Fallthrough is the fall-through table: each must NOT be informer-served.
var q618Fallthrough = []struct {
	name, path string
}{
	{"limit", q618ClusterPath + "?limit=1"},
	{"limit_with_selector", q618ClusterPath + "?labelSelector=env%3Dprod&limit=1"},
	{"continue", q618ClusterPath + "?continue=abc"},
	{"resource_version", q618ClusterPath + "?resourceVersion=0"},
	{"resource_version_match", q618ClusterPath + "?resourceVersion=0&resourceVersionMatch=NotOlderThan"},
	{"watch", q618ClusterPath + "?watch=true"},
	{"unsupported_field", q618ClusterPath + "?fieldSelector=status.phase%3DRunning"},
	{"unsupported_field_mixed", q618ClusterPath + "?fieldSelector=" + url.QueryEscape("metadata.name=team-a-w1,spec.foo=bar")},
	{"namespace_field_on_cluster_path", q618ClusterPath + "?fieldSelector=metadata.namespace%3Dteam-a"},
	{"repeated_label_selector", q618ClusterPath + "?labelSelector=env%3Dprod&labelSelector=env%3Ddev"},
	{"invalid_label_selector", q618ClusterPath + "?labelSelector=" + url.QueryEscape("env in (")},
	{"invalid_field_selector", q618ClusterPath + "?fieldSelector=" + url.QueryEscape("metadata.name")},
	{"unknown_param", q618ClusterPath + "?pretty=true"},
	{"unknown_param_with_selector", q618ClusterPath + "?labelSelector=env%3Dprod&timeoutSeconds=5"},
	{"get_by_name_with_query", q618ClusterPath + "/team-a-w1?labelSelector=env%3Dprod"},
	{"ns_get_by_name_with_query", q618NSPath("team-a") + "/team-a-w1?resourceVersion=0"},
}

func q618Ctx() context.Context {
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: q618User}))
	return cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: "http://test.invalid"})
}

// namesInEnvelopeBytes extracts item names from a LIST envelope.
func namesInEnvelopeBytes(t *testing.T, raw []byte) []string {
	t.Helper()
	var env struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		} `json:"items"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("envelope does not decode: %v (%s)", err, raw)
	}
	var out []string
	for _, it := range env.Items {
		out = append(out, it.Metadata.Name)
	}
	sort.Strings(out)
	return out
}

// namesIn walks any decoded value and returns every widget metadata.name.
func namesIn(v any) []string {
	set := map[string]bool{}
	var walk func(any)
	walk = func(x any) {
		switch xx := x.(type) {
		case map[string]any:
			if md, ok := xx["metadata"].(map[string]any); ok {
				if n, _ := md["name"].(string); n != "" && xx["kind"] == "Widget" {
					set[n] = true
				}
			}
			for _, vv := range xx {
				walk(vv)
			}
		case []any:
			for _, e := range xx {
				walk(e)
			}
		}
	}
	walk(v)
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

func eqNames(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- A + B: dispatchViaInformer ------------------------------------------

func TestIssue618_DispatchViaInformer_ServedQueriesMatchApiserverSemantics(t *testing.T) {
	newQ618Watcher(t)
	for _, branch := range []struct {
		name string
		ctx  func() context.Context
	}{
		{"decoded_rbac_branch", func() context.Context { return q618Ctx() }},
		{"raw_zero_decode_branch", func() context.Context { return cache.WithApistageContentResolve(q618Ctx()) }},
	} {
		for _, tc := range q618Served {
			t.Run(branch.name+"/"+tc.name, func(t *testing.T) {
				raw, served, _, _ := dispatchViaInformer(branch.ctx(), buildCall(http.MethodGet, tc.path))
				if !served {
					t.Fatalf("%s: a query the informer can reproduce exactly must be served; got served=false", tc.path)
				}
				if got := namesInEnvelopeBytes(t, raw); !eqNames(got, tc.want) {
					t.Fatalf("RED #618 %s: served %v, apiserver semantics give %v", tc.path, got, tc.want)
				}
			})
		}
	}
}

func TestIssue618_DispatchViaInformer_UnsupportedQueriesFallThrough(t *testing.T) {
	newQ618Watcher(t)
	for _, ctxName := range []string{"decoded", "raw"} {
		for _, tc := range q618Fallthrough {
			t.Run(ctxName+"/"+tc.name, func(t *testing.T) {
				ctx := q618Ctx()
				if ctxName == "raw" {
					ctx = cache.WithApistageContentResolve(ctx)
				}
				if raw, served, _, _ := dispatchViaInformer(ctx, buildCall(http.MethodGet, tc.path)); served {
					t.Fatalf("RED #618 %s: must fall through to the apiserver with its query intact; the informer "+
						"served %d bytes (items %v) — the param was silently dropped", tc.path, len(raw), namesInEnvelopeBytes(t, raw))
				}
			})
		}
	}
}

// --- C: apistageContentServe --------------------------------------------

func TestIssue618_ApistageContentServe_ServedQueriesMatch(t *testing.T) {
	newQ618Watcher(t)
	store := cache.ResolvedCache()
	// Two passes: the first fills each cell (MISS), the second serves it (HIT).
	// Interleaving every query on ONE store is the shared-cell probe: any two
	// queries mapping to one cell make the HIT pass serve the wrong set.
	for pass := 0; pass < 2; pass++ {
		for _, tc := range q618Served {
			v, served, ok, _, _ := apistageContentServe(q618Ctx(), store, buildCall(http.MethodGet, tc.path), false)
			if !ok || !served {
				t.Fatalf("pass %d %s: content layer must serve an exact selector; ok=%v served=%v", pass, tc.path, ok, served)
			}
			if got := namesIn(v); !eqNames(got, tc.want) {
				t.Fatalf("RED #618 pass %d %s: content cell served %v, want %v (a selector and an unselected stage "+
					"share one cell, or the selector was dropped)", pass, tc.path, got, tc.want)
			}
		}
	}
}

func TestIssue618_ApistageContentServe_UnsupportedQueriesNeverTouchACell(t *testing.T) {
	newQ618Watcher(t)
	store := cache.ResolvedCache()
	// Warm the unselected cells first: a fall-through query must not be served
	// from them either.
	for _, p := range []string{q618ClusterPath, q618NSPath("team-a")} {
		if _, _, ok, _, _ := apistageContentServe(q618Ctx(), store, buildCall(http.MethodGet, p), false); !ok {
			t.Fatalf("precondition: warm %s", p)
		}
	}
	before := store.Stats().StoreTotal
	for _, tc := range q618Fallthrough {
		v, served, ok, _, _ := apistageContentServe(q618Ctx(), store, buildCall(http.MethodGet, tc.path), false)
		if ok {
			t.Fatalf("RED #618 %s: the content layer claimed this call (served=%v, items %v); it must return "+
				"ok=false so the apiserver answers with the query intact", tc.path, served, namesIn(v))
		}
	}
	if after := store.Stats().StoreTotal; after != before {
		t.Fatalf("#618: a fall-through query wrote %d content cell(s)", after-before)
	}
}

// --- D: the real Resolve stage loop --------------------------------------

func q618Stage(id, path string) *templates.API {
	return &templates.API{
		Name:   id,
		Path:   path,
		Verb:   ptr.To(http.MethodGet),
		Filter: ptr.To("." + id + ".items"),
	}
}

func q618Resolve(t *testing.T, ctx context.Context, stages ...*templates.API) map[string]any {
	t.Helper()
	return Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               stages,
		RESTActionNamespace: "default",
		RESTActionName:      "q618-restaction",
	})
}

// TestIssue618_Resolve_SelectorAndUnselectedStagesOnOneGVR drives both orders
// in ONE resolve, then the coordinator's arm: an UNSELECTED cell filled in an
// earlier resolve of this process is never served to a selector stage that runs
// for the first time afterwards (and the reverse).
func TestIssue618_Resolve_SelectorAndUnselectedStagesOnOneGVR(t *testing.T) {
	sel := q618ClusterPath + "?labelSelector=env%3Dprod"
	wantAll, wantProd := names(all3, "w1", "w2", "w3"), names(all3, "w1")

	t.Run("one_resolve_unselected_first", func(t *testing.T) {
		newQ618Watcher(t)
		d := q618Resolve(t, q618Ctx(), q618Stage("all", q618ClusterPath), q618Stage("prod", sel))
		if got := namesIn(d["all"]); !eqNames(got, wantAll) {
			t.Fatalf("unselected stage: got %v want %v", got, wantAll)
		}
		if got := namesIn(d["prod"]); !eqNames(got, wantProd) {
			t.Fatalf("RED #618: selector stage after an unselected stage on the same GVR got %v, want %v", got, wantProd)
		}
	})
	t.Run("one_resolve_selector_first", func(t *testing.T) {
		newQ618Watcher(t)
		d := q618Resolve(t, q618Ctx(), q618Stage("prod", sel), q618Stage("all", q618ClusterPath))
		if got := namesIn(d["prod"]); !eqNames(got, wantProd) {
			t.Fatalf("RED #618: selector stage got %v, want %v", got, wantProd)
		}
		if got := namesIn(d["all"]); !eqNames(got, wantAll) {
			t.Fatalf("RED #618: unselected stage after a selector stage got %v, want %v — it was served the selector's cell", got, wantAll)
		}
	})
	t.Run("unselected_cell_filled_before_selector_stage_ever_ran", func(t *testing.T) {
		newQ618Watcher(t)
		// Resolve 1: ONLY the unselected stage — its cell is now resident.
		if got := namesIn(q618Resolve(t, q618Ctx(), q618Stage("all", q618ClusterPath))["all"]); !eqNames(got, wantAll) {
			t.Fatalf("precondition: unselected stage got %v", got)
		}
		hitsBefore := cache.ResolvedCache().Stats().HitTotal
		// Resolve 2: the selector stage runs for the FIRST time in this process.
		got := namesIn(q618Resolve(t, q618Ctx(), q618Stage("prod", sel))["prod"])
		if !eqNames(got, wantProd) {
			t.Fatalf("RED #618: a selector stage's first run, after the unselected cell was filled, got %v want %v — "+
				"the unselected cell was served to it", got, wantProd)
		}
		if hitsAfter := cache.ResolvedCache().Stats().HitTotal; hitsAfter != hitsBefore {
			t.Fatalf("#618: the selector stage's FIRST run produced %d content-cell HIT(s); it can only have hit the "+
				"unselected cell", hitsAfter-hitsBefore)
		}
		// Resolve 3: the unselected stage again — still its own cell.
		if got := namesIn(q618Resolve(t, q618Ctx(), q618Stage("all", q618ClusterPath))["all"]); !eqNames(got, wantAll) {
			t.Fatalf("RED #618: unselected stage after the selector cell was filled got %v want %v", got, wantAll)
		}
	})
	t.Run("selector_cell_filled_before_unselected_stage_ever_ran", func(t *testing.T) {
		newQ618Watcher(t)
		if got := namesIn(q618Resolve(t, q618Ctx(), q618Stage("prod", sel))["prod"]); !eqNames(got, wantProd) {
			t.Fatalf("RED #618: selector stage got %v want %v", got, wantProd)
		}
		if got := namesIn(q618Resolve(t, q618Ctx(), q618Stage("all", q618ClusterPath))["all"]); !eqNames(got, wantAll) {
			t.Fatalf("RED #618: an unselected stage's first run, after the selector cell was filled, got %v want %v",
				got, wantAll)
		}
	})
}

// --- E: cluster-list collapse (#602) -------------------------------------

// q618IterStage is a per-namespace iterator stage — the collapse-eligible shape
// — carrying query on every element.
func q618IterStage(id, query string) *templates.API {
	q := ""
	if query != "" {
		q = "?" + query
	}
	return &templates.API{
		Name: id,
		Path: `${ "/apis/widgets.krateo.io/v1/namespaces/" + . + "/widgets` + q + `" }`,
		Verb: ptr.To(http.MethodGet),
		DependsOn: &templates.Dependency{
			Iterator: ptr.To(`["team-a","team-b","team-c"]`),
		},
	}
}

func q618WaitAsyncPopulate(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		busy := false
		clusterListInflightCells.Range(func(_, _ any) bool { busy = true; return false })
		if !busy {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cluster-list async populate did not finish within 5s")
}

// TestIssue602_ClusterListCollapse_CarriesTheSelector drives the real Resolve
// twice per case: the first resolve is cold (per-NS fallback, async populate
// scheduled), the second takes the collapse's WARM path. Both must equal the
// hand-written set. An unselected iterator stage resolved first in the same
// process must not lend its collapsed cell to the selector stage.
func TestIssue602_ClusterListCollapse_CarriesTheSelector(t *testing.T) {
	withClusterListCollapseEnabledForTest(t)
	cases := []struct {
		name, query string
		want        []string
		wantWarm    bool // whether the second resolve must take the collapse
	}{
		{"label_selector", "labelSelector=env%3Dprod", names(all3, "w1"), true},
		{"label_set_based", "labelSelector=" + url.QueryEscape("env in (dev,test)"), names(all3, "w2", "w3"), true},
		{"field_name", "fieldSelector=metadata.name%3Dteam-b-w3", lit("team-b-w3"), true},
		// metadata.namespace cannot ride a CLUSTER path (Node/Namespace 400 on
		// it); the collapse declines and each per-NS element applies it.
		{"field_namespace_declines_collapse", "fieldSelector=metadata.namespace!%3Dteam-z", names(all3, "w1", "w2", "w3"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			newQ618Watcher(t)
			cache.ResetClusterListCellCountersForTest()
			// The unselected iterator stage runs first and its collapsed cell is
			// warmed — the cell a query-blind collapse would hand the selector.
			_ = q618Resolve(t, q618Ctx(), q618IterStage("all", ""))
			q618WaitAsyncPopulate(t)
			if got := namesIn(q618Resolve(t, q618Ctx(), q618IterStage("all", ""))["all"]); !eqNames(got, names(all3, "w1", "w2", "w3")) {
				t.Fatalf("precondition: unselected iterator got %v", got)
			}

			for round := 0; round < 2; round++ {
				warm0, _ := cache.ClusterListCellCounters()
				got := namesIn(q618Resolve(t, q618Ctx(), q618IterStage("sel", tc.query))["sel"])
				if !eqNames(got, tc.want) {
					t.Fatalf("RED #602 round %d ?%s: iterator stage got %v, want %v — the collapse discarded the selector",
						round, tc.query, got, tc.want)
				}
				warm1, _ := cache.ClusterListCellCounters()
				if round == 1 && tc.wantWarm && warm1 == warm0 {
					t.Fatalf("#602 round 1 ?%s: the warm collapse was not taken — the arm did not exercise the collapse path", tc.query)
				}
				if !tc.wantWarm && warm1 != warm0 {
					t.Fatalf("#602 ?%s: the collapse served a metadata.namespace selector on a cluster path", tc.query)
				}
				q618WaitAsyncPopulate(t)
			}
		})
	}
}

// TestIssue602_ClusterListCollapse_HeterogeneousQueriesDecline: elements whose
// queries differ cannot be answered by one cluster LIST.
func TestIssue602_ClusterListCollapse_HeterogeneousQueriesDecline(t *testing.T) {
	withClusterListCollapseEnabledForTest(t)
	newQ618Watcher(t)
	stage := &templates.API{
		Name: "het",
		Path: `${ "/apis/widgets.krateo.io/v1/namespaces/" + .ns + "/widgets?labelSelector=env%3D" + .env }`,
		Verb: ptr.To(http.MethodGet),
		DependsOn: &templates.Dependency{
			Iterator: ptr.To(`[{"ns":"team-a","env":"prod"},{"ns":"team-b","env":"dev"},{"ns":"team-c","env":"test"}]`),
		},
	}
	want := lit("team-a-w1", "team-b-w2", "team-c-w3")
	for round := 0; round < 2; round++ {
		got := namesIn(q618Resolve(t, q618Ctx(), stage)["het"])
		if !eqNames(got, want) {
			t.Fatalf("RED #602 round %d: heterogeneous-selector iterator got %v, want %v", round, got, want)
		}
		q618WaitAsyncPopulate(t)
	}
}

// --- F: the fall-through reaches the apiserver with the query INTACT ------

// TestIssue618_FallThroughReachesApiserverWithQueryIntact runs the real
// Resolve against a recording apiserver. For every fall-through query the
// apiserver must receive the request with the EXACT query string, and the
// stage must be fed the apiserver's answer (not the informer's).
func TestIssue618_FallThroughReachesApiserverWithQueryIntact(t *testing.T) {
	newQ618Watcher(t)
	var mu sync.Mutex
	var seen []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.URL.Path+"?"+r.URL.RawQuery)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"widgets.krateo.io/v1","kind":"WidgetList","items":[` +
			`{"apiVersion":"widgets.krateo.io/v1","kind":"Widget","metadata":{"name":"from-apiserver","namespace":"team-a"}}]}`))
	}))
	t.Cleanup(srv.Close)

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: q618User}))
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srv.URL})

	for _, tc := range q618Fallthrough {
		if strings.Contains(tc.path, "watch=true") {
			continue // a watch stream is not a LIST body; covered at the dispatch arm
		}
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			seen = nil
			mu.Unlock()
			stage := &templates.API{Name: "s", Path: tc.path, Verb: ptr.To(http.MethodGet)}
			d := q618Resolve(t, ctx, stage)
			mu.Lock()
			defer mu.Unlock()
			if len(seen) != 1 || seen[0] != tc.path {
				t.Fatalf("RED #618 %s: the apiserver must receive exactly this call with its query intact; got %v",
					tc.path, seen)
			}
			if got := namesIn(d["s"]); len(got) != 1 || got[0] != "from-apiserver" {
				t.Fatalf("RED #618 %s: the stage must be fed the apiserver's answer; got %v", tc.path, got)
			}
		})
	}
}

// --- G: the internal (SA / refresher) dispatcher -------------------------

// TestIssue618_InternalDispatch_HandsSelectorsToTheLiveList: the SA-credentialed
// dispatcher (Phase-1 walk / refresher / prewarm re-resolve) LISTed with bare
// ListOptions, so a selector stage re-resolved there got the unselected list.
// Post-fix: an exact selector reaches the apiserver as ListOptions; any other
// query is refused (served=false) so the later branch dispatches the path
// intact — the apiserver never sees a request with the query stripped.
func TestIssue618_InternalDispatch_HandsSelectorsToTheLiveList(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)

	var mu sync.Mutex
	var seen []url.Values
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.URL.Query())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"widgets.krateo.io/v1","kind":"WidgetList","metadata":{"resourceVersion":"1"},"items":[]}`))
	}))
	t.Cleanup(srv.Close)
	rc := &rest.Config{Host: srv.URL, BearerToken: "fake-sa", TLSClientConfig: rest.TLSClientConfig{CAData: pemEncodeCert(srv.Certificate())}}
	ctx := cache.WithInternalRESTConfig(context.Background(), rc)

	served := []struct {
		name, path, wantLabel, wantField string
	}{
		{"unselected", q618ClusterPath, "", ""},
		{"label", q618ClusterPath + "?labelSelector=env%3Dprod", "env=prod", ""},
		{"label_set_based", q618ClusterPath + "?labelSelector=" + url.QueryEscape("env in (prod,dev)"), "env in (dev,prod)", ""},
		{"field_name", q618NSPath("team-a") + "?fieldSelector=metadata.name%3Dteam-a-w1", "", "metadata.name=team-a-w1"},
		{"both", q618NSPath("team-a") + "?labelSelector=tier&fieldSelector=metadata.namespace%3Dteam-a", "tier", "metadata.namespace=team-a"},
	}
	for _, tc := range served {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			seen = nil
			mu.Unlock()
			_, ok, err := dispatchViaInternalRESTConfig(ownClusterCtx(ctx), buildCall(http.MethodGet, tc.path))
			if err != nil || !ok {
				t.Fatalf("%s: served=%v err=%v", tc.path, ok, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(seen) != 1 {
				t.Fatalf("%s: want one LIST request, got %d", tc.path, len(seen))
			}
			if got := seen[0].Get("labelSelector"); got != tc.wantLabel {
				t.Fatalf("RED #618 %s: the live LIST carried labelSelector=%q, want %q — the selector was dropped", tc.path, got, tc.wantLabel)
			}
			if got := seen[0].Get("fieldSelector"); got != tc.wantField {
				t.Fatalf("RED #618 %s: the live LIST carried fieldSelector=%q, want %q", tc.path, got, tc.wantField)
			}
		})
	}
	for _, tc := range q618Fallthrough {
		t.Run("refused/"+tc.name, func(t *testing.T) {
			mu.Lock()
			seen = nil
			mu.Unlock()
			_, ok, err := dispatchViaInternalRESTConfig(ownClusterCtx(ctx), buildCall(http.MethodGet, tc.path))
			mu.Lock()
			defer mu.Unlock()
			if ok || err != nil || len(seen) != 0 {
				t.Fatalf("RED #618 %s: the internal dispatcher must refuse a query it cannot pass through intact "+
					"(served=false, no request); got served=%v err=%v requests=%v", tc.path, ok, err, seen)
			}
		})
	}
}

// TestIssue618_InternalDispatch_InformerBranchFiltersSelectors: with a serve
// watcher on ctx (the Phase-1 walk / SA refresh, #121 1a) a selector LIST is
// served from the informer — zero live requests — and filtered to exactly the
// hand-written set. Pre-fix it was served UNFILTERED.
func TestIssue618_InternalDispatch_InformerBranchFiltersSelectors(t *testing.T) {
	resetInternalClientCacheForTest()
	t.Cleanup(resetInternalClientCacheForTest)
	rw := newQ618Watcher(t)

	var mu sync.Mutex
	live := 0
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		live++
		mu.Unlock()
		http.Error(w, "the informer branch should have served this", http.StatusTeapot)
	}))
	t.Cleanup(srv.Close)
	rc := &rest.Config{Host: srv.URL, BearerToken: "fake-sa", TLSClientConfig: rest.TLSClientConfig{CAData: pemEncodeCert(srv.Certificate())}}
	ctx := cache.WithServeWatcher(cache.WithInternalRESTConfig(context.Background(), rc), rw)

	for _, tc := range q618Served {
		t.Run(tc.name, func(t *testing.T) {
			mu.Lock()
			live = 0
			mu.Unlock()
			raw, ok, err := dispatchViaInternalRESTConfig(ownClusterCtx(ctx), buildCall(http.MethodGet, tc.path))
			if err != nil || !ok {
				t.Fatalf("%s: served=%v err=%v", tc.path, ok, err)
			}
			mu.Lock()
			defer mu.Unlock()
			if live != 0 {
				t.Fatalf("#618 %s: %d live LIST(s) — the selector bypassed the #121 1a informer branch", tc.path, live)
			}
			if got := namesInEnvelopeBytes(t, raw); !eqNames(got, tc.want) {
				t.Fatalf("RED #618 %s: informer branch served %v, want %v", tc.path, got, tc.want)
			}
		})
	}
}
