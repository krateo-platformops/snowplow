// list_query_618_unit_test.go — #618: the classifier, the key, the counter and
// the no-param-dropped guards. The behavioural parity arms (which compile
// against pre-#618 code and are RED there) are in
// list_query_618_falsifier_test.go.
package api

import (
	"expvar"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue618_PlanListQuery_Table(t *testing.T) {
	const p = "/apis/g/v1/widgets"
	const np = "/apis/g/v1/namespaces/ns1/widgets"
	cases := []struct {
		path       string
		isList     bool
		applicable bool
		reason     string
		canon      string
	}{
		{p, true, true, "", ""},
		{p + "?", true, true, "", ""},
		{p + "?labelSelector=", true, true, "", ""},
		{p + "?labelSelector=&fieldSelector=", true, true, "", ""},
		{p + "?labelSelector=env%3Dprod", true, true, listQueryReasonSelector, "labelSelector=env%3Dprod"},
		// two spellings, one canonical form
		{p + "?labelSelector=env=prod", true, true, listQueryReasonSelector, "labelSelector=env%3Dprod"},
		// requirement order does not move the key (labels.Parse sorts by key)
		{p + "?labelSelector=" + url.QueryEscape("tier,env=prod"), true, true, listQueryReasonSelector,
			"labelSelector=" + url.QueryEscape("env=prod,tier")},
		{p + "?labelSelector=" + url.QueryEscape("env=prod,tier"), true, true, listQueryReasonSelector,
			"labelSelector=" + url.QueryEscape("env=prod,tier")},
		// field requirements sorted; == and = are one operator
		{p + "?fieldSelector=" + url.QueryEscape("metadata.name==b"), true, true, listQueryReasonSelector, "fieldSelector=metadata.name%3Db"},
		{np + "?fieldSelector=" + url.QueryEscape("metadata.namespace=ns1,metadata.name=b"), true, true, listQueryReasonSelector,
			"fieldSelector=" + url.QueryEscape("metadata.name=b,metadata.namespace=ns1")},
		// param order does not move the key
		{p + "?fieldSelector=metadata.name%3Db&labelSelector=x", true, true, listQueryReasonSelector, "fieldSelector=metadata.name%3Db&labelSelector=x"},
		{p + "?labelSelector=x&fieldSelector=metadata.name%3Db", true, true, listQueryReasonSelector, "fieldSelector=metadata.name%3Db&labelSelector=x"},
		// fall-throughs
		{p + "?fieldSelector=metadata.namespace%3Dns1", true, false, listQueryReasonNamespaceFieldClusterPath, ""},
		{p + "?fieldSelector=spec.x%3D1", true, false, listQueryReasonUnsupportedField, ""},
		{p + "?limit=5", true, false, listQueryReasonPagination, ""},
		{p + "?continue=x", true, false, listQueryReasonPagination, ""},
		{p + "?resourceVersion=0", true, false, listQueryReasonResourceVersion, ""},
		{p + "?resourceVersionMatch=Exact", true, false, listQueryReasonResourceVersion, ""},
		{p + "?watch=1", true, false, listQueryReasonWatch, ""},
		{p + "?allowWatchBookmarks=true", true, false, listQueryReasonWatch, ""},
		{p + "?sendInitialEvents=true", true, false, listQueryReasonWatch, ""},
		{p + "?labelSelector=a&labelSelector=b", true, false, listQueryReasonRepeatedParam, ""},
		{p + "?labelSelector=" + url.QueryEscape("a in ("), true, false, listQueryReasonInvalidSelector, ""},
		{p + "?fieldSelector=nonsense", true, false, listQueryReasonInvalidSelector, ""},
		{p + "?pretty=true", true, false, listQueryReasonUnknownParam, ""},
		{p + "?LabelSelector=a", true, false, listQueryReasonUnknownParam, ""},
		{p + "?labelSelector=%zz", true, false, listQueryReasonMalformed, ""},
		{p + "/name?labelSelector=a", false, false, listQueryReasonQueryOnGet, ""},
		{p + "/name", false, true, "", ""},
	}
	for _, tc := range cases {
		_, _, namespaced := strings.Cut(tc.path, "/namespaces/")
		q := planListQuery(tc.path, tc.isList, namespaced)
		if q.applicable != tc.applicable || q.reason != tc.reason || q.canon != tc.canon {
			t.Errorf("%s: got applicable=%v reason=%q canon=%q; want %v %q %q",
				tc.path, q.applicable, q.reason, q.canon, tc.applicable, tc.reason, tc.canon)
		}
	}
}

// TestIssue602_PrewarmCanonEqualsCollapseCanon pins that the prewarm roster and
// the collapse plan the SAME canonical query for the same literal stage path,
// so a prewarmed cell is the cell the collapse later reads.
func TestIssue602_PrewarmCanonEqualsCollapseCanon(t *testing.T) {
	for _, q := range []string{
		"",
		"?labelSelector=env%3Dprod",
		"?labelSelector=" + url.QueryEscape("tier,env in (prod,dev)"),
		"?fieldSelector=" + url.QueryEscape("metadata.name==x") + "&labelSelector=a",
	} {
		path := q618NSPath("team-a") + q
		stage := &templates.API{Name: "s", Path: path}
		gvr, prewarmCanon, ok := extractClusterListTargetFromStage(stage)
		if !ok {
			t.Fatalf("%s: prewarm did not enumerate it", path)
		}
		cgvr, collapseCanon, cok := clusterListElementTarget(path, nil)
		if !cok || cgvr != gvr {
			t.Fatalf("%s: collapse target = %v ok=%v, prewarm %v", path, cgvr, cok, gvr)
		}
		if prewarmCanon != collapseCanon {
			t.Fatalf("#602 %s: prewarm canon %q != collapse canon %q — the prewarmed cell is not the one the collapse reads",
				path, prewarmCanon, collapseCanon)
		}
		pk := cache.ComputeKey(contentKeyInputsQ(gvr, "", "", prewarmCanon))
		ck := cache.ComputeKey(contentKeyInputsQ(cgvr, "", "", collapseCanon))
		if pk != ck {
			t.Fatalf("#602 %s: prewarm and collapse cell keys differ", path)
		}
	}
	// A metadata.namespace selector is neither prewarmed nor collapsed.
	ns := q618NSPath("team-a") + "?fieldSelector=metadata.namespace%3Dteam-a"
	if _, _, ok := extractClusterListTargetFromStage(&templates.API{Name: "s", Path: ns}); ok {
		t.Fatalf("prewarm must skip %s", ns)
	}
	if _, _, ok := clusterListElementTarget(ns, nil); ok {
		t.Fatalf("collapse must decline %s", ns)
	}
}

// TestIssue618_NoParamIsSilentlyDropped is the guard the team asked for: for
// every param name a caller can send, planListQuery either FALLS THROUGH or the
// param is one of the two it applies — and an applied selector is REFLECTED in
// the canonical form (so it reaches the key and the rebuilt path). A param
// added to the accepted set without being applied would fail the second half.
func TestIssue618_NoParamIsSilentlyDropped(t *testing.T) {
	params := []string{
		"labelSelector", "fieldSelector", "limit", "continue", "resourceVersion",
		"resourceVersionMatch", "watch", "allowWatchBookmarks", "sendInitialEvents",
		"timeoutSeconds", "pretty", "fieldManager", "dryRun", "propagationPolicy",
		"orphanDependents", "gracePeriodSeconds", "includeObject", "as", "x",
	}
	values := map[string]string{
		"labelSelector": "env=prod",
		"fieldSelector": "metadata.name=n",
	}
	for _, pn := range params {
		v, ok := values[pn]
		if !ok {
			v = "1"
		}
		for _, base := range []string{"/apis/g/v1/widgets", "/apis/g/v1/namespaces/ns/widgets"} {
			path := base + "?" + url.Values{pn: {v}}.Encode()
			q := planListQuery(path, true, strings.Contains(base, "/namespaces/"))
			if !q.applicable {
				continue // falls through intact: nothing dropped
			}
			if pn != listQueryParamLabelSelector && pn != listQueryParamFieldSelector {
				t.Fatalf("#618: param %q was ACCEPTED but is not applied in process — silently dropped", pn)
			}
			if !strings.Contains(q.canon, pn+"=") {
				t.Fatalf("#618: param %q was accepted but is missing from the canonical query %q — the key and the "+
					"rebuilt refresh path would drop it", pn, q.canon)
			}
		}
	}
}

// TestIssue618_ContentKey_SelectorAndUnselectedNeverShare: the selector cell's
// key differs from the unselected one's, two spellings share one cell, the
// cell's Inputs carry the canonical query, and the refresher's rebuilt call
// re-applies the selector.
func TestIssue618_ContentKey_SelectorAndUnselectedNeverShare(t *testing.T) {
	newQ618Watcher(t)
	store := cache.ResolvedCache()
	sel := q618ClusterPath + "?labelSelector=env%3Dprod"
	selSpelt := q618ClusterPath + "?labelSelector=env=prod"
	for _, p := range []string{q618ClusterPath, sel, selSpelt} {
		if _, served, ok, _ := apistageContentServe(q618Ctx(), store, buildCall(http.MethodGet, p), false); !ok || !served {
			t.Fatalf("serve %s", p)
		}
	}
	unKey := cache.ComputeKey(contentKeyInputs(f1WidgetsGVR, "", ""))
	selKey := cache.ComputeKey(contentKeyInputsQ(f1WidgetsGVR, "", "", "labelSelector=env%3Dprod"))
	if unKey == selKey {
		t.Fatalf("#618: the selector content key equals the unselected one")
	}
	unEntry, ok1 := store.GetNoTouch(unKey)
	selEntry, ok2 := store.GetNoTouch(selKey)
	if !ok1 || !ok2 {
		t.Fatalf("want both cells resident: unselected=%v selector=%v", ok1, ok2)
	}
	if got := namesInEnvelopeBytes(t, unEntry.RawJSON); !eqNames(got, names(all3, "w1", "w2", "w3")) {
		t.Fatalf("unselected cell holds %v", got)
	}
	if got := namesInEnvelopeBytes(t, selEntry.RawJSON); !eqNames(got, names(all3, "w1")) {
		t.Fatalf("selector cell holds %v", got)
	}
	if selEntry.Inputs == nil || selEntry.Inputs.Query != "labelSelector=env%3Dprod" {
		t.Fatalf("#618: the selector cell's Inputs must carry the canonical query; got %+v", selEntry.Inputs)
	}
	var apistage int
	queryHashes := map[string]bool{}
	store.RangeMetadata(func(m cache.ResolvedEntryMeta) bool {
		if m.CacheEntryClass == cache.CacheEntryClassApistage && m.Resource == "widgets" {
			apistage++
			queryHashes[m.QueryHash] = true
		}
		return true
	})
	if apistage != 2 || len(queryHashes) != 2 {
		t.Fatalf("#618: two spellings of one selector must share ONE cell, and the debug metadata must tell the two "+
			"cells apart; widgets content cells = %d, distinct queryHash = %d, want 2 and 2", apistage, len(queryHashes))
	}
	raw, err := RefreshContentEntry(q618Ctx(), *selEntry.Inputs)
	if err != nil {
		t.Fatalf("RefreshContentEntry: %v", err)
	}
	if got := namesInEnvelopeBytes(t, raw); !eqNames(got, names(all3, "w1")) {
		t.Fatalf("#618 refresher: re-dispatching the selector cell rebuilt %v — the refresher dropped the selector", got)
	}
}

// TestIssue618_ListQueryCounter_InProcessVsFallthrough: the counter moves by
// outcome and reason, reaches /debug/vars, and a call with no query is not
// counted.
func TestIssue618_ListQueryCounter_InProcessVsFallthrough(t *testing.T) {
	newQ618Watcher(t)
	RegisterListQueryExpvarForTest()
	ResetListQueryOutcomesForTest()
	t.Cleanup(ResetListQueryOutcomesForTest)

	ctx := q618Ctx()
	dispatchViaInformer(ctx, buildCall(http.MethodGet, q618ClusterPath))
	dispatchViaInformer(ctx, buildCall(http.MethodGet, q618ClusterPath+"?labelSelector=env%3Dprod"))
	dispatchViaInformer(ctx, buildCall(http.MethodGet, q618ClusterPath+"?limit=1"))
	dispatchViaInformer(ctx, buildCall(http.MethodGet, q618ClusterPath+"?fieldSelector=spec.x%3D1"))

	got := map[string]uint64{}
	for _, s := range ListQueryOutcomeSnapshot() {
		got[s.Outcome+"/"+s.Reason] = s.Count
	}
	want := map[string]uint64{
		"in_process/selector":           1,
		"fallthrough/pagination":        1,
		"fallthrough/unsupported_field": 1,
	}
	for k, v := range got {
		if v != want[k] {
			t.Errorf("series %s = %d, want %d", k, v, want[k])
		}
	}
	v := expvar.Get("snowplow_list_query_total")
	if v == nil || !strings.Contains(v.String(), `"selector":1`) || !strings.Contains(v.String(), `"pagination":1`) {
		t.Fatalf("#618: snowplow_list_query_total not on /debug/vars with the live counts: %v", v)
	}
}

// TestIssue618_EveryServeSiteClassifiesTheQuery is the structural guard: in the
// api package, every non-test function that parses an apiserver path with
// cache.ParseAPIServerPathToDep either classifies its query with planListQuery
// or is named in the reviewed list below with the reason it does not serve a
// LIST from a cache. A new serve site that forgets the query fails here.
func TestIssue618_EveryServeSiteClassifiesTheQuery(t *testing.T) {
	notServing := map[string]string{
		"callWantsWireDump":                  "wire-dump verbosity decision only",
		"k8sListGVR":                         "E6 normalisation flag only (#674): is the response a list envelope, of which GVR; serves and keys nothing",
		"dispatchOneCall":                    "sensitive-resource check only (cache.IsSensitiveResource); no serve",
		"runStage":                           "dep-edge record — a selector LIST depends on the same (gvr, ns) list edge (over-approximation is safe)",
		"gateContentEnvelope":                "gates an envelope apistageContentServe already classified and keyed",
		"maybeResolveInProcess":              "GET-by-name of a RESTAction/Widget only; a query on a GET falls through earlier (query_on_get)",
		"inspectInClusterStage":              "inspect/RBAC preflight: derives the RBAC verb from the path shape, serves nothing",
		"validInterpolatedPath":              "#293 malformed-dial guard: path-shape check, serves nothing",
		"extractClusterListGVRFromStagePath": "GVR shape half of extractClusterListTargetFromStage, which classifies",
		"classifyOwnDialForbidden":           "#681: classifies an apiserver 403 and builds its identity-free error text from (gvr, ns, name); serves and keys nothing",
		"stageMessage":                       "#709 (+#729 redirect variant): formats a failed stage's error text from (code, gvr, ns, name) only — never a key, never a served data body; TestIssue709_CanonicalStageMessage_CarriesNoQuery pins that no query reaches it",
		"isAccessReviewPath":                 "#615: classifies a POST path as an access-review create (group, resource, no name) to decide a branch-E dial's identity (impersonate or fail closed); a query makes it false; serves and keys nothing",
	}
	fset := token.NewFileSet()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var offenders []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, f, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range file.Decls {
			fn, ok := d.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			parses, classifies := false, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.SelectorExpr:
					if fun.Sel.Name == "ParseAPIServerPathToDep" {
						parses = true
					}
				case *ast.Ident:
					if fun.Name == "planListQuery" {
						classifies = true
					}
				}
				return true
			})
			if parses && !classifies {
				if _, ok := notServing[fn.Name.Name]; !ok {
					offenders = append(offenders, f+":"+fn.Name.Name)
				}
			}
		}
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Fatalf("#618: these functions parse an apiserver path but never classify its query string — a LIST "+
			"served or keyed there silently drops ?labelSelector/?fieldSelector/?limit. Call planListQuery, or add "+
			"the function to the reviewed notServing list with the reason it serves nothing:\n  %s",
			strings.Join(offenders, "\n  "))
	}
}
