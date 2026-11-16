// status_tree_1079_test.go — #1079 arms. BEFORE is the pre-#1079 code, run in
// the test: jqutil.Eval of the RESTAction filter (restactions.Resolve) and
// json.Unmarshal of its text into a map (apiref.rawExtensionToMapEncoded).
// AFTER is evalFilter + jqsupport.DecodedObjectOf. On the golden corpus the
// two must agree on the Status bytes, on the tree (reflect.DeepEqual: dynamic
// types too), and on every widget's widgetDataTemplate output byte for byte.

package restactions

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jqutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func before1079(t testing.TB, filter string, dict map[string]any) (string, map[string]any, error) {
	s, err := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: filter, Data: dict, ModuleLoader: jqsupport.ModuleLoader()})
	if err != nil {
		return "", nil, err
	}
	var m map[string]any
	err = json.Unmarshal([]byte(s), &m)
	return s, m, err
}

// widgetEval1079 is widgetdatatemplate.Resolve's evaluation of one entry.
func widgetEval1079(t testing.TB, expr string, ds map[string]any) string {
	s, err := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: expr, Data: ds, ModuleLoader: jqsupport.ModuleLoader()})
	if err != nil {
		return "ERROR: " + err.Error()
	}
	return s
}

func TestIssue1079_GoldenCorpus_TreeIsTheDecodeAndWidgetsAreByteIdentical(t *testing.T) {
	c := loadCorpus1079(t)
	objs := compositionObjects1079(350, time.Now()) // 7 states × 50 namespaces: every combination once
	// The slice the widgets resolver injects for a paginated request (and its
	// absence for an unpaginated one, the dashboard's).
	slices := []map[string]any{nil, {"page": 1, "perPage": 50, "offset": 0}, {"page": 3, "perPage": 20, "offset": 40}}

	raNames := make([]string, 0, len(c.Restactions))
	for n := range c.Restactions {
		raNames = append(raNames, n)
	}
	sort.Strings(raNames)
	var cells, widgetOutputs, nonEmpty int
	for _, raName := range raNames {
		ra := c.Restactions[raName]
		stages := stageOutputs1079(t, c, raName, objs)
		for _, extras := range extras1079[raName] {
			dict := dict1079(stages, extras)
			beforeText, beforeTree, err := before1079(t, ra.Filter, dict)
			if err != nil {
				t.Fatalf("%s %v: before: %v", raName, extras, err)
			}
			vals, afterText, err := evalFilter(t.Context(), ra.Filter, dict)
			if err != nil {
				t.Fatalf("%s %v: after: %v", raName, extras, err)
			}
			if afterText != beforeText {
				t.Fatalf("#1079 RED %s %v: Status bytes differ (%d vs %d bytes)", raName, extras, len(afterText), len(beforeText))
			}
			tree, exact := jqsupport.DecodedObjectOf(vals)
			if !exact {
				t.Fatalf("%s %v: a composition RESTAction's output must take the exact path", raName, extras)
			}
			if !reflect.DeepEqual(beforeTree, tree) {
				t.Fatalf("#1079 RED %s %v: the tree is not the decode", raName, extras)
			}
			cells++
			for _, w := range c.Widgets {
				if w.APIRef != raName {
					continue
				}
				for _, e := range w.WidgetDataTemplate {
					expr, isQuery := jqutil.MaybeQuery(e.Expression)
					if !isQuery {
						continue
					}
					for _, sl := range slices {
						setSlice := func(m map[string]any) {
							if sl == nil {
								delete(m, "slice")
							} else {
								m["slice"] = sl
							}
						}
						setSlice(beforeTree)
						setSlice(tree)
						b := widgetEval1079(t, expr, beforeTree)
						a := widgetEval1079(t, expr, tree)
						if a != b {
							// `now` (throughput-chart) may have crossed a bucket edge
							// between the two evaluations: re-evaluate BEFORE once.
							if b2 := widgetEval1079(t, expr, beforeTree); a != b2 {
								t.Fatalf("#1079 RED %s/%s %s %v slice=%v: widget output differs:\n  before: %.300s\n  after:  %.300s",
									w.Kind, w.Name, e.ForPath, extras, sl, b, a)
							}
						}
						widgetOutputs++
						if !strings.HasPrefix(a, "ERROR:") && a != "null" && a != "[]" && a != "{}" && a != `""` {
							nonEmpty++
						}
					}
				}
			}
		}
	}
	// The arm must have exercised the corpus, not an empty loop.
	if cells != 10 || widgetOutputs < 300 || nonEmpty < widgetOutputs*9/10 {
		t.Fatalf("corpus coverage: cells=%d widget outputs=%d non-empty=%d", cells, widgetOutputs, nonEmpty)
	}
	t.Logf("golden: %d RESTAction cells, %d widget outputs (%d non-empty) byte-identical", cells, widgetOutputs, nonEmpty)
}

// TestIssue1079_ResolveStatusTree_IsResolve drives the exported entry point on
// filter-only RESTActions: Status.Raw is the pre-#1079 text, and when exact the
// tree is its decode; when not exact the caller is told to decode.
func TestIssue1079_ResolveStatusTree_IsResolve(t *testing.T) {
	for _, tc := range []struct {
		name, filter string
		wantExact    bool
	}{
		{"object", `{"a": 1, "b": [1.5, "x", null, {}], "c": (1/3), "big": 9007199254740993}`, true},
		{"empty (null)", `empty`, true},
		{"null", `null`, true},
		{"array top level", `[1, 2]`, false},
		{"stream", `{"a": 1}, {"b": 2}`, false},
		{"infinite", `{"x": infinite}`, false},
		{"nan", `{"x": nan}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filter := tc.filter
			in := &templates.RESTAction{ObjectMeta: metav1.ObjectMeta{Name: "ra-1079", Namespace: "ns"}}
			in.Spec.Filter = &filter
			ra, tree, exact, err := ResolveStatusTree(t.Context(), ResolveOptions{In: in})
			if err != nil {
				t.Fatal(err)
			}
			if exact != tc.wantExact {
				t.Fatalf("exact=%v want %v", exact, tc.wantExact)
			}
			beforeText, _ := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: filter, Data: map[string]any{}, ModuleLoader: jqsupport.ModuleLoader()})
			if beforeText == "" {
				beforeText = "null"
			}
			if string(ra.Status.Raw) != beforeText {
				t.Fatalf("Status.Raw = %s, want %s", ra.Status.Raw, beforeText)
			}
			if !exact {
				return
			}
			var want map[string]any
			if err := json.Unmarshal(ra.Status.Raw, &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, tree) {
				t.Fatalf("tree %#v is not the decode %#v", tree, want)
			}
		})
	}
}

// TestIssue1079_EvalFilter_ErrorsAreJqutils — the parse, compile and runtime
// errors evalFilter returns are jqutil.Eval's, so Resolve's error text is too.
func TestIssue1079_EvalFilter_ErrorsAreJqutils(t *testing.T) {
	for _, q := range []string{`{`, `undefined_fn(1)`, `error("boom")`, `.a.b`, `1, error("late")`, `include "nope"; .`} {
		data := map[string]any{"a": "str"}
		_, want := jqutil.Eval(t.Context(), jqutil.EvalOptions{Query: q, Data: data, ModuleLoader: jqsupport.ModuleLoader()})
		_, _, got := evalFilter(t.Context(), q, data)
		if (want == nil) != (got == nil) || (want != nil && want.Error() != got.Error()) {
			t.Fatalf("%q: error %v, want jqutil's %v", q, got, want)
		}
	}
}
