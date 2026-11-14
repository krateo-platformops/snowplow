// run23_filter_test.go — run 23 CPU profile, the RESTAction filter:
//   - the filter ran on context.TODO(), and the gojq fork polls ctx.Done()
//     before every opcode on any context but context.Background()
//     (execute.go:24): ~2.7% of all CPU for a channel that is nil;
//   - it re-parsed and re-compiled its program on every resolve (≈0.56%).
//
// BEFORE is the pre-fix evalFilter, run in the test: inline gojq.Parse +
// gojq.Compile, on context.TODO(). AFTER is evalFilter (jqsupport.Compile) on
// context.Background(), as resolve calls it. On the real corpora — the portal
// chart run 20 installed (testdata/issue1079) and every RESTAction filter the
// deployed portal suite carries (api/testdata/portal_deployed_1.8.84_corpus)
// — the two must agree on every output value (reflect.DeepEqual: dynamic types
// too), on the Status text, and on every error.
//
//	go test ./internal/resolvers/restactions/ -run '^$' -bench Run23 -benchmem -count=6

package restactions

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/itchyny/gojq"
	"github.com/krateo-platformops/plumbing/jqutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// evalFilterBefore is evalFilter as it was before run 23's fix.
func evalFilterBefore(ctx context.Context, query string, data any) ([]any, string, error) {
	pq, err := gojq.Parse(query)
	if err != nil {
		return nil, "", fmt.Errorf("invalid jq query %q: %w", query, err)
	}
	code, err := gojq.Compile(pq, gojq.WithModuleLoader(jqsupport.ModuleLoader()))
	if err != nil {
		return nil, "", fmt.Errorf("unable to compile jq query %q: %w", query, err)
	}
	var (
		vals  []any
		first string
		rest  strings.Builder
	)
	iter := code.RunWithContext(ctx, data)
	for {
		v, ok := iter.Next()
		if !ok {
			break
		}
		if verr, isErr := v.(error); isErr {
			return nil, "", verr
		}
		s, err := jqutil.Eval(ctx, jqutil.EvalOptions{Query: ".", Data: v})
		if err != nil {
			return nil, "", err
		}
		vals = append(vals, v)
		switch len(vals) {
		case 1:
			first = s
		case 2:
			rest.WriteString(first)
			rest.WriteString(s)
		default:
			rest.WriteString(s)
		}
	}
	if len(vals) > 1 {
		return vals, rest.String(), nil
	}
	return vals, first, nil
}

type filterResult struct {
	vals []any
	text string
	err  string
}

func runBefore(q string, dict any) filterResult {
	v, s, err := evalFilterBefore(context.TODO(), q, dict)
	return filterResult{v, s, fmt.Sprint(err)}
}

func runAfter(q string, dict any) filterResult {
	v, s, err := evalFilter(context.Background(), q, dict)
	return filterResult{v, s, fmt.Sprint(err)}
}

// sameResult compares AFTER (twice: the second run reuses the cached program)
// with BEFORE. A filter that reads `now` can cross a time bucket between two
// evaluations, so a mismatch re-runs BEFORE once, as the #1079 golden does.
func sameResult(t *testing.T, label, q string, dict any) filterResult {
	t.Helper()
	want := runBefore(q, dict)
	for round := 0; round < 2; round++ {
		got := runAfter(q, dict)
		if reflect.DeepEqual(got, want) {
			continue
		}
		if want2 := runBefore(q, dict); reflect.DeepEqual(got, want2) {
			want = want2
			continue
		}
		t.Fatalf("%s round %d: after differs from before\n after:  err=%s text=%.300s\n before: err=%s text=%.300s",
			label, round, got.err, got.text, want.err, want.text)
	}
	return want
}

// TestRun23_Filter_Issue1079CorpusIdenticalToBefore — the two composition
// RESTActions of portal 1.8.85, over 350 compositions (every state × 50
// namespaces), through their REAL stage filters, for every request the
// composition pages send.
func TestRun23_Filter_Issue1079CorpusIdenticalToBefore(t *testing.T) {
	c := loadCorpus1079(t)
	objs := compositionObjects1079(350, time.Now())
	cells := 0
	for raName, ra := range c.Restactions {
		stages := stageOutputs1079(t, c, raName, objs)
		for _, extras := range extras1079[raName] {
			r := sameResult(t, fmt.Sprintf("%s %v", raName, extras), ra.Filter, dict1079(stages, extras))
			if r.err != "<nil>" || len(r.text) < 1000 {
				t.Fatalf("%s %v: the corpus cell must resolve to real content: err=%s, %d bytes", raName, extras, r.err, len(r.text))
			}
			cells++
		}
	}
	if cells != 10 {
		t.Fatalf("corpus coverage: %d cells, want 10", cells)
	}
}

// portalRESTActions1884 is every RESTAction of the portal suite deployed on
// krateo-057 (portal 1.8.84): its filter and its stage names.
type portalRA1884 struct {
	Name   string  `json:"name"`
	Filter *string `json:"filter"`
	Stages []struct {
		Name string `json:"name"`
	} `json:"stages"`
}

func loadPortalRESTActions1884(tb testing.TB) []portalRA1884 {
	tb.Helper()
	raw, err := os.ReadFile("api/testdata/portal_deployed_1.8.84_corpus.json")
	if err != nil {
		tb.Fatal(err)
	}
	var doc struct {
		RestActions []portalRA1884 `json:"restactions"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		tb.Fatal(err)
	}
	if len(doc.RestActions) < 80 {
		tb.Fatalf("corpus: want the deployed portal's 92 RESTActions, got %d", len(doc.RestActions))
	}
	return doc.RestActions
}

// portalDicts1884 are dicts for one RESTAction: no stage output, extras only,
// every stage holding a list of compositions, and every stage holding the
// list envelope a LIST stage stores ({items: [...]}) — the shapes that drive
// the deployed filters down their empty, extras and data branches.
func portalDicts1884(ra portalRA1884, objs []any) []map[string]any {
	extras := map[string]any{
		"namespace": "bench-ns-03", "name": "app-00003", "ns": "bench-ns-03",
		"username": "alice", "status": "failed", "q": "app", "range": "7d",
		"slice": map[string]any{"page": 1, "perPage": 20, "offset": 0},
	}
	asList, asEnvelope := map[string]any{}, map[string]any{}
	for k, v := range extras {
		asList[k], asEnvelope[k] = v, v
	}
	for _, s := range ra.Stages {
		asList[s.Name] = objs
		asEnvelope[s.Name] = map[string]any{"items": objs}
	}
	return []map[string]any{{}, extras, asList, asEnvelope}
}

// TestRun23_Filter_PortalCorpusIdenticalToBefore — every deployed portal
// RESTAction filter, on each of its dicts.
func TestRun23_Filter_PortalCorpusIdenticalToBefore(t *testing.T) {
	// The JSON-decoded form a stage's output takes (float64 numbers): these
	// dicts stand in for stage outputs, not for raw informer objects.
	var objs []any
	b, err := json.Marshal(compositionObjects1079(70, time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &objs); err != nil {
		t.Fatal(err)
	}
	filters, results, values := 0, 0, 0
	for _, ra := range loadPortalRESTActions1884(t) {
		if ra.Filter == nil {
			continue
		}
		filters++
		for i, d := range portalDicts1884(ra, objs) {
			r := sameResult(t, fmt.Sprintf("%s dict %d", ra.Name, i), *ra.Filter, d)
			results++
			if r.err == "<nil>" && r.text != "null" && r.text != "[]" && r.text != "{}" {
				values++
			}
		}
	}
	// The arm must have exercised the corpus: most filters produce content.
	if filters < 80 || values < results/2 {
		t.Fatalf("corpus coverage: %d filters, %d results, %d with content", filters, results, values)
	}
	t.Logf("%d filters × dicts: %d results identical (%d with content)", filters, results, values)
}

// TestRun23_Filter_ConstantOutputsStayUnshared — a cached program's folded
// constants are shared by every resolve that runs it. Through
// ResolveStatusTree the tree handed on is a fresh copy: modifying it changes
// neither the next resolve's Status nor its tree.
func TestRun23_Filter_ConstantOutputsStayUnshared(t *testing.T) {
	const filter = `{"items": [{"name": "a", "labels": {"x": "1"}}], "total": 1, "tags": ["p", "q"]}`
	resolveOnce := func() (string, map[string]any) {
		f := filter
		in := &templates.RESTAction{ObjectMeta: metav1.ObjectMeta{Name: "ra-run23", Namespace: "ns"}}
		in.Spec.Filter = &f
		ra, tree, exact, err := ResolveStatusTree(t.Context(), ResolveOptions{In: in})
		if err != nil || !exact {
			t.Fatalf("resolve: exact=%v err=%v", exact, err)
		}
		return string(ra.Status.Raw), tree
	}
	text1, tree1 := resolveOnce()
	want := map[string]any{}
	if err := json.Unmarshal([]byte(text1), &want); err != nil {
		t.Fatal(err)
	}
	// Modify every level of the first tree in place.
	tree1["total"] = 99.0
	tree1["tags"].([]any)[0] = "MUTATED"
	item := tree1["items"].([]any)[0].(map[string]any)
	item["name"] = "MUTATED"
	item["labels"].(map[string]any)["x"] = "MUTATED"
	for i := 0; i < 3; i++ {
		text, tree := resolveOnce()
		if text != text1 {
			t.Fatalf("resolve %d: Status %s, want %s", i, text, text1)
		}
		if !reflect.DeepEqual(tree, want) {
			t.Fatalf("resolve %d: tree %#v, want %#v", i, tree, want)
		}
	}
}

// TestRun23_Filter_BackgroundAndTODOAgree — the filter's result is the same on
// context.TODO() (before) and context.Background() (after): neither can be
// cancelled, Background only skips the fork's per-opcode poll.
func TestRun23_Filter_BackgroundAndTODOAgree(t *testing.T) {
	c := loadCorpus1079(t)
	objs := compositionObjects1079(350, time.Now())
	for raName, ra := range c.Restactions {
		dict := dict1079(stageOutputs1079(t, c, raName, objs), extras1079[raName][0])
		v1, s1, e1 := evalFilter(context.TODO(), ra.Filter, dict)
		v2, s2, e2 := evalFilter(context.Background(), ra.Filter, dict)
		if fmt.Sprint(e1) != fmt.Sprint(e2) || s1 != s2 || !reflect.DeepEqual(v1, v2) {
			t.Fatalf("%s: TODO and Background results differ", raName)
		}
	}
	// A cancelled context still cancels a program run on it: the fork polls
	// every context that is not Background, so the change is only where the
	// filter never could be cancelled.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := evalFilter(ctx, `[range(100000)] | length`, nil); err == nil {
		t.Fatal("a cancelled context no longer cancels the program")
	}
}

var (
	run23Sink    map[string]any
	run23Ballast []byte
)

// podLikeHeap gives the benchmark process a live heap so the GC paces like the
// pod's: run 23's snowplow GC'd every ~1.95 s at a ~3.3 GB heap (num_gc +158
// over 307 s, mem606 steady), many resolves per cycle. A benchmark's own heap
// is a few MB, so its GC would run several times per resolve, a regime the pod
// never sees and in which the compiled-program cache (swept once per GC
// cycle) is emptied between two uses. Both paths run on the same heap.
func podLikeHeap(b *testing.B) {
	if run23Ballast == nil {
		run23Ballast = make([]byte, 512<<20)
	}
}

// warmPrograms compiles every filter until all of them are cached at the end
// of a pass: the steady state of a pod resolving these RESTActions.
func warmPrograms(b *testing.B, queries ...string) {
	b.Helper()
	for pass := 0; pass < 20; pass++ {
		allCached := true
		for _, q := range queries {
			if _, err := jqsupport.Compile(q); err != nil {
				b.Fatal(err)
			}
		}
		for _, q := range queries {
			if !jqsupport.IsCompiledForTest(q) {
				allCached = false
			}
		}
		if allCached {
			return
		}
	}
	b.Fatal("the programs never all stayed cached for one pass")
}

// BenchmarkRun23_FilterContext — the RESTAction filter of the two composition
// RESTActions, program cached in both: ctx=todo is the pre-fix context (the
// fork polls Done before every opcode), ctx=background the shipped one.
func BenchmarkRun23_FilterContext(b *testing.B) {
	podLikeHeap(b)
	c := loadCorpus1079(b)
	objs := compositionObjects1079(1000, time.Now())
	for _, raName := range []string{"compositions-list", "dashboard-data"} {
		ra := c.Restactions[raName]
		dict := dict1079(stageOutputs1079(b, c, raName, objs), extras1079[raName][0])
		warmPrograms(b, ra.Filter)
		for _, cx := range []struct {
			name string
			ctx  context.Context
		}{{"todo", context.TODO()}, {"background", context.Background()}} {
			b.Run(fmt.Sprintf("ra=%s/n=1000/ctx=%s", raName, cx.name), func(b *testing.B) {
				b.ReportAllocs()
				cpu0 := processCPUNs1079() // see status_tree_1079_bench_test.go: steadier than ns/op on a loaded machine
				defer func() { b.ReportMetric(float64(processCPUNs1079()-cpu0)/float64(b.N), "cpu-ns/op") }()
				for b.Loop() {
					v, _, err := evalFilter(cx.ctx, ra.Filter, dict)
					if err != nil {
						b.Fatal(err)
					}
					run23Sink, _ = jqsupport.DecodedObjectOf(v)
				}
			})
		}
	}
}

// BenchmarkRun23_FilterCompile — the compile half: path=before parses and
// compiles per resolve (evalFilterBefore), path=after reuses the program; both
// on context.Background() so only the compile differs. corpus=portal runs
// every deployed portal RESTAction filter once per op on its extras-only dict
// (the small-RESTAction shape, where the compile is most of the work);
// ra=compositions-list is a 1000-composition resolve.
func BenchmarkRun23_FilterCompile(b *testing.B) {
	podLikeHeap(b)
	type cell struct {
		q    string
		dict map[string]any
	}
	var portal []cell
	for _, ra := range loadPortalRESTActions1884(b) {
		if ra.Filter != nil {
			portal = append(portal, cell{*ra.Filter, portalDicts1884(ra, nil)[1]})
		}
	}
	c := loadCorpus1079(b)
	objs := compositionObjects1079(1000, time.Now())
	ra := c.Restactions["compositions-list"]
	big := []cell{{ra.Filter, dict1079(stageOutputs1079(b, c, "compositions-list", objs), extras1079["compositions-list"][0])}}
	for _, set := range []struct {
		name  string
		cells []cell
	}{{"corpus=portal", portal}, {"ra=compositions-list/n=1000", big}} {
		for _, path := range []string{"before", "after"} {
			eval := evalFilter
			if path == "before" {
				eval = evalFilterBefore
			}
			b.Run(set.name+"/path="+path, func(b *testing.B) {
				if path == "after" {
					qs := make([]string, len(set.cells))
					for i, cl := range set.cells {
						qs[i] = cl.q
					}
					warmPrograms(b, qs...)
				}
				b.ReportAllocs()
				cpu0 := processCPUNs1079() // see status_tree_1079_bench_test.go: steadier than ns/op on a loaded machine
				defer func() { b.ReportMetric(float64(processCPUNs1079()-cpu0)/float64(b.N), "cpu-ns/op") }()
				for b.Loop() {
					for _, cl := range set.cells {
						_, _, _ = eval(context.Background(), cl.q, cl.dict)
					}
				}
			})
		}
	}
}
