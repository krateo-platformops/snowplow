// status_tree_1079_corpus_test.go — #1079: the shared fixture of the golden
// corpus and the benchmark. The corpus is the portal chart run 20 installed
// (testdata/issue1079, provenance inside): the two RESTActions that read every
// composition, and every widgetDataTemplate expression of every widget whose
// apiRef names one of them. The data is composition-shaped the way the
// informer hands it to a stage (unstructured: int64 numbers, nested
// conditions, labels), run through the RESTAction's REAL stage filters.

package restactions

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/maps"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

type corpus1079 struct {
	Provenance  string `json:"provenance"`
	Restactions map[string]struct {
		Source string `json:"source"`
		Stages []struct {
			Name   string  `json:"name"`
			Filter *string `json:"filter"`
		} `json:"stages"`
		Filter string `json:"filter"`
	} `json:"restactions"`
	Widgets []struct {
		Source             string `json:"source"`
		Kind               string `json:"kind"`
		Name               string `json:"name"`
		APIRef             string `json:"apiRef"`
		WidgetDataTemplate []struct {
			ForPath    string `json:"forPath"`
			Expression string `json:"expression"`
		} `json:"widgetDataTemplate"`
	} `json:"widgets"`
}

func loadCorpus1079(tb testing.TB) corpus1079 {
	tb.Helper()
	b, err := os.ReadFile("testdata/issue1079/portal-1.8.85-composition-widgets.json")
	if err != nil {
		tb.Fatal(err)
	}
	var c corpus1079
	if err := json.Unmarshal(b, &c); err != nil {
		tb.Fatal(err)
	}
	if len(c.Restactions) != 2 || len(c.Widgets) == 0 {
		tb.Fatalf("corpus: want the 2 composition RESTActions and their widgets, got %d / %d", len(c.Restactions), len(c.Widgets))
	}
	return c
}

// extras1079 are the request extras the composition pages send (the toolbar's
// ?status/?q/?projects/?scope and the dashboard's ?range), per RESTAction.
var extras1079 = map[string][]map[string]any{
	"compositions-list": {
		{},
		{"status": "failed"},
		{"status": "drift", "q": "app-01"},
		{"projects": "bench-ns-03,bench-ns-07", "scope": "all"},
		{"scope": "platform", "status": "healthy"},
	},
	"dashboard-data": {
		{},
		{"range": "24h"},
		{"range": "7d"},
		{"range": "30d"},
		{"range": "all"},
	},
}

// compositionKinds1079 are the composition kinds of the fixture: kind,
// version, plural.
var compositionKinds1079 = [][3]string{
	{"GithubScaffoldingWithCompositionPage", "v1-2-2", "githubscaffoldingwithcompositionpages"},
	{"FireworksApp", "v1-1-14", "fireworksapps"},
	{"BenchApp", "v1-0-3", "benchapps"},
	{"PostgresInstance", "v0-9-1", "postgresinstances"},
	{"VmInstance", "v2-0-0", "vminstances"},
}

// compositionObjects1079 is n compositions over 50 namespaces (run 20: 50K over
// 50), shaped as the informer stores them. States and ages rotate so every
// state, scope, project and time window of the corpus filters is populated;
// ages sit far from the 24h/7d/30d window edges so `now` moving during a test
// cannot move an item across one.
func compositionObjects1079(n int, now time.Time) []any {
	ages := []time.Duration{time.Hour, 30 * time.Hour, 5 * 24 * time.Hour, 20 * 24 * time.Hour, 45 * 24 * time.Hour}
	out := make([]any, n)
	for i := 0; i < n; i++ {
		k := compositionKinds1079[i%len(compositionKinds1079)]
		ns := fmt.Sprintf("bench-ns-%02d", i%50)
		name := fmt.Sprintf("app-%05d", i)
		ts := now.Add(-ages[i%len(ages)]).UTC().Format(time.RFC3339)
		cond := func(typ, status, reason string) map[string]any {
			return map[string]any{
				"type": typ, "status": status, "reason": reason,
				"lastTransitionTime": ts, "observedGeneration": int64(1 + i%3),
				"message": "reconciled <" + name + "> & ✓",
			}
		}
		var conds []any
		healthy := true
		switch i % 7 {
		case 0, 1, 2:
			conds = []any{cond("Ready", "True", "Available"), cond("Synced", "True", "ReconcileSuccess")}
		case 3:
			conds = []any{cond("Ready", "False", "Unavailable"), cond("Synced", "True", "ReconcileSuccess")}
		case 4:
			conds = []any{cond("Ready", "True", "Available"), cond("Synced", "False", "ReconcileError")}
		case 5:
			conds = []any{cond("Ready", "True", "Available"), cond("Synced", "True", "ReconcileSuccess")}
			healthy = false
		case 6:
			conds = nil // pending: no conditions yet
		}
		labels := map[string]any{"krateo.io/composition-definition-name": k[2]}
		if i%97 == 0 {
			labels["krateo.io/composition-name"] = "installer"
		}
		status := map[string]any{"workloadHealthy": healthy, "managed": []any{
			map[string]any{"apiVersion": "v1", "kind": "ConfigMap", "name": name + "-cm", "namespace": ns},
		}}
		if conds != nil {
			status["conditions"] = conds
		}
		if !healthy {
			status["healthMessage"] = "deployment " + name + " has 0/1 ready replicas"
		}
		out[i] = map[string]any{
			"apiVersion": "composition.krateo.io/" + k[1],
			"kind":       k[0],
			"metadata": map[string]any{
				"name": name, "namespace": ns,
				"uid":               fmt.Sprintf("0b1c%04x-1079-4000-8000-%012x", i%65536, i),
				"creationTimestamp": ts, "generation": int64(1 + i%3),
				"resourceVersion": fmt.Sprintf("%d", 1791534491122175009+int64(i)),
				"labels":          labels,
				"annotations":     map[string]any{"krateo.io/release-name": name},
				"finalizers":      []any{"composition.krateo.io/finalizer"},
			},
			"spec":   map[string]any{"app": map[string]any{"service": map[string]any{"port": int64(31180), "type": "NodePort"}}, "replicas": int64(1)},
			"status": status,
		}
	}
	return out
}

// definitionObjects1079 are the type-discovery stage's inputs: the
// CompositionDefinitions compositions-list lists and the CRDs dashboard-data
// lists.
func definitionObjects1079() (cds, crds []any) {
	for i, k := range compositionKinds1079 {
		cds = append(cds, map[string]any{
			"apiVersion": "core.krateo.io/v1alpha1", "kind": "CompositionDefinition",
			"metadata": map[string]any{"name": k[2], "namespace": "krateo-system", "generation": int64(i + 1)},
			"status":   map[string]any{"resource": k[2], "apiVersion": "composition.krateo.io/" + k[1], "kind": k[0]},
		})
		crds = append(crds, map[string]any{
			"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
			"metadata": map[string]any{"name": k[2] + ".composition.krateo.io"},
			"spec": map[string]any{"group": "composition.krateo.io", "versions": []any{
				map[string]any{"name": k[1], "served": true, "storage": true},
			}},
			"status": map[string]any{"acceptedNames": map[string]any{"plural": k[2], "kind": k[0]}},
		})
	}
	return cds, crds
}

// stageOutputs1079 runs one RESTAction's REAL stage filters over the fixture,
// the way the api stage dispatch hands each stage its response under the
// stage's own name, and returns the stage outputs keyed by stage name.
func stageOutputs1079(tb testing.TB, c corpus1079, ra string, objs []any) map[string]any {
	tb.Helper()
	cds, crds := definitionObjects1079()
	inputs := map[string]any{
		"crds":            map[string]any{"items": cds},
		"allCompositions": map[string]any{"items": objs},
	}
	if ra == "dashboard-data" {
		inputs["crds"] = map[string]any{"items": crds}
	}
	out := map[string]any{}
	for _, st := range c.Restactions[ra].Stages {
		if st.Filter == nil {
			tb.Fatalf("corpus: stage %s/%s has no filter", ra, st.Name)
		}
		v, ok, err := api.EvalValue(tb.Context(), *st.Filter, map[string]any{st.Name: inputs[st.Name]}, jqsupport.ModuleLoader())
		if err != nil || !ok {
			tb.Fatalf("stage %s/%s: ok=%v err=%v", ra, st.Name, ok, err)
		}
		out[st.Name] = v
	}
	return out
}

// dict1079 is the RESTAction's dict for one request: the extras (deep-copied,
// as api.ResolveWithFailure starts the dict) plus every stage output.
func dict1079(stages map[string]any, extras map[string]any) map[string]any {
	d := maps.DeepCopyJSON(extras)
	for k, v := range stages {
		d[k] = v
	}
	return d
}
