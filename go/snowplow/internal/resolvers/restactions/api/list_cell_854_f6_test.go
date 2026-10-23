// list_cell_854_f6_test.go — #854 F6 (MC8, C4): no consumer mutates a shared
// item of the incremental list cell.
//
// Under the incremental cell an unchanged object's decoded form is shared by
// every generation — possibly for the pod's lifetime — instead of by one fill.
// So every consumer that can alias a cell item must leave it untouched. This
// arm runs the PRODUCTION stage handler (jsonHandlerCore: the #74 list
// normalise/splice, the UAF refilter, the stage jq filter, the merge), the
// RESTAction's outer filter, and every widget's widgetDataTemplate expression
// and resourcesRefsTemplate iterator over the RESTAction output, for every
// RESTAction and widget of:
//
//   - testdata/portal_deployed_1.8.84_corpus.json — the portal suite DEPLOYED on
//     krateo-057 (15 releases, chart version 1.8.84; extracted with the repo's
//     corpusgen from `helm get manifest`, provenance inside the file), and
//   - internal/cache/jqinterest/testdata/portal_corpus.json — the #603 corpus.
//
// Every stage is fed the cell's shared items (serveParsedListEnvelope, the
// shallow envelope the content HIT serves), concurrently from several
// goroutines while another applies changes and builds generations. After each
// pass every item of the generation it read is re-decoded from its raw and
// compared: any difference is a consumer write into a shared item. Run it
// under -race (the gate does).
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/support/jq"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type f6Corpus struct {
	Source struct {
		Repo   string `json:"repo"`
		Commit string `json:"commit"`
	} `json:"source"`
	RESTActions []struct {
		Name      string  `json:"name"`
		Namespace string  `json:"namespace"`
		Filter    *string `json:"filter"`
		Stages    []struct {
			Name   string  `json:"name"`
			Path   string  `json:"path"`
			Filter *string `json:"filter"`
			UAF    *struct {
				Verb          string `json:"verb"`
				ResourcesFrom string `json:"resourcesFrom"`
				NamespaceFrom string `json:"namespaceFrom"`
				NameFrom      string `json:"nameFrom"`
			} `json:"uaf"`
		} `json:"stages"`
	} `json:"restactions"`
	Widgets []struct {
		Name          string `json:"name"`
		APIRef        string `json:"apiRef"`
		DataTemplates []struct {
			Expression string `json:"expression"`
		} `json:"widgetDataTemplate"`
		ResourcesRefs []struct {
			Iterator string `json:"iterator"`
		} `json:"resourcesRefsTemplate"`
	} `json:"widgets"`
}

func loadF6Corpus(t *testing.T, path string) *f6Corpus {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var c f6Corpus
	if err := json.Unmarshal(b, &c); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	if len(c.RESTActions) == 0 {
		t.Fatalf("%s holds no RESTAction", path)
	}
	return &c
}

// f6Obj is a fixture object with the fields portal filters read: labels,
// annotations, spec, status conditions, ownerReferences, timestamps.
func f6Obj(i, gen int) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "composition.krateo.io/v1",
		"kind":       "FullStackApp",
		"metadata": map[string]any{
			"namespace": r1Ns, "name": fmt.Sprintf("fsa-%05d", i),
			"uid":               fmt.Sprintf("uid-%d", i),
			"creationTimestamp": "2026-10-23T00:00:00Z",
			"labels":            map[string]any{"env": []string{"prod", "dev"}[i%2], "krateo.io/composition-id": fmt.Sprint(i)},
			"annotations":       map[string]any{"krateo.io/description": "f6"},
			"ownerReferences":   []any{map[string]any{"kind": "CompositionDefinition", "name": "cd", "uid": "cd-uid"}},
		},
		"spec": map[string]any{"replicas": int64(i % 3), "gen": int64(gen), "values": map[string]any{"a": []any{"x", "y"}}},
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": "Ready", "status": []string{"True", "False"}[gen%2], "reason": "f6", "lastTransitionTime": "2026-10-23T00:00:00Z"},
				map[string]any{"type": "Synced", "status": "True"},
			},
			"managed": r1ManagedList(1 + gen%3),
		},
	}}
}

// f6RunCorpus runs every RESTAction (and its widgets) of corpus over the
// generation v's shared items, through the production handlers. Errors from a
// filter that does not fit the fixture are expected and ignored: the arm is about
// writes into shared items, not about results.
func f6RunCorpus(ctx context.Context, corpus *f6Corpus, v cache.ListView) (stages, widgets int) {
	parsed := parsedListEnvelope{items: v.Items(), apiVersion: v.APIVersion(), kind: v.Kind()}
	raOut := map[string]any{}
	for _, ra := range corpus.RESTActions {
		out := map[string]any{}
		dict := map[string]any{"username": r1User, "ns": r1Ns}
		for _, st := range ra.Stages {
			env, ok := serveParsedListEnvelope(parsed) // the content HIT's shallow envelope
			if !ok {
				continue
			}
			opts := jsonHandlerOptions{
				key: st.Name, out: out, dict: dict, filter: st.Filter,
				apiCallName: st.Name, raNamespace: ra.Namespace, raName: ra.Name,
				k8sList: true, listGVR: r1FsaGVR,
			}
			if st.UAF != nil {
				opts.uaf = &templates.UserAccessFilterSpec{
					Verb: st.UAF.Verb, ResourcesFrom: st.UAF.ResourcesFrom,
					NamespaceFrom: st.UAF.NamespaceFrom, NameFrom: st.UAF.NameFrom,
				}
			}
			_ = jsonHandlerCore(ctx, opts, env)
			stages++
		}
		var final any = out
		if ra.Filter != nil {
			if fv, ok, err := EvalValue(ctx, *ra.Filter, out, jq.ModuleLoader()); err == nil && ok {
				final = fv
			}
		}
		raOut[ra.Namespace+"/"+ra.Name] = final
	}
	for _, w := range corpus.Widgets {
		data, ok := raOut[w.APIRef]
		if !ok {
			continue
		}
		for _, dt := range w.DataTemplates {
			expr := strings.TrimSpace(dt.Expression)
			expr = strings.TrimSuffix(strings.TrimPrefix(expr, "${"), "}")
			_, _, _ = EvalValue(ctx, expr, data, jq.ModuleLoader())
			widgets++
		}
		for _, rr := range w.ResourcesRefs {
			expr := strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(rr.Iterator), "${"), "}")
			_, _, _ = EvalValue(ctx, expr, data, jq.ModuleLoader())
			widgets++
		}
	}
	return stages, widgets
}

func TestListCell854_F6_PortalCorpusNeverMutatesSharedItems(t *testing.T) {
	const objects = 120
	dyn := newInc854Watcher(t, 0)
	res := dyn.Resource(r1FsaGVR).Namespace(r1Ns)
	for i := 0; i < objects; i++ {
		if _, err := res.Create(context.Background(), f6Obj(i, 0), metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	if !fill710WaitFor(10e9, func() bool {
		raw, _ := dispatchViaInformer(cache.WithApistageContentResolve(r1Ctx()), fill710ListCall())
		p, ok := parseListEnvelope(r1FsaGVR, raw)
		return ok && len(p.items) == objects
	}) {
		t.Fatal("PRECONDITION: the informer never held the fixture")
	}
	store := cache.ResolvedCache()
	c := inc854Anchor(t, store, fill710ListCall())

	corpora := []*f6Corpus{
		loadF6Corpus(t, "testdata/portal_deployed_1.8.84_corpus.json"),
		loadF6Corpus(t, "../../../cache/jqinterest/testdata/portal_corpus.json"),
	}
	ctx := r1Ctx()
	var bad, stagesRun, widgetsRun atomic.Int64
	stop := make(chan struct{})
	var wg sync.WaitGroup
	workers := runtime.GOMAXPROCS(0)
	if workers > 6 {
		workers = 6
	}
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				v, ok := c.Snapshot(cache.DepGenEpochNow())
				if !ok {
					return
				}
				for _, corpus := range corpora {
					s, wd := f6RunCorpus(ctx, corpus, v)
					stagesRun.Add(int64(s))
					widgetsRun.Add(int64(wd))
				}
				bad.Add(int64(v.VerifyItemsForTest()))
			}
		}()
	}
	// Concurrent applies: 200 changes while the corpus runs.
	submitted0 := cache.DepWatchStatsSnapshot().EventsSubmitted
	for g := 1; g <= 200; g++ {
		if _, err := res.Update(context.Background(), f6Obj(g%objects, g), metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
		if g%50 == 0 {
			// The fake watcher's channel holds 100 events (a full one panics):
			// let the informer deliver every write before writing on.
			want := submitted0 + uint64(g)
			if !fill710WaitFor(10*time.Second, func() bool { return cache.DepWatchStatsSnapshot().EventsSubmitted >= want }) {
				t.Fatalf("the informer never delivered write %d", g)
			}
		}
		if g%20 == 0 {
			if _, ok := c.Snapshot(cache.DepGenEpochNow()); !ok {
				t.Fatal("the cell unanchored during the corpus run")
			}
		}
	}
	// Let every worker finish at least one more full pass on a late generation.
	before := stagesRun.Load()
	for stagesRun.Load() < before+int64(workers) {
		runtime.Gosched()
	}
	close(stop)
	wg.Wait()
	if n := bad.Load(); n != 0 {
		t.Fatalf("F6 RED: %d shared item(s) no longer equal a fresh decode of their raw after the portal corpus ran over them — a consumer writes into the incremental cell's shared items", n)
	}
	v, _ := c.Snapshot(cache.DepGenEpochNow())
	if n := v.VerifyItemsForTest(); n != 0 {
		t.Fatalf("F6 RED: %d item(s) of the final generation were mutated", n)
	}
	t.Logf("F6: %d RESTActions + %d widgets (deployed 1.8.84) and %d + %d (#603 corpus); %d stage runs, %d widget expressions over shared items, 0 mutated",
		len(corpora[0].RESTActions), len(corpora[0].Widgets), len(corpora[1].RESTActions), len(corpora[1].Widgets),
		stagesRun.Load(), widgetsRun.Load())
}
