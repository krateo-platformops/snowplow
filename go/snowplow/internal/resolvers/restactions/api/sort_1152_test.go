//go:build unit
// +build unit

package api

// sort_1152_test.go — #1152: independent stages run in declared order, not in a
// Go map's. topologicalSort read the stages with no dependency out of a map, so
// they ran in a different order from one resolve to the next. Only that
// randomness goes: stages with no dependency still run first, and dependents
// still run as they become ready, so every order is one main's sort could
// produce. A stage that used to see an independent stage's result still does.

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
)

// s1152 is a stage; each dep is its dependsOn.
func s1152(name string, dep ...string) *templates.API {
	s := &templates.API{Name: name}
	if len(dep) > 0 {
		s.DependsOn = &templates.Dependency{Name: dep[0]}
	}
	return s
}

func TestStageOrder1152_IndependentStagesRunFirstInDeclaredOrder(t *testing.T) {
	cases := []struct {
		name  string
		items []*templates.API
		want  []string
	}{
		{"independent stages, not alphabetical", []*templates.API{s1152("z"), s1152("a"), s1152("m")}, []string{"z", "a", "m"}},
		{"the #1152 pair", []*templates.API{s1152("fast"), s1152("slow")}, []string{"fast", "slow"}},
		{"a dependent runs after every independent stage", []*templates.API{s1152("a"), s1152("b", "a"), s1152("c")}, []string{"a", "c", "b"}},
		{"the same, one stage later", []*templates.API{s1152("a"), s1152("b"), s1152("c", "a"), s1152("d")}, []string{"a", "b", "d", "c"}},
		{"the krateo-dashboard/events shape", []*templates.API{s1152("crds"), s1152("comps", "crds"), s1152("getEvents")}, []string{"crds", "getEvents", "comps"}},
		{"a dependency declared after its dependent", []*templates.API{s1152("a", "c"), s1152("b"), s1152("c")}, []string{"b", "c", "a"}},
		{"a chain declared backwards", []*templates.API{s1152("a", "b"), s1152("b", "c"), s1152("c")}, []string{"c", "b", "a"}},
		{"two dependents of one stage keep declared order", []*templates.API{s1152("x", "r"), s1152("y", "r"), s1152("r")}, []string{"r", "x", "y"}},
		{"dependents run as they become ready", []*templates.API{s1152("r1"), s1152("r2"), s1152("x", "r2"), s1152("y", "r1")}, []string{"r1", "r2", "y", "x"}},
		{"an empty dependsOn is no dependency", []*templates.API{s1152("b"), s1152("a", "")}, []string{"b", "a"}},
		{"a name declared twice is one stage", []*templates.API{s1152("x"), s1152("y"), s1152("x")}, []string{"x", "y"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := topologicalSort(c.items)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

// mainSort1152 is main's topologicalSort before #1152, verbatim: the stages with
// no dependency come out of a map.
func mainSort1152(items []*templates.API) ([]string, error) {
	graph := make(map[string][]string)
	inDegree := make(map[string]int)
	itemSet := make(map[string]bool)

	for _, item := range items {
		itemSet[item.Name] = true

		if item.DependsOn == nil {
			continue
		}

		if dep := item.DependsOn.Name; len(dep) > 0 {
			graph[dep] = append(graph[dep], item.Name)
			inDegree[item.Name]++
		}
	}

	var queue []string
	for item := range itemSet {
		if inDegree[item] == 0 {
			queue = append(queue, item)
		}
	}

	return mainDrain1152(queue, graph, inDegree, len(itemSet))
}

func mainDrain1152(queue []string, graph map[string][]string, inDegree map[string]int, total int) ([]string, error) {
	var sortedItems []string
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		sortedItems = append(sortedItems, item)

		for _, dependent := range graph[item] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	if len(sortedItems) != total {
		return nil, fmt.Errorf("cyclic dependency detected")
	}

	return sortedItems, nil
}

// mainFrom1152 is what main's sort returns when its map yields the stages with
// no dependency in the order seed. ok=false: seed is not exactly those stages.
func mainFrom1152(items []*templates.API, seed []string) (got []string, ok bool, err error) {
	graph := make(map[string][]string)
	inDegree := make(map[string]int)
	itemSet := make(map[string]bool)
	for _, item := range items {
		itemSet[item.Name] = true
		if item.DependsOn != nil && item.DependsOn.Name != "" {
			graph[item.DependsOn.Name] = append(graph[item.DependsOn.Name], item.Name)
			inDegree[item.Name]++
		}
	}
	roots := map[string]bool{}
	for name := range itemSet {
		if inDegree[name] == 0 {
			roots[name] = true
		}
	}
	if len(seed) != len(roots) {
		return nil, false, nil
	}
	for _, name := range seed {
		if !roots[name] {
			return nil, false, nil
		}
		delete(roots, name)
	}
	got, err = mainDrain1152(append([]string(nil), seed...), graph, inDegree, len(itemSet))
	return got, true, err
}

// spec1152 is an adversarial spec: a small alphabet, so names repeat, and a
// dependency that may be absent, empty, the stage itself, unknown, or anything
// declared before or after it. Cycles and unknown stages come out of this.
func spec1152(rng *rand.Rand) []*templates.API {
	names := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	n := 1 + rng.Intn(9)
	alphabet := 1 + rng.Intn(len(names))
	items := make([]*templates.API, n)
	for i := range items {
		name := names[rng.Intn(alphabet)]
		switch rng.Intn(5) {
		case 0:
			items[i] = s1152(name)
		case 1:
			items[i] = s1152(name, "")
		case 2:
			items[i] = s1152(name, "ghost")
			if rng.Intn(4) > 0 {
				items[i] = s1152(name, names[rng.Intn(alphabet)])
			}
		default:
			items[i] = s1152(name, names[rng.Intn(len(names))])
		}
	}
	return items
}

// The ruling's property, over 300,000 adversarial specs: every order is one
// main's sort could produce, whatever its map yielded. Main's sort starts by
// emitting the seed its map yields, so the only seed that can produce an order
// is the order's own first len(roots) names; replaying main's drain from that
// seed must return the same order. A spec main refuses is refused here too.
func TestStageOrder1152_EveryOrderIsOneMainCouldProduce(t *testing.T) {
	rng := rand.New(rand.NewSource(1152))
	solved, refused := 0, 0
	for i := 0; i < 300000; i++ {
		items := spec1152(rng)
		got, err := topologicalSort(items)
		_, wantErr := mainSort1152(items)
		if (err == nil) != (wantErr == nil) {
			t.Fatalf("spec %d %v: error %v, main's %v", i, specString1152(items), err, wantErr)
		}
		if err != nil {
			if got != nil {
				t.Fatalf("spec %d: an order with an error: %v", i, got)
			}
			refused++
			continue
		}
		solved++
		seed := got
		if n := countRoots1152(items); n <= len(got) {
			seed = got[:n]
		}
		want, ok, mainErr := mainFrom1152(items, seed)
		if !ok || mainErr != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("spec %d %v: order %v is not one main could produce (from seed %v: %v, %v, %v)", i, specString1152(items), got, seed, want, mainErr, ok)
		}
	}
	if solved < 10000 || refused < 10000 {
		t.Fatalf("the specs must cover both sides: %d solved, %d refused", solved, refused)
	}
}

func countRoots1152(items []*templates.API) int {
	blocked, all := map[string]bool{}, map[string]bool{}
	for _, it := range items {
		all[it.Name] = true
		if it.DependsOn != nil && it.DependsOn.Name != "" {
			blocked[it.Name] = true
		}
	}
	return len(all) - len(blocked)
}

func specString1152(items []*templates.API) string {
	var b strings.Builder
	for _, it := range items {
		dep := ""
		if it.DependsOn != nil {
			dep = "<" + it.DependsOn.Name
		}
		b.WriteString(it.Name + dep + " ")
	}
	return b.String()
}

// The same property against main's sort itself, map and all: over repeated runs
// main returns this order. Up to 8 names a Go map rotates its insertion order,
// and declared order is one rotation, so 500 runs find it. Past 8 the map's order
// is not a rotation, so the spec is skipped here and left to the replay above.
func TestStageOrder1152_MainsOwnSortReturnsThisOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(11520))
	checked := 0
	for checked < 300 {
		items := spec1152(rng)
		got, err := topologicalSort(items)
		if err != nil || len(got) > 8 {
			continue
		}
		checked++
		found := false
		for run := 0; run < 500 && !found; run++ {
			m, err := mainSort1152(items)
			found = err == nil && reflect.DeepEqual(m, got)
		}
		if !found {
			t.Fatalf("%v: main never returned %v", specString1152(items), got)
		}
	}
}

// What the sort refuses is unchanged: a cycle, and a dependency on a stage the
// spec does not declare.
func TestStageOrder1152_AnUnsatisfiableSpecIsStillAnError(t *testing.T) {
	cases := map[string][]*templates.API{
		"self":          {s1152("a", "a")},
		"two stages":    {s1152("a", "b"), s1152("b", "a")},
		"behind a root": {s1152("r"), s1152("a", "b"), s1152("b", "a")},
		"unknown stage": {s1152("a", "ghost")},
	}
	for name, items := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := topologicalSort(items)
			if err == nil || !strings.Contains(err.Error(), "cyclic dependency") || got != nil {
				t.Fatalf("got %v, %v", got, err)
			}
		})
	}
}

// Independent stages run the same way on every run. A loop over a Go map cannot
// pass this: eight names in declared order, 2000 times, declared last to first.
func TestStageOrder1152_IndependentStagesAreDeterministicAcrossRuns(t *testing.T) {
	var items []*templates.API
	var want []string
	for i := 7; i >= 0; i-- {
		name := fmt.Sprintf("s%d", i)
		items = append(items, s1152(name))
		want = append(want, name)
	}
	for run := 0; run < 2000; run++ {
		got, err := topologicalSort(items)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("run %d: got %v, want %v", run, got, want)
		}
	}
}

// Through a resolve. What the map decided before is now the same on every
// run: which stage the resolve reports as its failure, the order of the outcome
// header, and the order of the records under a shared errorKey.
func TestStageOrder1152_AResolveReportsTheSameFailureOnEveryRun(t *testing.T) {
	const n = 6
	routes := map[string]u904Route{}
	var stages []*templates.API
	var want []string
	for i := n - 1; i >= 0; i-- {
		name := fmt.Sprintf("s%d", i)
		path := "/" + name
		routes["GET "+path] = u904Route{status: http.StatusConflict, body: `{"kind":"Status","message":"` + name + `"}`}
		s := u904Continue(u904Stage(name, http.MethodGet, path))
		s.ErrorKey = ptr.To("shared")
		stages = append(stages, s)
		want = append(want, name)
	}
	var first string
	for run := 0; run < 40; run++ {
		u := u904NewUpstream(t, routes)
		r := u904Resolve(t, u, stages, false)
		var got []string
		for _, name := range strings.Split(r.raw, `"name":"`)[1:] {
			got = append(got, name[:strings.Index(name, `"`)])
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("run %d: outcomes %v, want %v", run, got, want)
		}
		records := fmt.Sprint(r.dict["shared"])
		if run == 0 {
			first = records
			continue
		}
		if records != first {
			t.Fatalf("run %d: the shared errorKey changed order\n got %s\n was %s", run, records, first)
		}
	}
}

// The first failure a resolve reports is the first declared stage's.
func TestStageOrder1152_TheResolveFailureIsTheFirstDeclaredStages(t *testing.T) {
	u := u904NewUpstream(t, map[string]u904Route{
		"GET /b": {status: http.StatusConflict, body: `{"kind":"Status"}`},
		"GET /a": {status: http.StatusTeapot, body: `{"kind":"Status"}`},
	})
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "cyberjoker", Groups: []string{"devs"}}))
	for run := 0; run < 40; run++ {
		_, failure := ResolveWithFailure(ctx, ResolveOptions{
			RC: &rest.Config{Host: u.srv.URL}, AuthnNS: u904AuthnNS,
			Items:               []*templates.API{u904Stage("b", http.MethodGet, "/b"), u904Stage("a", http.MethodGet, "/a")},
			RESTActionNamespace: "ns-1152", RESTActionName: "ra-1152",
		})
		if failure == nil || failure.Stage != "b" {
			t.Fatalf("run %d: StageFailure = %+v, want stage b", run, failure)
		}
	}
}

// The other caller of the sort: inspect reports its unresolved stages in the
// order it walks them.
func TestStageOrder1152_InspectWalksTheDeclaredOrder(t *testing.T) {
	withInspectSARESTConfig(t, fakeDiscoveryServer(t))
	var stages []*templates.API
	var want []string
	for i := 7; i >= 0; i-- {
		name := fmt.Sprintf("s%d", i)
		stages = append(stages, &templates.API{Name: name, Path: `${ "/apis/apps/v1/namespaces/x/" + (.upstream.items[0].name | error) }`})
		want = append(want, name)
	}
	ra := &templates.RESTAction{Spec: templates.RESTActionSpec{API: stages}}
	for run := 0; run < 40; run++ {
		_, unresolved, err := InspectReadSet(context.Background(), ra, nil)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, u := range unresolved {
			got = append(got, u.Stage)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("run %d: unresolved %v, want %v", run, got, want)
		}
	}
}

// The krateo-dashboard/events shape with its fan-out stalled past the page's
// budget: `crds`, then `comps` (dependsOn crds, an iterator), then `getEvents`
// (independent). Independent stages run first, so `getEvents` has answered before
// the stall begins. A strict declared order would run the fan-out first, and the
// stall would spend the budget getEvents needs.
func TestStageOrder1152_AStalledFanOutStillAnswersTheIndependentStage(t *testing.T) {
	release := make(chan struct{})
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/v1/namespaces/"+u904AuthnNS+"/secrets/"+u904Secret:
			b, _ := json.Marshal(corev1.Secret{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
				ObjectMeta: metav1.ObjectMeta{Name: u904Secret, Namespace: u904AuthnNS},
				Data:       map[string][]byte{"server-url": []byte(srv.URL)},
			})
			_, _ = w.Write(b)
		case r.URL.Path == "/crds":
			_, _ = w.Write([]byte(`{"items":[{"plural":"p1s"}]}`))
		case strings.HasPrefix(r.URL.Path, "/comps/"):
			<-release // the fan-out stalls: connected, never answers
		default:
			_, _ = w.Write([]byte(`{"events":["e1"]}`))
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	comps := u904Continue(u904Stage("comps", http.MethodGet, `${ "/comps/" + .plural }`))
	comps.DependsOn = &templates.Dependency{Name: "crds", Iterator: ptr.To(".crds.items")}
	stages := []*templates.API{
		u904Stage("crds", http.MethodGet, "/crds"),
		comps,
		u904Continue(u904Stage("getEvents", http.MethodGet, "/events")),
	}

	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "cyberjoker", Groups: []string{"devs"}}))
	ctx = pagebudget.WithCustomer(ctx, time.Now().Add(2*time.Second-pagebudget.PageMaxGoal))
	ctx, outcomes := WithStageOutcomes(ctx)
	dict, _ := ResolveWithFailure(ctx, ResolveOptions{
		RC: &rest.Config{Host: srv.URL}, AuthnNS: u904AuthnNS, Items: stages,
		RESTActionNamespace: "krateo-dashboard", RESTActionName: "events",
	})
	raw := outcomes.HeaderValue()
	if strings.Contains(raw, `"name":"getEvents"`) {
		t.Fatalf("getEvents did not answer: %s", raw)
	}
	if !strings.Contains(fmt.Sprint(dict["getEvents"]), "e1") {
		t.Fatalf("getEvents = %v, want its events; outcomes %s", dict["getEvents"], raw)
	}
	if !strings.Contains(raw, `"name":"comps","ok":false,"reason":"Timeout"`) {
		t.Fatalf("outcomes %s: want the stalled fan-out to end on the budget", raw)
	}
}

// A dependent that reads an independent stage declared after it still sees it,
// though it declares no dependency on that stage: independent stages run first.
// Main's sort gave this on every run, whatever its map yielded.
func TestStageOrder1152_ADependentStillSeesALaterDeclaredIndependentStage(t *testing.T) {
	stages := []*templates.API{
		u904Stage("a", http.MethodGet, "/a"),
		u904After(u904Stage("b", http.MethodGet, `${ "/b/" + .c.id }`), "a"),
		u904Stage("c", http.MethodGet, "/c"),
	}
	for run := 0; run < 40; run++ {
		u := u904NewUpstream(t, map[string]u904Route{"GET /c": {status: http.StatusOK, body: `{"id":"from-c"}`}})
		r := u904Resolve(t, u, stages, false)
		if n := u.n(http.MethodGet, "/b/from-c"); n != 1 {
			t.Fatalf("run %d: b dialled /b/from-c %d time(s), want once; outcomes %s", run, n, r.raw)
		}
		r.wantSucceeded(t, "a", "b", "c")
	}
}

func BenchmarkStageOrder1152_TwentyStages(b *testing.B) {
	var items []*templates.API
	for i := 0; i < 20; i++ {
		if i%3 == 2 {
			items = append(items, s1152(fmt.Sprintf("s%d", i), fmt.Sprintf("s%d", i-1)))
			continue
		}
		items = append(items, s1152(fmt.Sprintf("s%d", i)))
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := topologicalSort(items); err != nil {
			b.Fatal(err)
		}
	}
}
