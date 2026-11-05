//go:build unit
// +build unit

package api

// sort_1152_test.go — #1152: stages run in the order the author declared,
// wherever their dependencies allow. topologicalSort read the stages with no
// dependency out of a Go map, so independent stages ran in a different order
// from one resolve to the next.

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"reflect"
	"strings"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/ptr"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
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

func TestStageOrder1152_DeclaredOrderIsKept(t *testing.T) {
	cases := []struct {
		name  string
		items []*templates.API
		want  []string
	}{
		{"independent stages, not alphabetical", []*templates.API{s1152("z"), s1152("a"), s1152("m")}, []string{"z", "a", "m"}},
		{"the #1152 pair", []*templates.API{s1152("fast"), s1152("slow")}, []string{"fast", "slow"}},
		{"a dependency already declared first", []*templates.API{s1152("a"), s1152("b", "a"), s1152("c")}, []string{"a", "b", "c"}},
		{"a dependent keeps its place among the ready stages", []*templates.API{s1152("a"), s1152("b"), s1152("c", "a"), s1152("d")}, []string{"a", "b", "c", "d"}},
		{"only the dependent moves", []*templates.API{s1152("a", "c"), s1152("b"), s1152("c")}, []string{"b", "c", "a"}},
		{"a chain declared backwards", []*templates.API{s1152("a", "b"), s1152("b", "c"), s1152("c")}, []string{"c", "b", "a"}},
		{"two dependents of one stage", []*templates.API{s1152("x", "r"), s1152("y", "r"), s1152("r")}, []string{"r", "x", "y"}},
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

// indexOrders calls visit with every order of 0..n-1 that runs each stage after
// its dependency, smallest first when read as a sequence of declared positions.
// It knows nothing about how topologicalSort works: it is the independent answer.
func indexOrders(dep []int, visit func(order []int) bool) {
	n := len(dep)
	used := make([]bool, n)
	order := make([]int, 0, n)
	var walk func() bool
	walk = func() bool {
		if len(order) == n {
			return visit(order)
		}
		for i := 0; i < n; i++ {
			if used[i] || (dep[i] >= 0 && !used[dep[i]]) {
				continue
			}
			used[i] = true
			order = append(order, i)
			stop := walk()
			order = order[:len(order)-1]
			used[i] = false
			if stop {
				return true
			}
		}
		return false
	}
	walk()
}

// Dependencies win, and within what they allow the order is as close to the
// declared one as it can be: over random specs, the result is the first valid
// order an exhaustive walk finds. A spec's dependency can point at any stage
// that is not its own descendant, declared before or after it.
func TestStageOrder1152_DependenciesWinThenDeclaredOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(1152))
	specs := 0
	for specs < 2000 {
		n := 1 + rng.Intn(7)
		dep := make([]int, n)
		// A random rank makes the graph acyclic whichever way its edges point
		// in declared order.
		rank := rng.Perm(n)
		for i := range dep {
			dep[i] = -1
			if rng.Intn(3) > 0 {
				var below []int
				for j := range dep {
					if rank[j] < rank[i] {
						below = append(below, j)
					}
				}
				if len(below) > 0 {
					dep[i] = below[rng.Intn(len(below))]
				}
			}
		}
		items := make([]*templates.API, n)
		for i := range items {
			if dep[i] >= 0 {
				items[i] = s1152(fmt.Sprintf("s%d", i), fmt.Sprintf("s%d", dep[i]))
			} else {
				items[i] = s1152(fmt.Sprintf("s%d", i))
			}
		}
		var want []string
		indexOrders(dep, func(order []int) bool {
			for _, i := range order {
				want = append(want, fmt.Sprintf("s%d", i))
			}
			return true
		})
		got, err := topologicalSort(items)
		if err != nil {
			t.Fatalf("deps %v: %v", dep, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("deps %v: got %v, want %v", dep, got, want)
		}
		specs++
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
// pass this: eight names in declared order, 2000 times.
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

// Through a resolve. What the order decided before is now the same on every
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
