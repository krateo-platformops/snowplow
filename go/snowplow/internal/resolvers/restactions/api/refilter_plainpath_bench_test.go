// refilter_plainpath_bench_test.go — refilterSlice at the production list
// size (50K items), for the plain-field-path fast path of the per-item
// userAccessFilter expressions.
//
// Run:
//
//	go test ./internal/resolvers/restactions/api/ -run '^$' \
//	  -bench BenchmarkRefilterSlice50K -benchmem -count 6
//
// The fixture is the memo arms' RBAC store (newMemoTestWatcher): the user
// holds list on widgets in team-a and team-b, get on widgets named "alpha"
// in ns-named, and nothing elsewhere. Items spread over 50 namespaces, so the
// per-slice RBAC memo answers almost every item and what remains per item is
// the expression evaluation and the loop itself.

package api

import (
	"io"
	"log/slog"
	"strconv"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

const bench50KItems = 50_000

// benchCompositionItem is a composition-shaped list item: the fields the
// UAF expressions read, plus enough of the rest of an object (labels,
// annotations, spec, status) that a lookup walks a realistic map.
func benchCompositionItem(ns, name string, i int) map[string]any {
	return map[string]any{
		"apiVersion": "composition.krateo.io/v1-2-0",
		"kind":       "Widget",
		"metadata": map[string]any{
			"namespace":         ns,
			"name":              name,
			"uid":               "00000000-0000-0000-0000-" + strconv.Itoa(100000000000+i),
			"resourceVersion":   strconv.Itoa(1000 + i),
			"generation":        float64(1),
			"creationTimestamp": "2026-10-01T00:00:00Z",
			"labels": map[string]any{
				"krateo.io/composition-version": "v1-2-0",
				"app":                           name,
			},
			"annotations": map[string]any{
				"krateo.io/release-name": name,
			},
		},
		"spec": map[string]any{
			"replicas": float64(1),
			"image":    "example/app:1.0.0",
		},
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": "Ready", "status": "True"},
			},
		},
	}
}

// bench50KCompositions builds n items over 50 namespaces: team-a and team-b
// (granted), ns-named (get-on-alpha only) and 47 namespaces with no grant.
func bench50KCompositions(n int) []any {
	namespaces := []string{"team-a", "team-b", "ns-named"}
	for i := 0; len(namespaces) < 50; i++ {
		namespaces = append(namespaces, "bench-ns-"+strconv.Itoa(i))
	}
	items := make([]any, n)
	for i := range items {
		ns := namespaces[i%len(namespaces)]
		name := "comp-" + strconv.Itoa(i)
		if ns == "ns-named" && i%2 == 0 {
			name = "alpha"
		}
		items[i] = benchCompositionItem(ns, name, i)
	}
	return items
}

func BenchmarkRefilterSlice50K(b *testing.B) {
	newMemoTestWatcher(b)
	// The request logger at the shipped LOG_LEVEL=warn: the evaluator's
	// per-call DEBUG line must not be what the benchmark measures.
	ctx := xcontext.BuildContext(ctxWithUser(narrowUser, "devs"),
		xcontext.WithLogger(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))))
	groups := []string{"devs"}
	items := bench50KCompositions(bench50KItems)

	cases := []struct {
		name  string
		uaf   *templates.UserAccessFilterSpec
		items []any
	}{
		// The dominant portal shape: list verb, namespaceFrom
		// .metadata.namespace (explicit, as the charts write it).
		{"list_ns", &templates.UserAccessFilterSpec{Verb: "list", Group: memoGroup, Resource: "widgets", NamespaceFrom: ".metadata.namespace"}, items},
		// A name-specific verb also evaluates nameFrom (default .metadata.name).
		{"get_ns_name", &templates.UserAccessFilterSpec{Verb: "get", Group: memoGroup, Resource: "widgets"}, items},
	}
	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			var kept int
			for b.Loop() {
				k, _, _ := refilterSlice(ctx, discardLogger(), narrowUser, groups, c.uaf, []string{"widgets"}, c.items)
				kept = len(k)
			}
			b.ReportMetric(float64(kept), "kept")
		})
	}
}
