// status_tree_1079_bench_test.go — #1079 benchmarks at run 20's scale: 50,000
// compositions over 50 namespaces, through the two RESTActions that read them
// all (portal 1.8.85), one request shape each.
//
//	go test ./internal/resolvers/restactions/ -run '^$' -bench Issue1079 -benchmem -count=6 | tee out.txt
//	benchstat -col /path out.txt
//
// cpu-ns/op is the whole process's CPU time per op (getrusage: user+system,
// every thread, so the GC's background mark workers count too) — what a
// refresh costs the pod, and far steadier than ns/op on a loaded machine.
// n=50000 is the whole set (run 20's scale, an all-namespaces reader);
// n=1000 is one namespace's share of it (run 20: 1,000 per namespace).
//
// RefreshBody is the apiRef half of a widget refresh: the RESTAction filter,
// its Status bytes, and the tree handed to the widget (path=before decodes the
// bytes, path=after builds the tree from the filter's outputs). TreeOnly is
// the replaced step alone, from the same outputs and the same bytes.

package restactions

import (
	"encoding/json"
	"fmt"
	"syscall"
	"testing"
	"time"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

var sink1079 map[string]any

func processCPUNs1079() int64 {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return ru.Utime.Nano() + ru.Stime.Nano()
}

func BenchmarkIssue1079_RefreshBody(b *testing.B) {
	benchIssue1079(b, false)
}

func BenchmarkIssue1079_TreeOnly(b *testing.B) {
	benchIssue1079(b, true)
}

func benchIssue1079(b *testing.B, treeOnly bool) {
	c := loadCorpus1079(b)
	for _, n := range []int{1000, 50000} {
		benchIssue1079N(b, c, n, treeOnly)
	}
}

func benchIssue1079N(b *testing.B, c corpus1079, n int, treeOnly bool) {
	objs := compositionObjects1079(n, time.Now())
	for _, raName := range []string{"compositions-list", "dashboard-data"} {
		ra := c.Restactions[raName]
		dict := dict1079(stageOutputs1079(b, c, raName, objs), extras1079[raName][0])
		vals, text, err := evalFilter(b.Context(), ra.Filter, dict)
		if err != nil {
			b.Fatal(err)
		}
		raw := []byte(text)
		for _, path := range []string{"before", "after"} {
			b.Run(fmt.Sprintf("ra=%s/n=%d/path=%s", raName, n, path), func(b *testing.B) {
				b.SetBytes(int64(len(raw)))
				b.ReportAllocs()
				cpu0 := processCPUNs1079()
				defer func() { b.ReportMetric(float64(processCPUNs1079()-cpu0)/float64(b.N), "cpu-ns/op") }()
				for b.Loop() {
					if treeOnly {
						if path == "before" {
							var m map[string]any
							if err := json.Unmarshal(raw, &m); err != nil {
								b.Fatal(err)
							}
							sink1079 = m
						} else {
							m, ok := jqsupport.DecodedObjectOf(vals)
							if !ok {
								b.Fatal("not exact")
							}
							sink1079 = m
						}
						continue
					}
					if path == "before" {
						_, m, err := before1079(b, ra.Filter, dict)
						if err != nil {
							b.Fatal(err)
						}
						sink1079 = m
					} else {
						v, s, err := evalFilter(b.Context(), ra.Filter, dict)
						if err != nil {
							b.Fatal(err)
						}
						_ = []byte(s) // Status.Raw, as Resolve stores it
						m, ok := jqsupport.DecodedObjectOf(v)
						if !ok {
							b.Fatal("not exact")
						}
						sink1079 = m
					}
				}
			})
		}
	}
}
