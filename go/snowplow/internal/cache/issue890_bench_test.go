//go:build unit || integration

// issue890_bench_test.go — #892 re-review B1 evidence (a benchmark, not a
// pass/fail arm). The cost an accepted PutIfGen pays for the #890 re-assert, and
// what that does to an unrelated customer read, for a key whose resolve recorded
// `deps` exact deps + one cluster LIST dep while `others` other keys share that
// LIST bucket. Two modes:
//
//	skip  — production: no strip touched the key since its resolve recorded, so
//	        the re-assert is one identity check of the key's edge set.
//	exact — reassertAlwaysCheckForBench: every own edge is checked with the O(1)
//	        exact lookup, the cost after a strip.
//
// Run: go test -tags unit -run '^$' -bench Issue890 -benchmem ./internal/cache/

package cache

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func setupBench890(b testing.TB, others, deps int) (*ResolvedCacheStore, context.Context, string) {
	ResetDepsForTest()
	d := Deps()
	store := newResolvedCache(1_000_000, 1<<34, time.Hour)
	d.SetStore(store)
	gvr := schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "compositions"}
	bg := context.Background()
	for i := 0; i < others; i++ {
		k := fmt.Sprintf("L1_other_%d", i)
		d.RecordList(bg, k, gvr, "")
		store.Put(k, &ResolvedEntry{RawJSON: []byte(`{}`)})
	}
	const key = "L1_bench_K"
	ctx := WithL1KeyContext(bg, key)
	d.RecordList(ctx, key, gvr, "")
	for j := 0; j < deps; j++ {
		d.Record(ctx, key, gvr, fmt.Sprintf("ns-%d", j%20), fmt.Sprintf("comp-%d", j))
	}
	return store, ctx, key
}

var bench890Modes = []struct {
	name   string
	always bool
}{{"skip", false}, {"exact", true}}

// BenchmarkIssue890_AcceptedPut — two shapes:
//
//	fill    — the resolve's own records created the key's edges (one ctx, Put
//	          repeatedly): the cold fill.
//	refresh — every iteration is a NEW resolve of the resident key whose records
//	          are all dedups of existing edges, then its Put: the refresher /
//	          seed re-Put / concurrent fill, the most common accepted write
//	          (#892 FU-1). The records are inside the timed loop on every build.
func BenchmarkIssue890_AcceptedPut(b *testing.B) {
	gvr := schema.GroupVersionResource{Group: "composition.krateo.io", Version: "v1", Resource: "compositions"}
	for _, shape := range []string{"fill", "refresh"} {
		for _, mode := range bench890Modes {
			for _, others := range []int{0, 1000, 10000} {
				for _, deps := range []int{10, 100} {
					b.Run(fmt.Sprintf("%s/%s/others=%d/deps=%d", shape, mode.name, others, deps), func(b *testing.B) {
						reassertAlwaysCheckForBench.Store(mode.always)
						defer reassertAlwaysCheckForBench.Store(false)
						store, ctx, key := setupBench890(b, others, deps)
						defer ResetDepsForTest()
						d := Deps()
						nss, names := make([]string, deps), make([]string, deps)
						for j := range names {
							nss[j], names[j] = fmt.Sprintf("ns-%d", j%20), fmt.Sprintf("comp-%d", j)
						}
						e := &ResolvedEntry{RawJSON: []byte(`{"v":1}`)}
						if !store.PutIfGen(ctx, key, e, store.CaptureGen(key)) {
							b.Fatal("refused")
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							c := ctx
							if shape == "refresh" {
								c = WithL1KeyContext(context.Background(), key)
								d.RecordList(c, key, gvr, "")
								for j := range names {
									d.Record(c, key, gvr, nss[j], names[j])
								}
							}
							if !store.PutIfGen(c, key, e, store.CaptureGen(key)) {
								b.Fatal("refused")
							}
						}
					})
				}
			}
		}
	}
}

// BenchmarkIssue890_UnrelatedReadUnderPuts — an unrelated customer read
// (GetNoTouch) timed while one goroutine performs accepted Puts of the big key
// back to back, for 300ms per case. Reports the read's p99 and max.
func BenchmarkIssue890_UnrelatedReadUnderPuts(b *testing.B) {
	for _, mode := range bench890Modes {
		for _, others := range []int{0, 1000, 10000} {
			for _, deps := range []int{10, 100} {
				b.Run(fmt.Sprintf("%s/others=%d/deps=%d", mode.name, others, deps), func(b *testing.B) {
					reassertAlwaysCheckForBench.Store(mode.always)
					defer reassertAlwaysCheckForBench.Store(false)
					store, ctx, key := setupBench890(b, others, deps)
					defer ResetDepsForTest()
					store.Put("L1_customer", &ResolvedEntry{RawJSON: []byte(`{}`)})
					var lat []time.Duration
					for i := 0; i < b.N; i++ {
						var stop atomic.Bool
						var wg sync.WaitGroup
						wg.Add(1)
						go func() {
							defer wg.Done()
							e := &ResolvedEntry{RawJSON: []byte(`{"v":1}`)}
							for !stop.Load() {
								store.PutIfGen(ctx, key, e, store.CaptureGen(key))
							}
						}()
						deadline := time.Now().Add(300 * time.Millisecond)
						for time.Now().Before(deadline) {
							t0 := time.Now()
							store.GetNoTouch("L1_customer")
							lat = append(lat, time.Since(t0))
						}
						stop.Store(true)
						wg.Wait()
					}
					sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
					b.ReportMetric(float64(lat[int(float64(len(lat)-1)*0.99)].Nanoseconds()), "read-p99-ns")
					b.ReportMetric(float64(lat[len(lat)-1].Nanoseconds()), "read-max-ns")
				})
			}
		}
	}
}
