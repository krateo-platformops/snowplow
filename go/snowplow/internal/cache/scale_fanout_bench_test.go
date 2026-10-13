// scale_fanout_bench_test.go — #603 PR-0: the wall cost of ONE object event's
// dirty-mark fan-out on the #603 harness population, where list-dep cells carry
// the fan-out. It exists so a change to the per-mark path (the #479 1b
// entry-class attribution) is measured on the path that carries 99.66% of
// production marks. A benchmark, never a gate: the gate is a count.

package cache

import (
	"io"
	"log/slog"
	"strconv"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

func BenchmarkScaleFanOut_OnObjectEvent(b *testing.B) {
	// The per-event INFO line would otherwise dominate the measurement.
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer slog.SetDefault(prev)
	for _, comps := range []int{25, 400} {
		b.Run("comps="+strconv.Itoa(comps), func(b *testing.B) {
			d, _ := scalePopulation(b, comps, 2, 10, 0.3)
			d.SetRefreshHook(func(string, schema.GroupVersionResource) {})
			gvr := gvrCompositions()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				d.OnObjectEvent(gvr, "ns", "comp-000000", objExists)
			}
		})
	}
}
