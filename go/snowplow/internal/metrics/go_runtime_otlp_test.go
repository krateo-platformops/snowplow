//go:build unit

package metrics

// go_runtime_otlp_test.go — the effective GOGC and soft memory limit reach
// OTLP through the REAL registration and callback, and agree with the
// runtime (and so with the snowplow_go_runtime expvar, which reads the same
// memlimit.Effective).

import (
	"runtime/debug"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/memlimit"
)

func TestGoRuntimeInstruments_ReportEffectiveGOGCAndLimit(t *testing.T) {
	origGC := debug.SetGCPercent(87)
	origLimit := debug.SetMemoryLimit(3 << 30)
	t.Cleanup(func() {
		debug.SetGCPercent(origGC)
		debug.SetMemoryLimit(origLimit)
	})

	all := flatten(collectViaRealCallback(t, "deadbeef"))

	gc := pointsFor(all, "snowplow_go_gc_percent")
	if len(gc) != 1 || gc[0].value != 87 {
		t.Fatalf("snowplow_go_gc_percent = %+v, want one point = 87", gc)
	}
	lim := pointsFor(all, "snowplow_go_memory_limit_bytes")
	if len(lim) != 1 || lim[0].value != 3<<30 {
		t.Fatalf("snowplow_go_memory_limit_bytes = %+v, want one point = %d", lim, int64(3<<30))
	}
	if got, want := lim[0].attrs["source"], string(memlimit.Current().Source); got != want {
		t.Errorf("source = %q, want %q (memlimit.Current)", got, want)
	}
}
