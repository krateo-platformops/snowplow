// nested_resolve_bound_metrics.go — #720: the early warning for the risk the
// owner accepted when customer nested trees stopped waiting on the gate.
//
// Customer outermost trees bypass the nested-resolve admission gate ("No one
// waits in queue", Diego 2026-10-15) but still count toward its in-flight
// weight. That re-exposes the 1.5.27 OOMKill shape for a burst of cold
// composite pages. This gauge says how far customer trees have pushed the
// in-flight weight past the ceiling the gate admits background by:
//
//   - snowplow_nested_resolve_admission_over_ceiling_bytes       gauge, now;
//   - snowplow_nested_resolve_admission_over_ceiling_bytes_peak  high-water
//     mark since boot, so a /debug/vars poll or an OTLP interval cannot miss a
//     burst shorter than its period.
//
// Both are 0 while customer trees fit. The value is refreshed at every
// admission and release (nested_resolve_bound.go observeOverCeilingLocked),
// against a fresh ceiling sample. It is the twin of #710's
// snowplow_apistage_content_fill_admission_over_ceiling_bytes. Bench run 8
// (#606) reports its peak under a cold composite-page burst.
//
// OTLP: hand-wired in internal/metrics/metrics.go (no expvar bridge), reading
// NestedResolveAdmissionStatsSnapshot, the accessor the expvar closures read.
// No attributes: nothing here names a user, a resource or a cache body.
//
// CFG-1: gated on cache.Disabled(), like every cache-side publisher.
package api

import (
	"expvar"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

var (
	nestedOverCeilingBytes     atomic.Int64
	nestedOverCeilingBytesPeak atomic.Int64
)

// recordNestedOverCeiling stores the current over-ceiling bytes and raises the
// peak. Called with the gate's b.mu held, so stores are ordered; the CAS loop
// keeps the peak monotonic regardless.
func recordNestedOverCeiling(over int64) {
	if over < 0 {
		over = 0
	}
	nestedOverCeilingBytes.Store(over)
	for {
		peak := nestedOverCeilingBytesPeak.Load()
		if over <= peak || nestedOverCeilingBytesPeak.CompareAndSwap(peak, over) {
			return
		}
	}
}

// NestedResolveAdmissionStats is the #720 surface of the nested-resolve gate.
type NestedResolveAdmissionStats struct {
	OverCeilingBytes     int64
	OverCeilingBytesPeak int64
}

// NestedResolveAdmissionStatsSnapshot reads the live gauges. Both the expvar
// closures and the OTLP callback read it, so the surfaces cannot diverge.
func NestedResolveAdmissionStatsSnapshot() NestedResolveAdmissionStats {
	return NestedResolveAdmissionStats{
		OverCeilingBytes:     nestedOverCeilingBytes.Load(),
		OverCeilingBytesPeak: nestedOverCeilingBytesPeak.Load(),
	}
}

// resetNestedOverCeilingForTest zeroes the gauges. Test-only.
func resetNestedOverCeilingForTest() {
	nestedOverCeilingBytes.Store(0)
	nestedOverCeilingBytesPeak.Store(0)
}

// RecordNestedOverCeilingForTest moves the gauges through the production
// recorder so a cross-package arm can prove the OTLP mirror reads the LIVE
// values. Production MUST NOT use it.
func RecordNestedOverCeilingForTest(over int64) { recordNestedOverCeiling(over) }

// ResetNestedOverCeilingForTest is the exported reset. Production MUST NOT use it.
func ResetNestedOverCeilingForTest() { resetNestedOverCeilingForTest() }

var nestedAdmissionExpvarOnce sync.Once

func init() {
	if cache.Disabled() {
		return
	}
	registerNestedAdmissionExpvar()
}

func registerNestedAdmissionExpvar() {
	nestedAdmissionExpvarOnce.Do(func() {
		expvar.Publish("snowplow_nested_resolve_admission_over_ceiling_bytes", expvar.Func(func() any {
			return NestedResolveAdmissionStatsSnapshot().OverCeilingBytes
		}))
		expvar.Publish("snowplow_nested_resolve_admission_over_ceiling_bytes_peak", expvar.Func(func() any {
			return NestedResolveAdmissionStatsSnapshot().OverCeilingBytesPeak
		}))
	})
}

// RegisterNestedAdmissionExpvarForTest forces registration under tests that
// flip CACHE_ENABLED after init() ran. Idempotent. Production MUST NOT use it.
func RegisterNestedAdmissionExpvarForTest() { registerNestedAdmissionExpvar() }
