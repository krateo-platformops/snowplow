// malformed_dial_metrics.go — #288 observability for the empty-interpolation
// dial guard (pm-1218 gate C1).
//
// The guard (setup.go validInterpolatedPath) SKIPS a malformed single-object
// apiserver call instead of dialing it. A DEBUG-only skip would convert a loud
// 404 storm (~600/20min on 057) into an INVISIBLE skip storm: if an upstream
// interpolation / key-minting bug spikes empty or DNS-1123-invalid segments, a
// zero-visibility skip would read as health — the false-green trap
// (feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector +
// feedback_measurement_use_expvar_not_log_tails). So every skip is COUNTED and
// published to /debug/vars; the per-event DEBUG line stays for coordinate-shape
// triage.

package api

import (
	"expvar"
	"sync"
	"sync/atomic"
)

// malformedDialSkippedTotal counts api-call plan entries dropped by the #288
// guard (validInterpolatedPath==false): a rendered single-object apiserver path
// whose interpolated name/namespace segment was empty or DNS-1123-invalid.
// Package-level atomic — safe to Add/Load without external locking.
var malformedDialSkippedTotal atomic.Uint64

// bumpMalformedDialSkipped increments the guard's skip counter. Called from the
// SINGLE shared skip path (recordMalformedDialSkip) so both plan-append branches
// — the non-iterator single call and the iterator ForEach action — count
// identically and cannot drift.
func bumpMalformedDialSkipped() { malformedDialSkippedTotal.Add(1) }

// MalformedDialSkippedTotal returns the process-wide #288 skip count. Exported
// so the falsifier can assert the skip was COUNTED (C1: a counted skip, not just
// a DEBUG line), and so an operator can watch the guard-fire rate.
func MalformedDialSkippedTotal() uint64 { return malformedDialSkippedTotal.Load() }

var malformedDialExpvarOnce sync.Once

func init() { registerMalformedDialExpvar() }

// registerMalformedDialExpvar publishes snowplow_malformed_dial_skipped_total to
// /debug/vars. Idempotent (sync.Once). NOT gated on cache mode — the guard runs
// on every RA resolve (cache on or off), so the counter must always be
// observable. Mirrors the expvar.Publish idiom in cache/deps_expvar.go.
func registerMalformedDialExpvar() {
	malformedDialExpvarOnce.Do(func() {
		expvar.Publish("snowplow_malformed_dial_skipped_total", expvar.Func(func() any {
			return malformedDialSkippedTotal.Load()
		}))
	})
}
