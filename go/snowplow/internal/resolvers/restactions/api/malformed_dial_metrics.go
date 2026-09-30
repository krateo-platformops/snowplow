// malformed_dial_metrics.go — observability for the malformed-dial guard
// (#288 empty-interpolation + #293 garbage-dial + #302 swallowed-jq
// payload/header). pm gate: every skip is COUNTED and published to /debug/vars,
// PER REASON, so a spike is attributable from expvar alone without tailing logs
// (feedback_measurement_use_expvar_not_log_tails +
// feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector — a MIXED
// single counter is the fallthrough_total mistake). The per-event DEBUG line
// (recordMalformedDialSkip) also names the reason for coordinate-shape triage.
//
// NOTE (#311-class, tracked separately): this family is /debug/vars-only — there
// is no expvar→OTLP bridge, so the reasons below are visible at /debug/vars, not
// ClickStack. The #302 additions inherit that; wiring the family to OTLP is out
// of #302 scope.

package api

import (
	"expvar"
	"sync"
	"sync/atomic"
)

// #293/#302 reason keys — the canonical, stable metric label values (also the
// DEBUG "reason" field). Distinct owners/urgencies, so per-reason attribution
// matters:
//   - empty_interp        (#288) empty / DNS-1123-invalid segment — a key-minting
//     / interpolation regression;
//   - unrendered_template (#293) a literal ${...} survived render — an
//     RA-authoring / unresolved-template fault;
//   - jq_path_error       (#293) the path jq expression errored — a jq fault;
//   - jq_payload_error    (#302) the payload jq expression errored — a jq fault
//     that must never be dialed as a garbage request body;
//   - jq_header_error     (#302) a header jq expression errored — a jq fault that
//     must never be dialed as a garbage header value (e.g. Authorization).
const (
	reasonEmptyInterp        = "empty_interp"
	reasonUnrenderedTemplate = "unrendered_template"
	reasonJQPathError        = "jq_path_error"
	reasonJQPayloadError     = "jq_payload_error"
	reasonJQHeaderError      = "jq_header_error"
)

// skipReason is the two-tier skip descriptor: class is one of the bounded metric
// keys above (drives the per-reason counter — what a spike alert needs, no
// per-path cardinality); detail is the finer human sub-reason for the DEBUG line
// (coordinate-shape triage). pm-1217: bounded class on the metric, full
// sub-reason in the log.
type skipReason struct {
	class  string
	detail string
}

// malformedDialSkipped counts api-call plan entries dropped by the guard, broken
// out PER REASON. Package-level atomics — safe to Add/Load without external
// locking.
var malformedDialSkipped struct {
	emptyInterp        atomic.Uint64
	unrenderedTemplate atomic.Uint64
	jqPathError        atomic.Uint64
	jqPayloadError     atomic.Uint64
	jqHeaderError      atomic.Uint64
}

// bumpMalformedDialSkipped increments the per-reason skip counter. Called from
// the SINGLE shared skip path (recordMalformedDialSkip) so both plan-append
// branches — the non-iterator single call and the iterator ForEach action —
// count identically and cannot drift. An unknown reason falls to empty_interp
// (the #288 class: the collapse / DNS-invalid / mutating-verb sub-cases all map
// here — pm groups them as the one keying-regression bucket).
func bumpMalformedDialSkipped(reason string) {
	switch reason {
	case reasonUnrenderedTemplate:
		malformedDialSkipped.unrenderedTemplate.Add(1)
	case reasonJQPathError:
		malformedDialSkipped.jqPathError.Add(1)
	case reasonJQPayloadError:
		malformedDialSkipped.jqPayloadError.Add(1)
	case reasonJQHeaderError:
		malformedDialSkipped.jqHeaderError.Add(1)
	default:
		malformedDialSkipped.emptyInterp.Add(1)
	}
}

// MalformedDialSkippedByReason returns the process-wide skip count for one
// reason. Exported so the falsifier can assert the METRIC discriminates (the
// pm-1217 axis: reason attribution reaches the metric, not just the DEBUG).
func MalformedDialSkippedByReason(reason string) uint64 {
	switch reason {
	case reasonUnrenderedTemplate:
		return malformedDialSkipped.unrenderedTemplate.Load()
	case reasonJQPathError:
		return malformedDialSkipped.jqPathError.Load()
	case reasonJQPayloadError:
		return malformedDialSkipped.jqPayloadError.Load()
	case reasonJQHeaderError:
		return malformedDialSkipped.jqHeaderError.Load()
	default:
		return malformedDialSkipped.emptyInterp.Load()
	}
}

// MalformedDialSkippedTotal returns the process-wide total across all reasons.
// Exported so the falsifier can assert the aggregate skip count and so an
// operator can watch the overall guard-fire rate.
func MalformedDialSkippedTotal() uint64 {
	return malformedDialSkipped.emptyInterp.Load() +
		malformedDialSkipped.unrenderedTemplate.Load() +
		malformedDialSkipped.jqPathError.Load() +
		malformedDialSkipped.jqPayloadError.Load() +
		malformedDialSkipped.jqHeaderError.Load()
}

var malformedDialExpvarOnce sync.Once

func init() { registerMalformedDialExpvar() }

// registerMalformedDialExpvar publishes snowplow_malformed_dial_skipped_total to
// /debug/vars as a PER-REASON object {empty_interp, unrendered_template,
// jq_path_error, jq_payload_error, jq_header_error} (pm-1217: metric-level reason
// attribution). Deliberately the SAME single key via the SAME single
// expvar.Publish — the published KEY NAME is unchanged (only the inner reason set
// grows), so it stays the lone ungated publisher named in the CFG-1
// nonCacheInitPublishers exception list (cfg1_structural_test.go checks key
// NAMES, value shape is free). NOT gated on cache mode — the guard runs on every
// RA resolve (cache on or off), so the counter must always be observable.
// Idempotent (sync.Once).
func registerMalformedDialExpvar() {
	malformedDialExpvarOnce.Do(func() {
		expvar.Publish("snowplow_malformed_dial_skipped_total", expvar.Func(func() any {
			return map[string]uint64{
				reasonEmptyInterp:        malformedDialSkipped.emptyInterp.Load(),
				reasonUnrenderedTemplate: malformedDialSkipped.unrenderedTemplate.Load(),
				reasonJQPathError:        malformedDialSkipped.jqPathError.Load(),
				reasonJQPayloadError:     malformedDialSkipped.jqPayloadError.Load(),
				reasonJQHeaderError:      malformedDialSkipped.jqHeaderError.Load(),
			}
		}))
	})
}
