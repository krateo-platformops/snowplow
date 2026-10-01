// malformed_dial_metrics.go — observability for the malformed-dial guard
// (#288 empty-interpolation + #293 garbage-dial + #302 swallowed-jq
// payload/header). pm gate: every skip is COUNTED and published to /debug/vars,
// PER REASON, so a spike is attributable from expvar alone without tailing logs
// (feedback_measurement_use_expvar_not_log_tails +
// feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector — a MIXED
// single counter is the fallthrough_total mistake). The per-event DEBUG line
// (recordMalformedDialSkip) also names the reason for coordinate-shape triage.
//
// NOTE (#311): this family reaches BOTH /debug/vars AND OTLP/ClickStack. There is
// no expvar→OTLP auto-bridge, so metrics.go hand-wires it: registerInstruments
// observes MalformedDialSkippedByReasonSnapshot() per bounded reason into one
// Int64ObservableCounter. The hand-wire reads the SAME atomics the expvar Func
// below reads, so the two surfaces share a single source of truth and the reason
// key set cannot diverge.

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

// MalformedDialSkippedByReasonSnapshot returns the per-reason skip counts keyed
// by the BOUNDED reason enum — the fixed, code-defined set below, NEVER derived
// from request data — so an OTLP `reason` attribute built from these keys has
// cardinality bounded by construction (#311 / the #260 bumps_by_source bounded-
// attribute pattern; metrics.go ranges this and observes one counter per key).
// Mirrors cache.RBACSubGenBumpsBySourceSnapshot. The key set is EXACTLY the
// expvar map's reasons, so /debug/vars and OTLP never diverge.
func MalformedDialSkippedByReasonSnapshot() map[string]uint64 {
	return map[string]uint64{
		reasonEmptyInterp:        malformedDialSkipped.emptyInterp.Load(),
		reasonUnrenderedTemplate: malformedDialSkipped.unrenderedTemplate.Load(),
		reasonJQPathError:        malformedDialSkipped.jqPathError.Load(),
		reasonJQPayloadError:     malformedDialSkipped.jqPayloadError.Load(),
		reasonJQHeaderError:      malformedDialSkipped.jqHeaderError.Load(),
	}
}

// MalformedDialReasonEnum returns the fixed reason set — the bound the OTLP
// attribute must stay within. Exported so the #311 falsifier can assert every
// emitted reason is in this enum (cardinality-bound guard).
func MalformedDialReasonEnum() []string {
	return []string{
		reasonEmptyInterp,
		reasonUnrenderedTemplate,
		reasonJQPathError,
		reasonJQPayloadError,
		reasonJQHeaderError,
	}
}

// RecordMalformedDialSkipForTest bumps the per-reason counter for the #311 OTLP
// falsifier. Production MUST NOT use it — the real bump is recordMalformedDialSkip.
func RecordMalformedDialSkipForTest(reason string) { bumpMalformedDialSkipped(reason) }

// ResetMalformedDialSkippedForTest zeroes all per-reason counters so a falsifier
// can assert an exact delta. Production MUST NOT use it.
func ResetMalformedDialSkippedForTest() {
	malformedDialSkipped.emptyInterp.Store(0)
	malformedDialSkipped.unrenderedTemplate.Store(0)
	malformedDialSkipped.jqPathError.Store(0)
	malformedDialSkipped.jqPayloadError.Store(0)
	malformedDialSkipped.jqHeaderError.Store(0)
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
