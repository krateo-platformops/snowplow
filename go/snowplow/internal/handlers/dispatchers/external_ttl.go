// external_ttl.go — external-widget bounded-TTL cache (Option A, 2026-07-10).
//
// The external-touched Put-gate (external_touched_sink.go) DECLINES the L1 Put
// for any widget whose resolve reached a genuine external endpoint, because
// external data has no informer/dep edge to invalidate it — so every /call
// re-fetches live (the obs-widget ClickHouse query storm). This helper reads a
// per-widget OPT-IN annotation that inverts that decline into a bounded-
// staleness Put: the external result is Put under the UNCHANGED per-binding
// `widgets` key with a short TTLOverride, served from L1 for at most the TTL,
// then TTL-evicted and re-fetched. Correctness basis shifts from dep-edge
// invalidation to time-bounded staleness (design §2).
//
// ONLY A THIRD-PARTY TOUCH QUALIFIES (#1221): the decline is inverted only when
// every touch carried nothing of the requester (PutVerdict.WithExternalTTL). The
// annotation is a request, not a guarantee: a widget whose resolve dialled as the
// requester stays declined whatever it says.
//
// GENERAL, NOT a widget special-case (feedback_no_special_cases): any widget
// owner opts in by annotating; snowplow hard-codes no `obs-*` name. The portal
// chart sets the annotation on the obs-* widget CR templates.
//
// DEFAULT OFF: absent / "0" / unparseable / <=0 annotation → returns 0 → the
// caller falls through to today's unconditional external decline, byte-
// identical (C4). Present and >0 → returns the capped TTL.
package dispatchers

import (
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// externalCacheTTLAnnotation is the general per-widget opt-in surface. Its
// value is an integer number of SECONDS. Absent → feature off for the widget.
const externalCacheTTLAnnotation = "krateo.io/external-cache-ttl-seconds"

// externalCacheTTLCap is the defensive hard cap on the parsed TTL (design §5).
// A fat-fingered "3600" must NOT pin external stale data for an hour; the cap
// clamps any larger value down to this bound. General clamp, not a
// special-case. effectiveTTLLocked further clamps to min(override, store.ttl),
// so the effective bound is always the smaller of this cap and the store TTL.
const externalCacheTTLCap = 120 * time.Second

// externalCacheTTLFromAnnotations parses the opt-in annotation off a fetched
// widget CR's unstructured object. It returns:
//
//	0                 — feature OFF: annotation absent, empty, "0", negative,
//	                    or unparseable. Caller declines the Put as today.
//	(0, cap]          — feature ON: the parsed seconds, capped at
//	                    externalCacheTTLCap. Caller Puts with TTLOverride=this
//	                    and ExternalTTL=true.
//
// It reads from obj.GetAnnotations() (already in hand at the dispatch site —
// no extra apiserver round-trip, same pattern as isRBACSensitiveApiRefWidget).
// nil-safe: a nil obj or nil annotations map → 0 (off).
func externalCacheTTLFromAnnotations(obj *unstructured.Unstructured) time.Duration {
	if obj == nil {
		return 0
	}
	ann := obj.GetAnnotations()
	if ann == nil {
		return 0
	}
	raw, ok := ann[externalCacheTTLAnnotation]
	if !ok || raw == "" {
		return 0
	}
	secs, err := strconv.Atoi(raw)
	if err != nil || secs <= 0 {
		// Unparseable or non-positive → OFF (default), never an error to the
		// caller: a malformed annotation must not break the serve, it just
		// leaves the feature off for this widget (byte-identical to today).
		return 0
	}
	ttl := time.Duration(secs) * time.Second
	if ttl > externalCacheTTLCap {
		ttl = externalCacheTTLCap
	}
	return ttl
}
