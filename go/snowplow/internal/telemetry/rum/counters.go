package rum

import (
	"expvar"
	"sync"
	"sync/atomic"
)

// Counters. Requests, spans and attributes are counted in their own unit so no
// number mixes them. Each counter has one reason; there is no per-user or
// per-route dimension (identity never leaves the request).
var (
	requestsAccepted      atomic.Uint64 // decoded and filtered (whether or not any span survived)
	requestsDroppedAuth   atomic.Uint64 // 401 from the auth middleware
	requestsDroppedSize   atomic.Uint64 // 413: body over MaxBodyBytes or more than MaxSpansPerRequest spans
	requestsDroppedRate   atomic.Uint64 // 429: the user's limit
	requestsDroppedType   atomic.Uint64 // 415: not application/json
	requestsDroppedFormat atomic.Uint64 // 400: not OTLP/JSON
	requestsDroppedBusy   atomic.Uint64 // 429: every decode slot (all users) in use

	spansAccepted           atomic.Uint64 // passed the allowlist
	spansDroppedService     atomic.Uint64
	spansDroppedName        atomic.Uint64
	spansDroppedInvalid     atomic.Uint64
	attributesDropped       atomic.Uint64
	spansForwarded          atomic.Uint64 // the collector answered 2xx
	spansForwardFailed      atomic.Uint64 // the collector failed or answered non-2xx
	spansForwardOverflow    atomic.Uint64 // the forward queue was full: dropped without blocking
	spansForwardRateLimited atomic.Uint64 // over the global spans/s cap: not forwarded, 429
)

// Snapshot is a point-in-time read of every counter, keyed by the outcome
// label the OTLP mirror publishes.
type Snapshot struct {
	Requests   map[string]uint64 // accepted, dropped_auth, dropped_size, dropped_rate, dropped_content_type, dropped_malformed, dropped_busy
	Spans      map[string]uint64 // accepted, dropped_service, dropped_name, dropped_invalid, forwarded, forward_failed, forward_overflow, forward_rate_limited
	Attributes uint64            // span attributes dropped by the allowlist
}

// Read returns the current counters.
func Read() Snapshot {
	return Snapshot{
		Requests: map[string]uint64{
			"accepted":             requestsAccepted.Load(),
			"dropped_auth":         requestsDroppedAuth.Load(),
			"dropped_size":         requestsDroppedSize.Load(),
			"dropped_rate":         requestsDroppedRate.Load(),
			"dropped_content_type": requestsDroppedType.Load(),
			"dropped_malformed":    requestsDroppedFormat.Load(),
			"dropped_busy":         requestsDroppedBusy.Load(),
		},
		Spans: map[string]uint64{
			"accepted":             spansAccepted.Load(),
			"dropped_service":      spansDroppedService.Load(),
			"dropped_name":         spansDroppedName.Load(),
			"dropped_invalid":      spansDroppedInvalid.Load(),
			"forwarded":            spansForwarded.Load(),
			"forward_failed":       spansForwardFailed.Load(),
			"forward_overflow":     spansForwardOverflow.Load(),
			"forward_rate_limited": spansForwardRateLimited.Load(),
		},
		Attributes: attributesDropped.Load(),
	}
}

var expvarOnce sync.Once

// RegisterExpvar publishes snowplow_rum_ingest on /debug/vars. Unconditional
// (like the build-info key): a zero must read as "nothing ingested", never as
// "not instrumented".
func RegisterExpvar() {
	expvarOnce.Do(func() {
		expvar.Publish("snowplow_rum_ingest", expvar.Func(func() any {
			s := Read()
			return map[string]any{
				"requests":           s.Requests,
				"spans":              s.Spans,
				"attributes_dropped": s.Attributes,
			}
		}))
	})
}
