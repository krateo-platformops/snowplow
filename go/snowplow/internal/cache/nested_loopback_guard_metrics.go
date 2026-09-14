// nested_loopback_guard_metrics.go — R (composition-resources loopback guard)
// process-wide falsifier counters for the HTTP-edge recursion guard.
//
// These are the "did the guard fire?" signals the R falsifier reads (mirroring
// ExternalSkippedPut / BumpExternalSkippedPut). They are the HTTP-edge twin of
// the in-process cycle-stop log line at nested_call.go:170 — but a counter, so
// a test can assert ">= 1" without scraping logs, and so an on-cluster operator
// can watch the guard-fire rate via expvar.
//
// Two distinct counters so the falsifier can discriminate WHICH guard tripped:
//   - HTTPEdgeCycleStop: the #79 ancestor-set stop (the node re-appeared on the
//     current descent path → returned the raw CR). This is the PRIMARY,
//     cost-optimal stop (fires at the first self-reentry).
//   - HTTPEdgeDepthStop: the depth-8 BACKSTOP (a non-cyclic pathologically deep
//     chain hit NestedCallMaxDepth over HTTP). Should stay 0 on the
//     composition-resources path once R converges it (the ancestor stop fires
//     first); a non-zero here flags a chain the ancestor set could not see.
package cache

import "sync/atomic"

var (
	httpEdgeCycleStopTotal atomic.Uint64
	httpEdgeDepthStopTotal atomic.Uint64
)

// BumpHTTPEdgeCycleStop increments the ancestor-set HTTP-edge cycle-stop
// counter. Called by the dispatcher handlers when an inbound TRUSTED
// self-loopback re-enters with its own node already in the seeded ancestor set.
func BumpHTTPEdgeCycleStop() { httpEdgeCycleStopTotal.Add(1) }

// HTTPEdgeCycleStop returns the process-wide ancestor-set HTTP-edge stop count.
func HTTPEdgeCycleStop() uint64 { return httpEdgeCycleStopTotal.Load() }

// BumpHTTPEdgeDepthStop increments the depth-8 HTTP-edge backstop counter.
func BumpHTTPEdgeDepthStop() { httpEdgeDepthStopTotal.Add(1) }

// HTTPEdgeDepthStop returns the process-wide depth-8 HTTP-edge backstop count.
func HTTPEdgeDepthStop() uint64 { return httpEdgeDepthStopTotal.Load() }

// (1.12.6 C9: the D bounded-partial counter BumpPartialServedStale /
// PartialServedStale that lived here was retired with partial_result_ttl.go.)
