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
//
// PUBLISHED (#1274 review): until #1274 they were read only by tests. #1274
// makes the ingest trust a self-hop by the short Service name, which switches
// the guard on for loopbacks through the portal's short-form endpoint, so a
// chain that re-enters itself now gets the raw CR — and that must be visible on
// a live pod. Published as snowplow_nested_loopback_guard_stops_total{kind} on
// /debug/vars and OTLP, kind ∈ NestedLoopbackGuardStopKinds, both cells present
// at 0 from process start (absent and zero are different readings). No
// RESTAction name, node or identity is a label.
package cache

import (
	"expvar"
	"sync"
	"sync/atomic"
)

var (
	httpEdgeCycleStopTotal atomic.Uint64
	httpEdgeDepthStopTotal atomic.Uint64
)

// The stop kinds, the closed label set of the published counter.
const (
	NestedLoopbackGuardStopCycle = "cycle" // HTTPEdgeCycleStop
	NestedLoopbackGuardStopDepth = "depth" // HTTPEdgeDepthStop
)

// NestedLoopbackGuardStopKinds is the closed kind set, in emit order.
var NestedLoopbackGuardStopKinds = []string{NestedLoopbackGuardStopCycle, NestedLoopbackGuardStopDepth}

// NestedLoopbackGuardStopsSnapshot is every kind's count, both always present.
func NestedLoopbackGuardStopsSnapshot() map[string]uint64 {
	return map[string]uint64{
		NestedLoopbackGuardStopCycle: HTTPEdgeCycleStop(),
		NestedLoopbackGuardStopDepth: HTTPEdgeDepthStop(),
	}
}

func init() {
	// CFG-1: the guard runs only with the cache on (the dispatchers gate it on
	// !Disabled()), so under cache-off the key is absent, like every other
	// cache-side publisher.
	if Disabled() {
		return
	}
	registerNestedLoopbackGuardExpvar()
}

var nestedLoopbackGuardExpvarOnce sync.Once

func registerNestedLoopbackGuardExpvar() {
	nestedLoopbackGuardExpvarOnce.Do(func() {
		expvar.Publish("snowplow_nested_loopback_guard_stops_total", expvar.Func(func() any {
			return NestedLoopbackGuardStopsSnapshot()
		}))
	})
}

// RegisterNestedLoopbackGuardExpvarForTest publishes the key under a test that
// set CACHE_ENABLED after init ran with it unset. Idempotent. Production code
// MUST NOT call it.
func RegisterNestedLoopbackGuardExpvarForTest() { registerNestedLoopbackGuardExpvar() }

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
