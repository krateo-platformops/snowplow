// residency_metrics_374.go — #374 Part B diagnostic counter. (The
// side-effect-free Has() membership probe Part B uses is canonical on
// ResolvedCacheStore in resolved.go, next to Get.)
//
// DIAGNOSTIC, not a detector: a zero reads as "no non-resident no-op ever
// skipped a park" = health, so no OTLP hand-wire (#311-class) — expvar only.
// Published only cache-on (the refresher runs only cache-on; cache-off ⟹ no
// cache expvars, CFG-1).

package cache

import (
	"expvar"
	"sync"
	"sync/atomic"
)

// pickupNoopNoPark (#374 Part B) counts dequeues where the residency probe
// skipped the yieldToCustomer park because the key was non-resident at pickup (a
// no-op that would otherwise park a worker behind a customer burst).
var pickupNoopNoPark atomic.Uint64

// PickupNoopNoParkTotal — exported so the #374 Part B falsifier + the C1 WORK
// gate assert the realized cut via the counter (not by log-tailing).
func PickupNoopNoParkTotal() uint64 { return pickupNoopNoPark.Load() }

var residencyMetrics374Once sync.Once

func init() {
	// CFG-1: cache-off ⟹ no refresher ⟹ this never moves, so do not publish it
	// cache-off (keeps the cache-off expvar surface clean).
	if Disabled() {
		return
	}
	registerResidencyMetrics374Expvar()
}

func registerResidencyMetrics374Expvar() {
	residencyMetrics374Once.Do(func() {
		expvar.Publish("snowplow_refresher_residency_cheapen", expvar.Func(func() any {
			return map[string]uint64{
				"pickup_noop_no_park": pickupNoopNoPark.Load(),
			}
		}))
	})
}

// RegisterResidencyMetrics374ExpvarForTest forces registration under a test that
// flips CACHE_ENABLED=true after init() already ran cache-off. Idempotent.
func RegisterResidencyMetrics374ExpvarForTest() { registerResidencyMetrics374Expvar() }
