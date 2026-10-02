// residency_metrics_374.go — #374 Part B and #383 diagnostic counters. (The
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

// enqueueDroppedNonResident (#383) counts dirty-marks whose refresher queue slot
// was DROPPED at the hook because the key was not resident in L1 (the SSI still
// fired). Load-dependent by nature (the non-resident share of marks swings with
// traffic), so quote it with its load. DIAGNOSTIC, expvar only, like the counter
// above.
var enqueueDroppedNonResident atomic.Uint64

// EnqueueDroppedNonResidentTotal — exported for the #383 arms and the C1 harness.
func EnqueueDroppedNonResidentTotal() uint64 { return enqueueDroppedNonResident.Load() }

// refreshHookResidencyChecked is a TEST seam (nil in production): called by the
// refresher hook right after its residency check, before the drop/enqueue
// decision takes effect, so an arm can land a Put inside that window.
var refreshHookResidencyChecked atomic.Pointer[func(l1Key string, resident bool)]

// SetRefreshHookResidencyCheckedHookForTest installs fn as the #383 check→decision
// window seam. Production code MUST NOT call it. Returns a restore func.
func SetRefreshHookResidencyCheckedHookForTest(fn func(l1Key string, resident bool)) (restore func()) {
	var p *func(string, bool)
	if fn != nil {
		p = &fn
	}
	prev := refreshHookResidencyChecked.Swap(p)
	return func() { refreshHookResidencyChecked.Store(prev) }
}

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
				"pickup_noop_no_park":          pickupNoopNoPark.Load(),
				"enqueue_dropped_non_resident": enqueueDroppedNonResident.Load(),
			}
		}))
	})
}

// RegisterResidencyMetrics374ExpvarForTest forces registration under a test that
// flips CACHE_ENABLED=true after init() already ran cache-off. Idempotent.
func RegisterResidencyMetrics374ExpvarForTest() { registerResidencyMetrics374Expvar() }
