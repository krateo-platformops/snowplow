// informer_dispatch_shape_expvar.go — #578: publish the informer-dispatch serve
// shape, so the zero-decode envelope change has an instrument that is not
// confounded by load.
//
// WHY THIS EXISTS, AND WHY IT IS A CORRECTION. When #578's envelope change
// landed I deliberately did NOT publish dispatchInformerListServedRaw, reasoning
// that "its outcome metric is memstats.TotalAlloc rate and pod CPU — the
// instruments that detected the problem". That reasoning was wrong, and 057
// proved it the same day:
//
//	during the incident-controller churn   after it was fixed (10:47:55Z)
//	  CPU        3,950m / 4,000m             477m
//	  TotalAlloc 568 MB/s                    3.8 MB/s
//	  per refresh 16.9 MB                    1.4 MB
//
// Nothing in snowplow changed between those readings — incident-controller 0.3.0
// removed a 1 write/s retry loop and snowplow's amplification of it went with
// it. So allocation rate and CPU move by two orders of magnitude for reasons
// that have nothing to do with this code, and even the PER-REFRESH figure moves
// 12x because it depends on WHICH cells are hot (the 9.1 MB CRD list and the
// 4.4 MB events list were being rebuilt then and are not now).
//
// A metric that swings 149x on someone else's deploy cannot accept or refute a
// change in here. This ratio can: it is a share of serves, so it is independent
// of how many serves there are and of how large they were.
//
// READ IT AS A RATIO. list_served_raw / list_served is the share of LIST serves
// that took the zero-decode path — assembling the envelope from the per-item
// JSON the indexer already holds instead of decoding every object into map trees
// and re-encoding them. On a pod doing api-stage content work that share should
// dominate; a collapse toward 0 means the eligibility predicate regressed, which
// no allocation graph would distinguish from "the cluster went quiet".
//
// list_served_raw alone is NOT readable: a zero reads identically whether the
// path broke or no api-stage content resolve has happened yet
// (feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector). That is why
// the whole shape publishes together rather than the one new counter.
//
// CFG-1: gated on cache.Disabled() like every other cache surface. Informer
// dispatch is a cache mechanism — with the cache off, /call goes to the
// apiserver and this family is structurally zero, so publishing it would assert
// a measurement that cannot happen. It therefore needs NO entry in
// nonCacheInitPublishers: that list is for publishers which are deliberately
// UNgated because they stay meaningful cache-off (the malformed-dial guard, the
// readiness surfaces, partial discovery). This one does not.
package api

import (
	"expvar"
	"sync"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

var informerDispatchShapeOnce sync.Once

func init() {
	if cache.Disabled() {
		return
	}
	registerInformerDispatchShapeExpvar()
}

// registerInformerDispatchShapeExpvar publishes snowplow_informer_dispatch_shape.
// Idempotent, so init() and the test seam can both call it.
func registerInformerDispatchShapeExpvar() {
	informerDispatchShapeOnce.Do(func() {
		expvar.Publish("snowplow_informer_dispatch_shape", expvar.Func(func() any {
			s := DispatchInformerStatsSnapshot()
			return map[string]int64{
				// The #578 pair. The RATIO is the reading; neither is a detector alone.
				"list_served":     int64(s.ListServed),
				"list_served_raw": int64(s.ListServedRaw),
				// The denominators that say whether the pivot is doing anything at
				// all, so a zero ratio can be told apart from an idle pod.
				"get_served":       int64(s.GetServed),
				"fallthrough":      int64(s.Fallthrough),
				"rbac_dropped":     int64(s.RBACDropped),
				"sync_wait_served": int64(s.SyncWaitServed),
			}
		}))
	})
}

// RegisterInformerDispatchShapeExpvarForTest forces registration under tests that
// flip CACHE_ENABLED with t.Setenv after init() already ran. Idempotent.
// Production callers MUST NOT use it.
func RegisterInformerDispatchShapeExpvarForTest() {
	registerInformerDispatchShapeExpvar()
}
