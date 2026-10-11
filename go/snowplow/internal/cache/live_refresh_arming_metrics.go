// live_refresh_arming_metrics.go — #560: make it observable that live-refresh
// subscribers are failing to ARRIVE.
//
// THE DEFECT THIS EXISTS FOR. `GET /refreshes?sub=` carries the whole
// subscription set in the query string, so the URL grows ~361 encoded bytes per
// widget. On a 50-widget composition detail page that is an 18,098-character
// URL, and the ingress answers 431 (Request Header Fields Too Large) BEFORE
// snowplow ever sees the request. Measured on 057: 58 of 58 /call responses
// carried the refresh-key header (so the browser armed correctly), 5-6 subscribe
// attempts were made, every one returned 431, 0 stream chunks arrived. A clean
// A/B four minutes apart on the same cluster and build: /reviews with 17 widgets
// returned 200 and held the stream open; the 50-widget page returned 431 with 6
// immediate closes. One variable — widget count driving URL length.
//
// WHY NOTHING REPORTED IT, which is the part these counters fix. Snowplow could
// not distinguish "no subscribers" from "every subscriber rejected upstream":
//
//   - the request never reaches this process, so no snowplow log line exists;
//   - the success path logs at Info and the pod runs at LOG_LEVEL=warn, so even
//     WORKING subscriptions are invisible — there was no signal in EITHER
//     direction;
//   - and all three size defences sit ABOVE the real cliff, so none of them ever
//     fires: the ingress bites at ~22 widgets (8 KiB request line), while
//     snowplow's own refreshSubParamMaxBytes (16 KiB decoded) corresponds to ~60
//     widgets and the frontend's MAX_SUB_BYTES (refreshSse.ts, also 16 KiB) to
//     ~49-86. The 50-widget page decoded to 13,537 bytes — UNDER the frontend
//     cap, so it did not truncate; it built all 50 entries and the ingress killed
//     them. Every guard is placed where it cannot help.
//
// SO THESE COUNTERS MEASURE ARRIVAL, NOT CAPS. A cap that cannot be reached is
// not a detector (feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector);
// the question is whether a subscription the browser demonstrably built ever
// landed here.
//
// NOT DONE DELIBERATELY: #560 also suggests raising `refreshes: subscribed` out
// of Info. That would put a SUCCESS line at WARN, and warn-level volume in this
// service is supposed to measure failures rather than work
// (feedback_warn_level_log_volume_measures_failures_not_work). The counters
// below carry the same information to /debug/vars, which is the surface that is
// actually scraped, so the log level is left alone.
package cache

import (
	"expvar"
	"sync"
	"sync/atomic"
)

var (
	// refreshArmStampedTotal counts refresh-key headers stamped onto /call
	// responses — i.e. how many times snowplow TOLD a browser "this cell is
	// subscribable". Single funnel: dispatchers.setRefreshKeyHeader, which the
	// IfArmable and UnlessExternal variants both delegate to.
	refreshArmStampedTotal atomic.Uint64

	// refreshSubscribeArrivedTotal counts /refreshes requests that REACHED this
	// process. Incremented before validation and before the cache-off check, so
	// a rejected-by-snowplow subscription still counts as arrived — the whole
	// point is to separate "did not arrive" from "arrived and was refused".
	refreshSubscribeArrivedTotal atomic.Uint64

	// refreshSubscribeRejectedTotal counts arrivals snowplow itself refused
	// (400 — a malformed, oversized or over-long subscription). On the measured
	// deployment this is expected to stay 0 while the ingress is the thing
	// rejecting: that contrast is the signal.
	refreshSubscribeRejectedTotal atomic.Uint64

	// refreshIdleServedWarmupTotal / refreshIdleServedCacheOffTotal count
	// arrivals that were served an IDLE keepalive stream instead of an armed
	// one — #587. These exist because serveIdleSSE does NOT bump
	// refreshStreamsOpen (the only bump site is after the armed check), so
	// before this counter three OPPOSITE outcomes read identically as
	// "arrived +1, rejected +0, streams_open +0":
	//
	//	the len(armed)==0 refusal   — broken: nothing could be published
	//	the warmup divert           — HEALTHY: connected, waiting to warm
	//	the cache-off divert        — HEALTHY: transparent fallback
	//
	// Telling a refusal from a working stream required a pod-log read, which
	// is exactly what this detector existed to avoid.
	refreshIdleServedWarmupTotal   atomic.Uint64
	refreshIdleServedCacheOffTotal atomic.Uint64

	// refreshConnectWriteFailedTotal counts arrivals that were ARMED and had
	// their SSE headers committed, but whose very first frame could not be
	// written — the client went away during connect. Found by #587's own
	// enumeration guard: this return sits BEFORE RefreshStreamOpened(), so it
	// too produced the ambiguous "arrived +1, rejected +0, streams_open +0"
	// reading. Distinct from a refusal (snowplow was willing) and from an idle
	// stream (nothing is being held open).
	refreshConnectWriteFailedTotal atomic.Uint64

	// refreshStreamsOpen is a GAUGE of live-refresh streams currently held open.
	// It is the denominator that makes a zero arm-rate readable: widgets arming
	// with no stream open anywhere is the #560 signature.
	refreshStreamsOpen atomic.Int64
)

// BumpRefreshArmStamped records one refresh-key header stamped onto a /call
// response. Called from the single stamp funnel in dispatchers.
func BumpRefreshArmStamped() { refreshArmStampedTotal.Add(1) }

// BumpRefreshSubscribeArrived records that a /refreshes request reached this
// process. MUST be called at the top of the handler — before the cache-off
// branch and before validation — or the counter stops answering the question it
// exists for.
func BumpRefreshSubscribeArrived() { refreshSubscribeArrivedTotal.Add(1) }

// BumpRefreshSubscribeRejected records an arrival snowplow refused itself.
func BumpRefreshSubscribeRejected() { refreshSubscribeRejectedTotal.Add(1) }

// RefreshStreamOpened / RefreshStreamClosed move the open-stream gauge. Callers
// MUST pair them (defer the close at the point of open).
// BumpRefreshIdleServedWarmup records an arrival diverted to the idle keepalive
// stream because the pod is still warming. This is a HEALTHY outcome: the
// browser is connected and starts receiving publishes once warm.
func BumpRefreshIdleServedWarmup() { refreshIdleServedWarmupTotal.Add(1) }

// BumpRefreshIdleServedCacheOff records an arrival diverted to the idle
// keepalive stream because the cache is off. Also HEALTHY — the documented
// transparent fallback.
func BumpRefreshIdleServedCacheOff() { refreshIdleServedCacheOffTotal.Add(1) }

// BumpRefreshConnectWriteFailed records an armed subscription whose first SSE
// frame could not be written — the client disconnected during connect.
func BumpRefreshConnectWriteFailed() { refreshConnectWriteFailedTotal.Add(1) }

func RefreshStreamOpened() { refreshStreamsOpen.Add(1) }

// RefreshStreamClosed decrements the open-stream gauge.
func RefreshStreamClosed() { refreshStreamsOpen.Add(-1) }

// LiveRefreshArmingStats is an atomic snapshot of the #560 arming counters.
//
// HOW TO READ IT — the whole value is in the COMBINATION, and each row says what
// it reads DURING the defect rather than only when healthy:
//
//	ArmStamped > 0, SubscribeArrived == 0, StreamsOpen == 0
//	    Widgets are arming and NOTHING is arriving. Every subscription is being
//	    rejected before it reaches this process — the #560 signature (431 at the
//	    edge). ArmStamped KEEPS CLIMBING while this holds, so the detector is a
//	    positive signal, not an absence.
//
//	    MEASURED 2026-10-10, correcting an earlier note here: the edge bound is
//	    a ~16 KiB HTTP/2 HEADER-LIST budget, not an 8 KiB request line, and it
//	    is SHARED with the Authorization bearer token (928 B measured). Bisected
//	    to the byte: 16,133 passes / 16,134 is 431 bare; 14,506 / 14,507 with a
//	    browser-sized header set. So it bites around 40 widgets, not ~22, AND it
//	    MOVES PER USER with token size — there is no fixed safe widget count.
//	    Since 1.12.40 the POST transport carries the set in the body and does
//	    not spend this budget at all, so on an up-to-date client this signature
//	    should no longer appear.
//
//	SubscribeArrived > 0, SubscribeRejected ~= SubscribeArrived
//	    Subscriptions arrive and snowplow refuses them itself (400). Two paths
//	    reach here and BOTH now count: an oversized/malformed payload, and the
//	    blessed C64-1 honest-400 (warm, but nothing the requester asked for is
//	    cached or permitted — the expected outcome on a userAccessFilter-backed
//	    page, #561). Before #587 only the first counted, so this read 0 while
//	    every subscriber was turned away.
//
//	SubscribeArrived > 0, IdleServed* > 0, StreamsOpen == 0
//	    HEALTHY, not broken. The browser is connected to a keepalive stream and
//	    starts receiving publishes once the pod warms (or is on the documented
//	    cache-off fallback). Without the IdleServed counters this reads
//	    IDENTICALLY to the refusal above — same arrived, same rejected, same
//	    streams_open — which is the #587 blind spot.
//
//	SubscribeArrived > 0, StreamsOpen > 0
//	    Healthy. ArmStamped >> SubscribeArrived is EXPECTED here and is not a
//	    fault: one stream arms every widget on the page, so the stamp count
//	    scales with widgets while the arrival count scales with page loads.
//	    Do NOT read their ratio as an error rate.
//
//	All four == 0
//	    Nobody is using the product. Ambiguous on its own, which is why these
//	    publish together rather than separately.
type LiveRefreshArmingStats struct {
	ArmStamped         uint64
	SubscribeArrived   uint64
	SubscribeRejected  uint64
	IdleServedWarmup   uint64
	IdleServedCacheOff uint64
	ConnectWriteFailed uint64
	StreamsOpen        int64
}

// LiveRefreshArmingStatsSnapshot returns the current values.
func LiveRefreshArmingStatsSnapshot() LiveRefreshArmingStats {
	return LiveRefreshArmingStats{
		ArmStamped:         refreshArmStampedTotal.Load(),
		SubscribeArrived:   refreshSubscribeArrivedTotal.Load(),
		SubscribeRejected:  refreshSubscribeRejectedTotal.Load(),
		IdleServedWarmup:   refreshIdleServedWarmupTotal.Load(),
		IdleServedCacheOff: refreshIdleServedCacheOffTotal.Load(),
		ConnectWriteFailed: refreshConnectWriteFailedTotal.Load(),
		StreamsOpen:        refreshStreamsOpen.Load(),
	}
}

// ResetLiveRefreshArmingStatsForTest zeroes the counters. Test-only seam,
// composed into ResetCacheProcessStateForTest (#471); production code MUST NOT
// call it.
func ResetLiveRefreshArmingStatsForTest() {
	refreshArmStampedTotal.Store(0)
	refreshSubscribeArrivedTotal.Store(0)
	refreshSubscribeRejectedTotal.Store(0)
	refreshIdleServedWarmupTotal.Store(0)
	refreshIdleServedCacheOffTotal.Store(0)
	refreshConnectWriteFailedTotal.Store(0)
	refreshStreamsOpen.Store(0)
}

// liveRefreshArmingExpvarOnce guards Publish against the duplicate-key panic
// when both init() and a test seam run.
var liveRefreshArmingExpvarOnce sync.Once

// CFG-1: gated exactly like the other cache surfaces. Live refresh is a cache
// mechanism (RefreshSSEEnabled, PublishRefresh), so with the cache off there is
// nothing to arm and the key must not appear.
func init() {
	if Disabled() {
		return
	}
	registerLiveRefreshArmingExpvar()
}

// registerLiveRefreshArmingExpvar publishes snowplow_live_refresh_arming.
// Idempotent.
func registerLiveRefreshArmingExpvar() {
	liveRefreshArmingExpvarOnce.Do(func() {
		expvar.Publish("snowplow_live_refresh_arming", expvar.Func(func() any {
			s := LiveRefreshArmingStatsSnapshot()
			return map[string]int64{
				"arm_stamped_total":           int64(s.ArmStamped),
				"subscribe_arrived_total":     int64(s.SubscribeArrived),
				"subscribe_rejected_total":    int64(s.SubscribeRejected),
				"idle_served_warmup_total":    int64(s.IdleServedWarmup),
				"idle_served_cache_off_total": int64(s.IdleServedCacheOff),
				"connect_write_failed_total":  int64(s.ConnectWriteFailed),
				"streams_open":                s.StreamsOpen,
			}
		}))
	})
}

// RegisterLiveRefreshArmingExpvarForTest forces registration under tests that
// flip CACHE_ENABLED with t.Setenv after init() already ran. Idempotent.
// Production callers MUST NOT use it.
func RegisterLiveRefreshArmingExpvarForTest() {
	registerLiveRefreshArmingExpvar()
}
