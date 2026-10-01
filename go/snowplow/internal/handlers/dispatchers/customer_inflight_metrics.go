package dispatchers

// customer_inflight_metrics.go — #386 M3 (ruling A): expose the EXISTING
// resolve-path customer-in-flight COUNT (customerInFlightCount, prewarm_engine.go)
// as a precisely-scoped observability signal + an int accessor the refresher /
// #384 serveReserve reads.
//
// SCOPE (precisely named on purpose — NEVER "all customer activity"): this counts
// a customer /call ONLY while it is executing a RESOLVE-PATH dispatch — GET /call
// or POST /call/read that reached a restactions / widgets handler, where
// markCustomerInFlight is bracketed at the top of ServeHTTP (restactions.go /
// widgets.go). It DELIBERATELY EXCLUDES:
//   - the terminal direct-proxy Call() / CallRead() fallthrough (call.go) — a GVR
//     with no resolve handler is a plain apiserver passthrough;
//   - GET /list;
//   - all write verbs (POST/PUT/PATCH/DELETE /call route straight to Call() with
//     no Dispatcher, so they never mark).
//
// ONCE PER OUTERMOST CALL (no double-count): markCustomerInFlight is bracketed at
// the TOP of ServeHTTP ONLY (defer markCustomerInFlight()()). A nested resolve runs
// via the in-process apiref.Resolve path and NEVER re-enters ServeHTTP, so it does
// not re-mark — the count is exactly the number of OUTERMOST resolve-path
// dispatches. (A future change that ever routed a nested resolve back through
// ServeHTTP would break this once-per-outer-call invariant.)
//
// Those are I/O-bound apiserver proxies that do NOT contend for the refresher's
// resolve-CPU. This is the SAME population the refresher's customer-priority yield
// already keys off (cache.SetCustomerInflightHook), so #384 serveReserve sizes
// against the SAME customer definition the live yield uses — not a broader
// all-customer-activity signal (that would be a different metric; see #386 ruling).

import (
	"expvar"
	"sync"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// CustomerResolveInFlightCount returns the number of RESOLVE-PATH customer /call
// dispatches currently executing (see the scope note above). Clamped to >= 0: the
// count is maintained by balanced defer inc/dec at the ServeHTTP boundary (which
// covers error and panic exits), so it should never be negative — the clamp is a
// defensive read-side floor so a hypothetical imbalance can never feed a negative
// into #384 serveReserve.
func CustomerResolveInFlightCount() int64 {
	if n := customerInFlightCount.Load(); n > 0 {
		return n
	}
	return 0
}

var customerInflightMetricsOnce sync.Once

func init() {
	// Cache-subsystem observability: the count feeds the refresher's customer-
	// priority yield and #384 serveReserve, both of which only run under
	// CACHE_ENABLED=true. Gated like the other dispatcher cache metrics
	// (l1_lookup_metrics.go) — the atomic itself is still maintained cache-off
	// (markCustomerInFlight runs regardless), only its expvar exposure is gated.
	if cache.Disabled() {
		return
	}
	registerCustomerInflightMetrics()
}

// registerCustomerInflightMetrics publishes the resolve-path in-flight gauge.
// Guarded by customerInflightMetricsOnce (expvar.Publish panics on a duplicate
// key). expvar.Func is lazy — it reads the live count at scrape time.
func registerCustomerInflightMetrics() {
	customerInflightMetricsOnce.Do(func() {
		// Precisely named: RESOLVE-PATH in-flight, NEVER "all customer calls".
		expvar.Publish("snowplow_customer_resolve_inflight", expvar.Func(func() any {
			return CustomerResolveInFlightCount()
		}))
	})
}
