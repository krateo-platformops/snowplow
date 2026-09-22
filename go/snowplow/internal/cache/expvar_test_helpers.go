// expvar_test_helpers.go — CFG-1 (Ship 0.30.163) test scaffolding.
//
// Background: per project memory `project_cache_off_is_transparent_fallback`
// the cache expvar gauges are registered at package init() ONLY when
// CACHE_ENABLED is truthy at process start. Tests that exercise the
// expvar.Handler() surface need the gauges registered, but the
// `go test` binary's package init() runs BEFORE the test code can set
// CACHE_ENABLED via t.Setenv. TestMain is not enough either — it also
// runs after init().
//
// RegisterExpvarForTest is the explicit, test-only workaround:
// after t.Setenv("CACHE_ENABLED", "true"), call this helper to force
// the registration body to run. The underlying sync.Once guards
// prevent any double-Publish panic.
//
// This file is NOT _test.go because the helper is also useful for
// e2e bench harnesses that link the package. It must remain inert at
// runtime — calling it from the production binary is a no-op since
// init() will already have registered (the Once is consumed).

package cache

// RegisterExpvarForTest forces the cache expvar gauges to be
// registered at /debug/vars. Idempotent (sync.Once-guarded). Intended
// for unit tests that need to assert presence/absence of the five
// snowplow_* keys via expvar.Handler() while CACHE_ENABLED was not
// set at process start.
//
// Production callers MUST NOT use this function — it bypasses the
// cache-off compliance gate. The boot-time gate in init() is the
// authoritative mechanism; the falsifier is HG-321
// (e2e/bench/cfg1_falsifier.sh, a 4-env-value process-spawn matrix).
func RegisterExpvarForTest() {
	registerFallthroughExpvar()
	registerControllerHealthExpvar()
	registerRefresherMetrics()
	registerCRDDiscoveryExpvar()
	registerInformerWatchExpvar()
	registerReflectorPathExpvar()
}
