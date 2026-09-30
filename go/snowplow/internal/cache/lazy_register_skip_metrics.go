// lazy_register_skip_metrics.go — #215 observability counter for the #119
// unserved-GROUP lazy-register pre-check skip.
//
// THE GAP (#215). EnsureResourceType is the single spawn-site funnel for lazy
// informer registration. When the #119 pre-check finds a GVR's API GROUP
// entirely absent from a SUCCESSFUL ServerGroups() response, it SKIPS
// registration (no informer, no perpetual watch-404 churn) and logs
// `cache.lazy_register.skipped_unserved_group` at INFO. The shipped chart runs
// LOG_LEVEL=warn, so that line is DARK in production and the skip is
// unobservable: a brand-new API GROUP's first CRD can lag up to one
// servedGroupsMemoTTL (=discoveryRefreshInterval, ~30s) + one re-touch before
// its informer registers, and nothing surfaces it. The 1.12.6 S6 harness saw
// nothing in expvar for exactly this reason (its harness CRD `harness.krateo.io`
// is a new group). This counter promotes that INFO breadcrumb to an always-on
// expvar so a new-group registration lag is READABLE rather than inferred.
//
// GROUP-GRANULAR, deliberately. Only the group-absent branch of
// unregisterableReason bumps this — NOT the sibling resource-unwatchable (A2)
// branch, which is a different skip cause (a served group whose resource
// declares no list+watch). The issue is titled on the new-GROUP lag; conflating
// the two behind one counter would make the number uninterpretable.
//
// CFG-1 (option a — gate on !Disabled()). The skip path only runs cache-ON:
// under CACHE_ENABLED=false there is no ResourceWatcher, no EnsureResourceType,
// no pre-check — the request path is transparent apiserver fall-through
// (project_cache_off_is_transparent_fallback). So this key MUST NOT register
// with the cache off, exactly like every other cache counter
// (uaf_put_decline_metrics.go, deps_expvar.go): init() returns early on
// Disabled(). The CFG-1 structural falsifier
// (e2e/bench/cfg1_probe/cfg1_structural_test.go) then derives it into the gated
// set automatically — internal/cache is already in the probe's import graph, so
// no nonCacheInitPublishers allowlist entry is needed (that list is for NON-cache
// surfaces only).
//
// INTERPRETABLE ZERO
// (feedback_a_counter_whose_zero_reads_as_health_is_not_a_detector). A bare `0`
// is ambiguous: "no unserved-group skips happened" vs "the skip feature is
// switched OFF (SKIP_UNSERVED_GROUP_INFORMERS=false), so a skip can never be
// counted". The two read identically on a scalar. So the surface is a MAP that
// carries the count NEXT TO the enabled-state, read from the SAME knob the skip
// path consults (SkipUnservedGroupInformers()):
//
//	snowplow_lazy_register_skip = {
//	  "skipped_unserved_group_total":            <count>,
//	  "skip_unserved_group_informers_enabled":   <bool>,
//	}
//
// A reader who sees total=0 AND enabled=false knows the zero is vacuous; total=0
// AND enabled=true is genuine health.

package cache

import (
	"expvar"
	"sync"
	"sync/atomic"
)

// lazyRegisterSkippedUnservedGroupTotal counts EnsureResourceType lazy
// registrations SKIPPED because the GVR's API group was authoritatively absent
// from ServerGroups() (the #119 unserved-GROUP pre-check). Bumped once per skip
// through BumpLazyRegisterSkippedUnservedGroup at the group-absent branch of
// unregisterableReason — the single decision point.
var lazyRegisterSkippedUnservedGroupTotal atomic.Uint64

// BumpLazyRegisterSkippedUnservedGroup increments the unserved-group skip
// counter. Called only from unregisterableReason's group-absent branch.
func BumpLazyRegisterSkippedUnservedGroup() { lazyRegisterSkippedUnservedGroupTotal.Add(1) }

// LazyRegisterSkippedUnservedGroup returns the process-wide count of lazy
// registrations skipped because the group was unserved.
func LazyRegisterSkippedUnservedGroup() uint64 { return lazyRegisterSkippedUnservedGroupTotal.Load() }

// LazyRegisterSkipStats returns the interpretable-zero surface: the skip count
// alongside the enabled-state of the pre-check that produces it. This is the
// map both /debug/vars snowplow_lazy_register_skip and any future OTLP mirror
// read, so the count is never read without its enabled-state context.
func LazyRegisterSkipStats() map[string]any {
	return map[string]any{
		"skipped_unserved_group_total":          LazyRegisterSkippedUnservedGroup(),
		"skip_unserved_group_informers_enabled": SkipUnservedGroupInformers(),
	}
}

// ResetLazyRegisterSkipCountersForTest zeroes the counter so a falsifier can
// assert an EXACT delta regardless of earlier arms in the same test binary.
// Production callers MUST NOT use this.
func ResetLazyRegisterSkipCountersForTest() { lazyRegisterSkippedUnservedGroupTotal.Store(0) }

// lazyRegisterSkipMetricsOnce guards expvar.Publish against the duplicate-key
// panic (mirrors uafPutDeclineMetricsOnce).
var lazyRegisterSkipMetricsOnce sync.Once

func init() {
	// CFG-1 (option a): under CACHE_ENABLED=false there is no lazy-register
	// skip path, so this key MUST NOT register (transparent-fallback contract).
	if Disabled() {
		return
	}
	registerLazyRegisterSkipMetrics()
}

// registerLazyRegisterSkipMetrics publishes the #215 skip surface. Guarded by
// sync.Once so it is safe from both init() and the test helper.
func registerLazyRegisterSkipMetrics() {
	lazyRegisterSkipMetricsOnce.Do(func() {
		expvar.Publish("snowplow_lazy_register_skip", expvar.Func(func() any {
			return LazyRegisterSkipStats()
		}))
	})
}

// RegisterLazyRegisterSkipMetricsForTest forces registration under tests that
// flip CACHE_ENABLED=true via t.Setenv after init() already ran with the var
// unset. Idempotent. Production callers MUST NOT use this.
func RegisterLazyRegisterSkipMetricsForTest() { registerLazyRegisterSkipMetrics() }
