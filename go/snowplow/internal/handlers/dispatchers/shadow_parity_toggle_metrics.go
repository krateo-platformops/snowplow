// shadow_parity_toggle_metrics.go — #367: the UNGATED detectability surface for
// the shadow-parity toggle. Unlike the cache-only measurement COUNTERS
// (snowplow_v7_shadow_parity, gated off cache-off because they are meaningless
// without a snapshot — shadow_parity_hook.go init CFG-1 gate), the toggle STATE +
// SOURCE is a CONFIG fact a latency-acceptance window must be able to assert
// regardless of CACHE_ENABLED. So this publisher runs in EVERY mode (NO CFG-1
// gate), mirroring the other non-cache init publishers (readiness backstop,
// malformed-dial, nondial-jq). Scalar-only: a bool-as-0/1 + a source label, NO
// per-identity data. Allow-listed in e2e/bench/cfg1_probe/cfg1_structural_test.go
// (nonCacheInitPublishers).

package dispatchers

import (
	"expvar"
	"sync"

	"github.com/krateo-platformops/snowplow/internal/rbac"
)

var shadowParityToggleOnce sync.Once

// init publishes the toggle-state surface UNGATED (before/without any cache gate)
// so /debug/vars exposes it in every mode.
func init() { registerShadowParityToggleState() }

func registerShadowParityToggleState() {
	shadowParityToggleOnce.Do(func() {
		expvar.Publish("snowplow_v7_shadow_parity_toggle", expvar.Func(func() any {
			// #367 — effective toggle state + source (default|env|runtime-post) for
			// latency-window detectability: enabled==0 is the healthy default a
			// latency-acceptance window asserts before trusting the run. DIAGNOSTIC
			// (zero==off==healthy), scalar-only, no per-identity data.
			enabled := uint64(0)
			if rbac.ShadowParityEnabled() {
				enabled = 1
			}
			return map[string]any{
				"enabled": enabled,
				"source":  rbac.ShadowParitySource(),
			}
		}))
	})
}
