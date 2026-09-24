// rbac_binding_noop_expvar.go — #247, the /debug/vars half of the binding
// no-op-update counters (rbac_binding_noop_counters.go).
//
// Separate from internal/metrics for the same reason rbac_subgen_expvar.go is:
// internal/metrics is the OTLP MIRROR and publishes NOTHING to expvar (its own
// header says the /debug/vars surface is UNTOUCHED), and its whole pipeline is
// gated by OTEL_METRICS_ENABLED, default false. A counter wired only there is
// unreadable on a default pod.
//
// ZERO-READABILITY: published UNCONDITIONALLY from main.go's HTTP bootstrap,
// cache mode-agnostic, alongside RegisterRBACSnapshotExpvar and
// RegisterRBACSubGenExpvar. expvar.Func evaluates at scrape time, so the KEYS
// are present from the first scrape and a `0` reads as "no no-op updates", never
// as "not instrumented".

package cache

import (
	"expvar"
	"sync"
)

// rbacBindingNoopExpvarOnce guards RegisterRBACBindingNoopExpvar so the
// registration body runs at most once per process — expvar.Publish panics on a
// duplicate key.
var rbacBindingNoopExpvarOnce sync.Once

// RegisterRBACBindingNoopExpvar publishes the two binding no-op-update keys.
// Idempotent. Called from main.go's HTTP mux bootstrap; safe to call from tests
// via the public name.
func RegisterRBACBindingNoopExpvar() {
	rbacBindingNoopExpvarOnce.Do(func() {
		expvar.Publish("snowplow_rbac_binding_noop_updates_total", expvar.Func(func() any {
			return RBACBindingNoopUpdatesTotal()
		}))
		expvar.Publish("snowplow_rbac_binding_semantic_noop_updates_total", expvar.Func(func() any {
			return RBACBindingSemanticNoopUpdatesTotal()
		}))
	})
}
