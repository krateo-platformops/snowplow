// rbac_role_noop_expvar.go — #260, the /debug/vars half of the role UPDATE
// no-op counters (rbac_role_noop_counters.go). Same main()-bootstrap,
// cache-mode-agnostic, zero-readable surface as RegisterRBACBindingNoopExpvar /
// RegisterRBACSubGenExpvar: the KEYS are present from the first scrape so a `0`
// reads as "no role no-op updates", never as "not instrumented".

package cache

import (
	"expvar"
	"sync"
)

var rbacRoleNoopExpvarOnce sync.Once

// RegisterRBACRoleNoopExpvar publishes the three role UPDATE keys. Idempotent
// (sync.Once — expvar.Publish panics on a duplicate key). Called from main.go's
// HTTP mux bootstrap next to RegisterRBACBindingNoopExpvar; safe from tests via
// the public name.
func RegisterRBACRoleNoopExpvar() {
	rbacRoleNoopExpvarOnce.Do(func() {
		expvar.Publish("snowplow_rbac_role_update_events_total", expvar.Func(func() any {
			return RBACRoleUpdateEventsTotal()
		}))
		expvar.Publish("snowplow_rbac_role_noop_updates_total", expvar.Func(func() any {
			return RBACRoleNoopUpdatesTotal()
		}))
		expvar.Publish("snowplow_rbac_role_semantic_noop_updates_total", expvar.Func(func() any {
			return RBACRoleSemanticNoopUpdatesTotal()
		}))
	})
}
