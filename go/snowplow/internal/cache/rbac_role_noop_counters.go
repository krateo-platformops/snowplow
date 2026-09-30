// rbac_role_noop_counters.go — #257 (moved from #260 change 2): role-path UPDATE
// no-op attribution. Classifies each ClusterRole/Role UPDATE the same way
// rbac_binding_noop_counters.go classifies a binding UPDATE, so bumps_total's
// role share splits into "real rule change" vs "label / relist churn".
// role_semantic_noop counts EXACTLY what #257's skip skips (same predicate).
// Reuses policyRulesEqual (rbac_role_rules_equal.go — the C4 predicate).

package cache

import (
	"sync/atomic"

	rbacv1 "k8s.io/api/rbac/v1"
)

var (
	roleUpdateEvents        atomic.Uint64
	roleNoopUpdates         atomic.Uint64
	roleSemanticNoopUpdates atomic.Uint64
)

func RBACRoleUpdateEventsTotal() uint64        { return roleUpdateEvents.Load() }
func RBACRoleNoopUpdatesTotal() uint64         { return roleNoopUpdates.Load() }
func RBACRoleSemanticNoopUpdatesTotal() uint64 { return roleSemanticNoopUpdates.Load() }

// roleUpdateSide is the comparison surface of one side of a role UPDATE.
// kind == "" means the object was neither a ClusterRole nor a Role.
type roleUpdateSide struct {
	kind  string
	rv    string
	rules []rbacv1.PolicyRule
}

func roleSideOf(obj interface{}) roleUpdateSide {
	if o, ok := asCR(obj); ok {
		return roleUpdateSide{kind: "ClusterRole", rv: o.ResourceVersion, rules: o.Rules}
	}
	if o, ok := asRole(obj); ok {
		return roleUpdateSide{kind: "Role", rv: o.ResourceVersion, rules: o.Rules}
	}
	return roleUpdateSide{}
}

// recordRoleUpdateNoop classifies ONE role UPDATE event (mirrors
// recordBindingUpdateNoop). Called from onRoleUpdated.
func recordRoleUpdateNoop(oldObj, newObj interface{}) {
	oldSide := roleSideOf(oldObj)
	newSide := roleSideOf(newObj)
	if oldSide.kind == "" || newSide.kind == "" || oldSide.kind != newSide.kind {
		return
	}
	roleUpdateEvents.Add(1)
	if oldSide.rv != "" && oldSide.rv == newSide.rv {
		roleNoopUpdates.Add(1)
		roleSemanticNoopUpdates.Add(1)
		return
	}
	if policyRulesEqual(oldSide.rules, newSide.rules) {
		roleSemanticNoopUpdates.Add(1)
	}
}

// ResetRoleNoopCountersForTest zeroes the three counters. TEST-ONLY.
func ResetRoleNoopCountersForTest() {
	roleUpdateEvents.Store(0)
	roleNoopUpdates.Store(0)
	roleSemanticNoopUpdates.Store(0)
}
