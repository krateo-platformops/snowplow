// rbac_dangling_roleref.go — #947.
//
// A ClusterRoleBinding or RoleBinding whose roleRef names a role that does
// not exist is a valid Kubernetes state: the binding grants nothing until the
// role appears. Before #947 the evaluator logged a WARN
// (rbac.evaluate.snapshot.miss) on EVERY evaluation that reached such a
// binding, because denies are never memoized (#301). On one cluster that was
// 392M rows, up to ~1,500 lines/s, and it filled the log store.
//
// The state lives in the snapshot, so it is reported where it changes: at each
// publish of a Synced snapshot the writer computes the set of dangling
// bindings and compares it with the set of the previous Synced publish.
//   - A binding that enters the set logs one `dangling_roleref` line.
//   - A binding that leaves it (the role appeared, or the binding was changed
//     or deleted) logs one `dangling_roleref_resolved` line.
//   - The current count, by roleRef kind, is the expvar
//     snowplow_rbac_dangling_roleref.
//
// The lines name the binding and the role, never the binding's subjects. The
// gauge is keyed by roleRef kind only, never by any name.
//
// Concurrency: the production writer is single (the rebuild slot), so calls
// arrive in publish order. The mutex covers the test-only synchronous rebuild
// (RebuildRBACSnapshotForTest), which runs outside the slot.
package cache

import (
	"log/slog"
	"sync"

	rbacv1 "k8s.io/api/rbac/v1"
)

// danglingRoleRef identifies one binding and the absent role it references.
// The roleRef is part of the key, so a binding that now points at a different
// absent role is one resolution plus one new dangling reference.
type danglingRoleRef struct {
	bindingKind      string // "ClusterRoleBinding" | "RoleBinding"
	bindingNamespace string // "" for a ClusterRoleBinding
	bindingName      string
	roleKind         string // "ClusterRole" | "Role"
	roleName         string
}

// danglingRoleRefsOf returns the bindings of snap whose roleRef names a role
// absent from snap. It applies the evaluator's resolution rules
// (rbac.lookupRoleRefRules): a ClusterRole is looked up by name, a Role in the
// RoleBinding's namespace. A roleRef of any other shape (kind Role in a
// ClusterRoleBinding, an unknown kind) is rejected by the API server and is
// not tracked; the evaluator denies it silently, as before.
//
// Only meaningful on a Synced snapshot. Returns nil when nothing dangles.
func danglingRoleRefsOf(snap *RBACSnapshot) map[danglingRoleRef]struct{} {
	var out map[danglingRoleRef]struct{}
	add := func(k danglingRoleRef) {
		if out == nil {
			out = map[danglingRoleRef]struct{}{}
		}
		out[k] = struct{}{}
	}
	for _, crb := range snap.ClusterRoleBindings {
		if crb == nil || crb.RoleRef.Kind != "ClusterRole" {
			continue
		}
		if _, ok := snap.ClusterRolesByName[crb.RoleRef.Name]; !ok {
			add(danglingRoleRef{
				bindingKind: "ClusterRoleBinding", bindingName: crb.Name,
				roleKind: "ClusterRole", roleName: crb.RoleRef.Name,
			})
		}
	}
	for ns, rbs := range snap.RoleBindingsByNS {
		for _, rb := range rbs {
			if rb == nil || roleRefPresent(snap, ns, rb.RoleRef) {
				continue
			}
			add(danglingRoleRef{
				bindingKind: "RoleBinding", bindingNamespace: ns, bindingName: rb.Name,
				roleKind: rb.RoleRef.Kind, roleName: rb.RoleRef.Name,
			})
		}
	}
	return out
}

// roleRefPresent reports whether the role a binding in namespace ns
// references is in snap. Kinds other than ClusterRole and Role report true:
// they are not tracked as dangling (see danglingRoleRefsOf).
func roleRefPresent(snap *RBACSnapshot, ns string, ref rbacv1.RoleRef) bool {
	switch ref.Kind {
	case "ClusterRole":
		_, ok := snap.ClusterRolesByName[ref.Name]
		return ok
	case "Role":
		_, ok := snap.RolesByNSName[ns+"/"+ref.Name]
		return ok
	default:
		return true
	}
}

// danglingState is the dangling set of the last Synced publish, with its
// per-kind counts for the gauge.
var danglingState struct {
	mu          sync.Mutex
	current     map[danglingRoleRef]struct{}
	clusterRole int
	role        int
}

// noteDanglingRoleRefs records the dangling set of a just-published Synced
// snapshot: one line for each binding that became dangling, one for each that
// stopped, then the new set becomes the current one. A publish that changes
// nothing logs nothing, however many evaluations reach the binding.
func noteDanglingRoleRefs(snap *RBACSnapshot, next map[danglingRoleRef]struct{}) {
	danglingState.mu.Lock()
	defer danglingState.mu.Unlock()

	for k := range next {
		if _, was := danglingState.current[k]; was {
			continue
		}
		slog.Warn("cache.rbac.snapshot.dangling_roleref",
			slog.String("subsystem", "cache"),
			slog.String("binding_kind", k.bindingKind),
			slog.String("binding_namespace", k.bindingNamespace),
			slog.String("binding_name", k.bindingName),
			slog.String("roleref_kind", k.roleKind),
			slog.String("roleref_name", k.roleName),
			slog.Uint64("snap_seq", snap.PublishSeq),
			slog.String("hint", "the binding references a role that does not exist, so it grants nothing; "+
				"Kubernetes allows this. Logged once when it starts and once when it ends; "+
				"the current count is snowplow_rbac_dangling_roleref in /debug/vars"),
		)
	}
	for k := range danglingState.current {
		if _, still := next[k]; still {
			continue
		}
		reason := "binding_changed_or_deleted"
		if roleRefPresent(snap, k.bindingNamespace, rbacv1.RoleRef{Kind: k.roleKind, Name: k.roleName}) {
			reason = "role_present"
		}
		slog.Warn("cache.rbac.snapshot.dangling_roleref_resolved",
			slog.String("subsystem", "cache"),
			slog.String("binding_kind", k.bindingKind),
			slog.String("binding_namespace", k.bindingNamespace),
			slog.String("binding_name", k.bindingName),
			slog.String("roleref_kind", k.roleKind),
			slog.String("roleref_name", k.roleName),
			slog.String("reason", reason),
			slog.Uint64("snap_seq", snap.PublishSeq),
		)
	}

	danglingState.current = next
	danglingState.clusterRole, danglingState.role = 0, 0
	for k := range next {
		switch k.roleKind {
		case "ClusterRole":
			danglingState.clusterRole++
		case "Role":
			danglingState.role++
		}
	}
}

// RBACDanglingRoleRefs returns how many bindings of the last Synced snapshot
// reference an absent role, split by the kind of the referenced role. Backs
// the snowplow_rbac_dangling_roleref expvar.
func RBACDanglingRoleRefs() (clusterRole, role int) {
	danglingState.mu.Lock()
	defer danglingState.mu.Unlock()
	return danglingState.clusterRole, danglingState.role
}

// rbacDanglingRoleRefExpvar is the snowplow_rbac_dangling_roleref value: the
// gauge keyed by roleRef kind, both keys always present.
func rbacDanglingRoleRefExpvar() any {
	cr, r := RBACDanglingRoleRefs()
	return map[string]int{"ClusterRole": cr, "Role": r}
}

// ResetDanglingRoleRefsForTest clears the dangling set, so the next Synced
// publish reports every dangling binding it holds. The state is
// process-global, like the snapshot. TEST-ONLY.
func ResetDanglingRoleRefsForTest() {
	danglingState.mu.Lock()
	defer danglingState.mu.Unlock()
	danglingState.current = nil
	danglingState.clusterRole, danglingState.role = 0, 0
}
