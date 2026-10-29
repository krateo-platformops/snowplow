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
// IDENTITY. Binding, role and namespace names often embed a tenant username
// (on one cluster 28 of 423 ClusterRoleBindings, 12/417 RoleBindings, 28/434
// ClusterRoles and 12/414 Roles did). So the lines never carry a name in
// clear: the binding, the role and the namespace are redact labels
// (redact.Prefixed, keyed per process). A label is stable for the process
// lifetime, so the dangling line and the resolved line of one binding carry
// the same labels. The kinds, the reason and snap_seq are logged as they are.
// The binding's subjects are never logged. The gauge is keyed by roleRef kind
// only, never by any name. To find the real objects, the operator lists the
// bindings whose roleRef names a role that does not exist (the hint below and
// docs/architecture/observability.md give the query).
//
// Concurrency: the production writer is single (the rebuild slot), so calls
// arrive in publish order. The mutex covers the test-only synchronous rebuild
// (RebuildRBACSnapshotForTest), which runs outside the slot.
package cache

import (
	"log/slog"
	"sync"

	"github.com/krateo-platformops/snowplow/internal/redact"

	rbacv1 "k8s.io/api/rbac/v1"
)

// danglingRoleRefHint tells the operator how to find the objects behind the
// labels: the gauge says how many, the query lists them.
const danglingRoleRefHint = "the binding references a role that does not exist, so it grants nothing; " +
	"Kubernetes allows this. Logged once when it starts and once when it ends; the current count is " +
	"snowplow_rbac_dangling_roleref in /debug/vars. Names are redact labels: to find the objects, list " +
	"each binding's roleRef (kubectl get clusterrolebindings,rolebindings -A -o custom-columns=" +
	"KIND:.kind,NS:.metadata.namespace,NAME:.metadata.name,ROLEKIND:.roleRef.kind,ROLE:.roleRef.name) " +
	"and keep the rows whose role kubectl get cannot find"

// RBACBindingLabel is the log label of a binding: kind, namespace ("" for a
// ClusterRoleBinding) and name, never in clear. Stable per process.
func RBACBindingLabel(kind, namespace, name string) string {
	return redact.Prefixed("binding", kind+"/"+namespace+"/"+name)
}

// RBACRoleLabel is the log label of a referenced role: kind, namespace (the
// binding's namespace for a Role, "" for a ClusterRole) and name, never in
// clear. Stable per process, and the same label the evaluator's
// rbac.evaluate.snapshot.miss line uses.
func RBACRoleLabel(kind, namespace, name string) string {
	return redact.Prefixed("role", kind+"/"+namespace+"/"+name)
}

// RBACNamespaceLabel is the log label of a namespace; "" stays "" (a
// cluster-scoped object has none).
func RBACNamespaceLabel(namespace string) string {
	if namespace == "" {
		return ""
	}
	return redact.Prefixed("ns", namespace)
}

// roleNamespace is the namespace a roleRef resolves in: the binding's for a
// Role, none for a ClusterRole.
func (k danglingRoleRef) roleNamespace() string {
	if k.roleKind == "Role" {
		return k.bindingNamespace
	}
	return ""
}

// bindingLabel / namespaceLabel / roleLabel are the labels every
// dangling-roleRef line carries in place of the names. Written out at each
// slog call (no opaque attribute slice), so the #453 log guard sees every
// value a line carries.
func (k danglingRoleRef) bindingLabel() string {
	return RBACBindingLabel(k.bindingKind, k.bindingNamespace, k.bindingName)
}
func (k danglingRoleRef) namespaceLabel() string { return RBACNamespaceLabel(k.bindingNamespace) }
func (k danglingRoleRef) roleLabel() string {
	return RBACRoleLabel(k.roleKind, k.roleNamespace(), k.roleName)
}

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
			slog.String("binding", k.bindingLabel()),
			slog.String("binding_namespace", k.namespaceLabel()),
			slog.String("roleref_kind", k.roleKind),
			slog.String("roleref", k.roleLabel()),
			slog.Uint64("snap_seq", snap.PublishSeq),
			slog.String("hint", danglingRoleRefHint),
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
			slog.String("binding", k.bindingLabel()),
			slog.String("binding_namespace", k.namespaceLabel()),
			slog.String("roleref_kind", k.roleKind),
			slog.String("roleref", k.roleLabel()),
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
