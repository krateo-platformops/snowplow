// bindings_by_gvr_delta.go — Ship 1: the incremental delta hooks that
// keep the BindingsByGVR index current on RBAC informer events.
//
// These hooks ride the SAME RBAC informer events that drive
// scheduleRBACRebuild (rbacSnapshotEventHandlers, rbac_snapshot.go:804).
// They are O(navigatedGVRs) per event (Gate-2: ~5.8 µs/event,
// 0.0027% CPU at 4.6/s) — NOT the rejected wholesale rebuild (~265 ms,
// ~122% CPU).
//
// EVENT → DELTA mapping (mirrors deps_watch.go's per-event delta shape):
//
//   - ADD (binding created):     onBindingAdd → resolve roleRef rules,
//                                 enrol the new binding.
//   - UPDATE (binding edited):   onBindingUpdate(old,new) → unrol old,
//                                 enrol new. A subject-list or roleRef
//                                 edit changes the binding's bucket
//                                 membership; unrol+enrol re-derives it.
//   - DELETE (binding removed):  onBindingDelete → unrol the binding.
//
// ROLE-RULE CHANGE — a ClusterRole/Role rule edit changes the rules of
// every binding whose roleRef points at it, so those bindings' bucket
// membership may change. onRoleRulesChanged re-routes every binding in
// the byRole reverse map for that role (Gate-2 worst case: the
// most-referenced role had 4 referencing bindings → 831 ns). This is the
// rare path; binding churn is the bulk of the event rate.
//
// CONCURRENCY — every hook takes the index write lock for the duration of
// its (few) map ops. The hooks run on the RBAC informer processor
// goroutine (same as the existing snapshot/dep handlers); the lock hold
// is O(navigatedGVRs) map ops, well inside the non-blocking-handler
// budget. The role-resolution reads (rulesForRoleRef) are against the
// IMMUTABLE published snapshot — lock-free against the snapshot.
//
// EVENTUAL CONSISTENCY — the snapshot the hook resolves rules against may
// lag the informer's just-delivered object by one republish window. This
// is fine: the index is seed-targeting only (the authz boundary is the
// per-request EvaluateRBAC over the wholesale snapshot). A transiently
// stale bucket = a transiently over/under-included seed cohort = wasted
// seed / one per-user fallback resolve. Both benign.

package cache

import (
	"log/slog"
	"sync/atomic"

	rbacv1 "k8s.io/api/rbac/v1"
	clientcache "k8s.io/client-go/tools/cache"
)

// bindingsIndexDeltaSkippedNonTyped counts delta events whose object was
// NEITHER the expected typed *rbacv1.* pointer NOR convertible via the
// defensive convertUnstructured* fallback. A non-zero value means the
// typed transform missed (the logged-WARN regression case, rbac_snapshot.go:383)
// AND the conversion fallback also failed — the index dropped that event
// and will DRIFT until the next boot rebuild. S1: surfaced via expvar
// (bindings_by_gvr_metrics.go) so the drift is observable, not silent.
var bindingsIndexDeltaSkippedNonTyped atomic.Uint64

// BindingsIndexDeltaSkippedNonTyped returns the cumulative count of delta
// events the index could not type-assert OR convert — the drift canary.
func BindingsIndexDeltaSkippedNonTyped() uint64 {
	return bindingsIndexDeltaSkippedNonTyped.Load()
}

// deltaActive gates whether the delta hooks do work. The hooks no-op
// until the index has been built once (BuildBindingsByGVRIndex after
// WaitAllInformersSynced) — before the build, the boot enrol covers the
// initial population, so a pre-build event has nothing to delta. After the
// build, every event is a delta. This also keeps the hooks inert under
// cache-off (the RBAC informers are not registered, so events never fire).
func (idx *bindingsByGVRIndex) deltaActive() bool {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.built
}

// asCRB / asRB / asRole normalise an informer event object to the typed
// RBAC pointer: a direct type-assert on the happy path, else the
// defensive convertUnstructured* fallback (the same fallback
// rebuildRBACSnapshot uses for a transform-missed entry). Returns
// (nil,false) only when BOTH fail — the caller bumps the drift canary.
// S1: without the fallback a transform-missed event would silently drop
// from the index.
func asCRB(obj interface{}) (*rbacv1.ClusterRoleBinding, bool) {
	if o, ok := obj.(*rbacv1.ClusterRoleBinding); ok {
		return o, true
	}
	return convertUnstructuredCRB(obj)
}

func asRB(obj interface{}) (*rbacv1.RoleBinding, bool) {
	if o, ok := obj.(*rbacv1.RoleBinding); ok {
		return o, true
	}
	return convertUnstructuredRB(obj)
}

func asCR(obj interface{}) (*rbacv1.ClusterRole, bool) {
	if o, ok := obj.(*rbacv1.ClusterRole); ok {
		return o, true
	}
	return convertUnstructuredCR(obj)
}

func asRole(obj interface{}) (*rbacv1.Role, bool) {
	if o, ok := obj.(*rbacv1.Role); ok {
		return o, true
	}
	return convertUnstructuredR(obj)
}

// deltaDropNonTyped bumps the drift canary + logs once-per-call. Called
// when a binding/role event object is neither typed nor convertible.
func deltaDropNonTyped(kind string) {
	bindingsIndexDeltaSkippedNonTyped.Add(1)
	slog.Warn("cache.bindings_by_gvr.delta_skipped_non_typed",
		slog.String("subsystem", "cache"),
		slog.String("kind", kind),
		slog.String("effect", "delta event object neither typed nor convertible — "+
			"index drops this event and will drift until next boot rebuild; "+
			"see snowplow_bindings_by_gvr_delta_skipped_non_typed at /debug/vars"),
	)
}

// onBindingAdd enrols a newly-created (Cluster)RoleBinding. isBinding has
// already been decided by the caller via gvr, so a CRB event arrives here
// for the CRB GVR and an RB event for the RB GVR — but the wire object may
// be either kind under a future shared handler, so we try both
// normalisers. Falls back to convertUnstructured* (S1) before dropping.
func onBindingAdd(obj interface{}) {
	idx := bindingsByGVRSingleton()
	if !idx.deltaActive() {
		return
	}
	if o, ok := asCRB(obj); ok {
		subj := subjectsFromRBAC(o.Subjects)
		idx.applyBindingAdd("", o.RoleRef, crbBindingID(o), subj)
		recordPendingSubGenBumps(subj) // #118 (c)-v2 GAP-2 — defer the bump to snapshot-publish; this binding's subjects' effective RBAC changed
		return
	}
	if o, ok := asRB(obj); ok {
		subj := subjectsFromRBAC(o.Subjects)
		idx.applyBindingAdd(o.Namespace, o.RoleRef, rbBindingID(o), subj)
		recordPendingSubGenBumps(subj) // #118 (c)-v2 GAP-2
		return
	}
	deltaDropNonTyped("RoleBinding/ClusterRoleBinding(add)")
}

// onBindingUpdate re-derives a binding's bucket membership: unrol the OLD
// binding, enrol the NEW. The informer hands old,new (we wire UpdateFunc
// to pass both — see rbacSnapshotEventHandlers). A subject-list edit or a
// roleRef change both flow through unrol(old)+enrol(new).
//
// #247 INSTRUMENT (no behaviour change): the bumps below fire unconditionally,
// with NO old-vs-new comparison — so every UPDATE rotates the cache key for
// every subject of the binding, including an UPDATE that changed nothing. That
// includes the Sync deltas a watch re-establishment dispatches as OnUpdate with
// old == new. Each branch therefore also records what it normalised into a
// bindingUpdateSide, and recordBindingUpdateNoop classifies the event ONCE at
// the end. It only counts; skipping a bump is a separate, gated change.
func onBindingUpdate(oldObj, newObj interface{}) {
	idx := bindingsByGVRSingleton()
	if !idx.deltaActive() {
		return
	}
	var oldSide, newSide bindingUpdateSide
	if o, ok := asCRB(oldObj); ok {
		subj := subjectsFromRBAC(o.Subjects)
		idx.applyBindingDelete(crbBindingID(o), roleRefKey("", o.RoleRef))
		recordPendingSubGenBumps(subj) // #118 (c)-v2 GAP-2 — OLD subjects lost this grant
		oldSide = bindingUpdateSide{kind: bindingKindCRB, rv: o.ResourceVersion, roleRef: o.RoleRef, subjects: subj}
	} else if o, ok := asRB(oldObj); ok {
		subj := subjectsFromRBAC(o.Subjects)
		idx.applyBindingDelete(rbBindingID(o), roleRefKey(o.Namespace, o.RoleRef))
		recordPendingSubGenBumps(subj) // #118 (c)-v2 GAP-2
		oldSide = bindingUpdateSide{kind: bindingKindRB, rv: o.ResourceVersion, namespace: o.Namespace, roleRef: o.RoleRef, subjects: subj}
	} else {
		deltaDropNonTyped("RoleBinding/ClusterRoleBinding(update-old)")
	}
	if o, ok := asCRB(newObj); ok {
		subj := subjectsFromRBAC(o.Subjects)
		idx.applyBindingAdd("", o.RoleRef, crbBindingID(o), subj)
		recordPendingSubGenBumps(subj) // #118 (c)-v2 GAP-2 — NEW subjects gained this grant (a subject in BOTH old+new is deduped by the pending set; the key only needs to change once)
		newSide = bindingUpdateSide{kind: bindingKindCRB, rv: o.ResourceVersion, roleRef: o.RoleRef, subjects: subj}
	} else if o, ok := asRB(newObj); ok {
		subj := subjectsFromRBAC(o.Subjects)
		idx.applyBindingAdd(o.Namespace, o.RoleRef, rbBindingID(o), subj)
		recordPendingSubGenBumps(subj) // #118 (c)-v2 GAP-2
		newSide = bindingUpdateSide{kind: bindingKindRB, rv: o.ResourceVersion, namespace: o.Namespace, roleRef: o.RoleRef, subjects: subj}
	} else {
		deltaDropNonTyped("RoleBinding/ClusterRoleBinding(update-new)")
	}
	recordBindingUpdateNoop(oldSide, newSide)
}

// onBindingDelete unrols a removed (Cluster)RoleBinding. Unwraps a
// DeletedFinalStateUnknown tombstone (the informer wraps the last-known
// object when it missed the explicit DELETE) — same pattern as
// depEventHandlers.DeleteFunc.
func onBindingDelete(obj interface{}) {
	idx := bindingsByGVRSingleton()
	if !idx.deltaActive() {
		return
	}
	if tomb, ok := obj.(clientcache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	if o, ok := asCRB(obj); ok {
		idx.applyBindingDelete(crbBindingID(o), roleRefKey("", o.RoleRef))
		recordPendingSubGenBumps(subjectsFromRBAC(o.Subjects)) // #118 (c)-v2 GAP-2 — subjects lost this grant (REVOKE — the security-load-bearing arm)
		return
	}
	if o, ok := asRB(obj); ok {
		idx.applyBindingDelete(rbBindingID(o), roleRefKey(o.Namespace, o.RoleRef))
		recordPendingSubGenBumps(subjectsFromRBAC(o.Subjects)) // #118 (c)-v2 GAP-2
		return
	}
	deltaDropNonTyped("RoleBinding/ClusterRoleBinding(delete)")
}

// onRoleObjectChanged extracts the role identity + rules from a
// ClusterRole/Role event object (or its tombstone/Unstructured fallback)
// and re-routes every binding referencing it. The rules come from the
// EVENT OBJECT directly — NOT rbacSnap.Load() — because the wholesale
// snapshot rebuild is async and may still carry the OLD rules when this
// hook runs. A DELETE delivers the role with its last-known rules; we
// re-route against those, but since the role is gone the next binding
// event / build will re-resolve to "no grant".
func onRoleObjectChanged(obj interface{}) {
	idx := bindingsByGVRSingleton()
	if !idx.deltaActive() {
		return
	}
	if tomb, ok := obj.(clientcache.DeletedFinalStateUnknown); ok {
		obj = tomb.Obj
	}
	if o, ok := asCR(obj); ok {
		onRoleRulesChanged("ClusterRole", "", o.Name, o.Rules)
		return
	}
	if o, ok := asRole(obj); ok {
		onRoleRulesChanged("Role", o.Namespace, o.Name, o.Rules)
		return
	}
	deltaDropNonTyped("Role/ClusterRole(change)")
}

// onRoleRulesChanged re-routes every binding referencing the given role
// when that role's rules change (a ClusterRole/Role ADD/UPDATE/DELETE).
// The byRole reverse map gives the referencing binding ids; each is
// unrol'd + re-enrol'd under the role's NEW rules (passed in from the
// event object — NOT the async snapshot). Worst case is O(referencing-
// bindings × navigatedGVRs); the Gate-2 measurement found the
// most-referenced role had only 4 referencing bindings (the topology is
// ~1:1 role:binding from per-composition RBAC).
func onRoleRulesChanged(roleKind, namespace, name string, rules []rbacv1.PolicyRule) {
	idx := bindingsByGVRSingleton()
	if !idx.deltaActive() {
		return
	}
	var rk string
	switch roleKind {
	case "ClusterRole":
		rk = "C/" + name
	case "Role":
		rk = "R/" + namespace + "/" + name
	default:
		return
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()
	set := idx.byRole[rk]
	if len(set) == 0 {
		return
	}
	// Snapshot the ids — re-enrol mutates idx.byRole[rk] in place.
	ids := make([]bindingID, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	// #118 (c)-v2 GAP-1 — a role-rule edit changes the EFFECTIVE access of
	// every subject bound to that role, but (c) v1 never rotated their key
	// (this function re-routed the index but never bumped). Union the subjects
	// across every referencing binding and record them for the deferred
	// publish-time bump (§3.1), so a verb grant/revoke via a Role/ClusterRole
	// rule edit rotates the key exactly like a binding change does. The set
	// dedups a subject bound via multiple referencing bindings.
	changed := make(map[subjectKey]struct{})
	for _, id := range ids {
		entry, present := idx.entries[id]
		if !present {
			continue
		}
		for _, s := range entry.subjects {
			changed[s] = struct{}{}
		}
		// Unrol from GVR + wildcard buckets (keep byRole — the binding still
		// references this role; only its bucket membership changes). Then
		// re-enrol under the role's new rules. Empty rules (role deleted /
		// grants nothing) naturally enrol into no bucket below — the binding
		// stays only in byRole + entries so a future role re-create
		// re-routes it.
		delete(idx.wildcard, id)
		for _, b := range idx.byGVR {
			delete(b, id)
		}
		if rulesGrantWildcard(rules) {
			idx.wildcard[id] = struct{}{}
			continue
		}
		for gr := range idx.navigated {
			if rulesGrantGetList(rules, gr) {
				b := idx.byGVR[gr]
				if b == nil {
					b = map[bindingID]struct{}{}
					idx.byGVR[gr] = b
				}
				b[id] = struct{}{}
			}
		}
	}
	if len(changed) > 0 {
		subjects := make([]subjectKey, 0, len(changed))
		for s := range changed {
			subjects = append(subjects, s)
		}
		// recordPendingSubGenBumps takes a DIFFERENT lock (pendingSubGenBumps.mu)
		// and never acquires idx.mu, so calling it while holding idx.mu here
		// introduces no lock-ordering cycle. Deferred to publish per §3.2.
		recordPendingSubGenBumps(subjects)
	}
}

// applyBindingAdd resolves the binding's roleRef rules against the
// published snapshot then enrols the binding under the write lock.
func (idx *bindingsByGVRIndex) applyBindingAdd(namespace string, ref rbacv1.RoleRef, id bindingID, subjects []subjectKey) {
	snap := rbacSnap.Load()
	rules, ok := rulesForRoleRef(snap, namespace, ref)
	rk := roleRefKey(namespace, ref)

	idx.mu.Lock()
	defer idx.mu.Unlock()
	if !ok {
		// roleRef unresolvable (role not yet in snapshot) — record the
		// entry + byRole membership so a later onRoleRulesChanged for that
		// role can route it once the role appears. No GVR/wildcard
		// enrolment yet.
		idx.entries[id] = bindingEntry{id: id, subjects: subjects}
		if rk != "" {
			set := idx.byRole[rk]
			if set == nil {
				set = map[bindingID]struct{}{}
				idx.byRole[rk] = set
			}
			set[id] = struct{}{}
		}
		return
	}
	idx.enrolLocked(bindingEntry{id: id, subjects: subjects}, rules, rk)
}

// applyBindingDelete unrols the binding under the write lock.
func (idx *bindingsByGVRIndex) applyBindingDelete(id bindingID, rk string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.unrolLocked(id, rk)
}
