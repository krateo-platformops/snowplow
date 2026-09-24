// rbac_binding_noop_counters.go — #247: how many binding UPDATE events rotate
// the L1 key without changing anything that matters.
//
// THE TRACED PATH. onBindingUpdate (bindings_by_gvr_delta.go:155) calls
// recordPendingSubGenBumps for the OLD subjects and again for the NEW subjects,
// with NO comparison between them — no resourceVersion check, no subject-set
// diff, no roleRef diff. Every UPDATE on a (Cluster)RoleBinding therefore
// rotates the cache key for every subject of that binding, whether or not
// anything actually changed.
//
// WHY THAT MIGHT BE THE BULK OF THE ROTATION. The informers run resyncPeriod=0
// (watcher.go), so there is no periodic resync — but a watch RE-ESTABLISHMENT
// produces a DeltaFIFO Replace, whose Sync deltas the shared informer dispatches
// as OnUpdate with old and new being the SAME object. One relist therefore
// delivers one no-op UPDATE per binding. Against a few hundred bindings and
// genuine binding creations running at tens per day, relist fan-out is the
// candidate explanation for a rotation rate three orders of magnitude above the
// creation rate. That is a HYPOTHESIS; these counters are what makes it
// measurable instead of argued.
//
// THIS FILE CHANGES NO BEHAVIOUR. Every bump still fires exactly as before,
// including on both no-op paths. The number comes first; skipping a bump is a
// separate, gated change.
//
// # THE TWO COUNTERS AND WHY NEITHER IS SUFFICIENT ALONE
//
//   - snowplow_rbac_binding_noop_updates_total — old and new carry the SAME
//     per-object metadata.resourceVersion. This is RELIST FAN-OUT and nothing
//     else. A per-object resourceVersion is the etcd modRevision: it changes
//     only when THAT object is written. Two independent LISTs return
//     byte-identical per-item resourceVersions (verified on 057 against the
//     apiserver); it is the COLLECTION resourceVersion in ListMeta that
//     advances, not the items'. So same-RV on an UPDATE means the object was
//     redelivered, not rewritten.
//
//   - snowplow_rbac_binding_semantic_noop_updates_total — the subject set and
//     the roleRef are both unchanged, REGARDLESS of resourceVersion. This is a
//     strict SUPERSET of the first: an identical RV implies identical content,
//     so every same-RV update is also a semantic no-op and increments both.
//
// The quantity that decides the eventual fix is the DIFFERENCE between them:
// updates that genuinely rewrote the object but touched nothing RBAC-relevant
// (label churn, annotation churn, a controller rewriting metadata). Neither
// counter alone yields it. If the difference is ~0 the fix keys on
// resourceVersion; if it is large the fix must key on semantics, because an
// RV check would miss most of the waste.
//
// Deliberately NOT a whole-object comparison: label and annotation churn is
// exactly the traffic the second counter exists to SEE, so folding metadata
// into the comparison would defeat it.

package cache

import (
	"slices"
	"strings"
	"sync/atomic"

	rbacv1 "k8s.io/api/rbac/v1"
)

var (
	// bindingNoopUpdates counts UPDATE events whose old and new objects carry
	// the same non-empty per-object resourceVersion — relist fan-out.
	bindingNoopUpdates atomic.Uint64

	// bindingSemanticNoopUpdates counts UPDATE events whose subject set and
	// roleRef are both unchanged, at any resourceVersion.
	bindingSemanticNoopUpdates atomic.Uint64
)

// RBACBindingNoopUpdatesTotal returns the cumulative count of (Cluster)RoleBinding
// UPDATE events redelivering an object at the SAME resourceVersion — the
// relist-fan-out signal.
//
// READING THE ZERO — IT IS NARROWER THAN IT LOOKS. 0 means "no relists", NOT
// "no wasted rotation". Same-RV catches watch re-establishment and nothing
// else: a label or annotation write changes the resourceVersion and passes
// straight through to the bump while this counter stays flat.
// bindingSemanticNoopUpdates is the counter that sees those; read the pair,
// never this one alone. This counter also has no liveness arm proving it can
// move in production (unlike RBACSubGenBumpsTotal), so a zero here has not yet
// earned the reading "no relists occurred" rather than "not looking".
//
// STRUCTURALLY BLIND TO CREATES AND DELETES. onBindingAdd
// (bindings_by_gvr_delta.go:131) and onBindingDelete (:202) bump
// unconditionally, and an ADD has no "old" side to compare against — so NO
// no-op counter can cover them, by construction rather than by omission. This
// matters for where the numbers get read: the production workload is binding
// CREATION (composition installs, 1000 users / 50K compositions), while a
// fixed-size test cluster's binding count never grows. A quiet
// noop_updates_total on such a cluster is evidence about relists there and is
// NOT evidence about production rotation.
func RBACBindingNoopUpdatesTotal() uint64 { return bindingNoopUpdates.Load() }

// RBACBindingSemanticNoopUpdatesTotal returns the cumulative count of
// (Cluster)RoleBinding UPDATE events whose subjects and roleRef were both
// unchanged — every bump that rotated keys for nothing RBAC-relevant. A
// superset of RBACBindingNoopUpdatesTotal.
//
// READING THE ZERO: 0 means every UPDATE seen so far genuinely changed a
// subject set or a roleRef. Like its sibling it carries no liveness arm, and it
// shares the create/delete blindness described on RBACBindingNoopUpdatesTotal —
// an UPDATE-only counter says nothing about a workload dominated by binding
// CREATION. The quantity worth charting is this MINUS
// RBACBindingNoopUpdatesTotal: rewrites that touched nothing RBAC-relevant.
func RBACBindingSemanticNoopUpdatesTotal() uint64 { return bindingSemanticNoopUpdates.Load() }

// bindingUpdateSide is the comparison surface of ONE side of an UPDATE event,
// captured in onBindingUpdate as it normalises the object it was already
// normalising. kind == "" means the object was neither typed nor convertible
// via the asCRB/asRB fallbacks, i.e. this side was dropped to the drift canary.
//
// The subjects slice is the SAME one handed to recordPendingSubGenBumps — held,
// never mutated (the comparison sorts copies), so capturing it costs nothing
// beyond the allocation the hook already made.
type bindingUpdateSide struct {
	kind      string // bindingKindCRB / bindingKindRB; "" = not normalisable
	rv        string // metadata.resourceVersion (the etcd modRevision)
	namespace string // "" for a ClusterRoleBinding
	roleRef   rbacv1.RoleRef
	subjects  []subjectKey
}

// The two binding kinds a side can normalise to. Compared for EQUALITY across
// old and new, so a pathological event whose sides normalise to different kinds
// is counted as neither kind of no-op (see recordBindingUpdateNoop).
const (
	bindingKindCRB = "ClusterRoleBinding"
	bindingKindRB  = "RoleBinding"
)

// recordBindingUpdateNoop classifies ONE UPDATE event and bumps whichever
// counters it qualifies for. Called exactly once per onBindingUpdate call —
// after both the old and the new branch have run — so an event is counted once,
// not once per branch, and the count is unaffected by which of the typed
// assert / convertUnstructured fallbacks produced each side.
//
// MIXED OR MISSING KINDS ARE COUNTED AS NEITHER. If either side failed to
// normalise, or the two sides normalised to DIFFERENT kinds, there is no
// meaningful old-vs-new comparison to make and a guess either way would
// contaminate the very number this exists to produce. Those events are already
// visible: each unnormalisable side bumped the drift canary
// (snowplow_bindings_by_gvr_delta_skipped_non_typed) in its own branch.
func recordBindingUpdateNoop(oldSide, newSide bindingUpdateSide) {
	if oldSide.kind == "" || newSide.kind == "" || oldSide.kind != newSide.kind {
		return
	}

	// Same per-object resourceVersion ⇒ the object was redelivered, not
	// rewritten ⇒ byte-identical content ⇒ necessarily ALSO a semantic no-op.
	// Bumping both here is what keeps semantic_noop a true superset, and it
	// lets the high-volume relist path skip the subject comparison entirely.
	//
	// The non-empty guard matters: the apiserver always stamps a
	// resourceVersion, so two empty RVs mean a fabricated object, never a
	// relist. Counting those would let a synthetic event masquerade as
	// watch instability.
	if oldSide.rv != "" && oldSide.rv == newSide.rv {
		bindingNoopUpdates.Add(1)
		bindingSemanticNoopUpdates.Add(1)
		return
	}

	// Different RV: the object really was rewritten. Did the rewrite touch
	// anything the sub-generation bump exists to react to?
	//
	// The namespace is compared alongside the roleRef because a "Role" roleRef
	// is namespace-scoped — the same discrimination roleRefKey makes by folding
	// the namespace into the key for Role refs. Comparing the RoleRef struct
	// directly (APIGroup + Kind + Name, all comparable strings) rather than via
	// roleRefKey is deliberate: roleRefKey collapses every unrecognised Kind to
	// "", which would make two DIFFERENT malformed roleRefs compare equal and
	// over-report a no-op.
	if oldSide.namespace == newSide.namespace &&
		oldSide.roleRef == newSide.roleRef &&
		subjectSetsEqual(oldSide.subjects, newSide.subjects) {
		bindingSemanticNoopUpdates.Add(1)
	}
}

// subjectSetsEqual reports whether two normalised subject lists carry the same
// members, ORDER-INSENSITIVELY. The apiserver does not promise subject order is
// stable across writes, so a positional comparison would report a reordered but
// unchanged subject list as a genuine change — the failure mode most likely to
// silently deflate the semantic-no-op count.
//
// Multiset, not set: [A,A] and [A] are reported DIFFERENT. Duplicate subject
// entries are vanishingly rare and the effective grant is the same either way,
// so this is the conservative direction — it can only UNDER-report a no-op, and
// over-reporting is the error that would mislead the fix decision.
//
// The inputs are the live slices the caller handed to recordPendingSubGenBumps,
// so both are cloned before sorting; nothing observable is reordered.
func subjectSetsEqual(a, b []subjectKey) bool {
	if len(a) != len(b) {
		return false
	}
	if len(a) == 0 {
		return true
	}
	as := slices.Clone(a)
	bs := slices.Clone(b)
	slices.SortFunc(as, compareSubjectKey)
	slices.SortFunc(bs, compareSubjectKey)
	return slices.Equal(as, bs)
}

// compareSubjectKey orders subjectKeys totally over their three fields. Only
// the ordering's determinism matters, not its meaning.
func compareSubjectKey(x, y subjectKey) int {
	if c := strings.Compare(x.Kind, y.Kind); c != 0 {
		return c
	}
	if c := strings.Compare(x.Name, y.Name); c != 0 {
		return c
	}
	return strings.Compare(x.Namespace, y.Namespace)
}

// ResetBindingNoopCountersForTest zeroes both counters. TEST-ONLY — production
// never resets (they are monotonic for the process lifetime).
func ResetBindingNoopCountersForTest() {
	bindingNoopUpdates.Store(0)
	bindingSemanticNoopUpdates.Store(0)
}
