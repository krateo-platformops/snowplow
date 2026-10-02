// rbac_shift_hook.go — #258. Cache-side observer registry that fires when an
// RBAC sub-generation flush rotates a set of subjects' keys, so the dispatchers
// prewarm engine can scope-reseed the affected cohorts' NEW keys before the next
// navigation (the zero-cold-navigations rule on a warranted rotation).
//
// WHY A REGISTRY (NOT A DIRECT CALL). Same one-way import graph as
// gvr_discovered_hook.go: dispatchers → cache; cache cannot import dispatchers
// without a cycle. The dispatchers-side engine subscribes once at boot
// (registerEngineRBACShiftHook, before its worker spawns); the cache fires this
// SYNCHRONOUSLY from flushPendingSubGenBumps — the sub-gen publish-barrier drain
// point (rbac_subgen_pending.go), AFTER BumpSubjectSubGens so the counters are
// already incremented and the consumer's reseed mints the NEW sub-gen. MIRRORS
// RegisterGVRDiscoveredHook exactly (idempotent fn-pointer dedup,
// snapshot-under-lock + fire-unlocked, non-blocking-callback contract).
//
// SOURCE TAG (#260). The payload carries each rotated subject's OR'd
// subGenBumpSource mask so the consumer can split the reseed path: a NARROWING
// rotation (binding/role DELETE only) adds no topology → the engine reuses the
// harvester snapshot and re-keys the existing targets; a WIDENING rotation
// (binding/role ADD, or any UPDATE — which may re-route to new access, so
// unclassifiable UPDATEs fail toward WIDENING = warm) can make new widgets/
// namespaces visible → the engine does a per-subject scoped nav walk so the
// newly-visible cells are not left cold.

package cache

import (
	"reflect"
	"sync"
)

// wideningSources is the set of mask bits that mark a rotation as WIDENING — one
// that can EXPAND a subject's visible topology, so a snapshot reuse would miss
// newly-visible cells and the reseed must do a per-subject scoped WALK. ADDs
// inherently widen (a new binding/role grants new access). UPDATEs are NOT
// classified by event type — a plain bumpSrcBindingUpdate/bumpSrcRoleUpdate is
// the relist-storm / re-delivery shape and must stay NON-widening (snapshot
// re-key); the delta site content-classifies the genuinely-widening updates
// (binding gained-subjects/roleRef-change, non-semantic-noop role update) and
// OR's in the bumpSrcWiden TAG. Pure DELETEs only remove access → never widen.
// Single source for the widening predicate.
const wideningSources = bumpSrcBindingAdd | bumpSrcRoleAdd | bumpSrcWiden

// RotatedSubject is the exported subjectKey form, used by NotifyRBACShiftForTest
// to construct a rotated set from another package without the internal type.
// Kind is the rbac/v1 subject kind ("User" | "Group" | "ServiceAccount");
// Namespace is set only for a ServiceAccount; Widening selects the source mask
// the shim stamps (a widening ADD vs a narrowing DELETE) so a test can drive
// either reseed path.
type RotatedSubject struct {
	Kind      string
	Name      string
	Namespace string
	Widening  bool
}

// RotatedSubjectSet is the read-only payload carried to an RBAC-shift hook: the
// subjects whose sub-gen counter was bumped in ONE flush, each mapped to its
// OR'd source mask. subjectKey is cache-internal, so the set is opaque and
// exposes exactly the two membership tests the consumer needs. Keeping the
// subject→identity mapping HERE — mirroring RBACSubGenForSubject — makes it the
// single source shared with the key fold, so the reseed-reachability predicate
// can never drift from the serve-key derivation (the #64/#66 anti-shadow-drift
// rule).
type RotatedSubjectSet struct {
	set map[subjectKey]subGenBumpSource
}

// foldSubjects returns the subjectKeys an identity (username + presented groups)
// folds into its effective RBAC sub-gen — mirroring RBACSubGenForSubject EXACTLY
// (a ServiceAccount username folds the SA subject, a human username the User
// subject, every non-empty group its Group subject). Shared by Rotated/Widening
// so both tests use the identical mapping the key fold uses.
func foldSubjects(username string, groups []string) []subjectKey {
	out := make([]subjectKey, 0, 1+len(groups))
	if username != "" {
		if ns, name, ok := parseServiceAccountUsername(username); ok {
			out = append(out, subjectKey{Kind: subjectKindServiceAccount, Name: name, Namespace: ns})
		} else {
			out = append(out, subjectKey{Kind: subjectKindUser, Name: username})
		}
	}
	for _, g := range groups {
		if g == "" {
			continue
		}
		out = append(out, subjectKey{Kind: subjectKindGroup, Name: g})
	}
	return out
}

// Rotated reports whether the identity had its effective RBAC sub-gen rotated in
// this flush — i.e. whether any of its folded subjects is in the rotated set. A
// co-bound identity whose own subjects did not move returns false (#258
// falsifier b — precision).
func (r RotatedSubjectSet) Rotated(username string, groups []string) bool {
	if len(r.set) == 0 {
		return false
	}
	for _, s := range foldSubjects(username, groups) {
		if _, hit := r.set[s]; hit {
			return true
		}
	}
	return false
}

// Widening reports whether the identity's rotation includes a WIDENING source on
// at least one of its folded subjects — so the reseed must do a per-subject
// scoped nav walk (new topology possible) rather than reuse the snapshot. A
// delete-only rotation returns false (snapshot-reuse is safe). False for an
// identity that did not rotate at all.
func (r RotatedSubjectSet) Widening(username string, groups []string) bool {
	if len(r.set) == 0 {
		return false
	}
	for _, s := range foldSubjects(username, groups) {
		if mask, hit := r.set[s]; hit && mask&wideningSources != 0 {
			return true
		}
	}
	return false
}

// Len reports the number of rotated subjects (telemetry + empty-flush guard).
func (r RotatedSubjectSet) Len() int { return len(r.set) }

// rbacShiftHooks is the package-level registry, guarded by mu. Dual storage
// (hooks slice + pointer set) gives O(1) idempotent registration; mirrors
// gvrDiscoveredHooks.
var rbacShiftHooks struct {
	mu       sync.Mutex
	hooks    []func(RotatedSubjectSet)
	pointers map[uintptr]struct{}
}

// RegisterRBACShiftHook adds a callback fired when a sub-gen flush rotates one or
// more subjects. The callback runs SYNCHRONOUSLY on the flush goroutine (the
// rebuildRBACSnapshot publish path) — it MUST NOT block (the dispatchers handler
// only merges into an accumulator + an O(1) enqueueScope). Idempotent on the fn
// pointer. Mirrors RegisterGVRDiscoveredHook.
func RegisterRBACShiftHook(fn func(RotatedSubjectSet)) {
	if fn == nil {
		return
	}
	ptr := reflect.ValueOf(fn).Pointer()
	rbacShiftHooks.mu.Lock()
	defer rbacShiftHooks.mu.Unlock()
	if rbacShiftHooks.pointers == nil {
		rbacShiftHooks.pointers = map[uintptr]struct{}{}
	}
	if _, dup := rbacShiftHooks.pointers[ptr]; dup {
		return
	}
	rbacShiftHooks.pointers[ptr] = struct{}{}
	rbacShiftHooks.hooks = append(rbacShiftHooks.hooks, fn)
}

// notifyRBACShift fires every registered hook with the rotated-subject set built
// from the flush's parallel (drained, masks) slices. Called from
// flushPendingSubGenBumps AFTER BumpSubjectSubGens. No-op when nothing rotated OR
// no hook is registered (a flush with no consumer costs nothing; the set is
// built only when a hook will read it). Snapshot-under-lock + fire-unlocked.
func notifyRBACShift(drained []subjectKey, masks []subGenBumpSource) {
	if len(drained) == 0 {
		return
	}
	rbacShiftHooks.mu.Lock()
	hooks := append([]func(RotatedSubjectSet){}, rbacShiftHooks.hooks...)
	rbacShiftHooks.mu.Unlock()
	if len(hooks) == 0 {
		return
	}
	set := make(map[subjectKey]subGenBumpSource, len(drained))
	for i, s := range drained {
		var m subGenBumpSource
		if i < len(masks) {
			m = masks[i]
		}
		set[s] |= m
	}
	rs := RotatedSubjectSet{set: set}
	for _, fn := range hooks {
		fn(rs)
	}
}

// ResetRBACShiftHooksForTest clears the registry. TEST-ONLY — the production
// registry is append-only (boot-time wiring).
func ResetRBACShiftHooksForTest() {
	rbacShiftHooks.mu.Lock()
	rbacShiftHooks.hooks = nil
	rbacShiftHooks.pointers = nil
	rbacShiftHooks.mu.Unlock()
}

// NotifyRBACShiftForTest fires the hook chain with an explicit rotated set, for
// cross-package tests (internal/handlers/dispatchers) that drive the cache→engine
// reseed without standing up a full RBAC snapshot rebuild. Each RotatedSubject's
// Widening bit selects a representative widening (ADD) or narrowing (DELETE)
// source mask. Mirrors NotifyGVRDiscoveredForReprewarmTest.
func NotifyRBACShiftForTest(rotated []RotatedSubject) {
	set := make(map[subjectKey]subGenBumpSource, len(rotated))
	for _, r := range rotated {
		mask := bumpSrcBindingDelete
		if r.Widening {
			mask = bumpSrcBindingAdd
		}
		set[subjectKey{Kind: r.Kind, Name: r.Name, Namespace: r.Namespace}] |= mask
	}
	rbacShiftHooks.mu.Lock()
	hooks := append([]func(RotatedSubjectSet){}, rbacShiftHooks.hooks...)
	rbacShiftHooks.mu.Unlock()
	rs := RotatedSubjectSet{set: set}
	for _, fn := range hooks {
		fn(rs)
	}
}
