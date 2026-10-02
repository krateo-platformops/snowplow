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
// ONE PATH for every rotation. The payload is just the set of rotated subjects;
// the consumer reseeds every resident (unit × identity) target whose folded
// subject is in it. Narrowing and widening rotations take the same path: the
// identities are read from the live binding index at reseed time, so a subject
// newly authorized for a resident unit is already a target (Rotated covers it).

package cache

import (
	"reflect"
	"sync"
)

// RotatedSubject is the exported subjectKey form, used by NotifyRBACShiftForTest
// to construct a rotated set from another package without the internal type.
// Kind is the rbac/v1 subject kind ("User" | "Group" | "ServiceAccount");
// Namespace is set only for a ServiceAccount.
type RotatedSubject struct {
	Kind      string
	Name      string
	Namespace string
}

// RotatedSubjectSet is the read-only payload carried to an RBAC-shift hook: the
// subjects whose sub-gen counter was bumped in ONE flush. subjectKey is
// cache-internal, so the set is opaque and exposes the one membership test the
// consumer needs. Keeping the subject→identity mapping HERE — mirroring
// RBACSubGenForSubject — makes it the single source shared with the key fold, so
// the reseed-reachability predicate can never drift from the serve-key derivation
// (the #64/#66 anti-shadow-drift rule).
type RotatedSubjectSet struct {
	set map[subjectKey]struct{}
}

// foldSubjects returns the subjectKeys an identity (username + presented groups)
// folds into its effective RBAC sub-gen — mirroring RBACSubGenForSubject EXACTLY
// (a ServiceAccount username folds the SA subject, a human username the User
// subject, every non-empty group its Group subject).
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

// notifyRBACShift fires every registered hook with the flush's rotated subjects.
// Called from flushPendingSubGenBumps AFTER BumpSubjectSubGens. No-op when nothing
// rotated OR no hook is registered (a flush with no consumer costs nothing; the
// set is built only when a hook will read it). Snapshot-under-lock + fire-unlocked.
func notifyRBACShift(drained []subjectKey) {
	if len(drained) == 0 {
		return
	}
	set := make(map[subjectKey]struct{}, len(drained))
	for _, s := range drained {
		set[s] = struct{}{}
	}
	fireRBACShift(RotatedSubjectSet{set: set})
}

func fireRBACShift(rs RotatedSubjectSet) {
	rbacShiftHooks.mu.Lock()
	hooks := append([]func(RotatedSubjectSet){}, rbacShiftHooks.hooks...)
	rbacShiftHooks.mu.Unlock()
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
// reseed without standing up a full RBAC snapshot rebuild. Mirrors
// NotifyGVRDiscoveredForReprewarmTest.
func NotifyRBACShiftForTest(rotated []RotatedSubject) {
	set := make(map[subjectKey]struct{}, len(rotated))
	for _, r := range rotated {
		set[subjectKey{Kind: r.Kind, Name: r.Name, Namespace: r.Namespace}] = struct{}{}
	}
	fireRBACShift(RotatedSubjectSet{set: set})
}
