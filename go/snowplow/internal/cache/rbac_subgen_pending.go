// rbac_subgen_pending.go — #118 (c)-v2 GAP-2 fix: defer the per-subject
// sub-generation bump to snapshot-publish, closing the bump-vs-publish
// ordering race that (c) v1 left open.
//
// THE RACE (c) v1 LEFT OPEN
// (docs/118-cv2-rbac-ordering-rolerule-design-2026-07-23.md §GAP-2):
// (c) v1 bumped the sub-gen SYNCHRONOUSLY on the RBAC informer delta event
// (onBindingAdd/Update/Delete). But the RBAC snapshot the UAF refilter reads
// (EvaluateRBAC → rbacSnap.Load) is rebuilt ASYNC + debounced
// (scheduleRBACRebuild → detached goroutine → rebuildRBACSnapshot →
// rbacSnap.Store). In the window (bump landed, Store not yet) a request
// derives the NEW key (bump visible) but refilters against the OLD snapshot,
// caching the pre-change verdict UNDER the new key. Nothing re-bumps at
// publish, so that wrong-under-new-key cell sticks until TTL — the durable
// defect.
//
// THE FIX: accumulate the changed-subject set across the debounce and bump
// ALL of them INSIDE rebuildRBACSnapshot immediately AFTER rbacSnap.Store
// (the publish barrier, rbac_snapshot.go). By release/acquire, a request that
// observes a bumped sub-gen (new key) is guaranteed to observe a snapshot
// Store'd at or after the change that recorded the bump — the bump becomes
// visible only after the Store. So "a new key ⇒ a snapshot ≥ the change that
// rotated it" holds BY CONSTRUCTION: no stale-under-new-key window.
//
// DEBOUNCE SAFETY (record-then-rearm): the set is populated on EVERY event
// (record) and drained ONLY at publish (flush). The informer handler calls
// scheduleRBACRebuild BEFORE the delta hook records the subject, so a rebuild
// spawned by that first call could run its flush BEFORE the record lands — the
// subject would then sit unflushed until an unrelated future publish. To close
// that gap, recordPendingSubGenBumps RE-ARMS a rebuild AFTER inserting into the
// set: it sets rbacRebuildDirty and, if a rebuild is in flight, that rebuild's
// post-rebuild dirty re-check (rbac_snapshot.go dirty-loop) sees the flag and
// loops for another Store+flush that catches the just-recorded subject; if none
// is in flight, it spawns one via the global watcher. So a recorded subject is
// ALWAYS flushed by a publish sequenced AFTER its record — the happens-after
// invariant holds regardless of the record-vs-rebuild interleave.
//
// PER-SUBJECT / NO-CHURN: the flushed set is exactly {subjects whose
// bindings/roles changed in this rebuild batch} — never global. A 50K install
// storm creating tenant-X bindings flushes only tenant-X subjects per rebuild,
// identical blast radius to (c) v1 (the 50K analysis already blessed it,
// rbac_subgen.go). This is why option (a) survives where a global PublishSeq
// fold (option b) would churn every identity-bound key on every rebuild.

package cache

import (
	"sync"
	"sync/atomic"
)

// rebuildBarrierForTest, when non-nil, holds a callback invoked at the TOP of
// rebuildRBACSnapshot (rbac_snapshot.go) — after the informer event fired but
// before the snapshot Store + flush. TEST-ONLY: production leaves it nil and
// pays only a single atomic load. The falsifier's bump→publish-race arm sets
// it to block the rebuild goroutine in the window, injects an in-window
// request, then releases — making the race deterministic instead of flaky.
var rebuildBarrierForTest atomic.Pointer[func()]

// SetRebuildBarrierForTest installs (fn != nil) or clears (fn == nil) the
// rebuild barrier callback. TEST-ONLY.
func SetRebuildBarrierForTest(fn func()) {
	if fn == nil {
		rebuildBarrierForTest.Store(nil)
		return
	}
	rebuildBarrierForTest.Store(&fn)
}

// pendingSubGenBumps accumulates the subjects whose effective RBAC changed
// since the last snapshot publish. Populated (under mu) by the RBAC informer
// delta hooks via recordPendingSubGenBumps; drained (under mu) by
// flushPendingSubGenBumps, called right after rbacSnap.Store.
var pendingSubGenBumps = struct {
	mu  sync.Mutex
	set map[subjectKey]subGenBumpSource
}{set: map[subjectKey]subGenBumpSource{}}

// recordPendingSubGenBumps records the subjects whose effective RBAC changed
// on an informer delta event, to be bumped at the next snapshot publish. This
// REPLACES the synchronous BumpSubjectSubGens call the (c) v1 hooks made — the
// bump is deferred to flushPendingSubGenBumps so the key does not rotate until
// the snapshot the refilter will read is already live. A map insert under a
// short lock; cheap, called on the informer goroutine.
//
// After recording, it RE-ARMS a rebuild (scheduleRBACRebuild via the global
// watcher) so the just-recorded subject is guaranteed to be flushed by a
// publish sequenced AFTER this record — the informer handler already scheduled
// a rebuild BEFORE calling the delta hook, and that rebuild's flush could race
// ahead of this record; re-arming forces the in-flight rebuild's dirty-loop to
// run one more Store+flush (or spawns a fresh one). See the file header's
// DEBOUNCE SAFETY note.
func recordPendingSubGenBumps(subjects []subjectKey, source subGenBumpSource) {
	if len(subjects) == 0 {
		return
	}
	pendingSubGenBumps.mu.Lock()
	for _, s := range subjects {
		// #260: OR the event source into the subject's pending mask (a subject
		// touched by >1 source in the window carries every bit; flush counts each).
		pendingSubGenBumps.set[s] |= source
	}
	pendingSubGenBumps.mu.Unlock()

	// Re-arm a rebuild so this record is flushed by a happens-after publish.
	// Global() is the singleton watcher; nil only before boot wiring (no
	// informer events can fire then) or under cache-off (hooks are inert).
	if rw := Global(); rw != nil {
		scheduleRBACRebuild(rw)
	}
}

// flushPendingSubGenBumps drains the accumulated set and bumps each subject's
// sub-generation. Called from rebuildRBACSnapshot IMMEDIATELY AFTER
// rbacSnap.Store — the publish barrier — so the bump (and the key rotation it
// causes) becomes visible only after the fresh snapshot is live. Draining to a
// local slice and clearing the map UNDER the lock, then calling
// BumpSubjectSubGens OUTSIDE the lock, keeps the lock hold to O(changed
// subjects) map ops and never holds it across the atomic increments.
//
// Ordering note: this MUST be sequenced after rbacSnap.Store on the SAME
// goroutine. The Store is a release; a reader's rbacSnap.Load is an acquire;
// the atomic bump here, sequenced-after the Store, is observable to a reader
// only via a subsequent Load that also sees the new snapshot. See the file
// header for the full happens-after argument.
func flushPendingSubGenBumps() {
	pendingSubGenBumps.mu.Lock()
	if len(pendingSubGenBumps.set) == 0 {
		pendingSubGenBumps.mu.Unlock()
		return
	}
	drained := make([]subjectKey, 0, len(pendingSubGenBumps.set))
	masks := make([]subGenBumpSource, 0, len(pendingSubGenBumps.set))
	for s, m := range pendingSubGenBumps.set {
		drained = append(drained, s)
		masks = append(masks, m)
	}
	// Clear by reallocating — cheaper than deleting each key and lets the old
	// backing map be GC'd once the drained slice is done with the keys.
	pendingSubGenBumps.set = map[subjectKey]subGenBumpSource{}
	pendingSubGenBumps.mu.Unlock()

	// #260: attribute each subject's bump to its recorded source(s), counted
	// HERE at flush — the SAME (subject, publish) dedup point as bumps_total,
	// outside the lock (mirrors the BumpSubjectSubGens call below).
	for _, m := range masks {
		countSubGenBumpSources(m)
	}
	BumpSubjectSubGens(drained)

	// #258 — fire the RBAC-shift reseed hook AFTER the counters are bumped, so
	// the dispatchers engine's scoped reseed mints each rotated subject's NEW
	// sub-gen key. Fired synchronously here (the sub-gen publish-barrier drain);
	// the hook handler is non-blocking (builds a prewarmScope + O(1)
	// enqueueScope). No-op when no hook is registered (prewarm off / cache-off).
	// Mirrors notifyGVRDiscoveredForReprewarm's placement at the discovery
	// publish point. Carries the per-subject source masks (#260) so the consumer
	// splits snapshot-reuse (narrowing) vs per-subject scoped walk (widening).
	notifyRBACShift(drained, masks)
}

// ResetPendingSubGenBumpsForTest clears the accumulator. TEST-ONLY — production
// never resets (the set self-drains at every publish).
func ResetPendingSubGenBumpsForTest() {
	pendingSubGenBumps.mu.Lock()
	pendingSubGenBumps.set = map[subjectKey]subGenBumpSource{}
	pendingSubGenBumps.mu.Unlock()
}
