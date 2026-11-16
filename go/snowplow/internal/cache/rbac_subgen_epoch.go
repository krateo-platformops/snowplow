// rbac_subgen_epoch.go — #682: the boot-window rotation.
//
// THE GAP. A Role / ClusterRole RULE edit rotates a per-identity key only
// through the per-subject sub-generation bump onRoleRulesChanged records
// (bindings_by_gvr_delta.go, #257): the SubjectBindingSet digest covers binding
// ids, not role contents. No hook records a bump while the bindings index is
// not READY (isReady): until the RBAC handlers have been delivered their
// informers' initial lists the index does not hold every binding, so a role
// edit could not be routed to every subject it concerns. An RBAC edit that
// landed between the first RBAC snapshot publish and ready rotated no key, so
// a per-identity cell minted in that window could keep being served under the
// pre-edit grant.
//
// THE FIX. The RBAC informer handler (newRBACSnapshotHandler) notes, under the
// index's read lock, that an RBAC change the key can ONLY see through the
// sub-gen arrived while the index was not ready:
//
//   - a Role / ClusterRole ADD that is NOT part of the informer's initial list
//     (isInInitialList), a Role / ClusterRole DELETE, or a Role / ClusterRole
//     UPDATE whose rules changed (policyRulesEqual — the #257 predicate);
//   - a binding UPDATE whose roleRef changed.
//
// Binding ADD / DELETE and binding subject / uid edits are NOT noted: they
// change the requester's SubjectBindingSet digest, so they already rotate the
// key with no sub-gen bump. And nothing is noted before the first publish
// (RBACGen() == 0): no per-identity cell can exist before it (EvaluateRBAC
// denies without a snapshot and a "" BindingUID is never cached).
//
// The initial list is excluded by isInInitialList, NOT by the publish sequence:
// informer handlers run asynchronously to HasSynced, so initial-list ADDs can
// still be delivered after the first publish.
//
// The index's ONE false→true transition of ready (markBindingsIndexReady, under
// the index's write lock) reads-and-clears that note and, if it was set, bumps
// ONE process-wide epoch that RBACSubGenForSubject folds into every identity's
// sub-generation. ready never returns to false in production, so the note can
// only be taken before it.
//
// WHY A GLOBAL TERM IS SOUND. An identity's sub-generation is the LATEST state
// of the RBAC state clock over its subject counters and this epoch
// (rbac_subgen.go, #1439). The epoch bump takes the clock's next state, which is
// above every value any counter holds, so every identity's sub-generation moves
// to it and no key minted before the bump can be derived after it. Every later
// per-subject bump takes a later state still. The epoch is part of the existing
// RBACSubGen key dimension — not a new identity input — and identity-free
// classes (which do not fold RBACSubGen) do not rotate.
//
// (Until #1439 this paragraph argued from a SUM of counters that only ever
// Add(1): "a later state can never produce an earlier sum". That holds for ONE
// identity over time and is false ACROSS identities — another identity, with
// the same binding set and a different history, reaches that earlier sum later.)
//
// COST. At most ONE global rotation per process, and only when an RBAC edit
// really landed between the first publish and ready. The cells that exist in
// that window are mostly the boot seed's; on rotation they miss on their next
// read, and the refresher's identity-class drift guard re-picks or evicts them
// (#424/#444).
//
// #819 / #1458 — THE BUMP LANDS BEFORE READY. The index is marked ready by the
// initial-publish goroutine (waitAndPublishInitialRBACSnapshot), right after
// its complete publish and once the RBAC handlers have been delivered their
// initial lists, and readiness waits for that goroutine to return (#1231). So
// the bump, if owed, happens before /readyz is 200 on every readiness path,
// and every later RBAC edit rotates through the per-subject sub-gen. (Until
// #1458 the index was BUILT from the published snapshot, first by the engine's
// boot pass and, since #819, by the first readiness flip, with a deferral when
// no complete snapshot existed yet. There is no build any more: the first
// readiness flip only sets the navigated set, settleBindingsIndexBeforeReady.)

package cache

import (
	"expvar"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	clientcache "k8s.io/client-go/tools/cache"
)

var (
	// rbacSubGenEpoch COUNTS the boot-window rotations: 0, or 1 once the index
	// became ready with an RBAC event noted before it. It is what
	// RBACSubGenEpoch, the snowplow_rbac_subgen_epoch expvar and its OTLP mirror
	// report, unchanged by #1439. It is NOT folded into any key.
	rbacSubGenEpoch atomic.Uint64
	// rbacSubGenEpochState is the process-wide term of every identity's
	// RBACSubGen: the RBAC state clock's value at that rotation (0 before it).
	// Folded by RBACSubGenForSubject; not published.
	rbacSubGenEpochState atomic.Uint64
	// rbacEventSeenPreBuild is set by the RBAC handler when an RBAC event
	// arrived after the first publish while the index was not ready.
	rbacEventSeenPreBuild atomic.Bool
)

// notePreBuildRBACEvent is called under the index's read lock when the index
// is not ready (notePreBuildIfNotBuilt).
func notePreBuildRBACEvent() {
	if RBACGen() > 0 {
		rbacEventSeenPreBuild.Store(true)
	}
}

// rbacSnapshotHandler wraps the RBAC snapshot handler set so the #682 note sees
// isInInitialList (a plain ResourceEventHandlerFuncs drops it) and the event's
// semantic shape. Every event is still delivered to the wrapped funcs unchanged.
type rbacSnapshotHandler struct {
	funcs     clientcache.ResourceEventHandlerFuncs
	isBinding bool
}

func newRBACSnapshotHandler(funcs clientcache.ResourceEventHandlerFuncs, isBinding bool) rbacSnapshotHandler {
	return rbacSnapshotHandler{funcs: funcs, isBinding: isBinding}
}

func (h rbacSnapshotHandler) OnAdd(obj interface{}, isInInitialList bool) {
	if !h.isBinding && !isInInitialList {
		bindingsByGVRSingleton().notePreBuildIfNotBuilt()
	}
	h.funcs.OnAdd(obj, isInInitialList)
	if isInInitialList {
		if hook := rbacInitialListAddDeliveredHookForTest.Load(); hook != nil {
			(*hook)()
		}
	}
}

// rbacInitialListAddDeliveredHookForTest, when non-nil, runs after the RBAC
// snapshot handler has fully handled an initial-LIST ADD. TEST-ONLY: one
// atomic load per initial-list ADD in production. client-go delivers to each
// handler on its own goroutine, so a test that needs "this watcher's initial
// LIST has reached the snapshot handler" (no rebuild still to be scheduled by
// it) waits on this event rather than on time (#691 S2).
var rbacInitialListAddDeliveredHookForTest atomic.Pointer[func()]

func (h rbacSnapshotHandler) OnUpdate(oldObj, newObj interface{}) {
	if preBuildUpdateRotates(h.isBinding, oldObj, newObj) {
		bindingsByGVRSingleton().notePreBuildIfNotBuilt()
	}
	h.funcs.OnUpdate(oldObj, newObj)
}

func (h rbacSnapshotHandler) OnDelete(obj interface{}) {
	if !h.isBinding {
		bindingsByGVRSingleton().notePreBuildIfNotBuilt()
	}
	h.funcs.OnDelete(obj)
}

// preBuildUpdateRotates reports whether an UPDATE changes something only the
// sub-gen can carry: a role's rules, or a binding's roleRef. An event that
// cannot be normalised is treated as a change (doubt rotates).
func preBuildUpdateRotates(isBinding bool, oldObj, newObj interface{}) bool {
	if isBinding {
		if o, ok := asCRB(oldObj); ok {
			if n, ok := asCRB(newObj); ok {
				return o.RoleRef != n.RoleRef
			}
			return true
		}
		if o, ok := asRB(oldObj); ok {
			if n, ok := asRB(newObj); ok {
				return o.RoleRef != n.RoleRef
			}
			return true
		}
		return true
	}
	o, n := roleSideOf(oldObj), roleSideOf(newObj)
	if o.kind == "" || n.kind == "" || o.kind != n.kind {
		return true
	}
	return !policyRulesEqual(o.rules, n.rules)
}

// rotateIfRBACEventSeenPreBuild is called by markBindingsIndexReady under the
// index's write lock at the false→true transition of ready. It reads and
// clears the note and bumps the epoch once if it was set.
func rotateIfRBACEventSeenPreBuild() bool {
	if !rbacEventSeenPreBuild.Swap(false) {
		return false
	}
	// #1439 — the rotation is one state of the RBAC state clock, the same clock
	// the per-subject bumps take theirs from, so it is above every sub-generation
	// minted before it and below every one minted by a later bump.
	raiseSubGenTo(&rbacSubGenEpochState, nextRBACState())
	rbacSubGenEpoch.Add(1)
	noteIdentityClassesMayHaveMoved() // #819: tell armed tabs their keys rotated
	return true
}

// settleBindingsIndexBeforeReady (#819, #1458) gives the bindings index its
// navigated set, the GVRs registered so far, at the first readiness flip, so
// the per-GVR buckets the enumerators read exist when the pod becomes Ready.
// Called by MarkPhase1Done. A no-op when the cache is off or there is no
// watcher. Every prewarm pass sets the navigated set again over what is
// registered then, and AddNavigatedGVR widens it meanwhile.
//
// It writes buckets only. The index's membership is the event hooks' alone and
// its ready signal is the initial-publish goroutine's (markBindingsIndexReady),
// which readiness waits for (#1231): the #682 bump lands there, before Ready.
//
// A panic is recovered and counted: it must never skip the flip and pin
// /readyz at 503.
func settleBindingsIndexBeforeReady() {
	if Disabled() {
		return
	}
	rw := Global()
	if rw == nil || rw.mode == modePassthrough {
		return
	}
	start := time.Now()
	defer func() {
		readySettleLastNanos.Store(int64(time.Since(start)))
		if r := recover(); r != nil {
			readySettlePanics.Add(1)
			// log-ctx-waiver: boot: phase 1 settles the index before the pod is ready
			slog.Error("cache.bindings_by_gvr.ready_settle_panic",
				slog.String("subsystem", "cache"),
				slog.String("panic_type", fmt.Sprintf("%T", r)), // metadata only, never the value
				slog.String("effect", "readiness flips anyway; the next prewarm pass sets the navigated set"))
		}
	}()
	if hook := readySettleHookForTest.Load(); hook != nil {
		(*hook)()
	}
	SetNavigatedGVRs(rw.RegisteredGVRs())
	readySettleSet.Add(1)
}

var (
	readySettleSet       atomic.Uint64
	readySettlePanics    atomic.Uint64
	readySettleLastNanos atomic.Int64
	// readySettleHookForTest runs inside the settle, before it sets the
	// navigated set. TEST-ONLY: nil in production.
	readySettleHookForTest atomic.Pointer[func()]
)

// ReadySettleStats is the #819 readiness-settle readout.
type ReadySettleStats struct {
	// NavigatedSetAtFlip counts the readiness flips that gave the bindings
	// index its navigated set.
	NavigatedSetAtFlip uint64
	Panics             uint64
	LastMS             float64
}

// ReadySettleStatsSnapshot reads the settle counters.
func ReadySettleStatsSnapshot() ReadySettleStats {
	return ReadySettleStats{
		NavigatedSetAtFlip: readySettleSet.Load(),
		Panics:             readySettlePanics.Load(),
		LastMS:             float64(readySettleLastNanos.Load()) / float64(time.Millisecond),
	}
}

// RBACSubGenEpoch returns how many boot-window rotations this process made: 0
// or 1 (the value snowplow_rbac_subgen_epoch publishes). The term the keys fold
// is rbacSubGenEpochState.
func RBACSubGenEpoch() uint64 { return rbacSubGenEpoch.Load() }

// resetRBACSubGenEpochForTest zeroes the epoch and the note (composed into
// ResetRBACSubGenForTest).
func resetRBACSubGenEpochForTest() {
	rbacSubGenEpoch.Store(0)
	rbacSubGenEpochState.Store(0)
	rbacEventSeenPreBuild.Store(false)
}

// CFG-1: published only with the cache on.
func init() {
	if Disabled() {
		return
	}
	expvar.Publish("snowplow_rbac_subgen_epoch", expvar.Func(func() any {
		return RBACSubGenEpoch()
	}))
	// #819 / #1458: the readiness flip's bindings-index settle — how often it
	// gave the index its navigated set, recovered panics, and the last
	// settle's duration (the delay it added to Ready).
	expvar.Publish("snowplow_bindings_index_ready_settle", expvar.Func(func() any {
		st := ReadySettleStatsSnapshot()
		return map[string]any{
			"navigated_set_at_flip": st.NavigatedSetAtFlip,
			"panics":                st.Panics,
			"last_ms":               st.LastMS,
		}
	}))
}
