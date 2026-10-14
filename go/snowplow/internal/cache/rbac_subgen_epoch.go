// rbac_subgen_epoch.go — #682: the boot-window rotation.
//
// THE GAP. A Role / ClusterRole RULE edit rotates a per-identity key only
// through the per-subject sub-generation bump onRoleRulesChanged records
// (bindings_by_gvr_delta.go, #257): the SubjectBindingSet digest covers binding
// ids, not role contents. Every sub-gen hook returns early while the
// bindings-by-GVR delta index is not built (deltaActive), and the index is built
// once, by the prewarm engine boot (prewarm_engine_boot.go, step 3). An RBAC
// edit that landed between the first RBAC snapshot publish and that build
// rotated no key, so a per-identity cell minted in that window could keep being
// served under the pre-edit grant.
//
// THE FIX. deltaActive notes, under the index's read lock, that an RBAC event
// arrived while the index was not built — only once a snapshot has been
// published (RBACGen() > 0): the informers' initial-list ADDs land before the
// first publish, and no per-identity cell can exist before it (EvaluateRBAC
// denies without a snapshot and a "" BindingUID is never cached). The FIRST
// false→true transition of the index's built flag (BuildBindingsByGVRIndex,
// under the index's write lock) reads-and-clears that note and, if it was set,
// bumps ONE process-wide epoch that RBACSubGenForSubject adds to every identity's
// sum. Later rebuilds never bump: built never returns to false in production, so
// the note can only be taken before the first build.
//
// WHY A GLOBAL TERM IN THE SUM IS SOUND. RBACSubGenForSubject is a sum of
// counters that are each monotone non-decreasing (per-subject counters only ever
// Add(1); the epoch only ever Add(1)). Any change to any term therefore
// strictly increases the sum: a later state can never produce an earlier sum,
// so a rotated key can never collide with a key minted before the rotation. The
// epoch is part of the existing RBACSubGen key dimension — not a new identity
// input — and identity-free classes (which do not fold RBACSubGen) do not rotate.
//
// COST. At most ONE global rotation per process, and only when an RBAC edit
// really landed between the first publish and the first index build. Ready gates
// customer /call until boot completes, so the cells that exist in that window
// are mostly the boot seed's; on rotation they miss on their next read, and the
// refresher's identity-class drift guard re-picks or evicts them (#424/#444).

package cache

import (
	"expvar"
	"sync/atomic"
)

var (
	// rbacSubGenEpoch is the process-wide term of every identity's RBACSubGen.
	rbacSubGenEpoch atomic.Uint64
	// rbacEventSeenPreBuild is set by deltaActive when an RBAC event arrived
	// after the first publish while the delta index was not built.
	rbacEventSeenPreBuild atomic.Bool
)

// notePreBuildRBACEvent is called by deltaActive under the index's read lock
// when the index is not built.
func notePreBuildRBACEvent() {
	if RBACGen() > 0 {
		rbacEventSeenPreBuild.Store(true)
	}
}

// rotateIfRBACEventSeenPreBuild is called by BuildBindingsByGVRIndex under the
// index's write lock at the FIRST false→true transition of built. It reads and
// clears the note and bumps the epoch once if it was set.
func rotateIfRBACEventSeenPreBuild() bool {
	if !rbacEventSeenPreBuild.Swap(false) {
		return false
	}
	rbacSubGenEpoch.Add(1)
	return true
}

// RBACSubGenEpoch returns the current epoch.
func RBACSubGenEpoch() uint64 { return rbacSubGenEpoch.Load() }

// resetRBACSubGenEpochForTest zeroes the epoch and the note (composed into
// ResetRBACSubGenForTest).
func resetRBACSubGenEpochForTest() {
	rbacSubGenEpoch.Store(0)
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
}
