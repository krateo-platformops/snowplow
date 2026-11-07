package cache

// rbac_complete_wait.go — #1233: the writing work of the prewarm starts only on a
// complete RBAC snapshot.
//
// The store admits no write of a resolve that began while the snapshot was
// incomplete (put_admission.go). A prewarm or seed unit that ran in that window
// would resolve, be refused, and never be redone, and its cell would stay cold
// after the snapshot completed (#506: no cold navigation). Its key would not be
// reliable either: it folds a binding set read from the partial snapshot. So the
// units wait for the complete snapshot before they mint a key, and then run
// once, on keys and verdicts the complete snapshot derives.
//
// Waiting is on the publish itself: every snapshot the rebuild stores (and the
// test seam) wakes the waiters, which re-check. No timer decides it; a caller's
// ctx bounds the wait.

import (
	"context"
	"sync/atomic"
)

// rbacPublishedCh is closed and replaced at every snapshot store, so a waiter
// re-checks completeness at each publish.
var rbacPublishedCh atomic.Pointer[chan struct{}]

func init() {
	ch := make(chan struct{})
	rbacPublishedCh.Store(&ch)
}

// noteRBACSnapshotStored wakes every WaitRBACSnapshotComplete waiter. Called
// after each store of the RBAC snapshot.
func noteRBACSnapshotStored() {
	next := make(chan struct{})
	prev := rbacPublishedCh.Swap(&next)
	close(*prev)
}

// WaitRBACSnapshotComplete returns nil once the published RBAC snapshot is
// complete (RBACSnapshotComplete), or ctx's error if ctx ends first.
func WaitRBACSnapshotComplete(ctx context.Context) error {
	for {
		ch := *rbacPublishedCh.Load()
		if rbacSnapshotComplete() {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// RBACSnapshotComplete reports whether the published RBAC snapshot is complete:
// built after every RBAC informer finished its initial LIST (#947 Synced).
func RBACSnapshotComplete() bool { return rbacSnapshotComplete() }
