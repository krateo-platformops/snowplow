package cache

// rbac_complete_wait.go — #1233: the writing work of the prewarm starts only once
// the initial RBAC publish is done.
//
// The store admits no write of a resolve that began while the snapshot was
// incomplete (put_admission.go). A prewarm or seed unit that ran in that window
// would resolve, be refused, and never be redone, and its cell would stay cold
// after the snapshot completed (#506: no cold navigation). Its key would not be
// reliable either: it folds a binding set read from the partial snapshot. So the
// units wait before they mint a key, and then run once, on keys and verdicts the
// complete snapshot derives.
//
// The wait is #1231's: readiness waits for the same thing (rbac_readiness.go).
// waitAndPublishInitialRBACSnapshot closes rbacInitialPublishDone once it has
// published the complete snapshot and run the bindings-index build. No timer
// decides it; the caller's ctx bounds the wait.

import "context"

// WaitRBACSnapshotComplete returns nil once the initial RBAC publish is done
// (the snapshot is complete), or ctx's error if ctx ends first. A process whose
// watcher runs no RBAC informers (cache off) has nothing to wait for.
func WaitRBACSnapshotComplete(ctx context.Context) error {
	rw := Global()
	if !rw.runsRBACInformers() || rw.rbacInitialPublishDone == nil {
		return nil
	}
	select {
	case <-rw.rbacInitialPublishDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
