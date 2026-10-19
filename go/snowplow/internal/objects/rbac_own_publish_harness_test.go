package objects

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// waitOwnRBACPublish is the #385/#422 test-isolation pattern (the api
// package's waitOwnRBACPublish) for every harness here that builds a watcher
// and then GETs with an identity: the informer branch of objects.Get serves
// only after EvaluateRBAC allows, and EvaluateRBAC reads the GLOBAL RBAC
// snapshot. NewResourceWatcher publishes its first snapshot from an async
// goroutine that WaitForCacheSync does not wait for, so without this a GET can
// run against nil or a previous test's snapshot, read as denied and fall
// through to the apiserver with no user endpoint: #775's 401.
//
// It waits for THIS watcher's own initial publish to finish (so no later async
// initial publish of rw can race the test), then rebuilds synchronously FROM rw
// so the live snapshot is provably rw's even if a neighbour's late publish
// landed after rw's own.
func waitOwnRBACPublish(t *testing.T, rw *cache.ResourceWatcher) {
	t.Helper()
	if h := ownRBACPublishAwaitHook; h != nil {
		h()
	}
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		t.Fatalf("#775: own initial RBAC publish: %v", err)
	}
	if h := ownRBACPublishLandedHook; h != nil {
		h()
	}
	cache.RebuildRBACSnapshotForTest(rw)
}

// The #775 arms' seams; nil outside them, and no test in this package runs in
// parallel. ownRBACPublishAwaitHook runs right before the harness blocks on its
// watcher's own initial publish (P1 releases the parked publish there);
// ownRBACPublishLandedHook runs once that publish has returned, before the
// rebuild (P2 lands a neighbour's snapshot there).
var (
	ownRBACPublishAwaitHook  func()
	ownRBACPublishLandedHook func()
)
