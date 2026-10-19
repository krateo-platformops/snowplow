package objects

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// waitOwnRBACPublish is the #385 test-isolation step for every harness here
// that builds a watcher and then GETs with an identity: the informer branch of
// objects.Get serves only after EvaluateRBAC allows, and EvaluateRBAC reads
// the GLOBAL RBAC snapshot. NewResourceWatcher publishes its first snapshot
// from an async goroutine that WaitForCacheSync does not wait for, so without
// this a GET can run against nil or a previous test's snapshot, read as denied
// and fall through to the apiserver with no user endpoint: #775's 401.
//
// It blocks until THIS watcher's own initial publish has returned, so the live
// snapshot is rw's before the harness hands it to a test.
func waitOwnRBACPublish(t *testing.T, rw *cache.ResourceWatcher) {
	t.Helper()
	if h := ownRBACPublishAwaitHook; h != nil {
		h()
	}
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		t.Fatalf("#775: own initial RBAC publish: %v", err)
	}
}

// ownRBACPublishAwaitHook, when set, runs right before waitOwnRBACPublish
// blocks on the watcher's own initial publish (#775 P1 releases the parked
// publish there). nil outside that arm; no test in this package runs in
// parallel.
var ownRBACPublishAwaitHook func()
