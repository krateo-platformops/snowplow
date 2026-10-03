package api

import (
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// waitOwnRBACPublish is the #385 test-isolation pattern for every harness in
// this package that builds a watcher and then reads the GLOBAL RBAC snapshot
// (#422). NewResourceWatcher publishes its first snapshot from an async
// goroutine that races WaitForCacheSync, and the package binary shares one
// global snapshot across tests: without this, a test can run against the
// previous test's snapshot (its own seeded user then reads as denied — the
// "allowed:false, matched_binding_uid:\"\"" signature seen under load), or a
// neighbour's late async publish can overwrite it.
//
// It waits for THIS watcher's own initial publish to finish (so no later async
// initial publish of rw can race the test), then rebuilds synchronously FROM rw
// so the live snapshot is provably rw's.
func waitOwnRBACPublish(t *testing.T, rw *cache.ResourceWatcher) {
	t.Helper()
	if err := rw.WaitInitialRBACPublishForTest(5 * time.Second); err != nil {
		t.Fatalf("#385/#422: own initial RBAC publish: %v", err)
	}
	cache.RebuildRBACSnapshotForTest(rw)
}
