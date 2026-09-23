package cache

import "k8s.io/apimachinery/pkg/runtime/schema"

// lastSyncRVForTest reads the watcher's recorded sync position. In-package, so
// the B-5 arm asserts the forced LIST carries the SAME field the forced pass
// reads — rather than a value the test computed for itself, which would let
// both sides be wrong together.
func lastSyncRVForTest(rw *ResourceWatcher, gvr schema.GroupVersionResource) string {
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	return rw.lastSyncRV[gvr]
}
