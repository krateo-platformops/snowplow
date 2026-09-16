// gone_forget_hook.go — 1.12.7 F6b. Cache-side observer registry that fires
// when the dep tracker derives an AUTHORITATIVE GONE verdict for a coordinate,
// so subsystems holding a harvested in-memory copy of that object can drop it.
//
// WHY THIS EXISTS. The Phase-1 harvesters (nav widgets, content-prewarm
// apiRefs) are append-only: a widget harvested once keeps its DeepCopy for the
// life of the process, and the per-binding seed re-resolves THAT COPY on every
// pass. The seed never fetches the object, so nothing can 404 and no decline
// guard can fire — a deleted widget is replayed into L1 forever, which is what
// makes eviction unwinnable (evict → fresh-skip fails → resolve → write →
// repeat). The process ALREADY derives the authoritative verdict that the
// object is gone: OnObjectEvent's objAbsent branch, probed from a synced
// informer at processing time. This carries that verdict to the holders of
// the copy. It costs zero new API reads and needs no completion detection.
//
// REMOVE ON POSITIVE EVIDENCE, NEVER ON ABSENCE. The hook fires only for
// objAbsent — the informer is servable and its indexer does NOT hold the
// object. objUnknown / objUnknownDegraded never reach it: an informer that is
// not authoritative must not be able to empty the warm set.
//
// WHY A REGISTRY (NOT A DIRECT CALL). The import graph is one-way,
// `dispatchers → cache`. Cache cannot import dispatchers without a cycle, and
// the harvesters are dispatchers-private. Same shape and same rationale as
// RegisterGVRDiscoveredHook (gvr_discovered_hook.go), which this file
// deliberately mirrors: idempotent pointer-keyed registration,
// snapshot-under-lock then fire-unlocked, and a test reset.
//
// THERE IS DELIBERATELY NO TEST-ONLY FIRE SHIM, and none should be added.
// gvr_discovered_hook.go exports one (NotifyGVRDiscoveredForReprewarmTest);
// this file does not, because the cross-package arm that matters here must
// drive the REAL entry point — OnObjectEvent's absent branch — to be evidence
// of anything. A shim would let that arm fire the hook directly, which proves
// the registry works and says nothing about whether an absent verdict reaches
// it. Its absence is what makes the seam testable rather than merely the hook.

package cache

import (
	"reflect"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// goneForgetHooks is the package-level registry. Guarded by mu. Dual storage
// (hooks slice + pointers set) buys O(1) duplicate detection for a
// registration that happens once at boot.
var goneForgetHooks struct {
	mu       sync.Mutex
	hooks    []func(gvr schema.GroupVersionResource, namespace, name string)
	pointers map[uintptr]struct{}
}

// RegisterGoneForgetHook adds a callback that fires when the dep tracker
// derives an ABSENT verdict for a coordinate. The callback runs
// SYNCHRONOUSLY on the dep-event worker goroutine.
//
// IDEMPOTENT: registering the same fn pointer twice is a no-op.
//
// CONTRACT: callers must NOT block inside fn. The dispatchers-side handler is
// a bounded map scan under the harvester mutex — no I/O, no apiserver call.
func RegisterGoneForgetHook(fn func(gvr schema.GroupVersionResource, namespace, name string)) {
	if fn == nil {
		return
	}
	ptr := reflect.ValueOf(fn).Pointer()
	goneForgetHooks.mu.Lock()
	defer goneForgetHooks.mu.Unlock()
	if goneForgetHooks.pointers == nil {
		goneForgetHooks.pointers = map[uintptr]struct{}{}
	}
	if _, dup := goneForgetHooks.pointers[ptr]; dup {
		return
	}
	goneForgetHooks.pointers[ptr] = struct{}{}
	goneForgetHooks.hooks = append(goneForgetHooks.hooks, fn)
}

// notifyObjectGone fires every registered hook with the gone coordinate.
// Called from OnObjectEvent's objAbsent branch.
//
// SNAPSHOT-UNDER-LOCK + FIRE-UNLOCKED: copies the hook slice while holding the
// mutex, then releases before invoking, so the critical section never spans a
// user-supplied callback.
func notifyObjectGone(gvr schema.GroupVersionResource, namespace, name string) {
	goneForgetHooks.mu.Lock()
	if len(goneForgetHooks.hooks) == 0 {
		goneForgetHooks.mu.Unlock()
		return
	}
	hooks := append([]func(schema.GroupVersionResource, string, string){}, goneForgetHooks.hooks...)
	goneForgetHooks.mu.Unlock()
	for _, fn := range hooks {
		fn(gvr, namespace, name)
	}
}

// ResetGoneForgetHooksForTest clears the registry. TEST-ONLY — the production
// registry is append-only (boot-time wiring).
func ResetGoneForgetHooksForTest() {
	goneForgetHooks.mu.Lock()
	goneForgetHooks.hooks = nil
	goneForgetHooks.pointers = nil
	goneForgetHooks.mu.Unlock()
}
