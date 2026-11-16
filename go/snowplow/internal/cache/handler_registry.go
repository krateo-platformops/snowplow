// handler_registry.go — Ship 0 / 0.30.222: declarative handler-extension
// registry for the cache informer pipeline.
//
// PROBLEM. Before Ship 0, the watcher's addResourceTypeLocked /
// addResourceTypeMetadataOnlyLocked carried two hardcoded GVR-keyed
// handler-attach branches:
//
//  1. typed-RBAC snapshot writer: `if isTypedRBACGVR(gvr) { … attach
//     rbacSnapshotEventHandlers(gvr) … }` (watcher.go).
//
//  2. CRD-watch composition auto-discovery: installed via a separate
//     `StartCRDWatch` entry point, called once from phase1_walk.go's
//     Step 2 and guarded by a sync.Once idempotence field on the
//     watcher.
//     This arm was first refactored under Ship 0 into a declarative
//     registry entry, then DELETED entirely under Ship 0.5 / 0.30.223
//     (v6) — the CRD informer no longer exists; composition GVRs are
//     discovered via cache.DiscoverGroupResources (one-shot apiserver
//     discovery, synchronous, walker-invoked).
//
// Both arms were "attach this specialised handler set when this GVR's
// informer is created" — i.e. the same shape, expressed differently.
//
// SOLUTION. A single declarative registry. Each owner package registers a
// `HandlerExtension` from its own `init()`; the watcher iterates the
// registry blind every time it adds an informer and attaches every
// handler whose predicate matches. `addResourceTypeLocked` no longer
// names any specific GVR.
//
// CRITICAL — feedback_no_special_cases.md: the registry's call sites
// (the two `addResourceType*Locked` helpers) carry zero GVR literals.
// A `HandlerExtension` constant referenced by its predicate lives in
// the owner package, NOT in the watcher.
//
// THREAD SAFETY. Production registration happens at package init() and is
// therefore single-threaded by the Go runtime; the registry is read on
// the watcher's informer-creation path (always under rw.mu.Lock()) and on
// the test helper path. Reads use a RWMutex; writes a Mutex; both are
// cheap (the registry is tiny — post-Ship-0.5 only RBAC remains).
//
// OBSERVABILITY. Every successful attach increments the
// `handler_extensions_attached_count{name=X}` counter. The test corpus
// asserts this counter so a regression that drops an extension (the
// predicate stops matching, the registry loses an entry, the watcher
// stops iterating) fails loud.

package cache

import (
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
	clientcache "k8s.io/client-go/tools/cache"
)

// HandlerExtension is one declarative attach-this-handler-set-on-this-GVR
// rule. Each owner package registers its own instance from `init()`.
//
//   - Name: stable identifier for observability + falsifier counters
//     (e.g. "rbac.snapshot_writer"). Must be unique per process —
//     duplicate names panic at registration time.
//
//   - Predicate: returns true iff this extension's Handlers should attach
//     to the informer for `gvr`. The owner package decides — could be a
//     single-GVR equality, a set-membership check, or a group/resource
//     prefix. Called once per addResourceType* invocation per registered
//     extension, so it must be cheap (no apiserver calls, no locks).
//
//   - Handlers: factory returning the handler set to attach. The watcher
//     calls `gi.Informer().AddEventHandler(ext.Handlers(rw, gvr))` when
//     the predicate matches. The factory is invoked exactly once per
//     attach so closures over rw/gvr are safe.
type HandlerExtension struct {
	Name      string
	Predicate func(schema.GroupVersionResource) bool
	Handlers  func(rw *ResourceWatcher, gvr schema.GroupVersionResource) clientcache.ResourceEventHandler
}

// handlerExtensions is the process-wide registry. Append-only: an owner
// package registers exactly once at init() time. `handlerExtMu` guards
// reads + writes; writes are single-threaded by Go init() semantics, but
// the lock keeps the contract explicit + safe under the test reset path.
var (
	handlerExtMu      sync.RWMutex
	handlerExtensions []HandlerExtension

	// handlerExtensionsAttachedTotal counts successful attachments per
	// extension name. Keyed by name; values are `*atomic.Uint64`.
	// Falsifier reads through HandlerExtensionsAttachedCount.
	handlerExtensionsAttachedTotal sync.Map // name(string) -> *atomic.Uint64
)

// RegisterHandlerExtension records ext for future addResourceType*
// invocations. Called from an owner package's `init()`. Duplicate names
// panic — the registry is a single source of truth and a name collision
// is a programming error caught at boot rather than a silent override.
//
// Empty Name / nil Predicate / nil Handlers all panic — every field is
// load-bearing and a registration with any of them missing would silently
// short-circuit the iteration.
func RegisterHandlerExtension(ext HandlerExtension) {
	if ext.Name == "" {
		panic("cache.RegisterHandlerExtension: Name must be non-empty")
	}
	if ext.Predicate == nil {
		panic("cache.RegisterHandlerExtension: Predicate must be non-nil for name=" + ext.Name)
	}
	if ext.Handlers == nil {
		panic("cache.RegisterHandlerExtension: Handlers must be non-nil for name=" + ext.Name)
	}
	handlerExtMu.Lock()
	defer handlerExtMu.Unlock()
	for _, existing := range handlerExtensions {
		if existing.Name == ext.Name {
			panic("cache.RegisterHandlerExtension: duplicate name " + ext.Name)
		}
	}
	handlerExtensions = append(handlerExtensions, ext)
}

// eventHandlerAttacher is the narrow interface attachMatchingHandlerExtensions
// consumes — just `AddEventHandler`, which both
// `clientcache.SharedIndexInformer` (the production type
// `informers.GenericInformer.Informer()` returns) and a unit-test fake
// implement. Decoupling from the full SharedIndexInformer interface
// keeps the test fake small.
type eventHandlerAttacher interface {
	AddEventHandler(clientcache.ResourceEventHandler) (clientcache.ResourceEventHandlerRegistration, error)
}

// attachMatchingHandlerExtensions iterates the registry and attaches every
// handler whose predicate matches gvr. Called from addResourceTypeLocked
// and addResourceTypeMetadataOnlyLocked under rw.mu — they hold the lock
// for the duration of the informer-creation pass, so attach failures
// (post-Start informer or duplicate AddEventHandler) are logged but never
// fail the registration. The shape mirrors the previous inline RBAC
// branch — only the GVR-keyed naming moves out.
//
// Returns the count of extensions attached for observability assertions;
// the watcher does not propagate failures upstream.
// handlerRegistrationKeeper is an attacher that keeps each extension handler's
// registration, by extension name (#1458).
type handlerRegistrationKeeper interface {
	keepRegistration(extension string, reg clientcache.ResourceEventHandlerRegistration)
}

func attachMatchingHandlerExtensions(rw *ResourceWatcher, gvr schema.GroupVersionResource, attacher eventHandlerAttacher) int {
	handlerExtMu.RLock()
	exts := make([]HandlerExtension, len(handlerExtensions))
	copy(exts, handlerExtensions)
	handlerExtMu.RUnlock()

	attached := 0
	for _, ext := range exts {
		if !ext.Predicate(gvr) {
			continue
		}
		handlers := ext.Handlers(rw, gvr)
		if handlers == nil {
			continue
		}
		reg, err := attacher.AddEventHandler(handlers)
		if err != nil {
			// Best-effort: log via the watcher's logging path is owned by
			// the caller — the caller already has the resource type
			// string and the gvr context. We deliberately do NOT log here
			// to avoid a second log line for every attach. The attach
			// counter only ticks on success.
			_ = err
			continue
		}
		// #1458 — the registration says when THIS handler has been delivered
		// the informer's initial list (HasSynced); the attacher keeps it.
		if keeper, ok := attacher.(handlerRegistrationKeeper); ok {
			keeper.keepRegistration(ext.Name, reg)
		}
		attached++
		incHandlerExtensionsAttached(ext.Name)
	}
	return attached
}

// incHandlerExtensionsAttached ticks the per-name attach counter.
// Lazy-creates the *atomic.Uint64 via sync.Map.LoadOrStore so a
// never-attached extension does not allocate its counter at registration.
func incHandlerExtensionsAttached(name string) {
	v, _ := handlerExtensionsAttachedTotal.LoadOrStore(name, &atomic.Uint64{})
	v.(*atomic.Uint64).Add(1)
}

// HandlerExtensionsAttachedCount returns the running total of successful
// attachments for the named extension. Zero for an unknown / never-
// attached name. Post-Ship-0.5 the registry holds only the RBAC arm
// (the CRD-watch arm was deleted with the CRD informer); used by the
// falsifier corpus to assert
//
//	counter == 0 at start of Phase 1
//	counter == 4 after RBAC eager registration (one per GVR)
func HandlerExtensionsAttachedCount(name string) uint64 {
	v, ok := handlerExtensionsAttachedTotal.Load(name)
	if !ok {
		return 0
	}
	return v.(*atomic.Uint64).Load()
}

// HandlerExtensionsSnapshot returns the registered extensions in
// registration order. TEST + observability helper. Production callers
// must not depend on order.
func HandlerExtensionsSnapshot() []HandlerExtension {
	handlerExtMu.RLock()
	defer handlerExtMu.RUnlock()
	out := make([]HandlerExtension, len(handlerExtensions))
	copy(out, handlerExtensions)
	return out
}

// ResetHandlerExtensionsForTest clears the registry AND the attach
// counters. TEST-ONLY — the production lifecycle is append-only.
func ResetHandlerExtensionsForTest() {
	handlerExtMu.Lock()
	handlerExtensions = nil
	handlerExtMu.Unlock()
	handlerExtensionsAttachedTotal.Range(func(k, _ any) bool {
		handlerExtensionsAttachedTotal.Delete(k)
		return true
	})
}
