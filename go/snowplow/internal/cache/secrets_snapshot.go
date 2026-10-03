// secrets_snapshot.go — Ship D.2 (0.30.143). In-process cache of the
// per-user `<user>-clientconfig` Secrets in AUTHN_NAMESPACE, attacking
// F-3 (60.1% of snowplow-attributable apiserver request traffic per
// the tester's Ship D attribution table — 134/60 s baseline at
// 0.30.141).
//
// Pattern reuse — mirrors Ship B's RBAC snapshot
// (internal/cache/rbac_snapshot.go) line-for-line: typed-pointer
// indexer entries published via atomic.Pointer[SecretsSnapshot],
// single-writer rebuild loop with atomic.Bool tryLock + dirty-bit
// re-rebuild, panic-recovering writer goroutine, max one in-flight
// rebuild at a time.
//
// Sibling infrastructure — the Secrets informer runs on a NEW typed
// SharedInformerFactory scoped to AUTHN_NAMESPACE, NOT the existing
// dynamic factory (watcher.go:317-322). The dynamic factory is
// metav1.NamespaceAll cluster-scoped; adding the Secrets GVR to it
// would watch every Secret cluster-wide (helm/SA-token/dockerconfig/
// per-pod garbage → thousands at production scale — catastrophic).
// See design §2.1 for the team-lead-verified rationale.
//
// Concurrency invariants (§3 / AC-D2.5):
//
//   1. Snapshot is IMMUTABLE post-Store. The writer constructs a fresh
//      *SecretsSnapshot (new map, new pointers) and Store()s it;
//      previously-published snapshots are never mutated.
//   2. The pointed-to *corev1.Secret objects are owned by the client-
//      go indexer. Readers MUST NOT mutate sec.Data — client-go's
//      contract says the indexer hands out a SHARED pointer; an
//      UPDATE event delivers a NEW *corev1.Secret (the indexer never
//      mutates in place).
//   3. Atomic publish — atomic.Pointer.Store / .Load is a single
//      memory-ordered write/read. No torn read; no half-built
//      snapshot visible to a reader.
//   4. Single writer enforced via secretsRebuildLock atomic.Bool
//      tryLock; max one in-flight rebuild goroutine.
//   5. Cache-toggle compliance — Disabled() short-circuits
//      every reader-side helper (FromInformerSecret, SecretsCacheServable);
//      the writer infrastructure is never wired in cache=off mode
//      (StartSecretsInformer is gated upstream in main.go).
//
// AC-D2.13 explicit constraints satisfied:
//   - No upstream patch — every line of this file is snowplow-side.
//   - No new public API on `cache` beyond the documented set
//     (SecretsSnapshotLoad / SecretsCacheNamespace /
//     SecretsCacheServable / StartSecretsInformer /
//     AssertSecretsInformerWired plus the SecretsSnapshot type).
//   - No hardcoded special-cases — AUTHN_NAMESPACE comes from
//     StartSecretsInformer's `namespace` argument, sourced from the
//     CLI flag / env var at startup. No namespace literals here.
package cache

import (
	"log/slog"
	"runtime/debug"
	"sync/atomic"

	corev1 "k8s.io/api/core/v1"
)

// SecretsSnapshot is an immutable, atomically-published view of the
// Secrets in the configured AUTHN_NAMESPACE — built by an informer
// event handler in secrets_informer.go, read lock-free by
// FromInformerSecret in internal/resolvers/restactions/api/endpoints_cache.go.
//
// Immutable post-publish: none of these fields is ever mutated after
// secretsSnap.Store(s) completes. Readers MUST treat the map AND the
// pointed-to *corev1.Secret as read-only — the client-go indexer
// owns the Secret objects (its List() returns shared pointers;
// mutating sec.Data would corrupt every other reader).
type SecretsSnapshot struct {
	// ByName indexes Secrets by their object name. The namespace is
	// implicit — it equals the AUTHN_NAMESPACE the informer was
	// constructed with (SecretsCacheNamespace() returns it). A flat
	// per-name map is enough; the resolver mapper doesn't carry a
	// (ns, name) tuple at this level.
	ByName map[string]*corev1.Secret
}

// secretsSnap is the sole publish container. Single-writer / many-
// reader atomic.Pointer[SecretsSnapshot]. Load() returns nil before
// the initial rebuild publishes — reader-side helpers MUST treat nil
// as "soft miss → fall back to upstream FromSecret" (AC-D2.12 pre-
// readiness fallback).
var secretsSnap atomic.Pointer[SecretsSnapshot]

// secretsRebuildLock + secretsRebuildDirty: single-writer atomic.Bool
// tryLock with a dirty-bit re-rebuild. Mirrors the Ship B RBAC
// rebuild pattern (rbac_snapshot.go:120-122) — same goroutine-
// accounting guarantee (max one in-flight rebuild goroutine).
var (
	secretsRebuildLock  atomic.Bool
	secretsRebuildDirty atomic.Bool
)

// secretsSnapshotPublishSeq is bumped on every successful publish.
// Used only for log correlation; not load-bearing for correctness.
// Mirrors rbacSnapshotPublishSeq (rbac_snapshot.go:144).
var secretsSnapshotPublishSeq atomic.Uint64

// SecretsSnapshotLoad returns the current published Secrets snapshot,
// or nil if no snapshot has been published yet (pre-readiness /
// cache=off). Lock-free single atomic load.
//
// Readers MUST treat the returned *SecretsSnapshot as read-only.
func SecretsSnapshotLoad() *SecretsSnapshot {
	return secretsSnap.Load()
}

// scheduleSecretsRebuild flips the dirty bit; if no rebuild is in
// flight, spawns ONE goroutine that drains the bit by looping:
// rebuild → check dirty → if dirty rebuild again. Exits cleanly when
// dirty is false on the post-rebuild re-check.
//
// Bounded goroutines: at most one in-flight rebuild goroutine.
// Mirrors scheduleRBACRebuild (rbac_snapshot.go:206) precisely.
//
// k8s informer event-handler bodies MUST be non-blocking: the dirty
// flip + tryLock + maybe-spawn is a few atomics — the handler returns
// immediately. The actual indexer walk runs on the detached rebuild
// goroutine.
func scheduleSecretsRebuild() {
	// Mark dirty FIRST. If a rebuild is already in flight, it will
	// see this bit on its post-rebuild re-check and loop again.
	secretsRebuildDirty.Store(true)

	// Try to take the writer slot. CompareAndSwap from false→true
	// succeeds for exactly one goroutine; everyone else returns
	// immediately, leaving their dirty flip for the in-flight
	// rebuild to absorb.
	if !secretsRebuildLock.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer secretsRebuildLock.Store(false)
		// Panic-recovering: a rebuild that crashes must not poison
		// the writer slot for the rest of the process. Recover,
		// log loudly, re-mark dirty so the NEXT event re-acquires
		// and retries. Mirrors rbac_snapshot.go:224-233.
		defer func() {
			if r := recover(); r != nil {
				secretsRebuildDirty.Store(true)
				slog.Error("cache.secrets.snapshot.rebuild_panic",
					slog.String("subsystem", "cache"),
					slog.Any("recovered", r),
					slog.String("stack", string(debug.Stack())),
				)
			}
		}()

		// Loop until dirty is false on a fresh check AFTER a
		// rebuild. Each iteration clears dirty BEFORE rebuilding so
		// events arriving MID-rebuild re-flip it and force another
		// iteration. (Clearing AFTER would lose those events.)
		for {
			secretsRebuildDirty.Store(false)
			rebuildSecretsSnapshot()
			if !secretsRebuildDirty.Load() {
				return
			}
		}
	}()
}

// rebuildSecretsSnapshot walks the Secrets indexer once and publishes
// a fresh *SecretsSnapshot. The previous snapshot is replaced
// atomically; existing readers holding the old pointer continue using
// it safely (invariant 1 above).
//
// Failure modes:
//   - Informer not yet started (e.g. cache=off, or AUTHN_NAMESPACE
//     empty at boot): secretsInformer == nil → no-op return. The
//     event handler that called us was wired by StartSecretsInformer,
//     so reaching here with a nil indexer is a wiring regression.
//   - Indexer item is not a *corev1.Secret: skipped with a WARN.
//     Defensive; the typed factory guarantees the type assertion.
func rebuildSecretsSnapshot() {
	ind := secretsInformerIndexer()
	if ind == nil {
		// Wiring not in place — leave the snapshot at whatever it
		// was (likely nil). Readers fall back to upstream.
		return
	}

	items := ind.List()
	snap := &SecretsSnapshot{
		ByName: make(map[string]*corev1.Secret, len(items)),
	}
	typed := make([]*corev1.Secret, 0, len(items))
	for _, it := range items {
		sec, ok := it.(*corev1.Secret)
		if !ok {
			slog.Warn("cache.secrets.snapshot.skip_non_typed",
				slog.String("subsystem", "cache"),
				slog.String("got_type", goTypeOfSecret(it)),
				slog.String("hint", "indexer entry was not *corev1.Secret — "+
					"typed factory invariant broken"),
			)
			continue
		}
		snap.ByName[sec.Name] = sec
		typed = append(typed, sec)
	}

	secretsSnap.Store(snap)
	secretsSnapshotPublishSeq.Add(1)

	// #262 S1 — reconcile the learned identity classes with the clientconfig
	// Secrets (a certificate is parsed only when its ResourceVersion moved).
	syncLearnedFromSecrets(typed)

	slog.Debug("cache.secrets.snapshot.published",
		slog.String("subsystem", "cache"),
		slog.Int("count", len(snap.ByName)),
		slog.Uint64("seq", secretsSnapshotPublishSeq.Load()),
	)
}

// goTypeOfSecret returns a short type name for the WARN log when a
// non-typed entry surfaces. Allocation-cheap; mirrors the no-reflect
// approach in rbac_snapshot.go's goTypeOf.
func goTypeOfSecret(obj interface{}) string {
	switch obj.(type) {
	case *corev1.Secret:
		return "*corev1.Secret"
	case nil:
		return "<nil>"
	default:
		return "<other>"
	}
}

// PublishSecretsSnapshotForTest installs `s` as the current snapshot.
// Used only by tests that build snapshots manually (e.g. the
// AC-D2.5 race test, the AC-D2.12 pre-readiness fallback test).
// Production code MUST NOT call this — production publishes go
// through scheduleSecretsRebuild → rebuildSecretsSnapshot only.
func PublishSecretsSnapshotForTest(s *SecretsSnapshot) {
	secretsSnap.Store(s)
}

// RebuildSecretsSnapshotForTest publicly exposes a synchronous
// snapshot rebuild for tests. Production code uses
// scheduleSecretsRebuild (asynchronous, bounded, dirty-flag-
// coalesced) — never this.
func RebuildSecretsSnapshotForTest() {
	rebuildSecretsSnapshot()
}

// ResetSecretsSnapshotForTest clears the published snapshot and the
// publish-seq counter. TEST-ONLY — production code MUST NOT call it.
// Mirrors the established Reset*ForTest pattern.
func ResetSecretsSnapshotForTest() {
	secretsSnap.Store(nil)
	secretsSnapshotPublishSeq.Store(0)
}
