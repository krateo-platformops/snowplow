// rbac_snapshot.go — Ship B (resolver-path rebuild, 0.30.138).
//
// The four RBAC GVRs (rbac.authorization.k8s.io/v1
// ClusterRoleBindings / RoleBindings / ClusterRoles / Roles) are
// read by EvaluateRBAC on the hot resolver path. Pre-Ship-B each
// `evaluateAgainstInformer` call materialised
//
//   - `cache.(*ResourceWatcher).ListTypedObjects`  — 614 MB / 60-call
//     (`make([]runtime.Object, 0, len(items))` + per-item append in
//     watcher.go:1826-1834)
//   - `client-go/tools/cache.(*threadSafeMap).List` — 574 MB / 60-call
//     (a fresh `[]interface{}` of every indexer key, watcher.go:1816)
//
// — i.e. ~1.2 GB / 60 calls of pure slice rebuild on each /call's RBAC
// fan-out (verdict 0.30.136, design ship-b-typed-rbac-snapshot-design.md
// §0). Ship B replaces those per-call rebuilds with a single typed-RBAC
// snapshot kept up to date by the informer event handlers and atomically
// published. Readers in evaluate.go take **one** `rbacSnap.Load()` per
// EvaluateRBAC call and thread the resulting pointer through every
// sub-read so a single eval observes a coherent snapshot
// (AC-B.3 — correctness-load-bearing, see also §3 of the design).
//
// Ship B EXTENDS the 0.30.6 typed-RBAC indexer (commit d0b3baf) — it does
// NOT introduce a parallel mechanism. The typed transforms
// (`stripAndType*` in strip.go) and the informer routing stay exactly as
// they are; Ship B adds one writer (`scheduleRBACRebuild`) wired to the
// four RBAC informers' ADD/UPDATE/DELETE events and one reader-side
// `Snapshot()` getter. `ListTypedObjects` / `GetTypedObject` remain on
// the public API for non-RBAC callers — they are simply no longer
// reached by the RBAC hot path.
//
// Concurrency (design §3, AC-B.5):
//
//   - Snapshot is IMMUTABLE post-publish. The writer builds a brand-new
//     *RBACSnapshot (fresh maps, fresh slices) and Store()s it. No field
//     of a previously-published snapshot is ever mutated by any
//     goroutine. Readers iterate the maps/slices and read pointer
//     fields; they never write.
//   - `atomic.Pointer[RBACSnapshot]` provides the single
//     memory-ordered Store/Load — no torn read, no half-built snapshot
//     visible to a reader.
//   - One writer at a time via `rbacRebuildLock atomic.Bool` (tryLock),
//     same pattern as the L1 refresh's "Bounded async L1 refresh" lineage
//     (watcher.go:1028). A dirty bit (`rbacRebuildDirty`) absorbs bursts
//     so multiple events during an in-flight rebuild collapse into ONE
//     follow-up rebuild — k8s informer event handlers must NOT block.
//   - The pointed-to typed `*rbacv1.{...}` objects are owned by the
//     client-go indexer (its `List()` returns a slice of pointers; the
//     pointed-to objects are read-only by client-go contract). Ship B
//     follows the same rule — the writer puts pointers into the snapshot
//     and never mutates the pointed-to objects.
//
// Cache toggle (AC-B.7): the snapshot is constructed only in cache=on
// mode. cache=off (`Disabled() == true`) never registers the 4 RBAC
// informers with `stripAndType*` and EvaluateRBAC early-returns to
// UserCan/SubjectAccessReview at evaluate.go:124 unchanged.
package cache

import (
	"log/slog"
	"runtime/debug"
	"sync/atomic"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientcache "k8s.io/client-go/tools/cache"
)

// RBACSnapshot is an immutable, atomically-published view of the four
// RBAC GVRs — built by an informer event handler, read lock-free by
// `evaluateAgainstInformer` via `Snapshot()`.
//
// Immutable post-publish: none of these fields is ever mutated after
// `rbacSnap.Store(s)` completes. Readers MUST treat every field as
// read-only — including the pointed-to `*rbacv1.{...}` objects (the
// client-go indexer owns those; the snapshot just keeps pointers).
//
// Future co-evolution: if a new GVR is added to `rbacTypedGVRs`
// (strip.go:101-106) the snapshot's fields + `rebuildRBACSnapshot` +
// the migrated reader sites in internal/rbac/evaluate.go must all be
// extended in lockstep. AC-B.10's snapshot-miss canary will surface a
// half-done migration loudly.
type RBACSnapshot struct {
	// PublishSeq — Ship 0.30.242 H.c-layered (Phase 2 step 2a). Monotone
	// publish sequence stamped at construction by rebuildRBACSnapshot
	// BEFORE the atomic Store. Pairs with the free-floating
	// `rbacSnapshotPublishSeq atomic.Uint64` (kept for log correlation
	// in RecordRBACSnapshotMiss) but lives on the snapshot itself so any
	// reader who loads via Snapshot() observes a publish-seq that is
	// coherent with the snapshot pointer.
	//
	// Load-bearing for the L2 snapshot-generation authz memo
	// (internal/rbac/snapshot_authz_memo.go, Ship L2 / Task #291): each
	// memo shard is stamped with the PublishSeq it is valid for and is
	// swapped out on mismatch. Without snap-coherent stamping the memo
	// could read stale (snap N+1 contents but snap N seq). (The per-request
	// memo that originally motivated this stamping was deleted in #176; the
	// L2 memo now carries the same generation-binding contract.)
	//
	// 0 is a valid value (first publish stamps 1; the writer reads + adds
	// in one operation against rbacSnapshotPublishSeq).
	PublishSeq uint64

	// Cluster-wide ClusterRoleBindings — read at evaluate.go:198.
	ClusterRoleBindings []*rbacv1.ClusterRoleBinding

	// Namespace-keyed RoleBindings — read at evaluate.go:223. A
	// namespace absent from the map yields a nil slice; readers range
	// over it with zero iterations.
	RoleBindingsByNS map[string][]*rbacv1.RoleBinding

	// ClusterRoles indexed by name — read at evaluate.go:254 (one map
	// lookup per matched binding's `roleRef`).
	ClusterRolesByName map[string]*rbacv1.ClusterRole

	// Roles indexed by `ns + "/" + name` — read at evaluate.go:270.
	RolesByNSName map[string]*rbacv1.Role

	// ─────────────────────────────────────────────────────────────
	// Ship 0.30.169 — subject→bindings indexes.
	//
	// Built ONCE per `rebuildRBACSnapshot` via `rebuildSubjectIndexes`
	// (called as the final step of `rebuildRBACSnapshot`, before the
	// atomic Store). IMMUTABLE post-publish — same concurrency model
	// as ClusterRoleBindings / RoleBindingsByNS (AC-B.5 invariant 1).
	//
	// The values are pointer slices into the SAME typed objects already
	// stored in ClusterRoleBindings / RoleBindingsByNS — NO duplication
	// of the typed structs. RSS overhead is the pointer-slice headers
	// plus map-bucket allocations only.
	//
	// Routing rules — mirror the architect's §3 case-walk:
	//
	//   Subject.Kind == "User"           → CRBsByUser[s.Name]
	//   Subject.Kind == "Group"          → CRBsByGroup[s.Name]
	//   Subject.Kind == "ServiceAccount" → CRBsByServiceAccount[s.Namespace+"/"+s.Name]
	//   Subject.Kind == <anything else>  → CRBsCatchAll  (safety net for
	//                                       unrecognised / future Kinds;
	//                                       under-inclusion would be a
	//                                       permit-loss bug)
	//
	// A CRB with multiple subjects appears under multiple index entries;
	// the union at lookup time dedups by pointer. A CRB with EMPTY
	// Subjects appears in NO index (matches nothing in linear scan;
	// must match nothing in index lookup).
	//
	// Correctness barrier: these indexes are a PRE-FILTER for
	// `anySubjectMatches` (evaluate.go:402-431). Any superset of the
	// linear-scan match set is correct; the post-lookup matcher enforces
	// exact equality. Under-inclusion is a correctness defect.
	//
	// Reader-side contract: callers MUST NOT mutate the returned slices
	// (they are owned by the snapshot, just like ClusterRoleBindings).
	// Concurrent read is lock-free post-publish.
	CRBsByUser           map[string][]*rbacv1.ClusterRoleBinding
	CRBsByGroup          map[string][]*rbacv1.ClusterRoleBinding
	CRBsByServiceAccount map[string][]*rbacv1.ClusterRoleBinding // key = "<ns>/<name>"
	CRBsCatchAll         []*rbacv1.ClusterRoleBinding            // unrecognised Subject.Kind

	// Per-namespace RoleBinding indexes. Same shape as the CRB indexes
	// but keyed first by RoleBinding.Namespace. The outer map is missing
	// for namespaces with no RoleBindings; inner map lookups on a
	// missing namespace return nil (zero-iteration range).
	RBsByUserByNS           map[string]map[string][]*rbacv1.RoleBinding
	RBsByGroupByNS          map[string]map[string][]*rbacv1.RoleBinding
	RBsByServiceAccountByNS map[string]map[string][]*rbacv1.RoleBinding // inner key = "<ns>/<name>"
	RBsCatchAllByNS         map[string][]*rbacv1.RoleBinding

	// All-namespace RoleBinding subject reverse indexes (v7 Step 2A —
	// design §3). Flat analogues of the per-namespace RBs*ByNS maps above:
	// a subject key maps to the RoleBindings that name it as a subject
	// across EVERY namespace, so a caller can enumerate a subject's RBs
	// cluster-wide in O(subject's RBs) rather than walking every namespace.
	// The values are the SAME *rbacv1.RoleBinding pointers held in
	// RoleBindingsByNS — NO struct copy; each RB carries its own .Namespace
	// so per-RB scope is recoverable from a flat lookup. Keys mirror the
	// per-ns maps exactly: User→s.Name, Group→s.Name, ServiceAccount→
	// "<subject-ns>/<subject-name>", unrecognised Kind→RBsCatchAllAllNS.
	//
	// Built inside the SAME rebuildSubjectIndexes RB switch that builds the
	// RBs*ByNS maps (one flat append per arm), so they are structurally the
	// per-ns index flattened. An RB has exactly one namespace ⇒ one entry
	// per subject key ⇒ no cross-ns duplicate, no dedup at build time.
	//
	// ADDITIVE + DARK (Step 2A): no existing reader consults these fields;
	// they are nil under cache-off (rebuildRBACSnapshot early-returns in
	// modePassthrough before rebuildSubjectIndexes runs); GC'd with the old
	// snapshot on publish. Reader-side contract is identical to the
	// RBs*ByNS maps: callers MUST NOT mutate the returned slices;
	// concurrent read is lock-free post-publish.
	RBsByUserAllNS           map[string][]*rbacv1.RoleBinding
	RBsByGroupAllNS          map[string][]*rbacv1.RoleBinding
	RBsByServiceAccountAllNS map[string][]*rbacv1.RoleBinding // key = "<ns>/<name>"
	RBsCatchAllAllNS         []*rbacv1.RoleBinding            // unrecognised Subject.Kind, any namespace
}

// rbacSnap is the sole publish container — a single-writer / many-reader
// `atomic.Pointer[RBACSnapshot]`. Load() returns nil before the initial
// rebuild publishes; reader-side code MUST treat nil as "degrade to
// deny" (AC-B.8).
var rbacSnap atomic.Pointer[RBACSnapshot]

// rbacRebuildLock + rbacRebuildDirty: single-writer atomic.Bool tryLock
// with a dirty-flag re-rebuild. Multiple events during an in-flight
// rebuild collapse into ONE follow-up rebuild — k8s informer event
// handlers must NOT block. Same pattern as the L1 refresh
// (watcher.go:1028 lineage; "Bounded async L1 refresh" — Bug 7).
//
// Goroutine accounting: at most ONE in-flight rebuild goroutine at any
// moment (the tryLock acquirer); a queued rebuild rides as the
// next-loop iteration inside that same goroutine, NOT a second
// goroutine — so max concurrent rebuild goroutines is exactly 1
// regardless of event burst rate (AC-B.5 invariant).
var (
	rbacRebuildLock  atomic.Bool
	rbacRebuildDirty atomic.Bool
)

// rbacSnapshotMissCount counts every roleRef map-lookup miss in
// `roleRefPermits` (AC-B.10 — mirrors the 0.30.6 fallback=true
// invariant at evaluate.go:185-186). Production target: miss-count /
// EvaluateRBAC-count MUST stay below 1% over any 1-minute window. The
// steady-state >0% baseline is the rebuild-lag eventual-consistency
// window described in the design §4.2.
var rbacSnapshotMissCount atomic.Uint64

// RBACSnapshotMissCount returns the cumulative count of `roleRefPermits`
// snapshot misses since process start. Exported for the tester's
// AC-B.10 ratio probe; production code has no reason to read it.
func RBACSnapshotMissCount() uint64 {
	return rbacSnapshotMissCount.Load()
}

// RecordRBACSnapshotMiss is called by the RBAC `roleRefPermits` code
// when a roleRef name is not in the snapshot's CR/R map even though the
// binding still references it. The miss is WARN-logged and a counter
// incremented; the caller treats the miss as a deny (same fail-closed
// posture as today's `GetTypedObject !ok`).
//
// The expected steady-state miss rate is the bounded rebuild lag from
// §4.2 (a binding whose target CR/R was just deleted; the snapshot
// rebuild has not landed yet). >1% over any 1-minute window indicates a
// genuine half-done migration / registration bug — the tester polls the
// ratio for the AC-B.10 gate.
func RecordRBACSnapshotMiss(kind, namespace, name string) {
	rbacSnapshotMissCount.Add(1)
	// Snapshot-version sequence number — bumped on every publish —
	// lets ops correlate a miss-burst with a specific snapshot version
	// in the logs. Cheaper than a pointer-identity hash.
	snapSeq := rbacSnapshotPublishSeq.Load()
	slog.Warn("rbac.evaluate.snapshot.miss",
		slog.String("subsystem", "rbac"),
		slog.String("event", "snapshot.miss"),
		slog.String("kind", kind),
		slog.String("namespace", namespace),
		slog.String("name", name),
		slog.Uint64("snap_seq", snapSeq),
		slog.String("hint", "roleRef references a target absent from the snapshot — "+
			"either rebuild-lag (self-healing) or a genuine indexer/snapshot inconsistency"),
	)
}

// rbacSnapshotPublishSeq is bumped on every successful publish. Used
// only for log correlation (`snap_seq` field in
// `rbac.evaluate.snapshot.miss` WARN); not load-bearing for correctness.
var rbacSnapshotPublishSeq atomic.Uint64

// RBACGen returns the current RBAC snapshot publish generation. Bumped
// once per successful rebuildRBACSnapshot publish. Consumers (e.g. the
// per-cohort gate memo, Ship GMC / 0.30.174) compare a stamped gen
// against this live value to detect a stale memo against the current
// RBAC store.
//
// Lock-free: a single atomic load. Returns 0 when no snapshot has ever
// been published (pre-readiness / cache=off) — a 0 stamp on a memo will
// then compare equal to 0 here, but no memo is built before the snapshot
// is live (memo population reads the live RBAC store via filterListByRBAC
// → EvaluateRBAC, which fails closed when the snapshot is nil).
func RBACGen() uint64 {
	return rbacSnapshotPublishSeq.Load()
}

// Snapshot returns the current published RBAC snapshot, or nil if no
// snapshot has been published yet (pre-readiness / cache=off). Readers
// MUST call this ONCE per EvaluateRBAC call and thread the returned
// pointer through every sub-read in that eval — see AC-B.3 in the
// design (single-snapshot-per-evaluation invariant).
//
// Lock-free: a single atomic load.
func (rw *ResourceWatcher) Snapshot() *RBACSnapshot {
	_ = rw // signature is per-watcher for future multi-watcher symmetry; today there is one global rw
	return rbacSnap.Load()
}

// LiveRBACSnapshot returns the current published RBAC snapshot, or nil
// when no snapshot has been published yet (pre-readiness / cache=off).
// Mirrors (*ResourceWatcher).Snapshot but is callable without a watcher
// handle — used by per-cohort helpers (CohortNSACL) that have no
// natural ResourceWatcher binding on the call site.
//
// Lock-free: a single atomic.Pointer load. Production code MUST follow
// the same single-load-per-evaluation invariant the watcher's Snapshot
// method enforces — call ONCE per logical operation and thread the
// returned pointer through sub-reads (see AC-B.3 in rbac_snapshot.go's
// design header).
func LiveRBACSnapshot() *RBACSnapshot {
	return rbacSnap.Load()
}

// RBACSnapshotForTest returns the package-level snapshot for tests in
// this package and in evaltest. Production code uses ResourceWatcher
// .Snapshot(); this getter exists so a test can assert publish
// observability without going through the watcher.
func RBACSnapshotForTest() *RBACSnapshot {
	return rbacSnap.Load()
}

// scheduleRBACRebuild flips the dirty bit; if no rebuild is in flight,
// spawns ONE goroutine that drains the bit by looping: rebuild → check
// dirty → if dirty rebuild again. Exits cleanly when dirty is false on
// the post-rebuild re-check.
//
// Bounded goroutines: at most one in-flight rebuild goroutine.
// `feedback_l1_invalidation_delete_only`-aligned pattern — same shape
// as the L1 refresh atomic.Bool tryLock (Bug 7 fix, watcher.go:1028).
//
// `feedback_no_role_scope_stalls`-aligned: handler bodies are
// non-blocking. The dirty flip + tryLock + maybe-spawn is a few atomics
// — handler returns immediately. The actual indexer walk runs on the
// detached rebuild goroutine.
func scheduleRBACRebuild(rw *ResourceWatcher) {
	// Mark dirty FIRST. If a rebuild is already in flight, it will see
	// this bit on its post-rebuild re-check and loop again.
	rbacRebuildDirty.Store(true)

	// Try to take the writer slot. CompareAndSwap from false→true
	// succeeds for exactly one goroutine; everyone else returns
	// immediately, leaving their dirty flip for the in-flight rebuild
	// to absorb.
	if !rbacRebuildLock.CompareAndSwap(false, true) {
		return
	}

	go func() {
		defer rbacRebuildLock.Store(false)
		// Panic-recovering: a rebuild that crashes must not poison the
		// writer slot for the rest of the process. Recover, log loudly,
		// and re-mark dirty so the NEXT event re-acquires and retries.
		defer func() {
			if r := recover(); r != nil {
				rbacRebuildDirty.Store(true)
				slog.Error("cache.rbac.snapshot.rebuild_panic",
					slog.String("subsystem", "cache"),
					slog.Any("recovered", r),
					slog.String("stack", string(debug.Stack())),
				)
			}
		}()

		// Loop until dirty is false on a fresh check AFTER a rebuild.
		// Each iteration clears dirty BEFORE rebuilding so events arriving
		// MID-rebuild re-flip it and force another iteration. (Clearing
		// AFTER would lose those events.)
		for {
			rbacRebuildDirty.Store(false)
			rebuildRBACSnapshot(rw)
			if !rbacRebuildDirty.Load() {
				return
			}
		}
	}()
}

// rebuildRBACSnapshot walks the 4 RBAC indexers once each and publishes
// a fresh `*RBACSnapshot`. The previous snapshot is replaced
// atomically; existing readers holding the old pointer continue using
// it safely (immutability §3.1 invariant 1).
//
// Cost (INFERRED, design §4.2 — recorded NOT as a hard timeout but as
// a comment for cross-ship verification): on the live cluster
// (N₁=31797 CRBs, N₂=63316 RBs, N₃=31806 CRs, N₄=63314 Rs) this walks
// ~190K pointer copies + ~95K map inserts ≈ 5–10 ms per rebuild on
// commodity hardware. AC-B.12 empirically gates the resulting
// end-to-end mutation propagation under 1 s (100× headroom).
//
// Failure modes:
//   - Indexer item is not a typed pointer (e.g. transform conversion
//     failed and the entry is still *unstructured.Unstructured): the
//     defensive `as{Kind}` fallback at evaluate.go:486-547 will catch it
//     downstream — Ship B's writer skips the entry and continues (a
//     loud WARN, mirroring the 0.30.6 fallback=true invariant).
//   - Indexer for a GVR not yet registered (cache=off race / shutdown):
//     skip the GVR; the missing field in the snapshot will produce
//     denies until the indexer is wired and the next event triggers a
//     rebuild.
func rebuildRBACSnapshot(rw *ResourceWatcher) {
	if rw == nil || rw.mode == modePassthrough {
		return
	}

	// #118 (c)-v2 test-only barrier seam. When a test installs a barrier via
	// SetRebuildBarrierForTest, this blocks the rebuild goroutine here —
	// AFTER the informer event fired (so the pending bump was recorded, or
	// under pre-fix code the synchronous bump already landed) but BEFORE the
	// snapshot Store + flush. The falsifier's arm (ii) uses this to inject a
	// resolve deterministically in the bump→publish window and prove no
	// wrong-under-new-key cell is pinned. Production is unaffected: the hook
	// is nil unless a test sets it, and the read is a single atomic load.
	if b := rebuildBarrierForTest.Load(); b != nil {
		(*b)()
	}

	snap := &RBACSnapshot{
		RoleBindingsByNS:   map[string][]*rbacv1.RoleBinding{},
		ClusterRolesByName: map[string]*rbacv1.ClusterRole{},
		RolesByNSName:      map[string]*rbacv1.Role{},
	}

	// Reserve a sensible initial capacity for the CRB slice. The
	// indexer's List() does its own allocation; the snapshot's slice is
	// fresh.
	//
	// Defensive Unstructured fallback (0.30.6 contract preserved): when
	// the indexer entry is *unstructured.Unstructured rather than the
	// expected typed pointer (typed override disabled for a test, or
	// transform conversion previously failed and was logged WARN at
	// write time), the writer one-shot-converts via FromUnstructured
	// and includes the typed result in the snapshot. WARN-logged once
	// per kind+(ns,name) — the same loud signal the pre-Ship-B
	// as{Kind} helpers emitted at read time. Without this fallback the
	// 0.30.6 fallback equivalence test breaks: a CRB stored as
	// Unstructured would be invisible to EvaluateRBAC.
	if items := indexerList(rw, clusterRoleBindingsTypedGVR); items != nil {
		snap.ClusterRoleBindings = make([]*rbacv1.ClusterRoleBinding, 0, len(items))
		for _, it := range items {
			if crb, ok := it.(*rbacv1.ClusterRoleBinding); ok {
				snap.ClusterRoleBindings = append(snap.ClusterRoleBindings, crb)
				continue
			}
			if crb, ok := convertUnstructuredCRB(it); ok {
				snap.ClusterRoleBindings = append(snap.ClusterRoleBindings, crb)
				continue
			}
			rebuildSkipNonTyped("ClusterRoleBinding", it)
		}
	}

	if items := indexerList(rw, roleBindingsTypedGVR); items != nil {
		for _, it := range items {
			rb, ok := it.(*rbacv1.RoleBinding)
			if !ok {
				if conv, cok := convertUnstructuredRB(it); cok {
					rb = conv
				} else {
					rebuildSkipNonTyped("RoleBinding", it)
					continue
				}
			}
			ns := rb.Namespace
			snap.RoleBindingsByNS[ns] = append(snap.RoleBindingsByNS[ns], rb)
		}
	}

	if items := indexerList(rw, clusterRolesTypedGVR); items != nil {
		for _, it := range items {
			cr, ok := it.(*rbacv1.ClusterRole)
			if !ok {
				if conv, cok := convertUnstructuredCR(it); cok {
					cr = conv
				} else {
					rebuildSkipNonTyped("ClusterRole", it)
					continue
				}
			}
			snap.ClusterRolesByName[cr.Name] = cr
		}
	}

	if items := indexerList(rw, rolesTypedGVR); items != nil {
		for _, it := range items {
			r, ok := it.(*rbacv1.Role)
			if !ok {
				if conv, cok := convertUnstructuredR(it); cok {
					r = conv
				} else {
					rebuildSkipNonTyped("Role", it)
					continue
				}
			}
			key := r.Namespace + "/" + r.Name
			snap.RolesByNSName[key] = r
		}
	}

	// Ship 0.30.169 — populate the subject→bindings indexes BEFORE
	// publishing. Indexes are immutable post-publish; building them
	// after the slice/map fields are stable means a reader that observes
	// snap via Snapshot() sees a fully-formed snapshot, indexes and all.
	rebuildSubjectIndexes(snap)

	// Ship 0.30.242 H.c-layered (Phase 2 step 2a) — atomic publish-seq
	// stamp BEFORE rbacSnap.Store. Pre-ship the writer order was
	// `rbacSnap.Store(snap); rbacSnapshotPublishSeq.Add(1)` — a reader
	// that loaded snap between those two statements would observe a
	// snap whose PublishSeq did not yet exist (or trailed the global
	// counter). The fix: bump the global counter once, stamp it onto
	// the snapshot itself, THEN Store. Any reader observing snap via
	// Snapshot() now sees a snap with its PublishSeq already coherent.
	//
	// Defensive assertion: snap.PublishSeq > 0 guarantees the stamp
	// reached the struct. A bug that surfaced this would manifest as
	// an authz-memo invalidation storm (memo's stamped seq != snap's
	// seq for every lookup) — easy to detect.
	snap.PublishSeq = rbacSnapshotPublishSeq.Add(1)
	if snap.PublishSeq == 0 {
		// rbacSnapshotPublishSeq.Add(1) returns 0 only on wraparound after
		// 2^64 publishes — at 4.6 publishes/sec that's ~127 billion years
		// from now. Still: defence-in-depth — degrade-to-deny rather than
		// silently publish a snapshot whose PublishSeq is the sentinel
		// "no snapshot stamped yet" value.
		slog.Error("cache.rbac.snapshot.publish_seq_wraparound",
			slog.String("subsystem", "cache"),
			slog.String("hint", "rbacSnapshotPublishSeq wrapped — refusing to publish"))
		return
	}
	rbacSnap.Store(snap)

	// #118 (c)-v2 GAP-2 — flush the deferred per-subject sub-gen bumps at the
	// publish barrier, sequenced AFTER rbacSnap.Store. Any request that
	// observes a bumped sub-gen (new resolved key) is thereby guaranteed to
	// observe this snapshot (or a later one) at its EvaluateRBAC refilter —
	// the Store is a release, the reader's Load an acquire, and this bump is
	// sequenced-after the Store on this goroutine. This closes the
	// bump-vs-publish race (c) v1 left open (design §GAP-2 / rbac_subgen_pending.go).
	flushPendingSubGenBumps()

	slog.Debug("cache.rbac.snapshot.published",
		slog.String("subsystem", "cache"),
		slog.Int("crbs", len(snap.ClusterRoleBindings)),
		slog.Int("rb_namespaces", len(snap.RoleBindingsByNS)),
		slog.Int("crs", len(snap.ClusterRolesByName)),
		slog.Int("rs", len(snap.RolesByNSName)),
		slog.Int("crbs_by_user", len(snap.CRBsByUser)),
		slog.Int("crbs_by_group", len(snap.CRBsByGroup)),
		slog.Int("crbs_by_sa", len(snap.CRBsByServiceAccount)),
		slog.Int("crbs_catch_all", len(snap.CRBsCatchAll)),
	)
}

// rebuildSubjectIndexes populates the snapshot's CRBs*/RBs* subject
// indexes from the already-populated ClusterRoleBindings slice and
// RoleBindingsByNS map. Called as the final step of rebuildRBACSnapshot,
// BEFORE the atomic Store, so a reader that observes snap via Snapshot()
// always sees a fully-built snapshot.
//
// Routing — mirrors the architect's §3 case-walk one-to-one:
//
//   - rbacv1.UserKind:           binding → CRBsByUser[s.Name]
//   - rbacv1.GroupKind:          binding → CRBsByGroup[s.Name]
//   - rbacv1.ServiceAccountKind: binding → CRBsByServiceAccount[s.Namespace+"/"+s.Name]
//   - anything else:             binding → CRBsCatchAll  (safety net)
//
// Correctness invariants (HG-169-2):
//   - For any (snap, opts): {crb | crb ∈ ClusterRoleBindings ∧ anySubjectMatches}
//     ⊆ {crb | crb appears in at least one index landing reachable from opts}.
//     Equivalently: the index is a SUPERSET pre-filter. Anything narrower
//     is a permit-loss bug.
//   - A binding with EMPTY Subjects appears in NO index (matches nothing
//     in linear scan; must match nothing in lookup).
//   - A binding with multiple subjects is appended to MULTIPLE indexes;
//     the union+pointer-dedup at lookup time collapses to a single
//     candidate.
//
// Cost: O(Σᵢ |CRBᵢ.Subjects| + Σⱼ |RBⱼ.Subjects|). At 8533 CRBs × ~2
// subjects/binding ≈ 17K pointer-appends + ~3 map-insert/append per
// route. Empirical target: ≤ 100 ms per rebuild (AC-169.7).
//
// Exported indirectly via the snapshot getter; rebuildSubjectIndexes
// itself is package-private — production callers use rebuildRBACSnapshot.
// Tests reach it directly to seed synthetic snapshots without driving
// the full informer-backed rebuild path.
func rebuildSubjectIndexes(snap *RBACSnapshot) {
	if snap == nil {
		return
	}

	// Initial capacities are heuristic — index maps mirror the
	// subject-name cardinality, NOT the binding count. Production
	// observation: ~tens of distinct usernames, ~tens of groups,
	// ~hundreds of SA keys (one per namespace×name).
	snap.CRBsByUser = make(map[string][]*rbacv1.ClusterRoleBinding, 64)
	snap.CRBsByGroup = make(map[string][]*rbacv1.ClusterRoleBinding, 64)
	snap.CRBsByServiceAccount = make(map[string][]*rbacv1.ClusterRoleBinding, 256)
	// CRBsCatchAll left as nil slice until we encounter an
	// unrecognised-Kind subject — most production CRBs route to the
	// per-Kind maps, so the catch-all is typically empty.

	for _, crb := range snap.ClusterRoleBindings {
		if crb == nil {
			continue
		}
		for i := range crb.Subjects {
			s := &crb.Subjects[i] // pointer to avoid copying Subject (8-field struct)
			switch s.Kind {
			case rbacv1.UserKind:
				snap.CRBsByUser[s.Name] = append(snap.CRBsByUser[s.Name], crb)
			case rbacv1.GroupKind:
				snap.CRBsByGroup[s.Name] = append(snap.CRBsByGroup[s.Name], crb)
			case rbacv1.ServiceAccountKind:
				key := s.Namespace + "/" + s.Name
				snap.CRBsByServiceAccount[key] = append(snap.CRBsByServiceAccount[key], crb)
			default:
				// Unrecognised Kind — route to catch-all so the
				// post-lookup anySubjectMatches still sees this CRB.
				// Under-inclusion here = correctness defect (HG-169-2).
				snap.CRBsCatchAll = append(snap.CRBsCatchAll, crb)
			}
		}
	}

	// Per-namespace RoleBinding indexes. Same routing as CRBs, scoped
	// by RoleBinding.Namespace. Outer maps are allocated lazily as
	// namespaces appear.
	snap.RBsByUserByNS = make(map[string]map[string][]*rbacv1.RoleBinding, len(snap.RoleBindingsByNS))
	snap.RBsByGroupByNS = make(map[string]map[string][]*rbacv1.RoleBinding, len(snap.RoleBindingsByNS))
	snap.RBsByServiceAccountByNS = make(map[string]map[string][]*rbacv1.RoleBinding, len(snap.RoleBindingsByNS))
	snap.RBsCatchAllByNS = make(map[string][]*rbacv1.RoleBinding)

	// v7 Step 2A — all-namespace flat reverse indexes, allocated beside the
	// per-ns maps. Capacities mirror the CRB subject-index heuristics
	// (subject-name cardinality, NOT the binding count): ~tens of users /
	// groups, ~hundreds-to-thousands of SA keys. Maps grow as needed;
	// RBsCatchAllAllNS stays nil until an unrecognised-Kind subject appears.
	snap.RBsByUserAllNS = make(map[string][]*rbacv1.RoleBinding, 64)
	snap.RBsByGroupAllNS = make(map[string][]*rbacv1.RoleBinding, 64)
	snap.RBsByServiceAccountAllNS = make(map[string][]*rbacv1.RoleBinding, 256)

	for ns, rbs := range snap.RoleBindingsByNS {
		for _, rb := range rbs {
			if rb == nil {
				continue
			}
			for i := range rb.Subjects {
				s := &rb.Subjects[i]
				switch s.Kind {
				case rbacv1.UserKind:
					inner := snap.RBsByUserByNS[ns]
					if inner == nil {
						inner = make(map[string][]*rbacv1.RoleBinding, 16)
						snap.RBsByUserByNS[ns] = inner
					}
					inner[s.Name] = append(inner[s.Name], rb)
					// v7 Step 2A — flat all-namespace mirror (same key, same
					// pointer).
					snap.RBsByUserAllNS[s.Name] = append(snap.RBsByUserAllNS[s.Name], rb)
				case rbacv1.GroupKind:
					inner := snap.RBsByGroupByNS[ns]
					if inner == nil {
						inner = make(map[string][]*rbacv1.RoleBinding, 16)
						snap.RBsByGroupByNS[ns] = inner
					}
					inner[s.Name] = append(inner[s.Name], rb)
					snap.RBsByGroupAllNS[s.Name] = append(snap.RBsByGroupAllNS[s.Name], rb)
				case rbacv1.ServiceAccountKind:
					inner := snap.RBsByServiceAccountByNS[ns]
					if inner == nil {
						inner = make(map[string][]*rbacv1.RoleBinding, 16)
						snap.RBsByServiceAccountByNS[ns] = inner
					}
					key := s.Namespace + "/" + s.Name
					inner[key] = append(inner[key], rb)
					// Flat mirror uses the SAME "<subject-ns>/<subject-name>"
					// key as the per-ns SA map, so selectRBCandidatesAllNS's SA
					// lookup matches selectRBCandidates's exactly.
					snap.RBsByServiceAccountAllNS[key] = append(snap.RBsByServiceAccountAllNS[key], rb)
				default:
					snap.RBsCatchAllByNS[ns] = append(snap.RBsCatchAllByNS[ns], rb)
					snap.RBsCatchAllAllNS = append(snap.RBsCatchAllAllNS, rb)
				}
			}
		}
	}
}

// indexerList returns the typed-RBAC indexer's List() for gvr, or nil
// when the GVR is not registered. The caller iterates and type-asserts
// to the expected `*rbacv1.{...}` pointer (typed transform guarantees
// this on the happy path; the defensive `rebuildSkipNonTyped` covers
// transform-conversion failure).
func indexerList(rw *ResourceWatcher, gvr schema.GroupVersionResource) []interface{} {
	rw.mu.RLock()
	gi, ok := rw.informers[gvr]
	rw.mu.RUnlock()
	if !ok {
		return nil
	}
	return gi.Informer().GetIndexer().List()
}

// fallbackUnstructuredFromIndexer decodes an indexer entry that is NOT
// the expected typed RBAC pointer into an *unstructured.Unstructured.
// Handles both *bytesObject (the H5 routing-inversion path, reached when
// the typed override has been disabled for a test) and a bare
// *unstructured.Unstructured (test-seeding path). Returns (nil, false)
// for any other shape — the caller logs `rebuildSkipNonTyped`.
func fallbackUnstructuredFromIndexer(obj interface{}) (*unstructured.Unstructured, bool) {
	return decodeBytesObject(obj)
}

// convertUnstructuredCRB attempts the defensive Unstructured→typed
// fallback (0.30.6 contract). Returns (typed, true) on success; logs
// WARN and returns (nil, false) on conversion failure. The WARN
// matches the pre-Ship-B as{Kind} WARN's "loud" semantics so a
// transform-conversion regression remains visible at write time.
//
// Accepts the raw indexer entry (which may be *unstructured.Unstructured
// OR *bytesObject when the typed override is absent and the H5 bytes
// path applied) — fallbackUnstructuredFromIndexer normalises both.
func convertUnstructuredCRB(obj interface{}) (*rbacv1.ClusterRoleBinding, bool) {
	uns, ok := fallbackUnstructuredFromIndexer(obj)
	if !ok || uns == nil {
		return nil, false
	}
	out := &rbacv1.ClusterRoleBinding{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(uns.Object, out); err != nil {
		slog.Warn("cache.rbac.snapshot.unstructured_convert_failed",
			slog.String("subsystem", "cache"),
			slog.String("kind", "ClusterRoleBinding"),
			slog.String("name", uns.GetName()),
			slog.String("error", err.Error()),
		)
		return nil, false
	}
	slog.Warn("cache.rbac.snapshot.unstructured_fallback",
		slog.String("subsystem", "cache"),
		slog.String("kind", "ClusterRoleBinding"),
		slog.String("name", uns.GetName()),
		slog.String("hint", "indexer entry was Unstructured/bytesObject — typed transform did not fire; "+
			"converted at snapshot-rebuild time (mirrors 0.30.6 fallback=true)"),
	)
	return out, true
}

func convertUnstructuredRB(obj interface{}) (*rbacv1.RoleBinding, bool) {
	uns, ok := fallbackUnstructuredFromIndexer(obj)
	if !ok || uns == nil {
		return nil, false
	}
	out := &rbacv1.RoleBinding{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(uns.Object, out); err != nil {
		slog.Warn("cache.rbac.snapshot.unstructured_convert_failed",
			slog.String("subsystem", "cache"),
			slog.String("kind", "RoleBinding"),
			slog.String("name", uns.GetName()),
			slog.String("namespace", uns.GetNamespace()),
			slog.String("error", err.Error()),
		)
		return nil, false
	}
	slog.Warn("cache.rbac.snapshot.unstructured_fallback",
		slog.String("subsystem", "cache"),
		slog.String("kind", "RoleBinding"),
		slog.String("name", uns.GetName()),
		slog.String("namespace", uns.GetNamespace()),
		slog.String("hint", "indexer entry was Unstructured/bytesObject — typed transform did not fire; "+
			"converted at snapshot-rebuild time"),
	)
	return out, true
}

func convertUnstructuredCR(obj interface{}) (*rbacv1.ClusterRole, bool) {
	uns, ok := fallbackUnstructuredFromIndexer(obj)
	if !ok || uns == nil {
		return nil, false
	}
	out := &rbacv1.ClusterRole{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(uns.Object, out); err != nil {
		slog.Warn("cache.rbac.snapshot.unstructured_convert_failed",
			slog.String("subsystem", "cache"),
			slog.String("kind", "ClusterRole"),
			slog.String("name", uns.GetName()),
			slog.String("error", err.Error()),
		)
		return nil, false
	}
	slog.Warn("cache.rbac.snapshot.unstructured_fallback",
		slog.String("subsystem", "cache"),
		slog.String("kind", "ClusterRole"),
		slog.String("name", uns.GetName()),
		slog.String("hint", "indexer entry was Unstructured/bytesObject — typed transform did not fire; "+
			"converted at snapshot-rebuild time"),
	)
	return out, true
}

func convertUnstructuredR(obj interface{}) (*rbacv1.Role, bool) {
	uns, ok := fallbackUnstructuredFromIndexer(obj)
	if !ok || uns == nil {
		return nil, false
	}
	out := &rbacv1.Role{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(uns.Object, out); err != nil {
		slog.Warn("cache.rbac.snapshot.unstructured_convert_failed",
			slog.String("subsystem", "cache"),
			slog.String("kind", "Role"),
			slog.String("name", uns.GetName()),
			slog.String("namespace", uns.GetNamespace()),
			slog.String("error", err.Error()),
		)
		return nil, false
	}
	slog.Warn("cache.rbac.snapshot.unstructured_fallback",
		slog.String("subsystem", "cache"),
		slog.String("kind", "Role"),
		slog.String("name", uns.GetName()),
		slog.String("namespace", uns.GetNamespace()),
		slog.String("hint", "indexer entry was Unstructured/bytesObject — typed transform did not fire; "+
			"converted at snapshot-rebuild time"),
	)
	return out, true
}

// rebuildSkipNonTyped logs a WARN when the writer encounters an indexer
// entry that is neither a typed pointer NOR an *unstructured.Unstructured
// it can fall back to (e.g. *bytesObject — bytes routing is excepted
// for RBAC GVRs, but defense-in-depth). Mirrors the 0.30.6
// `fallback=true` invariant. The writer skips the entry; the downstream
// path cannot serve it.
func rebuildSkipNonTyped(kind string, obj interface{}) {
	name, namespace := "", ""
	if uns, ok := obj.(*unstructured.Unstructured); ok {
		name = uns.GetName()
		namespace = uns.GetNamespace()
	}
	slog.Warn("cache.rbac.snapshot.skip_non_typed",
		slog.String("subsystem", "cache"),
		slog.String("kind", kind),
		slog.String("name", name),
		slog.String("namespace", namespace),
		slog.String("got_type", goTypeOf(obj)),
		slog.String("hint", "indexer entry was not a typed *rbacv1.* pointer — "+
			"typed transform did not fire on this object (mirrors 0.30.6 fallback=true)"),
	)
}

// goTypeOf is the no-reflect equivalent of fmt.Sprintf("%T", obj) for a
// short class of expected types — used by the rebuild's WARN log. Falls
// back to "<unknown>" rather than reflect because the WARN runs on a
// processor goroutine and we keep it allocation-cheap.
func goTypeOf(obj interface{}) string {
	switch obj.(type) {
	case *rbacv1.ClusterRoleBinding:
		return "*rbacv1.ClusterRoleBinding"
	case *rbacv1.RoleBinding:
		return "*rbacv1.RoleBinding"
	case *rbacv1.ClusterRole:
		return "*rbacv1.ClusterRole"
	case *rbacv1.Role:
		return "*rbacv1.Role"
	case *unstructured.Unstructured:
		return "*unstructured.Unstructured"
	case *bytesObject:
		return "*cache.bytesObject"
	case nil:
		return "<nil>"
	default:
		return "<other>"
	}
}

// well-known typed RBAC GVRs — must match the writer-side population in
// strip.go's `rbacTypedGVRs` (strip.go:101-106). These are private
// duplicates because internal/rbac's own GVR vars are in a different
// package; Ship B's writer lives in cache. If `rbacTypedGVRs` ever
// grows, these vars + RBACSnapshot's fields + the rbac/evaluate.go
// reader sites must all be extended in lockstep (see AC-B.10).
//
// `feedback_no_special_cases`-compliant: these are the same GVRs the
// typed-RBAC indexer already discriminates on (strip.go:101-106); Ship B
// reads from the same set, not a new list.
var (
	clusterRoleBindingsTypedGVR = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}
	roleBindingsTypedGVR        = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}
	clusterRolesTypedGVR        = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	rolesTypedGVR               = schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}
)

// isTypedRBACGVR reports whether gvr is one of the four RBAC GVRs the
// snapshot tracks. The watcher's addResourceTypeLocked uses this to
// decide whether to attach the snapshot-rebuild event handler.
//
// Driven by the existing `rbacTypedGVRs` set in strip.go — Ship B does
// NOT introduce a new GVR list. Per `feedback_no_special_cases` the
// participating GVRs come from the same source of truth as the typed
// transforms.
func isTypedRBACGVR(gvr schema.GroupVersionResource) bool {
	for _, g := range rbacTypedGVRs {
		if g == gvr {
			return true
		}
	}
	return false
}

// rbacSnapshotEventHandlers builds the informer event-handler set that
// schedules a snapshot rebuild on ADD/UPDATE/DELETE. Wired by
// addResourceTypeLocked for each of the 4 typed-RBAC GVRs alongside the
// existing `depEventHandlers` (deps_watch.go).
//
// All three callbacks call scheduleRBACRebuild(rw) (the wholesale snapshot
// used by EvaluateRBAC) AND, for Ship 1's incremental BindingsByGVR index,
// the per-event index delta hooks (bindings_by_gvr_delta.go). The two are
// independent: the wholesale rebuild stays the authz boundary; the index
// delta is seed-targeting only.
//
// gvr discriminates a binding event (CRB/RB → onBinding{Add,Update,Delete})
// from a role-rule event (ClusterRole/Role → onRoleRulesChanged). The
// index hooks no-op until the index is built (deltaActive gate) so a
// pre-build replay event is free; the wholesale rebuild scheduling is
// unchanged. The handler bodies stay non-blocking: scheduleRBACRebuild is
// a few atomics, and the index delta is O(navigatedGVRs) map ops under the
// index lock (Gate-2: ~5.8 µs/event).
func (rw *ResourceWatcher) rbacSnapshotEventHandlers(gvr schema.GroupVersionResource) clientcache.ResourceEventHandlerFuncs {
	isBinding := gvr == clusterRoleBindingsTypedGVR || gvr == roleBindingsTypedGVR
	return clientcache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			scheduleRBACRebuild(rw)
			if isBinding {
				onBindingAdd(obj)
			} else {
				onRoleObjectChanged(obj)
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			scheduleRBACRebuild(rw)
			if isBinding {
				onBindingUpdate(oldObj, newObj)
			} else {
				onRoleObjectChanged(newObj)
			}
		},
		DeleteFunc: func(obj interface{}) {
			scheduleRBACRebuild(rw)
			if isBinding {
				onBindingDelete(obj)
			} else {
				onRoleObjectChanged(obj)
			}
		},
	}
}

// init registers the typed-RBAC-snapshot handler-extension with the
// cache-package declarative registry (Ship 0 / 0.30.222). Before Ship 0
// the watcher's addResourceTypeLocked / addResourceTypeMetadataOnlyLocked
// branched inline on `if isTypedRBACGVR(gvr)` and attached
// rbacSnapshotEventHandlers directly. Ship 0 generalises that branch
// (alongside the CRD-watch composition-auto-discovery branch) into the
// declarative registry; addResourceType*Locked now iterates blind.
//
// The handler set itself (AddFunc / UpdateFunc / DeleteFunc routing
// through scheduleRBACRebuild + the BindingsByGVR index delta) is
// byte-identical to the pre-Ship-0 inline branch — only the call-site
// indirection changes.
func init() {
	RegisterHandlerExtension(HandlerExtension{
		Name:      "rbac.snapshot_writer",
		Predicate: isTypedRBACGVR,
		Handlers: func(rw *ResourceWatcher, gvr schema.GroupVersionResource) clientcache.ResourceEventHandler {
			handlers := rw.rbacSnapshotEventHandlers(gvr)
			// markRBACSnapshotWired records that at least one typed-RBAC
			// informer was wired with the snapshot event handler — the
			// pre-condition AssertRBACSnapshotWired enforces at boot.
			markRBACSnapshotWired()
			return handlers
		},
	})
}

// rbacSnapshotWired records whether the watcher has at least one of
// the typed-RBAC informers wired with the snapshot event handler. Set
// at NewResourceWatcher time; AssertRBACSnapshotWired panics at boot if
// it ever stays false despite the 4 RBAC informers being registered.
var rbacSnapshotHandlerWired atomic.Bool

// markRBACSnapshotWired flips the wired flag. Called by the
// rbac.snapshot_writer HandlerExtension's Handlers factory (init() above)
// once per typed-RBAC GVR as the attach occurs.
func markRBACSnapshotWired() {
	rbacSnapshotHandlerWired.Store(true)
}

// rbacSnapshotAssertionDisabled, when true, makes AssertRBACSnapshotWired
// a no-op. Set only by `DisableRBACSnapshotForTest` so a test that
// deliberately exercises the no-snapshot pre-readiness path doesn't
// trip the startup assertion.
var rbacSnapshotAssertionDisabled atomic.Bool

// AssertRBACSnapshotWired panics if the 4 RBAC informers are registered
// (typed overrides registered → addResourceTypeLocked must have been
// called for each) but the snapshot event handler was never attached.
// Analogous to AssertRBACTypedOverridesRegistered (strip.go:173); makes
// a snapshot-wiring regression loud at boot, not silent at request time
// (design AC-B.8).
//
// Called by NewResourceWatcher after the constructor's eager RBAC
// registration loop runs and the initial snapshot has been published.
// Tests that need to exercise the no-snapshot path (degrade-to-deny)
// call DisableRBACSnapshotForTest first.
func AssertRBACSnapshotWired() {
	if rbacSnapshotAssertionDisabled.Load() {
		return
	}
	if !rbacSnapshotHandlerWired.Load() {
		panic("cache: RBAC snapshot event handler not wired — " +
			"addResourceTypeLocked must call markRBACSnapshotWired() for every typed-RBAC GVR " +
			"(regression: Ship B writer wiring missing)")
	}
}

// DisableRBACSnapshotForTest disables the snapshot-wired startup
// assertion AND clears any already-published snapshot. Returns a
// restore function that re-enables the assertion (test responsibility:
// invoke via t.Cleanup). The cleared snapshot is NOT automatically
// restored — tests that need the previous snapshot must capture and
// re-publish it explicitly.
//
// Production callers MUST NOT call this. WARN-logged on every
// invocation so accidental use in non-test code is loud.
func DisableRBACSnapshotForTest() func() {
	slog.Warn("cache.DisableRBACSnapshotForTest invoked — production code MUST NOT call this",
		slog.String("subsystem", "cache"),
	)
	prev := rbacSnapshotAssertionDisabled.Swap(true)
	saved := rbacSnap.Load()
	rbacSnap.Store(nil)
	return func() {
		rbacSnapshotAssertionDisabled.Store(prev)
		// Do NOT auto-restore `saved` — tests that want it re-published
		// must do so explicitly via the public Snapshot() observers, so
		// we don't surprise them with a stale snapshot.
		_ = saved
	}
}

// PublishRBACSnapshotForTest installs `s` as the current snapshot. Used
// only by tests that build snapshots manually (e.g. the
// TestRBACSnapshot_Equivalence + TestRBACSnapshot_DegradeToDeny suites).
// Production code MUST NOT call this — production publishes go through
// `scheduleRBACRebuild` → `rebuildRBACSnapshot` only.
func PublishRBACSnapshotForTest(s *RBACSnapshot) {
	rbacSnap.Store(s)
}

// BumpRBACGenForTest increments rbacSnapshotPublishSeq so RBACGen() reports a
// published snapshot (>0), letting a handler test simulate the WARM readiness
// state (#68: refreshWarmupIncomplete keys on RBACGen()==0 as one warmup
// disjunct). Production bumps this only via rebuildRBACSnapshot's publish.
// ResetRBACGenForTest restores it to 0 (the pre-readiness state).
func BumpRBACGenForTest()  { rbacSnapshotPublishSeq.Add(1) }
func ResetRBACGenForTest() { rbacSnapshotPublishSeq.Store(0) }

// RebuildSubjectIndexesForTest exposes the unexported
// rebuildSubjectIndexes for tests that hand-build snapshots and need
// to populate the CRBsBy*/RBsBy* subject-index maps (which the rbac
// evaluator's selectCRBCandidates/selectRBCandidates require).
//
// Production code uses rebuildRBACSnapshot (which calls
// rebuildSubjectIndexes internally before publishing) — never this.
// Ship 0.30.242 H.c-layered Phase 3 F3 added this seam for the mid-
// test mutation phase (synthetic-snapshot publish path).
func RebuildSubjectIndexesForTest(s *RBACSnapshot) {
	rebuildSubjectIndexes(s)
}

// RebuildRBACSnapshotForTest publicly exposes a synchronous snapshot
// rebuild for tests. Production code uses `scheduleRBACRebuild` (which
// is asynchronous, bounded, and dirty-flag-coalesced) — never this.
func RebuildRBACSnapshotForTest(rw *ResourceWatcher) {
	rebuildRBACSnapshot(rw)
}

// waitAndPublishInitialRBACSnapshot is the initial-publish goroutine
// spawned by NewResourceWatcher (Ship B / AC-B.9). It blocks until all
// 4 RBAC syncCh channels close — the "Servable" signal that the
// informer's initial LIST has reconciled — then runs
// rebuildRBACSnapshot synchronously to publish the first snapshot.
//
// Before this goroutine completes, rbacSnap.Load() returns nil and
// EvaluateRBAC's AC-B.8 degrade-to-deny fires for every request. After
// the publish, every subsequent ADD/UPDATE/DELETE flows through the
// event handlers → scheduleRBACRebuild for incremental updates.
//
// Exits early (no publish) if rw.stopCh closes mid-wait — process
// shutdown.
func waitAndPublishInitialRBACSnapshot(rw *ResourceWatcher) {
	// Snapshot the 4 RBAC sync channels under the watcher lock. The
	// channels are allocated by addResourceTypeLocked and never
	// re-allocated for the same GVR, so capturing the pointers here is
	// safe.
	rw.mu.RLock()
	channels := make([]chan struct{}, 0, len(rbacTypedGVRs))
	for _, gvr := range rbacTypedGVRs {
		if ch, ok := rw.syncCh[gvr]; ok && ch != nil {
			channels = append(channels, ch)
		}
	}
	rw.mu.RUnlock()

	if len(channels) == 0 {
		slog.Warn("cache.rbac.snapshot.initial_publish_skipped",
			slog.String("subsystem", "cache"),
			slog.String("hint", "no RBAC syncCh channels at NewResourceWatcher end — "+
				"snapshot will publish on the first ADD event instead (degrade-to-deny "+
				"covers the gap)"),
		)
		return
	}

	for _, ch := range channels {
		select {
		case <-ch:
			// closed → informer synced
		case <-rw.stopCh:
			slog.Info("cache.rbac.snapshot.initial_publish_aborted",
				slog.String("subsystem", "cache"),
				slog.String("hint", "stopCh closed before initial RBAC sync — process shutdown"),
			)
			return
		}
	}

	// All 4 informers are synced — publish the initial snapshot
	// synchronously so the very next EvaluateRBAC call sees it.
	rebuildRBACSnapshot(rw)
	slog.Info("cache.rbac.snapshot.initial_publish_done",
		slog.String("subsystem", "cache"),
		slog.String("hint", "first typed-RBAC snapshot is live; "+
			"subsequent updates ride scheduleRBACRebuild"),
	)
}
