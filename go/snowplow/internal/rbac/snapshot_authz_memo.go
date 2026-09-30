// snapshot_authz_memo.go — Ship L2 (0.30.253), Task #291.
//
// A process-wide, snapshot-GENERATION-scoped memo of EvaluateRBAC
// verdicts, shared by ALL callers (the six per-item refilter/dispatch/
// serve sites AND the identity-free perpetual refresher). It collapses
// the dominant post-L1 cost: the O(candidate-CRB) walk in
// evaluateAgainstInformerFirstMatch, which at 50K scale re-walks the
// 17,929 same-subject ClusterRoleBindings on EVERY EvaluateRBAC call for
// a verdict that is identical across thousands of repetitions within and
// across refilter passes / refresher cycles (design §0/§3).
//
// WHY NOT a per-/call-request memo (the original Phase-2b approach, the
// per-request RequestAuthzMemo — deleted in #176): that memo was
// per-/call-request, reborn every request and every refresher cycle, and
// single-goroutine by design. It gave the identity-free perpetual refresher
// nothing across cycles. L2 needs a shared, generation-scoped, CONCURRENT
// memo. Per the design (§2) this is a process-wide object that lives in
// internal/rbac (not internal/cache) so it can read
// *cache.RBACSnapshot.PublishSeq directly with no hook indirection
// (design §4 placement note).
//
// CORRECTNESS — generation binding (design §3.3, PM B1):
//   - The memo is a single atomic.Pointer[snapshotAuthzShard]; each shard
//     carries the generation (snap.PublishSeq) it is valid for, its map,
//     and an RWMutex.
//   - On EVERY lookup the shard's gen is compared to the PublishSeq of
//     the snapshot the caller ALREADY loaded (evaluate.go's `snap`). No
//     second snapshot load, no TTL, no time-based invalidation (B1).
//   - On gen mismatch the shard is stale: CAS a fresh empty shard stamped
//     with the new gen into the pointer (lost CAS race => another
//     goroutine already swapped; reload and use theirs). The whole-shard
//     swap is the primary eviction — entries NEVER outlive their snapshot,
//     so no stale verdict can ever be served across a republish.
//
// CONCURRENCY (design §7 / PM B2 / feedback_shared_vs_copy_is_a_concurrency_change):
// unlike a per-request memo this shard is SHARED MUTABLE state hit by many
// goroutines concurrently with snapshot republishes. The RWMutex guards
// the map; the atomic.Pointer guards the shard swap. F2 (-race republish
// hammer) is the hard gate on this file.
//
// CAPACITY (design §3.4 / PM B5 / feedback_capacity_caps_empirical_per_entry_cost):
// cap is EMPIRICALLY derived (see /tmp/b5-authz-memo-cardinality-0.30.252.txt):
// measured steady-state upper-bound cardinality ~6,300 keys/generation ×
// safety-multiplier 2 -> rounded to 16,384 (2^14). On cap breach the memo
// REFUSES to insert (the caller still gets the freshly-walked verdict —
// it just isn't cached), degrading that key to today's walk behaviour.
// Never evict-to-OOM.
//
// REMOVABILITY (project_caching_is_provisional / project_single_cache_flag_direction):
// the memo lookup sits AFTER the cache=off short-circuit in EvaluateRBAC,
// so CACHE_ENABLED=false never reaches it (cache=off is the SAR
// correctness baseline and must never be memoised). No new flag.

package rbac

import (
	"expvar"
	"sync"
	"sync/atomic"
)

// snapshotAuthzMemoCap is the per-generation entry cap. Derived
// empirically in the B5 pre-coding measurement (artifact
// /tmp/b5-authz-memo-cardinality-0.30.252.txt): measured upper-bound
// cardinality ~6,300 × safety-multiplier 2 ≈ 12,600, rounded up to the
// next power of two. NOT a design-time guess.
const snapshotAuthzMemoCap = 16384

// snapshotAuthzKey is the generic EvaluateOptions tuple plus the
// generation and the SkipUID discriminator. NO SA/CRB special-case
// (feedback_no_special_cases): the 17,929-CRB shape is never named here;
// it collapses simply because the tuple repeats.
//
// Gen is the RBACSnapshot.PublishSeq the entry was computed against.
// Gen-in-key is LOAD-BEARING (not mere defence-in-depth) during the
// store-race window: if a concurrent store carrying an older Gen lands
// in a shard that a faster goroutine has already CAS-advanced to a newer
// Gen — or if the atomic pointer is transiently observed pointing at a
// backwards generation under interleaving — the entry's Gen no longer
// equals the looking-up generation, so the lookup MISSES and re-walks
// rather than serving a wrong-generation verdict. The whole-shard swap
// is the primary invalidation; Gen-in-key is what closes the swap's
// race window so a transiently-backwards shard can never serve stale.
type snapshotAuthzKey struct {
	Gen        uint64
	Username   string
	GroupsHash uint64 // canonicalGroupsHash(opts.Groups) — groups_hash.go
	Verb       string
	Group      string
	Resource   string
	Namespace  string
	Name       string
	SkipUID    bool
}

// snapshotAuthzVerdict is the cached result — exactly EvaluateRBAC's
// (allowed, matchedBindingUID) contract. Evaluator ERRORS are NOT cached
// (the evaluator's own error path is fail-closed-per-call and transient;
// caching an error would pin a transient failure for a whole generation).
type snapshotAuthzVerdict struct {
	Allowed           bool
	MatchedBindingUID string
	// WinningSubjectClass — #261 case-2. The FIRST-permitting binding's
	// winning-subject class, persisted on the verdict at the EvaluateRBAC build
	// site so a memo HIT (the hot at-risk serve path) surfaces it via
	// EvaluateOptions.WinningSubjectClassOut. Field-transparent through
	// storeWithCap (dev-271b confirmed #327's memo-cap rework is field-agnostic);
	// it adds no cap/cardinality impact (not part of snapshotAuthzKey). Only
	// meaningful when Allowed (denies are never cached).
	WinningSubjectClass WinningSubjectClass
}

// snapshotAuthzShard is one generation's worth of memoised verdicts. The
// shard is immutable in its `gen` field once published; only `m` mutates,
// guarded by `mu`. A new generation gets a brand-new shard via CAS.
type snapshotAuthzShard struct {
	gen uint64
	mu  sync.RWMutex
	m   map[snapshotAuthzKey]snapshotAuthzVerdict
}

// snapshotAuthzMemo is the process-wide singleton. The atomic pointer is
// swapped (CAS) on generation change; readers load it once per lookup.
var snapshotAuthzMemo atomic.Pointer[snapshotAuthzShard]

// authz memo expvar counters (design §3 / wiring item 5). Registered via
// RegisterAuthzMemoExpvar (snapshot_authz_memo_expvar.go).
var (
	authzMemoHits    atomic.Uint64
	authzMemoMisses  atomic.Uint64
	authzMemoSwaps   atomic.Uint64
	authzMemoRefused atomic.Uint64 // cap-breach refused inserts (F5 / B5)
	// authzMemoDenyUncached — Ship 0.30.254 / Task #301: count of deny
	// verdicts deliberately NOT cached (they fall back to the walk every
	// call so a transiently-wrong fail-closed deny self-heals). > 0 and
	// rising under load is the #301 falsifier that denies take the walk
	// path.
	authzMemoDenyUncached atomic.Uint64
)

// currentAuthzShard returns the shard valid for gen, swapping in a fresh
// empty shard (CAS) if the live shard is absent or stale. The returned
// shard is guaranteed to have shard.gen == gen.
//
// Lost-CAS handling: if another goroutine swapped concurrently we reload
// and, if its gen matches, use it; otherwise (it swapped to a DIFFERENT
// gen — only possible if a third republish raced) we retry. Bounded retry
// loop — generations are monotone so it converges immediately in practice.
func currentAuthzShard(gen uint64) *snapshotAuthzShard {
	for {
		cur := snapshotAuthzMemo.Load()
		if cur != nil && cur.gen == gen {
			return cur
		}
		fresh := &snapshotAuthzShard{
			gen: gen,
			m:   make(map[snapshotAuthzKey]snapshotAuthzVerdict, 64),
		}
		if snapshotAuthzMemo.CompareAndSwap(cur, fresh) {
			authzMemoSwaps.Add(1)
			return fresh
		}
		// Lost the race — re-evaluate the loaded pointer next iteration.
	}
}

// authzMemoLookup returns (verdict, true) on a hit for the given
// generation+key, or (_, false) on miss. It performs the generation
// guard via currentAuthzShard and a read-locked map read. Pure read; no
// store.
func authzMemoLookup(gen uint64, key snapshotAuthzKey) (snapshotAuthzVerdict, bool) {
	shard := currentAuthzShard(gen)
	shard.mu.RLock()
	v, ok := shard.m[key]
	shard.mu.RUnlock()
	if ok {
		authzMemoHits.Add(1)
	} else {
		authzMemoMisses.Add(1)
	}
	return v, ok
}

// authzMemoStore records a freshly-computed verdict for the
// generation+key. Refuses the insert (no-op, counted) when the shard is
// already at the empirical cap — degrading that key to the walk on the
// next lookup rather than risking unbounded growth (PM B5). A repeated
// store of an existing key (idempotent overwrite) is always allowed so a
// hot key never gets stuck refused after the map fills.
func authzMemoStore(gen uint64, key snapshotAuthzKey, v snapshotAuthzVerdict) {
	currentAuthzShard(gen).storeWithCap(key, v)
}

// storeWithCap records v under key on THIS shard, refusing a NEW key (no-op,
// counted via authzMemoRefused) once the shard is at snapshotAuthzMemoCap,
// while an idempotent overwrite of an existing key is always allowed so a hot
// key never gets stuck refused after the map fills. Extracted from
// authzMemoStore (behaviour-identical) so the cap-refuse-never-evict invariant
// can be exercised on a PINNED shard — immune to the generation pointer swap
// (currentAuthzShard's CAS) that a background republish performs — in a
// package-rbac unit test (#327), instead of only through the swap-prone
// end-to-end EvaluateRBAC path.
func (s *snapshotAuthzShard) storeWithCap(key snapshotAuthzKey, v snapshotAuthzVerdict) {
	s.mu.Lock()
	if _, exists := s.m[key]; !exists && len(s.m) >= snapshotAuthzMemoCap {
		s.mu.Unlock()
		authzMemoRefused.Add(1)
		return
	}
	s.m[key] = v
	s.mu.Unlock()
}

// authzMemoEntriesForExpvar returns the current shard's entry count for
// the snowplow_authz_memo_entries expvar. Read-locked snapshot.
func authzMemoEntriesForExpvar() int {
	cur := snapshotAuthzMemo.Load()
	if cur == nil {
		return 0
	}
	cur.mu.RLock()
	n := len(cur.m)
	cur.mu.RUnlock()
	return n
}

// ResetAuthzMemoForTest clears the memo singleton and all counters.
// TEST-ONLY (evaltest F2/F3/F5) — production code MUST NOT call it.
func ResetAuthzMemoForTest() {
	snapshotAuthzMemo.Store(nil)
	authzMemoHits.Store(0)
	authzMemoMisses.Store(0)
	authzMemoSwaps.Store(0)
	authzMemoRefused.Store(0)
	authzMemoDenyUncached.Store(0)
}

// AuthzMemoStatsForTest returns (hits, misses, swaps, refused, entries).
// TEST-ONLY instrumentation for F5; mirrors the expvar values.
func AuthzMemoStatsForTest() (hits, misses, swaps, refused uint64, entries int) {
	return authzMemoHits.Load(), authzMemoMisses.Load(), authzMemoSwaps.Load(),
		authzMemoRefused.Load(), authzMemoEntriesForExpvar()
}

// AuthzShardCapInvariantForTest exercises the cap-refuse-never-evict invariant
// of a single shard on a PINNED shard, immune BY CONSTRUCTION to the
// generation pointer swap that currentAuthzShard performs on a background
// snapshot republish (the correct per-gen invalidation) — the swap that made
// the old end-to-end evaltest fill flake 2/3 under -count (#241-M8 / #327). It
// resets the memo, pins ONE shard, fills it past snapshotAuthzMemoCap with
// distinct keys via storeWithCap, then re-stores an EXISTING key to check the
// idempotent-overwrite exemption. Returns the post-fill entry count, the count
// of inserts refused during the fill, and whether the idempotent overwrite
// grew the map (it must not). TEST-ONLY (evaltest) — production code MUST NOT
// call it. No shard type escapes the signature (plain ints/bool); the
// assertions live in the caller.
func AuthzShardCapInvariantForTest() (entries int, refusedDelta uint64, overwriteGrew bool) {
	ResetAuthzMemoForTest()
	const overfill = snapshotAuthzMemoCap + 500
	// Pin one shard: a background republish swaps the GLOBAL pointer to a fresh
	// shard at a new gen, but this captured object is filled directly.
	shard := currentAuthzShard(1)
	permit := snapshotAuthzVerdict{Allowed: true}
	// Distinct keys via GroupsHash (one new key each) — no fmt dependency.
	mkKey := func(i int) snapshotAuthzKey {
		return snapshotAuthzKey{Gen: 1, Username: "alice", Verb: "get", Resource: "secrets", Namespace: "default", GroupsHash: uint64(i)}
	}
	before := authzMemoRefused.Load()
	for i := 0; i < overfill; i++ {
		shard.storeWithCap(mkKey(i), permit)
	}
	shard.mu.RLock()
	entries = len(shard.m)
	shard.mu.RUnlock()
	refusedDelta = authzMemoRefused.Load() - before

	// Idempotent overwrite of an EXISTING key (one of the first cap stored)
	// must be allowed and must NOT grow the map (exempt from the cap refuse).
	shard.mu.RLock()
	lenBefore := len(shard.m)
	shard.mu.RUnlock()
	shard.storeWithCap(mkKey(0), snapshotAuthzVerdict{Allowed: true, MatchedBindingUID: "u"})
	shard.mu.RLock()
	overwriteGrew = len(shard.m) != lenBefore
	shard.mu.RUnlock()
	return entries, refusedDelta, overwriteGrew
}

// AuthzMemoDenyUncachedForTest returns the count of deny verdicts not
// cached (Ship 0.30.254 / #301). TEST-ONLY — the deny-not-stored
// falsifier asserts this increments on a deny.
func AuthzMemoDenyUncachedForTest() uint64 {
	return authzMemoDenyUncached.Load()
}

// AuthzMemoSnapshot returns the live snapshot-authz memo counters
// (hits, misses, swaps, refused, denyUncached, entries) mirroring the
// snowplow_authz_memo_* expvar keys. Exported (unlike AuthzMemoStatsForTest,
// which is test-only and resettable) so the OTel metrics mirror
// (internal/metrics) can observe the same values as /debug/vars in
// production.
func AuthzMemoSnapshot() (hits, misses, swaps, refused, denyUncached uint64, entries int) {
	return authzMemoHits.Load(), authzMemoMisses.Load(), authzMemoSwaps.Load(),
		authzMemoRefused.Load(), authzMemoDenyUncached.Load(), authzMemoEntriesForExpvar()
}

// authzMemoExpvarFuncs is consumed by RegisterAuthzMemoExpvar; kept here
// so the counters' wiring stays adjacent to their definitions.
func authzMemoExpvarFuncs() map[string]expvar.Func {
	return map[string]expvar.Func{
		"snowplow_authz_memo_hits":    func() any { return authzMemoHits.Load() },
		"snowplow_authz_memo_misses":  func() any { return authzMemoMisses.Load() },
		"snowplow_authz_memo_swaps":   func() any { return authzMemoSwaps.Load() },
		"snowplow_authz_memo_refused": func() any { return authzMemoRefused.Load() },
		"snowplow_authz_memo_entries": func() any { return authzMemoEntriesForExpvar() },
		// Ship 0.30.254 / #301 — denies are never cached; this counts them.
		"snowplow_authz_memo_deny_uncached_total": func() any { return authzMemoDenyUncached.Load() },
	}
}
