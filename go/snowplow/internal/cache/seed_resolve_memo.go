// seed_resolve_memo.go — the #130 F4 per-seed-pass RA-resolve memo.
//
// THE DEFECT (docs/f4-statwidget-apiref-cost-design-2026-07-12.md): the boot
// seed resolves ~71 statistics/tag widgets whose apiRef points at one of a
// SMALL set of heavy RESTActions (compositions-list / dashboard-data). Each
// such RESTAction runs ~10 full gojq passes over the 60K-item benchapps array
// — ~18s of CPU — and the seed runs it TWICE per widget (the paginated
// widgets.Resolve pass AND the unpaginated seedRAFullListForWidget accelerator),
// once per cohort. The I/O is already cheap (informer-served, F3b Fix 2:
// content_hits=29/misses=0) — the residual cost is pure jq CPU over a list that
// is IDENTICAL across every widget sharing that (RA, identity, page). ~71
// widgets × 2 passes × N cohorts collapses to (#distinct RA) × (#cohorts) ×
// (#distinct pages) real resolves once identical (RA, identity, page, extras)
// resolves are memoized.
//
// WHY A CONTEXT-CARRIED MEMO (not a process-global cache) — mirrors
// StageErrorSink / ExternalTouchedSink (the established per-resolve sink idiom):
//
//   - CORRECTNESS / TEARDOWN: the memo is installed by the seed pass
//     (withCohortSeedContext) and lives ONLY for that pass's context. It is
//     torn down when the pass's context goes away — it can never serve a stale
//     RA body to a later seed pass or (critically) to a user /call. A user
//     /call context never carries a memo, so SeedResolveMemoFromContext returns
//     nil there and the memo is a strict no-op on the request path (C-F4-8).
//
//   - RBAC SAFETY (C-F4-4): the memo KEY folds the full RBAC-determining
//     identity — username + sorted groups, exactly the (Username, Groups) that
//     internal/resolvers/restactions/api/refilter.go + cluster_list.go +
//     apiref/ra_full_list.go read off ctx to FILTER the list. Two cohorts with
//     divergent RBAC derive divergent identity keys, so cohort B can NEVER read
//     cohort A's memoized (RBAC-filtered) body. Dropping identity from the key
//     is a cross-user leak — proven RED by the divergent-output memo test.
//
//   - JSON-NATIVE VALUES (C-F4-3): the caller stores an already-deep-copied,
//     JSON-native snapshot (plumbing/maps.DeepCopyJSON at the apiref seam) and
//     the memo returns a fresh deep copy on every hit, so no two callers alias
//     the stored map and no caller can mutate it. The stored value is opaque to
//     this package — the apiref seam owns the DeepCopyJSON round-trip so a
//     non-JSON-native value ([]string etc.) panics AT THE SEAM under -race,
//     never silently aliased here.
//
// TOGGLE (C-F4-9): the memo is only ever installed on the seed context, which
// only exists when the seed runs, which only runs under
// ResolvedCacheEnabled() / the prewarm gate. A cache-off process never installs
// a memo → SeedResolveMemoFromContext is nil everywhere → zero behavior change.
// The install site (withCohortSeedContext) additionally no-ops the install when
// Disabled().

package cache

import (
	"context"
	"strconv"
	"strings"
	"sync"
)

// ctxKeySeedResolveMemoType is the typed empty-struct context key for the
// per-seed-pass RA-resolve memo. Distinct unexported type — no cross-package
// raw-string-key collision (mirrors ctxKeyExternalTouchedSinkType).
type ctxKeySeedResolveMemoType struct{}

var ctxKeySeedResolveMemo = ctxKeySeedResolveMemoType{}

// seedMemoValue is the memo's stored value: the resolved body PLUS the
// dependency edges captured while it was produced (issue #277 / edge-3). A
// memo hit replays Deps under the hitting caller's L1 key so a widget served
// from the memo carries the SAME backing-GVR edges a real resolve would have
// recorded — without which the memo-served widget cell goes stale on a backing
// mutation (edge-3). Body stays the JSON-native, caller-deep-copied snapshot;
// Deps is a flat slice of cache-package DepKeys, captured by the apiref seam
// via Deps().BeginCapture/EndCapture around the producing resolve.
//
// [C-lifetime] the struct lives in the SeedResolveMemo = seed-pass lifetime;
// it is never promoted into the L1 store, so the captured Deps cannot outlive
// the pass or cross into a user /call.
//
// [#411] Stamp is the dep-event seq (depEventSeq, #375) the body is fresh AS OF:
// the PRODUCER resolve's entry epoch (its dep-gen sink startSeq), the
// conservative choice — every read the body folds happened at or after it, so a
// dep event at seq <= Stamp is already reflected in Body, and one at seq > Stamp
// may not be. Load treats the entry as STALE (a miss) when any of Deps moved
// past Stamp. The struct is immutable once stored: a fresher producer REPLACES
// the pointer (Store), it never mutates one in place, so a concurrent Load reads
// a consistent (Body, Deps, Stamp) triple without a lock.
type seedMemoValue struct {
	Body  map[string]any
	Deps  []DepKey
	Stamp uint64
}

// SeedResolveMemo memoizes the resolved output of a heavy RESTAction across the
// many widgets that share it WITHIN A SINGLE SEED PASS. It is keyed by
// (RA ns/name, RBAC identity, effective-extras hash, perPage, page) — every
// input that can change the resolved body — so a hit is only ever served to a
// caller that would compute a byte-identical result (C-F4-6). Concurrency-safe
// (sync.Map): the seed fans widgets across cohort goroutines.
//
// The memo holds ONE entry per distinct (RA, identity, page, extras) tuple; the
// value is a JSON-native map[string]any snapshot the CALLER deep-copied before
// Store (and the memo deep-copies again on Load) so stored bodies are never
// aliased or mutated.
type SeedResolveMemo struct {
	m sync.Map // key string -> *seedMemoValue (JSON-native body, caller-deep-copied, + captured deps)
	// copyFn deep-copies a stored JSON-native map on Load so no two callers
	// alias the stored value. Injected by the installer (apiref/widgets seam
	// owns plumbing/maps.DeepCopyJSON; keeping the dep out of the cache package
	// avoids importing a resolver util downward). nil copyFn ⇒ Load returns the
	// stored map directly (test-only / degenerate); production always injects.
	copyFn func(map[string]any) map[string]any

	hits   uint64
	misses uint64
	// staleMisses counts the #411 hits-turned-misses: a key WAS present but a
	// captured dep moved past the entry's Stamp, so the caller resolved fresh.
	// Also counted in misses (a stale entry is a miss to the caller).
	staleMisses uint64
	mu          sync.Mutex // guards hits/misses/staleMisses (diagnostic counters only)
}

// seedResolveMemoKey canonicalizes the memo key. The identity component is the
// RBAC-determining tuple (username + SORTED groups, unit-separated) so the key
// is stable regardless of group ordering and cannot collide across cohorts with
// divergent RBAC. extrasHash is the caller-computed stable hash of the
// effective extras map (the same effective-extras the resolve folds).
func seedResolveMemoKey(raNS, raName, username string, groups []string, extrasHash string, perPage, page int) string {
	// Copy + sort groups so ["a","b"] and ["b","a"] fold identically without
	// mutating the caller's slice.
	g := make([]string, len(groups))
	copy(g, groups)
	sortStrings(g)
	var b strings.Builder
	b.WriteString(raNS)
	b.WriteByte('|')
	b.WriteString(raName)
	b.WriteString("|u=")
	b.WriteString(username)
	b.WriteString("|g=")
	b.WriteString(strings.Join(g, "\x1f"))
	b.WriteString("|x=")
	b.WriteString(extrasHash)
	b.WriteString("|pp=")
	b.WriteString(strconv.Itoa(perPage))
	b.WriteString("|p=")
	b.WriteString(strconv.Itoa(page))
	return b.String()
}

// Key builds the canonical memo key for a resolve of RESTAction (raNS/raName)
// under the RBAC identity (username + groups), effective-extras hash extrasHash
// (from HashExtras), at pagination (perPage, page). The apiref seam calls this
// so the identity/extras/page folding lives in ONE place (cannot drift from the
// Load/Store consumers). nil-receiver-safe: a nil memo still produces a
// well-formed key (harmless — the subsequent nil.Load is a miss).
func (mo *SeedResolveMemo) Key(raNS, raName, username string, groups []string, extrasHash string, perPage, page int) string {
	return seedResolveMemoKey(raNS, raName, username, groups, extrasHash, perPage, page)
}

// sortStrings is a tiny insertion sort (groups slices are short: a handful of
// RBAC groups). Avoids pulling "sort" for a hot-path helper.
func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

// Load returns the memoized resolved body for the key and true on a hit, or
// (nil, nil, false) on a miss. On a hit the returned map is a FRESH deep copy
// (via the injected copyFn) so the caller may mutate/consume it without
// corrupting the stored snapshot or aliasing a sibling caller. nil-receiver-
// safe: (nil).Load is always a miss so a call site with no memo installed
// behaves as "always resolve".
//
// #411 — FAIL SAFE TOWARD FRESHNESS. A present entry is served ONLY if none of
// its captured deps saw a dep event after the entry's Stamp (the producer's
// entry epoch): the SAME per-bucket lastBumpSeq / per-GVR floor test #375's
// accepted-Put check uses (DepTracker.depMovedSince — no new map). A dep that
// moved after the body was produced but BEFORE the hitting resolve's own entry
// is invisible to the hitter's #375 Put-check (its bump is < the hitter's
// startSeq), so without this check the hitter would Put the pre-move body as
// fresh. A moved dep turns the hit into a MISS: the caller resolves fresh and
// its Store REPLACES this entry with a later Stamp, so later siblings hit the
// fresh body again (no cascade of re-resolves). Lock-free: the value is
// immutable and the seq reads are atomics.
//
// The fourth return is the served entry's Stamp, read off the SAME value the
// body and deps come from (no second lookup, no TOCTOU against a concurrent
// replacing Store). The caller hands it to DepTracker.ReplayEdgesAsOf so its own
// Put-check judges the reused deps from the body's as-of, not its own entry
// (#411 C1, the torn [depEventSeq.Add → bucket stamp] window).
func (mo *SeedResolveMemo) Load(key string) (map[string]any, []DepKey, uint64, bool) {
	if mo == nil {
		return nil, nil, 0, false
	}
	v, ok := mo.m.Load(key)
	if !ok {
		mo.mu.Lock()
		mo.misses++
		mo.mu.Unlock()
		return nil, nil, 0, false
	}
	val, _ := v.(*seedMemoValue)
	if val != nil && seedMemoDepsMovedSince(val.Deps, val.Stamp) {
		mo.mu.Lock()
		mo.misses++
		mo.staleMisses++
		mo.mu.Unlock()
		return nil, nil, 0, false
	}
	mo.mu.Lock()
	mo.hits++
	mo.mu.Unlock()
	if val == nil {
		return nil, nil, 0, true
	}
	// Deps is returned as a slice-header copy — the caller only reads it to
	// replay edges; the underlying DepKeys are immutable value structs.
	deps := val.Deps
	if mo.copyFn == nil {
		return val.Body, deps, val.Stamp, true
	}
	return mo.copyFn(val.Body), deps, val.Stamp, true
}

// seedMemoDepsMovedSince reports whether any dep saw a dep event after stamp,
// through the process DepTracker's #375 generation (bucket lastBumpSeq, or the
// per-GVR floor for a coordinate with no bucket). nil tracker ⇒ false.
func seedMemoDepsMovedSince(deps []DepKey, stamp uint64) bool {
	d := Deps()
	if d == nil {
		return false
	}
	for _, dk := range deps {
		if d.depMovedSince(dk, stamp) {
			return true
		}
	}
	return false
}

// Store records a JSON-native, caller-deep-copied resolved body under key,
// fresh as of stamp (#411: the producer resolve's entry epoch —
// cache.DepGenStartSeqFromContext, or DepGenEpochNow taken before the
// producer's first read). The caller MUST deep-copy (plumbing/maps.DeepCopyJSON)
// before Store so the stored snapshot is not aliased by the caller's live
// result. Replacement is MONOTONE in stamp: an entry is replaced only by one
// with a strictly later stamp (the fresh resolve that a stale hit turned into a
// miss); an equal-or-older concurrent producer of the same tuple discards its
// body (first-writer-wins among same-epoch producers, as before #411). A
// later-stamped producer can therefore never be overwritten by an older one
// racing it. nil-receiver-safe (no-op) so an uninstalled call site never stores.
func (mo *SeedResolveMemo) Store(key string, snapshot map[string]any, deps []DepKey, stamp uint64) {
	if mo == nil {
		return
	}
	nv := &seedMemoValue{Body: snapshot, Deps: deps, Stamp: stamp}
	for {
		cur, loaded := mo.m.LoadOrStore(key, nv)
		if !loaded {
			return
		}
		cv, _ := cur.(*seedMemoValue)
		if cv != nil && cv.Stamp >= stamp {
			return // an equal-or-fresher body is already memoised
		}
		if mo.m.CompareAndSwap(key, cur, nv) {
			return
		}
		// Lost a race with another Store; re-read and re-decide.
	}
}

// StaleMisses returns how many lookups found an entry whose captured deps had
// moved past its stamp and therefore resolved fresh (#411). Diagnostic;
// nil-receiver-safe. Each is also counted in Stats' misses.
func (mo *SeedResolveMemo) StaleMisses() uint64 {
	if mo == nil {
		return 0
	}
	mo.mu.Lock()
	defer mo.mu.Unlock()
	return mo.staleMisses
}

// Stats returns the memo's hit/miss counters (diagnostic — feeds the
// phase1.seed.memo.summary falsifier line). nil-receiver-safe.
func (mo *SeedResolveMemo) Stats() (hits, misses uint64) {
	if mo == nil {
		return 0, 0
	}
	mo.mu.Lock()
	defer mo.mu.Unlock()
	return mo.hits, mo.misses
}

// NewSeedResolveMemo builds a memo with the given deep-copy function. copyFn is
// applied to a stored map on every Load hit so callers never alias the stored
// snapshot. Production passes plumbing/maps.DeepCopyJSON (injected from the
// apiref seam to keep the resolver util out of the cache package's imports).
func NewSeedResolveMemo(copyFn func(map[string]any) map[string]any) *SeedResolveMemo {
	return &SeedResolveMemo{copyFn: copyFn}
}

// WithSeedResolveMemo returns a child context carrying memo. Installed ONLY by
// the seed pass (withCohortSeedContext) — a user /call, the refresher, and the
// discovery walk never install one, so the memo is a strict no-op off the seed
// path (C-F4-8). Inert under Disabled(): returns ctx unchanged so a cache-off
// process carries no memo.
func WithSeedResolveMemo(ctx context.Context, memo *SeedResolveMemo) context.Context {
	if ctx == nil || memo == nil || Disabled() {
		return ctx
	}
	return context.WithValue(ctx, ctxKeySeedResolveMemo, memo)
}

// SeedResolveMemoFromContext returns the *SeedResolveMemo installed on ctx, or
// nil when none is present. A nil return MUST be treated as "no memo — always
// resolve" (the methods are nil-receiver-safe). Off the seed path this is
// always nil, which is what makes the memo provably untouched on the /call path.
func SeedResolveMemoFromContext(ctx context.Context) *SeedResolveMemo {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(ctxKeySeedResolveMemo).(*SeedResolveMemo)
	return v
}
