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
//     #598 ADDED A SECOND INSTALLER: the REFRESHER (resolveAndPopulateL1) now
//     installs a process-held memo of this same type, for the same reason on a
//     different caller — one dirty-mark burst re-resolves the same RESTAction
//     once per widget sharing it. The teardown argument above is what that memo
//     had to answer, and it answers it two ways: a /call still carries no memo
//     (unchanged, and the load-bearing half), and a stale body is already
//     refused by the #411 dep-moved check below rather than by teardown. See
//     dispatchers/refresh_resolve_memo.go.
//
//   - RBAC SAFETY (C-F4-4): the memo KEY folds the full RBAC-determining
//     identity — username + sorted groups, exactly the (Username, Groups) that
//     internal/resolvers/restactions/api/refilter.go + cluster_list.go +
//     apiref/ra_full_list.go read off ctx to FILTER the list. Two cohorts with
//     divergent RBAC derive divergent identity keys, so cohort B can NEVER read
//     cohort A's memoized (RBAC-filtered) body. Dropping identity from the key
//     is a cross-user leak — proven RED by the divergent-output memo test.
//
//   - ENCODED VALUES (C-F4-3, #883 F4): the caller stores the RESTAction's
//     ENCODED result — the JSON bytes its own miss path decodes — and every hit
//     decodes those bytes once, with encoding/json, the decoder the miss path
//     uses. So a hit is the miss path's own decode of the same bytes, no two
//     callers alias a stored map, and no caller can mutate one. Before #883 the
//     memo held a decoded deep copy (plumbing/maps.DeepCopyJSON) and deep-copied
//     it again on every hit; DeepCopyJSON shares strings, so each stored body
//     also pinned the decoded strings of the resolve that produced it. Bench
//     run 15 measured DeepCopyJSON 12 -> 774 MB and rawExtensionToMap 11 -> 541
//     MB in lockstep and never falling.
//
//   - ONLY LIVE ENTRIES ARE KEPT (#883 F4). An entry is deleted once it can no
//     longer be served: when Load finds one of its captured deps moved past its
//     Stamp (#411), and when the dependency tracker moves one of its deps — the
//     dep-event bump that precedes every dirty-mark fan-out (TrackDepMoves wires
//     a memo to it). A dep whose tracker bucket is pruned is judged by the GVR
//     floor from then on; the memo learns of the prune at the next dep event or
//     memo Load / Store, because the prune runs under the resolved-cache lock
//     customers take and must not wait on memo work. Before #883 Load refused a
//     stale entry but left it in the map, so an entry nobody looked up again (a
//     churned coordinate, an identity gone quiet) held its body for the life of
//     the memo: the ~1 GB/h rising floor of bench run 15.
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
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/runtime/schema"
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
// mutation (edge-3). Body is the RESTAction's ENCODED result (#883 F4), decoded
// afresh on every hit; Deps is a flat slice of cache-package DepKeys, captured
// by the apiref seam via Deps().BeginCapture/EndCapture around the producing
// resolve.
//
// [C-lifetime] the struct lives in the SeedResolveMemo; it is never promoted
// into the L1 store, so the captured Deps cannot cross into a user /call.
//
// [#411] Stamp is the dep-event seq (depEventSeq, #375) the body is fresh AS OF:
// the PRODUCER resolve's entry epoch (its dep-gen sink startSeq), the
// conservative choice — every read the body folds happened at or after it, so a
// dep event at seq <= Stamp is already reflected in Body, and one at seq > Stamp
// may not be. Load treats the entry as STALE (a miss, and deletes it, #883)
// when any of Deps moved past Stamp. The struct is immutable once stored: a
// fresher producer REPLACES the pointer (Store), it never mutates one in place.
type seedMemoValue struct {
	Body  []byte
	Deps  []DepKey
	Stamp uint64
	// Layers — #406: the raKey content versions the producing resolve Go-sliced. A
	// hit replays them onto its own sink (ReplayRAFullListSlices) so the memo-served
	// widget's Put is judged against them, like the producer's.
	Layers LayeredSources
	// Narrow (#765 B) holds the producer's per-edge interest states,
	// index-aligned with Deps, snapshotted AFTER the producer's resolve returned
	// (its commit ran inside it). nil = replay plain.
	Narrow []EdgeNarrowState
}

// SeedResolveMemo memoizes the resolved output of a heavy RESTAction across the
// many widgets that share it. It is keyed by (RA ns/name, RBAC identity,
// effective-extras hash, perPage, page) — every input that can change the
// resolved body — so a hit is only ever served to a caller that would compute a
// byte-identical result (C-F4-6). Concurrency-safe: the seed fans widgets across
// cohort goroutines and the refresher runs several workers.
//
// It holds ONE entry per distinct (RA, identity, page, extras) tuple whose
// captured deps have not moved since it was produced (#883 F4), so its size is
// the live working set and nothing else.
type SeedResolveMemo struct {
	// mu guards m, deps and the hit/miss counters. It is never held across a
	// decode or a resolve: every critical section is map work plus the #411 seq
	// reads (atomics), so concurrent resolves never serialise behind each other
	// on it — and a customer /call never carries a memo at all (C-F4-8).
	mu sync.Mutex
	m  map[string]*seedMemoValue // key -> encoded body + captured deps
	// deps indexes the stored keys by the deps they captured, per GVR, so a dep
	// event re-judges exactly the entries it can have made stale instead of
	// scanning the memo (see memoDepIndex).
	deps map[schema.GroupVersionResource]*memoDepIndex

	// entries / bodyBytes are the #883 gauge: the live entry count and the sum
	// of the stored encoded bodies. Written under mu, read without it.
	entries   atomic.Int64
	bodyBytes atomic.Int64

	hits   uint64
	misses uint64
	// staleMisses counts the #411 hits-turned-misses: a key WAS present but a
	// captured dep moved past the entry's Stamp, so the caller resolved fresh.
	// Also counted in misses (a stale entry is a miss to the caller).
	staleMisses uint64
}

// memoDepIndex is one GVR's slice of the memo's dep index.
//
// byDep maps every captured dep of this GVR to the memo keys that captured it.
//
// floorJudged is the subset of byDep's deps that have NO bucket in the dep
// tracker. depMovedSince judges such a dep by the GVR's last-bump floor, which
// ANY object event of the GVR raises — not only an event on that coordinate —
// so an object event must re-judge these too, alongside the (at most four)
// buckets bumpCoordinateGen stamps. A dep enters the set when its bucket is
// absent as the entry is stored, or when the tracker prunes the bucket later
// (logged by notifyResolveMemosBucketGone, judged at the next dep event or memo
// Load / Store); it leaves with its last key.
type memoDepIndex struct {
	byDep       map[DepKey]map[string]struct{}
	floorJudged map[DepKey]struct{}
}

// Process-wide #883 drop totals, monotone across memo swaps (the refresher
// replaces its memo wholesale, which would reset a per-memo counter).
var (
	// resolveMemoDroppedStaleTotal counts entries deleted because a Load found
	// them stale (#411).
	resolveMemoDroppedStaleTotal atomic.Uint64
	// resolveMemoDroppedMovedTotal counts entries deleted because the dep
	// tracker moved one of their deps with nobody looking them up.
	resolveMemoDroppedMovedTotal atomic.Uint64
)

// ResolveMemoDropTotals returns the process-wide #883 drop totals: entries
// deleted by a stale Load, and entries deleted by a dep move.
func ResolveMemoDropTotals() (stale, moved uint64) {
	return resolveMemoDroppedStaleTotal.Load(), resolveMemoDroppedMovedTotal.Load()
}

// seedResolveMemoKey canonicalizes the memo key. The identity component is the
// RBAC-determining tuple (username + SORTED groups, unit-separated) so the key
// is stable regardless of group ordering and cannot collide across cohorts with
// divergent RBAC. extrasHash is the caller-computed stable hash of the
// effective extras map (the same effective-extras the resolve folds).
//
// rbacClass (#411 x #424) is the identity's CURRENT RBAC class: the same
// (SubjectBindingSet digest, RBACSubGen) the widgets / restactions L1 key folds
// since #424. (username, groups) alone is not enough: within one seed pass a
// binding grant/revoke or a rules edit on a bound role moves the SAME identity
// into another class. The hitter's L1 key then names the new class and #424's
// Put-time guard re-derives the new class, so only the memo key can stop a body
// resolved under the old class from being written into the new class's cell
// (served to every member of that class: a #423-class leak).
func seedResolveMemoKey(raNS, raName, username string, groups []string, rbacClass, extrasHash string, perPage, page int) string {
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
	b.WriteString("|c=")
	b.WriteString(rbacClass)
	b.WriteString("|x=")
	b.WriteString(extrasHash)
	b.WriteString("|pp=")
	b.WriteString(strconv.Itoa(perPage))
	b.WriteString("|p=")
	b.WriteString(strconv.Itoa(page))
	return b.String()
}

// Key builds the canonical memo key for a resolve of RESTAction (raNS/raName)
// under the RBAC identity (username + groups) in RBAC class rbacClass (#424;
// see seedResolveMemoKey), effective-extras hash extrasHash
// (from HashExtras), at pagination (perPage, page). The apiref seam calls this
// so the identity/extras/page folding lives in ONE place (cannot drift from the
// Load/Store consumers). nil-receiver-safe: a nil memo still produces a
// well-formed key (harmless — the subsequent nil.Load is a miss).
func (mo *SeedResolveMemo) Key(raNS, raName, username string, groups []string, rbacClass, extrasHash string, perPage, page int) string {
	return seedResolveMemoKey(raNS, raName, username, groups, rbacClass, extrasHash, perPage, page)
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
// (nil, nil, false) on a miss. On a hit the returned map is decoded afresh from
// the stored encoded body (#883 F4), so the caller may mutate/consume it
// without corrupting the stored value or aliasing a sibling caller.
// nil-receiver-safe: (nil).Load is always a miss so a call site with no memo
// installed behaves as "always resolve".
//
// #411 — FAIL SAFE TOWARD FRESHNESS. A present entry is served ONLY if none of
// its captured deps saw a dep event after the entry's Stamp (the producer's
// entry epoch): the SAME per-bucket lastBumpSeq / per-GVR floor test #375's
// accepted-Put check uses (DepTracker.depMovedSince — no new map). A dep that
// moved after the body was produced but BEFORE the hitting resolve's own entry
// is invisible to the hitter's #375 Put-check (its bump is < the hitter's
// startSeq), so without this check the hitter would Put the pre-move body as
// fresh. A moved dep turns the hit into a MISS and, since #883, DELETES the
// entry: it can never be served again, so keeping it only held its body. The
// caller resolves fresh and its Store puts a later-stamped entry in its place.
//
// The fourth return is the served entry's Stamp, read off the SAME value the
// body and deps come from (no second lookup, no TOCTOU against a concurrent
// replacing Store). The caller hands it to DepTracker.ReplayEdgesAsOf so its own
// Put-check judges the reused deps from the body's as-of, not its own entry
// (#411 C1, the torn [depEventSeq.Add → bucket stamp] window).
func (mo *SeedResolveMemo) Load(key string) (map[string]any, []DepKey, uint64, bool) {
	body, deps, stamp, _, ok := mo.LoadLayered(key)
	return body, deps, stamp, ok
}

// LoadLayered is Load plus the raKey versions the producing resolve sliced (#406),
// read off the SAME value as the body, deps and stamp.
func (mo *SeedResolveMemo) LoadLayered(key string) (map[string]any, []DepKey, uint64, LayeredSources, bool) {
	body, deps, _, stamp, layers, ok := mo.LoadLayeredWithStates(key)
	return body, deps, stamp, layers, ok
}

// LoadLayeredWithStates is LoadLayered plus the producer's per-edge interest
// states (#765 B), read off the SAME value.
func (mo *SeedResolveMemo) LoadLayeredWithStates(key string) (map[string]any, []DepKey, []EdgeNarrowState, uint64, LayeredSources, bool) {
	if mo == nil {
		return nil, nil, nil, 0, nil, false
	}
	drainResolveMemoBucketGone()
	mo.mu.Lock()
	val := mo.m[key]
	if val == nil {
		mo.misses++
		mo.mu.Unlock()
		return nil, nil, nil, 0, nil, false
	}
	if seedMemoDepsMovedSince(val.Deps, val.Stamp) {
		mo.misses++
		mo.staleMisses++
		mo.removeLocked(key, val)
		mo.mu.Unlock()
		resolveMemoDroppedStaleTotal.Add(1)
		return nil, nil, nil, 0, nil, false
	}
	mo.hits++
	mo.mu.Unlock()
	// The decode runs outside the lock: val is immutable once stored.
	body, err := decodeMemoBody(val.Body)
	if err != nil {
		// Unreachable for bytes encoding/json produced; fail toward a fresh
		// resolve and drop the entry rather than serve something else.
		mo.mu.Lock()
		if mo.m[key] == val {
			mo.removeLocked(key, val)
		}
		mo.mu.Unlock()
		return nil, nil, nil, 0, nil, false
	}
	// Deps is returned as a slice-header copy — the caller only reads it to
	// replay edges; the underlying DepKeys are immutable value structs.
	return body, val.Deps, val.Narrow, val.Stamp, val.Layers, true
}

// decodeMemoBody is the hit's one decode: encoding/json into a map, exactly as
// the apiref miss path decodes the RESTAction's Status (rawExtensionToMap). An
// empty body decodes to nil, as a nil body was served before #883.
func decodeMemoBody(b []byte) (map[string]any, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var out map[string]any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
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

// Store records the RESTAction's ENCODED resolved body (#883 F4: the JSON
// bytes its miss path decodes) under key, fresh as of stamp (#411: the producer
// resolve's entry epoch — cache.DepGenStartSeqFromContext, or DepGenEpochNow
// taken before the producer's first read). The memo keeps the slice: the caller
// must not mutate it after Store. Replacement is MONOTONE in stamp: an entry is
// replaced only by one with a strictly later stamp; an equal-or-older concurrent
// producer of the same tuple discards its body (first-writer-wins among
// same-epoch producers, as before #411). nil-receiver-safe (no-op) so an
// uninstalled call site never stores.
func (mo *SeedResolveMemo) Store(key string, encoded []byte, deps []DepKey, stamp uint64) {
	mo.StoreLayered(key, encoded, deps, stamp, nil)
}

// StoreLayered is Store plus the raKey versions the producing resolve sliced (#406).
// Same monotone-in-stamp replacement as Store.
func (mo *SeedResolveMemo) StoreLayered(key string, encoded []byte, deps []DepKey, stamp uint64, layers LayeredSources) {
	mo.StoreLayeredWithStates(key, encoded, deps, nil, stamp, layers)
}

// StoreLayeredWithStates is StoreLayered plus the producer's per-edge interest
// states (#765 B), index-aligned with deps.
//
// #883 — an entry whose deps ALREADY moved past its stamp is removed right after
// it is stored. The check must follow the insert, not precede it: a dep event
// stamps its bucket and then re-judges the memo's entries (DropMoved), while
// Store inserts and then reads the stamps, so whichever runs second sees the
// other — a check before the insert could miss an event landing in between and
// leave an unservable entry behind.
func (mo *SeedResolveMemo) StoreLayeredWithStates(key string, encoded []byte, deps []DepKey, states []EdgeNarrowState, stamp uint64, layers LayeredSources) {
	if mo == nil {
		return
	}
	drainResolveMemoBucketGone()
	nv := &seedMemoValue{Body: encoded, Deps: deps, Stamp: stamp, Layers: layers, Narrow: states}
	mo.mu.Lock()
	if cur := mo.m[key]; cur != nil {
		if cur.Stamp >= stamp {
			mo.mu.Unlock()
			return // an equal-or-fresher body is already memoised
		}
		mo.removeLocked(key, cur)
	}
	mo.insertLocked(key, nv)
	mo.mu.Unlock()
	if seedMemoDepsMovedSince(deps, stamp) {
		mo.mu.Lock()
		if mo.m[key] == nv {
			mo.removeLocked(key, nv)
			resolveMemoDroppedMovedTotal.Add(1)
		}
		mo.mu.Unlock()
	}
}

// insertLocked stores v under key and indexes it by each captured dep. mu held.
func (mo *SeedResolveMemo) insertLocked(key string, v *seedMemoValue) {
	if mo.m == nil {
		mo.m = make(map[string]*seedMemoValue)
	}
	if mo.deps == nil {
		mo.deps = make(map[schema.GroupVersionResource]*memoDepIndex)
	}
	mo.m[key] = v
	d := Deps()
	for _, dk := range v.Deps {
		ix := mo.deps[dk.GVR]
		if ix == nil {
			ix = &memoDepIndex{byDep: make(map[DepKey]map[string]struct{}), floorJudged: make(map[DepKey]struct{})}
			mo.deps[dk.GVR] = ix
		}
		keys := ix.byDep[dk]
		if keys == nil {
			keys = make(map[string]struct{})
			ix.byDep[dk] = keys
		}
		keys[key] = struct{}{}
		if d != nil && !d.hasBucket(dk) {
			ix.floorJudged[dk] = struct{}{}
		}
	}
	mo.entries.Add(1)
	mo.bodyBytes.Add(int64(len(v.Body)))
}

// removeLocked deletes key (holding v) and its index entries. mu held.
func (mo *SeedResolveMemo) removeLocked(key string, v *seedMemoValue) {
	delete(mo.m, key)
	for _, dk := range v.Deps {
		ix := mo.deps[dk.GVR]
		if ix == nil {
			continue
		}
		if keys := ix.byDep[dk]; keys != nil {
			delete(keys, key)
			if len(keys) == 0 {
				delete(ix.byDep, dk)
				delete(ix.floorJudged, dk)
			}
		}
		if len(ix.byDep) == 0 {
			delete(mo.deps, dk.GVR)
		}
	}
	mo.entries.Add(-1)
	mo.bodyBytes.Add(-int64(len(v.Body)))
}

// dropMovedLocked re-judges every entry that captured one of cands with the
// #411 predicate Load uses, and deletes the ones it finds stale. Returns how
// many it deleted. mu held.
func (mo *SeedResolveMemo) dropMovedLocked(ix *memoDepIndex, cands []DepKey) int {
	var stale []string
	for _, dk := range cands {
		for key := range ix.byDep[dk] {
			if v := mo.m[key]; v != nil && seedMemoDepsMovedSince(v.Deps, v.Stamp) {
				stale = append(stale, key)
			}
		}
	}
	n := 0
	for _, key := range stale {
		if v := mo.m[key]; v != nil { // a key under two moved deps is listed twice
			mo.removeLocked(key, v)
			n++
		}
	}
	return n
}

// dropMovedCoordinate handles one object event on (gvr, namespace, name): the
// buckets bumpCoordinateGen stamped (coordinateBuckets) and every
// floor-judged dep of the GVR, whose floor the same event raised.
func (mo *SeedResolveMemo) dropMovedCoordinate(gvr schema.GroupVersionResource, namespace, name string) int {
	mo.mu.Lock()
	defer mo.mu.Unlock()
	ix := mo.deps[gvr]
	if ix == nil {
		return 0
	}
	stamped, ns := coordinateBuckets(gvr, namespace, name)
	cands := make([]DepKey, 0, ns+len(ix.floorJudged))
	for _, dk := range stamped[:ns] {
		if _, ok := ix.byDep[dk]; ok {
			cands = append(cands, dk)
		}
	}
	for dk := range ix.floorJudged {
		cands = append(cands, dk)
	}
	return mo.dropMovedLocked(ix, cands)
}

// dropMovedType handles a type-level event on gvr (bumpResourceTypeGen): it
// stamps every bucket of the GVR (or every LIST bucket) and raises the floor,
// so every dep of the GVR is re-judged.
func (mo *SeedResolveMemo) dropMovedType(gvr schema.GroupVersionResource) int {
	mo.mu.Lock()
	defer mo.mu.Unlock()
	ix := mo.deps[gvr]
	if ix == nil {
		return 0
	}
	cands := make([]DepKey, 0, len(ix.byDep))
	for dk := range ix.byDep {
		cands = append(cands, dk)
	}
	return mo.dropMovedLocked(ix, cands)
}

// dropBucketGone handles the tracker having pruned dk's bucket: from then on the
// GVR floor judges dk, and that floor may ALREADY be past an entry's stamp
// (raised by an event on another coordinate of the GVR), so the entries holding
// dk are re-judged, and dk joins the floor-judged set for the events to come.
func (mo *SeedResolveMemo) dropBucketGone(dk DepKey) int {
	mo.mu.Lock()
	defer mo.mu.Unlock()
	ix := mo.deps[dk.GVR]
	if ix == nil {
		return 0
	}
	if _, ok := ix.byDep[dk]; !ok {
		return 0
	}
	ix.floorJudged[dk] = struct{}{}
	return mo.dropMovedLocked(ix, []DepKey{dk})
}

// resolveMemoMoveSubscribers holds the memos the dep tracker re-judges on every
// dep move (TrackDepMoves). One or two at a time: the refresher's process-held
// memo and a seed pass's memo while that pass runs. resolveMemoTracked counts
// them so a pruned bucket costs one atomic load when no memo is tracked.
var (
	resolveMemoMoveSubscribers sync.Map // *SeedResolveMemo -> struct{}
	resolveMemoTracked         atomic.Int64
)

// TrackDepMoves makes the dep tracker delete mo's entries as their deps move
// (#883 F4), whether or not anyone looks them up again. The returned func stops
// it (UntrackDepMoves); call it when mo is dropped, or the registry keeps the
// memo alive. nil-safe.
func TrackDepMoves(mo *SeedResolveMemo) (untrack func()) {
	if mo == nil {
		return func() {}
	}
	if _, loaded := resolveMemoMoveSubscribers.LoadOrStore(mo, struct{}{}); !loaded {
		resolveMemoTracked.Add(1)
	}
	return func() { UntrackDepMoves(mo) }
}

// UntrackDepMoves stops TrackDepMoves for mo. nil-safe and idempotent.
func UntrackDepMoves(mo *SeedResolveMemo) {
	if mo == nil {
		return
	}
	if _, loaded := resolveMemoMoveSubscribers.LoadAndDelete(mo); loaded {
		resolveMemoTracked.Add(-1)
	}
}

// notifyResolveMemosCoordinateMoved is called by bumpCoordinateGen after it
// stamped the event's buckets and floor — before the dirty-mark fan-out.
func notifyResolveMemosCoordinateMoved(gvr schema.GroupVersionResource, namespace, name string) {
	drainResolveMemoBucketGone()
	resolveMemoMoveSubscribers.Range(func(k, _ any) bool {
		if n := k.(*SeedResolveMemo).dropMovedCoordinate(gvr, namespace, name); n > 0 {
			resolveMemoDroppedMovedTotal.Add(uint64(n))
		}
		return true
	})
}

// notifyResolveMemosTypeMoved is called by bumpResourceTypeGen after its stamps.
func notifyResolveMemosTypeMoved(gvr schema.GroupVersionResource) {
	drainResolveMemoBucketGone()
	resolveMemoMoveSubscribers.Range(func(k, _ any) bool {
		if n := k.(*SeedResolveMemo).dropMovedType(gvr); n > 0 {
			resolveMemoDroppedMovedTotal.Add(uint64(n))
		}
		return true
	})
}

// The bucket-gone log. The tracker prunes a bucket from RemoveL1Key, which the
// resolved-cache store calls while it holds its own lock — the lock every
// customer read takes. So the pruner never touches a memo: it appends the
// coordinate here (a leaf lock held for one append) and the memos judge it at
// the next dep event or the next memo Load / Store, on the informer or
// refresher goroutine. A customer therefore never waits on memo work.
var (
	bucketGoneMu      sync.Mutex
	bucketGoneLog     []DepKey
	bucketGonePending atomic.Int64
)

// notifyResolveMemosBucketGone is called after the tracker deleted dk's bucket.
func notifyResolveMemosBucketGone(dk DepKey) {
	if resolveMemoTracked.Load() == 0 {
		return // no memo to judge it: nothing may accumulate here
	}
	bucketGoneMu.Lock()
	bucketGoneLog = append(bucketGoneLog, dk)
	bucketGonePending.Store(int64(len(bucketGoneLog)))
	bucketGoneMu.Unlock()
}

// drainResolveMemoBucketGone hands every logged bucket-gone coordinate to every
// tracked memo (dropBucketGone). One atomic load when the log is empty.
func drainResolveMemoBucketGone() {
	if bucketGonePending.Load() == 0 {
		return
	}
	bucketGoneMu.Lock()
	log := bucketGoneLog
	bucketGoneLog = nil
	bucketGonePending.Store(0)
	bucketGoneMu.Unlock()
	resolveMemoMoveSubscribers.Range(func(k, _ any) bool {
		mo := k.(*SeedResolveMemo)
		n := 0
		for _, dk := range log {
			n += mo.dropBucketGone(dk)
		}
		if n > 0 {
			resolveMemoDroppedMovedTotal.Add(uint64(n))
		}
		return true
	})
}

// Entries is the #883 gauge: how many bodies the memo holds. nil-safe.
func (mo *SeedResolveMemo) Entries() int64 {
	if mo == nil {
		return 0
	}
	return mo.entries.Load()
}

// BodyBytes is the #883 gauge: the summed size of the stored encoded bodies.
// nil-safe.
func (mo *SeedResolveMemo) BodyBytes() int64 {
	if mo == nil {
		return 0
	}
	return mo.bodyBytes.Load()
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

// NewSeedResolveMemo builds an empty memo. A hit decodes the stored encoded
// body (#883 F4), so no copy function is needed: every caller gets its own map.
func NewSeedResolveMemo() *SeedResolveMemo {
	return &SeedResolveMemo{}
}

// WithSeedResolveMemo returns a child context carrying memo. Installed by the
// seed pass (withCohortSeedContext) and, since #598, by the refresher
// (resolveAndPopulateL1). A user /call and the discovery walk never install one,
// so the memo remains a strict no-op on the request path (C-F4-8) — that half of
// the invariant is unchanged and is the one that matters. Inert under
// Disabled(): returns ctx unchanged so a cache-off process carries no memo.
func WithSeedResolveMemo(ctx context.Context, memo *SeedResolveMemo) context.Context {
	if ctx == nil || memo == nil || Disabled() {
		return ctx
	}
	return context.WithValue(ctx, ctxKeySeedResolveMemo, memo)
}

// SeedResolveMemoFromContext returns the *SeedResolveMemo installed on ctx, or
// nil when none is present. A nil return MUST be treated as "no memo — always
// resolve" (the methods are nil-receiver-safe). On the /call and discovery-walk
// paths this is always nil, which is what makes the memo provably untouched on
// the request path; since #598 the refresher DOES carry one.
func SeedResolveMemoFromContext(ctx context.Context) *SeedResolveMemo {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(ctxKeySeedResolveMemo).(*SeedResolveMemo)
	return v
}
