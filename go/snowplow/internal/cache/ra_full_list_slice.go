// ra_full_list_slice.go — Ship 4a (0.30.198): the page-independent
// RAFullList cache's pure-logic core.
//
// THE SHIP. The compositions-panels RESTAction's top-level filter
// sort_by(creationTimestamp)|reverse then slices `$sorted[offset:offset+
// perPage]` driven by the injected `.slice`. Today every page (page 1, 2,
// 3 …) re-runs the WHOLE resolve (62-namespace fan-out + 49K-element sort +
// deep-copy + gojq recompile) because the L1 cell folds page/perPage into
// its key. Ship 4a caches the RA's full result ONCE per (RA × cohort ×
// non-slice inputs) resolved UNPAGINATED (no `.slice` → the RA's own jq
// returns the full sorted set) and serves each page as a cheap Go-slice
// over that one cached full array.
//
// This file holds ONLY pure functions + the sliceability memo — no cache
// store, no HTTP, no resolver coupling — so the falsifiers (page-
// independent key, Go-slice == RA `.slice` jq, sliceability memo) exercise
// it in isolation. The dispatcher/resolver wiring lives elsewhere; removing
// the layer is deleting this file + the apiRef Get/Put + the resident
// region (project_caching_is_provisional — wholesale-deletable).

package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"
)

// sliceExtrasKeys is the set of Extras keys that carry PAGINATION/slice
// intent. They are STRIPPED from the RAFullList key (extrasMinusSlice) so a
// /call differing only in slice lands on the SAME page-independent cell.
// Mechanism-uniform (feedback_no_special_cases): a fixed, slice-semantic
// key set — NOT a resource/path/name literal.
var sliceExtrasKeys = map[string]struct{}{
	"slice":   {},
	"page":    {},
	"perPage": {},
	"offset":  {},
}

// extrasMinusSlice returns a COPY of extras with every slice-bearing key
// (slice/page/perPage/offset) removed. Used to build the page-independent
// RAFullList key: two /calls differing only in slice produce identical
// stripped extras → identical key. A nil/empty input returns nil (so the
// key's Extras fold is byte-identical to "no extras"). The input map is
// never mutated.
//
// Falsifier HG-4a.1 asserts the strip + the page-independence it yields.
func extrasMinusSlice(extras map[string]any) map[string]any {
	if len(extras) == 0 {
		return nil
	}
	out := make(map[string]any, len(extras))
	for k, v := range extras {
		if _, isSlice := sliceExtrasKeys[k]; isSlice {
			continue
		}
		out[k] = v
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// RAFullListKeyInputs builds the canonical page-INDEPENDENT ResolvedKeyInputs
// for a RAFullList cell. It takes the RESTACTION's OWN identity (gvr/ns/name
// — NOT the calling widget's), the per-layer BindingUID, and the request
// Extras, and forces PerPage=0/Page=0 + extrasMinusSlice(extras). The result
// fed to ComputeKey folds ONLY the page-independent material (ComputeKey
// folds PerPage at resolved.go's PerPage write + Page; both 0 here) so every
// page of the same (RA × binding × non-slice-extras) hashes to ONE key.
//
// bindingUID is the per-layer first-match binding identity (cache.BindingUIDFromCRB /
// FromRB on the binding returned by EvaluateRBAC) — the SAME per-binding
// identity the restactions/widgets classes use; RAFullList is identity-
// BOUND (RA output is RBAC-narrowed), so ComputeKey folds it.
//
// Ship 0.30.242 H.c-layered Phase 2b: signature changed from
// `bindingSetHash uint64` to `bindingUID string`. The caller (widgets/apiref/
// ra_full_list.go:85) is migrated in lockstep to derive the BindingUID via
// rbac.EvaluateRBAC instead of cache.BindingSetHash.
//
// #423: the caller MUST also set the returned SubjectBindingSet to the
// requester's full matching-binding-set digest (rbac.SubjectBindingSetDigest —
// package cache cannot import rbac). The sole production caller,
// apiref.seedFullListRAKey, does; RA output is narrowed per requester by every
// step, so the first-match bindingUID alone is not a sound sharing class.
//
// #435: likewise RBACSubGen (cache.RBACSubGenForSubject over
// rbac.WithAuthenticatedGroup(groups)) — set by the same caller, so a Role-rules
// edit or a grant-then-revoke that leaves the binding set unchanged rotates the
// key, exactly as it does for the restactions/widgets classes.
func RAFullListKeyInputs(group, version, resource, namespace, name string,
	bindingUID string, extras map[string]any) ResolvedKeyInputs {
	return ResolvedKeyInputs{
		CacheEntryClass: CacheEntryClassRAFullList,
		Group:           group,
		Version:         version,
		Resource:        resource,
		Namespace:       namespace,
		Name:            name,
		BindingUID:      bindingUID,
		// Page-INDEPENDENT: slice folded out of the key.
		PerPage: 0,
		Page:    0,
		Extras:  extrasMinusSlice(extras),
	}
}

// GoSliceFullList applies a per-/call page slice [offset:offset+perPage] over
// the single array-valued key of a RA full-result map, returning a NEW result
// map of the same shape with that key sliced. It is the Go equivalent of the
// RA's own output jq `$sorted[$offset : $offset+$perPage]`.
//
// SHAPE CONTRACT (the sliceability precondition — verified per (key ×
// sliceShape) by the byte-verify before this is trusted): the full map must
// have EXACTLY ONE key whose value is a []any (the sorted list); every other
// key is copied through unchanged. ok=false when the shape is not a clean
// single-array map (zero or multiple array-valued keys) — the caller then
// falls back to the page-keyed resolve (never a wrong result).
//
// offset/perPage are the per-/call slice (offset=(page-1)*perPage). The slice
// is clamped to the array bounds exactly as the RA jq's
// `range($offset;$offset+$perPage) | select(. < $len)` clamps. A perPage<=0
// (unpaginated) returns the full array unchanged (ok=true) — the identity
// case. The returned map shares the element VALUES with the input (a shallow
// slice — the elements are not deep-copied); callers that re-encode
// immediately (the serve path) never mutate them.
func GoSliceFullList(full map[string]any, offset, perPage int) (map[string]any, bool) {
	if full == nil {
		return nil, false
	}
	// Find the single array-valued key.
	arrKey := ""
	var arr []any
	arrCount := 0
	for k, v := range full {
		if a, isArr := v.([]any); isArr {
			arrCount++
			arrKey = k
			arr = a
		}
	}
	if arrCount != 1 {
		// Zero or multiple arrays — not a clean single-list shape. Fail-safe.
		return nil, false
	}

	out := make(map[string]any, len(full))
	for k, v := range full {
		if k != arrKey {
			out[k] = v
		}
	}

	if perPage <= 0 {
		// Unpaginated identity — full array through.
		out[arrKey] = arr
		return out, true
	}

	// Clamp [offset:offset+perPage] to bounds, mirroring the RA jq's
	// out-of-bounds guard.
	length := len(arr)
	start := offset
	if start < 0 {
		start = 0
	}
	if start > length {
		start = length
	}
	end := offset + perPage
	if end < start {
		end = start
	}
	if end > length {
		end = length
	}
	// Materialise a fresh slice (not a sub-slice alias) so the cached full
	// array's backing store is never referenced by the served page (defensive
	// against any downstream append mutating shared backing memory).
	page := make([]any, end-start)
	copy(page, arr[start:end])
	out[arrKey] = page
	return out, true
}

// IsStructurallyNonSliceable is the Ship #91 / 0.30.211 Class C heuristic.
// It reports whether the byte-verify just established a SHAPE-IDENTITY
// between the freshly-resolved unpaginated full result and the page-keyed
// fall-back reference S_ra — i.e. the RA's slice-jq did NOT actually narrow
// the result on the page-keyed resolve. That is exactly the shape of an
// aggregation RA (a sum/count/groupBy/etc. that emits the SAME shape per
// /call regardless of pagination): paginating it cannot ever produce a
// proper slice over the underlying array, because the underlying array is
// not the wire shape — the aggregation IS the wire shape.
//
// Trigger condition (both required):
//   - canonicalJSONEqual(full, sRA) == true   — full and S_ra are byte-
//     identical to canonical JSON. NB: the caller establishes this by
//     re-encoding to canonical JSON; cheap given both are already in hand.
//   - arrLen(full) > perPage                  — the underlying single-array
//     value carries more rows than one page; a correct slice would have to
//     differ from the un-paginated full. perPage==0 is excluded (the
//     unpaginated case can't be permanent — the page-keyed reference IS
//     the full by construction).
//
// SHAPE CONTRACT: full must be a single-array map (the SAME contract
// GoSliceFullList honours). When `full` is not a single-array map this
// returns FALSE — the entry is NOT Class C, and the standard Class
// A/D retry cap (3) applies. Mechanism-uniform: NO resource/name/GVR
// literal — keyed off (canonicalJSONEqual(full,sRA), arrLen(full),
// perPage).
//
// Co-located with GoSliceFullList because it shares the same "single
// array-valued key" probe.
func IsStructurallyNonSliceable(full, sRA map[string]any, perPage int) bool {
	if perPage <= 0 || full == nil || sRA == nil {
		return false
	}
	// Re-encode + compare. canonicalJSONEqual is defined in the apiref
	// package (ra_full_list.go); we re-do the equivalent here so the cache
	// package needs no dependency on apiref. encoding/json sorts map keys
	// by default → canonical.
	ab, err1 := jsonMarshalCanonical(full)
	bb, err2 := jsonMarshalCanonical(sRA)
	if err1 != nil || err2 != nil || string(ab) != string(bb) {
		return false
	}
	// arrLen(full): probe the single array-valued key. If full has zero or
	// multiple array-valued keys, the shape contract is broken and we
	// refuse Class C classification.
	arrCount := 0
	arrLen := 0
	for _, v := range full {
		if a, isArr := v.([]any); isArr {
			arrCount++
			arrLen = len(a)
		}
	}
	if arrCount != 1 {
		return false
	}
	return arrLen > perPage
}

// FullListIsEmpty reports whether a freshly-resolved RA full-result map is
// "empty" — i.e. its SINGLE array-valued key has length 0. It is the
// mechanism-uniform emptiness probe used by the serve path to refuse to
// freeze a sliceability verdict on an empty full (the "cache empty at the
// prewarm tuple → permanent stale hit" foot-gun): an empty full under a
// continueOnError stage is INDISTINGUISHABLE from a not-yet-synced / degraded
// resolve, so it must never be recorded as an authoritative sliceable verdict.
//
// Keyed off the SAME single-array shape contract as GoSliceFullList (NO
// resource/name/GVR literal — feedback_no_special_cases): a clean single-list
// map whose list is empty → true. A nil map, a zero-array-key map, or a
// multi-array map → false (NOT "empty" in the sliceable sense — those shapes
// are handled by GoSliceFullList's ok=false fail-safe and never reach a
// "freeze the empty verdict" decision). A non-empty single list → false.
func FullListIsEmpty(full map[string]any) bool {
	if full == nil {
		return false
	}
	arrCount := 0
	emptyArr := false
	for _, v := range full {
		if a, isArr := v.([]any); isArr {
			arrCount++
			emptyArr = len(a) == 0
		}
	}
	return arrCount == 1 && emptyArr
}

// --- Sliceability memo --------------------------------------------------
//
// Per the design §3: the verdict "is this RAFullList cell safely sliceable
// in Go for THIS caller's slice shape?" is memoised per (RAFullList-key ×
// sliceShape). sliceShape fingerprints HOW the caller slices — its handler
// class + the caller CR identity + a hash of the RA's output slice jq — so
// widget A's verdict NEVER applies to widget B (different sliceShape). The
// verdict is computed on FIRST sight (the byte-verify) and reused after;
// re-evaluated per new shape + on pod restart (the memo is process-local).

// SliceabilityLabels carry the HUMAN-READABLE caller-CR identity for a memo
// entry. Captured at record time so the read-side snapshot (used by
// snowplow_ra_full_list_memo) can describe each entry by its callerNamespace
// / callerName / callerResource — without those, every entry is a sha256
// hash and the operator cannot pick out a specific widget's verdict.
//
// Mechanism-uniform (feedback_no_special_cases): the labels are GENERIC
// caller-identity fields populated for EVERY caller, not a per-widget literal.
// The same struct describes apiref's "compositions-page-datagrid" and any
// future caller class identically.
//
// Optional (zero-value safe): when the labels are unset (e.g. tests calling
// the legacy RecordSliceability) the snapshot emits empty strings — verdicts
// are still recorded, just less describable. Production callers MUST pass
// real labels.
type SliceabilityLabels struct {
	// CallerClass mirrors sliceShapeHash's callerClass argument (e.g.
	// "apiref" / "restactions") — distinguishes serve-path families.
	CallerClass string
	// CallerGroup/Version/Resource is the caller CR's GVR (e.g. the apiRef
	// widget's GVR).
	CallerGroup    string
	CallerVersion  string
	CallerResource string
	// CallerNamespace/Name is the caller CR's ns/name (e.g. the apiRef
	// widget's ns/name — what the operator greps for in the snapshot).
	CallerNamespace string
	CallerName      string
}

// sliceabilityMemoEntry is the in-memory value stored in the memo's sync.Map.
// Pre-0.30.210 this was a bare `bool`; we widened it so the read-side
// snapshot can describe each entry by raKey + sliceShape + labels +
// recordedAt. The hot lookup() path still does one sync.Map.Load + one
// pointer-deref of an immutable struct (no extra locks, no allocations on
// the read path).
//
// Ship #91 / 0.30.211: added `permanent` + `retryCount` to support the
// Lever A re-verify gate. `permanent` is set by the Class C heuristic (the
// full result is shape-identical to the page-keyed S_ra modulo length —
// see IsStructurallyNonSliceable) and bounds the retry cap to 1: once a
// shape is proven STRUCTURALLY non-sliceable (e.g. an aggregation RA whose
// output is the SAME shape regardless of pagination) no number of re-
// verifies will change that, so we stop. `retryCount` increments on every
// record() AFTER the first; the lookup-gate at expiry uses it to bound
// re-verify attempts to N attempts per (raKey, sliceShape) per pod life.
type sliceabilityMemoEntry struct {
	verdict bool
	// RAKey/SliceShape are the SAME strings the caller passed to record() —
	// kept here only so the snapshot can emit them without a reverse lookup.
	// Reading them is read-only (immutable post-Store), no mutex needed.
	raKey      string
	sliceShape string
	labels     SliceabilityLabels
	// recordedAtUnix is the Unix-seconds timestamp captured at first record().
	// Re-record (refresh in place) does NOT update it — the snapshot field
	// answers "when was this verdict first frozen", which is what the
	// operator needs to distinguish "recorded long ago" from "just now".
	recordedAtUnix int64
	// lastUpdatedAtUnix is the Unix-seconds timestamp of the MOST RECENT
	// record() call for this (raKey, sliceShape) — updated on every record()
	// call (insert AND refresh-in-place). Discriminates Mode-3 stuck-since-boot
	// (verdict=false, lastUpdated ≈ pod-start) from Mode-3 refreshing-still-
	// failing (verdict=false, lastUpdated ≈ now) — the two need different
	// next ships. Architect REQ-2 (0.30.210).
	lastUpdatedAtUnix int64
	// permanent is set TRUE when the Class C heuristic
	// (IsStructurallyNonSliceable) fires at byte-verify time: the FULL result
	// is shape-identical to the page-keyed S_ra (an aggregation that does
	// not paginate) AND has more rows than perPage. Such an entry's verdict
	// will NEVER flip on re-verify, so the retry cap is effectively 1.
	// Mechanism-uniform: derived from the SHAPE of (full, sRA, perPage), no
	// resource/name/GVR literal (feedback_no_special_cases).
	permanent bool
	// retryCount is the number of record() calls AFTER the first. Used by
	// the lookup-gate to refuse re-verify once it crosses the cap.
	retryCount int8
}

// sliceabilityMemo is the process-wide bounded map of
// hash(RAFullListKey × sliceShape) -> *sliceabilityMemoEntry. sync.Map for
// lock-free Load on the hot serve path; the insertion-order cap is enforced
// only on the Store-miss path (rare — once per distinct (key, shape)).
type sliceabilityMemo struct {
	verdicts sync.Map // string(hash) -> *sliceabilityMemoEntry
	count    int64    // approximate entry count (atomic via mu)
	mu       sync.Mutex
	cap      int

	// #42 FIX-A — SHAPE-LEVEL negative-permanent set. When a
	// (raKey × sliceShape) verdict is recorded false AND permanent (Class C
	// structurally non-sliceable), the sliceShape is ALSO recorded here.
	// sliceShape is IDENTITY-FREE (sliceShapeHash folds caller class/gvr/ns/
	// name + the RA slice-jq — NO BindingUID, no per-user dimension), so a
	// structurally-non-sliceable shape proven under identity #1 is proven for
	// EVERY identity. lookup() consults it when the per-key verdict is UNKNOWN
	// → returns (false, true) → the always-correct page-keyed fallback (never
	// a wrong result — a shared NEGATIVE can only route to fallback; positives
	// stay strictly per-key, never shared). This is what collapses the 3
	// resolves/identity to 1 on aggregation RAs (design §A5 FIX-A). Keyed on
	// sliceShape alone (no memoKey/raKey) so it hits across the per-BindingUID
	// raKeys that never share today.
	shapeNegative sync.Map // sliceShape(string) -> struct{}
}

var raSliceabilityMemo = &sliceabilityMemo{cap: defaultSliceabilityMemoCap}

// nowUnix is the clock used to stamp recordedAtUnix. Indirected via atomic
// so tests can replace it without a race; production reads time.Now().Unix().
var nowUnix atomic.Pointer[func() int64]

func init() {
	def := func() int64 { return time.Now().Unix() }
	nowUnix.Store(&def)
}

// currentNowUnix returns the active clock function.
func currentNowUnix() int64 {
	f := nowUnix.Load()
	if f == nil {
		return time.Now().Unix()
	}
	return (*f)()
}

// defaultSliceabilityMemoCap bounds the sliceability memo. It is small:
// the distinct (RA × sliceShape) population is O(num_slicing_RAs ×
// num_caller_widget_shapes), a few hundred at most. The cap is a runaway
// guard, not a tuning knob.
const defaultSliceabilityMemoCap = 4096

// Ship #91 / 0.30.211 — Lever A + Lever C tunables.
//
// SLICEABILITY_REVERIFY_RATE_FLOOR_SECONDS — the minimum interval, in
// seconds, between re-verify attempts for any one (raKey × sliceShape).
// The same floor is reused by Lever C's dep-event invalidate: a noisy
// informer stream against a depended-on object cannot churn the memo
// faster than this floor. Default 60s; design-time falsifier expects 60s.
// The PM brief calls out a conditional bump to 300s if Phase 6 observes
// per-RA cohort count > 12 (vs PF-1's 7) — re-tune via this env knob, no
// code change.
const envSliceabilityReverifyRateFloorSeconds = "SLICEABILITY_REVERIFY_RATE_FLOOR_SECONDS"

// defaultSliceabilityReverifyRateFloorSeconds is the design-time default.
const defaultSliceabilityReverifyRateFloorSeconds int64 = 60

// SLICEABILITY_RETRY_CAP — the max retryCount the lookup-gate enforces for
// NON-permanent entries (Class A/D). Permanent (Class C) entries are
// capped at 1 regardless of this knob. Default 3. Lowering to 1 effectively
// disables Lever A (one retry total, after which stuck-false stays stuck);
// raising past int8-saturate (127) is a no-op.
const envSliceabilityRetryCap = "SLICEABILITY_RETRY_CAP"

const defaultSliceabilityRetryCap = 3

// sliceabilityReverifyRateFloorSeconds returns the active rate-floor
// (in seconds) for Lever A re-verify + Lever C dep-event invalidate.
// Read on every lookup/invalidate; environment-driven so deployers can
// re-tune at pod start without a code change.
func sliceabilityReverifyRateFloorSeconds() int64 {
	return int64FromEnv(envSliceabilityReverifyRateFloorSeconds, defaultSliceabilityReverifyRateFloorSeconds)
}

// sliceabilityRetryCap returns the effective retry cap for an entry:
// 1 when permanent==true (Class C — never retry beyond once), the env
// override / default otherwise.
func sliceabilityRetryCap(permanent bool) int {
	if permanent {
		return 1
	}
	return intFromEnv(envSliceabilityRetryCap, defaultSliceabilityRetryCap)
}

// jsonMarshalCanonical is the canonical-JSON encoder used by
// IsStructurallyNonSliceable's shape-equality probe. encoding/json sorts
// map keys by default → byte-stable for equality comparison.
func jsonMarshalCanonical(m map[string]any) ([]byte, error) {
	return json.Marshal(m)
}

// sliceShapeHash fingerprints HOW a caller slices a RAFullList cell:
// (callerClass, caller gvr/ns/name, hash of the RA's output slice jq). Two
// callers with the same shape share a verdict; different shapes get
// INDEPENDENT verdicts. The RA slice-jq hash distinguishes RAs that emit a
// non-sliceable shape (e.g. a per-page aggregation) from clean sort-then-
// slice RAs even under the same caller identity.
func sliceShapeHash(callerClass, callerGroup, callerVersion, callerResource,
	callerNamespace, callerName, raSliceJQ string) string {
	h := sha256.New()
	h.Write([]byte("sliceShape\x00"))
	h.Write([]byte(callerClass))
	h.Write([]byte{0})
	h.Write([]byte(callerGroup))
	h.Write([]byte{0})
	h.Write([]byte(callerVersion))
	h.Write([]byte{0})
	h.Write([]byte(callerResource))
	h.Write([]byte{0})
	h.Write([]byte(callerNamespace))
	h.Write([]byte{0})
	h.Write([]byte(callerName))
	h.Write([]byte{0})
	h.Write([]byte(raSliceJQ))
	return hex.EncodeToString(h.Sum(nil))
}

// memoKey composes the per (RAFullList-key × sliceShape) memo key.
func memoKey(raFullListKey, sliceShape string) string {
	h := sha256.New()
	h.Write([]byte(raFullListKey))
	h.Write([]byte{0})
	h.Write([]byte(sliceShape))
	return hex.EncodeToString(h.Sum(nil))
}

// SliceabilityVerdict returns (sliceable, known) for the given
// (RAFullListKey × sliceShape). known=false means no verdict has been
// recorded yet — the caller must run the byte-verify and RecordSliceability.
//
// Ship #91 / 0.30.211 — Lever A async-worker TTL gate (PM-ratified
// 2026-05-30, supersedes the architect-brief "return known=false" wording).
// When an entry's verdict is FALSE and the entry has aged past T_unverify
// AND retryCount is below its cap, lookup STILL returns (false, true) —
// the cached false verdict is served, the customer /call falls through to
// the page-keyed fallback and returns IMMEDIATELY. No synchronous re-
// verify burden on the customer. The lookup also schedules two off-/call
// async actions:
//
//  1. SubmitSliceabilityInvalidate(raFullListKey) — the bounded async
//     invalidator worker will Delete() the memo entry (after the rate-
//     floor check) so the NEXT /call after the worker runs sees
//     known=false and enters first-sight under its own /call ctx.
//  2. EnqueueRefresh(raFullListKey) — schedules an L1 re-resolve on the
//     refresher's workqueue, so by the time that "next /call" hits the
//     first-sight path the unpaginated full is warm in L1 (≈50-100ms
//     hot re-resolve, not the cold ~50s fan-out).
//
// Permanent entries (Class C heuristic — IsStructurallyNonSliceable) are
// short-circuited: lookup returns the cached false verdict but does NOT
// submit or enqueue. A structurally non-sliceable shape will never flip,
// so burning worker + refresher cycles on it is pure waste.
//
// Customer-priority preserved (feedback_customer_priority_over_refresher):
// the customer /call NEVER blocks on a re-verify. The verdict flips on a
// LATER /call after the worker has invalidated AND the refresher has
// warmed the L1 cell. retryCount climbs only when the eventual first-
// sight path runs record() with a fresh verdict; the cap (3 normal, 1
// permanent) bounds the total re-verify cycles per pod life.
//
// Mechanism-uniform: keyed off (lastUpdatedAtUnix, retryCount, permanent)
// on EVERY raFullList entry — no resource/name/GVR literal
// (feedback_no_special_cases).
//
// The TTL gate does NOT mutate the entry — invalidate (via the async
// worker) Delete()s it, and the next /call's record() repopulates it.
func (m *sliceabilityMemo) lookup(raFullListKey, sliceShape string) (sliceable, known bool) {
	v, ok := m.verdicts.Load(memoKey(raFullListKey, sliceShape))
	if !ok {
		// #42 FIX-A — no PER-KEY verdict for this (raKey × shape) yet. Before
		// returning unknown (which forces first-sight = the expensive triple
		// resolve), consult the SHAPE-LEVEL negative set: if this sliceShape was
		// proven structurally non-sliceable under ANY raKey (identity), it is
		// non-sliceable for THIS raKey too (the shape is identity-free). Return
		// (false, true) → the caller takes the always-correct page-keyed
		// fallback — NEVER a wrong result (a shared negative can only route to
		// fallback; positives are never shared). This collapses identities
		// #2..N on a structurally-non-sliceable RA from 3 resolves to 1.
		if _, neg := m.shapeNegative.Load(sliceShape); neg {
			return false, true
		}
		return false, false
	}
	e, ok := v.(*sliceabilityMemoEntry)
	if !ok || e == nil {
		return false, false
	}
	if !e.verdict && !e.permanent {
		// Lever A: stuck-false TTL gate. If the entry has aged past
		// T_unverify AND retryCount is below its cap, schedule the async
		// re-verify path off the customer /call. The cached false verdict
		// is STILL returned (caller falls back fast); flipping happens on
		// the next /call AFTER the worker has invalidated + the refresher
		// has warmed L1.
		now := currentNowUnix()
		ttl := sliceabilityReverifyRateFloorSeconds()
		cap := sliceabilityRetryCap(false)
		if (now-e.lastUpdatedAtUnix) > ttl && int(e.retryCount) < cap {
			SubmitSliceabilityInvalidate(raFullListKey)
			EnqueueRefresh(raFullListKey)
		}
	}
	return e.verdict, true
}

// record stores the verdict for (RAFullListKey × sliceShape). Idempotent;
// the bounded cap is enforced on the miss path only — once full, new
// verdicts are dropped (the caller then re-verifies per /call, which is the
// fail-safe page-keyed path, never a wrong result).
//
// TIMESTAMP SEMANTICS (architect REQ-2, 0.30.210):
//   - recordedAtUnix:    set on INSERT only, preserved on refresh-in-place.
//     Answers "when was this verdict first frozen".
//   - lastUpdatedAtUnix: set on EVERY record() (insert AND refresh-in-place).
//     Answers "when was the verdict last (re-)written". Discriminates
//     Mode-3 stuck-since-boot from Mode-3 refreshing-still-failing.
//
// RETRY / PERMANENT SEMANTICS (Ship #91 / 0.30.211, Lever A):
//   - retryCount:        incremented on every refresh-in-place; the lookup-
//     gate uses it (with the cap) to bound re-verify attempts.
//   - permanent:         STICKY-once-true (Class C heuristic). Once a /call
//     observes a structurally non-sliceable shape and passes permanent=true,
//     the field stays true across all subsequent record()s — a later /call
//     never silently un-permanents a verdict it could not authoritatively
//     re-classify (the cleanest read of "structurally non-sliceable" is
//     monotonic: if it ever was, it forever is for this pod's run).
//
// Labels and raKey/sliceShape are preserved across refresh-in-place (a
// refresh-without-labels never overwrites real labels with empty).
func (m *sliceabilityMemo) record(raFullListKey, sliceShape string, sliceable, permanent bool, labels SliceabilityLabels) {
	// #42 FIX-A — record the SHAPE-LEVEL negative when this verdict is
	// structurally-non-sliceable (false + permanent). Monotonic: once a shape
	// is proven structurally non-sliceable it stays so for the pod's life
	// (Class C is permanent by construction — same monotonicity as the
	// per-entry permanent flag below). A later positive verdict on a DIFFERENT
	// raKey of the same shape is impossible (structural shape doesn't flip),
	// so we never need to un-record.
	if !sliceable && permanent {
		m.shapeNegative.Store(sliceShape, struct{}{})
	}
	mk := memoKey(raFullListKey, sliceShape)
	now := currentNowUnix()
	if prev, exists := m.verdicts.Load(mk); exists {
		// Refresh in place: build a NEW entry (entries are immutable post-Store)
		// preserving recordedAt + labels, updating verdict + lastUpdatedAt +
		// retryCount + permanent (sticky-once-true).
		pe, _ := prev.(*sliceabilityMemoEntry)
		if pe != nil {
			next := pe.retryCount
			if int(next) < 127 { // saturate to avoid int8 overflow
				next++
			}
			m.verdicts.Store(mk, &sliceabilityMemoEntry{
				verdict:           sliceable,
				raKey:             pe.raKey,
				sliceShape:        pe.sliceShape,
				labels:            pe.labels,
				recordedAtUnix:    pe.recordedAtUnix,
				lastUpdatedAtUnix: now,
				permanent:         pe.permanent || permanent,
				retryCount:        next,
			})
		} else {
			// Defensive: a pre-0.30.210 raw bool snuck in somehow — replace it
			// with a fresh entry. No correctness impact.
			m.verdicts.Store(mk, &sliceabilityMemoEntry{
				verdict:           sliceable,
				raKey:             raFullListKey,
				sliceShape:        sliceShape,
				labels:            labels,
				recordedAtUnix:    now,
				lastUpdatedAtUnix: now,
				permanent:         permanent,
				retryCount:        0,
			})
		}
		return
	}
	m.mu.Lock()
	if m.cap > 0 && m.count >= int64(m.cap) {
		m.mu.Unlock()
		return // cap reached — drop; caller re-verifies (fail-safe)
	}
	m.count++
	m.mu.Unlock()
	m.verdicts.Store(mk, &sliceabilityMemoEntry{
		verdict:           sliceable,
		raKey:             raFullListKey,
		sliceShape:        sliceShape,
		labels:            labels,
		recordedAtUnix:    now,
		lastUpdatedAtUnix: now,
		permanent:         permanent,
		retryCount:        0,
	})
}

// invalidate removes EVERY memo entry whose raKey matches raFullListKey.
// Used by Lever C (dep-event invalidate) — when an informer event fires
// against an object that an RAFullList cell depends on, the verdicts keyed
// by that raFullListKey are stale and must be re-verified on next /call.
// Rate-limited to one invalidation per (raFullListKey, sliceShape) per
// T_unverify (matches Lever A's gate so a noisy informer event stream
// cannot churn the memo). Returns the number of entries that were
// invalidated for observability.
//
// Mechanism-uniform: keyed off raFullListKey only (no resource/name
// literal); rate-limit floor read from
// sliceabilityReverifyRateFloorSeconds() (the same knob Lever A uses).
//
// Walks the memo's sync.Map — O(entries). The memo is capped at
// defaultSliceabilityMemoCap (4096) so even a worst-case walk is trivial
// at the dep-event cadence.
func (m *sliceabilityMemo) invalidate(raFullListKey string) int {
	if raFullListKey == "" {
		return 0
	}
	now := currentNowUnix()
	ttl := sliceabilityReverifyRateFloorSeconds()
	removed := 0
	m.verdicts.Range(func(k, v any) bool {
		e, ok := v.(*sliceabilityMemoEntry)
		if !ok || e == nil {
			return true
		}
		if e.raKey != raFullListKey {
			return true
		}
		// Rate-floor: refuse to invalidate if the entry was last updated
		// within T_unverify (Lever C floor shared with Lever A).
		if (now - e.lastUpdatedAtUnix) < ttl {
			return true
		}
		m.verdicts.Delete(k)
		// #42 FIX-A arch C-1: a per-key invalidation must ALSO clear the
		// shape-level negative it fed, or the shape set would pin a permanent
		// false that SliceabilityLookup keeps returning known=false for even
		// after the underlying shape may have changed — defeating the Lever-C
		// dep-event self-heal for the shared-negative path. The entry carries
		// its sliceShape, so we clear it here at the same delete point (the
		// rate-floor above already bounds churn). Next lookup on this shape then
		// returns known=false → first-sight re-verify restores the correct
		// verdict.
		m.shapeNegative.Delete(e.sliceShape)
		m.mu.Lock()
		if m.count > 0 {
			m.count--
		}
		m.mu.Unlock()
		removed++
		return true
	})
	return removed
}

// InvalidateSliceabilityForKey is the package-level entry point for Lever
// C dep-event invalidation. The refresher's dep-event hook calls this for
// every RAFullList L1 entry whose dep tuple matched. Rate-limited inside
// invalidate(); safe to call from the refresher hook goroutine without
// further synchronisation. Returns the number of memo entries invalidated.
func InvalidateSliceabilityForKey(raFullListKey string) int {
	return raSliceabilityMemo.invalidate(raFullListKey)
}

// SliceabilityLookup is the package-level lookup against the process memo.
func SliceabilityLookup(raFullListKey, sliceShape string) (sliceable, known bool) {
	return raSliceabilityMemo.lookup(raFullListKey, sliceShape)
}

// SliceabilityShapeKnownNegative reports whether the given sliceShape has been
// recorded structurally non-sliceable (Class C, false+permanent) under ANY
// raKey — the #42 FIX-A shape-level negative set. Identity-free by
// construction (sliceShape folds no BindingUID). FIX-B (seedRAFullListForWidget)
// consults it to SKIP the discarded fallback resolve on a known-non-sliceable
// widget without needing the per-BindingUID raKey.
func SliceabilityShapeKnownNegative(sliceShape string) bool {
	_, neg := raSliceabilityMemo.shapeNegative.Load(sliceShape)
	return neg
}

// RecordSliceability records a byte-verify verdict in the process memo with
// EMPTY labels. Test/back-compat entry point — production callers SHOULD use
// RecordSliceabilityWithLabels so the read-side snapshot can describe the
// entry's caller-CR identity. permanent=false (the test-default).
func RecordSliceability(raFullListKey, sliceShape string, sliceable bool) {
	raSliceabilityMemo.record(raFullListKey, sliceShape, sliceable, false, SliceabilityLabels{})
}

// RecordSliceabilityWithLabels records a byte-verify verdict + the caller-CR
// identity labels. The labels are READ-SIDE ONLY (the snowplow_ra_full_list_memo
// expvar snapshot) — they DO NOT participate in the verdict key (memoKey is
// built from raKey + sliceShape only, byte-identical to pre-0.30.210). So two
// callers that hash to the SAME sliceShape (an architectural impossibility
// given sliceShapeHash folds caller ns/name) would share one verdict and the
// labels of WHICHEVER recorded first; in practice sliceShape is unique per
// caller-CR so labels are 1:1 with entries.
//
// permanent=false. The /call site that wants to mark a verdict as
// structurally non-sliceable (Class C heuristic — see
// IsStructurallyNonSliceable) MUST call RecordSliceabilityClassified.
func RecordSliceabilityWithLabels(raFullListKey, sliceShape string, sliceable bool, labels SliceabilityLabels) {
	raSliceabilityMemo.record(raFullListKey, sliceShape, sliceable, false, labels)
}

// RecordSliceabilityClassified records a byte-verify verdict + labels + the
// `permanent` flag from the Class C heuristic (IsStructurallyNonSliceable).
// When permanent is true the retry cap is effectively 1 (architectural
// invariant: a shape proven non-sliceable BY STRUCTURE — full-shape ==
// page-shape modulo length — will never start slicing cleanly on re-verify;
// continuing to retry would just thrash the cache).
//
// `permanent` is monotonic on refresh-in-place: once an entry has been
// recorded permanent=true, a later record() with permanent=false does NOT
// un-permanent it (see record()).
func RecordSliceabilityClassified(raFullListKey, sliceShape string, sliceable, permanent bool, labels SliceabilityLabels) {
	raSliceabilityMemo.record(raFullListKey, sliceShape, sliceable, permanent, labels)
}

// SliceabilityMemoEntry is the public, snapshot-friendly view of one memo
// entry. Field names map directly to the snowplow_ra_full_list_memo JSON keys.
//
// RecordedAtUnixSeconds / LastUpdatedAtUnixSeconds / Permanent / RetryCount
// are deliberately NOT omitempty — operators reading /debug/vars shouldn't
// have to guess "field not in this build" vs "value zero" (architect REQ-2
// callout, reaffirmed for Ship #91 Permanent + RetryCount). Caller labels
// keep omitempty because zero-label entries (legacy RecordSliceability
// tests) shouldn't crowd the snapshot.
type SliceabilityMemoEntry struct {
	RAKey                    string `json:"raKey"`
	SliceShape               string `json:"sliceShape"`
	Verdict                  bool   `json:"verdict"`
	RecordedAtUnixSeconds    int64  `json:"recordedAtUnixSeconds"`
	LastUpdatedAtUnixSeconds int64  `json:"lastUpdatedAtUnixSeconds"`
	// Permanent — TRUE when the Class C heuristic
	// (IsStructurallyNonSliceable) fired; gates retry cap to 1. Ship #91.
	Permanent bool `json:"permanent"`
	// RetryCount — number of record() refreshes for this entry. Ship #91.
	RetryCount      int8   `json:"retryCount"`
	CallerClass     string `json:"callerClass,omitempty"`
	CallerGroup     string `json:"callerGroup,omitempty"`
	CallerVersion   string `json:"callerVersion,omitempty"`
	CallerResource  string `json:"callerResource,omitempty"`
	CallerNamespace string `json:"callerNamespace,omitempty"`
	CallerName      string `json:"callerName,omitempty"`
}

// SliceabilityMemoSnapshot returns a flat list of every recorded entry in the
// process memo. The walk is sync.Map.Range — concurrent record/lookup callers
// race-free; the result is a point-in-time view (entries added during the walk
// may or may not appear). Order is unspecified.
//
// Cost: O(entries). The memo is capped at defaultSliceabilityMemoCap (4096)
// so worst case is a 4096-element slice — trivial at the typical
// expvar-scrape cadence (10-60s).
//
// READ-ONLY: this function never mutates the memo. It is intended for
// /debug/vars consumption only; nothing on the serve path calls it.
func SliceabilityMemoSnapshot() []SliceabilityMemoEntry {
	return raSliceabilityMemo.snapshot()
}

// snapshot walks the memo's sync.Map and returns a flat slice of public
// entries.
func (m *sliceabilityMemo) snapshot() []SliceabilityMemoEntry {
	// Pre-size from the approximate count under the cap mutex; sync.Map.Range
	// may add/remove during iteration but the cap is small so over/under by
	// a few is fine.
	m.mu.Lock()
	n := m.count
	m.mu.Unlock()
	if n < 0 {
		n = 0
	}
	out := make([]SliceabilityMemoEntry, 0, n)
	m.verdicts.Range(func(_, v any) bool {
		e, ok := v.(*sliceabilityMemoEntry)
		if !ok || e == nil {
			return true
		}
		out = append(out, SliceabilityMemoEntry{
			RAKey:                    e.raKey,
			SliceShape:               e.sliceShape,
			Verdict:                  e.verdict,
			RecordedAtUnixSeconds:    e.recordedAtUnix,
			LastUpdatedAtUnixSeconds: e.lastUpdatedAtUnix,
			Permanent:                e.permanent,
			RetryCount:               e.retryCount,
			CallerClass:              e.labels.CallerClass,
			CallerGroup:              e.labels.CallerGroup,
			CallerVersion:            e.labels.CallerVersion,
			CallerResource:           e.labels.CallerResource,
			CallerNamespace:          e.labels.CallerNamespace,
			CallerName:               e.labels.CallerName,
		})
		return true
	})
	return out
}

// SliceShapeHash is the exported sliceShape fingerprint (see sliceShapeHash).
func SliceShapeHash(callerClass, callerGroup, callerVersion, callerResource,
	callerNamespace, callerName, raSliceJQ string) string {
	return sliceShapeHash(callerClass, callerGroup, callerVersion, callerResource,
		callerNamespace, callerName, raSliceJQ)
}

// resetSliceabilityMemoForTest clears the process memo. TEST-ONLY.
func resetSliceabilityMemoForTest() {
	raSliceabilityMemo = &sliceabilityMemo{cap: defaultSliceabilityMemoCap}
}
