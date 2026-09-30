// ra_full_list_store.go — Ship 4a (0.30.198): the RAFullList cell Put +
// cost-based pin predicate + serve-outcome metrics. Kept in its own file so
// the 4a layer is wholesale-deletable (project_caching_is_provisional).

package cache

import (
	"encoding/json"
	"os"
	"strconv"
	"sync/atomic"
)

const (
	// envRAFullListPinBytesThreshold — Ship 4a (0.30.198). A RAFullList cell
	// whose encoded envelope is at least this many bytes is "EXPENSIVE" and
	// is PINNED (resident, eviction-protected) so an expensive cohort's
	// prewarmed full-list survives LRU thrash until its first visit
	// (feedback_zero_cold_navigations_hard_requirement). Cheap cells (small
	// envelopes) stay TRANSIENT — fast-cold from the warm substrate is fine.
	//
	// This is a MEASURED-COST predicate (envelope bytes), NOT an identity
	// literal (feedback_no_special_cases): every RA of every GVR is judged
	// by the same byte threshold. The default 1 MiB matches the design's
	// "envelope bytes > ~1MB" expensive-cell bar; admin's ~18 MiB
	// compositions-panels full list is far over it and pins, while a narrow
	// cohort's few-KB list stays transient.
	envRAFullListPinBytesThreshold     = "RESOLVED_CACHE_RAFULLLIST_PIN_BYTES"
	defaultRAFullListPinBytesThreshold = int64(1) * 1024 * 1024 // 1 MiB
)

// raFullListPinBytesThreshold returns the expensive-cell byte threshold.
func raFullListPinBytesThreshold() int64 {
	if v := os.Getenv(envRAFullListPinBytesThreshold); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			return n
		}
	}
	return defaultRAFullListPinBytesThreshold
}

// PutRAFullList stores the RA full-result map under key as a RAFullList cell,
// applying the cost-based pin predicate: a cell whose encoded envelope is
// >= raFullListPinBytesThreshold() is marked Pinned (resident region,
// eviction-protected); a cheaper cell is stored transient. The pin is
// HONOURED by Put only when the resident budget permits (else demoted — see
// Put). Returns the encoded bytes (for the caller's serve, avoiding a second
// marshal) and whether the cell was pinned.
//
// Inputs is stored so the refresher re-resolves the cell on a dirty-mark
// (and re-pins via the refresher's own path — resolve_populate.go carries the
// prior pin and re-Puts through the #189-guarded ReplaceIfGen). A nil/marshal-
// fail full is a no-op returning (nil,false).
func (c *ResolvedCacheStore) PutRAFullList(key string, inputs ResolvedKeyInputs, full map[string]any) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	encoded, err := json.Marshal(full)
	if err != nil {
		return nil, false
	}
	pin := int64(len(encoded)) >= raFullListPinBytesThreshold()
	in := inputs // copy onto the heap for the entry
	// uaf-scope-waiver: UNREACHABLE for a refilter-narrowed body. The only caller is apiref.raFullListServe, which bypasses this whole layer (returns served=false) for a RESTAction declaring a userAccessFilter BEFORE it derives the key — so no refilter output can arrive here. Falsified by TestA1_RAFullList_UAFBypassesCell_ControlServes (no cell under the derived raKey) and the warm-cell ordering arm.
	// scope-waiver:TTLOverride: raFullList-class cell — 1.12.3 A-1 CORRECTED WAIVER. The pre-1.12.3 text claimed UAF refilter output "lands in the per-page restactions cell, never here"; that was WRONG (this cell holds the apiRef'd RA's own full resolve output, UAF stage included, keyed on BindingUID ALONE — no RBACSubGen). A UAF-bearing RA can no longer REACH this Put: raFullListServe bypasses the whole layer for it (ra_full_list.go), so every cell written here is non-UAF and needs no UAF cap. Pinned/TTL governed by the 4a resident-budget path. Revisit when 1.13.0 folds the UAF scope into the key (uaf_shortttl.go R-d-4 SITE MAP, "THE FOURTH SITE").
	c.Put(key, &ResolvedEntry{
		RawJSON: encoded,
		Inputs:  &in,
		Pinned:  pin,
	})
	return encoded, pin
}

// PutRAFullListIfGen is the #323 generation-guarded PutRAFullList. It stores the
// RA full-result cell ONLY if key's generation still equals capturedGen — the
// value captured (via CaptureGen) BEFORE the raFullListServe re-resolve that
// produced `full`. If a DELETE-eviction bumped the generation during that
// resolve, the write is REFUSED (the pre-delete full body is not resurrected) and
// it returns false — the caller still serves the fresh slice from its already-
// resolved `full` and declines the dep-Record. Returns true iff the cell was
// stored. Same marshal + cost-based pin predicate as PutRAFullList; a nil/marshal-
// fail full is a no-op returning false. PutIfGen (insert-or-replace), not
// ReplaceIfGen: raFullListServe is the SERVE path and first-populates the raKey
// cell on a cold /call, so an insert-on-absent (gen 0) must succeed.
func (c *ResolvedCacheStore) PutRAFullListIfGen(key string, inputs ResolvedKeyInputs, full map[string]any, capturedGen uint64) bool {
	if c == nil {
		return false
	}
	encoded, err := json.Marshal(full)
	if err != nil {
		return false
	}
	pin := int64(len(encoded)) >= raFullListPinBytesThreshold()
	in := inputs // copy onto the heap for the entry
	// uaf-scope-waiver: UNREACHABLE for a refilter-narrowed body — twin of PutRAFullList above. The only caller is apiref.raFullListServe, which bypasses this whole layer (returns served=false) for a RESTAction declaring a userAccessFilter BEFORE it derives the key, so no refilter output can arrive here. The #323 generation guard does not change the UAF-scope reasoning — it is the same identity-free cell as PutRAFullList (uaf_shortttl.go R-d-4 SITE MAP).
	// scope-waiver:TTLOverride: raFullList-class cell — identical class + reasoning to PutRAFullList above (a UAF-bearing RA can no longer reach this Put; raFullListServe bypasses the whole layer for it before deriving the key), uaf_shortttl.go R-d-4 SITE MAP "THE FOURTH SITE".
	return c.PutIfGen(key, &ResolvedEntry{
		RawJSON: encoded,
		Inputs:  &in,
		Pinned:  pin,
	}, capturedGen)
}

// #323 — PutRAFullListPinned (an explicit-pin RAFullList re-Put) was REMOVED
// here as dead code: it had zero call sites (verified across all *.go incl.
// tests; no method-value / interface use). Leaving it unguarded would have been
// a #189-resurrection loaded gun — a future caller would get an un-gen-guarded
// Put with no arm covering it. The live RAFullList re-pin path is the refresher
// (resolve_populate.go: CacheEntryClassRAFullList → prePinned → ReplaceIfGen),
// already generation-guarded by #189. If an explicit-pin re-Put is ever needed
// again, add it as a gen-guarded variant (ReplaceIfGen — a re-pin replaces a
// resident cell and must never resurrect an evicted one).

// --- Serve-outcome metrics ---------------------------------------------
//
// Per feedback_measurement_use_expvar_not_log_tails: surface the 4a serve
// path's outcome distribution as atomic counters (also folded into the
// resolved_cache.summary line / Stats). The ratio of hit:repopulate:
// verified-slice:fallback is the 4a effectiveness signal.

type RAFullListServeOutcome int

const (
	// RAFullListServeHit — served a Go-slice over a CACHED full cell under a
	// known-sliceable verdict (the steady-state cheap path — no resolve).
	RAFullListServeHit RAFullListServeOutcome = iota
	// RAFullListServeRepopulateSlice — known-sliceable verdict but the cell
	// had been evicted; resolved unpaginated ONCE, re-Put, then Go-sliced.
	RAFullListServeRepopulateSlice
	// RAFullListServeVerifiedSlice — first sight of (RA × shape): byte-verify
	// PASSED; Put the cell + served the Go-slice.
	RAFullListServeVerifiedSlice
	// RAFullListServeFallback — byte-verify FAILED (not cleanly sliceable for
	// this shape); served the page-keyed resolve (correct, slower).
	RAFullListServeFallback
)

var (
	raFullListServeHitTotal           atomic.Uint64
	raFullListServeRepopulateTotal    atomic.Uint64
	raFullListServeVerifiedSliceTotal atomic.Uint64
	raFullListServeFallbackTotal      atomic.Uint64
)

// RecordRAFullListServe bumps the per-outcome counter.
func RecordRAFullListServe(o RAFullListServeOutcome) {
	switch o {
	case RAFullListServeHit:
		raFullListServeHitTotal.Add(1)
	case RAFullListServeRepopulateSlice:
		raFullListServeRepopulateTotal.Add(1)
	case RAFullListServeVerifiedSlice:
		raFullListServeVerifiedSliceTotal.Add(1)
	case RAFullListServeFallback:
		raFullListServeFallbackTotal.Add(1)
	}
}

// RAFullListServeStats is the read-only snapshot of the serve-outcome
// counters.
type RAFullListServeStats struct {
	Hit           uint64
	Repopulate    uint64
	VerifiedSlice uint64
	Fallback      uint64
}

// RAFullListServeSnapshot returns the current serve-outcome counters.
func RAFullListServeSnapshot() RAFullListServeStats {
	return RAFullListServeStats{
		Hit:           raFullListServeHitTotal.Load(),
		Repopulate:    raFullListServeRepopulateTotal.Load(),
		VerifiedSlice: raFullListServeVerifiedSliceTotal.Load(),
		Fallback:      raFullListServeFallbackTotal.Load(),
	}
}
