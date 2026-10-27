// ra_full_list_store.go — Ship 4a (0.30.198): the RAFullList cell Put +
// cost-based pin predicate + serve-outcome metrics. Kept in its own file so
// the 4a layer is wholesale-deletable (project_caching_is_provisional).

package cache

import (
	"context"
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
	// put-verdict: taken by the sole caller apiref.raFullListServe over the full list's own sinks (raFullListPermanentDecline, raFullListStageDecline).
	// #592 — BOTH PRIOR WAIVERS HERE WERE WITHDRAWN, because both rested on the
	// raFullListServe bypass that #592 removed. They read "UNREACHABLE for a
	// refilter-narrowed body ... the only caller bypasses this whole layer for a
	// RESTAction declaring a userAccessFilter BEFORE it derives the key" and
	// "every cell written here is non-UAF and needs no UAF cap". A narrowed body
	// now reaches this Put BY DESIGN, so neither sentence is true any more. (This
	// is the third time a waiver outlived the gate it cited — #561 withdrew the
	// widgets.go and seedOneWidget pair for the same reason. Grep the waivers
	// citing a gate before scoping it.)
	//
	// WHAT MAKES IT SAFE is not the bypass but the key: raFullList is
	// identity-BOUND (not in identityFreeClasses), and RAFullListKeyInputs folds
	// the #423 SubjectBindingSet digest + #435 sub-gen via SetIdentity, so two
	// requesters owed different narrowings occupy different cells (#588, and the
	// #561 argument this class was named in but not reached by).
	//
	// WHAT IS STILL EXPOSED is staleness, not leakage: the key folds the
	// requester's bindings, not the per-object verdicts the refilter re-evaluates,
	// so an out-of-band RBAC change makes that requester's OWN view wrong with
	// nothing to evict it. UAFTTLOverrideForEntry caps exactly that window, from
	// the HasUAF the caller stamps (declared + observed limbs, ra_full_list.go).
	// It returns 0 for a non-UAF cell, so every non-UAF raFullList cell keeps the
	// standard TTL and the 4a resident-budget pin path is unchanged.
	//
	// uaf-scope-waiver: CAPPED HERE, STAMPED BY THE CALLER — the one disposition
	// TestA1_EveryResolvedEntryPutSiteIsUAFGatedOrWaived cannot see, because it
	// matches at FUNCTION granularity and this is the first Put site whose two
	// halves sit either side of a package boundary. The cap is local and
	// unconditional (UAFTTLOverrideForEntry immediately below); the stamp is
	// apiref.raFullListServe's, which sets inputs.HasUAF from the declaration on
	// the RA it holds and from the UAFTouchedSink delta across its resolve — a
	// frame this package cannot reach and the guard cannot follow.
	//
	// WHY THIS IS NOT THE WAIVER SHAPE #592 JUST WITHDREW TWICE. Those asserted
	// "unreachable, because a gate elsewhere refuses" — a claim about someone
	// else's control flow, which went silently false the moment that gate was
	// removed. This asserts "the cap is here and the stamp is the caller's", and
	// it is backed by a live arm rather than by structure: deleting the caller's
	// stamp turns TestA1_RAFullList_UAFCrossUser_NoSharedCellServe RED on the
	// TTLOverride assertion (mutation 2 of the #592 falsifier), and making the
	// observed limb inert turns the ObservedUAFLimb arm RED (mutation 8). A
	// waiver whose failure mode is covered by a test that fails cannot rot
	// quietly; one that reasons about a gate can.
	//
	// uaf-scope-waiver: capped here via UAFTTLOverrideForEntry; HasUAF is stamped by the sole caller apiref.raFullListServe (declared + observed limbs) — see the paragraphs above.
	c.Put(key, &ResolvedEntry{
		RawJSON:     encoded,
		Inputs:      &in,
		Pinned:      pin,
		TTLOverride: UAFTTLOverrideForEntry(&in),
	})
	// #592 acceptance signal — counted AFTER the write, so it measures cells that
	// exist rather than Puts attempted. Put is unconditional, so every HasUAF cell
	// reaching here is stored.
	if in.HasUAF {
		BumpRAFullListUAFCached()
	}
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
func (c *ResolvedCacheStore) PutRAFullListIfGen(ctx context.Context, key string, inputs ResolvedKeyInputs, full map[string]any, capturedGen uint64) bool {
	if c == nil {
		return false
	}
	// #443 (a) — an inert (dry-run) resolve persists nothing.
	if Inert(ctx) {
		return false
	}
	encoded, err := json.Marshal(full)
	if err != nil {
		return false
	}
	pin := int64(len(encoded)) >= raFullListPinBytesThreshold()
	in := inputs // copy onto the heap for the entry
	// put-verdict: taken by the sole caller apiref.raFullListServe over the full list's own sinks (raFullListPermanentDecline, raFullListStageDecline).
	// #592 — twin of PutRAFullList above: both prior waivers withdrawn, same
	// reason (they cited the removed bypass), same cap from the same single
	// derivation. The #323 generation guard is orthogonal to the UAF scope — it
	// governs WHETHER this write lands, not what the body is narrowed by.
	//
	// Note the prior waiver also called this "the same identity-free cell as
	// PutRAFullList". That was wrong twice over: raFullList is identity-BOUND
	// (ComputeKey writes its identity segment), and being identity-bound is
	// precisely why the body may be cached at all.
	//
	// CAPPED HERE, STAMPED BY THE CALLER — twin of PutRAFullList above; see its
	// waiver for why this shape is not the one #592 withdrew. Same single caller
	// (apiref.raFullListServe), same two stamp limbs, same arms holding it up.
	//
	// uaf-scope-waiver: capped here via UAFTTLOverrideForEntry; HasUAF is stamped by the sole caller apiref.raFullListServe (declared + observed limbs) — twin of PutRAFullList above.
	stored := c.PutIfGen(ctx, key, &ResolvedEntry{
		RawJSON:     encoded,
		Inputs:      &in,
		Pinned:      pin,
		TTLOverride: UAFTTLOverrideForEntry(&in),
	}, capturedGen)
	// #592 acceptance signal — gated on `stored`, NOT on reaching the Put: a
	// generation-REFUSED write (a DELETE-eviction bumped the gen during the
	// resolve) stored no cell, and counting it would report cached UAF bodies
	// that do not exist.
	if stored && in.HasUAF {
		BumpRAFullListUAFCached()
	}
	return stored
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
