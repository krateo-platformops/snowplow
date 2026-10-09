// resolved_entry_items.go — #578: materialise a LIST content entry's
// pre-parsed items on FIRST SERVE rather than at Put.
//
// See the itemsLazy field doc on ResolvedEntry (resolved.go) for why the timing
// moved and why this is never worse than either earlier placement.
package cache

import (
	"sync/atomic"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// lazyListItems is the published result of one lazy materialisation. Every
// field is write-once: the struct is built, CAS-published, and thereafter only
// read. `ok` is part of the published value so a NEGATIVE result (a malformed
// envelope) is memoised too — otherwise a cell whose RawJSON cannot be parsed
// would re-attempt the parse on every single serve, which is precisely the
// per-hit cost this exists to remove.
type lazyListItems struct {
	items      []*unstructured.Unstructured
	apiVersion string
	kind       string
	ok         bool
}

// Counters for the #578 lazy path. Package-level atomics.
//
// READ THEM AS A PAIR. `parsed` alone cannot distinguish "lazy is working" from
// "lazy is thrashing": the signal is parsed vs served. In steady state served
// should dominate parsed by the read:refresh ratio, and parsed climbing toward
// served means cells are being rebuilt as fast as they are read, so the lazy
// memo is buying nothing (feedback_a_counter_whose_zero_reads_as_health_is_not_
// a_detector).
var (
	lazyItemsParsedTotal atomic.Uint64
	lazyItemsServedTotal atomic.Uint64
	lazyItemsEagerTotal  atomic.Uint64
)

// LazyItemsStats is an atomic snapshot of the #578 lazy-materialisation
// counters. Exported so tests and the expvar surface can read them without
// reaching into package state.
type LazyItemsStats struct {
	// Parsed counts materialisations that actually ran a parse.
	Parsed uint64
	// Served counts EnsureItems calls answered from an already-published
	// memo (the steady-state hit).
	Served uint64
	// Eager counts EnsureItems calls short-circuited by a Put-time Items
	// slice — the pre-#578 shape, still honoured for back-compatibility.
	Eager uint64
}

// LazyItemsStatsSnapshot returns the current counter values.
func LazyItemsStatsSnapshot() LazyItemsStats {
	return LazyItemsStats{
		Parsed: lazyItemsParsedTotal.Load(),
		Served: lazyItemsServedTotal.Load(),
		Eager:  lazyItemsEagerTotal.Load(),
	}
}

// ResetLazyItemsStatsForTest zeroes the counters. Test-only seam; production
// code MUST NOT call it.
func ResetLazyItemsStatsForTest() {
	lazyItemsParsedTotal.Store(0)
	lazyItemsServedTotal.Store(0)
	lazyItemsEagerTotal.Store(0)
}

// EnsureItems returns the entry's pre-parsed LIST envelope, materialising it
// at most once per entry via `parse`.
//
// Resolution order, and why:
//
//  1. A non-empty eager Items slice wins. An entry Put by a path that still
//     fills Items (or by an older binary mid-rollout) is served exactly as
//     before, so this field is purely additive and the change is safe to roll
//     without draining the cache.
//  2. An already-published memo is returned as-is — the steady-state path,
//     one atomic load.
//  3. Otherwise `parse` runs against RawJSON and the result is CAS-published.
//
// `parse` is injected because parseListEnvelope lives in the api package (it
// needs the GVR to synthesize a missing apiVersion/kind); this package must not
// import it. It is called with the entry's RawJSON and must NOT retain or
// mutate those bytes.
//
// A nil receiver, a nil parse, or an empty RawJSON returns ok=false without
// publishing anything, so a caller can always fall back to its own unmarshal
// path.
func (e *ResolvedEntry) EnsureItems(
	parse func(raw []byte) (items []*unstructured.Unstructured, apiVersion, kind string, ok bool),
) ([]*unstructured.Unstructured, string, string, bool) {
	if e == nil {
		return nil, "", "", false
	}
	// 1. Eager Items from a Put-time parse.
	if len(e.Items) > 0 {
		lazyItemsEagerTotal.Add(1)
		return e.Items, e.ItemsAPIVersion, e.ItemsKind, true
	}
	// 2. Published memo.
	if p := e.itemsLazy.Load(); p != nil {
		lazyItemsServedTotal.Add(1)
		return p.items, p.apiVersion, p.kind, p.ok
	}
	if parse == nil || len(e.RawJSON) == 0 {
		return nil, "", "", false
	}
	// 3. Materialise. A racing caller may do the same work; the CAS decides
	// which private tree becomes the shared one, and the Load below makes
	// every caller return THAT tree rather than its own.
	items, apiVersion, kind, ok := parse(e.RawJSON)
	lazyItemsParsedTotal.Add(1)
	e.itemsLazy.CompareAndSwap(nil, &lazyListItems{
		items:      items,
		apiVersion: apiVersion,
		kind:       kind,
		ok:         ok,
	})
	p := e.itemsLazy.Load()
	if p == nil {
		// Unreachable: the CAS either stored our value or found another.
		// Returning the local result keeps the method total rather than
		// nil-panicking if that ever changes.
		return items, apiVersion, kind, ok
	}
	return p.items, p.apiVersion, p.kind, p.ok
}

// HasMaterialisedItems reports whether this entry already carries pre-parsed
// items, by either route. Test/diagnostic seam — it does NOT trigger a parse,
// so a debug surface can report the shape without changing it.
func (e *ResolvedEntry) HasMaterialisedItems() bool {
	if e == nil {
		return false
	}
	if len(e.Items) > 0 {
		return true
	}
	p := e.itemsLazy.Load()
	return p != nil && p.ok
}
