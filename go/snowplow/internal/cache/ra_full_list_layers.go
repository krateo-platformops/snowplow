// ra_full_list_layers.go — #406: converge LAYERED cells. A widget cell served by the
// Ship-4a fast path (apiref ra_full_list.go, the verdict-and-hit branch) is a Go-slice
// of the RAFullList cell (raKey). Its body is DERIVED from another L1 cell, not from
// the objects that cell was resolved from.
//
// THE GAP. While raKey is dirty-marked but not yet refreshed, a widget resolve that
// slices it serves the pre-change body, and when raKey is re-Put fresh nothing
// re-marked the widget: the #375 guard watches object coordinates, and the coordinate
// moved BEFORE the widget's resolve began. Two drivers reach it:
//   - the refresher: an RA edit (or backing event) marks both cells, and the widget is
//     dequeued first (the fan-out iterates a map; N parallel workers);
//   - the seed pass: an edit lands before the seed reaches the widget (raKey marked, the
//     widget cell does not exist yet); the SeedResolveMemo MISS slices the stale raKey,
//     and every memo HIT sibling replays that body.
//
// THE FIX (option a), in CONTENT VERSIONS:
//   - every stored RAFullList entry carries a contentVersion, from one process-wide
//     monotonic sequence. A re-Put whose bytes are EXACTLY equal to the resident prior
//     (compared under the store lock) keeps the prior's version; any other commit (a
//     change, or a refill after the cell was evicted) takes a new one, so a version a
//     widget sliced is never reissued;
//   - the fast path notes (raKey, version-it-sliced) on the resolve's dep-gen sink
//     (NoteRAFullListSlice); a SeedResolveMemo hit replays the producer's notes onto its
//     own sink (ReplayRAFullListSlices). The notes travel with THAT resolve to ITS Put;
//   - a widget Put carrying notes records them as the cell's sources, and remarks the
//     cell once if any noted raKey is now absent or at another version — whichever
//     resolve Puts last, the check is against what that Put's body holds;
//   - a raKey commit that takes a new version remarks every resident widget whose
//     recorded source version differs. An identical re-Put remarks nobody.
//
// The index lives under the store, updated under c.mu (lock order c.mu → layers.mu),
// so it is atomic with the store writes. It is bounded by the RESIDENT widget cells:
// entries are added by an accepted widget Put that left the cell resident, and removed
// when the widget cell leaves the store.

package cache

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
)

// raContentVersionSeq issues RAFullList content versions. Never reused.
var raContentVersionSeq atomic.Uint64

// layeredSource is one (raKey, contentVersion) a resolve Go-sliced.
type layeredSource struct {
	raKey   string
	version uint64
}

// LayeredSources is an opaque list of raKey versions a resolve sliced (#406).
type LayeredSources []layeredSource

// raLayerIndex is the raKey <-> resident-widget source index. Zero value is ready.
type raLayerIndex struct {
	mu       sync.Mutex
	byRA     map[string]map[string]uint64 // raKey -> widget L1 key -> version its body holds
	byWidget map[string]map[string]uint64 // widget L1 key -> raKey -> version
}

func (ix *raLayerIndex) dropWidgetLocked(widgetKey string) {
	for raKey := range ix.byWidget[widgetKey] {
		if ws := ix.byRA[raKey]; ws != nil {
			delete(ws, widgetKey)
			if len(ws) == 0 {
				delete(ix.byRA, raKey)
			}
		}
	}
	delete(ix.byWidget, widgetKey)
}

func (ix *raLayerIndex) dropEdgeLocked(raKey, widgetKey string) {
	if ws := ix.byRA[raKey]; ws != nil {
		delete(ws, widgetKey)
		if len(ws) == 0 {
			delete(ix.byRA, raKey)
		}
	}
	if rs := ix.byWidget[widgetKey]; rs != nil {
		delete(rs, raKey)
		if len(rs) == 0 {
			delete(ix.byWidget, widgetKey)
		}
	}
}

// sinkSources returns ctx's sink notes, one per raKey (the OLDEST version noted, so a
// resolve that saw two versions of one raKey is checked against the stale one).
// ok=false when ctx carries no sink: the Put's provenance is unknown.
func sinkSources(ctx context.Context) (map[string]uint64, bool) {
	s := depGenSinkFromContext(ctx)
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]uint64, len(s.layered))
	for _, src := range s.layered {
		if v, seen := out[src.raKey]; !seen || src.version < v {
			out[src.raKey] = src.version
		}
	}
	return out, true
}

// raLayerResidentEntryLocked returns key's resident entry, or nil. Callers hold c.mu.
func (c *ResolvedCacheStore) raLayerResidentEntryLocked(key string) *ResolvedEntry {
	if el, ok := c.index[key]; ok {
		return el.Value.(*lruItem).entry
	}
	return nil
}

// raLayerAfterCompareHook is a test seam (nil in production): it fires between the
// off-lock byte compare and the store lock, the window a concurrent commit can land in.
var raLayerAfterCompareHook atomic.Pointer[func()]

// raLayerEqualPrior is the #406 identical-content test, run BEFORE the store lock: a
// RAFullList body can be tens of MB, and c.mu is the lock every customer Get takes. It
// loads the resident prior under c.mu (a pointer read), releases it, and compares the
// bytes outside. It returns that prior when the bytes are equal, else nil. The caller
// re-checks, under c.mu, that the SAME prior is still resident: any other entry there
// (a concurrent commit, an eviction) means the compare is void and the Put counts as a
// change. Entries are never mutated once stored, so the off-lock read is safe.
func (c *ResolvedCacheStore) raLayerEqualPrior(key string, entry *ResolvedEntry) *ResolvedEntry {
	if !isRAFullListEntry(entry) {
		return nil
	}
	c.mu.Lock()
	prior := c.raLayerResidentEntryLocked(key)
	c.mu.Unlock()
	var eq *ResolvedEntry
	if prior != nil && isRAFullListEntry(prior) && prior.contentVersion != 0 &&
		bytes.Equal(prior.RawJSON, entry.RawJSON) {
		eq = prior
	}
	if fn := raLayerAfterCompareHook.Load(); fn != nil {
		(*fn)()
	}
	return eq
}

// raLayerConfirmPriorLocked returns equalPrior iff it is still the resident entry under
// key, else nil (the off-lock compare is void). Callers hold c.mu, before putCoreLocked.
func (c *ResolvedCacheStore) raLayerConfirmPriorLocked(key string, equalPrior *ResolvedEntry) *ResolvedEntry {
	if equalPrior != nil && c.raLayerResidentEntryLocked(key) == equalPrior {
		return equalPrior
	}
	return nil
}

// raLayerCommitLocked runs right after putCoreLocked stored entry under key. samePrior
// is non-nil when raLayerEqualPrior found the bytes equal AND that same prior was still
// resident when the store lock was re-taken (raLayerConfirmPriorLocked). provenanceKnown is true for the
// ctx-carrying Puts (PutIfGen / ReplaceIfGen / ReplaceIfGenReMint / PutThenRemark): their ctx sink holds
// exactly what the stored body sliced. Returns the keys to remark; the caller enqueues
// them after releasing c.mu (remarkLayerConsumers). Callers hold c.mu.
func (c *ResolvedCacheStore) raLayerCommitLocked(ctx context.Context, provenanceKnown bool, key string, samePrior, entry *ResolvedEntry) []string {
	if isRAFullListEntry(entry) {
		if samePrior != nil {
			// Same bytes as the prior it replaced: no consumer went stale. The prior was
			// read and confirmed resident under c.mu, so its version is stable here.
			entry.contentVersion = samePrior.contentVersion
			return nil
		}
		entry.contentVersion = raContentVersionSeq.Add(1)
		ix := &c.layers
		ix.mu.Lock()
		defer ix.mu.Unlock()
		var remark []string
		for w, v := range ix.byRA[key] {
			if v != entry.contentVersion {
				if _, resident := c.index[w]; resident {
					remark = append(remark, w)
				}
			}
		}
		return remark
	}
	var srcs map[string]uint64
	if provenanceKnown {
		srcs, provenanceKnown = sinkSources(ctx)
	}
	ix := &c.layers
	ix.mu.Lock()
	defer ix.mu.Unlock()
	// The new body replaces whatever the cell held, so its recorded sources go either
	// way. A Put of unknown provenance (a ctx-less Put, e.g. the external-TTL carrier)
	// records none: it stays out of the layered remark path.
	ix.dropWidgetLocked(key)
	if !provenanceKnown {
		return nil
	}
	if len(srcs) == 0 {
		return nil // the body was not sliced from a cached raKey
	}
	_, resident := c.index[key]
	stale := false
	for raKey, v := range srcs {
		cur := c.raLayerResidentEntryLocked(raKey)
		if cur == nil || cur.contentVersion != v {
			stale = true // raKey evicted, or committed with other content, since the slice
		}
		if !resident {
			continue
		}
		if ix.byRA == nil {
			ix.byRA = map[string]map[string]uint64{}
			ix.byWidget = map[string]map[string]uint64{}
		}
		if ix.byRA[raKey] == nil {
			ix.byRA[raKey] = map[string]uint64{}
		}
		ix.byRA[raKey][key] = v
		if ix.byWidget[key] == nil {
			ix.byWidget[key] = map[string]uint64{}
		}
		ix.byWidget[key][raKey] = v
	}
	if stale && resident {
		return []string{key}
	}
	return nil
}

// raLayerForgetLocked runs when key leaves the store (both removal funnels). Callers
// hold c.mu, after the index delete. A removed raKey needs nothing here: its widgets
// keep their recorded version, which the next commit (a new version) differs from.
func (c *ResolvedCacheStore) raLayerForgetLocked(key string) {
	ix := &c.layers
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if len(ix.byWidget) != 0 {
		ix.dropWidgetLocked(key)
	}
}

// NoteRAFullListSlice notes, on ctx's dep-gen sink (and its ancestors, whose bodies
// embed this one), that the resolve served a Go-slice of the raKey entry `entry` (the
// 4a fast-path hit). The note travels with this resolve to its Put. Called by apiref
// raFullListServe only.
func (c *ResolvedCacheStore) NoteRAFullListSlice(ctx context.Context, raKey string, entry *ResolvedEntry) {
	if c == nil || raKey == "" || entry == nil {
		return
	}
	ReplayRAFullListSlices(ctx, LayeredSources{{raKey: raKey, version: entry.contentVersion}})
}

// ReplayRAFullListSlices appends srcs to ctx's sink: a SeedResolveMemo hit serves the
// body another widget sliced, so its Put holds the same raKey versions.
func ReplayRAFullListSlices(ctx context.Context, srcs LayeredSources) {
	if len(srcs) == 0 {
		return
	}
	for s := depGenSinkFromContext(ctx); s != nil; s = s.parent {
		s.mu.Lock()
		s.layered = append(s.layered, srcs...)
		s.mu.Unlock()
	}
}

// LayeredSourcesMark returns a position in ctx's sink's note list, to pass to
// LayeredSourcesSince after the producing resolve (the memo capture window).
func LayeredSourcesMark(ctx context.Context) int {
	s := depGenSinkFromContext(ctx)
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.layered)
}

// LayeredSourcesSince returns the raKey versions noted on ctx since mark.
func LayeredSourcesSince(ctx context.Context, mark int) LayeredSources {
	s := depGenSinkFromContext(ctx)
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if mark < 0 || mark >= len(s.layered) {
		return nil
	}
	return append(LayeredSources(nil), s.layered[mark:]...)
}

// ForgetRAFullListConsumer drops widgetKey's recorded source raKey. apiref calls it
// before the repopulate / first-sight branches, whose body is built from a FRESH
// resolve, so the raKey Put that follows does not remark the widget producing it.
func (c *ResolvedCacheStore) ForgetRAFullListConsumer(raKey, widgetKey string) {
	if c == nil || raKey == "" || widgetKey == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.layers.mu.Lock()
	defer c.layers.mu.Unlock()
	c.layers.dropEdgeLocked(raKey, widgetKey)
}

// remarkLayerConsumers enqueues the keys raLayerCommitLocked returned. Off c.mu.
func remarkLayerConsumers(keys []string) {
	for _, w := range keys {
		observeDepGenRemark(w, "layered")
		Deps().enqueueRemark(w, nil)
	}
}

// RAFullListLayerStatsForTest reports (raKeys with a recorded widget, widget->raKey edges).
func (c *ResolvedCacheStore) RAFullListLayerStatsForTest() (records, consumers int) {
	c.layers.mu.Lock()
	defer c.layers.mu.Unlock()
	for _, ws := range c.layers.byRA {
		consumers += len(ws)
	}
	return len(c.layers.byRA), consumers
}
