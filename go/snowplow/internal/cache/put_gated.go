// put_gated.go — the ONLY exported writes of a ResolvedEntry (#912 delta-3
// item 3). The COMPILER enforces the Put-gate rule: the ungated store methods
// (put, putIfGen, putThenRemark, putEndingKnownStale, replaceIfGen,
// replaceIfGenRefresh) are unexported, so no code outside this package can
// store a cell without going through one of these.
//
//   - The verdict-gated writes of a RESOLVE OUTPUT (a RESTAction or widget body,
//     the widget-content envelope, the raFullList full list — PutRAFullList and
//     PutRAFullListIfGen in ra_full_list_store.go). Each takes the Put-gate
//     verdict (PutGateVerdict) and stores nothing unless it allows the Put. A
//     site that mishandles a decline reason, or forgets to check, cannot store a
//     declined resolve: the write itself refuses.
//   - PutContentIfGen, the write of a GET-only CONTENT cell (apistage,
//     cluster_list, the incremental list cells): the raw apiserver envelope of
//     one GET or LIST, which no RESTAction stage, write or external endpoint can
//     reach. It takes no verdict, and it can write nothing but such a cell.
//
// Test fixtures write through the gated methods with a verdict of a resolve that
// recorded nothing.

package cache

import "context"

// PutResolved is put (the plain insert-or-replace, no generation guard), gated
// by v. It reports whether it wrote.
func (c *ResolvedCacheStore) PutResolved(v PutVerdict, key string, entry *ResolvedEntry) bool {
	if !v.Store() {
		return false
	}
	c.put(key, entry)
	return true
}

// PutResolvedIfGen is putIfGen, gated by v.
func (c *ResolvedCacheStore) PutResolvedIfGen(ctx context.Context, v PutVerdict, key string, entry *ResolvedEntry, capturedGen uint64) bool {
	if !v.Store() {
		return false
	}
	return c.putIfGen(ctx, key, entry, capturedGen)
}

// PutResolvedThenRemark is putThenRemark, gated by v. It reports whether it
// wrote.
func (c *ResolvedCacheStore) PutResolvedThenRemark(ctx context.Context, v PutVerdict, key string, entry *ResolvedEntry) bool {
	if !v.Store() {
		return false
	}
	c.putThenRemark(ctx, key, entry)
	return true
}

// PutResolvedEndingKnownStale is putEndingKnownStale, gated by v. It reports
// whether it wrote.
func (c *ResolvedCacheStore) PutResolvedEndingKnownStale(ctx context.Context, v PutVerdict, key string, entry *ResolvedEntry) bool {
	if !v.Store() {
		return false
	}
	c.putEndingKnownStale(ctx, key, entry)
	return true
}

// ReplaceResolvedIfGen is replaceIfGen (replace a live entry, never insert),
// gated by v.
func (c *ResolvedCacheStore) ReplaceResolvedIfGen(ctx context.Context, v PutVerdict, key string, entry *ResolvedEntry, capturedGen uint64) bool {
	if !v.Store() {
		return false
	}
	return c.replaceIfGen(ctx, key, entry, capturedGen)
}

// ReplaceResolvedIfGenRefresh is replaceIfGenRefresh, gated by v.
func (c *ResolvedCacheStore) ReplaceResolvedIfGenRefresh(ctx context.Context, v PutVerdict, key string, entry *ResolvedEntry, capturedGen uint64) bool {
	if !v.Store() {
		return false
	}
	return c.replaceIfGenRefresh(ctx, key, entry, capturedGen)
}

// PutContentIfGen is putIfGen for a GET-only CONTENT cell. It refuses (stores
// nothing, returns false) any entry that is not one: its Inputs must carry the
// apistage class, and key must be the key those Inputs compute. So a resolve
// output — whose Inputs carry its own class (restactions, widgets,
// widgetContent, raFullList) and whose key folds its identity — can never be
// written through it.
func (c *ResolvedCacheStore) PutContentIfGen(ctx context.Context, key string, entry *ResolvedEntry, capturedGen uint64) bool {
	if entry == nil || entry.Inputs == nil || !isContentCellKey(key, *entry.Inputs) {
		return false
	}
	// #443 (a) — an inert (dry-run) resolve persists nothing (the resolve path
	// reaches this method directly, so it says so itself).
	if Inert(ctx) {
		return false
	}
	return c.putIfGen(ctx, key, entry, capturedGen)
}

// isContentCellKey reports whether key is the content-cell key of in: the
// apistage class, and the key its inputs compute. It re-hashes the entry's
// CARRIED inputs, which were minted (identity-free, by contentKeyInputsQ) where
// the key was.
func isContentCellKey(key string, in ResolvedKeyInputs) bool {
	return in.CacheEntryClass == CacheEntryClassApistage && ComputeKey(in) == key
}
