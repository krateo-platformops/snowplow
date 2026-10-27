// put_gated.go — the verdict-gated writes of a RESOLVE OUTPUT (#912
// re-review): a RESTAction or widget body, the widget-content envelope, the
// raFullList full list.
//
// Each takes the Put-gate verdict (PutGateVerdict) and stores nothing unless it
// allows the Put. A site that mishandles a decline reason, or forgets to check,
// cannot store a declined resolve: the write itself refuses. The resolve-output
// packages (internal/handlers/dispatchers, internal/resolvers/widgets/apiref)
// write through these methods only; scripts/check_resolved_entry_put_sites.sh
// fails a call there to an ungated write (Put, PutIfGen, PutThenRemark,
// PutEndingKnownStale, ReplaceIfGen, ReplaceIfGenRefresh). The ungated methods
// remain for the GET-only content cells (apistage, cluster_list, list cells),
// which no RESTAction stage, write or external endpoint can reach, and for test
// fixtures.

package cache

import "context"

// PutResolvedIfGen is PutIfGen, gated by v.
func (c *ResolvedCacheStore) PutResolvedIfGen(ctx context.Context, v PutVerdict, key string, entry *ResolvedEntry, capturedGen uint64) bool {
	if !v.Store() {
		return false
	}
	return c.PutIfGen(ctx, key, entry, capturedGen)
}

// PutResolvedThenRemark is PutThenRemark, gated by v. It reports whether it
// wrote.
func (c *ResolvedCacheStore) PutResolvedThenRemark(ctx context.Context, v PutVerdict, key string, entry *ResolvedEntry) bool {
	if !v.Store() {
		return false
	}
	c.PutThenRemark(ctx, key, entry)
	return true
}

// PutResolvedEndingKnownStale is PutEndingKnownStale, gated by v. It reports
// whether it wrote.
func (c *ResolvedCacheStore) PutResolvedEndingKnownStale(ctx context.Context, v PutVerdict, key string, entry *ResolvedEntry) bool {
	if !v.Store() {
		return false
	}
	c.PutEndingKnownStale(ctx, key, entry)
	return true
}

// ReplaceResolvedIfGenRefresh is ReplaceIfGenRefresh, gated by v.
func (c *ResolvedCacheStore) ReplaceResolvedIfGenRefresh(ctx context.Context, v PutVerdict, key string, entry *ResolvedEntry, capturedGen uint64) bool {
	if !v.Store() {
		return false
	}
	return c.ReplaceIfGenRefresh(ctx, key, entry, capturedGen)
}
