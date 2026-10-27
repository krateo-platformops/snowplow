// Package verdict is the site-class 4 RED-arm fixture (#912 re-review). The
// typed checker (-verdict-only) must FLAG every ungated write of a
// ResolvedEntry — a direct call, a method value, an ad-hoc interface over the
// store, an entry built without a literal — and accept the verdict-gated ones.
package verdict

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func gated(ctx context.Context, c *cache.ResolvedCacheStore, v cache.PutVerdict) {
	c.PutResolvedIfGen(ctx, v, "k", &cache.ResolvedEntry{}, 0)
}

func direct(ctx context.Context, c *cache.ResolvedCacheStore) {
	c.PutIfGen(ctx, "k", &cache.ResolvedEntry{}, 0)
}

func methodValue(c *cache.ResolvedCacheStore) {
	put := c.Put
	put("k", new(cache.ResolvedEntry))
}

func viaInterface(c *cache.ResolvedCacheStore, e *cache.ResolvedEntry) {
	var w interface {
		PutThenRemark(ctx context.Context, key string, entry *cache.ResolvedEntry)
	} = c
	copied := *e
	w.PutThenRemark(context.Background(), "k", &copied)
}
