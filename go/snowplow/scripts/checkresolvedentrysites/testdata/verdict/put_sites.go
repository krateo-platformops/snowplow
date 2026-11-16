// Package verdict is the site-class 4 rule-A RED-arm fixture (#912 re-review).
// The store's ungated writes are unexported, so what is left to catch outside
// the cache package is a write reached around the verdict through ANY receiver
// with an ungated write's name: a direct call on an ad-hoc interface, a method
// value of a wrapper, an entry built without a literal. The verdict-gated write
// is accepted.
package verdict

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func gated(ctx context.Context, c *cache.ResolvedCacheStore, v cache.PutVerdict) {
	c.PutResolvedIfGen(ctx, v, "k", &cache.ResolvedEntry{}, 0)
}

type ifGenWriter interface {
	PutIfGen(ctx context.Context, key string, entry *cache.ResolvedEntry, capturedGen uint64) bool
}

func direct(ctx context.Context, w ifGenWriter) {
	w.PutIfGen(ctx, "k", &cache.ResolvedEntry{}, 0)
}

type wrapper struct{ v cache.PutVerdict }

func (w wrapper) Put(key string, entry *cache.ResolvedEntry) {}

func methodValue(w wrapper) {
	put := w.Put
	put("k", new(cache.ResolvedEntry))
}

func viaInterface(w interface {
	PutThenRemark(ctx context.Context, key string, entry *cache.ResolvedEntry)
}, e *cache.ResolvedEntry) {
	copied := *e
	w.PutThenRemark(context.Background(), "k", &copied)
}
