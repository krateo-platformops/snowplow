// Package servecell is site-class 3's discrimination fixture: the checker must
// flag the unrouted read (line 19), the method value (line 23) and the
// empty-reason waiver (line 29), and must NOT flag the routed read or the
// reasoned probe. Line numbers are asserted by main_test.go.
package servecell

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func routed(ctx context.Context, c *cache.ResolvedCacheStore) {
	_, _ = cache.ServeCell(ctx, c, "k", true)
}

func unrouted(c *cache.ResolvedCacheStore) {
	// a body read that bypasses ServeCell
	_, _ = c.Get("k")
}

func methodValue(c *cache.ResolvedCacheStore) {
	read := c.GetNoTouch
	_, _ = read("k")
}

func emptyReason(c *cache.ResolvedCacheStore) {
	//serve-probe:
	_, _ = c.GetNoTouch("k")
}

func probe(c *cache.ResolvedCacheStore) bool {
	//serve-probe: warmth probe in the fixture, serves no body
	_, hit := c.GetNoTouch("k")
	return hit
}
