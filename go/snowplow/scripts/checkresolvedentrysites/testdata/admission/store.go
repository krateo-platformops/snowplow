// Package admission is the site-class 6 RED-arm fixture (#1233): a miniature of
// the cache package's store. Every function that calls putCoreLocked must return
// on !admitWrite(...) first, and nothing but putCoreLocked inserts into the index
// or pushes onto the LRU order.
package admission

import (
	"container/list"
	"context"
)

type ResolvedEntry struct{ RawJSON []byte }

type ResolvedCacheStore struct {
	index map[string]*list.Element
	order *list.List
}

func admitWrite(ctx context.Context, e *ResolvedEntry) bool { return ctx != nil && e != nil }

// The one insertion: may touch the index and the order.
func (c *ResolvedCacheStore) putCoreLocked(key string, e *ResolvedEntry, freshMint bool) {
	_ = freshMint
	el := c.order.PushFront(e)
	c.index[key] = el
}

// Accepted: returns on !admitWrite first.
func (c *ResolvedCacheStore) putIfGen(ctx context.Context, key string, e *ResolvedEntry) bool {
	if c == nil || !admitWrite(ctx, e) {
		return false
	}
	c.putCoreLocked(key, e, false)
	return true
}

// FLAGGED: calls putCoreLocked without admitting.
func (c *ResolvedCacheStore) putUnadmitted(key string, e *ResolvedEntry) {
	c.putCoreLocked(key, e, false)
}

// FLAGGED: admits only after the insertion.
func (c *ResolvedCacheStore) putAdmittedLate(ctx context.Context, key string, e *ResolvedEntry) {
	c.putCoreLocked(key, e, false)
	if !admitWrite(ctx, e) {
		return
	}
}

// FLAGGED twice: inserts by hand, outside putCoreLocked.
func (c *ResolvedCacheStore) insertByHand(key string, e *ResolvedEntry) {
	el := c.order.PushFront(e)
	c.index[key] = el
}
