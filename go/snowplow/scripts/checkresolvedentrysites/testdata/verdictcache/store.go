// Package verdictcache is the site-class 4 rule-B RED-arm fixture (#912
// delta-3 item 3): a miniature of the cache package's store. Inside the package
// that declares ResolvedCacheStore, an exported helper must not re-open the
// escape the compiler closed: every call to an ungated primitive sits in a
// function that takes a PutVerdict and returns on !v.Store() first, or in
// PutContentIfGen; no method carries an exported ungated name.
package verdictcache

import "context"

type ResolvedEntry struct{ RawJSON []byte }

type PutVerdict struct{ store bool }

func (v PutVerdict) Store() bool { return v.store }

type ResolvedCacheStore struct{ m map[string]*ResolvedEntry }

// The primitives (unexported, ungated): they may call one another.
func (c *ResolvedCacheStore) put(key string, e *ResolvedEntry) { c.m[key] = e }

func (c *ResolvedCacheStore) putIfGen(ctx context.Context, key string, e *ResolvedEntry, gen uint64) bool {
	c.put(key, e)
	return true
}

// Accepted: takes the verdict, returns on !v.Store() first.
func (c *ResolvedCacheStore) PutResolvedIfGen(ctx context.Context, v PutVerdict, key string, e *ResolvedEntry, gen uint64) bool {
	if c == nil || !v.Store() {
		return false
	}
	return c.putIfGen(ctx, key, e, gen)
}

// Accepted: the named content-cell write.
func (c *ResolvedCacheStore) PutContentIfGen(ctx context.Context, key string, e *ResolvedEntry, gen uint64) bool {
	return c.putIfGen(ctx, key, e, gen)
}

// FLAGGED: an exported helper that writes without a verdict.
func (c *ResolvedCacheStore) SeedForSomething(key string, raw []byte) {
	c.put(key, &ResolvedEntry{RawJSON: raw})
}

// FLAGGED: takes the verdict but does not return on it first.
func WriteWithVerdict(c *ResolvedCacheStore, v PutVerdict, key string, e *ResolvedEntry) {
	_ = v.Store()
	c.put(key, e)
}

// FLAGGED: an ungated write under an exported name.
func (c *ResolvedCacheStore) Put(key string, e *ResolvedEntry) { c.put(key, e) }
