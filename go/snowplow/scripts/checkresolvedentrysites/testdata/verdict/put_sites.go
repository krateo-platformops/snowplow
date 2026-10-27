// Package verdict is the site-class 4 RED-arm fixture (#912 re-review): every
// ResolvedEntry Put site reads the shared Put-gate verdict. It is parsed, never
// compiled. The checker (-verdict-only) must FLAG the two offending sites and
// accept the two well-formed ones.
package verdict

import "github.com/krateo-platformops/snowplow/internal/cache"

func gatedSite(handle *cache.ResolvedCacheStore, key string, g cache.PutGateSinks) {
	// WELL-FORMED: the function decides through the verdict.
	if !cache.PutGateVerdict(g, "restactions").Store() {
		return
	}
	handle.Put(key, &cache.ResolvedEntry{})
}

func namedSite(handle *cache.ResolvedCacheStore, key string) {
	// WELL-FORMED: names where its verdict is taken.
	// put-verdict: taken by the caller, fixture.
	handle.Put(key, &cache.ResolvedEntry{})
}

func bypassSite(handle *cache.ResolvedCacheStore, key string) {
	// OFFENDING: a Put that never reads the verdict (the #915 / #912 N1 shape).
	handle.Put(key, &cache.ResolvedEntry{})
}

func emptyReasonSite(handle *cache.ResolvedCacheStore, key string) {
	// OFFENDING: an annotation with no reason.
	// put-verdict:
	handle.Put(key, &cache.ResolvedEntry{})
}
