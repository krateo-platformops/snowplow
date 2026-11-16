// Package dispatchers is the site-class 5 RED-arm fixture (#911 re-review). It
// is parsed, never compiled. The checker must FLAG the two offending dispatcher
// Put sites and accept the two well-formed ones.
package dispatchers

import "github.com/krateo-platformops/snowplow/internal/cache"

func keptSite(handle *cache.ResolvedCacheStore, key, outcomes string) {
	// WELL-FORMED: keeps the stage outcomes.
	handle.Put(key, &cache.ResolvedEntry{StageOutcomes: outcomes})
}

func waivedSite(handle *cache.ResolvedCacheStore, key string) {
	// WELL-FORMED: a class that never answers a RESTAction /call.
	// stage-outcomes-waiver: fixture — a widgets cell.
	handle.Put(key, &cache.ResolvedEntry{})
}

func droppedSite(handle *cache.ResolvedCacheStore, key string) {
	// OFFENDING: a restactions Put that drops the outcomes.
	handle.Put(key, &cache.ResolvedEntry{})
}

func emptyReasonSite(handle *cache.ResolvedCacheStore, key string) {
	// OFFENDING: a waiver with no reason.
	// stage-outcomes-waiver:
	handle.Put(key, &cache.ResolvedEntry{})
}
