package dispatchers

import "github.com/krateo-platformops/snowplow/internal/cache"

// putFixture writes a TEST FIXTURE through the store behind h, ungated. The
// cacheHandle interface carries only verdict-gated writes (#912 re-review: a
// dispatcher stores a resolve output only through its Put-gate verdict), so a
// fixture reaches the store's own Put, which every implementation behind the
// handle has.
// fixtureStoreVerdict is a storing Put-gate verdict (no sink recorded
// anything) for arms that exercise a verdict-gated write's OTHER guards.
var fixtureStoreVerdict = cache.PutGateVerdict(cache.PutGateSinks{}, "restactions")

func putFixture(h cacheHandle, key string, e *cache.ResolvedEntry) {
	h.(interface {
		Put(key string, entry *cache.ResolvedEntry)
	}).Put(key, e)
}
