package dispatchers

import (
	"context"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// fixtureStoreVerdict is the Put-gate verdict of a resolve that recorded nothing
// on its three sinks: it allows the Put. Fixtures, and arms that exercise a
// verdict-gated write's OTHER guards, write through the gated store methods
// with it — the ungated ones are unexported, so the compiler keeps every write
// behind a verdict (#912 delta-3 item 3).
var fixtureStoreVerdict = func() cache.PutVerdict {
	_, g := cache.WithChildPutGateSinks(context.Background())
	return cache.PutGateVerdict(g, "restactions")
}()

// putFixture writes a TEST FIXTURE through the store behind h. The cacheHandle
// interface carries only verdict-gated writes, and so does every store behind
// it: the fixture takes the plain gated insert with fixtureStoreVerdict.
func putFixture(h cacheHandle, key string, e *cache.ResolvedEntry) {
	h.(interface {
		PutResolved(v cache.PutVerdict, key string, entry *cache.ResolvedEntry) bool
	}).PutResolved(fixtureStoreVerdict, key, e)
}
