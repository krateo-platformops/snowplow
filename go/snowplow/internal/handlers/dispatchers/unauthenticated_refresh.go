// unauthenticated_refresh.go — #1334: the refresher's refusal to refresh an
// unauthenticated identity's cell under snowplow's own ServiceAccount
// (resolve_populate.go). Counts only, never identity.

package dispatchers

import (
	"expvar"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// unauthenticatedRefreshEvicted / unauthenticatedRefreshAbsent count the
// refreshes resolveAndPopulateL1 refused because the cell's identity is not
// authenticated (cache.Authenticated) and could not be impersonated, by whether
// a cell was evicted.
var unauthenticatedRefreshEvicted, unauthenticatedRefreshAbsent atomic.Int64

// The refresher is a cache surface: published only with the cache on (CFG-1).
func init() {
	if cache.Disabled() {
		return
	}
	expvar.Publish("snowplow_unauthenticated_refresh_refused", expvar.Func(func() any {
		return map[string]int64{
			"evicted": unauthenticatedRefreshEvicted.Load(),
			"absent":  unauthenticatedRefreshAbsent.Load(),
		}
	}))
}

func noteUnauthenticatedRefreshRefused(evicted bool) {
	if evicted {
		unauthenticatedRefreshEvicted.Add(1)
		return
	}
	unauthenticatedRefreshAbsent.Add(1)
}

// UnauthenticatedRefreshRefused reads one snowplow_unauthenticated_refresh_refused
// counter ("evicted" or "absent").
func UnauthenticatedRefreshRefused(outcome string) int64 {
	switch outcome {
	case "evicted":
		return unauthenticatedRefreshEvicted.Load()
	case "absent":
		return unauthenticatedRefreshAbsent.Load()
	}
	return 0
}
