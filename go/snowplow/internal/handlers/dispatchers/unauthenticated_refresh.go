// unauthenticated_refresh.go — #1334: the refresher's refusal to refresh an
// unauthenticated identity's cell under snowplow's own ServiceAccount
// (resolve_populate.go). Counts only, never identity.

package dispatchers

import (
	"expvar"
)

// unauthenticatedRefreshRefused counts the refreshes resolveAndPopulateL1
// refused because the cell's identity is not authenticated (cache.Authenticated)
// and could not be impersonated, by whether the cell was evicted.
// snowplow_unauthenticated_refresh_refused{evicted|absent} on /debug/vars.
var unauthenticatedRefreshRefused = expvar.NewMap("snowplow_unauthenticated_refresh_refused")

func noteUnauthenticatedRefreshRefused(evicted bool) {
	if evicted {
		unauthenticatedRefreshRefused.Add("evicted", 1)
		return
	}
	unauthenticatedRefreshRefused.Add("absent", 1)
}

// UnauthenticatedRefreshRefused reads one snowplow_unauthenticated_refresh_refused
// counter ("evicted" or "absent").
func UnauthenticatedRefreshRefused(outcome string) int64 {
	if v, ok := unauthenticatedRefreshRefused.Get(outcome).(*expvar.Int); ok {
		return v.Value()
	}
	return 0
}
