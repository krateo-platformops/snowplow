// stale_write_point.go — #710 PR-2b: the D2 write-point guarantee.
//
// Diego's rule: "Serving stale data is correct only if the updated value will
// be served later in few seconds". Whether a response can be corrected — its
// cell stored and its refresh key armed — is not knowable when the content
// cell is read (later stages, UAF, external and PutIfGen decide it), but it is
// EXACTLY known at the write point: both dispatchers run the whole Put-gate
// chain before the header and the body, and refreshArmable is true only on an
// accepted PutIfGen. So (architect ruling, #710 issuecomment-6195582848):
//
//   - armable: COMMIT — the owed-push mark is set and the replace is scheduled
//     (now, if the content debt already cleared); the stale body is written.
//   - not armable: WITHDRAW — the stale body is NEVER written; the request is
//     re-run once with D2 disabled (content reads read through, #547, never
//     queued) and that fresh body is served, armed or not.
//
// Invariant: a stale-derived body is written only behind an accepted Put, the
// armed refresh header and an owed-push mark (D2-17 sweeps it).
package dispatchers

import (
	"net/http"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// staleWritePoint applies the write-point rule for a cold response. It
// returns true when it re-ran the request (the caller writes nothing more).
func staleWritePoint(h http.Handler, wri http.ResponseWriter, req *http.Request,
	sink *cache.StaleServeSink, armable bool) (rerun bool) {
	if !sink.StaleDerived() {
		return false
	}
	if !armable {
		api.WithdrawStaleServe(sink)
		h.ServeHTTP(wri, req.WithContext(cache.WithStaleServeDisabled(req.Context())))
		return true
	}
	api.CommitStaleServe(sink)
	return false
}

// staleHitWritePoint keeps the invariant on a HIT of a stale-derived cell (a
// co-bound request before the replace lands): the hit is armed, so it only
// needs the key's owed-push mark.
func staleHitWritePoint(key string, entry *cache.ResolvedEntry) {
	if entry != nil && entry.StaleDerived {
		cache.MarkStaleOwed(key)
	}
}

// staleWriteHookForTest observes every customer body write that could be
// stale-derived (D2-17's invariant sweep). Nil in production.
var staleWriteHookForTest func(key string, staleDerived, armed bool)

func noteStaleWrite(key string, staleDerived, armed bool) {
	if f := staleWriteHookForTest; f != nil {
		f(key, staleDerived, armed)
	}
}
