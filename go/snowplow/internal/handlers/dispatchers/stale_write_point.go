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
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
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
		// Nothing has been written yet (the header and body follow this point);
		// the one header the first pass may have set describes the discarded
		// pass, so it goes — the re-run sets its own.
		wri.Header().Del(util.HeaderStageOutcomes)
		if f := staleRerunHookForTest; f != nil {
			f(wri)
		}
		h.ServeHTTP(wri, req.WithContext(cache.WithStaleServeDisabled(req.Context())))
		return true
	}
	api.CommitStaleServe(sink)
	return false
}

// isStaleRerun reports whether req is a withdrawn stale serve's re-run. The
// dispatchers treat it like a demand revalidation (no L1 read, no second
// customer-in-flight mark or live-caller observation, its own per-call
// l1_hit), so the one customer request is counted once.
func isStaleRerun(req *http.Request) bool {
	return cache.StaleServeDisabled(req.Context())
}

// staleHitWritePoint keeps the invariant on a HIT of a stale-derived cell (a
// co-bound request before the replace lands): the hit is armed, so it only
// needs the key's owed-push mark.
func staleHitWritePoint(key string, entry *cache.ResolvedEntry) {
	if entry != nil && entry.StaleDerived {
		cache.MarkStaleOwed(key, time.Now())
	}
}

// staleRerunHookForTest observes the ResponseWriter at the moment of the
// re-run (D2-15d: nothing may have been written to it yet). Nil in production.
var staleRerunHookForTest func(wri http.ResponseWriter)

// stalePutHookForTest observes every customer-dispatcher L1 Put attempt
// (#710 F1): its site and whether the body is stale-derived. The invariant
// the D2 arms sweep: a stale-derived body is only ever offered to the
// armable PutIfGen ("putifgen"). Nil in production.
var stalePutHookForTest func(key, site string, staleDerived bool)

func noteCustomerPut(key, site string, staleDerived bool) {
	if f := stalePutHookForTest; f != nil {
		f(key, site, staleDerived)
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
