package dispatchers

// customer_serves.go — #1265 P0: what each top-level customer /call was served,
// counted where snowplow answers it.
//
// THE DEFINITION (Diego, 2026-11-08). A navigation is cold when at least one
// widget had nothing cached to show, fresh or stale, so the user waited on live
// computation. Stale-then-replace (#710) is warm. A widget-cell miss served
// from a stale apistage input (D2) is cold, cause no_cell: the user still
// waited on the refilter and the jq (ruling Q6).
//
// WHAT IS COUNTED. Exactly one outcome per top-level customer /call to the
// restactions or widgets handler, decided at the serve decision:
//
//   - fresh   — the body is a resident cell (the per-identity L1 cell, or the
//     identity-free widgetContent shell) with no stale mark;
//   - stale   — the body is a resident cell whose inputs moved since its Put
//     (an open invalidation window or KNOWN-STALE, cache.InputsMovedSincePut)
//     or that was stored from a stale input (StaleDerived): the #1062 stale
//     ledger's definition, read with the same predicate;
//   - cold    — the body comes from a live resolve run for this request, with
//     the cause in snowplow_customer_serves_cold_total;
//   - refused — the request was refused before anything was served or read: a
//     bad request (an inline object at the widgets handler, unparseable
//     extras), a CR that could not be read, an RBAC denial, or the #1116
//     reserved-extras 400 the resolver answers before it reads anything.
//
// A live resolve that then fails is still cold: the user waited on it.
//
// WHAT IS NOT COUNTED (isTopLevelCustomerCall). Only a request the user is
// waiting on, at the top of its call tree:
//   - a refresher refill, a seed, a prewarm walk and every other background
//     driver resolve in process and never enter ServeHTTP; their HTTP
//     self-loopbacks carry the background marker and their work class
//     (cache.NonCustomerWriteOrigin, workclass);
//   - a #547 demand revalidation (work class fill_bg) and a #710 withdrawn
//     stale serve's re-run (the same request's second pass: its first pass
//     already decided);
//   - an export's in-process re-dispatch (cache.ExportRead): the /export
//     request is the customer's call, its pages are not navigations;
//   - a customer resolve's own nested self-loopback (a trusted hop carrying
//     X-Snowplow-Nested-Depth): part of its caller's live computation.
//
// So the count is customer traffic by construction, not by subtraction: the
// store's hit/miss counters (contaminated by internal reads, #376) and its
// stale_served counter (#552) are not inputs here.
//
// THE HEADER. Every counted response that reached the serve decision carries
// X-Snowplow-Serve-Outcome: fresh | stale | cold, set at the decision and so
// before the first byte, and the request's server span carries
// snowplow.serve.outcome (plus snowplow.serve.cold_cause on a cold serve). A
// refused response carries neither (there was no serve to describe), and
// neither does a response that is not counted, so the header and the counter
// describe the same population. Neither carries identity, a key, a resource
// name or a body. Unlike X-Snowplow-Refresh-Key it is never suppressed on a
// counted serve: the refresh key is withheld where no subscription can fire,
// but every serve has an outcome. The SPA ignores the header; the bench reads
// it per navigation (ruling Q8).
//
// LABELS are closed sets: handler {restactions, widgets}, outcome (above),
// class_warm {none} and cause (coldCauses). class_warm reports the requester's
// sign-in warm state; until that registry exists (#1265 P3) every serve is
// "none", and the other values are not emitted, so no series can read as a
// passing zero for a state nothing measures yet.

import (
	"context"
	"expvar"
	"net/http"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/workclass"
)

// ServeOutcomeHeader carries a counted customer response's serve outcome
// (fresh | stale | cold). Identity-free. It must be in the CORS ExposedHeaders
// (main.go), like X-Snowplow-Refresh-Key.
const ServeOutcomeHeader = "X-Snowplow-Serve-Outcome"

// Span attribute names (#1265). Distinct from #1264's snowplow.stages.*.
const (
	spanAttrServeOutcome   = "snowplow.serve.outcome"
	spanAttrServeColdCause = "snowplow.serve.cold_cause"
)

type serveHandler uint8

const (
	serveHandlerRestactions serveHandler = iota
	serveHandlerWidgets
	numServeHandlers
)

var serveHandlerNames = [numServeHandlers]string{
	serveHandlerRestactions: "restactions",
	serveHandlerWidgets:     "widgets",
}

type serveOutcome uint8

const (
	serveOutcomeRefused serveOutcome = iota // the zero value: no decision was made
	serveOutcomeFresh
	serveOutcomeStale
	serveOutcomeCold
	numServeOutcomes
)

var serveOutcomeNames = [numServeOutcomes]string{
	serveOutcomeFresh:   "fresh",
	serveOutcomeStale:   "stale",
	serveOutcomeCold:    "cold",
	serveOutcomeRefused: "refused",
}

// The cold causes. Each names why there was nothing cached to show:
const (
	// coldCauseNoCell — the lookup found no cell under the request's key: never
	// stored, declined at Put, evicted (LRU, DELETE) or expired and already
	// removed. A D2 stale-input resolve is this cause too (ruling Q6).
	coldCauseNoCell = "no_cell"
	// coldCauseExpiredTTL — this lookup found the cell past its TTL.
	coldCauseExpiredTTL = "expired_ttl"
	// coldCauseEvictedMaxAge — this lookup found the cell past the max entry
	// age (#378).
	coldCauseEvictedMaxAge = "evicted_maxage"
	// coldCauseIneligibleKey — no servable key: the key's first-match
	// BindingUID is "" (#95, never served), or the request carries no identity.
	coldCauseIneligibleKey = "ineligible_key"
	// coldCauseDryRun — an inert (dry-run) request: it never reads a cell (#443).
	coldCauseDryRun = "dry_run"
	// coldCauseCacheOff — there is no resolved-output store (cache or L1 off).
	coldCauseCacheOff = "cache_off"
)

// coldCauses is the closed cause set, in emit order.
var coldCauses = []string{
	coldCauseNoCell, coldCauseExpiredTTL, coldCauseEvictedMaxAge,
	coldCauseIneligibleKey, coldCauseDryRun, coldCauseCacheOff,
}

// classWarmNone is the only class_warm value until #1265 P3 (see the file
// comment).
const classWarmNone = "none"

var (
	// customerServes[h][o] — outcomes, class_warm=none.
	customerServes [numServeHandlers][numServeOutcomes]atomic.Uint64
	// customerColdServes[h][i] — cold serves by coldCauses[i].
	customerColdServes [numServeHandlers][]atomic.Uint64
)

func init() {
	for h := range customerColdServes {
		customerColdServes[h] = make([]atomic.Uint64, len(coldCauses))
	}
	// Published whatever the cache mode: with the cache off every serve is
	// cold{cache_off}, which is the true answer (a control arm reads it). Not a
	// cache surface — listed in the CFG-1 probe's nonCacheInitPublishers.
	expvar.Publish("snowplow_customer_serves_total", expvar.Func(func() any {
		out := map[string]map[string]map[string]uint64{}
		for _, c := range CustomerServesSnapshot() {
			if out[c.Handler] == nil {
				out[c.Handler] = map[string]map[string]uint64{}
			}
			out[c.Handler][c.Outcome] = map[string]uint64{c.ClassWarm: c.Count}
		}
		return out
	}))
	expvar.Publish("snowplow_customer_serves_cold_total", expvar.Func(func() any {
		out := map[string]map[string]uint64{}
		for _, c := range CustomerColdServesSnapshot() {
			if out[c.Handler] == nil {
				out[c.Handler] = map[string]uint64{}
			}
			out[c.Handler][c.Cause] = c.Count
		}
		return out
	}))
}

func coldCauseIndex(cause string) int {
	for i, c := range coldCauses {
		if c == cause {
			return i
		}
	}
	return -1
}

// CustomerServeCell is one {handler, outcome, class_warm} count.
type CustomerServeCell struct {
	Handler, Outcome, ClassWarm string
	Count                       uint64
}

// CustomerServesSnapshot is every customer-serve cell, all pre-populated (a 0
// is a reading, not an absence), for /debug/vars and the OTLP mirror.
func CustomerServesSnapshot() []CustomerServeCell {
	out := make([]CustomerServeCell, 0, int(numServeHandlers)*int(numServeOutcomes))
	for h := serveHandler(0); h < numServeHandlers; h++ {
		for o := serveOutcome(0); o < numServeOutcomes; o++ {
			out = append(out, CustomerServeCell{
				Handler: serveHandlerNames[h], Outcome: serveOutcomeNames[o],
				ClassWarm: classWarmNone, Count: customerServes[h][o].Load(),
			})
		}
	}
	return out
}

// CustomerColdServeCell is one {handler, cause} cold count.
type CustomerColdServeCell struct {
	Handler, Cause string
	Count          uint64
}

// CustomerColdServesSnapshot is every cold-cause cell, all pre-populated.
func CustomerColdServesSnapshot() []CustomerColdServeCell {
	out := make([]CustomerColdServeCell, 0, int(numServeHandlers)*len(coldCauses))
	for h := serveHandler(0); h < numServeHandlers; h++ {
		for i, c := range coldCauses {
			out = append(out, CustomerColdServeCell{
				Handler: serveHandlerNames[h], Cause: c, Count: customerColdServes[h][i].Load(),
			})
		}
	}
	return out
}

// isTopLevelCustomerCall reports whether req is a top-level customer /call: a
// request a user is waiting on that is not part of another call's work (see
// the file comment). req is the request after beginServeWorkClass.
//
// Every loopback marker a caller can send (the work class, the background hop,
// the nested depth) excludes a request only on a trusted self-loopback, so a
// caller cannot hide its own calls by sending one. The work class is already
// read that way (loopbackWorkClass). The background hop is honoured from any
// host as a write refusal (reseedBackgroundFromHeaders), so here it excludes
// only when the hop arrived on the self host.
func isTopLevelCustomerCall(req *http.Request) bool {
	ctx := req.Context()
	return workclass.Of(ctx) == workclass.Customer &&
		!isStaleRerun(req) &&
		!isNonCustomerOrigin(req) &&
		!isNestedSelfLoopback(req)
}

// isNonCustomerOrigin reports whether req's resolve belongs to someone other
// than the customer, reading a background-hop marker only on a trusted
// self-loopback. Every origin is weighed on its own
// (cache.EachNonCustomerOrigin), never the first alone: an untrusted hop
// marker outranks an export, and must not hide it.
func isNonCustomerOrigin(req *http.Request) bool {
	other := false
	cache.EachNonCustomerOrigin(req.Context(), func(o string) bool {
		if o != cache.NonCustomerBackgroundHop || isTrustedSelfLoopback(req) {
			other = true
		}
		return !other
	})
	return other
}

// isNestedSelfLoopback reports whether req is a hop of a resolve that called
// snowplow's own /call: the egress (api resolve.go, the self-host arm) sends
// X-Snowplow-Nested-Depth on every such hop, and it is honoured only on a
// trusted self-loopback, exactly as the ingest reseeds it
// (reseedNestedGuardsFromHeaders).
func isNestedSelfLoopback(req *http.Request) bool {
	return req.Header.Get(cache.NestedDepthHeader) != "" && isTrustedSelfLoopback(req)
}

// customerServe is one dispatch's serve-outcome recorder. Every method is a
// no-op on a request that is not counted.
type customerServe struct {
	counted bool
	handler serveHandler
	wri     http.ResponseWriter
	ctx     context.Context
	decided bool
	outcome serveOutcome
	cause   string
}

// beginCustomerServe starts the recorder for one restactions / widgets
// dispatch; defer the returned end. req is the request after
// beginServeWorkClass. end counts exactly one outcome for a counted request:
// the decision, or refused when the dispatch returned before deciding.
func beginCustomerServe(wri http.ResponseWriter, req *http.Request, h serveHandler) (*customerServe, func()) {
	s := &customerServe{counted: isTopLevelCustomerCall(req), handler: h, wri: wri, ctx: req.Context()}
	return s, s.end
}

func (s *customerServe) end() {
	if !s.counted {
		return
	}
	customerServes[s.handler][s.outcome].Add(1)
	if s.outcome == serveOutcomeRefused {
		return // no serve to describe: no span attribute (and no header)
	}
	attrs := []attribute.KeyValue{attribute.String(spanAttrServeOutcome, serveOutcomeNames[s.outcome])}
	if s.outcome == serveOutcomeCold {
		if i := coldCauseIndex(s.cause); i >= 0 {
			customerColdServes[s.handler][i].Add(1)
		}
		attrs = append(attrs, attribute.String(spanAttrServeColdCause, s.cause))
	}
	// Set at the end, with the final outcome: the request's server span is
	// still open (the handler runs inside it).
	trace.SpanFromContext(s.ctx).SetAttributes(attrs...)
}

// refused records that the request was refused after all, by a check that
// answers only once the resolve is entered (the #1116 reserved-extras 400:
// nothing was read or dialled). It overrides a decision already made and
// withdraws its header, so call it before the refusal is written.
func (s *customerServe) refused() {
	if !s.counted {
		return
	}
	s.decided, s.outcome, s.cause = true, serveOutcomeRefused, ""
	s.wri.Header().Del(ServeOutcomeHeader)
}

// servedCell decides a serve of the resident cell entry stored under key:
// stale when its bytes are stale (the #1062 ledger's predicate, NoteStaleHit),
// else fresh. Call it before the body is written.
func (s *customerServe) servedCell(key string, entry *cache.ResolvedEntry) {
	if entry != nil && (entry.StaleDerived || cache.InputsMovedSincePut(key)) {
		s.decide(serveOutcomeStale, "")
		return
	}
	s.decide(serveOutcomeFresh, "")
}

// servedLive decides a cold serve: the body will come from a live resolve run
// for this request, for cause (one of coldCauses). Call it before the resolve.
func (s *customerServe) servedLive(cause string) {
	s.decide(serveOutcomeCold, cause)
}

func (s *customerServe) decide(o serveOutcome, cause string) {
	if !s.counted || s.decided {
		return
	}
	s.decided, s.outcome, s.cause = true, o, cause
	s.wri.Header().Set(ServeOutcomeHeader, serveOutcomeNames[o])
}

// coldCauseOfLookup names why a request with this key mint and lookup result
// had no cell to serve. handle and inputs are dispatchCacheLookupKey's; miss is
// l1Get's result, or cache.LookupAbsent when no lookup ran (demand).
func coldCauseOfLookup(ctx context.Context, handle cacheHandle, inputs *cache.ResolvedKeyInputs, miss cache.LookupMiss) string {
	if handle == nil {
		if _, _, bypass := keyMintInputs(ctx); bypass != "" {
			return bypass
		}
		return coldCauseIneligibleKey
	}
	if !serveFromCacheEligible(inputs) {
		return coldCauseIneligibleKey
	}
	switch miss {
	case cache.LookupExpiredTTL:
		return coldCauseExpiredTTL
	case cache.LookupPastMaxAge:
		return coldCauseEvictedMaxAge
	}
	return coldCauseNoCell
}
