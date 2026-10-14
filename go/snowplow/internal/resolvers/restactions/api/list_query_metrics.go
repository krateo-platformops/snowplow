// list_query_metrics.go — #618: how often an informer-servable call carried a
// query string, and what happened to it.
//
// One counter keyed by {outcome, reason}, both drawn from the closed,
// code-defined enum below, so an OTLP attribute built from them has bounded
// cardinality by construction (the #311 bounded-attribute discipline):
//
//   - outcome=in_process, reason=selector — a labelSelector and/or a
//     metadata.name / metadata.namespace fieldSelector was applied to the
//     informer's items;
//   - outcome=fallthrough, reason=<why> — the call went to the apiserver with
//     its query intact.
//
// A call with no query is NOT counted: it is the overwhelming majority and
// would make the two outcomes unreadable as a share.
//
// It reaches /debug/vars as snowplow_list_query_total and OTLP as
// snowplow_list_query_total{outcome,reason} (hand-wired in metrics.go; there is
// no expvar->OTLP bridge). Both read ListQueryOutcomeSnapshot, so the two
// surfaces cannot diverge.
//
// CFG-1: gated on cache.Disabled(). With the cache off nothing is served from
// an informer, so the family is structurally zero and is not published.
package api

import (
	"expvar"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/apicall"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

const (
	listQueryOutcomeInProcess   = "in_process"
	listQueryOutcomeFallthrough = "fallthrough"

	listQueryReasonSelector                  = apicall.ReasonSelector
	listQueryReasonMalformed                 = apicall.ReasonMalformed
	listQueryReasonQueryOnGet                = apicall.ReasonQueryOnGet
	listQueryReasonRepeatedParam             = apicall.ReasonRepeatedParam
	listQueryReasonPagination                = apicall.ReasonPagination
	listQueryReasonResourceVersion           = apicall.ReasonResourceVersion
	listQueryReasonWatch                     = apicall.ReasonWatch
	listQueryReasonUnknownParam              = apicall.ReasonUnknownParam
	listQueryReasonInvalidSelector           = apicall.ReasonInvalidSelector
	listQueryReasonUnsupportedField          = apicall.ReasonUnsupportedField
	listQueryReasonNamespaceFieldClusterPath = apicall.ReasonNamespaceFieldClusterPath
)

// listQueryFallthroughReasons is the closed fall-through reason set.
// It is defined once, in internal/apicall, next to the classifier that emits
// these reasons.
var listQueryFallthroughReasons = apicall.FallthroughReasons()

var (
	listQueryInProcess   atomic.Uint64
	listQueryFallthrough = func() map[string]*atomic.Uint64 {
		m := make(map[string]*atomic.Uint64, len(listQueryFallthroughReasons))
		for _, r := range listQueryFallthroughReasons {
			m[r] = new(atomic.Uint64)
		}
		return m
	}()
)

// recordListQuery counts one serve decision for a call that carried a query.
// An empty query records nothing. The map is never written after init, so the
// lookup is race-free.
func recordListQuery(q listQuery) {
	if q.empty() {
		return
	}
	if q.applicable {
		listQueryInProcess.Add(1)
		return
	}
	if c, ok := listQueryFallthrough[q.reason]; ok {
		c.Add(1)
		return
	}
	// Unreachable: planListQuery only emits enum reasons. Count under
	// unknown_param rather than lose the fall-through.
	listQueryFallthrough[listQueryReasonUnknownParam].Add(1)
}

// ListQueryOutcome is one {outcome, reason} series.
type ListQueryOutcome struct {
	Outcome string
	Reason  string
	Count   uint64
}

// ListQueryOutcomeSnapshot returns every series of the closed enum, zeros
// included, in a fixed order — the single source both /debug/vars and OTLP read.
func ListQueryOutcomeSnapshot() []ListQueryOutcome {
	out := make([]ListQueryOutcome, 0, 1+len(listQueryFallthroughReasons))
	out = append(out, ListQueryOutcome{listQueryOutcomeInProcess, listQueryReasonSelector, listQueryInProcess.Load()})
	for _, r := range listQueryFallthroughReasons {
		out = append(out, ListQueryOutcome{listQueryOutcomeFallthrough, r, listQueryFallthrough[r].Load()})
	}
	return out
}

// ResetListQueryOutcomesForTest zeroes every series. Production MUST NOT use it.
func ResetListQueryOutcomesForTest() {
	listQueryInProcess.Store(0)
	for _, c := range listQueryFallthrough {
		c.Store(0)
	}
}

// RecordListQueryForTest classifies path p as a namespaced LIST and records the
// outcome — the OTLP falsifier's way to bump a real series. Production MUST NOT
// use it.
func RecordListQueryForTest(p string) { recordListQuery(planListQuery(p, true, true)) }

var listQueryExpvarOnce sync.Once

func init() {
	if cache.Disabled() {
		return
	}
	registerListQueryExpvar()
}

func registerListQueryExpvar() {
	listQueryExpvarOnce.Do(func() {
		expvar.Publish("snowplow_list_query_total", expvar.Func(func() any {
			m := map[string]map[string]uint64{
				listQueryOutcomeInProcess:   {},
				listQueryOutcomeFallthrough: {},
			}
			for _, s := range ListQueryOutcomeSnapshot() {
				m[s.Outcome][s.Reason] = s.Count
			}
			return m
		}))
	})
}

// RegisterListQueryExpvarForTest forces registration under tests that flip
// CACHE_ENABLED after init() ran. Idempotent. Production MUST NOT use it.
func RegisterListQueryExpvarForTest() { registerListQueryExpvar() }
