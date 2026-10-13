// external_fetch_metrics.go — #643 PR-A: how many external fetches snowplow
// issues, for whom, and how many it walks away from.
//
// WHY. On 057 the background prewarm engine re-fetched every external
// (ClickHouse) cell once per cohort per pass, ~19 queries/min from one pod, and
// cut ~7.7/min of them at the 5s boot-readiness bound while the server kept
// running them. None of it was visible from snowplow: the only trace was a Warn
// per cut, with an empty traceId. These two counters make it a reading.
//
//   - snowplow_external_fetch_total{origin}: one count per external fetch, at
//     the ExternalTouchedSink bump site (resolve.go) — the same site that makes
//     the resolve's L1 Put decline. origin is "background" for any non-customer
//     resolve (the prewarm engine's seed / walk, the content prewarm, the
//     refresher) and "request" for a customer /call. During the defect the
//     background cell climbs ~19/min; it is not a counter whose zero is health.
//   - snowplow_external_fetch_abandoned_total{origin}: the fetch returned an
//     error AFTER its own ctx was done — the caller stopped waiting before the
//     response body was read (the 5s bound, a cut scope, a client that left).
//     The server may still be executing the request.
//
// CLOSED LABEL SET: origin ∈ {background, request}, both pre-populated. Hand-
// wired onto OTLP in metrics.go (there is no expvar->OTLP bridge).

package api

import (
	"context"
	"expvar"
	"sync"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// External-fetch origins (closed set).
const (
	ExternalFetchOriginBackground = "background"
	ExternalFetchOriginRequest    = "request"
)

var (
	externalFetchBackground          atomic.Uint64
	externalFetchRequest             atomic.Uint64
	externalFetchAbandonedBackground atomic.Uint64
	externalFetchAbandonedRequest    atomic.Uint64
)

// externalFetchOrigin classifies the resolve the fetch belongs to. It reuses
// cache.RefDenialOrigin, the single "who is resolving" classifier: anything
// that is not a customer /call (a named background driver, the content
// prewarm's prewarm-path marker, or an unattributed background marker) is
// background.
func externalFetchOrigin(ctx context.Context) string {
	if cache.RefDenialOrigin(ctx) == cache.RefDenialOriginServe {
		return ExternalFetchOriginRequest
	}
	return ExternalFetchOriginBackground
}

// recordExternalFetch counts one external fetch about to be issued.
func recordExternalFetch(ctx context.Context) {
	if externalFetchOrigin(ctx) == ExternalFetchOriginBackground {
		externalFetchBackground.Add(1)
		return
	}
	externalFetchRequest.Add(1)
}

// recordExternalFetchAbandoned counts one fetch whose caller gave up before the
// response was read: fetchErr != nil while the fetch ctx is done.
func recordExternalFetchAbandoned(ctx context.Context) {
	if externalFetchOrigin(ctx) == ExternalFetchOriginBackground {
		externalFetchAbandonedBackground.Add(1)
		return
	}
	externalFetchAbandonedRequest.Add(1)
}

// ExternalFetchCount is one {origin} cell.
type ExternalFetchCount struct {
	Origin string
	Count  uint64
}

// ExternalFetchSnapshot returns snowplow_external_fetch_total's cells, zeros
// included, background first.
func ExternalFetchSnapshot() []ExternalFetchCount {
	return []ExternalFetchCount{
		{Origin: ExternalFetchOriginBackground, Count: externalFetchBackground.Load()},
		{Origin: ExternalFetchOriginRequest, Count: externalFetchRequest.Load()},
	}
}

// ExternalFetchAbandonedSnapshot returns snowplow_external_fetch_abandoned_total's
// cells, zeros included, background first.
func ExternalFetchAbandonedSnapshot() []ExternalFetchCount {
	return []ExternalFetchCount{
		{Origin: ExternalFetchOriginBackground, Count: externalFetchAbandonedBackground.Load()},
		{Origin: ExternalFetchOriginRequest, Count: externalFetchAbandonedRequest.Load()},
	}
}

// RecordExternalFetchForTest bumps both counters for origin's ctx shape — the
// OTLP falsifier's way to move a real series. Production MUST NOT use it.
func RecordExternalFetchForTest(background bool) {
	ctx := context.Background()
	if background {
		ctx = cache.WithBackgroundResolveOrigin(ctx, cache.BackgroundOriginCohortSeed)
	}
	recordExternalFetch(ctx)
	recordExternalFetchAbandoned(ctx)
}

var externalFetchExpvarOnce sync.Once

// CFG-1: the same Disabled() gate as every other cache-side publisher in this
// package (list_query_metrics.go).
func init() {
	if cache.Disabled() {
		return
	}
	externalFetchExpvarOnce.Do(func() {
		expvar.Publish("snowplow_external_fetch_total", expvar.Func(func() any {
			return externalFetchMap(ExternalFetchSnapshot())
		}))
		expvar.Publish("snowplow_external_fetch_abandoned_total", expvar.Func(func() any {
			return externalFetchMap(ExternalFetchAbandonedSnapshot())
		}))
	})
}

func externalFetchMap(s []ExternalFetchCount) map[string]uint64 {
	m := make(map[string]uint64, len(s))
	for _, c := range s {
		m[c.Origin] = c.Count
	}
	return m
}
