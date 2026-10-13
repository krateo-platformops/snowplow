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
// LOGICAL FETCHES, NOT HTTP ATTEMPTS. Both counters count one per external
// stage call (dispatchOneCall). plumbing's RetryClient (http/util/retry.go) can
// send up to CLIENT_MAX_RETRIES+1 HTTP attempts per logical fetch on a network
// error, a 5xx or a 429 — and ClickHouse answers TOO_MANY_SIMULTANEOUS_QUERIES
// with a 5xx. Retries inside the RetryClient are NOT counted, so these do not
// compare 1:1 with a server-side attempt count such as ClickHouse query_log.
//
// KNOWN LIMIT: origin comes from cache.RefDenialOrigin, which defaults to
// "serve" for a ctx with no marker. A background goroutine that carries no
// background/prewarm marker therefore counts as origin=request. Every
// production background driver stamps one (the #563 census), and F9 pins the
// engine paths at request-delta 0.
//
// CLOSED LABEL SET: origin ∈ {background, request}, both pre-populated. Hand-
// wired onto OTLP in metrics.go (there is no expvar->OTLP bridge).
//
// #643 PR-B2 — THE driver LABEL. origin says "customer or not"; it cannot say
// WHICH background resolver dialled, and after the 1.12.46 roll ~60/min stayed
// in the background cell with the seed already skipping. driver is
// cache.RefDenialOrigin(ctx), the existing closed classifier: refresher,
// cohort-seed, prewarm-engine-boot (the discovery walk — rePrewarmBootScoped
// stamps it before withPhase1SAContext), prewarm-path (the content prewarm),
// background-unattributed, serve. Every cell is pre-populated. On OTLP each
// series carries BOTH labels (driver determines origin: serve → request, every
// other driver → background), so sum-by-origin reads exactly what origin alone
// read before. On expvar the {origin} maps are unchanged and the {driver} maps
// are published under their own keys (…_by_driver_total).

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
	d := cache.RefDenialOrigin(ctx)
	countExternalFetch(originForDriver(d))
	externalFetchByDriver.add(d)
}

// recordExternalFetchAbandoned counts one fetch whose caller gave up before the
// response was read: fetchErr != nil while the fetch ctx is done.
func recordExternalFetchAbandoned(ctx context.Context) {
	d := cache.RefDenialOrigin(ctx)
	countExternalFetchAbandoned(originForDriver(d))
	externalFetchAbandonedByDriver.add(d)
}

// originForDriver is the {origin} cell a driver belongs to — the same split
// externalFetchOrigin makes, so the two labels can never disagree on a series.
func originForDriver(driver string) string {
	if driver == cache.RefDenialOriginServe {
		return ExternalFetchOriginRequest
	}
	return ExternalFetchOriginBackground
}

// driverCounter is one counter per closed driver value, every cell present from
// construction. A value outside the closed set folds into
// background-unattributed (as RecordRefDenied does): the label set cannot grow
// from data.
type driverCounter map[string]*atomic.Uint64

func newDriverCounter() driverCounter {
	m := make(driverCounter, len(cache.RefDenialOrigins))
	for _, d := range cache.RefDenialOrigins {
		m[d] = new(atomic.Uint64)
	}
	return m
}

func (m driverCounter) add(driver string) {
	c, ok := m[driver]
	if !ok {
		c = m[cache.RefDenialOriginBackgroundUnattributed]
	}
	c.Add(1)
}

func (m driverCounter) snapshot() []ExternalFetchDriverCount {
	out := make([]ExternalFetchDriverCount, 0, len(cache.RefDenialOrigins))
	for _, d := range cache.RefDenialOrigins {
		out = append(out, ExternalFetchDriverCount{Driver: d, Origin: originForDriver(d), Count: m[d].Load()})
	}
	return out
}

var (
	externalFetchByDriver          = newDriverCounter()
	externalFetchAbandonedByDriver = newDriverCounter()
)

// ExternalFetchDriverCount is one {driver} cell, with the origin it rolls up to.
type ExternalFetchDriverCount struct {
	Driver string
	Origin string
	Count  uint64
}

// ExternalFetchByDriverSnapshot returns snowplow_external_fetch_total by driver,
// every closed cell included, in cache.RefDenialOrigins order.
func ExternalFetchByDriverSnapshot() []ExternalFetchDriverCount {
	return externalFetchByDriver.snapshot()
}

// ExternalFetchAbandonedByDriverSnapshot is the abandoned counter by driver.
func ExternalFetchAbandonedByDriverSnapshot() []ExternalFetchDriverCount {
	return externalFetchAbandonedByDriver.snapshot()
}

func countExternalFetch(origin string) {
	if origin == ExternalFetchOriginBackground {
		externalFetchBackground.Add(1)
		return
	}
	externalFetchRequest.Add(1)
}

func countExternalFetchAbandoned(origin string) {
	if origin == ExternalFetchOriginBackground {
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

// RecordExternalFetchForTest bumps both counters under the given origin — the
// OTLP falsifier's way to move a real series. It takes the origin, not a
// background ctx, so no production file stamps a background origin outside a
// real driver (the #563 census). Production MUST NOT use it.
func RecordExternalFetchForTest(background bool) {
	driver := cache.RefDenialOriginServe
	if background {
		driver = cache.RefDenialOriginBackgroundUnattributed
	}
	RecordExternalFetchDriverForTest(driver)
}

// RecordExternalFetchDriverForTest bumps both counters under the given driver
// (and the origin it rolls up to). Tests only — the #643 PR-B2 OTLP arm.
func RecordExternalFetchDriverForTest(driver string) {
	countExternalFetch(originForDriver(driver))
	countExternalFetchAbandoned(originForDriver(driver))
	externalFetchByDriver.add(driver)
	externalFetchAbandonedByDriver.add(driver)
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
		expvar.Publish("snowplow_external_fetch_by_driver_total", expvar.Func(func() any {
			return externalFetchDriverMap(ExternalFetchByDriverSnapshot())
		}))
		expvar.Publish("snowplow_external_fetch_abandoned_by_driver_total", expvar.Func(func() any {
			return externalFetchDriverMap(ExternalFetchAbandonedByDriverSnapshot())
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

func externalFetchDriverMap(s []ExternalFetchDriverCount) map[string]uint64 {
	m := make(map[string]uint64, len(s))
	for _, c := range s {
		m[c.Driver] = c.Count
	}
	return m
}
