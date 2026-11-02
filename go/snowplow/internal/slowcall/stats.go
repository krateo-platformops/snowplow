package slowcall

import (
	"expvar"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// stats are one monitor's counters, phase histogram and rings.
type stats struct {
	mu      sync.Mutex
	byRoute map[RouteClass]uint64
	hist    [numPhases]phaseHist
	recent  []map[string]any // oldest first, ≤ recentRecords
	snaps   []map[string]any // oldest first, ≤ recentSnapshots

	snapshots  atomic.Uint64
	suppressed atomic.Uint64
	recorder   atomic.Pointer[func(ms int64, phase string)]
}

type phaseHist struct {
	// buckets[i] counts samples with PhaseBoundsMS[i-1] < ms ≤ PhaseBoundsMS[i];
	// the last slot is +Inf.
	buckets []uint64
	count   uint64
	sumMS   int64
	maxMS   int64
}

func newStats() *stats {
	s := &stats{byRoute: map[RouteClass]uint64{}}
	for i := range s.hist {
		s.hist[i].buckets = make([]uint64, len(PhaseBoundsMS)+1)
	}
	return s
}

var defaultStats = newStats()

// RouteClass is one series of snowplow_slow_requests_total: the route (a
// ServeMux pattern) and the request's work class, so a background
// self-loopback is never counted as a customer's slow call.
type RouteClass struct{ Route, Class string }

// observe counts one slow request: its route and class, every phase it
// entered into the phase histogram, and its record into the recent ring.
func (s *stats) observe(r *Record, attrs []slog.Attr, at time.Time) {
	views := r.phases()
	rec := s.recorder.Load()
	rc := RouteClass{Route: r.route, Class: r.workClass().String()}
	s.mu.Lock()
	s.byRoute[rc]++
	for _, v := range views {
		if v.n == 0 {
			continue
		}
		s.hist[v.p].record(ms(v.ns))
	}
	s.recent = pushRing(s.recent, attrMap(attrs, at), recentRecords)
	s.mu.Unlock()
	if rec != nil {
		for _, v := range views {
			if v.n > 0 {
				(*rec)(ms(v.ns), v.p.String())
			}
		}
	}
}

func (h *phaseHist) record(v int64) {
	v = max(v, 0)
	i := 0
	for i < len(PhaseBoundsMS) && v > PhaseBoundsMS[i] {
		i++
	}
	h.buckets[i]++
	h.count++
	h.sumMS += v
	h.maxMS = max(h.maxMS, v)
}

func pushRing(ring []map[string]any, e map[string]any, capacity int) []map[string]any {
	if len(ring) >= capacity {
		ring = append(ring[:0:0], ring[len(ring)-capacity+1:]...)
	}
	return append(ring, e)
}

func (s *stats) noteSnapshot(e map[string]any) {
	s.snapshots.Add(1)
	s.mu.Lock()
	s.snaps = pushRing(s.snaps, e, recentSnapshots)
	s.mu.Unlock()
}

func (s *stats) counts() map[RouteClass]uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[RouteClass]uint64, len(s.byRoute))
	for k, v := range s.byRoute {
		out[k] = v
	}
	return out
}

// routes renders the counter for /debug/vars: route → class → count.
func (s *stats) routes() map[string]map[string]uint64 {
	out := map[string]map[string]uint64{}
	for k, v := range s.counts() {
		if out[k.Route] == nil {
			out[k.Route] = map[string]uint64{}
		}
		out[k.Route][k.Class] = v
	}
	return out
}

// histValue renders the phase histogram for /debug/vars: per phase,
// cumulative le_<bound> buckets plus le_inf, count, sum_ms and max_ms (the
// shape of the content-fill histograms).
func (s *stats) histValue() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]any, numPhases)
	for p := Phase(0); p < numPhases; p++ {
		h := &s.hist[p]
		buckets := make(map[string]uint64, len(h.buckets))
		var cum uint64
		for i, b := range h.buckets {
			cum += b
			if i < len(PhaseBoundsMS) {
				buckets["le_"+strconv.FormatInt(PhaseBoundsMS[i], 10)] = cum
			} else {
				buckets["le_inf"] = cum
			}
		}
		out[p.String()] = map[string]any{"buckets": buckets, "count": h.count, "sum_ms": h.sumMS, "max_ms": h.maxMS}
	}
	return out
}

func (s *stats) ring(which *[]map[string]any) []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]map[string]any(nil), (*which)...)
}

// SlowRequests is snowplow_slow_requests_total by route and class, for the
// OTLP mirror.
func SlowRequests() map[RouteClass]uint64 { return defaultStats.counts() }

// SnapshotCounts are the goroutine snapshots taken, and those a rate window
// suppressed (one per slow request that crossed the bound inside it).
func SnapshotCounts() (taken, suppressed uint64) {
	return defaultStats.snapshots.Load(), defaultStats.suppressed.Load()
}

// SetPhaseRecorder installs the phase histogram's OTLP hook (this package
// cannot import metrics); nil uninstalls it.
func SetPhaseRecorder(fn func(ms int64, phase string)) {
	if fn == nil {
		defaultStats.recorder.Store(nil)
		return
	}
	defaultStats.recorder.Store(&fn)
}

func init() {
	expvar.Publish("snowplow_slow_requests_total", expvar.Func(func() any { return defaultStats.routes() }))
	expvar.Publish("snowplow_slow_request_phase_ms", expvar.Func(func() any { return defaultStats.histValue() }))
	expvar.Publish("snowplow_slow_requests_recent", expvar.Func(func() any { return defaultStats.ring(&defaultStats.recent) }))
	expvar.Publish("snowplow_slow_request_snapshots_total", expvar.Func(func() any { return defaultStats.snapshots.Load() }))
	expvar.Publish("snowplow_slow_request_snapshots_suppressed_total", expvar.Func(func() any { return defaultStats.suppressed.Load() }))
	expvar.Publish("snowplow_slow_request_snapshots_recent", expvar.Func(func() any { return defaultStats.ring(&defaultStats.snaps) }))
	expvar.Publish("snowplow_slow_request_bound_ms", expvar.Func(func() any { return Bound.Milliseconds() }))
}
