package slowcall

import (
	"context"
	"log/slog"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/krateo-platformops/snowplow/internal/redact"
)

// snapshotEvery is the goroutine snapshot's rate limit: at most one per
// process per minute (#1054). Once a minute is enough to catch every episode
// that is not a continuous stall, and bounds the profile's cost.
const snapshotEvery = time.Minute

// The bounds of one snapshot (#1054: bounded). A stall's waiters share one
// stack, so the top stacks name it; the cap keeps one log line readable.
const (
	maxSnapshotBytes    = 32 << 10
	maxSnapshotRequests = 8
	maxStacksPerClass   = 12
	maxFramesPerStack   = 24
)

// The /debug/vars rings: the last records and snapshots, so a sampler that
// reads /debug/vars (the bench's mem606) collects them unscrubbed.
const (
	recentRecords   = 32
	recentSnapshots = 2
)

// PhaseBoundsMS are the phase histogram's upper bounds (ms). They include the
// goal's own marks, Bound (3000) and PageMaxGoal (10000), so a bucket answers
// "did this phase alone break the goal".
var PhaseBoundsMS = []int64{1, 10, 100, 250, 500, 1000, 2000, 3000, 5000, 10000, 30000, 60000}

// monitor is the in-flight registry, the watchdog and the counters of one
// recorder. Production has one (defaultMonitor); tests build their own.
type monitor struct {
	bound     time.Duration
	snapEvery time.Duration
	tick      time.Duration
	// capture returns the goroutine profile (debug=1 text). A seam for the
	// size-cap and redaction arms.
	capture func() []byte
	logger  func() *slog.Logger
	stats   *stats

	mu         sync.Mutex
	head, tail *Record
	n          int
	lastSnap   time.Time

	watchOnce sync.Once
	stop      chan struct{}
}

func newMonitor(bound time.Duration, st *stats) *monitor {
	return &monitor{
		bound:     bound,
		snapEvery: snapshotEvery,
		// Four checks per bound: a request is seen within a quarter bound of
		// crossing it, while it is still stuck.
		tick:    bound / 4,
		capture: goroutineProfile,
		logger:  slog.Default,
		stats:   st,
	}
}

var (
	defaultMonitorOnce sync.Once
	defaultMon         *monitor
)

func defaultMonitor() *monitor {
	defaultMonitorOnce.Do(func() { defaultMon = newMonitor(Bound, defaultStats) })
	return defaultMon
}

func goroutineProfile() []byte {
	var b bytesBuffer
	_ = pprof.Lookup("goroutine").WriteTo(&b, 1)
	return b.b
}

type bytesBuffer struct{ b []byte }

func (w *bytesBuffer) Write(p []byte) (int, error) { w.b = append(w.b, p...); return len(p), nil }

func (m *monitor) add(r *Record) {
	m.mu.Lock()
	r.prev = m.tail
	if m.tail != nil {
		m.tail.next = r
	} else {
		m.head = r
	}
	m.tail = r
	m.n++
	m.mu.Unlock()
}

func (m *monitor) remove(r *Record) {
	m.mu.Lock()
	if r.prev != nil {
		r.prev.next = r.next
	} else if m.head == r {
		m.head = r.next
	}
	if r.next != nil {
		r.next.prev = r.prev
	} else if m.tail == r {
		m.tail = r.prev
	}
	r.prev, r.next = nil, nil
	m.n--
	m.mu.Unlock()
}

// phaseView is one phase of a record as reported.
type phaseView struct {
	p             Phase
	ns, max, n, o int64
}

func (r *Record) phases() []phaseView {
	var out []phaseView
	for p := Phase(0); p < numPhases; p++ {
		a := &r.ph[p]
		v := phaseView{p: p, ns: a.ns.Load(), max: a.max.Load(), n: a.n.Load(), o: a.open.Load()}
		if v.n > 0 || v.o > 0 {
			out = append(out, v)
		}
	}
	return out
}

// reportedWait is the wait a record names: the longest one, or a wait still
// open at `at` that has already lasted longer.
func (r *Record) reportedWait(at time.Time) waitInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	w := r.longest
	for _, o := range r.open {
		if !o.set {
			continue
		}
		if age := int64(at.Sub(o.since)); !w.set || age > w.ns {
			o.ns, o.cause = age, ReleaseOpen
			w = o
		}
	}
	return w
}

func (r *Record) workClass() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.class
}

func ms(ns int64) int64 { return ns / int64(time.Millisecond) }

// waitKeyLabel is the reported form of a waited-on key: a per-process keyed
// digest (redact), so two records in one pod that waited on the same key carry
// the same label, and no key is ever logged.
func waitKeyLabel(key string) string { return redact.Prefixed("wait", key) }

func waitAttrs(w waitInfo) []slog.Attr {
	if !w.set {
		return nil
	}
	out := []slog.Attr{
		slog.String("wait_phase", w.phase.String()),
		slog.String("kind", w.kind),
		slog.Int64("wait_ms", ms(w.ns)),
		slog.String("wait_outcome", w.cause.String()),
	}
	if w.key != "" {
		out = append(out, slog.String("wait_key", waitKeyLabel(w.key)))
	}
	if w.gvr != "" {
		out = append(out, slog.String("gvr", w.gvr))
	}
	return out
}

// recordAttrs is a slow request's log record: FLAT attributes (one level, no
// groups), numbers for every measure, so a line scrubber that hashes nested
// values keeps them.
func recordAttrs(r *Record, wall time.Duration, end time.Time, cause Release, bound time.Duration) []slog.Attr {
	views := r.phases()
	attrs := make([]slog.Attr, 0, 12+4*len(views))
	attrs = append(attrs,
		slog.String("subsystem", "slowcall"),
		slog.String("route", r.route),
		slog.String("method", r.method),
		slog.Int64("wall_ms", wall.Milliseconds()),
		slog.Int64("bound_ms", bound.Milliseconds()),
		slog.String("outcome", cause.String()),
	)
	if c := r.workClass(); c != "" {
		attrs = append(attrs, slog.String("class", c))
	}
	dominant, sum := "unattributed", int64(0)
	var top int64
	for _, v := range views {
		sum += v.ns
		if v.ns > top {
			top, dominant = v.ns, v.p.String()
		}
		name := v.p.String()
		attrs = append(attrs,
			slog.Int64(name+"_ms", ms(v.ns)),
			slog.Int64(name+"_n", v.n),
			slog.Int64(name+"_max_ms", ms(v.max)))
		if v.o > 0 {
			attrs = append(attrs, slog.Int64(name+"_open", v.o))
		}
	}
	attrs = append(attrs, slog.String("phase", dominant), slog.Int64("phase_sum_ms", ms(sum)))
	return append(attrs, waitAttrs(r.reportedWait(end))...)
}

func (m *monitor) emit(ctx context.Context, r *Record, wall time.Duration, end time.Time, cause Release) {
	attrs := recordAttrs(r, wall, end, cause, m.bound)
	m.stats.observe(r, attrs, end)
	m.logger().LogAttrs(ctx, slog.LevelWarn, "slowcall.request", attrs...)
}

// attrMap renders flat attributes for a /debug/vars ring entry.
func attrMap(attrs []slog.Attr, at time.Time) map[string]any {
	out := make(map[string]any, len(attrs)+1)
	out["time"] = at.UTC().Format(time.RFC3339Nano)
	for _, a := range attrs {
		out[a.Key] = a.Value.Any()
	}
	return out
}
