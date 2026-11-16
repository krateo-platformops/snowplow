package slowcall

import (
	"context"
	"log/slog"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/redact"
)

// snapshotEvery is the goroutine snapshot's rate limit: at most one per
// process per minute (#1054). Once a minute is enough to catch every episode
// that is not a continuous stall, and bounds the profile's cost.
const snapshotEvery = time.Minute

// The bounds of one snapshot (#1054: bounded). A stall's waiters share one
// stack, so the top stacks name it; the cap keeps one log line readable.
// maxSnapshotRequests caps what is RENDERED: every slow request in flight is
// considered for coverage and suppression.
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

// registryShards spreads the in-flight registry so concurrent requests rarely
// share a lock (#1059 review F8: the single mutex was the only contention).
const registryShards = 16

type shard struct {
	mu         sync.Mutex
	head, tail *Record
	n          int
	_          [32]byte // keep neighbouring shards off one cache line
}

// monitor is the in-flight registry, the watchdog, the CPU sampler and the
// counters of one recorder. Production has one (defaultMonitor); tests build
// their own.
type monitor struct {
	bound     time.Duration
	snapEvery time.Duration
	tick      time.Duration
	now       func() time.Time
	// capture returns the goroutine profile (debug=1 text). A seam for the
	// size-cap and redaction arms.
	capture func() []byte
	logger  func() *slog.Logger
	stats   *stats
	cpu     *cpuSampler

	shards [registryShards]shard
	next   atomic.Uint32

	watchMu  sync.Mutex // the watchdog's marks and rate window
	lastSnap time.Time

	watchOnce sync.Once
}

func newMonitor(bound time.Duration, st *stats) *monitor {
	return &monitor{
		bound:     bound,
		snapEvery: snapshotEvery,
		// Four checks per bound: a request is seen within a quarter bound of
		// crossing it, while it is still stuck.
		tick:    bound / 4,
		now:     time.Now,
		capture: goroutineProfile,
		logger:  slog.Default,
		stats:   st,
		cpu:     newCPUSampler(),
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
	s := &m.shards[m.next.Add(1)%registryShards]
	r.shard = s
	s.mu.Lock()
	r.prev = s.tail
	if s.tail != nil {
		s.tail.next = r
	} else {
		s.head = r
	}
	s.tail = r
	s.n++
	s.mu.Unlock()
}

func (m *monitor) remove(r *Record) {
	s := r.shard
	s.mu.Lock()
	if r.prev != nil {
		r.prev.next = r.next
	} else if s.head == r {
		s.head = r.next
	}
	if r.next != nil {
		r.next.prev = r.prev
	} else if s.tail == r {
		s.tail = r.prev
	}
	r.prev, r.next = nil, nil
	s.n--
	s.mu.Unlock()
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

// reported returns, at `at`, the record's longest wait (or a wait still open
// that has already lasted longer), its longest dial (likewise) and its
// longest lead (likewise).
func (r *Record) reported(at time.Time) (wait, dial, lead waitInfo, leads int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	wait, dial, lead, leads = r.longest, r.dial, r.lead, r.leads
	for _, o := range r.open {
		if !o.set {
			continue
		}
		o.ns, o.cause = int64(at.Sub(o.since)), ReleaseOpen
		if !wait.set || o.ns > wait.ns {
			wait = o
		}
		if o.dial != nil && (!dial.set || o.ns > dial.ns) {
			dial = o
		}
	}
	if o := r.openLd; o.set {
		o.ns, o.cause = int64(at.Sub(o.since)), ReleaseOpen
		if !lead.set || o.ns > lead.ns {
			lead = o
		}
		leads++
	}
	return wait, dial, lead, leads
}

func ms(ns int64) int64 { return ns / int64(time.Millisecond) }

// waitKeyLabel is the reported form of a waited-on key: a per-process keyed
// digest (redact), so two records in one pod that waited on the same key carry
// the same label, and no key is ever logged.
func waitKeyLabel(key string) string { return redact.Prefixed("wait", key) }

// infoAttrs renders one wait, dial or lead under prefix ("wait_", "dial_",
// "lead_"). FLAT: one level, numbers for every measure.
func infoAttrs(prefix string, w waitInfo, withPhase bool) []slog.Attr {
	if !w.set {
		return nil
	}
	out := []slog.Attr{
		slog.String(prefix+"kind", w.kind),
		slog.Int64(prefix+"ms", ms(w.ns)),
		slog.String(prefix+"outcome", w.cause.String()),
	}
	if withPhase {
		out = append(out, slog.String(prefix+"phase", w.phase.String()))
	}
	if w.key != "" {
		out = append(out, slog.String(prefix+"key", waitKeyLabel(w.key)))
	}
	if w.gvr != "" {
		out = append(out, slog.String(prefix+"gvr", w.gvr))
	}
	if w.verb != "" {
		out = append(out, slog.String(prefix+"verb", w.verb))
	}
	if w.leaderClass != "" {
		out = append(out, slog.String(prefix+"leader_class", w.leaderClass))
	}
	if w.leaderWork != "" {
		out = append(out, slog.String(prefix+"leader_work", w.leaderWork))
	}
	if w.branch != "" {
		out = append(out, slog.String(prefix+"branch", w.branch))
	}
	if d := w.dial; d != nil {
		detail, host, class := d.attemptDetail()
		out = append(out,
			slog.Int64(prefix+"attempts", d.attempts.Load()),
			slog.Int64(prefix+"status", d.lastStatus.Load()),
			slog.String(prefix+"last_cause", class),
			slog.Int64(prefix+"retry_wait_ms", ms(d.retryWait.Load())),
			slog.Int64(prefix+"conn_wait_ms", ms(d.connWait.Load())),
			slog.Int64(prefix+"dns_ms", ms(d.dns.Load())),
			slog.Int64(prefix+"connect_ms", ms(d.connect.Load())),
			slog.Int64(prefix+"tls_ms", ms(d.tls.Load())),
			slog.Int64(prefix+"ttfb_ms", ms(d.ttfb.Load())),
			slog.Int64(prefix+"reused", d.reused.Load()),
			slog.Int64(prefix+"http_429", d.n429.Load()),
			slog.Int64(prefix+"http_5xx", d.n5xx.Load()),
			slog.Int64(prefix+"http_network_errors", d.nNet.Load()),
			slog.String(prefix+"attempt_detail", detail))
		if host != "" {
			out = append(out, slog.String(prefix+"host", host))
		}
	}
	return out
}

// recordAttrs is a slow request's log record: FLAT attributes (one level, no
// groups), numbers for every measure, so a line scrubber that hashes nested
// values keeps them.
func recordAttrs(r *Record, wall time.Duration, end time.Time, cause Release, bound time.Duration) []slog.Attr {
	views := r.phases()
	attrs := make([]slog.Attr, 0, 40+4*len(views))
	attrs = append(attrs,
		slog.String("subsystem", "slowcall"),
		slog.String("route", r.route),
		slog.String("method", r.method),
		slog.String("class", r.workClass().String()),
		slog.Int64("wall_ms", wall.Milliseconds()),
		slog.Int64("bound_ms", bound.Milliseconds()),
		slog.String("outcome", cause.String()),
		// #1115 — what the request's page budget did to its network
		// exchanges (pagebudget's closed set; none without a budget).
		slog.String("budget", r.budget.Load().Result().String()),
	)
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
	unattributed := int64(wall) - sum
	if unattributed < 0 {
		unattributed = 0
	}
	if unattributed > top {
		dominant = "unattributed"
	}
	attrs = append(attrs,
		slog.String("phase", dominant),
		slog.Int64("phase_sum_ms", ms(sum)),
		slog.Int64("unattributed_ms", ms(unattributed)))
	t := &r.dials
	if n := t.attempts.Load() + t.unscopedN.Load(); n > 0 {
		attrs = append(attrs,
			slog.Int64("http_attempts", t.attempts.Load()),
			slog.Int64("http_429", t.n429.Load()),
			slog.Int64("http_5xx", t.n5xx.Load()),
			slog.Int64("http_network_errors", t.nNet.Load()),
			slog.Int64("http_reused", t.reused.Load()),
			slog.Int64("retry_wait_ms", ms(t.retryWait.Load())),
			slog.Int64("conn_wait_ms", ms(t.connWait.Load())),
			slog.Int64("connect_ms", ms(t.connect.Load())),
			slog.Int64("ttfb_ms", ms(t.ttfb.Load())),
			slog.Int64("unscoped_http_n", t.unscopedN.Load()),
			slog.Int64("unscoped_http_ms", ms(t.unscopedNS.Load())))
	}
	wait, dial, lead, leads := r.reported(end)
	attrs = append(attrs, infoAttrs("wait_", wait, true)...)
	attrs = append(attrs, infoAttrs("dial_", dial, false)...)
	if leads > 0 {
		attrs = append(attrs, slog.Int("leads", leads))
		attrs = append(attrs, infoAttrs("lead_", lead, false)...)
	}
	return attrs
}

func (m *monitor) emit(ctx context.Context, r *Record, wall time.Duration, end time.Time, cause Release) {
	attrs := recordAttrs(r, wall, end, cause, m.bound)
	attrs = append(attrs, m.cpu.windowAttrs(r.start)...)
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
