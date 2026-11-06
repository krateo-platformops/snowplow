package slowcall

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// MonitorForTest is a recorder with its own bound, counters and log, and no
// watchdog goroutine. It lets arms in other packages drive the real
// middleware and the real phase sites without the process monitor's 3 s
// bound. Production MUST NOT use it.
type MonitorForTest struct{ m *monitor }

// NewMonitorForTest builds a test monitor whose records go to log.
func NewMonitorForTest(bound time.Duration, log *slog.Logger) *MonitorForTest {
	m := newMonitor(bound, newStats())
	m.logger = func() *slog.Logger { return log }
	return &MonitorForTest{m: m}
}

// Middleware is the production request recorder over this monitor.
func (t *MonitorForTest) Middleware(next http.Handler) http.Handler { return t.m.middleware(next) }

// SlowRequestsForTest is this monitor's snowplow_slow_requests_total.
func (t *MonitorForTest) SlowRequestsForTest() map[RouteClass]uint64 { return t.m.stats.counts() }

// RecordingForTest reports whether ctx carries a request's slow-call record.
func RecordingForTest(ctx context.Context) bool { return fromContext(ctx) != nil }

// BodyTimedForTest reports whether w is the writer Body installs (the
// handler's writes into the compressor are timed).
func BodyTimedForTest(w http.ResponseWriter) bool {
	_, ok := w.(*bodyWriter)
	return ok
}

// WaitKeyLabelForTest is the label a record reports for a waited-on key.
func WaitKeyLabelForTest(key string) string { return waitKeyLabel(key) }

// EmitSlowForTest runs the process monitor's production emit path (counter,
// phase histogram and its OTLP hook, ring, WARN line) for one slow request on
// route that spent d in phase p. The phase accounting itself is driven through
// Start/End by this package's own arms. Production MUST NOT use it.
func EmitSlowForTest(route string, p Phase, d time.Duration) {
	m := defaultMonitor()
	rc := &recordCtx{Context: context.Background()}
	r := &rc.rec
	end := time.Now()
	r.start, r.route = end.Add(-(m.bound + d)), route
	if i := strings.IndexByte(route, ' '); i > 0 {
		r.method = route[:i]
	}
	r.ph[p].ns.Store(int64(d))
	r.ph[p].max.Store(int64(d))
	r.ph[p].n.Store(1)
	// log-ctx-waiver: detached: a test seam that emits a record with no request behind it
	m.emit(context.Background(), r, m.bound+d, end, ReleaseCompleted)
}

// ResetDefaultStatsForTest zeroes the process monitor's counters, histogram
// and rings (the OTLP hook stays). Production MUST NOT use it.
func ResetDefaultStatsForTest() {
	s := defaultStats
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byRoute = map[RouteClass]uint64{}
	for i := range s.hist {
		s.hist[i] = phaseHist{buckets: make([]uint64, len(PhaseBoundsMS)+1)}
	}
	s.recent, s.snaps = nil, nil
	s.snapshots.Store(0)
	s.suppressed.Store(0)
}
