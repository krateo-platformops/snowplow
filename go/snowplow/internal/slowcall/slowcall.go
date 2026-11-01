// Package slowcall names where a slow request's time went (#1054).
//
// Bench runs 19 and 20 saw rare multi-second stalls (one call of 30.5 s) in
// which concurrent calls were released together, while the apiserver queued
// nothing and the server was otherwise quiet. Snowplow logs WARN and above
// only and had no per-request timing, so the wait could not be named. This
// package is the instrument; it changes no behaviour.
//
// THE RECORD. Middleware gives every request on a tracked route (every
// request/response API route: not the SSE stream, the probes, the RUM upload
// or the /debug subtree) a Record on its context. Code on the request's path
// marks the time it spends in each Phase with Start/End (or StartWait/
// EndRelease for a wait on work another goroutine owns). When the request
// ends, its wall time is compared with Bound. Over it, ONE structured WARN
// line ("slowcall.request") names the route, the wall time, the time and
// count of every phase it entered, the longest wait (its phase, kind, the
// keyed digest of the waited-on key, and why it was released) and the
// request's own release cause; snowplow_slow_requests_total{route} and the
// phase histogram count it. Under the bound nothing is logged or counted.
//
// PHASES OVERLAP BY DESIGN. A phase is the summed time the request's
// goroutines spent in it, so parallel stages can sum past the wall time, and
// a phase may contain another (a stage dial can contain an informer sync
// wait). The record reports each phase as measured, never a residual.
//
// THE SNAPSHOT (snapshot.go). While a tracked request is in flight past the
// bound, a watchdog captures one goroutine-profile summary (the top blocking
// stacks, aggregated, frames and allowlisted labels only), at most one per
// process per minute and capped in size, and logs it with the in-flight slow
// requests' open phases and waits.
//
// PRIVACY. No identity and no raw key ever leaves this package: a waited-on
// key is rendered through redact (a per-process keyed digest), routes are
// ServeMux patterns, and the goroutine summary carries function names, file
// basenames, line numbers and allowlisted pprof labels, never arguments.
//
// COST. Off the slow path a tracked request costs one allocation (its Record,
// which is also the context the handler sees), two registry lock round-trips
// and the clock reads of its phases; an untracked context costs one
// ctx.Value lookup per Start. BenchmarkS1054_* measure it.
package slowcall

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The page-latency goal (#1054, #710's bar): a page loads in p95 ≤ 3 s and
// never over 10 s.
const (
	PageP95Goal = 3 * time.Second
	PageMaxGoal = 10 * time.Second
)

// Bound is the server time past which one request is a SLOW CALL.
//
// Derivation: a page cannot finish before its slowest call, so a single call
// whose server time exceeds the page p95 goal makes its page miss the p95 goal
// on its own, whatever its other calls do. PageP95Goal is therefore the
// smallest per-call time that is by itself a goal breach. Calls under it can
// still add up to a slow page, but then no single call is the cause and a
// per-call record would name nothing. It is the goal, not a tunable: it moves
// only if the goal does.
const Bound = PageP95Goal

// Phase is where a request spends time.
type Phase uint8

const (
	// PhaseAuth: JWT validation and the caller's clientconfig lookup
	// (middleware.UserConfig), including its apiserver re-read on a miss.
	PhaseAuth Phase = iota
	// PhaseObjectFetch: reading the dispatched CR (RESTAction / widget).
	PhaseObjectFetch
	// PhaseRBACCheck: the dispatch RBAC gate against the in-memory snapshot.
	PhaseRBACCheck
	// PhaseKeyMint: deriving the L1 key (the requester's binding set).
	PhaseKeyMint
	// PhaseL1Get: the L1 store read of the response cell (the store's single
	// mutex and a map read: time here is that lock).
	PhaseL1Get
	// PhaseContentRead: reading an identity-free content cell (the store
	// lock, an incremental list's snapshot drain, the stale-debt registry).
	PhaseContentRead
	// PhaseFlightWait: blocked on ANOTHER request's in-flight work of the
	// same key (single-flight / collapse). A wait span: carries the key.
	PhaseFlightWait
	// PhaseLockWait: blocked on a named lock another goroutine holds. A wait
	// span: carries the lock's key.
	PhaseLockWait
	// PhaseFillAdmission: a content fill's memory admission.
	PhaseFillAdmission
	// PhaseStageDial: one stage round-trip on the network (apiserver GET /
	// LIST page, discovery, external endpoint). Its count is the dial count.
	PhaseStageDial
	// PhaseInformerDispatch: a stage call served (or refused) from an
	// informer store.
	PhaseInformerDispatch
	// PhaseInformerSyncWait: waiting for an informer's initial sync.
	PhaseInformerSyncWait
	// PhaseRefresherYield: a worker parked yielding to customer requests.
	// Background work, so a request records it only if its context reaches
	// a yield; the process-wide parked count is in every snapshot.
	PhaseRefresherYield
	numPhases
)

var phaseNames = [numPhases]string{
	"auth", "object_fetch", "rbac_check", "key_mint", "l1_get", "content_read",
	"flight_wait", "lock_wait", "fill_admission", "stage_dial",
	"informer_dispatch", "informer_sync_wait", "refresher_yield",
}

func (p Phase) String() string {
	if p < numPhases {
		return phaseNames[p]
	}
	return "unknown"
}

// Release is why a request, or one of its waits, stopped waiting.
type Release uint8

const (
	// ReleaseCompleted: the work it waited for finished.
	ReleaseCompleted Release = iota
	// ReleaseCancelled: its context was cancelled (the client went away).
	ReleaseCancelled
	// ReleaseTimeout: its context's deadline passed.
	ReleaseTimeout
	// ReleaseLeaderFailed: the waited-on work ended without a usable result
	// (a single-flight leader that fell through, failed or timed out).
	ReleaseLeaderFailed
	// ReleaseOpen: still waiting when observed.
	ReleaseOpen
	numReleases
)

var releaseNames = [numReleases]string{"completed", "ctx_cancelled", "timeout", "leader_failed", "open"}

func (c Release) String() string {
	if c < numReleases {
		return releaseNames[c]
	}
	return "unknown"
}

// CauseOf maps a context error to its Release: nil is completed.
func CauseOf(err error) Release {
	switch {
	case err == nil:
		return ReleaseCompleted
	case errors.Is(err, context.DeadlineExceeded):
		return ReleaseTimeout
	case errors.Is(err, context.Canceled):
		return ReleaseCancelled
	}
	return ReleaseCompleted
}

// maxOpenWaits is how many concurrently open waits one record tracks for the
// watchdog (a request's parallel stages may each wait). A wait past it is
// still timed; it is only not listed while open.
const maxOpenWaits = 4

type phaseAcc struct {
	ns, max, n, open atomic.Int64
}

type waitInfo struct {
	set   bool
	phase Phase
	kind  string
	key   string // raw: rendered only through redact, only when reported
	gvr   string
	since time.Time
	ns    int64
	cause Release
}

// Record is one request's phase accounting.
type Record struct {
	start  time.Time
	route  string // the ServeMux pattern ("GET /call")
	method string

	ph     [numPhases]phaseAcc
	closed atomic.Bool

	mu      sync.Mutex
	class   string
	longest waitInfo
	open    [maxOpenWaits]waitInfo

	// The monitor's in-flight list and watchdog marks, under monitor.mu.
	prev, next          *Record
	snapped, suppressed bool
}

// recordCtx is the context the handler sees: the request's own context, with
// the Record it carries in the same allocation.
type recordCtx struct {
	context.Context
	rec Record
}

type recordKey struct{}

func (c *recordCtx) Value(k any) any {
	if _, ok := k.(recordKey); ok {
		return &c.rec
	}
	return c.Context.Value(k)
}

func fromContext(ctx context.Context) *Record {
	if ctx == nil {
		return nil
	}
	r, _ := ctx.Value(recordKey{}).(*Record)
	return r
}

// Detach returns ctx without its request's Record, for work that outlives the
// request it was started from (a demand revalidation runs on a detached copy
// of an already-served request): its time is not that request's.
func Detach(ctx context.Context) context.Context {
	if fromContext(ctx) == nil {
		return ctx
	}
	return context.WithValue(ctx, recordKey{}, (*Record)(nil))
}

// NoteClass records the request's work class (customer, or a background
// unit's class across a self-loopback) for its slow-call record.
func NoteClass(ctx context.Context, class string) {
	if r := fromContext(ctx); r != nil {
		r.mu.Lock()
		r.class = class
		r.mu.Unlock()
	}
}

// Span is one timed phase of one request. The zero Span (no Record on the
// context) is a no-op.
type Span struct {
	r     *Record
	t0    time.Time
	kind  string
	key   string
	gvr   string
	p     Phase
	slot  int8
	wait  bool
	yield bool
}

// Start opens a phase on ctx's request. End it exactly once.
func Start(ctx context.Context, p Phase) Span {
	r := fromContext(ctx)
	if r == nil || r.closed.Load() {
		return Span{}
	}
	r.ph[p].open.Add(1)
	return Span{r: r, p: p, t0: time.Now(), slot: -1}
}

// StartWait opens a wait on work another goroutine owns: kind names the
// mechanism ("content_flight", "discovery_group", ...), key the waited-on
// key (never logged: only its redact digest is), gvr the resource type when
// the key has one. End it with EndRelease.
func StartWait(ctx context.Context, p Phase, kind, key, gvr string) Span {
	s := Start(ctx, p)
	if s.r == nil {
		return s
	}
	s.wait, s.kind, s.key, s.gvr = true, kind, key, gvr
	s.slot = s.r.openWait(&s)
	return s
}

// End closes a phase whose work completed.
func (s Span) End() { s.EndRelease(ReleaseCompleted) }

// EndRelease closes a phase, recording why a wait was released.
func (s Span) EndRelease(cause Release) {
	if s.yield {
		yieldParked.Add(-1)
	}
	r := s.r
	if r == nil {
		return
	}
	d := int64(time.Since(s.t0))
	a := &r.ph[s.p]
	a.open.Add(-1)
	a.ns.Add(d)
	a.n.Add(1)
	storeMax(&a.max, d)
	if s.wait {
		r.closeWait(&s, d, cause)
	}
}

// yieldParked is the process-wide count of workers parked in a customer
// yield now (refresher and prewarm engine).
var yieldParked atomic.Int64

// BeginYield marks a worker parked yielding to customer requests: the
// process-wide parked count, and PhaseRefresherYield on ctx's request if it
// has one. End it once the worker resumes.
func BeginYield(ctx context.Context) Span {
	yieldParked.Add(1)
	s := Start(ctx, PhaseRefresherYield)
	s.yield = true
	return s
}

func (r *Record) openWait(s *Span) int8 {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.open {
		if !r.open[i].set {
			r.open[i] = waitInfo{set: true, phase: s.p, kind: s.kind, key: s.key, gvr: s.gvr, since: s.t0}
			return int8(i)
		}
	}
	return -1
}

func (r *Record) closeWait(s *Span, d int64, cause Release) {
	r.mu.Lock()
	if s.slot >= 0 {
		r.open[s.slot] = waitInfo{}
	}
	if !r.longest.set || d > r.longest.ns {
		r.longest = waitInfo{set: true, phase: s.p, kind: s.kind, key: s.key, gvr: s.gvr, since: s.t0, ns: d, cause: cause}
	}
	r.mu.Unlock()
}

func storeMax(m *atomic.Int64, v int64) {
	for {
		cur := m.Load()
		if v <= cur || m.CompareAndSwap(cur, v) {
			return
		}
	}
}

// routePath is the path half of a ServeMux pattern ("GET /call" → "/call").
func routePath(pattern string) string {
	if i := strings.IndexByte(pattern, '/'); i >= 0 {
		return pattern[i:]
	}
	return ""
}

// untrackedRoutes are request paths that are not request/response API calls:
// the SSE stream (open for minutes by design), the kubelet probes and the
// browser RUM upload (tracing.unspannedRoutes' set). The /debug/ subtree and
// /swagger/ are excluded below; a request no pattern serves has no route.
var untrackedRoutes = map[string]bool{
	"/refreshes": true, "/health": true, "/readyz": true, "/telemetry/v1/traces": true,
}

func tracked(path string) bool {
	return path != "" && !untrackedRoutes[path] &&
		!strings.HasPrefix(path, "/debug/") && !strings.HasPrefix(path, "/swagger/")
}

// Middleware is the process monitor's request recorder. It must run where
// r.Pattern already names the route (inside tracing.NewServerHandler, which
// resolves it before the mux does).
func Middleware(next http.Handler) http.Handler {
	m := defaultMonitor()
	m.startWatchdog()
	return m.middleware(next)
}

func (m *monitor) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !tracked(routePath(r.Pattern)) {
			next.ServeHTTP(w, r)
			return
		}
		rc := &recordCtx{Context: r.Context()}
		rc.rec.start = time.Now()
		rc.rec.route = r.Pattern
		rc.rec.method = r.Method
		m.add(&rc.rec)
		defer m.finish(r.Context(), &rc.rec)
		next.ServeHTTP(w, r.WithContext(rc))
	})
}

// finish ends a request: it leaves the in-flight list and, over the bound,
// emits its record. ctx is the request's own context (its trace correlation).
func (m *monitor) finish(ctx context.Context, rec *Record) {
	end := time.Now()
	m.remove(rec)
	rec.closed.Store(true)
	wall := end.Sub(rec.start)
	if wall <= m.bound {
		return
	}
	m.emit(ctx, rec, wall, end, CauseOf(ctx.Err()))
}
