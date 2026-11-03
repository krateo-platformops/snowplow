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
// marks the time it spends in each Phase with Start/End, a wait on work
// another goroutine owns with StartWait/EndRelease, and a network round-trip
// with StartDial (whose attempts, statuses and retry sleeps the Transport
// records). When the request ends, its wall time is compared with Bound. Over
// it, ONE structured WARN line ("slowcall.request") names the route, the work
// class, the wall time, every phase it entered, the longest wait (a dial is a
// wait candidate), the longest dial, the flight it led, the CPU starvation
// over its lifetime (cgroup throttling, process CPU) and its release cause.
// snowplow_slow_requests_total{route,class} and the phase histogram count it.
// Under the bound nothing is logged or counted.
//
// PHASES OVERLAP BY DESIGN. A phase is the summed time the request's
// goroutines spent in it, so parallel stages can sum past the wall time, and
// a phase may contain another (a stage dial can contain an informer sync
// wait, a refilter its JQ). The record reports each phase as measured; what
// no phase covers is reported as unattributed_ms, never folded into a phase.
//
// THE SNAPSHOT (snapshot.go). While a CUSTOMER request is in flight past the
// bound, a watchdog captures one goroutine-profile summary (the top blocking
// stacks, aggregated, frames and allowlisted labels only), at most one per
// process per minute and capped in size, and logs it with every in-flight
// slow request's open phases and waits and the CPU starvation since the
// oldest one began.
//
// PRIVACY. No identity and no raw key ever leaves this package: a waited-on
// key is rendered through redact (a per-process keyed digest), routes are
// ServeMux patterns, a dial names a verb and a GVR (never a path or a host),
// and the goroutine summary carries function names, file basenames, line
// numbers and allowlisted pprof labels, never arguments.
//
// COST. Off the slow path a tracked request costs one allocation (its Record,
// which is also the context and the response writer the handler sees) plus
// net/http's request copy, one registry shard lock round-trip at each end, and
// the clock reads of its phases; a span on a context without a record costs
// one ctx.Value lookup. BenchmarkS1054_* measure it.
package slowcall

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/workclass"
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
	// same key (single-flight / collapse). A wait: carries the key and, for a
	// content flight, the leader's class.
	PhaseFlightWait
	// PhaseLockWait: blocked on a named lock another goroutine holds. A wait.
	PhaseLockWait
	// PhaseFillAdmission: a content fill's memory admission.
	PhaseFillAdmission
	// PhaseStageDial: one network call (apiserver GET / LIST page, discovery,
	// an external endpoint, an access review), all its attempts and the
	// sleeps between them. A wait candidate (StartDial).
	PhaseStageDial
	// PhaseInformerDispatch: a stage call served (or refused) from an
	// informer store.
	PhaseInformerDispatch
	// PhaseInformerSyncWait: waiting for an informer's initial sync. A wait.
	PhaseInformerSyncWait
	// PhaseRefresherYield: a worker parked yielding to customer requests.
	// Background work, so a request records it only if its context reaches
	// a yield; the process-wide parked count is in every snapshot.
	PhaseRefresherYield
	// PhaseJQ: JQ evaluation (stage filters, the RESTAction filter, widget
	// templates).
	PhaseJQ
	// PhaseRefilter: the per-user re-filter of a served list (its JQ
	// resource selection included).
	PhaseRefilter
	// PhaseParse: decoding a stage body and parsing a list envelope.
	PhaseParse
	// PhaseEncode: encoding the response body.
	PhaseEncode
	// PhaseCompress: gzip of the response, net of the network writes it
	// makes.
	PhaseCompress
	// PhaseWrite: writing response bytes to the connection (a slow reader
	// shows here).
	PhaseWrite
	numPhases
)

var phaseNames = [numPhases]string{
	"auth", "object_fetch", "rbac_check", "key_mint", "l1_get", "content_read",
	"flight_wait", "lock_wait", "fill_admission", "stage_dial",
	"informer_dispatch", "informer_sync_wait", "refresher_yield",
	"jq", "refilter", "parse", "encode", "compress", "write",
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
	// ReleaseTimeout: its context's deadline passed, or a bounded wait ran
	// out.
	ReleaseTimeout
	// ReleaseLeaderFailed: the waited-on work ended without a usable result
	// (a single-flight leader that fell through, failed or timed out).
	ReleaseLeaderFailed
	// ReleaseFailed: the work itself failed (a dial with a transport error,
	// a review that errored).
	ReleaseFailed
	// ReleaseOpen: still waiting when observed.
	ReleaseOpen
	numReleases
)

var releaseNames = [numReleases]string{"completed", "ctx_cancelled", "timeout", "leader_failed", "failed", "open"}

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

// waitInfo is one wait (or dial, or lead) as a record reports it.
type waitInfo struct {
	set   bool
	phase Phase
	kind  string
	key   string // raw: rendered only through redact, only when reported
	gvr   string
	verb  string
	// branch is a dial's dispatch branch (Branch*).
	branch string
	since  time.Time
	ns     int64
	cause  Release
	// leaderClass / leaderWork: a content flight's leader (its admission
	// class and its work class), so a wait names WHOSE work it waited on.
	leaderClass, leaderWork string
	dial                    *dialState // a dial's attempts, or nil
}

// Record is one request's phase accounting.
type Record struct {
	start  time.Time
	route  string // the ServeMux pattern ("GET /call")
	method string

	ph     [numPhases]phaseAcc
	closed atomic.Bool
	dials  dialTotals

	mu      sync.Mutex
	class   workclass.Class
	longest waitInfo // the longest wait, a dial included
	dial    waitInfo // the longest dial
	lead    waitInfo // the longest flight this request led
	leads   int
	open    [maxOpenWaits]waitInfo
	openLd  waitInfo // the flight this request leads now

	// Response-path accounting (writer.go), single-goroutine by net/http.
	handlerEnd time.Time
	netAtEnd   int64
	bodyRan    bool

	// Registry and watchdog state (monitor.go, snapshot.go).
	shard               *shard
	prev, next          *Record
	snapped, suppressed bool // under monitor.watchMu
}

// recordCtx is the one allocation a tracked request makes: the context the
// handler sees, its Record, and the two response writers that time the
// response path.
type recordCtx struct {
	context.Context
	rec   Record
	net   netWriter
	inner bodyWriter
}

type recordKey struct{}

func (c *recordCtx) Value(k any) any {
	switch k.(type) {
	case recordKey:
		return &c.rec
	case recordCtxKey:
		return c
	}
	return c.Context.Value(k)
}

type recordCtxKey struct{}

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
	return context.WithValue(context.WithValue(ctx, recordKey{}, (*Record)(nil)), recordCtxKey{}, (*recordCtx)(nil))
}

// NoteClass records the request's work class: Customer, or a background
// unit's class across a trusted self-loopback. A background request is
// counted under its own class and never triggers a snapshot.
func NoteClass(ctx context.Context, c workclass.Class) {
	if r := fromContext(ctx); r != nil {
		r.mu.Lock()
		r.class = c
		r.mu.Unlock()
	}
}

// workClass is the request's class: Customer unless a dispatcher said
// otherwise (only the resolve dispatchers serve background loopbacks).
func (r *Record) workClass() workclass.Class {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.class == workclass.None {
		return workclass.Customer
	}
	return r.class
}

// Span is one timed phase of one request. The zero Span (no Record on the
// context) is a no-op.
type Span struct {
	r      *Record
	t0     time.Time
	kind   string
	key    string
	gvr    string
	verb   string
	branch string
	lc     string
	lw     string
	dial   *dialState
	p      Phase
	slot   int8
	wait   bool
	yield  bool
	lead   bool
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
	return StartLedWait(ctx, p, kind, key, gvr, "", "")
}

// StartLedWait is StartWait for a wait on a single-flight whose leader is
// known: leaderClass is the leader's admission class, leaderWork its work
// class.
func StartLedWait(ctx context.Context, p Phase, kind, key, gvr, leaderClass, leaderWork string) Span {
	s := Start(ctx, p)
	if s.r == nil {
		return s
	}
	s.wait, s.kind, s.key, s.gvr, s.lc, s.lw = true, kind, key, gvr, leaderClass, leaderWork
	s.slot = s.r.openWait(&s)
	return s
}

// StartLead marks the request as the LEADER of a single-flight other readers
// may wait on: the record carries the same key digest a waiter's record
// names, so the two records join. A lead is not a phase (its time is the
// phases of the work it does). End it when the flight completes.
func StartLead(ctx context.Context, kind, key, gvr string) Span {
	r := fromContext(ctx)
	if r == nil || r.closed.Load() {
		return Span{}
	}
	s := Span{r: r, t0: time.Now(), slot: -1, lead: true, kind: kind, key: key, gvr: gvr}
	r.mu.Lock()
	r.openLd = waitInfo{set: true, kind: kind, key: key, gvr: gvr, since: s.t0}
	r.mu.Unlock()
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
	if s.lead {
		r.closeLead(&s, d)
		return
	}
	a := &r.ph[s.p]
	a.open.Add(-1)
	a.ns.Add(d)
	a.n.Add(1)
	storeMax(&a.max, d)
	if s.wait {
		r.closeWait(&s, d, cause)
	}
}

// Discard closes a span without recording it: the work it was opened for
// turned out not to be a wait (a single-flight caller that led instead).
func (s Span) Discard() {
	r := s.r
	if r == nil {
		return
	}
	r.ph[s.p].open.Add(-1)
	if s.slot >= 0 {
		r.mu.Lock()
		r.open[s.slot] = waitInfo{}
		r.mu.Unlock()
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

func (s *Span) info(d int64, cause Release) waitInfo {
	return waitInfo{set: true, phase: s.p, kind: s.kind, key: s.key, gvr: s.gvr, verb: s.verb, branch: s.branch,
		since: s.t0, ns: d, cause: cause, leaderClass: s.lc, leaderWork: s.lw, dial: s.dial}
}

func (r *Record) openWait(s *Span) int8 {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.open {
		if !r.open[i].set {
			r.open[i] = s.info(0, ReleaseOpen)
			return int8(i)
		}
	}
	return -1
}

func (r *Record) closeWait(s *Span, d int64, cause Release) {
	w := s.info(d, cause)
	r.mu.Lock()
	if s.slot >= 0 {
		r.open[s.slot] = waitInfo{}
	}
	if !r.longest.set || d > r.longest.ns {
		r.longest = w
	}
	if s.dial != nil && (!r.dial.set || d > r.dial.ns) {
		r.dial = w
	}
	r.mu.Unlock()
}

func (r *Record) closeLead(s *Span, d int64) {
	r.mu.Lock()
	r.leads++
	if !r.lead.set || d > r.lead.ns {
		r.lead = s.info(d, ReleaseCompleted)
	}
	if r.openLd.since.Equal(s.t0) {
		r.openLd = waitInfo{}
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
// resolves it before the mux does), and OUTSIDE the response compressor, so
// the writer it installs sees the bytes that reach the connection.
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
		rec := &rc.rec
		rec.start = m.now()
		rec.route = r.Pattern
		rec.method = r.Method
		rc.net = netWriter{ResponseWriter: w, r: rec}
		m.add(rec)
		defer m.finish(r.Context(), rec)
		next.ServeHTTP(&rc.net, r.WithContext(rc))
		rec.compressTail()
	})
}

// finish ends a request: it leaves the in-flight list and, over the bound,
// emits its record. ctx is the request's own context (its trace correlation).
func (m *monitor) finish(ctx context.Context, rec *Record) {
	end := m.now()
	m.remove(rec)
	rec.closed.Store(true)
	wall := end.Sub(rec.start)
	if wall <= m.bound {
		return
	}
	m.emit(ctx, rec, wall, end, CauseOf(ctx.Err()))
}
