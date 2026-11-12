// route.go — an audited route's one record (AUDIT-4 installer#410; #1379
// review F2). Which routes are audited, and as what, is declared once, where
// the route is registered (package main's routeTable.HandleAudited, with the
// route's descriptor, Route). Registration wraps the route OUTERMOST in
// OpenRecord, before any middleware decodes a body or verifies a token, so
// every exit of an audited route emits exactly one record:
//
//   - the handler emits it, through EmitRoute, with what the authority
//     answered (a /call write, an export);
//   - a refusal by snowplow before anything was sent (a 4xx exit with no
//     handler record: a body the decoder refuses, a malformed query) is
//     recorded by OpenRecord when the request ends: what the request asked
//     for, denied, reason invalid_request, no status code. What it asked for
//     is the descriptor the handler stored once it parsed the request
//     (SetAsked), else the route's descriptor of the request's query alone
//     (Route.Asked); no body is read a second time;
//   - any other exit with no handler record (a 2xx, 3xx or 5xx answer, or a
//     panic) is a DESIGN ERROR of the handler: every such exit must record
//     its own outcome. It is still recorded, as a failure (the outcome is
//     required and cannot be marked) with its own fixed body, and counted
//     (DesignErrorVar); a panic is recovered, recorded as one, and re-raised;
//   - a request that never got a verified caller (an authentication failure
//     before verification) gets none: those are counted, never recorded
//     (coverage.md, Exclusions). The caller is the one the login middleware
//     verified (SetCaller).
package audit

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

// Route is the audit descriptor of one audited route.
type Route struct {
	// Asked is the record of what req asked for, from its method, path and
	// query alone (never its body), by the route handler's own rules: the
	// action, the method and verb of the authority request it would send,
	// and the target when the query names one the handler would dial (none
	// otherwise: the shared writer then marks the record target). The
	// emitter adds the outcome. It describes an exit before the handler
	// parsed the request; after that, the handler's own descriptor
	// (SetAsked) is used.
	Asked func(req *http.Request) Event
}

// MessageRequestRefused is the body of a record OpenRecord writes for a
// request snowplow refused before sending anything: the rule, never a value of
// the request.
const MessageRequestRefused = "refused: snowplow did not accept the request; nothing was sent"

// MessageUnrecordedAnswer is the body of a record OpenRecord writes for an
// exit the handler answered without recording its outcome: a design error,
// never a description of what was or was not sent.
const MessageUnrecordedAnswer = "the handler answered without recording its outcome"

// MessagePanic is the body of a record OpenRecord writes for a handler that
// panicked before recording its outcome.
const MessagePanic = "panic: the handler panicked before recording its outcome"

type slotKey struct{}

// recordSlot is one audited request's record: whether it was emitted, and what
// the request had established when it ended.
type recordSlot struct {
	route   Route
	emitted atomic.Bool

	mu     sync.Mutex
	base   context.Context   // the request context as Middleware established it
	caller *jwtutil.UserInfo // the verified caller (SetCaller)
	asked  *Event            // the handler's parsed descriptor (SetAsked)
}

// OpenRecord is the outermost middleware of an audited route (see the file
// comment).
func OpenRecord(route Route) func(http.Handler) http.Handler {
	if route.Asked == nil {
		panic("audit.OpenRecord: a route's descriptor needs Asked")
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			slot := &recordSlot{route: route, base: req.Context()}
			sw := &statusWriter{ResponseWriter: w}
			defer func() {
				if p := recover(); p != nil {
					slot.close(req, sw, true)
					panic(p)
				}
				slot.close(req, sw, false)
			}()
			next.ServeHTTP(sw, req.WithContext(context.WithValue(req.Context(), slotKey{}, slot)))
		})
	}
}

// EmitRoute emits the audited route's record: the one OpenRecord then does not
// write. Any other record a request causes (a RESTAction stage's) is Emit's.
func EmitRoute(ctx context.Context, ev Event) {
	if s, ok := ctx.Value(slotKey{}).(*recordSlot); ok {
		s.emitted.Store(true)
	}
	Emit(ctx, ev)
}

// SetCaller records on ctx's audited request that its caller is verified as
// ui. Called by the middleware that verified it.
func SetCaller(ctx context.Context, ui jwtutil.UserInfo) {
	if s, ok := ctx.Value(slotKey{}).(*recordSlot); ok {
		s.mu.Lock()
		s.caller = &ui
		s.mu.Unlock()
	}
}

// SetAsked records on ctx's audited request the handler's own descriptor of
// what it asked for, once the handler parsed it: OpenRecord describes a later
// exit with no record by it, never by parsing the request again.
func SetAsked(ctx context.Context, ev Event) {
	if s, ok := ctx.Value(slotKey{}).(*recordSlot); ok {
		s.mu.Lock()
		s.asked = &ev
		s.mu.Unlock()
	}
}

// established records the request context Middleware established (its session
// id, what it was received with).
func established(ctx context.Context) {
	if s, ok := ctx.Value(slotKey{}).(*recordSlot); ok {
		s.mu.Lock()
		s.base = ctx
		s.mu.Unlock()
	}
}

func (s *recordSlot) close(req *http.Request, sw *statusWriter, panicked bool) {
	if s.emitted.Load() {
		return
	}
	s.mu.Lock()
	base, caller, asked := s.base, s.caller, s.asked
	s.mu.Unlock()
	if caller == nil {
		return // never verified: counted, never recorded
	}
	ctx := xcontext.BuildContext(base, xcontext.WithUserInfo(*caller))
	var ev Event
	if asked != nil {
		ev = *asked
	} else {
		ev = s.route.Asked(req.WithContext(ctx))
	}
	status := sw.status
	if status == 0 {
		status = http.StatusOK // net/http's answer to a handler that never set one
	}
	switch {
	case panicked:
		designErrors.Add("panic", 1)
		ev.Outcome, ev.DenialReason, ev.Message = OutcomeFailure, "", MessagePanic
	case status >= 400 && status < 500:
		ev.Outcome, ev.DenialReason, ev.Message = OutcomeDenied, DenialInvalidRequest, MessageRequestRefused
	default:
		designErrors.Add("answered", 1)
		ev.Outcome, ev.DenialReason, ev.Message = OutcomeFailure, "", MessageUnrecordedAnswer
	}
	ev.StatusCode = 0 // no authority's answer is known
	Emit(ctx, ev)
}

// statusWriter keeps the status the response was answered with, read from
// WriteHeader only. It never wraps Write: the body goes to the original
// writer untouched (no second write sink). A handler that writes a body
// without calling WriteHeader answered 200, as net/http answers it, so
// close reads a status never set as 200.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the wrapped writer.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
