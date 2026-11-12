// route.go — an audited route's one record (AUDIT-4 installer#410; #1379
// review F2). Which routes are audited, and as what, is declared once, where
// the route is registered (package main's routeTable.HandleAudited, with the
// route's descriptor, Route). Registration wraps the route OUTERMOST in
// OpenRecord, before any middleware decodes a body or verifies a token, so
// every exit of an audited route emits exactly one record:
//
//   - the handler emits it, through EmitRoute, with what the authority
//     answered (a /call write, an export);
//   - an exit that emitted none (a refusal before or inside the handler: a
//     body the decoder refuses, a malformed query) is recorded by OpenRecord
//     when the request ends: what the route's descriptor says the request
//     asked for (Asked), denied with reason invalid_request when snowplow
//     answered 4xx, a failure with no status code otherwise;
//   - a request that never got a verified caller (an authentication failure
//     before verification) gets none: those are counted, never recorded
//     (coverage.md, Exclusions). The caller is the one the UserConfig
//     middleware verified (SetCaller).
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
	// Asked is the record of what req asked for, by the route handler's own
	// rules, before anything happened: the action, the method and verb of
	// the authority request it would send, and the target when the request
	// names one the handler would dial (none otherwise: the shared writer
	// then marks the record target). The emitter adds the outcome.
	Asked func(req *http.Request) Event
}

// MessageRequestRefused is the body of a record OpenRecord writes for a
// request snowplow refused before sending anything: the rule, never a value of
// the request.
const MessageRequestRefused = "refused: snowplow did not accept the request; nothing was sent"

// MessageRequestEnded is the body of a record OpenRecord writes for a request
// that ended with no record and no refusal (a server error before the send).
const MessageRequestEnded = "the request ended before anything was sent"

type slotKey struct{}

// recordSlot is one audited request's record: whether it was emitted, and what
// the request had established when it ended.
type recordSlot struct {
	route   Route
	emitted atomic.Bool

	mu     sync.Mutex
	base   context.Context   // the request context as Middleware established it
	caller *jwtutil.UserInfo // the verified caller (SetCaller)
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
			defer slot.close(req, sw)
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

// established records the request context Middleware established (its session
// id, what it was received with).
func established(ctx context.Context) {
	if s, ok := ctx.Value(slotKey{}).(*recordSlot); ok {
		s.mu.Lock()
		s.base = ctx
		s.mu.Unlock()
	}
}

func (s *recordSlot) close(req *http.Request, sw *statusWriter) {
	if s.emitted.Load() {
		return
	}
	s.mu.Lock()
	base, caller := s.base, s.caller
	s.mu.Unlock()
	if caller == nil {
		return // never verified: counted, never recorded
	}
	ctx := xcontext.BuildContext(base, xcontext.WithUserInfo(*caller))
	ev := s.route.Asked(req.WithContext(ctx))
	status := sw.status
	if status == 0 {
		status = http.StatusOK
	}
	switch {
	case status >= 400 && status < 500:
		ev.Outcome, ev.DenialReason, ev.Message = OutcomeDenied, DenialInvalidRequest, MessageRequestRefused
	default:
		ev.Outcome, ev.Message = OutcomeFailure, MessageRequestEnded
	}
	ev.StatusCode = 0 // no authority answered
	Emit(ctx, ev)
}

// statusWriter keeps the status the response was answered with.
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

func (w *statusWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

// Unwrap lets http.ResponseController reach the wrapped writer.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
