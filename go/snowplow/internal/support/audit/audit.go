// Package audit is snowplow's side of the canonical Krateo AuditEvent: what
// snowplow knows about the request a record is made on, handed to the shared
// writer, github.com/krateo-platformops/audit (schema v1, #1291). The shared
// writer builds the OTLP LogRecord, marks and counts its gaps, sanitises every
// string and writes the v0.3 keys beside their replacements; this package
// supplies, for every emit site alike, what only snowplow knows:
//
//   - the actor, from the validated JWT on the request context: user.name is
//     the token's actor (its outermost act subject on a delegated token), the
//     actor class is its signed `principal` claim through audit.ParseClaim,
//     omitted (and so marked) when the claim is absent or unknown, never
//     defaulted; a delegated token adds on_behalf_of and the act chain;
//   - what the request was received with (request.go) and its session id;
//   - the preview flag of a Kubernetes target, read from apiserver state
//     (preview.go);
//   - the dry-run suffix of krateo.action, snowplow's own literal.
//
// It is TRACE-CORRELATED: when a valid span is on ctx (snowplow wraps every
// HTTP request in an otelhttp server span) the Logs SDK stamps
// TraceId/SpanId onto the record, joining otel_logs.idx_trace_id to the
// traces/logs the action caused. That join is the whole point — an audit
// id-space parallel to `traceparent`/`trace_id` would be un-joinable in the
// very index the stack is built on.
//
// The cross-request business/session correlation id rides W3C BAGGAGE
// (`session.id`), NEVER a bespoke X-Krateo-Correlation-Id header. Snowplow
// is the id origin: the middleware mints/accepts the id and writes it into
// baggage; the global propagation.Baggage propagator (installed by
// tracing.Setup) then serializes it into the outbound `baggage` header for
// every downstream adapter call, so an adapter can group its own audit
// events with snowplow's across trace boundaries.
//
// COEXISTENCE CONTRACT: the shortid `X-Krateo-TraceId` (plumbing
// xcontext.TraceId) and the OTel W3C `traceparent` are UNTOUCHED. This
// package no longer defines or forwards any bespoke correlation header; the
// session id lives only in baggage.
//
// IDENTITY. A record carries identity on purpose (the actor, the request URI,
// the source IPs). It goes to the audit LoggerProvider and nowhere else: never
// to a span (#848; the only span attribute the shared writer sets is the
// boolean krateo.audit.recorded) and never to slog (#1192).
package audit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"sync/atomic"
	"time"

	krateoaudit "github.com/krateo-platformops/audit"
	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/log"
)

const (
	// BaggageSessionKey is the W3C baggage member carrying the business/
	// session correlation id. The edge (portal) may seed it; snowplow mints
	// it when absent. It REPLACES the old X-Krateo-Correlation-Id header
	// entirely — the `session.id` semconv attribute on each audit record is
	// read from here (by the shared writer).
	BaggageSessionKey = krateoaudit.BaggageSessionKey

	// EventName is the discriminator stamped on every audit LogRecord
	// (event.name="audit") — the constant every audit row is filtered on.
	EventName = krateoaudit.EventName

	// Scope is the instrumentation scope of snowplow's records: the
	// population the shared schema maps to the request writer.
	Scope = "github.com/krateo-platformops/snowplow/audit"

	// Writer is snowplow's writer class (schema v1 "request", H): it serves a
	// person's or an agent's request. An agent's /call record is also an
	// action-writer (T) record of the same population; it conforms to both.
	Writer = krateoaudit.WriterRequest

	// DryRunSuffix is appended to krateo.action for a dry-run write
	// (Event.DryRun): "call.dryRun", "restaction.dryRun". It is snowplow's own
	// literal; readers use krateo.audit.dry_run, which the shared writer sets
	// on every write record.
	DryRunSuffix = ".dryRun"
)

// SanitizeID validates an inbound session id; it returns "" when the id is not
// of the schema's session_id format (audit.ValidSessionID: 1 to 128 of
// [A-Za-z0-9._-]), so a hostile caller cannot inject baggage or pipeline
// control characters. The shared writer omits any other shape too; this is
// where snowplow, the id's origin, refuses it and mints its own.
func SanitizeID(id string) string {
	if !krateoaudit.ValidSessionID(id) {
		return ""
	}
	return id
}

// NewID mints a random session id (16 hex chars).
func NewID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return hex.EncodeToString([]byte(time.Now().Format("150405.000")))
	}
	return hex.EncodeToString(b[:])
}

// SessionID returns the session correlation id carried in ctx's W3C
// baggage, or "" when none is present.
func SessionID(ctx context.Context) string {
	return baggage.FromContext(ctx).Member(BaggageSessionKey).Value()
}

// WithSessionID returns a ctx whose W3C baggage carries the given session
// id as the `session.id` member. Best-effort: if id is not baggage-safe the
// original ctx is returned unchanged. The installed propagation.Baggage
// propagator serializes this into the outbound `baggage` header on every
// call made with the returned ctx.
func WithSessionID(ctx context.Context, id string) context.Context {
	member, err := baggage.NewMember(BaggageSessionKey, id)
	if err != nil {
		return ctx
	}
	bag, err := baggage.FromContext(ctx).SetMember(member)
	if err != nil {
		return ctx
	}
	return baggage.ContextWithBaggage(ctx, bag)
}

// Middleware resolves the request session id (inbound baggage -> shortid
// request trace id -> minted) and writes it into ctx's W3C baggage as
// `session.id`, so it (a) is stamped on every AuditEvent and (b) propagates
// downstream via the Baggage propagator. Snowplow is the id origin. It also
// captures on ctx the raw values the request was received with (peer address,
// forwarding headers, User-Agent, URL; #1251, request.go), from which Emit
// derives the source IPs, User-Agent and request URI of every record. It is
// additive-only: no request is ever rejected here.
func Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(wri http.ResponseWriter, req *http.Request) {
			ctx := req.Context()
			// If the propagator already extracted a session.id from an
			// inbound `baggage` header, keep it; otherwise mint one.
			id := SanitizeID(SessionID(ctx))
			if id == "" {
				id = xcontext.TraceId(ctx, false)
			}
			if id == "" {
				id = NewID()
			}
			ctx = WithSessionID(ctx, id)
			ctx = withReceivedRequest(ctx, req)
			// An audited route's record (OpenRecord) is written on this
			// context when the request ends without one.
			established(ctx)
			next.ServeHTTP(wri, req.WithContext(ctx))
		})
	}
}

// Event is the shared writer's Event: one emit site's record, in the schema's
// own terms. An emit site sets what it did (Action, Verb, HTTPMethod, Outcome,
// StatusCode, Message, DryRun), and to which target: a Kubernetes object
// (Group, Version, Resource, Subresource, ObjectName, Namespace and, for a
// successful write, the written object's identity, RecordWrittenObject) or an
// external endpoint (ServerAddress), never both. Emit fills in everything the
// request context holds; whatever an emit site puts in those fields is
// replaced.
type Event = krateoaudit.Event

// The outcomes (krateo.audit.outcome).
const (
	OutcomeSuccess = krateoaudit.OutcomeSuccess
	OutcomeFailure = krateoaudit.OutcomeFailure
	// OutcomeDenied: snowplow itself refused, so no authority answered and
	// the record carries no status code (schema v1; audit#13).
	OutcomeDenied = krateoaudit.OutcomeDenied
)

// Emitter is snowplow's emitter: the shared writer, declared as the request
// writer, behind the enrichment every snowplow record gets (Emit). Construct
// once at startup from the process LoggerProvider and install it via
// SetDefault. A nil Emitter (pipeline disabled / not installed) is a no-op.
type Emitter struct {
	lib *krateoaudit.Emitter
}

// New returns an Emitter bound to logger: the shared writer, declared as
// snowplow's writer class, so it marks every gap that class must mark, writes
// krateo.audit.dry_run on every write record, and counts each marker under
// krateo.audit.records.incomplete on the global MeterProvider (OTLP, when
// metrics are on). opts are the shared writer's (a test passes its own
// MeterProvider). Obtain logger from the LoggerProvider via
// provider.Logger(Scope). A nil logger is a no-op Emitter.
func New(logger log.Logger, opts ...krateoaudit.Option) *Emitter {
	if logger == nil {
		return &Emitter{}
	}
	all := append([]krateoaudit.Option{krateoaudit.WithWriter(Writer)}, opts...)
	return &Emitter{lib: krateoaudit.New(incompleteMirror{Logger: logger}, all...)}
}

// defaultEmitter is the process-wide audit emitter installed from main once
// the Logs pipeline is up. When nil (default-off / disabled), the package
// Emit function is a no-op — preserving the byte-identical off-path.
var defaultEmitter atomic.Pointer[Emitter]

// SetDefault installs e as the process-wide emitter used by the package-level
// Emit. Called once from main after logging.Setup; pass nil (or skip) when
// the pipeline is disabled to keep Emit a no-op.
func SetDefault(e *Emitter) { defaultEmitter.Store(e) }

// Emit writes one canonical AuditEvent via the process-wide emitter. It is a
// no-op when no emitter is installed (audit-log pipeline disabled).
func Emit(ctx context.Context, ev Event) {
	if e := defaultEmitter.Load(); e != nil {
		e.Emit(ctx, ev)
	}
}

// Emit writes one canonical AuditEvent. ctx MUST carry the active span (so the
// SDK stamps TraceId/SpanId), the baggage (so session.id is captured) and the
// request's validated user info. A nil Emitter is a no-op.
//
// AUDIT-4 (installer#410) completeness is the shared writer's, for every emit
// site: a record is never dropped. One that lacks a key snowplow's writer
// class must write (the actor, its class, the action, the verb, the method,
// the preview flag, a target) is emitted without it, marked
// krateo.audit.incomplete=<markers>, and each marker is counted
// (krateo.audit.records.incomplete{marker}, OTLP and /debug/vars).
func (e *Emitter) Emit(ctx context.Context, ev Event) {
	if e == nil || e.lib == nil {
		return
	}
	setActor(ctx, &ev)
	if ev.DryRun && ev.Action != "" {
		ev.Action += DryRunSuffix
	}
	ev.SourceIPs, ev.UserAgent, ev.RequestURI = nil, "", ""
	if f, ok := requestFactsFrom(ctx); ok {
		ev.SourceIPs, ev.UserAgent, ev.RequestURI = f.sourceIPs, f.userAgent, f.requestURI
	}
	if ev.Resource != "" {
		ev.Preview = previewOf(ctx, ev.Namespace)
	}
	e.lib.Emit(ctx, ev)
}

// setActor sets ev's actor from the request's validated user info, and only
// from it (the UserConfig middleware puts it there after verifying the JWT):
//
//   - user.name is the principal performing the action, the token's Actor():
//     its outermost act subject on a delegated token, else its subject;
//   - enduser.id, the v0.3 key snowplow has always written, carries the same
//     value (the shared writer requires the two to agree during W1);
//   - user.roles is the token's groups;
//   - krateo.audit.actor_class is that principal's signed class
//     (ActorPrincipal(), through audit.ParseClaim). A token with no
//     `principal` claim, or one this schema does not know, gets none: the
//     record is emitted marked actor_class, never defaulted to human;
//   - a delegated token adds on_behalf_of (the subject, the person) and the
//     signed act chain, outermost actor first and ending with the subject.
//
// No user info on ctx: the record names no actor and is marked actor.
func setActor(ctx context.Context, ev *Event) {
	ev.UserName, ev.EndUserID, ev.UserRoles, ev.ActorClass = "", "", nil, ""
	ev.OnBehalfOf, ev.Delegation = "", nil
	ui, err := xcontext.UserInfo(ctx)
	if err != nil {
		return
	}
	ev.UserName = ui.Actor()
	ev.EndUserID = ev.UserName
	ev.UserRoles = ui.Groups
	if c, ok := krateoaudit.ParseClaim(ui.ActorPrincipal()); ok {
		ev.ActorClass = c
	}
	if ui.Act != nil {
		ev.OnBehalfOf = ui.Username
		ev.Delegation = delegationOf(ui)
	}
}

// delegationOf is a delegated token's signed act chain as the schema writes
// it: each actor, outermost first, then the subject, each with its signed
// class ("" when the token does not say).
func delegationOf(ui jwtutil.UserInfo) []krateoaudit.Link {
	var chain []krateoaudit.Link
	for a := ui.Act; a != nil; a = a.Act {
		c, _ := krateoaudit.ParseClaim(a.Principal)
		chain = append(chain, krateoaudit.Link{Class: c, Subject: a.Sub})
	}
	c, _ := krateoaudit.ParseClaim(ui.Principal)
	return append(chain, krateoaudit.Link{Class: c, Subject: ui.Username})
}
