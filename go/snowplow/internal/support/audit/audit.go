// Package audit emits the canonical Krateo AuditEvent as an OTLP LogRecord
// on the shared OTel Collector -> ClickHouse otel_logs plane (D19a).
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
package audit

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"regexp"
	"sync/atomic"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

const (
	// BaggageSessionKey is the W3C baggage member carrying the business/
	// session correlation id. The edge (portal) may seed it; snowplow mints
	// it when absent. It REPLACES the old X-Krateo-Correlation-Id header
	// entirely — the `session.id` semconv attribute on each audit record is
	// read from here.
	BaggageSessionKey = "session.id"

	// EventName is the discriminator stamped on every audit LogRecord
	// (event.name="audit") — the constant every audit row is filtered on.
	EventName = "audit"

	// maxIDLen bounds an inbound session id; anything longer (or with
	// characters outside idPattern) is rejected and replaced, so a hostile
	// caller cannot inject baggage/pipeline control characters.
	maxIDLen = 128
)

// idPattern is the accepted shape of an inbound session id. Kept to
// baggage-safe token characters.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// SanitizeID validates an inbound session id; it returns "" when the id is
// empty, oversized or contains unexpected characters.
func SanitizeID(id string) string {
	if id == "" || len(id) > maxIDLen || !idPattern.MatchString(id) {
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
// downstream via the Baggage propagator. Snowplow is the id origin. It is
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
			next.ServeHTTP(wri, req.WithContext(ctx))
		})
	}
}

// Event is the caller-facing input for one AuditEvent. All fields are
// optional; unset fields are omitted from the record. Field names map to the
// semconv attribute keys in D19a §1.
type Event struct {
	// Action is the logical operation -> krateo.action ("call", "export").
	Action string
	// Verb is the HTTP verb -> http.request.method.
	Verb string
	// Group/Version/Resource/Name/Namespace identify the object acted on ->
	// k8s.resource.{group,version,resource,name} and k8s.namespace.name.
	Group     string
	Version   string
	Resource  string
	Name      string
	Namespace string
	// Subresource is the addressed subresource (#282), e.g. "status" ->
	// k8s.resource.subresource. Empty for a whole-object call (omitted).
	Subresource string
	// Outcome is "success" | "failure" | "denied" -> outcome (and drives
	// SeverityNumber: Info on success, Error otherwise).
	Outcome string
	// Code is the HTTP status code of the outcome (0 => omitted) ->
	// http.response.status_code.
	Code int
	// Message is an optional human-readable detail -> LogRecord.Body.
	Message string
}

// Emitter wraps a single OTel Logs API Logger. Construct once at startup
// from the process LoggerProvider (see internal/logging) and install it via
// SetDefault. A nil Emitter (pipeline disabled / not installed) is a no-op.
type Emitter struct {
	logger log.Logger
}

// New returns an Emitter bound to logger. Obtain logger from the
// LoggerProvider via provider.Logger("github.com/krateo-platformops/snowplow/audit").
func New(logger log.Logger) *Emitter { return &Emitter{logger: logger} }

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

// Emit writes one canonical AuditEvent as an OTLP LogRecord. ctx MUST carry
// the active span (so the SDK stamps TraceId/SpanId) and baggage (so
// session.id is captured). A nil Emitter is a no-op.
func (e *Emitter) Emit(ctx context.Context, ev Event) {
	if e == nil || e.logger == nil {
		return
	}

	var rec log.Record

	now := time.Now()
	rec.SetTimestamp(now)
	rec.SetObservedTimestamp(now)
	rec.SetBody(log.StringValue(ev.Message))

	// Severity from outcome. Success (or unset) => Info; anything else
	// (failure/denied) => Error.
	if ev.Outcome == "success" || ev.Outcome == "" {
		rec.SetSeverity(log.SeverityInfo)
		rec.SetSeverityText("INFO")
	} else {
		rec.SetSeverity(log.SeverityError)
		rec.SetSeverityText("ERROR")
	}

	// Acting user identity, from the request's authenticated user info.
	user := ""
	var groups []string
	if ui, err := xcontext.UserInfo(ctx); err == nil {
		user = ui.Username
		groups = ui.Groups
	}

	attrs := make([]log.KeyValue, 0, 16)
	add := func(k, v string) {
		if v != "" {
			attrs = append(attrs, log.String(k, v))
		}
	}
	// event.name="audit" — the discriminator every audit row is filtered on.
	// The otel/log v0.10.0 Record has no dedicated event-name field, so it is
	// carried as the semconv `event.name` attribute (lands in
	// otel_logs.LogAttributes['event.name']).
	add("event.name", EventName)
	add("enduser.id", user)
	add("user.name", user)
	add("k8s.namespace.name", ev.Namespace)
	add("k8s.resource.group", ev.Group)
	add("k8s.resource.version", ev.Version)
	add("k8s.resource.resource", ev.Resource)
	add("k8s.resource.subresource", ev.Subresource)
	add("k8s.resource.name", ev.Name)
	add("krateo.action", ev.Action)
	add("http.request.method", ev.Verb)
	add("outcome", ev.Outcome)

	if len(groups) > 0 {
		vals := make([]log.Value, len(groups))
		for i, r := range groups {
			vals[i] = log.StringValue(r)
		}
		attrs = append(attrs, log.Slice("user.roles", vals...))
	}
	if ev.Code != 0 {
		attrs = append(attrs, log.Int("http.response.status_code", ev.Code))
	}

	// Business/session correlation id from W3C baggage — NOT a header.
	if id := SessionID(ctx); id != "" {
		attrs = append(attrs, log.String(BaggageSessionKey, id))
	}

	// Belt-and-suspenders: the Logs SDK stamps TraceId/SpanId from ctx's
	// span (they are LogRecord fields, not LogAttributes — we do NOT re-add
	// them). Only surface the pathological case of an audit event emitted
	// with no trace context at all, so it is detectable in otel_logs.
	if sc := trace.SpanContextFromContext(ctx); !sc.IsValid() {
		attrs = append(attrs, log.Bool("krateo.audit.no_trace_context", true))
	}

	rec.AddAttributes(attrs...)

	e.logger.Emit(ctx, rec) // SDK stamps Timestamp + TraceId/SpanId from ctx
}
