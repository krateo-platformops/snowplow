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
	"strings"
	"sync/atomic"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"

	"go.opentelemetry.io/otel/attribute"
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

	// DryRunSuffix is appended to krateo.action for a dry-run write
	// (Event.DryRun): "call.dryRun", "restaction.dryRun".
	DryRunSuffix = ".dryRun"

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
	// Action is the logical operation -> krateo.action ("call", "export",
	// "restaction").
	Action string
	// DryRun marks a write the apiserver was asked to validate and not
	// persist (dryRun=All). krateo.action then carries the suffix DryRunSuffix
	// ("call.dryRun", "restaction.dryRun"), and RecordWrittenObject records no
	// object identity: nothing was written (#957).
	DryRun bool
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

	// ServerAddress is the host (and port) the action was sent to ->
	// server.address. Set by the RESTAction stage emit (AUDIT-4), where a
	// write can go to an external API rather than the Kubernetes apiserver;
	// it never carries userinfo, a path or a query. The request PATH is
	// deliberately not recorded: a webhook path (/services/T/B/<secret>,
	// /bot<token>/…) or a jq-built one can carry a credential, and this trail
	// is kept for 365 days. The RESTAction, its stage and this host identify
	// the action and the authority without it.
	ServerAddress string
	// RESTActionNamespace/RESTActionName/Stage name the RESTAction and the
	// api[] stage that performed the action -> krateo.restaction.{namespace,
	// name,stage}. Empty for an action that is not a RESTAction stage.
	RESTActionNamespace string
	RESTActionName      string
	Stage               string

	// ObjectUID/ObjectGeneration/ObjectResourceVersion are the identity of the
	// object a successful Kubernetes write wrote, as the apiserver RETURNED it
	// (#957, krateo-acmp#59 Option B) -> k8s.object.{uid,generation,
	// resource_version}. They are the join key to the audit record of the
	// controller that later reconciles that object: it records the same uid and
	// the generation it acted on. The attribute keys are those of the shared
	// emitter, github.com/krateo-platformops/audit, so the join cannot drift.
	// Optional: set only by RecordWrittenObject (written_object.go), so a failed
	// write, a read, a dry run or a write to a non-Kubernetes endpoint carries
	// none of them. A zero generation is omitted.
	ObjectUID             string
	ObjectGeneration      int64
	ObjectResourceVersion string
}

// actionOf is the krateo.action value of ev: its Action, marked DryRunSuffix
// when the write was a dry run. An absent action stays absent (and is counted
// missing).
func actionOf(ev Event) string {
	if ev.DryRun && ev.Action != "" {
		return ev.Action + DryRunSuffix
	}
	return ev.Action
}

// missingFields returns, in a fixed order, the fields other than the actor
// that ev lacks to answer "what was done to which target": action, method,
// target (a Kubernetes resource or a server address). Empty when complete.
//
// The written object's identity (ObjectUID, ObjectGeneration,
// ObjectResourceVersion) is OPTIONAL and never listed here: only a successful
// Kubernetes write has a returned object, so its absence on a failure, a read,
// a dry run or an external endpoint is the correct record, not a gap.
func missingFields(ev Event) []string {
	var out []string
	if ev.Action == "" {
		out = append(out, IncompleteAction)
	}
	if ev.Verb == "" {
		out = append(out, IncompleteMethod)
	}
	if ev.Resource == "" && ev.ServerAddress == "" {
		out = append(out, IncompleteTarget)
	}
	return out
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
//
// AUDIT-4 (installer#410) completeness, enforced here because every emit site
// passes through it:
//   - NO ACTOR, NO RECORD. "Prefer no record to a recordless actor": a record
//     with no authenticated user on ctx is refused and counted under "actor".
//   - Any other missing field (action, method, target) does not hide the record:
//     it is emitted marked krateo.audit.incomplete=<fields> and each missing
//     field is counted, so a gap in a site is visible both in the trail and in
//     snowplow_audit_incomplete_total.
func (e *Emitter) Emit(ctx context.Context, ev Event) {
	if e == nil || e.logger == nil {
		return
	}

	// Acting user identity, from the request's authenticated user info.
	user := ""
	var groups []string
	if ui, err := xcontext.UserInfo(ctx); err == nil {
		user = ui.Username
		groups = ui.Groups
	}
	if user == "" {
		incomplete.Add(IncompleteActor, 1)
		return
	}
	missing := missingFields(ev)
	for _, f := range missing {
		incomplete.Add(f, 1)
	}

	var rec log.Record

	now := time.Now()
	rec.SetTimestamp(now)
	rec.SetObservedTimestamp(now)
	rec.SetBody(attribute.StringValue(ev.Message))

	// Severity from outcome. Success (or unset) => Info; anything else
	// (failure/denied) => Error.
	if ev.Outcome == "success" || ev.Outcome == "" {
		rec.SetSeverity(log.SeverityInfo)
		rec.SetSeverityText("INFO")
	} else {
		rec.SetSeverity(log.SeverityError)
		rec.SetSeverityText("ERROR")
	}

	attrs := make([]attribute.KeyValue, 0, 25)
	add := func(k, v string) {
		if v != "" {
			attrs = append(attrs, attribute.String(k, v))
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
	add("k8s.object.uid", ev.ObjectUID)
	add("k8s.object.resource_version", ev.ObjectResourceVersion)
	add("krateo.action", actionOf(ev))
	add("http.request.method", ev.Verb)
	add("outcome", ev.Outcome)
	add("server.address", ev.ServerAddress)
	add("krateo.audit.incomplete", strings.Join(missing, ","))
	add("krateo.restaction.namespace", ev.RESTActionNamespace)
	add("krateo.restaction.name", ev.RESTActionName)
	add("krateo.restaction.stage", ev.Stage)

	if len(groups) > 0 {
		// v0.21.0: the log API adopted the shared attribute value types, so the
		// hand-rolled []log.Value is now just a typed string slice.
		attrs = append(attrs, attribute.StringSlice("user.roles", groups))
	}
	if ev.ObjectGeneration != 0 {
		attrs = append(attrs, attribute.Int64("k8s.object.generation", ev.ObjectGeneration))
	}
	if ev.Code != 0 {
		attrs = append(attrs, attribute.Int("http.response.status_code", ev.Code))
	}

	// Business/session correlation id from W3C baggage — NOT a header.
	if id := SessionID(ctx); id != "" {
		attrs = append(attrs, attribute.String(BaggageSessionKey, id))
	}

	// Belt-and-suspenders: the Logs SDK stamps TraceId/SpanId from ctx's
	// span (they are LogRecord fields, not LogAttributes — we do NOT re-add
	// them). Only surface the pathological case of an audit event emitted
	// with no trace context at all, so it is detectable in otel_logs.
	if sc := trace.SpanContextFromContext(ctx); !sc.IsValid() {
		attrs = append(attrs, attribute.Bool("krateo.audit.no_trace_context", true))
	}

	rec.AddAttributes(attrs...)

	e.logger.Emit(ctx, rec) // SDK stamps Timestamp + TraceId/SpanId from ctx
}
