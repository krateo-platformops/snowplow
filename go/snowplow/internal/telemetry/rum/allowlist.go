// Package rum is snowplow's authenticated ingest for the portal's browser RUM
// spans (#1015, acmp X5; Diego's ruling 2026-10-30): the browser posts OTLP to
// snowplow, never to a public collector route, and snowplow forwards a
// rebuilt, allowlisted copy to the same collector its own spans go to.
//
// ALLOWLIST, NOT DENYLIST. Nothing the browser sends is forwarded as is. The
// request is decoded into structs that name only the fields this package
// reads; the outbound payload is BUILT from scratch out of those fields after
// each one passed a value check. A field nobody listed here cannot reach the
// collector, whatever the browser adds tomorrow. The token-shape check on
// http.route is a second guard on an already allowlisted value, never the only
// one.
//
// Seven span names are forwarded, each with its own attribute set
// (spanAllowlist): krateo.widget.fetch and the four page-level spans carry the
// http.* keys (and the three interaction spans krateo.interaction.abandoned,
// #1060); krateo.agentiko.claim.unbacked carries ONLY
// krateo.agentiko.claim.family and krateo.agentiko.claim.outcome, each a
// lower-case token (^[a-z][a-z0-9_]{0,31}$, claimToken), and no http.*,
// session, conversation or identity key; krateo.agentiko.a2a (#1398) carries
// ONLY http.request.method (exactly POST, postMethod),
// http.response.status_code (statusCode), http.route (routeTemplate) and
// krateo.a2a.method (one of message/stream, message/send, tasks/get,
// tasks/resubscribe, a2aMethod), and no session, conversation or identity
// key. An attribute that is not in its
// span's set, or whose value fails the key's check, is dropped and counted;
// the span is still forwarded.
//
// NO IDENTITY. The authenticated user keys the per-user rate limit and nothing
// else: no span, resource or scope attribute carries it. (enduser.id is the
// only identity attribute ever allowed on a span, and only by explicit ruling;
// none exists for RUM.)
package rum

import (
	"encoding/hex"
	"strconv"
	"strings"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// ServiceName is the only resource service.name accepted. A ResourceSpans
// whose service.name is anything else is dropped whole.
const ServiceName = "krateo-frontend"

// ScopeName is the instrumentation scope snowplow stamps on every forwarded
// span, so a forwarded RUM span is told apart from one the collector received
// directly (otel_traces.ScopeName).
const ScopeName = "snowplow/rum-ingest"

// Span names forwarded (#1015). SpanWidgetFetch is in routes.go.
const (
	SpanPageLoad        = "krateo.page.load"
	SpanCompositionOpen = "krateo.composition.open"
	SpanFormSubmit      = "krateo.form.submit"
	SpanPagePublish     = "krateo.page.publish"
	// SpanAgentikoClaimUnbacked is the portal's mark for an agentiko claim
	// that is not backed (contract agreed with the frontend, 2026-11-09). It
	// carries only AttrClaimFamily and AttrClaimOutcome.
	SpanAgentikoClaimUnbacked = "krateo.agentiko.claim.unbacked"
	// SpanAgentikoA2A is the portal's fetch span for one of the agent's
	// JSON-RPC POSTs under the Agentiko A2A base (contract agreed with the
	// frontend, #1398, 2026-11-13): the auto fetch span, renamed by the
	// frontend's span processor, so the gateway's POST /api/a2a/* span is its
	// child. It carries only AttrMethod (exactly POST), AttrStatus, AttrRoute
	// (a template) and AttrA2AMethod.
	SpanAgentikoA2A = "krateo.agentiko.a2a"
)

// Attribute keys a forwarded span may carry; spanAllowlist says on which span
// names and with which values. The rest of the #1015 allowlist is span FIELDS
// rather than attributes: traceId, spanId and parentSpanId (the traceparent
// ids, which link krateo.widget.fetch to snowplow's Server span), kind, start
// and end time (the duration) and the status code (never its message).
const (
	AttrRoute  = "http.route"
	AttrMethod = "http.request.method"
	AttrStatus = "http.response.status_code"
	// AttrInteractionAbandoned marks an interaction the user left before it
	// finished (frontend contract, 2026-11-01): it is excluded from the
	// latency SLI but NOT dropped, so it still counts for throughput and
	// error rate. That is why it is a bool attribute and never a span status
	// (ERROR would move it into the error-rate SLI). The frontend only ever
	// sends true; absent means not abandoned.
	AttrInteractionAbandoned = "krateo.interaction.abandoned"
	// AttrClaimFamily and AttrClaimOutcome classify an unbacked agentiko
	// claim (frontend contract, 2026-11-09): each a lower-case token
	// (claimToken), never free text, an id or an identity.
	AttrClaimFamily  = "krateo.agentiko.claim.family"
	AttrClaimOutcome = "krateo.agentiko.claim.outcome"
	// AttrA2AMethod is the A2A JSON-RPC method of a krateo.agentiko.a2a span
	// (#1398): one of a closed set (a2aMethods), never free text.
	AttrA2AMethod = "krateo.a2a.method"
)

// valueCheck returns the value to forward for one attribute, rebuilt from the
// decoded input, or false when the value is not one the key allows. routes is
// snowplow's live routing table (nil when the ingest has none).
type valueCheck func(v anyValue, routes RouteSet) (*commonpb.AnyValue, bool)

// spanAllowlist is the whole #1015 attribute allowlist: per forwarded span
// name, the attribute keys that span may carry and each key's value check. A
// span whose name is not a key here is dropped (dropped_name); an attribute
// whose key is not in its span's set, or whose value fails the key's check,
// is dropped and counted (attributes_dropped).
//
//   - The http.* keys are allowed on krateo.widget.fetch and the four
//     page-level spans (and, with the checks below, on krateo.agentiko.a2a),
//     never on krateo.agentiko.claim.unbacked. http.route
//     has two checks (Diego, 2026-10-30, #1020 F3: "exact where possible,
//     trust the rest"):
//     on krateo.widget.fetch it names a snowplow route, so it must be EXACTLY
//     one of the live mux's routes (registeredRoute); on the page-level spans
//     it is the portal router's pattern, which snowplow cannot know, so it
//     passes the template check (routeTemplate).
//   - krateo.interaction.abandoned is allowed only on the three interaction
//     spans, and only as bool true (frontend contract, 2026-11-01). On
//     krateo.widget.fetch (a child span) and krateo.page.load it is dropped.
//   - krateo.agentiko.claim.family and krateo.agentiko.claim.outcome are
//     allowed only on krateo.agentiko.claim.unbacked, and only as a string
//     token (claimToken); that span carries no other attribute (frontend
//     contract, 2026-11-09).
//   - krateo.agentiko.a2a carries http.request.method, but only as exactly
//     POST (postMethod: the availability-probe GET is never exported, so any
//     other method breaks the contract), http.response.status_code
//     (statusCode; an aborted stream omits it and sends span status ERROR),
//     http.route as a template (routeTemplate: the A2A route is the
//     gateway's, not snowplow's, so registeredRoute cannot apply), and
//     krateo.a2a.method from a closed set (a2aMethod). No other key (#1398).
var spanAllowlist = map[string]map[string]valueCheck{
	SpanWidgetFetch: {
		AttrRoute: registeredRoute, AttrMethod: methodToken, AttrStatus: statusCode,
	},
	SpanPageLoad: {
		AttrRoute: routeTemplate, AttrMethod: methodToken, AttrStatus: statusCode,
	},
	SpanCompositionOpen: {
		AttrRoute: routeTemplate, AttrMethod: methodToken, AttrStatus: statusCode,
		AttrInteractionAbandoned: boolTrue,
	},
	SpanFormSubmit: {
		AttrRoute: routeTemplate, AttrMethod: methodToken, AttrStatus: statusCode,
		AttrInteractionAbandoned: boolTrue,
	},
	SpanPagePublish: {
		AttrRoute: routeTemplate, AttrMethod: methodToken, AttrStatus: statusCode,
		AttrInteractionAbandoned: boolTrue,
	},
	SpanAgentikoClaimUnbacked: {
		AttrClaimFamily: claimToken, AttrClaimOutcome: claimToken,
	},
	SpanAgentikoA2A: {
		AttrMethod: postMethod, AttrStatus: statusCode, AttrRoute: routeTemplate,
		AttrA2AMethod: a2aMethod,
	},
}

// registeredRoute: a string that is exactly one of snowplow's own routes.
func registeredRoute(v anyValue, routes RouteSet) (*commonpb.AnyValue, bool) {
	if s := v.StringValue; s != nil && routes != nil && routes(*s) {
		return strValue(*s), true
	}
	return nil, false
}

// routeTemplate: a string that passes validRoute (no query string, escape or
// space; no token- or JWT-shaped segment).
func routeTemplate(v anyValue, _ RouteSet) (*commonpb.AnyValue, bool) {
	if s := v.StringValue; s != nil && validRoute(*s) {
		return strValue(*s), true
	}
	return nil, false
}

// methodToken: a string that is an upper-case HTTP method token.
func methodToken(v anyValue, _ RouteSet) (*commonpb.AnyValue, bool) {
	if s := v.StringValue; s != nil && methods[*s] {
		return strValue(*s), true
	}
	return nil, false
}

// postMethod: exactly the string POST (no other member of the value, as
// claimToken). Any other method, a lower-case "post" included, is dropped.
func postMethod(v anyValue, _ RouteSet) (*commonpb.AnyValue, bool) {
	if s := v.StringValue; s != nil && v.members == 1 && *s == "POST" {
		return strValue(*s), true
	}
	return nil, false
}

// a2aMethods is the closed set of A2A JSON-RPC methods the portal sends on
// krateo.agentiko.a2a (frontend contract, #1398).
var a2aMethods = map[string]bool{
	"message/stream": true, "message/send": true, "tasks/get": true, "tasks/resubscribe": true,
}

// a2aMethod: exactly a string (no other member of the value, as claimToken)
// that is one of a2aMethods. Anything else is dropped, never coerced.
func a2aMethod(v anyValue, _ RouteSet) (*commonpb.AnyValue, bool) {
	if s := v.StringValue; s != nil && v.members == 1 && a2aMethods[*s] {
		return strValue(*s), true
	}
	return nil, false
}

// statusCode: an integer from 100 to 599.
func statusCode(v anyValue, _ RouteSet) (*commonpb.AnyValue, bool) {
	if n := v.IntValue; n != nil && *n >= 100 && *n <= 599 {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: int64(*n)}}, true
	}
	return nil, false
}

// boolTrue: exactly the bool true, and no other member of the value (members
// counts every member the decoder saw, skipped ones included). false is never
// sent by contract, and a string "true", an int or any other type is not a
// bool: each is dropped, never coerced.
func boolTrue(v anyValue, _ RouteSet) (*commonpb.AnyValue, bool) {
	if b := v.BoolValue; b != nil && *b && v.members == 1 {
		return &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, true
	}
	return nil, false
}

// claimToken: exactly a string (no other member of the value, as boolTrue)
// matching ^[a-z][a-z0-9_]{0,31}$. Anything else, an int, a bool or a string
// of another shape, is dropped, never coerced.
func claimToken(v anyValue, _ RouteSet) (*commonpb.AnyValue, bool) {
	if s := v.StringValue; s != nil && v.members == 1 && validClaimToken(*s) {
		return strValue(*s), true
	}
	return nil, false
}

// maxClaimTokenLen bounds a claim token (the {0,31} after the first letter).
const maxClaimTokenLen = 32

// validClaimToken reports whether v matches ^[a-z][a-z0-9_]{0,31}$: ASCII
// only, byte by byte, so no case folding or Unicode letter can pass.
func validClaimToken(v string) bool {
	if v == "" || len(v) > maxClaimTokenLen || v[0] < 'a' || v[0] > 'z' {
		return false
	}
	for i := 1; i < len(v); i++ {
		c := v[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			return false
		}
	}
	return true
}

var methods = map[string]bool{
	"GET": true, "HEAD": true, "POST": true, "PUT": true, "PATCH": true, "DELETE": true, "OPTIONS": true,
}

// maxRouteLen bounds http.route. A route template is short; anything longer is
// not one.
const maxRouteLen = 256

// tokenSegmentLen is the length from which a single route segment is treated
// as token-shaped: no route template segment of the portal comes near it, and
// a JWT, an API key, a hex or base64 secret all exceed it.
const tokenSegmentLen = 24

// validRoute reports whether v is a route TEMPLATE: an absolute path made only
// of [A-Za-z0-9/_.:{}-] (so no query string, fragment, percent-escape, space
// or '='), at most maxRouteLen long, with no token-shaped segment.
func validRoute(v string) bool {
	if v == "" || v[0] != '/' || len(v) > maxRouteLen {
		return false
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '/', c == '_', c == '.', c == ':', c == '{', c == '}', c == '-':
		default:
			return false
		}
	}
	for _, seg := range strings.Split(v, "/") {
		if tokenShaped(seg) {
			return false
		}
	}
	return true
}

// tokenShaped: a segment as long as a credential, or a JWT's dotted
// three-part shape.
func tokenShaped(seg string) bool {
	if len(seg) >= tokenSegmentLen {
		return true
	}
	parts := strings.Split(seg, ".")
	if len(parts) == 3 && parts[0] != "" && parts[1] != "" && parts[2] != "" &&
		len(parts[0]) >= 8 && len(parts[1]) >= 8 {
		return true
	}
	return false
}

// --- OTLP/JSON decoding: only the fields above have a Go field to land in. ---
// decode.go walks the body; these are what it fills.

// exportRequest is what decodeCapped keeps of an OTLP/JSON
// ExportTraceServiceRequest: per resource, its attributes and its spans.
type exportRequest struct {
	resources []decodedResource
}

type decodedResource struct {
	attributes []keyValue
	spans      []span
}

type span struct {
	TraceID           string
	SpanID            string
	ParentSpanID      string
	Name              string
	Kind              int32
	StartTimeUnixNano jsonUint64
	EndTimeUnixNano   jsonUint64
	Attributes        []keyValue
	StatusCode        int32
}

type keyValue struct {
	Key   string
	Value anyValue
}

// anyValue keeps the AnyValue members some check reads; decode.go scans past
// every other member without building it, but counts it in members. An OTLP
// AnyValue is a oneof, so a conforming value has exactly one member; a check
// that must see one and only one value (boolTrue) reads members.
type anyValue struct {
	StringValue *string
	IntValue    *jsonInt64
	BoolValue   *bool
	members     int // every member of the value object, kept or skipped
}

// jsonUint64 / jsonInt64 accept the two encodings OTLP/JSON allows for 64-bit
// integers: a decimal string (the proto3 JSON mapping) or a bare number.
type jsonUint64 uint64

func (u *jsonUint64) UnmarshalJSON(b []byte) error {
	n, err := strconv.ParseUint(strings.Trim(string(b), `"`), 10, 64)
	*u = jsonUint64(n)
	return err
}

type jsonInt64 int64

func (i *jsonInt64) UnmarshalJSON(b []byte) error {
	n, err := strconv.ParseInt(strings.Trim(string(b), `"`), 10, 64)
	*i = jsonInt64(n)
	return err
}

// resourceServiceName returns the resource's service.name string value.
func resourceServiceName(attrs []keyValue) string {
	for _, kv := range attrs {
		if kv.Key == "service.name" && kv.Value.StringValue != nil {
			return *kv.Value.StringValue
		}
	}
	return ""
}

// hexID decodes an OTLP/JSON id (lower- or upper-case hex, as the spec
// requires; never base64) of exactly n bytes that is not all zero.
func hexID(s string, n int) ([]byte, bool) {
	if len(s) != 2*n {
		return nil, false
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, false
	}
	for _, c := range b {
		if c != 0 {
			return b, true
		}
	}
	return nil, false
}
