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
// NO IDENTITY. The authenticated user keys the per-user rate limit and nothing
// else: no span, resource or scope attribute carries it. (enduser.id is the
// only identity attribute ever allowed on a span, and only by explicit ruling;
// none exists for RUM.)
package rum

import (
	"encoding/hex"
	"strconv"
	"strings"
)

// ServiceName is the only resource service.name accepted. A ResourceSpans
// whose service.name is anything else is dropped whole.
const ServiceName = "krateo-frontend"

// ScopeName is the instrumentation scope snowplow stamps on every forwarded
// span, so a forwarded RUM span is told apart from one the collector received
// directly (otel_traces.ScopeName).
const ScopeName = "snowplow/rum-ingest"

// SpanNames are the only span names forwarded (#1015).
var SpanNames = map[string]bool{
	"krateo.page.load":        true,
	"krateo.composition.open": true,
	"krateo.form.submit":      true,
	"krateo.page.publish":     true,
	"krateo.widget.fetch":     true,
}

// Attribute keys forwarded, each with the only values it may carry. Every
// other span attribute is dropped and counted. The rest of the #1015
// allowlist is span FIELDS rather than attributes: traceId, spanId and
// parentSpanId (the traceparent ids, which link krateo.widget.fetch to
// snowplow's Server span), kind, start and end time (the duration) and the
// status code (never its message).
const (
	AttrRoute  = "http.route"
	AttrMethod = "http.request.method"
	AttrStatus = "http.response.status_code"
)

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
	Key   string `json:"key"`
	Value struct {
		StringValue *string    `json:"stringValue"`
		IntValue    *jsonInt64 `json:"intValue"`
	} `json:"value"`
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
