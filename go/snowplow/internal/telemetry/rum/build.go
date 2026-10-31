package rum

import (
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// filtered is the outcome of applying the allowlist to one request.
type filtered struct {
	spans          []*tracepb.Span
	droppedService int // spans under a resource whose service.name is not ServiceName
	droppedName    int // spans whose name is not in SpanNames
	droppedInvalid int // spans whose ids, times, kind or status are not valid OTLP
	droppedAttrs   int // span attributes not allowlisted, or with a value the key does not allow
	// unregisteredRoutes is the subset of droppedAttrs that were a
	// krateo.widget.fetch http.route not among snowplow's own routes.
	unregisteredRoutes int
}

func (f filtered) rejected() int { return f.droppedService + f.droppedName + f.droppedInvalid }

// filter applies the #1015 allowlist and returns the spans to forward, each
// a NEW tracepb.Span populated field by field from the allowlisted inputs.
func filter(req *exportRequest, snowplowRoutes RouteSet) filtered {
	var f filtered
	if snowplowRoutes != nil {
		// A batch repeats a handful of routes across up to 512 spans: look
		// each up on the mux once per request.
		known := map[string]bool{}
		live := snowplowRoutes
		snowplowRoutes = func(route string) bool {
			v, ok := known[route]
			if !ok {
				v = live(route)
				known[route] = v
			}
			return v
		}
	}
	for _, rs := range req.resources {
		service := resourceServiceName(rs.attributes)
		for i := range rs.spans {
			in := &rs.spans[i]
			switch {
			case service != ServiceName:
				f.droppedService++
			case !SpanNames[in.Name]:
				f.droppedName++
			default:
				out, dropped, unregistered, ok := buildSpan(in, snowplowRoutes)
				if !ok {
					f.droppedInvalid++
					continue
				}
				f.droppedAttrs += dropped
				f.unregisteredRoutes += unregistered
				f.spans = append(f.spans, out)
			}
		}
	}
	return f
}

// buildSpan copies the allowlisted fields of in into a new span. ok is false
// when an id, the times, the kind or the status code is not valid; dropped is
// the number of span attributes left out, unregistered how many of those were
// a krateo.widget.fetch http.route that is not one of snowplowRoutes.
func buildSpan(in *span, snowplowRoutes RouteSet) (out *tracepb.Span, dropped, unregistered int, ok bool) {
	traceID, ok := hexID(in.TraceID, 16)
	if !ok {
		return nil, 0, 0, false
	}
	spanID, ok := hexID(in.SpanID, 8)
	if !ok {
		return nil, 0, 0, false
	}
	var parentID []byte
	if in.ParentSpanID != "" {
		if parentID, ok = hexID(in.ParentSpanID, 8); !ok {
			return nil, 0, 0, false
		}
	}
	start, end := uint64(in.StartTimeUnixNano), uint64(in.EndTimeUnixNano)
	if start == 0 || end < start {
		return nil, 0, 0, false
	}
	if _, known := tracepb.Span_SpanKind_name[in.Kind]; !known {
		return nil, 0, 0, false
	}
	if _, known := tracepb.Status_StatusCode_name[in.StatusCode]; !known {
		return nil, 0, 0, false
	}

	var attrs []*commonpb.KeyValue
	seen := map[string]bool{}
	for _, kv := range in.Attributes {
		v, keep := allowedValue(kv, in.Name, snowplowRoutes)
		if !keep || seen[kv.Key] {
			dropped++
			if !keep && kv.Key == AttrRoute && in.Name == SpanWidgetFetch {
				unregistered++
			}
			continue
		}
		seen[kv.Key] = true
		attrs = append(attrs, &commonpb.KeyValue{Key: kv.Key, Value: v})
	}
	return &tracepb.Span{
		TraceId:           traceID,
		SpanId:            spanID,
		ParentSpanId:      parentID,
		Name:              in.Name,
		Kind:              tracepb.Span_SpanKind(in.Kind),
		StartTimeUnixNano: start,
		EndTimeUnixNano:   end,
		Attributes:        attrs,
		Status:            &tracepb.Status{Code: tracepb.Status_StatusCode(in.StatusCode)},
	}, dropped, unregistered, true
}

// allowedValue returns the value to forward for an allowlisted key, or false
// when the key is not allowlisted or its value is not one the key allows.
//
// http.route (Diego, 2026-10-30, #1020 F3: "exact where possible, trust the
// rest"):
//   - on krateo.widget.fetch the route names a snowplow route, so it must be
//     EXACTLY one of snowplowRoutes (the live mux), else it is dropped;
//   - on the page-level spans it is the portal router's pattern, which
//     snowplow cannot know: it is accepted when validRoute passes (no query
//     string, escape or space; no token- or JWT-shaped segment).
func allowedValue(kv keyValue, spanName string, snowplowRoutes RouteSet) (*commonpb.AnyValue, bool) {
	switch kv.Key {
	case AttrRoute:
		s := kv.Value.StringValue
		if s == nil {
			return nil, false
		}
		if spanName == SpanWidgetFetch {
			if snowplowRoutes != nil && snowplowRoutes(*s) {
				return strValue(*s), true
			}
			return nil, false
		}
		if validRoute(*s) {
			return strValue(*s), true
		}
	case AttrMethod:
		if s := kv.Value.StringValue; s != nil && methods[*s] {
			return strValue(*s), true
		}
	case AttrStatus:
		if n := kv.Value.IntValue; n != nil && *n >= 100 && *n <= 599 {
			return &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: int64(*n)}}, true
		}
	}
	return nil, false
}

func strValue(s string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: s}}
}

// envelope wraps forwarded spans in the one resource and scope snowplow
// stamps: resource service.name=ServiceName (nothing else from the browser's
// resource is kept) and scope ScopeName.
func envelope(spans []*tracepb.Span) *collectortracepb.ExportTraceServiceRequest {
	return &collectortracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				{Key: "service.name", Value: strValue(ServiceName)},
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope: &commonpb.InstrumentationScope{Name: ScopeName},
				Spans: spans,
			}},
		}},
	}
}
