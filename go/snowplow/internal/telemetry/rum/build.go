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
}

func (f filtered) rejected() int { return f.droppedService + f.droppedName + f.droppedInvalid }

// filter applies the #1015 allowlist and returns the spans to forward, each
// a NEW tracepb.Span populated field by field from the allowlisted inputs.
func filter(req *exportRequest) filtered {
	var f filtered
	for _, rs := range req.ResourceSpans {
		service := resourceServiceName(rs.Resource.Attributes)
		for _, ss := range rs.ScopeSpans {
			for i := range ss.Spans {
				in := &ss.Spans[i]
				switch {
				case service != ServiceName:
					f.droppedService++
				case !SpanNames[in.Name]:
					f.droppedName++
				default:
					out, dropped, ok := buildSpan(in)
					if !ok {
						f.droppedInvalid++
						continue
					}
					f.droppedAttrs += dropped
					f.spans = append(f.spans, out)
				}
			}
		}
	}
	return f
}

// buildSpan copies the allowlisted fields of in into a new span. ok is false
// when an id, the times, the kind or the status code is not valid; dropped is
// the number of span attributes left out.
func buildSpan(in *span) (out *tracepb.Span, dropped int, ok bool) {
	traceID, ok := hexID(in.TraceID, 16)
	if !ok {
		return nil, 0, false
	}
	spanID, ok := hexID(in.SpanID, 8)
	if !ok {
		return nil, 0, false
	}
	var parentID []byte
	if in.ParentSpanID != "" {
		if parentID, ok = hexID(in.ParentSpanID, 8); !ok {
			return nil, 0, false
		}
	}
	start, end := uint64(in.StartTimeUnixNano), uint64(in.EndTimeUnixNano)
	if start == 0 || end < start {
		return nil, 0, false
	}
	if _, known := tracepb.Span_SpanKind_name[in.Kind]; !known {
		return nil, 0, false
	}
	if _, known := tracepb.Status_StatusCode_name[in.Status.Code]; !known {
		return nil, 0, false
	}

	var attrs []*commonpb.KeyValue
	seen := map[string]bool{}
	for _, kv := range in.Attributes {
		v, keep := allowedValue(kv)
		if !keep || seen[kv.Key] {
			dropped++
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
		Status:            &tracepb.Status{Code: tracepb.Status_StatusCode(in.Status.Code)},
	}, dropped, true
}

// allowedValue returns the value to forward for an allowlisted key, or false
// when the key is not allowlisted or its value is not one the key allows.
func allowedValue(kv keyValue) (*commonpb.AnyValue, bool) {
	switch kv.Key {
	case AttrRoute:
		if s := kv.Value.StringValue; s != nil && validRoute(*s) {
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
