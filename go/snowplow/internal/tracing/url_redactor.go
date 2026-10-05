package tracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/krateo-platformops/snowplow/internal/redact"
)

// urlAttrKeys are the span attributes that carry a whole URL, query included.
// `url.full` is the current semconv key otelhttp's client instrumentation
// records; `http.url` is the pre-1.21 name, kept here so an older or a
// differently-versioned instrumentation cannot route around the redactor.
//
// Deliberately NOT in this list: `url.path`, `http.route`, `http.target` and
// `server.address`, which carry no query. snowplow's own SERVER spans record
// url.path only (main.go), so they are unaffected either way.
var urlAttrKeys = [...]attribute.Key{"url.full", "http.url"}

// redactingExporter strips credentials out of URL-bearing span attributes on
// the way out of the process (#489).
//
// WHY AT THE EXPORTER, and not at a SpanProcessor's OnStart: otelhttp records
// url.full on the span AFTER trace.Start returns, so an OnStart rewrite is
// overwritten by the instrumentation and the credential still reaches the
// collector. That is not a theory — the first cut of this fix did exactly that
// and TestS489_ClientSpanNeverCarriesAQueryCredential caught it, printing the
// api_key in clear out of url.full. The exporter is the one hook that sees a
// span in its FINAL state, whatever order the instrumentation wrote it in.
//
// WHY NOT AN OPTION ON THE ONE INSTRUMENTATION SITE: the leak is a property of
// "any span that records a full URL", not of otelhttp. Wrapping the exporter
// covers the outbound external-fetch transport today and anything a later
// instrumentation adds without knowing this rule exists.
//
// otelhttp v0.69 records url.full WITH the query (it strips only userinfo,
// semconv/client.go:115-123), so a RESTAction api-step whose endpoint carries
// `?token=` / `?api_key=` / a pre-signed signature would otherwise write that
// credential to otel_traces.
type redactingExporter struct {
	sdktrace.SpanExporter
}

var _ sdktrace.SpanExporter = redactingExporter{}

// ExportSpans redacts every URL-bearing attribute, then delegates. A span with
// nothing to redact is passed through UNCHANGED (same pointer), so the
// off-path cost is one attribute scan and no allocation.
//
// The caller's slice is rewritten IN PLACE. That is sound against both SDK
// processors as of otel/sdk v1.45.0, but by implementation rather than by
// contract, so it is written down here: the batch processor clears and
// truncates its buffer unconditionally right after ExportSpans returns
// (`clear(bsp.batch); bsp.batch = bsp.batch[:0]`, batch_span_processor.go), and
// the simple processor hands over a freshly allocated one-element slice. If a
// future SDK retained the slice across calls, this would need to allocate a
// copy instead — a reader who upgrades the SDK should check that line.
func (e redactingExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	for i, s := range spans {
		spans[i] = redactSpanURLs(s)
	}
	return e.SpanExporter.ExportSpans(ctx, spans)
}

// redactSpanURLs returns s with every URL attribute rendered through
// redact.URL (userinfo dropped, query values replaced, keys kept), or s itself
// when there was nothing to change.
func redactSpanURLs(s sdktrace.ReadOnlySpan) sdktrace.ReadOnlySpan {
	if s == nil {
		return s
	}
	attrs := s.Attributes()
	var out []attribute.KeyValue
	for i, a := range attrs {
		if a.Value.Type() != attribute.STRING || !isURLAttr(a.Key) {
			continue
		}
		raw := a.Value.AsString()
		safe := redact.URL(raw)
		if safe == raw {
			continue
		}
		if out == nil {
			out = make([]attribute.KeyValue, len(attrs))
			copy(out, attrs)
		}
		out[i] = a.Key.String(safe)
	}
	if out == nil {
		return s
	}
	return redactedSpan{ReadOnlySpan: s, attrs: out}
}

func isURLAttr(k attribute.Key) bool {
	for _, want := range urlAttrKeys {
		if k == want {
			return true
		}
	}
	return false
}

// redactedSpan is the exported view of a span whose URL attributes have been
// rewritten. Embedding the ReadOnlySpan interface promotes every other method
// (including the SDK's unexported marker), so this stays a faithful view of
// the original span with exactly one field substituted.
type redactedSpan struct {
	sdktrace.ReadOnlySpan
	attrs []attribute.KeyValue
}

func (r redactedSpan) Attributes() []attribute.KeyValue { return r.attrs }
