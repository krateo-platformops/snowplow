package tracing

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/krateo-platformops/snowplow/internal/redact"
)

// urlAttrKeys are the span attributes that carry a whole URL, query included.
// `url.full` is the current semconv key otelhttp's client instrumentation
// records; `http.url` is the pre-1.21 name, kept here so an older or a
// differently-versioned instrumentation cannot route around the redactor.
//
// Deliberately NOT in this list: `url.path`, `http.route`, `http.target` and
// `server.address`, which carry no query. url.path is handled separately, on
// Server spans only (serverPathKey below).
var urlAttrKeys = [...]attribute.Key{"url.full", "http.url"}

// serverPathKey is dropped from every SERVER span (#1013). otelhttp records the
// raw request path as url.path, and a raw path can carry a name: snowplow's own
// routes are all literal ("/call", "/call/read", …), so the only paths that
// differ from their route are ones the client chose freely, e.g. an unmatched
// "/call/<name>" that the mux 404s, or a "/swagger/<name>". The span already
// carries the templated http.route (NewServerHandler), which is what the SLIs
// and every view group on, so url.path adds nothing but that exposure. Client
// spans keep it: their path is a RESTAction endpoint snowplow itself built.
const serverPathKey attribute.Key = "url.path"

// redactingExporter strips credentials out of URL-bearing span attributes on
// the way out of the process (#489), and the raw path out of Server spans
// (#1013).
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
// redact.URL (userinfo dropped, query values replaced, keys kept) and, on a
// Server span, url.path removed; or s itself when there was nothing to change.
func redactSpanURLs(s sdktrace.ReadOnlySpan) sdktrace.ReadOnlySpan {
	if s == nil {
		return s
	}
	server := s.SpanKind() == trace.SpanKindServer
	attrs := s.Attributes()
	var out []attribute.KeyValue // nil until the first change, then the rewritten list
	for i, a := range attrs {
		drop := server && a.Key == serverPathKey
		changed := drop
		if !drop && a.Value.Type() == attribute.STRING && isURLAttr(a.Key) {
			if safe := redact.URL(a.Value.AsString()); safe != a.Value.AsString() {
				a, changed = a.Key.String(safe), true
			}
		}
		if changed && out == nil {
			out = append(make([]attribute.KeyValue, 0, len(attrs)), attrs[:i]...)
		}
		if out != nil && !drop {
			out = append(out, a)
		}
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
// rewritten or dropped. Embedding the ReadOnlySpan interface promotes every
// other method (including the SDK's unexported marker), so this stays a
// faithful view of the original span with exactly one field substituted.
type redactedSpan struct {
	sdktrace.ReadOnlySpan
	attrs []attribute.KeyValue
}

func (r redactedSpan) Attributes() []attribute.KeyValue { return r.attrs }
