package tracing

import (
	"net/http"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// NewServerHandler is snowplow's HTTP server instrumentation: it gives every
// request it serves exactly ONE span of kind Server, which is what the platform
// API SLIs are computed over (#1013, acmp X5: availability and p95 over
// SpanKind='Server' AND ServiceName='snowplow').
//
// next is the handler that serves the request (in main, the gzip-wrapped mux);
// routes is the mux itself, consulted only to learn which registered pattern
// will serve the request. Each span carries:
//
//   - http.route: the TEMPLATED route, i.e. the path half of the registered
//     ServeMux pattern ("GET /call" → "/call"). It is never the raw request
//     path: a path can carry a name (an unmatched "/call/<name>", a
//     "/swagger/<name>" asset), a route cannot. A request no pattern serves (a
//     404 or 405 from the mux) carries no http.route.
//   - span name "<METHOD> <route>" ("GET /call", "POST /call/read"), or just
//     "<METHOD>" when there is no route. This is otelhttp's own semconv
//     formatter; snowplow no longer overrides it with the raw path.
//   - http.request.method and http.response.status_code (otelhttp semconv).
//   - status Error for a 5xx ONLY. A 4xx (403, 404, 429 …) is the client's
//     fault and leaves the status Unset, so it counts as available.
//
// WHY THE ROUTE IS RESOLVED BEFORE THE SPAN STARTS. otelhttp reads
// Request.Pattern for http.route when it STARTS the span, but the mux fills
// Pattern in only when it dispatches, after the span has started. Before
// #1013 every snowplow span therefore had no http.route at all (057,
// otel_traces, 7 days: 0 of 6,100 Server spans carried it), and the name came
// from a custom formatter over r.URL.Path. Asking the mux up front, with the
// same lookup ServeMux.ServeHTTP performs, sets Pattern to exactly the value
// the mux will set a moment later, so the route is on the span from its first
// instant and the metric and the span agree on it.
//
// PROBE AND DEBUG ROUTES GET NO SPAN (see spanned). That is the original
// main.go filter, now decided on the route instead of the raw path.
func NewServerHandler(next http.Handler, routes *http.ServeMux) http.Handler {
	h := otelhttp.NewHandler(markPanics(next), serviceName,
		otelhttp.WithFilter(func(r *http.Request) bool { return spanned(route(r.Pattern)) }),
	)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// ServeMux.ServeHTTP assigns r.Pattern from this very lookup; doing it
		// one step earlier changes nothing the mux sees.
		_, r.Pattern = routes.Handler(r)
		h.ServeHTTP(w, r)
	})
}

// markPanics makes a panicking request count as unavailable (#1018 review).
// otelhttp sets the span status and http.response.status_code only after the
// handler RETURNS. A panic unwinds past that code, so the span used to export
// Unset with no status code, an available request in the SLIs, while net/http
// recovered the panic by closing the connection and the client (the gateway)
// saw a 5xx. Here, inside otelhttp's span, a panic sets status Error and
// http.response.status_code=500 on the span and is then RE-PANICKED unchanged,
// so net/http's own recovery (log, close the connection) is exactly as before;
// otelhttp's deferred span.End still exports the span. http.ErrAbortHandler
// (a deliberate abort) is marked too: the client gets no response either way.
func markPanics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				span := trace.SpanFromContext(r.Context())
				span.SetAttributes(attribute.Int("http.response.status_code", http.StatusInternalServerError))
				span.SetStatus(codes.Error, "handler panic")
				panic(v)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// route is the path half of a ServeMux pattern: "GET /call" → "/call",
// "/swagger/" → "/swagger/", "" → "". It is the same derivation otelhttp
// applies for http.route, kept here so the probe decision and the span agree.
func route(pattern string) string {
	if i := strings.IndexByte(pattern, '/'); i >= 0 {
		return pattern[i:]
	}
	return ""
}

// unspannedRoutes are the routes that get no Server span.
//
//   - /health and /readyz, the kubelet probes. They serve no client: the
//     liveness probe hits /health every 10 s and the readiness probe hits
//     /readyz every 5 s (helm/snowplow/values.yaml), 25,920 requests a day per
//     pod, which is more than all of 057's client traffic (6,962 requests over
//     the 24 h to 2026-10-30 11:00Z). Spanning them would make probes most of
//     the volume and would push availability and p95 toward the probes'
//     trivially fast 200s unless every SLI query remembered to filter them out.
//   - /telemetry/v1/traces, the browser RUM upload (#1015; Diego, 2026-10-30,
//     #1020 D6: no Server span for RUM uploads, treated exactly like the
//     probes). It is telemetry about the portal, not platform API; its health
//     is read from the snowplow_rum_ingest_* counters (accepted, refused, busy,
//     forward_rate_limited). The path is spelled here, not imported, because
//     internal/telemetry/rum imports this package; the main-package arm
//     TestS1020_RUMUploadGetsNoServerSpan pins it to rum.Path.
var unspannedRoutes = map[string]bool{"/health": true, "/readyz": true, "/telemetry/v1/traces": true}

// spanned reports whether a request served under rt gets a Server span.
//
// EXCLUDED, NOT MARKED. The two choices the issue offered were to skip span
// creation or to stamp an attribute the SLI filters on. Skipping is chosen
// because a mark makes correctness depend on every SLI query (acmp's, HyperDX
// views, ad-hoc ones) remembering the filter, and still pays the probe volume
// above. The /debug/ subtree is excluded for a different reason: it is the
// JWT-gated operator surface (pprof, expvar, store dumps), not platform API,
// and a 30 s /debug/pprof/profile would land in the API p95.
//
// A request no pattern serves (rt == "") IS spanned: it is a real client
// request that got a 404 or 405, and 4xx counts as available.
func spanned(rt string) bool {
	return !unspannedRoutes[rt] && !strings.HasPrefix(rt, "/debug/")
}
