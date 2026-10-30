package tracing

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// server_test.go — #1013: the platform API SLIs (acmp X5) are computed over
// snowplow's Server spans, so the shape of those spans is a contract:
// span name "<METHOD> <route>", http.route = the templated mux pattern,
// status Error only for 5xx, and no span for probes or the debug surface.
//
// PRODUCTION PATH. Every arm drives NewServerHandler (the function main wires
// via newServerHandler) over a real http.ServeMux, with the provider built by
// newTracerProvider (the constructor Setup uses, redacting exporter included).
// Only the exporter is in-memory.

// nameSentinel stands in for a user or resource name that reaches snowplow in
// a request path. It must never be exported in a span name or attribute.
const nameSentinel = "zq1013-person-name"

// serverSpans serves each request through NewServerHandler(mux, mux) and
// returns the exported spans, flushed through the production exporter chain.
func serverSpans(t *testing.T, mux *http.ServeMux, reqs ...*http.Request) []tracetest.SpanStub {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := newTracerProvider(exp)
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	h := NewServerHandler(mux, mux)
	for _, r := range reqs {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, r)
		_, _ = io.Copy(io.Discard, rec.Body)
	}
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatalf("SETUP: flush: %v", err)
	}
	return exp.GetSpans()
}

func attr(s tracetest.SpanStub, key string) (string, bool) {
	for _, a := range s.Attributes {
		if string(a.Key) == key {
			return a.Value.Emit(), true
		}
	}
	return "", false
}

func status(code int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) })
}

// TestS1013_RouteIsTemplatedNeverRawPath — http.route is the registered
// pattern's path and the span name is "<METHOD> <route>"; a name carried in
// the path or the query is in neither, nor in any other exported attribute.
//
// MUTATIONS (each RED, shown in the PR):
//   - drop the up-front `_, r.Pattern = routes.Handler(r)` in NewServerHandler
//     → no span carries http.route (otelhttp reads Pattern at span start);
//   - restore the pre-#1013 formatter `r.Method + " " + r.URL.Path`
//     → the unmatched span is named "GET /call/<name>";
//   - stop dropping url.path from Server spans in redactSpanURLs
//     → the name is exported in url.path.
func TestS1013_RouteIsTemplatedNeverRawPath(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("GET /call", status(http.StatusOK))
	mux.Handle("POST /call/read", status(http.StatusOK))
	mux.Handle("GET /swagger/", status(http.StatusOK))

	cases := []struct {
		req       *http.Request
		wantName  string
		wantRoute string // "" = the span must carry NO http.route
	}{
		{httptest.NewRequest(http.MethodGet, "/call?resource=widgets&name="+nameSentinel, nil), "GET /call", "/call"},
		{httptest.NewRequest(http.MethodPost, "/call/read?name="+nameSentinel, strings.NewReader(`{}`)), "POST /call/read", "/call/read"},
		{httptest.NewRequest(http.MethodGet, "/swagger/"+nameSentinel, nil), "GET /swagger/", "/swagger/"},
		// No pattern serves this path: the mux answers 404. It is still a
		// served client request, so it is spanned, but with no route to name.
		{httptest.NewRequest(http.MethodGet, "/call/"+nameSentinel, nil), "GET", ""},
	}
	reqs := make([]*http.Request, len(cases))
	for i, c := range cases {
		reqs[i] = c.req
	}
	spans := serverSpans(t, mux, reqs...)
	if len(spans) != len(cases) {
		t.Fatalf("exported %d spans for %d requests, want one each", len(spans), len(cases))
	}
	for i, c := range cases {
		s := spans[i]
		if s.SpanKind != trace.SpanKindServer {
			t.Errorf("%s: span kind %v, want Server", c.wantName, s.SpanKind)
		}
		if s.Name != c.wantName {
			t.Errorf("span name %q, want %q", s.Name, c.wantName)
		}
		route, ok := attr(s, "http.route")
		switch {
		case c.wantRoute == "" && ok:
			t.Errorf("%s: unmatched request carries http.route=%q, want none", c.wantName, route)
		case c.wantRoute != "" && route != c.wantRoute:
			t.Errorf("%s: http.route=%q (present=%v), want %q", c.wantName, route, ok, c.wantRoute)
		}
		if _, ok := attr(s, "url.path"); ok {
			t.Errorf("%s: Server span exports url.path; the raw path is dropped at export (#1013)", c.wantName)
		}
		if strings.Contains(s.Name, nameSentinel) {
			t.Errorf("span name %q carries the name from the request", s.Name)
		}
		for _, a := range s.Attributes {
			if strings.Contains(a.Value.Emit(), nameSentinel) {
				t.Errorf("%s: attribute %s carries the name from the request path", c.wantName, a.Key)
			}
		}
	}
}

// TestS1013_StatusErrorOnlyFor5xx — availability counts non-Error spans, so a
// 4xx (the client's fault) must leave the status Unset and only a 5xx sets
// Error. http.response.status_code and http.request.method are always set.
//
// MUTATION (RED, shown in the PR): an inner wrapper in NewServerHandler that
// sets codes.Error on the span for any status >= 400 — the 4xx arms fail.
func TestS1013_StatusErrorOnlyFor5xx(t *testing.T) {
	cases := []struct {
		code int
		want codes.Code
	}{
		{http.StatusOK, codes.Unset},
		{http.StatusForbidden, codes.Unset},
		{http.StatusNotFound, codes.Unset},
		{http.StatusTooManyRequests, codes.Unset},
		{http.StatusInternalServerError, codes.Error},
		{http.StatusServiceUnavailable, codes.Error},
	}
	for _, c := range cases {
		t.Run(http.StatusText(c.code), func(t *testing.T) {
			mux := http.NewServeMux()
			mux.Handle("GET /call", status(c.code))
			spans := serverSpans(t, mux, httptest.NewRequest(http.MethodGet, "/call", nil))
			if len(spans) != 1 {
				t.Fatalf("exported %d spans, want 1", len(spans))
			}
			s := spans[0]
			if s.Status.Code != c.want {
				t.Errorf("status %d → span status %v, want %v", c.code, s.Status.Code, c.want)
			}
			if got, _ := attr(s, "http.response.status_code"); got != strconv.Itoa(c.code) {
				t.Errorf("http.response.status_code=%q, want %d", got, c.code)
			}
			if got, _ := attr(s, "http.request.method"); got != http.MethodGet {
				t.Errorf("http.request.method=%q, want GET", got)
			}
		})
	}
}

// TestS1013_ProbesAndDebugGetNoSpan — the kubelet probes and the JWT-gated
// /debug/ surface are EXCLUDED from span creation (not marked), so an SLI
// query needs no filter. The decision is made on the ROUTE: an unmatched
// "/healthz" is a client request like any other and IS spanned, which also
// shows the arm is not vacuous (the harness does export spans).
//
// MUTATIONS (each RED, shown in the PR): spanned() returning true for every
// route; "/readyz" removed from unspannedRoutes; the /debug/ prefix check
// removed.
func TestS1013_ProbesAndDebugGetNoSpan(t *testing.T) {
	mux := http.NewServeMux()
	for _, p := range []string{"GET /health", "GET /readyz", "GET /debug/vars", "GET /debug/pprof/", "GET /call"} {
		mux.Handle(p, status(http.StatusOK))
	}
	spans := serverSpans(t, mux,
		httptest.NewRequest(http.MethodGet, "/health", nil),
		httptest.NewRequest(http.MethodGet, "/readyz", nil),
		httptest.NewRequest(http.MethodGet, "/debug/vars", nil),
		httptest.NewRequest(http.MethodGet, "/debug/pprof/heap", nil),
		httptest.NewRequest(http.MethodGet, "/call", nil),
		httptest.NewRequest(http.MethodGet, "/healthz", nil),
	)
	var names []string
	for _, s := range spans {
		names = append(names, s.Name)
	}
	if got, want := strings.Join(names, ","), "GET /call,GET"; got != want {
		t.Fatalf("exported spans [%s], want [%s]: probes and /debug/ get none, /call and the unmatched /healthz one each", got, want)
	}
}
