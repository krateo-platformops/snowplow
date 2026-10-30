package main

// main_server_spans_test.go — #1013: every request snowplow serves produces
// EXACTLY ONE Server span, through the handler composition the pod serves
// (newServerHandler: CORS → tracing.NewServerHandler → gzip → mux), over a real
// HTTP listener.
//
// The platform API SLIs (acmp X5) are counts over SpanKind='Server' spans with
// ServiceName='snowplow'. A route that emits two Server spans per request
// double-counts; a route that emits none is invisible to availability and p95.
// The arm covers every route family main mounts, including the ones the issue
// names: GET /call, POST /call/read, GET /export (which re-dispatches the /call
// chain IN-PROCESS, possibly once per page) and the SSE stream GET/POST
// /refreshes (a long-lived, flushed response).
//
// Route patterns come from the production registrars where main has one
// (mountCallWriteRoutes, registerDebugRoutes); the rest are mounted with main's
// exact pattern strings over stub handlers, since what is under test is the
// span layer around the mux, not the handlers' bodies. GET /export uses the
// real handlers.Export over a paged stub /call so its in-process re-dispatch is
// exercised.
//
// MUTATIONS (each RED, shown in the PR):
//   - newServerHandler wraps tracing.NewServerHandler twice → 2 spans/request;
//   - tracing.spanned() also excludes "/refreshes" → the SSE routes get none.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func ok200(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{}`))
}

// sseStub stands in for handlers.Refreshes: an event stream flushed per event.
func sseStub(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	f, _ := w.(http.Flusher)
	for i := 0; i < 3; i++ {
		_, _ = fmt.Fprintf(w, "event: refresh\ndata: %d\n\n", i)
		if f != nil {
			f.Flush()
		}
	}
}

// exportInnerCalls counts /export's in-process re-dispatches into the /call
// lane, so the arm can prove it exercised more than one.
var exportInnerCalls atomic.Int32

// pagedCallStub serves perPage+1 rows in pages of perPage: a full first page
// and a one-row second page, so /export's internal pagination re-dispatches
// the /call lane twice for one client request.
func pagedCallStub(w http.ResponseWriter, r *http.Request) {
	exportInnerCalls.Add(1)
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	perPage, _ := strconv.Atoi(r.URL.Query().Get("perPage"))
	n := perPage
	if page > 1 {
		n = 1
	}
	items := make([]string, n)
	for i := range items {
		items[i] = fmt.Sprintf(`{"id":%d}`, (page-1)*perPage+i)
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"items":[%s]}`, strings.Join(items, ","))
}

// buildSpanTestMux mounts main's route table: production registrars where they
// exist, main's literal patterns otherwise.
func buildSpanTestMux() *http.ServeMux {
	mux := http.NewServeMux()
	stub := http.HandlerFunc(ok200)
	mux.Handle("GET /swagger/", stub)
	mux.Handle("GET /health", stub)
	mux.Handle("GET /readyz", stub)
	mux.Handle("GET /api-info/names", stub)
	mux.Handle("GET /list", stub)
	mux.Handle("GET /call", stub)
	mux.Handle("POST /call/read", middleware.BodyExtrasDecode(stub))
	mux.Handle("GET /export", handlers.Export(http.HandlerFunc(pagedCallStub)))
	mux.Handle("GET /refreshes", http.HandlerFunc(sseStub))
	mux.Handle("POST /refreshes", http.HandlerFunc(sseStub))
	mux.Handle("GET /rbac", stub)
	// Production registrar; the identity middleware is replaced by the stub
	// handler (no JWT / clientconfig in a unit test).
	mountCallWriteRoutes(mux, use.NewChain(), func(http.Handler) http.Handler { return stub })
	mux.Handle("GET /capabilities", stub)
	mux.Handle("POST /jq", stub)
	registerDebugRoutes(mux, use.NewChain(), debugAuthTestKeys)
	return mux
}

func TestS1013_ExactlyOneServerSpanPerRequest(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	srv := httptest.NewServer(newServerHandler(buildSpanTestMux()))
	t.Cleanup(srv.Close)

	type call struct {
		method, target, body string
		wantSpan             string // "" = no span (probe, debug, CORS preflight)
	}
	calls := []call{
		{"GET", "/call?apiVersion=v1&resource=configmaps&namespace=ns&name=" + nameSentinel1013, "", "GET /call"},
		{"POST", "/call/read?apiVersion=v1&resource=configmaps&namespace=ns&name=x", `{"extras":{"k":"v"}}`, "POST /call/read"},
		{"GET", "/export?apiVersion=v1&resource=configmaps&namespace=ns&name=x&format=json", "", "GET /export"},
		{"GET", "/refreshes?sub=a", "", "GET /refreshes"},
		{"POST", "/refreshes", `{"sub":["a"]}`, "POST /refreshes"},
		{"POST", "/call?apiVersion=v1&resource=configmaps&namespace=ns&name=x", `{}`, "POST /call"},
		{"PUT", "/call?apiVersion=v1&resource=configmaps&namespace=ns&name=x", `{}`, "PUT /call"},
		{"PATCH", "/call?apiVersion=v1&resource=configmaps&namespace=ns&name=x", `{}`, "PATCH /call"},
		{"DELETE", "/call?apiVersion=v1&resource=configmaps&namespace=ns&name=x", "", "DELETE /call"},
		{"POST", "/call/dry-run?apiVersion=v1&resource=configmaps&namespace=ns&name=x", `{}`, "POST /call/dry-run"},
		{"GET", "/list?category=x", "", "GET /list"},
		{"GET", "/rbac?name=x", "", "GET /rbac"},
		{"GET", "/api-info/names?apiVersion=v1", "", "GET /api-info/names"},
		{"GET", "/capabilities", "", "GET /capabilities"},
		{"POST", "/jq", `{}`, "POST /jq"},
		{"GET", "/swagger/index.html", "", "GET /swagger/"},
		{"GET", "/no-such-route", "", "GET"},
		{"GET", "/health", "", ""},
		{"GET", "/readyz", "", ""},
		{"GET", "/debug/vars", "", ""},
		{"OPTIONS", "/call", "", ""},
	}

	var want []string
	for _, c := range calls {
		var body io.Reader
		if c.body != "" {
			body = strings.NewReader(c.body)
		}
		req, err := http.NewRequest(c.method, srv.URL+c.target, body)
		if err != nil {
			t.Fatal(err)
		}
		if c.method == http.MethodOptions {
			req.Header.Set("Origin", "http://localhost:8080")
			req.Header.Set("Access-Control-Request-Method", http.MethodGet)
		}
		if c.body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", c.method, c.target, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if c.wantSpan != "" && c.wantSpan != "GET" && resp.StatusCode != http.StatusOK {
			t.Errorf("SETUP: %s %s answered %d; the arm needs each route actually served", c.method, c.target, resp.StatusCode)
		}
		if c.wantSpan != "" {
			want = append(want, c.wantSpan)
		}
	}
	// The span ends when the handler chain returns; the client can see the
	// last byte a moment before that, so close the listener (waits for every
	// in-flight handler) before reading the exporter.
	srv.Close()
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := exportInnerCalls.Load(); n < 2 {
		t.Fatalf("SETUP: /export re-dispatched the /call lane %d time(s); the arm needs a multi-page export", n)
	}

	var got []string
	for _, s := range exp.GetSpans() {
		if s.SpanKind != trace.SpanKindServer {
			t.Errorf("span %q has kind %v; snowplow's request layer emits Server spans only", s.Name, s.SpanKind)
		}
		route := ""
		for _, a := range s.Attributes {
			if a.Key == "http.route" {
				route = a.Value.AsString()
			}
		}
		if s.Name != "GET" && !strings.HasSuffix(s.Name, " "+route) {
			t.Errorf("span %q: http.route=%q does not match the span name", s.Name, route)
		}
		if strings.Contains(s.Name, nameSentinel1013) {
			t.Errorf("span name %q carries a name from the request", s.Name)
		}
		got = append(got, s.Name)
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("Server spans do not match one-per-served-request:\n got (%d): %v\nwant (%d): %v",
			len(got), got, len(want), want)
	}
}

const nameSentinel1013 = "zq1013-person-name"
