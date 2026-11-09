package main

// main_stage_outcomes_span_1264_test.go — #1264: through the handler
// composition the pod serves (newServerHandler, over a real listener), a
// response's X-Snowplow-Stage-Outcomes reaches its Server span as
// snowplow.stages.failed / snowplow.stages.failure_reasons, and a response
// without it gets neither.
//
//   - a /call whose stages failed (a 200): both attributes, status Unset;
//   - an /export of two pages that each failed a different stage: the span
//     carries the export's MERGED outcomes (its own header), once;
//   - a /list (no header): neither attribute.
//
// The handlers are stubs: what is under test is that the wrapper is in the
// served composition (removing it from newServerHandler turns the first two
// arms red), not the resolver, whose arms live in internal/handlers/dispatchers.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/krateo-platformops/snowplow/internal/handlers"
)

const (
	s1264AttrFailed  = attribute.Key("snowplow.stages.failed")
	s1264AttrReasons = attribute.Key("snowplow.stages.failure_reasons")
)

// s1264PagedCall serves two pages (a full one, then one row), each reporting a
// different failed stage, as a RESTAction /call would.
func s1264PagedCall(w http.ResponseWriter, r *http.Request) {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	perPage, _ := strconv.Atoi(r.URL.Query().Get("perPage"))
	n, reason := perPage, "Forbidden"
	if page > 1 {
		n, reason = 1, "NotFound"
	}
	items := make([]string, n)
	for i := range items {
		items[i] = fmt.Sprintf(`{"id":%d}`, (page-1)*perPage+i)
	}
	w.Header().Set(handlers.HeaderStageOutcomes, `[{"name":"stage-p`+strconv.Itoa(page)+`","ok":false,"reason":"`+reason+`"}]`)
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"items":[%s]}`, strings.Join(items, ","))
}

func TestS1264_ServedCompositionPutsStageOutcomesOnTheServerSpan(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})

	mux := http.NewServeMux()
	mux.Handle("GET /call", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(handlers.HeaderStageOutcomes,
			`[{"name":"b","ok":false,"reason":"NotFound"},{"name":"a","ok":false,"reason":"Forbidden"},{"name":"c","ok":false,"reason":"Skipped","dependsOn":"a"}]`)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":{"aErr":[{"code":403}]}}`))
	}))
	mux.Handle("GET /export", handlers.Export(http.HandlerFunc(s1264PagedCall)))
	mux.Handle("GET /list", http.HandlerFunc(ok200))
	srv := httptest.NewServer(newServerHandler(mux))
	t.Cleanup(srv.Close)

	for _, target := range []string{
		"/call?apiVersion=v1&resource=restactions&namespace=ns&name=x",
		"/export?apiVersion=v1&resource=restactions&namespace=ns&name=x&format=json",
		"/list?category=x",
	} {
		resp, err := srv.Client().Get(srv.URL + target)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("SETUP: GET %s answered %d", target, resp.StatusCode)
		}
	}
	srv.Close() // waits for every handler, so every span has ended
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}

	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range exp.GetSpans().Snapshots() {
		if _, dup := byName[s.Name()]; dup {
			t.Fatalf("SETUP: two %q spans", s.Name())
		}
		byName[s.Name()] = s
	}
	attrs := func(name string) map[attribute.Key]attribute.Value {
		s, ok := byName[name]
		if !ok {
			t.Fatalf("no %q span; got %v", name, byName)
		}
		if s.Status().Code != codes.Unset {
			t.Fatalf("%s: status %v, want Unset for a 200", name, s.Status().Code)
		}
		out := map[attribute.Key]attribute.Value{}
		for _, kv := range s.Attributes() {
			out[kv.Key] = kv.Value
		}
		return out
	}
	want := func(name string, n int64, reasons ...string) {
		t.Helper()
		a := attrs(name)
		if a[s1264AttrFailed].AsInt64() != n || !slices.Equal(a[s1264AttrReasons].AsStringSlice(), reasons) {
			t.Fatalf("%s: %s=%v %s=%v, want %d %q", name, s1264AttrFailed, a[s1264AttrFailed].Emit(),
				s1264AttrReasons, a[s1264AttrReasons].Emit(), n, reasons)
		}
	}
	want("GET /call", 3, "Forbidden", "NotFound", "Skipped")
	want("GET /export", 2, "Forbidden", "NotFound")
	for k := range attrs("GET /list") {
		if k == s1264AttrFailed || k == s1264AttrReasons {
			t.Fatalf("GET /list carries %s: a response without stage outcomes must get neither attribute", k)
		}
	}
}
