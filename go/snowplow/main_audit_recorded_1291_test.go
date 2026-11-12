package main

// main_audit_recorded_1291_test.go — #1291 (AUDIT-4 item 4): the Server span of
// a request that wrote an audit record carries krateo.audit.recorded=true, so
// the tail sampler keeps its trace whole; the Server span of one that wrote
// none does not. Driven through the composition the pod serves
// (newServerHandler, newRequestChain, mountCallWriteRoutes) over a real
// listener.
//
//   W  a /call/dry-run write (the real handler) marks its Server span;
//   St a read whose write happened in a RESTAction stage — emitted on a child
//      span's context after that span ended, as a stage records after its
//      dial — marks the Server span too: the holder RecordedMiddleware
//      installs carries the mark, not the span that happens to be on ctx;
//   R  a read that wrote nothing leaves its Server span unmarked;
//   I  the audit layer adds no identity to a Server span (#848): the caller's
//      name and groups and a sentinel in the query never reach a span
//      attribute. (otelhttp records the User-Agent on every Server span by
//      itself, with or without a record; that is not the audit writer's.)

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/logtest"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

const sentinel1291 = "sentinel-zq1291"

func TestS1291_RecordedMarkerOnlyOnTheServerSpanOfARequestThatWroteARecord(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})
	rec := logtest.NewRecorder()
	audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
	t.Cleanup(func() { audit.SetDefault(nil) })

	got, ep := fakeAPIMain443(t)
	mux := http.NewServeMux()
	chain := newRequestChain(discardLogger1291())
	mountCallWriteRoutes(mux, chain, testUserConfig443(ep))
	// A read whose RESTAction stage wrote: the stage's record is emitted on the
	// context of the stage's own (ended) span.
	mux.Handle("GET /call", chain.Then(testUserConfig443(ep)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, stage := otel.Tracer("stage").Start(r.Context(), "stage dial", trace.WithSpanKind(trace.SpanKindClient))
		stage.End()
		audit.Emit(ctx, audit.Event{Action: "restaction", HTTPMethod: http.MethodPost, Verb: "post", Outcome: audit.OutcomeSuccess,
			StatusCode: http.StatusCreated, ServerAddress: "render.example", RESTActionNamespace: "ns", RESTActionName: "ra", RESTActionStage: "s"})
		ok200(w, r)
	}))))
	// A read that writes nothing.
	mux.Handle("GET /list", chain.Then(testUserConfig443(ep)(http.HandlerFunc(ok200))))
	srv := httptest.NewServer(newServerHandler(mux))
	t.Cleanup(srv.Close)

	send := func(method, target, body string) {
		t.Helper()
		var rdr io.Reader
		if body != "" {
			rdr = bytes.NewReader([]byte(body))
		}
		req, err := http.NewRequest(method, srv.URL+target, rdr)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("User-Agent", "portal/1.0") // otelhttp records the User-Agent on every Server span itself
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("SETUP: %s %s answered %d", method, target, resp.StatusCode)
		}
	}
	send(http.MethodPost, "/call/dry-run"+cmQ443+"&owner="+sentinel1291, cm443)
	send(http.MethodGet, "/call?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=ns&name=ra&owner="+sentinel1291, "")
	send(http.MethodGet, "/list?category=x&owner="+sentinel1291, "")
	srv.Close()
	if err := tp.ForceFlush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(got()); n != 1 {
		t.Fatalf("SETUP: the apiserver got %d requests, want the dry run's one", n)
	}
	if n := len(flatten1291(rec)); n != 2 {
		t.Fatalf("SETUP: %d audit records, want 2 (the dry run's, the stage's)", n)
	}

	want := map[string]bool{"POST /call/dry-run": true, "GET /call": true, "GET /list": false}
	seen := map[string]bool{}
	for _, s := range exp.GetSpans() {
		if s.SpanKind != trace.SpanKindServer {
			continue
		}
		marked := false
		for _, kv := range s.Attributes {
			if string(kv.Key) == "krateo.audit.recorded" && kv.Value.AsBool() {
				marked = true
			}
			for _, leak := range []string{sentinel1291, "alice", "devs"} {
				if strings.Contains(kv.Value.Emit(), leak) {
					t.Errorf("I: Server span %q attribute %s carries identity", s.Name, kv.Key)
				}
			}
		}
		w, ok := want[s.Name]
		if !ok {
			t.Fatalf("unexpected Server span %q", s.Name)
		}
		seen[s.Name] = true
		if marked != w {
			t.Errorf("Server span %q: krateo.audit.recorded=%t, want %t", s.Name, marked, w)
		}
	}
	for name := range want {
		if !seen[name] {
			t.Errorf("SETUP: no Server span %q", name)
		}
	}
}

func discardLogger1291() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func flatten1291(rec *logtest.Recorder) []logtest.Record {
	var out []logtest.Record
	for _, rs := range rec.Result() {
		out = append(out, rs...)
	}
	return out
}
