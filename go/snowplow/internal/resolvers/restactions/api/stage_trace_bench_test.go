package api

// stage_trace_bench_test.go — the cost of the resolve and stage spans with
// tracing OFF (the production default off the OTEL gate), measured on the REAL
// api.Resolve. It uses only APIs that exist without stage_trace.go, so the same
// file runs against the unedited resolver for the paired comparison:
//
//	go test -run '^$' -bench BenchmarkResolveThreeStages -benchmem -count 10 ./internal/resolvers/restactions/api/

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	"k8s.io/client-go/rest"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
)

func BenchmarkResolveThreeStages(b *testing.B) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"name":"x"}`))
	}))
	defer srv.Close()
	items := []*templates.API{
		{Name: "a", Path: "/ok", Verb: ptr.To(http.MethodGet)},
		{Name: "b", Path: "/ok", Verb: ptr.To(http.MethodGet), DependsOn: &templates.Dependency{Name: "a"}},
		{Name: "c", Path: "/ok", Verb: ptr.To(http.MethodGet), DependsOn: &templates.Dependency{Name: "b"}},
	}
	quiet := xcontext.BuildContext(iterResolveCtx(), xcontext.WithLogger(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))))
	ctx := cache.WithInternalEndpoint(quiet, &endpoints.Endpoint{ServerURL: srv.URL})
	opts := ResolveOptions{RC: &rest.Config{}, Items: items, RESTActionNamespace: "bench", RESTActionName: "bench"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Resolve(ctx, opts)
	}
}
