package cache

// issue1184_log_trace_test.go — #1184 for the two cache paths a request runs
// synchronously, which took no context before: a read off the informer store and
// a lazy informer registration. A real record goes through the real decorator
// (tracing.NewTraceCorrelationHandler over the JSON handler, at WARN, as the
// default logger) under a real SDK span; the id compared is the span's. Both arms
// go RED when their site is reverted to the plain form (shown in the PR). The
// record is written before the call returns: nothing waits.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/tracing"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

// log1184 is the default logger's stream: informers and workers of the same
// process may write it while the test reads it (#392).
type log1184 struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *log1184) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *log1184) records(t *testing.T, msg string) []map[string]any {
	t.Helper()
	l.mu.Lock()
	raw := append([]byte(nil), l.b.Bytes()...)
	l.mu.Unlock()
	var out []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var rec map[string]any
		if json.Unmarshal(line, &rec) == nil && rec["msg"] == msg {
			out = append(out, rec)
		}
	}
	return out
}

// spanned1184 installs the real log chain as the default logger and returns a
// context carrying a real span, with that span's trace id.
func spanned1184(t *testing.T) (context.Context, string, *log1184) {
	t.Helper()
	sink := &log1184{}
	prev := slog.Default()
	slog.SetDefault(slog.New(tracing.NewTraceCorrelationHandler(
		slog.NewJSONHandler(sink, &slog.HandlerOptions{Level: slog.LevelWarn}))))
	t.Cleanup(func() { slog.SetDefault(prev) })
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("s1184").Start(context.Background(), "GET /call", trace.WithSpanKind(trace.SpanKindServer))
	t.Cleanup(func() { span.End() })
	return ctx, span.SpanContext().TraceID().String(), sink
}

func assertTrace1184(t *testing.T, sink *log1184, msg, traceID string) {
	t.Helper()
	recs := sink.records(t, msg)
	if len(recs) == 0 {
		t.Fatalf("PRECONDITION: no %q line", msg)
	}
	for _, rec := range recs {
		if rec["trace_id"] != traceID {
			t.Fatalf("#1184 RED: %q carries trace_id %v, the request's span is %s", msg, rec["trace_id"], traceID)
		}
	}
}

func TestIssue1184_AStoreReadLogsTheRequestsTraceID(t *testing.T) {
	gvr := schema.GroupVersionResource{Group: "example.krateo.io", Version: "v1", Resource: "widgets1184"}
	rw, idx, stop := newIndexerBackedWatcher(t, gvr)
	defer stop()
	if err := idx.Add(&metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{Name: "bad", Namespace: "ns"}}); err != nil {
		t.Fatal(err)
	}
	ctx, traceID, sink := spanned1184(t)

	if _, hit := rw.GetObject(ctx, gvr, "ns", "bad"); hit {
		t.Fatal("an unrenderable slot must miss")
	}
	assertTrace1184(t, sink, "cache.store_item.unrenderable", traceID)
}

func TestIssue1184_ALazyRegistrationLogsTheRequestsTraceID(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	gvr := schema.GroupVersionResource{Group: "example.krateo.io", Version: "v1", Resource: "lazies1184"}
	scheme := runtime.NewScheme()
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		gvr: "Lazy1184List",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}:        "ClusterRoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterrolebindings"}: "ClusterRoleBindingList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"}:               "RoleList",
		{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "rolebindings"}:        "RoleBindingList",
	}, &unstructured.UnstructuredList{})
	rw, err := NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	rw.MarkEagerSet([]schema.GroupVersionResource{gvr})
	ctx, traceID, sink := spanned1184(t)

	// The GVR is in the eager set yet a request registers it: the falsifier
	// WARN, on the request's goroutine.
	if added, _ := rw.EnsureResourceTypeFor(ctx, gvr); !added {
		t.Fatal("PRECONDITION: the registration must run")
	}
	assertTrace1184(t, sink, "lazy-AddResourceType-unexpected", traceID)
}
