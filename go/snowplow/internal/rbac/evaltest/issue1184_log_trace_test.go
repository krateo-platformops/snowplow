package evaltest

// issue1184_log_trace_test.go — #1184: the RBAC evaluation's snapshot-miss line
// runs on the request's goroutine, two calls below EvaluateRBAC, in a function
// that took no context. A real record through the real decorator under a real
// span; RED when roleRefPermits or the leaf goes back to the plain form (PR).

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/tracing"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	ktesting "k8s.io/client-go/testing"
)

type log1184 struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *log1184) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *log1184) records(msg string) []map[string]any {
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

func TestIssue1184_SnapshotMissLogsTheRequestsTraceID(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetRBACSubGenForTest()
	t.Cleanup(cache.ResetRBACSubGenForTest)
	cache.ResetBindingsByGVRIndexForTest()
	t.Cleanup(cache.ResetBindingsByGVRIndexForTest)
	cache.ResetDanglingRoleRefsForTest()
	t.Cleanup(cache.ResetDanglingRoleRefsForTest)

	sink := &log1184{}
	log := slog.New(tracing.NewTraceCorrelationHandler(slog.NewJSONHandler(sink, &slog.HandlerOptions{Level: slog.LevelWarn})))
	prev := slog.Default()
	slog.SetDefault(log)
	t.Cleanup(func() { slog.SetDefault(prev) })

	sch := runtime.NewScheme()
	if err := rbacv1.AddToScheme(sch); err != nil {
		t.Fatalf("rbacv1.AddToScheme: %v", err)
	}
	const tok = "s1184"
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(sch, rbacListKinds(),
		clusterRoleBinding(tok+"-crb", tok+"-cr", groupSubject(tok+"-group")),
		clusterRole(tok+"-cr", rule([]string{""}, []string{"pods"}, []string{"get"})),
	)
	// The ClusterRole LIST stays failing, so the published snapshot is unsynced
	// and holds the binding but not its role: the miss this arm needs (#947).
	var released atomic.Bool
	dyn.PrependReactor("list", "clusterroles", func(ktesting.Action) (bool, runtime.Object, error) {
		if released.Load() {
			return false, nil, nil
		}
		return true, nil, errors.New("clusterroles LIST held by the test (#1184)")
	})
	rw, err := cache.NewResourceWatcher(context.Background(), dyn)
	if err != nil {
		t.Fatalf("NewResourceWatcher: %v", err)
	}
	t.Cleanup(rw.Stop)
	t.Cleanup(func() { released.Store(true) })
	cache.SetGlobal(rw)
	t.Cleanup(func() { cache.SetGlobal(nil) })
	waitFor947(t, "a snapshot holding this test's binding", func() bool {
		s := cache.RBACSnapshotForTest()
		return s != nil && !s.Synced && len(s.ClusterRoleBindings) == 1 && s.ClusterRoleBindings[0].Name == tok+"-crb"
	})

	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("s1184").Start(xcontext.BuildContext(context.Background(), xcontext.WithLogger(log)), "GET /call")
	t.Cleanup(func() { span.End() })

	opts := rbac.EvaluateOptions{Username: tok, Groups: []string{tok + "-group"}, Verb: "get", Resource: "pods"}
	if allowed, _, err := rbac.EvaluateRBAC(ctx, opts); err != nil || allowed {
		t.Fatalf("EvaluateRBAC: allowed=%v err=%v; want a deny", allowed, err)
	}

	recs := sink.records("rbac.evaluate.snapshot.miss")
	if len(recs) != 1 {
		t.Fatalf("PRECONDITION: want one snapshot.miss line for one evaluation, got %d", len(recs))
	}
	if got, want := recs[0]["trace_id"], span.SpanContext().TraceID().String(); got != want {
		t.Fatalf("#1184 RED: snapshot.miss carries trace_id %v, the request's span is %s", got, want)
	}
}
