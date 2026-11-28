package api

// stage_trace_test.go — a span per RESTAction resolve and per stage
// (stage_trace.go). Every arm drives the REAL api.Resolve over an httptest
// server, with a real SDK tracer provider and a span recorder, so the arms read
// what the exporter would be handed.

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/ptr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"k8s.io/client-go/rest"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/tracing"
)

// withRecorder installs a recording provider as the global one and turns the
// tracing gate on, restoring both.
func withRecorder(t *testing.T) *tracetest.SpanRecorder {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	restore := tracing.SetActiveForTest(true)
	t.Cleanup(func() {
		restore()
		otel.SetTracerProvider(prev)
		_ = tp.Shutdown(context.Background())
	})
	return sr
}

// traceFixture serves /ok and /fail (403, a Status body).
func traceFixture(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if strings.HasPrefix(r.URL.Path, "/fail") {
			w.WriteHeader(http.StatusForbidden)
			fmt.Fprint(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":403,"reason":"Forbidden","message":"no"}`)
			return
		}
		fmt.Fprint(w, `{"name":"x"}`)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func runTraced(ctx context.Context, srv *httptest.Server, items []*templates.API, extras map[string]any) {
	ctx = cache.WithInternalEndpoint(ctx, &endpoints.Endpoint{ServerURL: srv.URL})
	_ = Resolve(ctx, ResolveOptions{
		RC:                  &rest.Config{},
		Items:               items,
		Extras:              extras,
		RESTActionNamespace: "trace-ns",
		RESTActionName:      "trace-ra",
	})
}

func threeStages() []*templates.API {
	return []*templates.API{
		{Name: "a", Path: "/ok", Verb: ptr.To(http.MethodGet)},
		{Name: "b", Path: `${ "/ok/" + .n }`, Verb: ptr.To(http.MethodGet),
			DependsOn: &templates.Dependency{Name: "a", Iterator: ptr.To(".vals")}},
		{Name: "c", Path: "/ok", Verb: ptr.To(http.MethodGet), DependsOn: &templates.Dependency{Name: "b"}},
	}
}

func attrMap(s sdktrace.ReadOnlySpan) map[string]attribute.Value {
	m := map[string]attribute.Value{}
	for _, kv := range s.Attributes() {
		m[string(kv.Key)] = kv.Value
	}
	return m
}

func spansNamed(sr *tracetest.SpanRecorder, name string) []sdktrace.ReadOnlySpan {
	var out []sdktrace.ReadOnlySpan
	for _, s := range sr.Ended() {
		if s.Name() == name {
			out = append(out, s)
		}
	}
	return out
}

func TestStageTrace_ResolveSpanWithThreeStageChildren(t *testing.T) {
	t.Setenv("OTEL_ENABLED", "true") // the dial wraps its transport (tracing.WrapTransport)
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	sr := withRecorder(t)
	srv := traceFixture(t)

	runTraced(iterResolveCtx(), srv, threeStages(),
		map[string]any{"vals": []any{map[string]any{"n": "1"}, map[string]any{"n": "2"}}})

	res := spansNamed(sr, SpanRestactionResolve)
	if len(res) != 1 {
		t.Fatalf("want exactly one %s span, got %d", SpanRestactionResolve, len(res))
	}
	stages := spansNamed(sr, SpanRestactionStage)
	if len(stages) != 3 {
		t.Fatalf("want 3 %s spans, got %d", SpanRestactionStage, len(stages))
	}
	ra := attrMap(res[0])
	if ra[attrRASource].AsString() != sourceCustomer || ra[attrRANamespace].AsString() != "trace-ns" ||
		ra[attrRAName].AsString() != "trace-ra" || ra[attrRAStageCount].AsInt64() != 3 ||
		ra[attrRAOutcome].AsString() != outcomeOK {
		t.Fatalf("resolve span attributes wrong: %v", res[0].Attributes())
	}

	want := []struct {
		name, shape string
		items       int64
	}{{"a", shapeSingle, 1}, {"b", shapeIterator, 2}, {"c", shapeSingle, 1}}
	for i, s := range stages { // Ended() is in end order, and stages run one after another
		if s.Parent().SpanID() != res[0].SpanContext().SpanID() || s.SpanContext().TraceID() != res[0].SpanContext().TraceID() {
			t.Errorf("stage %d is not a child of the resolve span", i)
		}
		a := attrMap(s)
		if a[attrStage].AsString() != want[i].name || a[attrStageShape].AsString() != want[i].shape ||
			a[attrStageItems].AsInt64() != want[i].items || a[attrStageTarget].AsString() != targetInCluster ||
			a[attrStageOutcome].AsString() != outcomeOK || a[attrRANamespace].AsString() != "trace-ns" ||
			a[attrRAName].AsString() != "trace-ra" {
			t.Errorf("stage %d attributes wrong: %v", i, s.Attributes())
		}
		if i > 0 && s.StartTime().Before(stages[i-1].EndTime()) {
			t.Errorf("stage %d started before stage %d ended: stages are sequential", i, i-1)
		}
	}

	// Every other span (the dial's client spans, when the dial is instrumented)
	// hangs under a stage span, never directly under the resolve.
	stageIDs := map[string]bool{}
	clients := 0
	for _, s := range stages {
		stageIDs[s.SpanContext().SpanID().String()] = true
	}
	for _, s := range sr.Ended() {
		if s.Name() == SpanRestactionResolve || s.Name() == SpanRestactionStage {
			continue
		}
		clients++
		if !stageIDs[s.Parent().SpanID().String()] {
			t.Errorf("span %q is not a child of a stage span", s.Name())
		}
	}
	if clients == 0 {
		t.Fatal("no client span was recorded: the stage-tree arm is vacuous")
	}
}

// A stage that failed an item, a stage that never ran (a write behind a failed
// stage), and a stage the loop stopped on are each marked.
func TestStageTrace_ItemErrorsNotExecutedAndFailed(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	t.Setenv("RESOLVER_ITER_PARALLELISM", "1")
	sr := withRecorder(t)
	srv := traceFixture(t)

	items := []*templates.API{
		{Name: "bad", Path: "/fail", Verb: ptr.To(http.MethodGet), ContinueOnError: ptr.To(true), ErrorKey: ptr.To("badErr")},
		{Name: "w", Path: "/ok", Verb: ptr.To(http.MethodPost), DependsOn: &templates.Dependency{Name: "bad"}},
	}
	runTraced(iterResolveCtx(), srv, items, nil)

	out := map[string]string{}
	for _, s := range spansNamed(sr, SpanRestactionStage) {
		a := attrMap(s)
		out[a[attrStage].AsString()] = a[attrStageOutcome].AsString()
	}
	if out["bad"] != outcomeItemErrors {
		t.Errorf("a stage with a failed item must read %s, got %q", outcomeItemErrors, out["bad"])
	}
	if out["w"] != outcomeNotExec {
		t.Errorf("a write behind a failed stage must read %s, got %q", outcomeNotExec, out["w"])
	}

	// The loop stops on the first stage when the request is gone: failed.
	sr2 := withRecorder(t)
	ctx, cancel := context.WithCancel(iterResolveCtx())
	cancel()
	runTraced(ctx, srv, threeStages(), nil)
	st := spansNamed(sr2, SpanRestactionStage)
	if len(st) != 1 || attrMap(st[0])[attrStageOutcome].AsString() != outcomeFailed {
		t.Fatalf("a truncating stop must leave one stage span marked %s, got %d spans", outcomeFailed, len(st))
	}
	if r := spansNamed(sr2, SpanRestactionResolve); len(r) != 1 {
		t.Fatalf("the resolve span must still end, got %d", len(r))
	}
}

func TestStageTrace_SourceFromContext(t *testing.T) {
	srv := traceFixture(t)
	for _, c := range []struct {
		want string
		ctx  func() context.Context
	}{
		{sourceCustomer, iterResolveCtx},
		{sourceRefresher, func() context.Context {
			return cache.WithBackgroundResolveOrigin(iterResolveCtx(), cache.BackgroundOriginRefresher)
		}},
		{sourcePrewarm, func() context.Context { return cache.WithPrewarmPath(iterResolveCtx()) }},
		{sourcePrewarm, func() context.Context {
			return cache.WithBackgroundResolveOrigin(iterResolveCtx(), cache.BackgroundOriginPrewarmEngineBoot)
		}},
	} {
		sr := withRecorder(t)
		runTraced(c.ctx(), srv, threeStages()[:1], nil)
		res := spansNamed(sr, SpanRestactionResolve)
		if len(res) != 1 || attrMap(res[0])[attrRASource].AsString() != c.want {
			t.Errorf("want source %q, got %v", c.want, res)
		}
	}
}

// Every attribute key and every enum value on these spans is in the closed set
// stage_trace.go declares; a value outside it is a leak path.
func TestStageTrace_AttributesAreAClosedSet(t *testing.T) {
	t.Setenv("CLIENT_MAX_RETRIES", "0")
	sr := withRecorder(t)
	srv := traceFixture(t)
	items := append(threeStages(),
		&templates.API{Name: "bad", Path: "/fail", Verb: ptr.To(http.MethodGet), ContinueOnError: ptr.To(true)},
		&templates.API{Name: "w", Path: "/ok", Verb: ptr.To(http.MethodPost), DependsOn: &templates.Dependency{Name: "bad"}})
	runTraced(iterResolveCtx(), srv, items, map[string]any{"vals": []any{map[string]any{"n": "1"}}})

	keys := map[string]bool{}
	for _, k := range TraceAttrKeys() {
		keys[k] = true
	}
	enums := TraceAttrEnumValues()
	var n int
	for _, s := range sr.Ended() {
		if s.Name() != SpanRestactionResolve && s.Name() != SpanRestactionStage {
			continue
		}
		for _, kv := range s.Attributes() {
			n++
			k := string(kv.Key)
			if !keys[k] {
				t.Errorf("span %q carries attribute %q outside the closed set", s.Name(), k)
			}
			if vals, ok := enums[k]; ok {
				found := false
				for _, v := range vals {
					found = found || v == kv.Value.AsString()
				}
				if !found {
					t.Errorf("attribute %q carries %q, outside its closed values %v", k, kv.Value.AsString(), vals)
				}
			}
			// No identity, ever: the fixture's caller is "iter-falsifier-user".
			if strings.Contains(kv.Value.Emit(), "iter-falsifier-user") || strings.Contains(kv.Value.Emit(), srv.URL) {
				t.Errorf("attribute %q carries identity or a URL: %q", k, kv.Value.Emit())
			}
		}
	}
	if n == 0 {
		t.Fatal("no attribute was checked: the arm is vacuous")
	}
}

// BenchmarkStageTraceOff is the span code's own cost per three-stage resolve
// with tracing off: the figure the paired BenchmarkResolveThreeStages hides in
// loopback noise.
func BenchmarkStageTraceOff(b *testing.B) {
	defer tracing.SetActiveForTest(false)()
	r := &resolveRun{ctx: context.Background()}
	opts := ResolveOptions{Items: threeStages()}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, rt := startResolveTrace(context.Background(), opts)
		for _, id := range []string{"a", "b", "c"} {
			_, st := r.beginStageTrace(id)
			st.planned(false, true, 1)
			st.noteItemErrs(nil)
			st.end(false, false, "")
		}
		rt.end(nil)
	}
}

// With tracing off nothing is recorded and the hot path allocates nothing.
func TestStageTrace_OffRecordsNothingAndAllocatesNothing(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() { otel.SetTracerProvider(prev) })
	defer tracing.SetActiveForTest(false)()

	r := &resolveRun{ctx: context.Background()}
	opts := ResolveOptions{Items: threeStages()}
	if got := testing.AllocsPerRun(100, func() {
		ctx, rt := startResolveTrace(context.Background(), opts)
		_ = ctx
		_, st := r.beginStageTrace("a")
		st.planned(true, true, 3)
		st.noteItemErrs(nil)
		st.end(false, false, "")
		r.traceNotRun("b")
		rt.end(nil)
	}); got != 0 {
		t.Fatalf("tracing off must not allocate, got %v allocs/op", got)
	}
	runTraced(iterResolveCtx(), traceFixture(t), threeStages()[:1], nil)
	if n := len(sr.Ended()); n != 0 {
		t.Fatalf("tracing off recorded %d spans", n)
	}
}
