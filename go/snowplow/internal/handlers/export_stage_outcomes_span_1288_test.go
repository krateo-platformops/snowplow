package handlers

// export_stage_outcomes_span_1288_test.go — #1288: an export's refused write
// stage (#970) is a deliberate non-execution, not a failure. Through the real
// export handler and resolver (the #970 harness), served inside a Server span
// through dispatchers.StageOutcomesSpan as main.go composes it:
//
//   - the X-Snowplow-Stage-Outcomes header still lists the write stage (it did
//     not succeed), now with the closed-set reason WriteRefused, never the
//     generic Error;
//   - the span reads snowplow.stages.failed=0, snowplow.stages.not_executed=1,
//     and no snowplow.stages.failure_reasons;
//   - nothing was sent (the recorder's count, the #970 control lives in
//     export_never_writes_test.go).
//
// The oracle is the stage the fixture declares (one read, one POST) and the
// header as this file decodes it, never api.SummarizeStageOutcomes.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/krateo-platformops/snowplow/internal/handlers/dispatchers"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// spanned1288 serves h inside a Server span, with the stage-outcomes wrapper
// between the span and the route, and returns the span's attributes.
func spanned1288(t *testing.T, h http.Handler) (http.Handler, func() map[attribute.Key]attribute.Value) {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tp.Tracer("t").Start(r.Context(), r.Method+" /export", trace.WithSpanKind(trace.SpanKindServer))
		dispatchers.StageOutcomesSpan(h).ServeHTTP(w, r.WithContext(ctx))
		span.End()
	})
	return wrapped, func() map[attribute.Key]attribute.Value {
		spans := exp.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("SETUP: %d spans exported, want 1", len(spans))
		}
		out := map[attribute.Key]attribute.Value{}
		for _, kv := range spans[0].Attributes {
			out[kv.Key] = kv.Value
		}
		return out
	}
}

func TestExport1288_ARefusedWriteIsNotExecutedNotAFailure(t *testing.T) {
	for _, l := range exportLanes970(writeRAName970) {
		t.Run(l.name, func(t *testing.T) {
			apisrv, serverURL := freshWriteHarness970(t)
			h, attrs := spanned1288(t, l.h())
			rec := serveAs970(h, l.method, l.target, l.body, serverURL)
			if rec.Code != http.StatusOK {
				t.Fatalf("export: %d %s", rec.Code, rec.Body.String())
			}
			if w := writes(apisrv.take()); len(w) != 0 {
				t.Fatalf("SETUP: the export sent write(s) %v", w)
			}
			v := rec.Header().Get(util.HeaderStageOutcomes)
			var rows []struct {
				Name   string `json:"name"`
				Reason string `json:"reason"`
			}
			if err := json.Unmarshal([]byte(v), &rows); err != nil {
				t.Fatalf("%s=%q is not a JSON array: %v", util.HeaderStageOutcomes, v, err)
			}
			if len(rows) != 1 || rows[0].Name != "notify" {
				t.Fatalf("header %s: want exactly the refused write stage notify listed (it did not succeed)", v)
			}
			if rows[0].Reason != api.StageReasonWriteRefused {
				t.Fatalf("header %s: the export's refused write reads %q, want %q (never the generic %q)",
					v, rows[0].Reason, api.StageReasonWriteRefused, api.StageReasonError)
			}
			a := attrs()
			if f, ok := a["snowplow.stages.failed"]; !ok || f.AsInt64() != 0 {
				t.Errorf("snowplow.stages.failed = %v (set=%v), want 0: nothing failed", f.Emit(), ok)
			}
			if n, ok := a["snowplow.stages.not_executed"]; !ok || n.AsInt64() != 1 {
				t.Errorf("snowplow.stages.not_executed = %v (set=%v), want 1", n.Emit(), ok)
			}
			if r, ok := a["snowplow.stages.failure_reasons"]; ok {
				t.Errorf("snowplow.stages.failure_reasons = %v, want unset: nothing failed", r.Emit())
			}
		})
	}
}
