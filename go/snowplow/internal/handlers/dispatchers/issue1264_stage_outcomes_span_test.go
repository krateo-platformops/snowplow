// issue1264_stage_outcomes_span_test.go — #1264: a RESTAction response's
// Server span carries what its X-Snowplow-Stage-Outcomes header says
// (snowplow.stages.failed, snowplow.stages.failure_reasons), through
// StageOutcomesSpan, the wrapper main.go puts around the route table.
//
// THE ORACLE IS NOT THE INSTRUMENT. The expected count and reasons come from
// the stages the arm declared (which upstream answers it serves) and from the
// response header as this file's own decoder reads it, never from
// api.SummarizeStageOutcomes, the parser under test.
//
//   - P1 a 200 whose stages failed, from a REAL resolve (the in-process
//     apiserver of the #443 harness answers one stage 404): both attributes,
//     the right count, the sorted distinct reasons; status stays Unset;
//   - P2 an all-success 200: snowplow.stages.failed=0 and no reasons;
//   - P3 an L1 hit carries the outcomes stored with the cell it served (and a
//     cell stored all-success reads 0); nothing is invented for a hit;
//   - P4 a non-RESTAction route (the widgets handler; a plain stub) gets
//     neither attribute, and #1265's snowplow.serve.* is untouched;
//   - P5 the reasons are a closed set on the span too: an unexpected reason
//     reads "other";
//   - P6 no identity: no stage name, user or RESTAction name on the span.
package dispatchers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sort"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// s1264Span is one request's Server span as exported.
type s1264Span struct {
	attrs  map[attribute.Key]attribute.Value
	status codes.Code
	rec    *httptest.ResponseRecorder
}

// s1264Serve serves req through StageOutcomesSpan(h) inside a Server span, as
// main.go's composition does, and returns the exported span.
func s1264Serve(t *testing.T, h http.Handler, req *http.Request) s1264Span {
	t.Helper()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("t").Start(req.Context(), req.Method+" /call", trace.WithSpanKind(trace.SpanKindServer))
	rec := httptest.NewRecorder()
	StageOutcomesSpan(h).ServeHTTP(rec, req.WithContext(ctx))
	span.End()
	spans := exp.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("SETUP: %d spans exported, want 1", len(spans))
	}
	out := s1264Span{attrs: map[attribute.Key]attribute.Value{}, status: spans[0].Status.Code, rec: rec}
	for _, kv := range spans[0].Attributes {
		out.attrs[kv.Key] = kv.Value
	}
	return out
}

// s1264HeaderOracle decodes the response header with this file's own decoder:
// the number of listed entries and their distinct reasons, sorted.
func s1264HeaderOracle(t *testing.T, v string) (int, []string) {
	t.Helper()
	var rows []struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal([]byte(v), &rows); err != nil {
		t.Fatalf("SETUP: %s=%q is not a JSON array: %v", util.HeaderStageOutcomes, v, err)
	}
	var reasons []string
	for _, r := range rows {
		if !slices.Contains(reasons, r.Reason) {
			reasons = append(reasons, r.Reason)
		}
	}
	sort.Strings(reasons)
	return len(rows), reasons
}

// wantStages asserts the two attributes: failed == n, and the reasons slice
// equal to reasons (absent when reasons is nil).
func (s s1264Span) wantStages(t *testing.T, what string, n int, reasons []string) {
	t.Helper()
	f, ok := s.attrs[spanAttrStagesFailed]
	if !ok || f.Type() != attribute.INT64 || f.AsInt64() != int64(n) {
		t.Fatalf("%s: %s = %v (set=%v), want int %d; attrs=%v", what, spanAttrStagesFailed, f.Emit(), ok, n, s.attrs)
	}
	r, ok := s.attrs[spanAttrStagesFailureReasons]
	if reasons == nil {
		if ok {
			t.Fatalf("%s: %s = %v, want it unset", what, spanAttrStagesFailureReasons, r.Emit())
		}
		return
	}
	if !ok || r.Type() != attribute.STRINGSLICE || !slices.Equal(r.AsStringSlice(), reasons) {
		t.Fatalf("%s: %s = %v (set=%v), want %q", what, spanAttrStagesFailureReasons, r.Emit(), ok, reasons)
	}
}

func (s s1264Span) wantNoStages(t *testing.T, what string) {
	t.Helper()
	for _, k := range []attribute.Key{spanAttrStagesFailed, spanAttrStagesFailureReasons} {
		if v, ok := s.attrs[k]; ok {
			t.Fatalf("%s: %s = %v, want it unset (no stage outcomes in the response)", what, k, v.Emit())
		}
	}
}

func (s s1264Span) wantStatusUnset(t *testing.T, what string) {
	t.Helper()
	if s.status != codes.Unset {
		t.Fatalf("%s: span status %v, want Unset (an HTTP %d; semconv)", what, s.status, s.rec.Code)
	}
}

func (s s1264Span) wantNoIdentity(t *testing.T, what string, banned ...string) {
	t.Helper()
	for k, v := range s.attrs {
		for _, b := range banned {
			if strings.Contains(v.Emit(), b) {
				t.Fatalf("%s: span attribute %s=%v carries %q", what, k, v.Emit(), b)
			}
		}
	}
}

// s1264StoredRequest is serveStored's request (a stored RESTAction /call of
// obj, resolved for real against the #443 harness's apiserver).
func s1264StoredRequest(t *testing.T, f *in443API, ctx context.Context, obj map[string]any) (http.Handler, *http.Request) {
	t.Helper()
	cr := &unstructured.Unstructured{Object: obj}
	t.Cleanup(setFetchObjectForTest(func(*http.Request) objects.Result {
		return objects.Result{GVR: h1RAGVR, Unstructured: cr.DeepCopy()}
	}))
	req := httptest.NewRequest(http.MethodGet, "/call", nil).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+in443JWT())
	return &restActionHandler{authnNS: psAuthnNS, saRC: &rest.Config{Host: f.srv.URL}}, req
}

func TestIssue1264_P1_A200WithFailedStagesCarriesCountAndSortedReasons(t *testing.T) {
	f := in443Setup(t)
	// target: alice may read it (succeeds). missing-a and missing-b: the
	// apiserver answers 404 (NotFound, twice: the reason appears once).
	stages := []map[string]any{
		targetStage(),
		{"name": "missing-a", "continueOnError": true, "errorKey": "aErr",
			"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps/does-not-exist-a"},
		{"name": "missing-b", "continueOnError": true, "errorKey": "bErr",
			"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps/does-not-exist-b"},
	}
	h, req := s1264StoredRequest(t, f, in443Ctx(f, psAlice), in443RA(psRAName, "", stages...))
	s := s1264Serve(t, h, req)
	if s.rec.Code != http.StatusOK {
		t.Fatalf("SETUP: code=%d body=%s; the arm needs a 200 whose stages failed", s.rec.Code, s.rec.Body.String())
	}
	v := s.rec.Header().Get(util.HeaderStageOutcomes)
	n, reasons := s1264HeaderOracle(t, v)
	if n != 2 || !slices.Equal(reasons, []string{api.StageReasonNotFound}) {
		t.Fatalf("SETUP: header %s lists %d entries %q; the arm declared 2 NotFound stages", v, n, reasons)
	}
	s.wantStages(t, "two NotFound stages", 2, []string{api.StageReasonNotFound})
	s.wantStatusUnset(t, "a 200 whose stages failed")
	s.wantNoIdentity(t, "P6", "missing-a", "missing-b", "target", psAlice, psRAName, psTargetNS)
}

func TestIssue1264_P1b_MixedReasonsAreSortedAndDistinct(t *testing.T) {
	f := in443Setup(t)
	// The #443 outcome stages: target ok, missing NotFound, a write refused by
	// the dry run (StageNotExecuted) on an inline resolve.
	obj := in443RA("draft-1264", "", outcomeStages443()...)
	rec0 := serveInline(t, f, in443Ctx(f, psAlice), obj)
	n, reasons := s1264HeaderOracle(t, rec0.Header().Get(util.HeaderStageOutcomes))
	if n < 2 || len(reasons) < 2 {
		t.Fatalf("SETUP: the inline draft's header %q has %d entries %q; the arm needs two reasons",
			rec0.Header().Get(util.HeaderStageOutcomes), n, reasons)
	}
	// Serve the same draft again, through the span wrapper.
	body, _ := json.Marshal(map[string]any{"extras": map[string]any{}, "object": obj})
	req := httptest.NewRequest(http.MethodPost, "/call/read?apiVersion="+h1RAGVR.Group+"/"+h1RAGVR.Version+
		"&resource=restactions&namespace="+h1NS+"&name=draft-1264", strings.NewReader(string(body))).
		WithContext(in443Ctx(f, psAlice))
	req.Header.Set("Authorization", "Bearer "+in443JWT())
	s := s1264Serve(t, in443Handler(f), req)
	if s.rec.Code != http.StatusOK {
		t.Fatalf("SETUP: code=%d", s.rec.Code)
	}
	n, reasons = s1264HeaderOracle(t, s.rec.Header().Get(util.HeaderStageOutcomes))
	if !sort.StringsAreSorted(reasons) {
		t.Fatalf("SETUP: oracle reasons unsorted %q", reasons)
	}
	s.wantStages(t, "an inline draft with NotFound and StageNotExecuted stages", n, reasons)
	if !slices.Contains(reasons, api.StageReasonNotFound) || !slices.Contains(reasons, api.StageReasonNotExecuted) {
		t.Fatalf("SETUP: reasons %q, want NotFound and StageNotExecuted", reasons)
	}
	s.wantStatusUnset(t, "an inline 200 whose stages failed")
}

func TestIssue1264_P2_AllSuccessIsZeroWithNoReasons(t *testing.T) {
	f := in443Setup(t)
	h, req := s1264StoredRequest(t, f, in443Ctx(f, psAlice), in443RA(psRAName, "", targetStage()))
	s := s1264Serve(t, h, req)
	if s.rec.Code != http.StatusOK || s.rec.Header().Get(util.HeaderStageOutcomes) != "[]" {
		t.Fatalf("SETUP: code=%d header=%q; the arm needs an all-success 200", s.rec.Code, s.rec.Header().Get(util.HeaderStageOutcomes))
	}
	s.wantStages(t, "all succeeded", 0, nil)
	s.wantStatusUnset(t, "all succeeded")
}

func TestIssue1264_P3_AHitCarriesTheOutcomesStoredWithItsCell(t *testing.T) {
	tg := s1265Targets[0] // restactions
	if tg.name != "restactions" {
		t.Fatalf("SETUP: target %q", tg.name)
	}
	stored := `[{"name":"list","ok":false,"reason":"Forbidden"},{"name":"get","ok":false,"reason":"NotFound"},` +
		`{"name":"write","ok":false,"reason":"Skipped","dependsOn":"get"}]`
	for _, c := range []struct {
		name, stored string
		n            int
		reasons      []string
	}{
		{"stored-failures", stored, 3, []string{api.StageReasonForbidden, api.StageReasonNotFound, api.StageReasonSkipped}},
		{"stored-all-success", "", 0, nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := s1265Setup(t, tg)
			ctx := h1ReqCtx(h1User)
			e.warmCell(t, ctx, &cache.ResolvedEntry{RawJSON: []byte(`{"cached":1}` + "\n"), StageOutcomes: c.stored})
			req := httptest.NewRequest("GET", "/call", nil).WithContext(ctx)
			s := s1264Serve(t, tg.handler(), req)
			e.wantResolves(t, "a hit", 0)
			if got := s.attrs[spanAttrServeOutcome].AsString(); got != "fresh" {
				t.Fatalf("SETUP: %s=%q, want fresh (an L1 hit)", spanAttrServeOutcome, got)
			}
			s.wantStages(t, "a hit of a cell stored with "+c.name, c.n, c.reasons)
			s.wantStatusUnset(t, "a hit")
			s.wantNoIdentity(t, "P6", "list", "get", "write", h1User, h1RAName)
		})
	}
}

func TestIssue1264_P4_ANonRESTActionRouteIsUnaffected(t *testing.T) {
	t.Run("widgets", func(t *testing.T) {
		tg := s1265Targets[1]
		if tg.name != "widgets" {
			t.Fatalf("SETUP: target %q", tg.name)
		}
		e := s1265Setup(t, tg)
		req := httptest.NewRequest("GET", "/call", nil).WithContext(h1ReqCtx(h1User))
		s := s1264Serve(t, tg.handler(), req)
		e.wantResolves(t, "a widget miss", 1)
		s.wantNoStages(t, "a widgets /call")
		// #1265's attributes are exactly as its own arm pins them.
		if got := s.attrs[spanAttrServeOutcome].AsString(); got != "cold" || len(s.attrs) != 2 {
			t.Fatalf("a widgets cold serve's attributes = %v, want exactly %s=cold and its cause", s.attrs, spanAttrServeOutcome)
		}
	})
	t.Run("stub", func(t *testing.T) {
		s := s1264Serve(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		}), httptest.NewRequest("GET", "/list", nil))
		s.wantNoStages(t, "a route that sets no stage-outcomes header")
		if len(s.attrs) != 0 {
			t.Fatalf("a plain route's span gained attributes %v", s.attrs)
		}
	})
}

func TestIssue1264_P5_AnUnexpectedReasonReadsOther(t *testing.T) {
	const raw = "Leaky-" + psSentinel
	s := s1264Serve(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(util.HeaderStageOutcomes,
			`[{"name":"a","ok":false,"reason":"`+raw+`"},{"name":"b","ok":false,"reason":"Timeout"}]`)
		w.WriteHeader(http.StatusOK)
	}), httptest.NewRequest("GET", "/call", nil))
	s.wantStages(t, "an unexpected reason", 2, []string{api.StageReasonTimeout, api.StageReasonOther}) // byte order
	s.wantNoIdentity(t, "P5", raw, psSentinel)
}
