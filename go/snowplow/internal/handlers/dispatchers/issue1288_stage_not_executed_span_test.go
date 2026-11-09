// issue1288_stage_not_executed_span_test.go — #1288: on a RESTAction
// response's Server span, snowplow.stages.failed counts only the stages that
// FAILED; a stage snowplow deliberately did not execute (a dry run's refused
// write) is counted in snowplow.stages.not_executed and names no failure
// reason. The X-Snowplow-Stage-Outcomes header still lists every stage that
// did not succeed (#904).
//
// Every arm is a REAL resolve (the #443 harness's in-process apiserver),
// served through StageOutcomesSpan inside a Server span, as main.go composes
// it. THE ORACLE IS NOT THE INSTRUMENT: the expected counts are the stages the
// arm declared, cross-checked against the response header as this file's own
// decoder reads it, never api.SummarizeStageOutcomes.
//
//   - D1 a dry run whose only non-success is the refused write: failed=0,
//     not_executed=1, no failure_reasons; the header still lists the write;
//   - D2 a real 404 stage: failed=1, not_executed=0, [NotFound];
//   - D3 a 404 plus a write that depends on it (Skipped): failed=2,
//     [NotFound Skipped], and the write was not sent;
//   - D4 a dry run with a 404, a write skipped after it and a refused write:
//     failed=2, not_executed=1, [NotFound Skipped];
//   - D5 the truncated summary: failed is its N, not_executed is not set.
//
// The export's refused write (WriteRefused) is armed through the export
// handler in internal/handlers (export_stage_outcomes_span_1288_test.go).
package dispatchers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"go.opentelemetry.io/otel/attribute"

	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

// s1288Row is one header row as this file decodes it.
type s1288Row struct {
	Name      string `json:"name"`
	Reason    string `json:"reason"`
	DependsOn string `json:"dependsOn"`
}

// s1288Rows decodes the response header by stage name.
func s1288Rows(t *testing.T, v string) map[string]s1288Row {
	t.Helper()
	var rows []s1288Row
	if err := json.Unmarshal([]byte(v), &rows); err != nil {
		t.Fatalf("SETUP: %s=%q is not a JSON array: %v", util.HeaderStageOutcomes, v, err)
	}
	out := map[string]s1288Row{}
	for _, r := range rows {
		out[r.Name] = r
	}
	return out
}

// wantHeader asserts the header lists exactly want (its #904 meaning: every
// stage that did not succeed, deliberate non-executions included).
func (s s1264Span) wantHeader(t *testing.T, what string, want ...s1288Row) {
	t.Helper()
	v := s.rec.Header().Get(util.HeaderStageOutcomes)
	got := s1288Rows(t, v)
	if len(got) != len(want) {
		t.Fatalf("%s: header %s lists %d stage(s), want %d %+v", what, v, len(got), len(want), want)
	}
	for _, w := range want {
		if g, ok := got[w.Name]; !ok || g != w {
			t.Fatalf("%s: header %s: stage %q = %+v (listed=%v), want %+v", what, v, w.Name, g, ok, w)
		}
	}
}

// wantNotExecuted asserts snowplow.stages.not_executed == n.
func (s s1264Span) wantNotExecuted(t *testing.T, what string, n int) {
	t.Helper()
	v, ok := s.attrs[attribute.Key(spanAttrStagesNotExecuted)]
	if !ok || v.Type() != attribute.INT64 || v.AsInt64() != int64(n) {
		t.Fatalf("%s: %s = %v (set=%v), want int %d; attrs=%v", what, spanAttrStagesNotExecuted, v.Emit(), ok, n, s.attrs)
	}
}

// s1288Inline is an inline draft (POST /call/read with the object: a dry run)
// served through the span wrapper.
func s1288Inline(t *testing.T, f *in443API, obj map[string]any) s1264Span {
	t.Helper()
	body, err := json.Marshal(map[string]any{"extras": map[string]any{}, "object": obj})
	if err != nil {
		t.Fatal(err)
	}
	name := obj["metadata"].(map[string]any)["name"].(string)
	req := httptest.NewRequest(http.MethodPost, "/call/read?apiVersion="+h1RAGVR.Group+"/"+h1RAGVR.Version+
		"&resource=restactions&namespace="+h1NS+"&name="+name, bytes.NewReader(body)).
		WithContext(in443Ctx(f, psAlice))
	req.Header.Set("Authorization", "Bearer "+in443JWT())
	s := s1264Serve(t, in443Handler(f), req)
	if s.rec.Code != http.StatusOK {
		t.Fatalf("SETUP: inline draft code=%d body=%s", s.rec.Code, s.rec.Body.String())
	}
	assertInlineEcho(t, s.rec, "inline draft")
	return s
}

func s1288Missing() map[string]any {
	return map[string]any{"name": "missing", "continueOnError": true, "errorKey": "missingErr",
		"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps/does-not-exist"}
}

func s1288Create(name string) map[string]any {
	return map[string]any{"name": name, "verb": "POST", "continueOnError": true, "errorKey": name + "Err",
		"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps", "payload": `{"metadata":{"name":"evil"}}`}
}

// s1288AfterMissing is a write that depends on the missing stage, without
// continueOnError: #904 does not send it.
func s1288AfterMissing() map[string]any {
	return map[string]any{"name": "cleanup", "verb": "DELETE", "errorKey": "cleanupErr",
		"path":      "/api/v1/namespaces/" + psTargetNS + "/configmaps/" + psTargetObj,
		"dependsOn": map[string]any{"name": "missing"}}
}

func (f *in443API) s1288Writes() []in443Req {
	var out []in443Req
	for _, r := range f.requests() {
		if r.method != http.MethodGet {
			out = append(out, r)
		}
	}
	return out
}

func TestIssue1288_D1_ADryRunsRefusedWriteIsNotAFailure(t *testing.T) {
	f := in443Setup(t)
	s := s1288Inline(t, f, in443RA("draft-1288-d1", "", targetStage(), s1288Create("create")))
	s.wantHeader(t, "D1", s1288Row{Name: "create", Reason: api.StageReasonNotExecuted})
	s.wantStages(t, "a dry run whose only non-success is its refused write", 0, nil)
	s.wantNotExecuted(t, "D1", 1)
	s.wantStatusUnset(t, "D1")
	if w := f.s1288Writes(); len(w) != 0 {
		t.Fatalf("SETUP: a dry run sent write(s) %+v", w)
	}
}

func TestIssue1288_D2_ARealNotFoundIsOneFailure(t *testing.T) {
	f := in443Setup(t)
	h, req := s1264StoredRequest(t, f, in443Ctx(f, psAlice), in443RA(psRAName, "", targetStage(), s1288Missing()))
	s := s1264Serve(t, h, req)
	if s.rec.Code != http.StatusOK {
		t.Fatalf("SETUP: code=%d body=%s", s.rec.Code, s.rec.Body.String())
	}
	s.wantHeader(t, "D2", s1288Row{Name: "missing", Reason: api.StageReasonNotFound})
	s.wantStages(t, "a 404 stage", 1, []string{api.StageReasonNotFound})
	s.wantNotExecuted(t, "D2", 0)
}

func TestIssue1288_D3_ANotFoundAndItsSkippedWriteAreTwoFailures(t *testing.T) {
	f := in443Setup(t)
	h, req := s1264StoredRequest(t, f, in443Ctx(f, psAlice),
		in443RA(psRAName, "", targetStage(), s1288Missing(), s1288AfterMissing()))
	s := s1264Serve(t, h, req)
	if s.rec.Code != http.StatusOK {
		t.Fatalf("SETUP: code=%d body=%s", s.rec.Code, s.rec.Body.String())
	}
	if w := f.s1288Writes(); len(w) != 0 {
		t.Fatalf("SETUP: the write over a failed stage was sent: %+v", w)
	}
	s.wantHeader(t, "D3",
		s1288Row{Name: "missing", Reason: api.StageReasonNotFound},
		s1288Row{Name: "cleanup", Reason: api.StageReasonSkipped, DependsOn: "missing"})
	// Skipped is counted: it only ever follows a real failure.
	s.wantStages(t, "a 404 and its skipped write", 2, []string{api.StageReasonNotFound, api.StageReasonSkipped})
	s.wantNotExecuted(t, "D3", 0)
}

func TestIssue1288_D4_AMixCountsFailuresAndDeliberateRefusalsApart(t *testing.T) {
	f := in443Setup(t)
	s := s1288Inline(t, f, in443RA("draft-1288-d4", "",
		targetStage(), s1288Missing(), s1288AfterMissing(), s1288Create("create")))
	s.wantHeader(t, "D4",
		s1288Row{Name: "missing", Reason: api.StageReasonNotFound},
		s1288Row{Name: "cleanup", Reason: api.StageReasonSkipped, DependsOn: "missing"},
		s1288Row{Name: "create", Reason: api.StageReasonNotExecuted})
	s.wantStages(t, "a dry run with a 404, its skipped write and a refused write", 2,
		[]string{api.StageReasonNotFound, api.StageReasonSkipped})
	s.wantNotExecuted(t, "D4", 1)
	r := s.attrs[spanAttrStagesFailureReasons].AsStringSlice()
	if slices.Contains(r, api.StageReasonNotExecuted) {
		t.Fatalf("failure_reasons %q names the dry run's refusal: it is not a failure", r)
	}
	s.wantNoIdentity(t, "D4", "missing", "cleanup", "create", psAlice, psTargetNS)
}

// D5 — the truncated summary cannot tell which stages were deliberate: failed
// is its N (the trace is kept) and not_executed is not set, never a guessed 0.
func TestIssue1288_D5_TheTruncatedSummarySetsNoNotExecuted(t *testing.T) {
	s := s1264Serve(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(util.HeaderStageOutcomes, api.TruncatedStageOutcomes(9))
		w.WriteHeader(http.StatusOK)
	}), httptest.NewRequest("GET", "/call", nil))
	s.wantStages(t, "a truncated summary", 9, nil)
	if v, ok := s.attrs[attribute.Key(spanAttrStagesNotExecuted)]; ok {
		t.Fatalf("a truncated summary set %s=%v: the summary cannot tell", spanAttrStagesNotExecuted, v.Emit())
	}
}
