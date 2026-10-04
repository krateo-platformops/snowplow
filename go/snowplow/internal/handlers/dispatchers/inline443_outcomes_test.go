// inline443_outcomes_test.go — #443 part 2 builder requests (accepted on
// #443): the StageNotExecuted machine field on a refused write-verb stage,
// and the X-Snowplow-Stage-Outcomes header on INLINE replies.
//
//	a FilterDropsErrorKeys — a draft whose filter drops every error key still
//	  surfaces each failed stage (and why) in the header.
//	b AbsentOnStored — the header is never set on a stored resolve.
//	c TruncationBound — above 4 KiB the header is {"truncated":true,"failed":N}.
//	d NoMessageOrIdentity — the header carries names, ok and reason codes from
//	  the closed set only: no error text, path, data or identity.
//	k ReasonField — the refused stage's error object carries
//	  reason "StageNotExecuted" next to the stable message.
package dispatchers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/handlers/util"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions/api"
)

type outcomeRow443 struct {
	Name   string `json:"name"`
	OK     bool   `json:"ok"`
	Reason string `json:"reason"`
}

func outcomeStages443() []map[string]any {
	return []map[string]any{
		targetStage(), // alice may read it: ok
		{"name": "missing", "continueOnError": true, "errorKey": "missingErr",
			"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps/does-not-exist"},
		{"name": "create", "verb": "POST", "continueOnError": true, "errorKey": "createErr",
			"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps", "payload": `{"metadata":{"name":"evil"}}`},
	}
}

func parseOutcomes443(t *testing.T, v string) map[string]outcomeRow443 {
	t.Helper()
	if v == "" {
		t.Fatalf("%s header missing on an inline reply", util.HeaderStageOutcomes)
	}
	var rows []outcomeRow443
	if err := json.Unmarshal([]byte(v), &rows); err != nil {
		t.Fatalf("%s is not a JSON array: %v (%s)", util.HeaderStageOutcomes, err, v)
	}
	out := map[string]outcomeRow443{}
	for _, r := range rows {
		out[r.Name] = r
	}
	return out
}

func TestS443_StageOutcomes(t *testing.T) {
	t.Run("a_FilterDropsErrorKeys", func(t *testing.T) {
		f := in443Setup(t)
		obj := in443RA("draft-out-a", `{only: .target.metadata.name}`, outcomeStages443()...)
		rec := serveInline(t, f, in443Ctx(f, psAlice), obj)
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
		var env struct {
			Status json.RawMessage `json:"status"`
		}
		_ = json.Unmarshal(rec.Body.Bytes(), &env)
		if st := string(env.Status); strings.Contains(st, "missingErr") || strings.Contains(st, "not executed") || !strings.Contains(st, psTargetObj) {
			t.Fatalf("PRE: the filter must keep only the target and drop the error keys from .status: %s", st)
		}
		got := parseOutcomes443(t, rec.Header().Get(util.HeaderStageOutcomes))
		want := map[string]outcomeRow443{
			"target":  {Name: "target", OK: true},
			"missing": {Name: "missing", OK: false, Reason: api.StageReasonNotFound},
			"create":  {Name: "create", OK: false, Reason: api.StageReasonNotExecuted},
		}
		for name, w := range want {
			if got[name] != w {
				t.Errorf("stage %q outcome=%+v, want %+v (header=%s)", name, got[name], w, rec.Header().Get(util.HeaderStageOutcomes))
			}
		}
	})

	t.Run("b_AbsentOnStored", func(t *testing.T) {
		f := in443Setup(t)
		rec := serveStored(t, f, in443Ctx(f, psAlice), in443RA(psRAName, "", outcomeStages443()...))
		if rec.Code != http.StatusOK {
			t.Fatalf("code=%d", rec.Code)
		}
		if v := rec.Header().Get(util.HeaderStageOutcomes); v != "" {
			t.Fatalf("%s set on a STORED resolve: %s", util.HeaderStageOutcomes, v)
		}
	})

	t.Run("c_TruncationBound", func(t *testing.T) {
		f := in443Setup(t)
		const n = 80
		stages := make([]map[string]any, 0, n)
		for i := 0; i < n; i++ {
			stages = append(stages, map[string]any{
				"name": fmt.Sprintf("a-deliberately-long-stage-name-for-the-bound-%03d", i), "continueOnError": true,
				"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps/absent-" + fmt.Sprint(i)})
		}
		rec := serveInline(t, f, in443Ctx(f, psAlice), in443RA("draft-out-c", "", stages...))
		v := rec.Header().Get(util.HeaderStageOutcomes)
		if len(v) > api.StageOutcomesMaxHeaderBytes {
			t.Fatalf("header is %d bytes, over the %d bound", len(v), api.StageOutcomesMaxHeaderBytes)
		}
		var trunc struct {
			Truncated bool `json:"truncated"`
			Failed    int  `json:"failed"`
		}
		if err := json.Unmarshal([]byte(v), &trunc); err != nil || !trunc.Truncated || trunc.Failed != n {
			t.Fatalf("want {\"truncated\":true,\"failed\":%d}, got %q (err=%v)", n, v, err)
		}
		// Control: the same draft with few stages is NOT truncated.
		rec = serveInline(t, f, in443Ctx(f, psAlice), in443RA("draft-out-c2", "", stages[:3]...))
		if got := parseOutcomes443(t, rec.Header().Get(util.HeaderStageOutcomes)); len(got) != 3 {
			t.Fatalf("CONTROL: a 3-stage draft should report 3 rows, got %v", got)
		}
	})

	t.Run("d_NoMessageOrIdentity", func(t *testing.T) {
		f := in443Setup(t)
		f.extra["GET /api/v1/namespaces/"+psTargetNS+"/configmaps/leaky"] = func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Forbidden","code":403,"message":"LEAKY-MESSAGE ` + psSentinel + ` user ` + psAlice + `"}`))
		}
		stages := append(outcomeStages443(), map[string]any{"name": "leaky", "continueOnError": true,
			"path": "/api/v1/namespaces/" + psTargetNS + "/configmaps/leaky"})
		rec := serveInline(t, f, in443Ctx(f, psAlice), in443RA("draft-out-d", "", stages...))
		v := rec.Header().Get(util.HeaderStageOutcomes)
		for _, bad := range []string{"LEAKY-MESSAGE", psSentinel, psAlice, "/api/", "not executed", "not found", "dry-run"} {
			if strings.Contains(v, bad) {
				t.Errorf("%s carries %q: %s", util.HeaderStageOutcomes, bad, v)
			}
		}
		var raw []map[string]any
		if err := json.Unmarshal([]byte(v), &raw); err != nil {
			t.Fatalf("decode: %v", err)
		}
		closed := map[string]bool{"": true, api.StageReasonNotExecuted: true, api.StageReasonForbidden: true,
			api.StageReasonNotFound: true, api.StageReasonUnauthorized: true, api.StageReasonNotRun: true, api.StageReasonError: true}
		for _, row := range raw {
			for k := range row {
				if k != "name" && k != "ok" && k != "reason" {
					t.Errorf("unexpected field %q in a stage outcome: %v", k, row)
				}
			}
			if r, _ := row["reason"].(string); !closed[r] {
				t.Errorf("reason %q is not a closed-set code", r)
			}
		}
		if got := parseOutcomes443(t, v)["leaky"]; got.Reason != api.StageReasonForbidden {
			t.Errorf("leaky stage outcome=%+v, want Forbidden", got)
		}
	})

	t.Run("k_ReasonField", func(t *testing.T) {
		f := in443Setup(t)
		rec := serveInline(t, f, in443Ctx(f, psAlice), in443RA("draft-out-k", "", outcomeStages443()...))
		var env struct {
			Status map[string]any `json:"status"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
			t.Fatal(err)
		}
		errs, _ := env.Status["createErr"].([]any)
		if len(errs) != 1 {
			t.Fatalf("createErr=%v, want one error object", env.Status["createErr"])
		}
		e, _ := errs[0].(map[string]any)
		if e["reason"] != "StageNotExecuted" || e["message"] != `dry-run: stage "create" verb POST is not executed` {
			t.Fatalf("refused-stage error object=%v, want reason StageNotExecuted + the stable message", e)
		}
	})
}
