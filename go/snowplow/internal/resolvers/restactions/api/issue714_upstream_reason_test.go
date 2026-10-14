//go:build unit
// +build unit

package api

// issue714_upstream_reason_test.go — #714 unit arms: the shaping rule where an
// upstream body becomes a Status, and the code-first stage outcome.

import (
	"net/http"
	"testing"

	"github.com/krateo-platformops/plumbing/http/response"
)

func TestIssue714_NonOKStatus_StatusLineDecides(t *testing.T) {
	cases := []struct {
		name       string
		code       int
		body       string
		wantReason string
	}{
		{"agreeing reason kept", 403, `{"kind":"Status","status":"Failure","reason":"Forbidden","code":403}`, "Forbidden"},
		{"alternate 409 reason kept", 409, `{"reason":"AlreadyExists","code":409}`, "AlreadyExists"},
		{"disagreeing reason dropped", 409, `{"reason":"Forbidden","code":409}`, ""},
		{"unknown reason dropped", 404, `{"reason":"TotallyMadeUp"}`, ""},
		{"claimed code ignored", 404, `{"code":403}`, ""},
		{"no reason stays empty (#681 A4b)", 403, `{"message":"rate limited"}`, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			st, _, _, _, err := fetchAgainst(t, c.code, "application/json", c.body, noAuth)
			if err != nil {
				t.Fatal(err)
			}
			if st.Code != c.code || string(st.Reason) != c.wantReason || st.Status != response.StatusFailure {
				t.Fatalf("got code=%d reason=%q status=%q, want code=%d reason=%q Failure", st.Code, st.Reason, st.Status, c.code, c.wantReason)
			}
		})
	}
}

func TestIssue714_StageOutcome_CodeFirst(t *testing.T) {
	cases := []struct {
		v    any
		want string
	}{
		{map[string]any{"code": float64(409), "reason": "Forbidden"}, StageReasonError},
		{map[string]any{"code": http.StatusInternalServerError, "reason": "NotFound"}, StageReasonError},
		{map[string]any{"code": float64(403), "reason": "Forbidden"}, StageReasonForbidden},
		{map[string]any{"code": float64(404)}, StageReasonNotFound},
		{map[string]any{"reason": StageReasonNotExecuted, "message": "dry-run"}, StageReasonNotExecuted},
		{"a string", StageReasonError},
	}
	for _, c := range cases {
		if got := stageReasonOf(c.v); got != c.want {
			t.Fatalf("stageReasonOf(%v) = %q, want %q", c.v, got, c.want)
		}
	}
}
