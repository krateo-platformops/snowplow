package handlers

// audit909_shadow_parity_test.go — #909, audit v0.12.0: POST
// /debug/shadow-parity is recorded. action debug.shadowParity, verb post,
// method POST, the request writer's non-resource target url.path (the route's
// own path), dry_run false (a write).
//
//   S  a flip is one success row, its body the committed state;
//   R  an unparseable value is one denied row, invalid_request, no status
//      code, its body the rule, never the value;
//   C  both conform, and the library's falsifiers and F1's bite on them.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

func TestAudit909_S_R_C_TheToggleIsRecorded(t *testing.T) {
	prev := rbac.ShadowParityEnabled()
	t.Cleanup(func() { rbac.SetShadowParityEnabled(prev) })
	for _, c := range []struct {
		name, query     string
		code            int
		outcome, reason string
		body            string
	}{
		{"a flip", "enabled=true", http.StatusOK, krateoaudit.OutcomeSuccess, "", "enabled=true"},
		{"an unparseable value", "enabled=canary-909", http.StatusBadRequest, krateoaudit.OutcomeDenied, audit.DenialInvalidRequest, shadowParityRefused},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := audittest.NewRecorder(t)
			audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
			t.Cleanup(func() { audit.SetDefault(nil) })
			caller := func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					next.ServeHTTP(w, r.WithContext(xcontext.BuildContext(r.Context(),
						xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Principal: jwtutil.PrincipalHuman}))))
				})
			}
			req := httptest.NewRequest(http.MethodPost, ShadowParityPath+"?"+c.query, nil)
			w := httptest.NewRecorder()
			audit.OpenRecord(ShadowParityRoute())(audit.Middleware()(caller(DebugShadowParitySet()))).ServeHTTP(w, req)
			if w.Code != c.code {
				t.Fatalf("answered %d, want %d", w.Code, c.code)
			}
			recs := rec.Records()
			if len(recs) != 1 {
				t.Fatalf("%d rows, want exactly 1", len(recs))
			}
			a := asStrings1291(recs[0])
			for k, want := range map[string]string{
				krateoaudit.KeyAction:     "debug.shadowParity",
				krateoaudit.KeyVerb:       "post",
				krateoaudit.KeyHTTPMethod: http.MethodPost,
				krateoaudit.KeyURLPath:    ShadowParityPath,
				krateoaudit.KeyDryRun:     "false",
				krateoaudit.KeyOutcome:    c.outcome,
			} {
				if a[k] != want {
					t.Errorf("%s = %q, want %q", k, a[k], want)
				}
			}
			if a[krateoaudit.KeyDenialReason] != c.reason {
				t.Errorf("denial_reason %q, want %q", a[krateoaudit.KeyDenialReason], c.reason)
			}
			if body := recs[0].Body().AsString(); body != c.body {
				t.Errorf("body %q, want %q", body, c.body)
			}
			assertConforms4(t, recs[0])
		})
	}
}
