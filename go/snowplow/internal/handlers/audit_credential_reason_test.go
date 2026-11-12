package handlers

// audit_credential_reason_test.go — #1379 re-review LOW b: a verified caller
// whose own credential is not on the request (call.go: no user endpoint after
// verification) is refused for that reason: denied,
// session_credential_missing (step_up_required for a krateo:loa:2 token), no
// status code; never the caller's invalid_request.

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

func TestAudit_OpenRecord_LowB_ACallerWithNoCredentialIsTheCredentialReason(t *testing.T) {
	for _, c := range []struct {
		name   string
		groups []string
		reason string
	}{
		{"a session token", []string{"devs"}, audit.DenialSessionCredentialMissing},
		{"a step-up token", []string{"devs", credsecret.StepUpGroup}, audit.DenialStepUpRequired},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := audittest.NewRecorder(t)
			audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
			t.Cleanup(func() { audit.SetDefault(nil) })
			noCredential := func(next http.Handler) http.Handler {
				return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					ui := jwtutil.UserInfo{Username: "alice", Groups: c.groups, Principal: jwtutil.PrincipalHuman}
					ctx := xcontext.BuildContext(r.Context(), xcontext.WithUserInfo(ui)) // no user endpoint
					audit.SetCaller(ctx, ui)
					next.ServeHTTP(w, r.WithContext(ctx))
				})
			}
			req := httptest.NewRequest(http.MethodPut, "/call?apiVersion=v1&resource=configmaps&namespace=default&name=cm-x",
				bytes.NewReader([]byte(cmRequest957)))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			audit.OpenRecord(CallRoute())(audit.Middleware()(noCredential(Call()))).ServeHTTP(w, req)
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("answered %d, want 401", w.Code)
			}
			recs := rec.Records()
			if len(recs) != 1 {
				t.Fatalf("%d rows, want exactly 1", len(recs))
			}
			a := asStrings1291(recs[0])
			if a[krateoaudit.KeyOutcome] != krateoaudit.OutcomeDenied || a[krateoaudit.KeyDenialReason] != c.reason {
				t.Errorf("outcome %q reason %q, want denied %q", a[krateoaudit.KeyOutcome], a[krateoaudit.KeyDenialReason], c.reason)
			}
			if a[krateoaudit.KeyObjectResource] != "configmaps" || a[krateoaudit.KeyObjectName] != "cm-x" {
				t.Errorf("the row names %q/%q, want the configmap asked for", a[krateoaudit.KeyObjectResource], a[krateoaudit.KeyObjectName])
			}
			assertConforms4(t, recs[0])
		})
	}
}
