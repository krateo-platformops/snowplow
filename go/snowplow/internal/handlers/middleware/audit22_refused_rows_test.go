package middleware_test

// audit22_refused_rows_test.go — audit v0.12.0 (audit#22 (a)), AUDIT-4
// installer#410: a verified caller UserConfig refuses because their own
// credential Secret is missing gets a denied row exactly when the same
// request, admitted, would have produced one. The row is built by the route's
// own audit descriptor (handlers.CallRoute, handlers.ExportRoute), declared at
// registration, never by a list in the middleware.
//
//   R  POST /call, PUT /call/dry-run, GET /export refused: one denied row,
//      reason session_credential_missing (step_up_required for a krateo:loa:2
//      token), no status code, the action, verb and target the handler would
//      have recorded. It names the object asked for, never the credential
//      Secret (its name embeds the username).
//   N  a route with no descriptor (not audited): no row.
//   M  a /call write the handler would refuse as malformed, refused here
//      first: one denied row, its target unresolved (marked target).
//   C  every row conforms, and the library's falsifiers bite on it.

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

const cm22 = `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-x","namespace":"default"},"data":{"k":"v"}}`

// serve22 serves method target as a token whose credential Secret is missing,
// through route's descriptor (none when route is nil), UserConfig, then h.
func serve22(t *testing.T, route *audit.Route, h http.Handler, method, target, body string, groups ...string) (int, []sdklog.Record) {
	t.Helper()
	other := su994Secret(credsecret.StepUpName(su994User), su994StepUp)
	if len(groups) > 1 { // a step-up token: its step-up Secret is the missing one
		other = su994Secret(credsecret.SessionName(su994User), su994Session)
	}
	su994Informer(t, other)
	su994APIServer(t, other)
	rec := audittest.NewRecorder(t)
	audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
	t.Cleanup(func() { audit.SetDefault(nil) })

	mws := []func(http.Handler) http.Handler{audit.Middleware()}
	if route != nil {
		mws = append([]func(http.Handler) http.Handler{audit.OpenRecord(*route)}, mws...)
	}
	mws = append(mws, middleware.UserConfig(testKeys, testAuthnNS))
	wrapped := use.NewChain(mws...).Then(h)
	var rdr *bytes.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	var r *http.Request
	if rdr != nil {
		r = httptest.NewRequest(method, target, rdr)
		r.Header.Set("Content-Type", "application/json")
	} else {
		r = httptest.NewRequest(method, target, nil)
	}
	r.Header.Set("Authorization", "Bearer "+token22(t, groups...))
	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, r)
	return w.Code, rec.Records()
}

func ptrRoute(r audit.Route) *audit.Route { return &r }

// token22 is su994Token carrying authn's principal claim (human), as authn
// 0.39.0+ issues it: the record's actor class is the signed claim.
func token22(t *testing.T, groups ...string) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: su994User, Groups: groups, Duration: time.Hour,
		KeyID: testKeyID, PrivateKey: testPrivateKey, Principal: jwtutil.PrincipalHuman,
	})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	return tok
}

// namespaces22 is a cluster in which every Namespace exists and none is the
// preview sandbox: the preview flag reads false.
type namespaces22 struct{}

func (namespaces22) NamespaceLabels(context.Context, string) (map[string]string, bool, error) {
	return map[string]string{}, true, nil
}

func init() { audit.SetNamespaceReader(namespaces22{}) }

func TestAudit22_UserConfig_R_C_ARefusedAuditedRequestIsOneDeniedRow(t *testing.T) {
	const q = "apiVersion=v1&resource=configmaps&namespace=default&name=cm-x"
	for _, c := range []struct {
		name           string
		route          audit.Route
		h              http.Handler
		method, target string
		body           string
		groups         []string
		action, verb   string
		reason         string
	}{
		{"POST /call", handlers.CallRoute(), handlers.Call(), http.MethodPost, "/call?" + q, cm22,
			[]string{su994Group}, "call", "create", audit.DenialSessionCredentialMissing},
		{"PUT /call/dry-run", handlers.CallDryRunRoute(), handlers.CallDryRun(), http.MethodPut, "/call/dry-run?" + q, cm22,
			[]string{su994Group}, "call.dryRun", "update", audit.DenialSessionCredentialMissing},
		{"DELETE /call, a step-up token", handlers.CallRoute(), handlers.Call(), http.MethodDelete, "/call?" + q, "",
			[]string{su994Group, credsecret.StepUpGroup}, "call", "delete", audit.DenialStepUpRequired},
		{"GET /export", handlers.ExportRoute(), handlers.ExportGET(), http.MethodGet, "/export?format=json&" + q, "",
			[]string{su994Group}, "export", "get", audit.DenialSessionCredentialMissing},
	} {
		t.Run(c.name, func(t *testing.T) {
			code, recs := serve22(t, ptrRoute(c.route), c.h, c.method, c.target, c.body, c.groups...)
			if code != http.StatusUnauthorized || len(recs) != 1 {
				t.Fatalf("code %d, %d rows; want 401 and exactly 1", code, len(recs))
			}
			a := map[string]string{}
			for _, kv := range audittest.Attributes(recs[0]) {
				a[string(kv.Key)] = kv.Value.Emit()
			}
			for k, want := range map[string]string{
				krateoaudit.KeyOutcome:        krateoaudit.OutcomeDenied,
				krateoaudit.KeyDenialReason:   c.reason,
				krateoaudit.KeyAction:         c.action,
				krateoaudit.KeyVerb:           c.verb,
				krateoaudit.KeyHTTPMethod:     c.method,
				krateoaudit.KeyObjectResource: "configmaps",
				krateoaudit.KeyObjectName:     "cm-x",
				krateoaudit.KeyNamespace:      "default",
				krateoaudit.KeyUserName:       su994User,
			} {
				if a[k] != want {
					t.Errorf("%s = %q, want %q", k, a[k], want)
				}
			}
			if v, ok := a[krateoaudit.KeyHTTPStatusCode]; ok {
				t.Errorf("a denied row carries http.response.status_code=%s", v)
			}
			for k, v := range a {
				if strings.Contains(v, "-clientconfig") {
					t.Errorf("%s names the credential Secret", k)
				}
			}
			audittest.AssertConformant(t, audit.Writer, recs[0])
			audittest.AssertRequiredKeysDetected(t, audit.Writer, recs[0])
			audittest.AssertEnumsDetected(t, audit.Writer, recs[0])
		})
	}
}

func TestAudit22_UserConfig_N_NoRowWhenTheAdmittedRequestHasNone(t *testing.T) {
	const q = "apiVersion=v1&resource=configmaps&namespace=default&name=cm-x"
	for _, c := range []struct {
		name           string
		route          *audit.Route
		h              http.Handler
		method, target string
		body           string
	}{
		{"a route with no descriptor", nil, handlers.Call(), http.MethodPost, "/call?" + q, cm22},
	} {
		t.Run(c.name, func(t *testing.T) {
			code, recs := serve22(t, c.route, c.h, c.method, c.target, c.body, su994Group)
			if code != http.StatusUnauthorized {
				t.Fatalf("code %d, want 401", code)
			}
			if len(recs) != 0 {
				t.Errorf("%d rows, want 0", len(recs))
			}
		})
	}
}
