package handlers

// audit_open_record_test.go — #1379 review F2: an audited route's record is
// opened OUTERMOST (audit.OpenRecord, wrapped at registration with the route's
// descriptor), so every exit of the route emits exactly one record.
//
//   O  an exit the handler emits no record for (a body the decoder refuses, a
//      malformed /call query, a /call whose dial would act on another object)
//      is one denied row, reason invalid_request, no status code, built by the
//      route's descriptor: the target when the request names one the handler
//      would dial, else none (marked target).
//   E  an exit the handler records is still exactly one row: the fallback
//      never adds a second.
//   N  a request with no verified caller gets no row (pre-verification
//      refusals are counted, never recorded).
//   C  every row conforms; the library's falsifiers and F1's bite on it.

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

// serveRoute serves method target through the route's outermost record, then
// audit.Middleware, then (when verified) a stand-in for UserConfig that
// attaches and verifies the caller, then h.
func serveRoute(t *testing.T, route audit.Route, h http.Handler, verified bool, method, target, body string, status int, resp string) (int, []sdklog.Record) {
	t.Helper()
	api, ep := newFakeAPI443(t)
	api.respond(status, resp)
	rec := audittest.NewRecorder(t)
	audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
	t.Cleanup(func() { audit.SetDefault(nil) })
	caller := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !verified {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			ui := jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}, Principal: jwtutil.PrincipalHuman}
			ctx := xcontext.BuildContext(r.Context(), xcontext.WithAccessToken("test-token"),
				xcontext.WithUserInfo(ui), xcontext.WithUserConfig(ep))
			audit.SetCaller(ctx, ui)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req := httptest.NewRequest(method, target, rdr)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.RemoteAddr = "10.0.0.9:41000"
	w := httptest.NewRecorder()
	audit.OpenRecord(route)(audit.Middleware()(caller(h))).ServeHTTP(w, req)
	return w.Code, rec.Records()
}

func TestAudit_OpenRecord_O_E_N_C_EveryExitOfAnAuditedRouteIsExactlyOneRow(t *testing.T) {
	const q = "apiVersion=v1&resource=configmaps&namespace=default&name=cm-x"
	for _, c := range []struct {
		name           string
		route          audit.Route
		h              http.Handler
		verified       bool
		method, target string
		body           string
		code           int    // the response
		outcome        string // "" = no row
		reason         string
		named          bool
		action, verb   string
		wantMethod     string
	}{
		{"POST /export, a body the decoder refuses", ExportRoute(), ExportPOST(), true,
			http.MethodPost, "/export?format=json&" + target1055, `{"extras": "not an object"}`,
			http.StatusBadRequest, krateoaudit.OutcomeDenied, audit.DenialInvalidRequest, true, "export", "get", http.MethodGet},
		{"PUT /call, a name that is not one segment", CallRoute(), Call(), true,
			http.MethodPut, "/call?apiVersion=v1&resource=configmaps&namespace=default&name=" + url.QueryEscape("cm-x/status"), cmRequest957,
			http.StatusBadRequest, krateoaudit.OutcomeDenied, audit.DenialInvalidRequest, false, "call", "put", http.MethodPut},
		{"DELETE /call, a dial that acts on another object", CallRoute(), Call(), true,
			http.MethodDelete, "/call?apiVersion=v1&resource=namespaces&namespace=foo&name=bar", "",
			http.StatusBadRequest, krateoaudit.OutcomeDenied, audit.DenialInvalidRequest, false, "call", "delete", http.MethodDelete},
		{"POST /call, a dryRun on the plain route", CallRoute(), Call(), true,
			http.MethodPost, "/call?" + q + "&dryRun=All", cmRequest957,
			http.StatusBadRequest, krateoaudit.OutcomeDenied, audit.DenialInvalidRequest, false, "call", "post", http.MethodPost},
		{"POST /call, written", CallRoute(), Call(), true,
			http.MethodPost, "/call?" + q, cmRequest957,
			http.StatusOK, krateoaudit.OutcomeSuccess, "", true, "call", "create", http.MethodPost},
		{"POST /call/dry-run, the apiserver refused", CallDryRunRoute(), CallDryRun(), true,
			http.MethodPost, "/call/dry-run?" + q, cmRequest957,
			http.StatusForbidden, krateoaudit.OutcomeFailure, "", true, "call.dryRun", "create", http.MethodPost},
		{"GET /export, no verified caller", ExportRoute(), ExportGET(), false,
			http.MethodGet, "/export?format=json&" + target1055, "",
			http.StatusUnauthorized, "", "", false, "", "", ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			status, resp := http.StatusOK, cmResponse957
			if c.code == http.StatusForbidden {
				status, resp = http.StatusForbidden, cmForbidden957
			}
			if c.action == "export" {
				resp = body1055
			}
			code, recs := serveRoute(t, c.route, c.h, c.verified, c.method, c.target, c.body, status, resp)
			if code != c.code {
				t.Fatalf("answered %d, want %d", code, c.code)
			}
			if c.outcome == "" {
				if len(recs) != 0 {
					t.Fatalf("%d rows, want none", len(recs))
				}
				return
			}
			if len(recs) != 1 {
				t.Fatalf("%d rows, want exactly 1", len(recs))
			}
			a := asStrings1291(recs[0])
			if a[krateoaudit.KeyOutcome] != c.outcome || a[krateoaudit.KeyDenialReason] != c.reason {
				t.Errorf("outcome %q reason %q, want %q %q", a[krateoaudit.KeyOutcome], a[krateoaudit.KeyDenialReason], c.outcome, c.reason)
			}
			if c.outcome == krateoaudit.OutcomeDenied {
				if v, ok := a[krateoaudit.KeyHTTPStatusCode]; ok {
					t.Errorf("a denied row carries http.response.status_code=%s", v)
				}
			}
			if _, ok := a[krateoaudit.KeyObjectResource]; ok != c.named {
				t.Errorf("names a Kubernetes object: %t, want %t", ok, c.named)
			}
			if a[krateoaudit.KeyAction] != c.action || a[krateoaudit.KeyVerb] != c.verb || a[krateoaudit.KeyHTTPMethod] != c.wantMethod {
				t.Errorf("action %q verb %q method %q, want %s %s %s", a[krateoaudit.KeyAction], a[krateoaudit.KeyVerb], a[krateoaudit.KeyHTTPMethod], c.action, c.verb, c.wantMethod)
			}
			var opts []audittest.Option
			if !c.named {
				opts = append(opts, audittest.AllowIncomplete(krateoaudit.RuleTarget))
			}
			assertConforms4(t, recs[0], opts...)
		})
	}
}
