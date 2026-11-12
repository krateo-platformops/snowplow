package handlers

// audit4_1257_986_test.go — AUDIT-4 (installer#410): the /call, /call/dry-run
// and /export rows, judged against the shared schema (audit v0.10.0) through
// the real handlers and audit.Middleware.
//
//   D  #1257: the object the apiserver resolves from the path snowplow
//      dialled is the object the record names. The fake apiserver classifies
//      the path it RECEIVED with the apiserver's own RequestInfoFactory, and
//      every object_ref key of the record must equal it, for names that are
//      one path segment only once escaped ("?", "#", a space).
//   R  #1257: a value that is not one apiserver path segment ("/", "%", "."
//      or "..", in the API version, the resource, the namespace, the name or
//      the subresource) is a 400 before anything is dialled, with no record.
//   X  #986: every exit of an export emits exactly one record, with its real
//      outcome: denied (no status code) for the export's own refusals before
//      any page is dispatched, failure with the code the export answered for
//      a /call failure and an extraction error. An export whose target the
//      /call lane cannot dial names no object and is marked target.
//   C  every record conforms for snowplow's writer class, and the library's
//      falsifiers bite on it.

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// serveAudit4 serves method target through audit.Middleware as alice, with
// the fake apiserver answering status resp, and returns every record, the
// requests the apiserver received and the response.
func serveAudit4(t *testing.T, h http.Handler, method, target, body string, status int, resp string) ([]sdklog.Record, []rec443, *httptest.ResponseRecorder) {
	t.Helper()
	api, ep := newFakeAPI443(t)
	api.respond(status, resp)
	rec := audittest.NewRecorder(t)
	audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
	t.Cleanup(func() { audit.SetDefault(nil) })
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewReader([]byte(body))
	}
	req := withCaller443(httptest.NewRequest(method, target, rdr), ep)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req = req.WithContext(audit.WithSessionID(req.Context(), "audit4-1257"))
	w := httptest.NewRecorder()
	audit.Middleware()(h).ServeHTTP(w, req)
	return rec.Records(), api.requests(), w
}

// dialledObject is the object the apiserver resolves from the path it
// received, keyed as the record names it.
func dialledObject(t *testing.T, method, path string) map[string]string {
	t.Helper()
	f := &apirequest.RequestInfoFactory{APIPrefixes: sets.NewString("api", "apis"), GrouplessAPIPrefixes: sets.NewString("api")}
	info, err := f.NewRequestInfo(&http.Request{Method: method, URL: &url.URL{Path: path}})
	if err != nil || !info.IsResourceRequest {
		t.Fatalf("the apiserver would not read the dialled path as a resource request (err %v)", err)
	}
	return map[string]string{
		krateoaudit.KeyObjectAPIGroup:    info.APIGroup,
		krateoaudit.KeyObjectAPIVersion:  info.APIVersion,
		krateoaudit.KeyObjectResource:    info.Resource,
		krateoaudit.KeyObjectSubresource: info.Subresource,
		krateoaudit.KeyObjectName:        info.Name,
		krateoaudit.KeyNamespace:         info.Namespace,
	}
}

func assertConforms4(t *testing.T, r sdklog.Record, opts ...audittest.Option) {
	t.Helper()
	audittest.AssertConformant(t, audit.Writer, r, opts...)
	audittest.AssertRequiredKeysDetected(t, audit.Writer, r, opts...)
	audittest.AssertEnumsDetected(t, audit.Writer, r, opts...)
}

func TestAudit4_1257_D_C_TheRecordNamesTheObjectTheApiserverResolved(t *testing.T) {
	for _, c := range []struct {
		name   string
		h      func() http.Handler
		method string
		query  string
		dry    bool
	}{
		{"plain name", Call, http.MethodPut, "apiVersion=v1&resource=configmaps&namespace=default&name=cm-x", false},
		{"subresource", Call, http.MethodPut, "apiVersion=v1&resource=configmaps&namespace=default&name=cm-x&subresource=status", false},
		{"a ? in the name stays in the name", Call, http.MethodPut, "apiVersion=v1&resource=configmaps&namespace=default&name=" + url.QueryEscape("cm-x?dryRun=All"), false},
		{"a # in the name stays in the name", Call, http.MethodDelete, "apiVersion=v1&resource=configmaps&namespace=default&name=" + url.QueryEscape("cm#x"), false},
		{"a space in the name", Call, http.MethodPatch, "apiVersion=v1&resource=configmaps&namespace=default&name=" + url.QueryEscape("cm x"), false},
		{"a colon in a cluster-scoped name", func() http.Handler { return CallWithScopeResolver(clusterScoped1257) }, http.MethodPut,
			"apiVersion=rbac.authorization.k8s.io/v1&resource=clusterroles&name=" + url.QueryEscape("system:viewer"), false},
		{"a ? in the namespace", Call, http.MethodPut, "apiVersion=v1&resource=configmaps&namespace=" + url.QueryEscape("ns?x") + "&name=cm-x", false},
		{"dry run, a ? in the name", CallDryRun, http.MethodPut, "apiVersion=v1&resource=configmaps&namespace=default&name=" + url.QueryEscape("cm-x?dryRun=None"), true},
	} {
		t.Run(c.name, func(t *testing.T) {
			route := "/call?"
			if c.dry {
				route = "/call/dry-run?"
			}
			body := ""
			if c.method != http.MethodDelete {
				body = cmRequest957
			}
			recs, reqs, w := serveAudit4(t, c.h(), c.method, route+c.query, body, http.StatusOK, cmResponse957)
			if len(reqs) != 1 || len(recs) != 1 {
				t.Fatalf("%d dials and %d records (code %d), want 1 and 1", len(reqs), len(recs), w.Code)
			}
			want := dialledObject(t, c.method, reqs[0].path)
			got := asStrings1291(recs[0])
			for k, v := range want {
				if got[k] != v {
					t.Errorf("%s: the record says %q, the apiserver resolved %q", k, got[k], v)
				}
			}
			// The query the apiserver received is only snowplow's own: a ?
			// in a name never becomes one.
			for k := range reqs[0].query {
				if k != "dryRun" {
					t.Errorf("the apiserver received query parameter %q", k)
				}
			}
			if sent, rec := reqs[0].query.Get("dryRun") == "All", got[krateoaudit.KeyDryRun] == "true"; sent != c.dry || rec != c.dry {
				t.Errorf("dryRun=All sent %t, recorded %t, want %t", sent, rec, c.dry)
			}
			assertConforms4(t, recs[0])
		})
	}
}

func clusterScoped1257(_ schema.GroupVersionResource) (bool, error) { return false, nil }

func TestAudit4_1257_R_AValueThatIsNotOneSegmentIsRefusedBeforeTheDial(t *testing.T) {
	esc := url.QueryEscape
	for _, c := range []struct {
		name, query, param string
	}{
		{"slash in the name (a subresource)", "apiVersion=v1&resource=configmaps&namespace=default&name=" + esc("cm-x/status"), "name"},
		{"dot-dot in the name (another resource)", "apiVersion=v1&resource=configmaps&namespace=default&name=" + esc("../secrets/s1"), "name"},
		{"name ..", "apiVersion=v1&resource=configmaps&namespace=default&name=..", "name"},
		{"name .", "apiVersion=v1&resource=configmaps&namespace=default&name=.", "name"},
		{"percent in the name (decoded back into a slash)", "apiVersion=v1&resource=configmaps&namespace=default&name=" + esc("cm-x%2Fstatus"), "name"},
		{"slash in the subresource", "apiVersion=v1&resource=configmaps&namespace=default&name=cm-x&subresource=" + esc("status/../../secrets"), "subresource"},
		{"slash in the namespace", "apiVersion=v1&resource=configmaps&namespace=" + esc("default/secrets") + "&name=cm-x", "namespace"},
		{"dot-dot namespace", "apiVersion=v1&resource=configmaps&namespace=..&name=cm-x", "namespace"},
		{"slash in the resource", "apiVersion=v1&resource=" + esc("configmaps/../secrets") + "&namespace=default&name=cm-x", "resource"},
		{"dot-dot version", "apiVersion=" + esc("apps/..") + "&resource=deployments&namespace=default&name=d", "apiVersion"},
		{"dot group", "apiVersion=" + esc("./v1") + "&resource=deployments&namespace=default&name=d", "apiVersion"},
	} {
		for _, route := range []struct {
			path string
			h    func() http.Handler
		}{{"/call?", Call}, {"/call/dry-run?", CallDryRun}} {
			t.Run(route.path+c.name, func(t *testing.T) {
				recs, reqs, w := serveAudit4(t, route.h(), http.MethodPut, route.path+c.query, cmRequest957, http.StatusOK, cmResponse957)
				if w.Code != http.StatusBadRequest {
					t.Errorf("code %d, want 400", w.Code)
				}
				if !strings.Contains(w.Body.String(), `\"`+c.param+`\"`) {
					t.Errorf("the refusal does not name parameter %q", c.param)
				}
				if len(reqs) != 0 {
					t.Errorf("%d requests reached the apiserver, want 0", len(reqs))
				}
				if len(recs) != 0 {
					t.Errorf("%d records for a request that dialled nothing, want 0", len(recs))
				}
			})
		}
	}
}

func TestAudit986_X_C_EveryExportExitEmitsExactlyOneRecord(t *testing.T) {
	type want struct {
		code    int // the export's answer
		outcome string
		status  string // http.response.status_code, "" = absent
		target  bool   // the record names a Kubernetes object
	}
	for _, c := range []struct {
		name     string
		query    string
		status   int
		resp     string
		want     want
		incomple []string
	}{
		{"success", "format=json&" + target1055, http.StatusOK, body1055,
			want{http.StatusOK, audit.OutcomeSuccess, "200", true}, nil},
		{"unsupported format (refused)", "format=xml&" + target1055, http.StatusOK, body1055,
			want{http.StatusBadRequest, audit.OutcomeDenied, "", true}, nil},
		{"header labels without fields (refused)", "format=csv&header=A&" + target1055, http.StatusOK, body1055,
			want{http.StatusBadRequest, audit.OutcomeDenied, "", true}, nil},
		{"/call failure, full list", "format=json&" + target1055, http.StatusForbidden, cmForbidden957,
			want{http.StatusForbidden, audit.OutcomeFailure, "403", true}, nil},
		{"/call failure, one page", "format=json&page=1&perPage=5&" + target1055, http.StatusForbidden, cmForbidden957,
			want{http.StatusForbidden, audit.OutcomeFailure, "403", true}, nil},
		{"row path does not evaluate, full list", "format=csv&path=" + url.QueryEscape(".[") + "&" + target1055, http.StatusOK, body1055,
			want{http.StatusBadRequest, audit.OutcomeFailure, "400", true}, nil},
		{"row path does not evaluate, one page", "format=csv&page=1&perPage=5&path=" + url.QueryEscape(".[") + "&" + target1055, http.StatusOK, body1055,
			want{http.StatusBadRequest, audit.OutcomeFailure, "400", true}, nil},
		{"pdf", "format=pdf&" + target1055, http.StatusOK, body1055,
			want{http.StatusOK, audit.OutcomeSuccess, "200", true}, nil},
		{"a target the /call lane cannot dial", "format=json&apiVersion=v1&resource=configmaps&namespace=default&name=" + url.QueryEscape("cm-x/status"), http.StatusOK, body1055,
			want{http.StatusBadRequest, audit.OutcomeFailure, "400", false}, []string{krateoaudit.RuleTarget}},
	} {
		for _, m := range []struct {
			method string
			h      func() http.Handler
			body   string
		}{
			{http.MethodGet, func() http.Handler { return Export(Call()) }, ""},
			{http.MethodPost, ExportPOST, `{"extras":{}}`},
		} {
			t.Run(m.method+" "+c.name, func(t *testing.T) {
				recs, _, w := serveAudit4(t, m.h(), m.method, "/export?"+c.query, m.body, c.status, c.resp)
				if w.Code != c.want.code {
					t.Fatalf("the export answered %d, want %d", w.Code, c.want.code)
				}
				if len(recs) != 1 {
					t.Fatalf("%d records, want exactly 1", len(recs))
				}
				got := asStrings1291(recs[0])
				if got[krateoaudit.KeyOutcome] != c.want.outcome {
					t.Errorf("outcome %q, want %q", got[krateoaudit.KeyOutcome], c.want.outcome)
				}
				if s, ok := got[krateoaudit.KeyHTTPStatusCode]; s != c.want.status || (c.want.status == "" && ok) {
					t.Errorf("status code %q (present %t), want %q", s, ok, c.want.status)
				}
				if _, ok := got[krateoaudit.KeyObjectResource]; ok != c.want.target {
					t.Errorf("names a Kubernetes object: %t, want %t", ok, c.want.target)
				}
				if got[krateoaudit.KeyAction] != "export" || got[krateoaudit.KeyVerb] != "get" || got[krateoaudit.KeyHTTPMethod] != m.method {
					t.Errorf("action %q verb %q method %q, want export get %s", got[krateoaudit.KeyAction], got[krateoaudit.KeyVerb], got[krateoaudit.KeyHTTPMethod], m.method)
				}
				assertConforms4(t, recs[0], audittest.AllowIncomplete(c.incomple...))
			})
		}
	}
}

// TestAudit986_X_ARefusalNamesTheRuleNeverTheValue — the body of a refused
// export's record is the rule's closed phrase: the caller's value (here a
// format) never reaches the trail through it.
func TestAudit986_X_ARefusalNamesTheRuleNeverTheValue(t *testing.T) {
	recs, _, _ := serveAudit4(t, Export(Call()), http.MethodGet, "/export?format=canary-986&"+target1055, "", http.StatusOK, body1055)
	if len(recs) != 1 {
		t.Fatalf("%d records, want 1", len(recs))
	}
	if body := recs[0].Body().AsString(); body != exportRefusedFormat {
		t.Errorf("the record body is not the refusal's closed phrase")
	}
}
