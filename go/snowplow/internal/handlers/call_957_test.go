package handlers

// call_957_test.go — #957 (krateo-acmp#59 Option B) at the /call and /export
// emit sites, driven through the REAL handlers against a fake apiserver that
// answers every write with an object whose uid, generation and resourceVersion
// all DIFFER from the request body's, so a record built from the request is
// told apart from one built from the response.
//
//   W  create, update, patch: the record carries the RESPONSE object's
//      metadata.uid / generation / resourceVersion.
//   Wd delete: the uid only, of the returned object (deletion pending) or of
//      the Status the apiserver answers with once the object is gone.
//   F  a failed write records none of them; so does a dry run.
//   X  an export (a read) records none, though the rows it serializes carry
//      object metadata.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"go.opentelemetry.io/otel/log/logtest"
)

const (
	// The request: what the caller sent. Its uid, generation and
	// resourceVersion are never the ones the record may carry.
	cmRequest957 = `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-x","namespace":"default",` +
		`"uid":"uid-req-957","generation":9,"resourceVersion":"100"},"data":{"k":"v"}}`
	// The apiserver's answer: what was stored.
	cmResponse957 = `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-x","namespace":"default",` +
		`"uid":"uid-resp-957","generation":4,"resourceVersion":"101"},"data":{"k":"v"}}`
	cmDeleting957 = `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cm-x","namespace":"default",` +
		`"uid":"uid-resp-957","generation":5,"resourceVersion":"102","deletionTimestamp":"2026-10-28T10:00:00Z",` +
		`"finalizers":["example.io/hold"]}}`
	cmDeleted957 = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Success",` +
		`"details":{"name":"cm-x","kind":"configmaps","uid":"uid-gone-957"}}`
	cmForbidden957 = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","reason":"Forbidden",` +
		`"message":"configmaps \"cm-x\" is forbidden","code":403,"details":{"name":"cm-x","kind":"configmaps","uid":"uid-resp-957"}}`
)

var objectKeys957 = []string{"k8s.object.uid", "k8s.object.generation", "k8s.object.resource_version"}

func recorder957(t *testing.T) *logtest.Recorder {
	t.Helper()
	rec := logtest.NewRecorder()
	audit.SetDefault(audit.New(rec.Logger("test-957")))
	t.Cleanup(func() { audit.SetDefault(nil) })
	return rec
}

// records957 flattens every emitted record to its attribute map.
func records957(rec *logtest.Recorder) []map[string]string {
	var out []map[string]string
	for _, recs := range rec.Result() {
		for _, r := range recs {
			m := map[string]string{}
			for _, kv := range r.Attributes {
				m[string(kv.Key)] = kv.Value.String()
			}
			out = append(out, m)
		}
	}
	return out
}

func one957(t *testing.T, rec *logtest.Recorder) map[string]string {
	t.Helper()
	got := records957(rec)
	if len(got) != 1 {
		t.Fatalf("records = %d, want 1: %v", len(got), got)
	}
	return got[0]
}

// wantObject957 asserts the record's k8s.object.* attributes are exactly want
// (a key absent from want must be absent from the record).
func wantObject957(t *testing.T, got map[string]string, want map[string]string) {
	t.Helper()
	for _, k := range objectKeys957 {
		w, wantIt := want[k]
		g, has := got[k]
		switch {
		case wantIt && (!has || g != w):
			t.Errorf("%s = %q (present=%t), want %q (record %v)", k, g, has, w, got)
		case !wantIt && has:
			t.Errorf("%s = %q, want it absent (record %v)", k, g, got)
		}
	}
}

func TestCall957_W_WriteRecordsTheResponseObject(t *testing.T) {
	respObject := map[string]string{
		"k8s.object.uid": "uid-resp-957", "k8s.object.generation": "4", "k8s.object.resource_version": "101",
	}
	cases := []struct {
		name, method, reqBody, respBody string
		status                          int
		want                            map[string]string
	}{
		{"create", http.MethodPost, cmRequest957, cmResponse957, http.StatusCreated, respObject},
		{"update", http.MethodPut, cmRequest957, cmResponse957, http.StatusOK, respObject},
		{"patch", http.MethodPatch, `{"metadata":{"resourceVersion":"100","uid":"uid-req-957"},"data":{"k":"w"}}`,
			cmResponse957, http.StatusOK, respObject},
		{"delete, deletion pending", http.MethodDelete, "", cmDeleting957, http.StatusOK,
			map[string]string{"k8s.object.uid": "uid-resp-957"}},
		{"delete, object gone", http.MethodDelete, "", cmDeleted957, http.StatusOK,
			map[string]string{"k8s.object.uid": "uid-gone-957"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			api, ep := newFakeAPI443(t)
			api.respond(c.status, c.respBody)
			rec := recorder957(t)

			w := serve443(Call(), ep, c.method, "/call?"+cmQuery443, c.reqBody)
			if w.Code != http.StatusOK {
				t.Fatalf("%s /call → %d, want 200: %s", c.method, w.Code, w.Body.String())
			}
			if reqs := api.requests(); len(reqs) != 1 || reqs[0].method != c.method || reqs[0].body != c.reqBody {
				t.Fatalf("precondition: the apiserver got %+v, want one %s carrying the request body", reqs, c.method)
			}
			got := one957(t, rec)
			if got["outcome"] != "success" || got["krateo.action"] != "call" || got["k8s.resource.name"] != "cm-x" {
				t.Fatalf("precondition: record %v is not the successful call record", got)
			}
			wantObject957(t, got, c.want)
		})
	}
}

func TestCall957_F_FailureAndDryRunRecordNone(t *testing.T) {
	t.Run("refused write", func(t *testing.T) {
		for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
			api, ep := newFakeAPI443(t)
			api.respond(http.StatusForbidden, cmForbidden957)
			rec := recorder957(t)
			body := cmRequest957
			if m == http.MethodDelete {
				body = ""
			}
			serve443(Call(), ep, m, "/call?"+cmQuery443, body)
			got := one957(t, rec)
			if got["outcome"] != "failure" || got["http.response.status_code"] != "403" {
				t.Fatalf("%s: precondition: record %v is not the refused write's", m, got)
			}
			wantObject957(t, got, nil)
		}
	})
	t.Run("server error", func(t *testing.T) {
		t.Setenv("CLIENT_MAX_RETRIES", "0") // a 5xx is retried; one answer is enough here
		api, ep := newFakeAPI443(t)
		api.respond(http.StatusInternalServerError, cmResponse957) // an object-shaped error body
		rec := recorder957(t)
		serve443(Call(), ep, http.MethodPut, "/call?"+cmQuery443, cmRequest957)
		got := one957(t, rec)
		if got["outcome"] != "failure" {
			t.Fatalf("precondition: record %v is not a failure", got)
		}
		wantObject957(t, got, nil)
	})
	t.Run("dry run", func(t *testing.T) {
		api, ep := newFakeAPI443(t)
		api.respond(http.StatusCreated, cmResponse957)
		rec := recorder957(t)
		w := serve443(CallDryRun(), ep, http.MethodPost, "/call/dry-run?"+cmQuery443, cmRequest957)
		if w.Code != http.StatusOK {
			t.Fatalf("dry run → %d", w.Code)
		}
		got := one957(t, rec)
		if got["krateo.action"] != "call.dryRun" || got["outcome"] != "success" {
			t.Fatalf("precondition: record %v is not the dry run's", got)
		}
		wantObject957(t, got, nil)
	})
}

func TestExport957_X_ExportRecordsNone(t *testing.T) {
	api, ep := newFakeAPI443(t)
	api.respond(http.StatusOK, `{"apiVersion":"v1","kind":"ConfigMapList","metadata":{"resourceVersion":"900"},`+
		`"items":[{"metadata":{"name":"cm-x","uid":"uid-item-957","generation":3,"resourceVersion":"901"}}]}`)
	rec := recorder957(t)

	req := withCaller443(httptest.NewRequest(http.MethodGet, "/export?format=json&"+cmQuery443, nil), ep)
	w := httptest.NewRecorder()
	Export(Call()).ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("export → %d: %s", w.Code, w.Body.String())
	}
	got := one957(t, rec)
	if got["krateo.action"] != "export" || got["outcome"] != "success" {
		t.Fatalf("precondition: record %v is not the export's", got)
	}
	wantObject957(t, got, nil)
}
