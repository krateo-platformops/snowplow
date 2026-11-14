package handlers

// call_audit_body_1298_test.go — #1298: a failed /call write's audit record
// body is a reason class from the closed set the RESTAction stage records use
// (audit.FailureMessage), never upstream text. Driven through the REAL /call
// handler against a fake apiserver (call_443_test.go).
//
//   S  a 403 whose Status.message names a sentinel identity: the record body
//      never holds it; it is "upstream answered 403 Forbidden".
//   N  a non-Status error body carrying a sentinel token (plain text, and a
//      JSON object that is not a Status): never in the record.
//   OK a successful write and a dry run: the record body stays empty.
//   C  in every arm the CALLER's response is unchanged: the failure envelope
//      still carries the upstream message the record must not hold.

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"go.opentelemetry.io/otel/log/logtest"
)

const (
	sentinelUser1298  = "sentinel-user-1298@corp.example"
	sentinelToken1298 = "sentinel-token-1298-eyJhbGciOiJSUzI1NiJ9"

	forbidden1298 = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","reason":"Forbidden",` +
		`"message":"configmaps \"cm-x\" is forbidden: User \"` + sentinelUser1298 +
		`\" cannot create resource \"configmaps\" in API group \"\" in the namespace \"default\"","code":403}`
	forbiddenMsg1298 = `configmaps "cm-x" is forbidden: User "` + sentinelUser1298 +
		`" cannot create resource "configmaps" in API group "" in the namespace "default"`
)

// recorded1298 is one emitted record: its body and attributes.
type recorded1298 struct {
	body  string
	attrs map[string]string
}

func oneRecord1298(t *testing.T, rec *logtest.Recorder) recorded1298 {
	t.Helper()
	var out []recorded1298
	for _, recs := range rec.Result() {
		for _, r := range recs {
			m := map[string]string{}
			for _, kv := range r.Attributes {
				m[string(kv.Key)] = kv.Value.String()
			}
			out = append(out, recorded1298{body: r.Body.AsString(), attrs: m})
		}
	}
	if len(out) != 1 {
		t.Fatalf("records = %d, want 1: %+v", len(out), out)
	}
	return out[0]
}

// callerStatus1298 decodes the failure envelope the caller received.
func callerStatus1298(t *testing.T, body []byte) response.Status {
	t.Helper()
	var st response.Status
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatalf("caller body is not a Status: %v: %s", err, body)
	}
	return st
}

func TestCall1298_FailureRecordBodyIsAClosedSetReasonClass(t *testing.T) {
	cases := []struct {
		name     string
		method   string
		reqBody  string
		status   int
		respBody string
		sentinel string
		// what the caller must still receive, as before #1298
		wantCallerMsg    string
		wantCallerReason string
		wantRecordBody   string
	}{
		{
			name: "S 403 Status naming the user", method: http.MethodPost, reqBody: cmRequest957,
			status: http.StatusForbidden, respBody: forbidden1298, sentinel: sentinelUser1298,
			wantCallerMsg: forbiddenMsg1298, wantCallerReason: "Forbidden",
			wantRecordBody: "upstream answered 403 Forbidden",
		},
		{
			name: "S 403 Status naming the user, delete", method: http.MethodDelete,
			status: http.StatusForbidden, respBody: forbidden1298, sentinel: sentinelUser1298,
			wantCallerMsg: forbiddenMsg1298, wantCallerReason: "Forbidden",
			wantRecordBody: "upstream answered 403 Forbidden",
		},
		{
			name: "N plain-text error body with a token", method: http.MethodPut, reqBody: cmRequest957,
			status: http.StatusBadRequest, respBody: "admission webhook said: Authorization: Bearer " + sentinelToken1298,
			sentinel:      sentinelToken1298,
			wantCallerMsg: "admission webhook said: Authorization: Bearer " + sentinelToken1298, wantCallerReason: "BadRequest",
			wantRecordBody: "upstream answered 400 Bad Request",
		},
		{
			name: "N JSON object (not a Status) error body with a token", method: http.MethodPatch, reqBody: `{"data":{"k":"w"}}`,
			status: http.StatusConflict, respBody: `{"error":"conflict","token":"` + sentinelToken1298 + `"}`,
			sentinel:      sentinelToken1298,
			wantCallerMsg: `{"error":"conflict","token":"` + sentinelToken1298 + `"}`, wantCallerReason: "Conflict",
			wantRecordBody: "upstream answered 409 Conflict",
		},
		{
			name: "N 5xx JSON object error body with a token", method: http.MethodPut, reqBody: cmRequest957,
			status: http.StatusBadGateway, respBody: `{"upstream":"` + sentinelToken1298 + `"}`,
			sentinel:       sentinelToken1298,
			wantCallerMsg:  `{"upstream":"` + sentinelToken1298 + `"}`,
			wantRecordBody: "upstream answered 502 Bad Gateway",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("CLIENT_MAX_RETRIES", "0") // a 5xx is retried; one answer is enough here
			api, ep := newFakeAPI443(t)
			api.respond(c.status, c.respBody)
			rec := recorder957(t)

			w := serve443(Call(), ep, c.method, "/call?"+cmQuery443, c.reqBody)

			// C — the caller's response is the upstream failure, as before.
			if w.Code != c.status {
				t.Fatalf("caller got %d, want the upstream's %d: %s", w.Code, c.status, w.Body.String())
			}
			st := callerStatus1298(t, w.Body.Bytes())
			if st.Status != response.StatusFailure || st.Code != c.status || st.Message != c.wantCallerMsg ||
				string(st.Reason) != c.wantCallerReason {
				t.Errorf("caller envelope = {status:%q code:%d reason:%q message:%q}, want {Failure %d %q %q}",
					st.Status, st.Code, st.Reason, st.Message, c.status, c.wantCallerReason, c.wantCallerMsg)
			}

			got := oneRecord1298(t, rec)
			if got.attrs["outcome"] != "failure" || got.attrs["krateo.action"] != "call" {
				t.Fatalf("precondition: record %+v is not the failed call record", got)
			}
			if got.attrs["http.response.status_code"] != strconv.Itoa(c.status) {
				t.Errorf("record status code = %q, want the real code %d", got.attrs["http.response.status_code"], c.status)
			}
			// S / N — no upstream text in the 365-day trail.
			if strings.Contains(got.body, c.sentinel) {
				t.Errorf("RED #1298: the record body carries upstream text %q: %q", c.sentinel, got.body)
			}
			for k, v := range got.attrs {
				if strings.Contains(v, c.sentinel) {
					t.Errorf("RED #1298: record attribute %s carries upstream text %q: %q", k, c.sentinel, v)
				}
			}
			if got.body != c.wantRecordBody {
				t.Errorf("record body = %q, want the reason class %q", got.body, c.wantRecordBody)
			}
			if got.body != audit.FailureMessage(c.status) {
				t.Errorf("record body = %q, not the shared closed set's %q", got.body, audit.FailureMessage(c.status))
			}
		})
	}
}

func TestCall1298_OK_SuccessAndDryRunRecordBodiesUnchanged(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		api, ep := newFakeAPI443(t)
		api.respond(http.StatusCreated, cmResponse957)
		rec := recorder957(t)
		w := serve443(Call(), ep, http.MethodPost, "/call?"+cmQuery443, cmRequest957)
		if w.Code != http.StatusOK {
			t.Fatalf("POST /call → %d, want 200: %s", w.Code, w.Body.String())
		}
		var obj map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &obj); err != nil || obj["kind"] != "ConfigMap" {
			t.Errorf("caller body = %s (err %v), want the apiserver's object", w.Body.String(), err)
		}
		got := oneRecord1298(t, rec)
		if got.attrs["outcome"] != "success" || got.attrs["krateo.action"] != "call" ||
			got.attrs["http.response.status_code"] != "200" {
			t.Fatalf("precondition: record %+v is not the successful call record", got)
		}
		if got.body != "" {
			t.Errorf("success record body = %q, want empty", got.body)
		}
	})
	t.Run("dry run", func(t *testing.T) {
		api, ep := newFakeAPI443(t)
		api.respond(http.StatusCreated, cmResponse957)
		rec := recorder957(t)
		w := serve443(CallDryRun(), ep, http.MethodPost, "/call/dry-run?"+cmQuery443, cmRequest957)
		if w.Code != http.StatusOK {
			t.Fatalf("dry run → %d: %s", w.Code, w.Body.String())
		}
		got := oneRecord1298(t, rec)
		if got.attrs["krateo.action"] != "call.dryRun" || got.attrs["outcome"] != "success" {
			t.Fatalf("precondition: record %+v is not the dry run's", got)
		}
		if got.body != "" {
			t.Errorf("dry-run record body = %q, want empty", got.body)
		}
	})
}
