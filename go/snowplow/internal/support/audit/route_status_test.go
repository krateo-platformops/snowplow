package audit

// route_status_test.go — OpenRecord reads the status the request was answered
// with from WriteHeader only (it never wraps Write: #1379 CodeQL), so a handler
// that writes a body without calling WriteHeader answered 200, as net/http
// does, and one that calls WriteHeader(4xx) answered that.
//
//   200  a handler that emits no record and writes a body with no WriteHeader
//        is read as 200: the fallback record is a failure with no code,
//        never a denied refusal;
//   4xx  a handler that answers WriteHeader(400) and emits no record is read
//        as 400: the fallback record is denied, invalid_request;
//   W    the body still reaches the client unchanged, and the writer the
//        handler sees reaches the original through Unwrap.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

func TestOpenRecord_StatusIsReadFromWriteHeaderOnly(t *testing.T) {
	route := Route{Asked: func(*http.Request) Event {
		return Event{Action: "call", HTTPMethod: http.MethodPost, Verb: "post", ServerAddress: "example.test"}
	}}
	for _, c := range []struct {
		name            string
		handler         func(w http.ResponseWriter)
		code            int
		outcome, reason string
	}{
		{"a body with no WriteHeader is a 200", func(w http.ResponseWriter) { _, _ = w.Write([]byte("ok")) },
			http.StatusOK, krateoaudit.OutcomeFailure, ""},
		{"no write at all is a 200", func(http.ResponseWriter) {},
			http.StatusOK, krateoaudit.OutcomeFailure, ""},
		{"an explicit 4xx", func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("bad"))
		}, http.StatusBadRequest, krateoaudit.OutcomeDenied, DenialInvalidRequest},
		{"an explicit 5xx", func(w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) },
			http.StatusInternalServerError, krateoaudit.OutcomeFailure, ""},
	} {
		t.Run(c.name, func(t *testing.T) {
			rec := audittest.NewRecorder(t)
			SetDefault(New(rec.Logger(Scope)))
			t.Cleanup(func() { SetDefault(nil) })
			h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				SetCaller(r.Context(), jwtutil.UserInfo{Username: "alice", Principal: jwtutil.PrincipalHuman})
				if _, ok := w.(interface{ Unwrap() http.ResponseWriter }); !ok {
					t.Errorf("the writer the handler sees does not Unwrap to the original")
				}
				c.handler(w)
			})
			req := httptest.NewRequest(http.MethodPost, "/x", nil)
			req = req.WithContext(WithSessionID(context.Background(), "s-route"))
			w := httptest.NewRecorder()
			OpenRecord(route)(Middleware()(h)).ServeHTTP(w, req)
			if w.Code != c.code {
				t.Fatalf("the client got %d, want %d", w.Code, c.code)
			}
			recs := rec.Records()
			if len(recs) != 1 {
				t.Fatalf("%d records, want 1", len(recs))
			}
			got := map[string]string{}
			for _, kv := range audittest.Attributes(recs[0]) {
				got[string(kv.Key)] = kv.Value.Emit()
			}
			if got[krateoaudit.KeyOutcome] != c.outcome || got[krateoaudit.KeyDenialReason] != c.reason {
				t.Errorf("outcome %q reason %q, want %q %q", got[krateoaudit.KeyOutcome], got[krateoaudit.KeyDenialReason], c.outcome, c.reason)
			}
			if v, ok := got[krateoaudit.KeyHTTPStatusCode]; ok {
				t.Errorf("the fallback record carries a status code %s: no authority answered", v)
			}
		})
	}
}
