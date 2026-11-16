package audit

// route_status_test.go — OpenRecord's fallback record (#1379 review F2).
//
//   4xx  an exit the handler answered 4xx without a record is snowplow's
//        refusal: one denied row, invalid_request, no status code. The status
//        is read from WriteHeader only (OpenRecord never wraps Write).
//   D    any other exit without a handler record (a 2xx with or without
//        WriteHeader, no write at all, a 5xx) is a DESIGN ERROR of the
//        handler: the row is a failure (the outcome is required and cannot be
//        marked) with MessageUnrecordedAnswer, never a description of what
//        was sent, and snowplow_audit_unrecorded_exit_total{answered} moves.
//        These cases exist to prove the detector can say yes; the end-to-end
//        arms (main_audit_e2e_test.go) fail if it ever fires on a real route.
//   P    a handler that panics before recording its outcome: one row saying
//        panic, the counter's {panic} cell moves, and the panic is re-raised.
//   S    the descriptor the handler stored (SetAsked) is the one the
//        fallback uses, not the route's.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"github.com/krateo-platformops/plumbing/jwtutil"
	sdklog "go.opentelemetry.io/otel/sdk/log"
)

func serveRouteStatus(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) (int, []sdklog.Record) {
	t.Helper()
	rec := routeRecorder(t)
	code := serveRoute(t, handler)
	return code, rec.Records()
}

func routeRecorder(t *testing.T) *audittest.Recorder {
	t.Helper()
	rec := audittest.NewRecorder(t)
	SetDefault(New(rec.Logger(Scope)))
	t.Cleanup(func() { SetDefault(nil) })
	return rec
}

func serveRoute(t *testing.T, handler func(w http.ResponseWriter, r *http.Request)) int {
	t.Helper()
	route := Route{Asked: func(*http.Request) Event {
		return Event{Action: "route-asked", HTTPMethod: http.MethodPost, Verb: "post", ServerAddress: "example.test"}
	}}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		SetCaller(r.Context(), jwtutil.UserInfo{Username: "alice", Principal: jwtutil.PrincipalHuman})
		if _, ok := w.(interface{ Unwrap() http.ResponseWriter }); !ok {
			t.Errorf("the writer the handler sees does not Unwrap to the original")
		}
		handler(w, r)
	})
	req := httptest.NewRequest(http.MethodPost, "/x", nil)
	req = req.WithContext(WithSessionID(context.Background(), "s-route"))
	w := httptest.NewRecorder()
	OpenRecord(route)(Middleware()(h)).ServeHTTP(w, req)
	return w.Code
}

func rowOf(t *testing.T, recs []sdklog.Record) (map[string]string, string) {
	t.Helper()
	if len(recs) != 1 {
		t.Fatalf("%d records, want exactly 1", len(recs))
	}
	got := map[string]string{}
	for _, kv := range audittest.Attributes(recs[0]) {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	if v, ok := got[krateoaudit.KeyHTTPStatusCode]; ok {
		t.Errorf("the fallback record carries a status code %s: no authority's answer is known", v)
	}
	return got, recs[0].Body().AsString()
}

func TestOpenRecord_4xx_ARefusalIsDeniedInvalidRequest(t *testing.T) {
	before := DesignErrorCount("answered")
	code, recs := serveRouteStatus(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("bad"))
	})
	if code != http.StatusBadRequest {
		t.Fatalf("the client got %d", code)
	}
	got, body := rowOf(t, recs)
	if got[krateoaudit.KeyOutcome] != krateoaudit.OutcomeDenied || got[krateoaudit.KeyDenialReason] != DenialInvalidRequest || body != MessageRequestRefused {
		t.Errorf("outcome %q reason %q body %q", got[krateoaudit.KeyOutcome], got[krateoaudit.KeyDenialReason], body)
	}
	if d := DesignErrorCount("answered") - before; d != 0 {
		t.Errorf("a refusal moved the design-error counter by %d", d)
	}
}

func TestOpenRecord_D_AnUnrecordedNon4xxExitIsADesignError(t *testing.T) {
	for _, c := range []struct {
		name    string
		handler func(w http.ResponseWriter, _ *http.Request)
		code    int
	}{
		{"a body with no WriteHeader (an implicit 200)", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }, http.StatusOK},
		{"no write at all (an implicit 200)", func(http.ResponseWriter, *http.Request) {}, http.StatusOK},
		{"an explicit 302", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusFound) }, http.StatusFound},
		{"an explicit 500", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }, http.StatusInternalServerError},
	} {
		t.Run(c.name, func(t *testing.T) {
			before := DesignErrorCount("answered")
			code, recs := serveRouteStatus(t, c.handler)
			if code != c.code {
				t.Fatalf("the client got %d, want %d", code, c.code)
			}
			got, body := rowOf(t, recs)
			if got[krateoaudit.KeyOutcome] != krateoaudit.OutcomeFailure || body != MessageUnrecordedAnswer {
				t.Errorf("outcome %q body %q, want failure %q", got[krateoaudit.KeyOutcome], body, MessageUnrecordedAnswer)
			}
			if _, ok := got[krateoaudit.KeyDenialReason]; ok {
				t.Errorf("a design error carries a denial reason")
			}
			if d := DesignErrorCount("answered") - before; d != 1 {
				t.Errorf("snowplow_audit_unrecorded_exit_total{answered} moved by %d, want 1", d)
			}
		})
	}
}

func TestOpenRecord_P_APanicIsOneRowSayingPanicAndIsReRaised(t *testing.T) {
	before := DesignErrorCount("panic")
	rec := routeRecorder(t)
	func() {
		defer func() {
			if p := recover(); p != "after the dial" {
				t.Errorf("the panic was not re-raised as it was (got %v)", p)
			}
		}()
		serveRoute(t, func(http.ResponseWriter, *http.Request) { panic("after the dial") })
		t.Errorf("the handler's panic did not propagate")
	}()
	got, body := rowOf(t, rec.Records())
	if got[krateoaudit.KeyOutcome] != krateoaudit.OutcomeFailure || body != MessagePanic {
		t.Errorf("outcome %q body %q, want failure %q", got[krateoaudit.KeyOutcome], body, MessagePanic)
	}
	if d := DesignErrorCount("panic") - before; d != 1 {
		t.Errorf("snowplow_audit_unrecorded_exit_total{panic} moved by %d, want 1", d)
	}
}

func TestOpenRecord_S_TheHandlersDescriptorIsUsed(t *testing.T) {
	_, recs := serveRouteStatus(t, func(w http.ResponseWriter, r *http.Request) {
		SetAsked(r.Context(), Event{Action: "handler-asked", HTTPMethod: http.MethodPut, Verb: "put", ServerAddress: "example.test"})
		w.WriteHeader(http.StatusBadRequest)
	})
	got, _ := rowOf(t, recs)
	if got[krateoaudit.KeyAction] != "handler-asked" {
		t.Errorf("krateo.action %q: the fallback did not use the handler's stored descriptor", got[krateoaudit.KeyAction])
	}
}
