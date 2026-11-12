package handlers

// call_audit_answer_1298_test.go — #1298 review: a failed /call write is
// recorded from what was observed at the dial (credhttp.Answer), never from
// the envelope's code, which snowplow often made up itself. Driven through the
// REAL /call handler.
//
//   R  snowplow's own doing, nothing answered: connection refused, a refused
//      destination, the page budget (not sent; sent and possibly applied). The
//      record carries NO http.response.status_code ("when the authority
//      answered", audit-coverage.md) and a closed-set body saying what snowplow
//      did, never "upstream answered".
//   X  the upstream answered a 3xx and snowplow refused to follow it: the
//      record carries the 3xx, and says snowplow refused it.
//   J  the upstream answered 2xx with a non-JSON body: the write was accepted,
//      so the record is a success with the real 2xx code and a note, as the
//      stage record already says (stage_audit.go).
//   U  real upstream answers 403, 404, 409: the real code, "upstream answered".
//   D  a failed dry run is recorded as call.dryRun with the same closed set.
//   C  in every arm the CALLER's response is unchanged.

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

// rawAPI1298 starts a fake apiserver with handler h (test mode on, so the
// injected ServerURL is kept, as newFakeAPI443 does).
func rawAPI1298(t *testing.T, h http.HandlerFunc) endpoints.Endpoint {
	t.Helper()
	prev := xenv.TestMode()
	xenv.SetTestMode(true)
	srv := httptest.NewServer(h)
	t.Cleanup(func() {
		srv.Close()
		xenv.SetTestMode(prev)
	})
	return endpoints.Endpoint{ServerURL: srv.URL}
}

// serveBudget1298 is serve443 with the request inside a customer page budget
// that started at arrival.
func serveBudget1298(ep endpoints.Endpoint, arrival time.Time) *httptest.ResponseRecorder {
	req := withCaller443(httptest.NewRequest(http.MethodPost, "/call?"+cmQuery443, strings.NewReader(cmRequest957)), ep)
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(pagebudget.WithCustomer(req.Context(), arrival))
	w := httptest.NewRecorder()
	Call().ServeHTTP(w, req)
	return w
}

func TestCall1298_RecordIsWhatWasObservedAtTheDial(t *testing.T) {
	cases := []struct {
		name  string
		serve func(t *testing.T) *httptest.ResponseRecorder
		// what the caller receives, unchanged by #1298
		wantCaller int
		// the record
		wantOutcome string
		wantCode    int // 0 = no http.response.status_code attribute
		wantBody    string
	}{
		{
			name: "R connection refused",
			serve: func(t *testing.T) *httptest.ResponseRecorder {
				_, ep := newFakeAPI443(t)
				gone := httptest.NewServer(http.NotFoundHandler())
				ep.ServerURL = gone.URL
				gone.Close() // nothing listens
				return serve443(Call(), ep, http.MethodPost, "/call?"+cmQuery443, cmRequest957)
			},
			wantCaller: http.StatusInternalServerError, wantOutcome: "failure", wantCode: 0,
			wantBody: audit.MessageNoResponse,
		},
		{
			name: "R refused destination",
			serve: func(t *testing.T) *httptest.ResponseRecorder {
				_, ep := newFakeAPI443(t)
				ep.ServerURL = "http://169.254.169.254" // link-local: refused in the dialer, never sent
				return serve443(Call(), ep, http.MethodPost, "/call?"+cmQuery443, cmRequest957)
			},
			// #1291 — snowplow itself refused: denied (schema v1), and a
			// denied record never carries a status code (audit#13).
			wantCaller: http.StatusBadGateway, wantOutcome: "denied", wantCode: 0,
			wantBody: audit.MessageDestinationRefused,
		},
		{
			name: "R page budget spent before the send",
			serve: func(t *testing.T) *httptest.ResponseRecorder {
				api, ep := newFakeAPI443(t)
				w := serveBudget1298(ep, time.Now().Add(-2*pagebudget.PageMaxGoal))
				if n := len(api.requests()); n != 0 {
					t.Fatalf("precondition: a spent budget sent %d requests", n)
				}
				return w
			},
			wantCaller: http.StatusGatewayTimeout, wantOutcome: "failure", wantCode: 0,
			wantBody: audit.MessageBudgetNotSent,
		},
		{
			name: "R page budget ran out after the write was sent",
			serve: func(t *testing.T) *httptest.ResponseRecorder {
				release := make(chan struct{})
				ep := rawAPI1298(t, func(w http.ResponseWriter, r *http.Request) {
					select { // hold the answer past the budget
					case <-r.Context().Done():
					case <-release:
					}
				})
				t.Cleanup(func() { close(release) })
				return serveBudget1298(ep, time.Now().Add(-pagebudget.PageMaxGoal+400*time.Millisecond))
			},
			wantCaller: http.StatusGatewayTimeout, wantOutcome: "failure", wantCode: 0,
			wantBody: audit.MessageBudgetMayBeApplied,
		},
		{
			name: "X refused redirect",
			serve: func(t *testing.T) *httptest.ResponseRecorder {
				ep := rawAPI1298(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Location", "/api/v1/namespaces/default/configmaps/elsewhere")
					w.WriteHeader(http.StatusFound)
				})
				return serve443(Call(), ep, http.MethodPost, "/call?"+cmQuery443, cmRequest957)
			},
			wantCaller: http.StatusBadGateway, wantOutcome: "failure", wantCode: http.StatusFound,
			wantBody: "snowplow refused to follow a 302 Found redirect",
		},
		{
			name: "J 2xx with a non-JSON body",
			serve: func(t *testing.T) *httptest.ResponseRecorder {
				ep := rawAPI1298(t, func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/plain")
					w.WriteHeader(http.StatusCreated)
					_, _ = w.Write([]byte("created"))
				})
				return serve443(Call(), ep, http.MethodPost, "/call?"+cmQuery443, cmRequest957)
			},
			wantCaller: http.StatusNotAcceptable, wantOutcome: "success", wantCode: http.StatusCreated,
			wantBody: audit.NoteBodyNotJSON,
		},
		{
			name: "U real 403",
			serve: func(t *testing.T) *httptest.ResponseRecorder {
				api, ep := newFakeAPI443(t)
				api.respond(http.StatusForbidden, forbidden1298)
				return serve443(Call(), ep, http.MethodPost, "/call?"+cmQuery443, cmRequest957)
			},
			wantCaller: http.StatusForbidden, wantOutcome: "failure", wantCode: http.StatusForbidden,
			wantBody: "upstream answered 403 Forbidden",
		},
		{
			name: "U real 404",
			serve: func(t *testing.T) *httptest.ResponseRecorder {
				api, ep := newFakeAPI443(t)
				api.respond(http.StatusNotFound, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,"message":"configmaps \"cm-x\" not found"}`)
				return serve443(Call(), ep, http.MethodDelete, "/call?"+cmQuery443, "")
			},
			wantCaller: http.StatusNotFound, wantOutcome: "failure", wantCode: http.StatusNotFound,
			wantBody: "upstream answered 404 Not Found",
		},
		{
			name: "U real 409",
			serve: func(t *testing.T) *httptest.ResponseRecorder {
				api, ep := newFakeAPI443(t)
				api.respond(http.StatusConflict, `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"AlreadyExists","code":409,"message":"configmaps \"cm-x\" already exists"}`)
				return serve443(Call(), ep, http.MethodPost, "/call?"+cmQuery443, cmRequest957)
			},
			wantCaller: http.StatusConflict, wantOutcome: "failure", wantCode: http.StatusConflict,
			wantBody: "upstream answered 409 Conflict",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := recorder957(t)
			w := c.serve(t)

			// C — the caller's response is what it was before #1298.
			if w.Code != c.wantCaller {
				t.Fatalf("caller got %d, want %d: %s", w.Code, c.wantCaller, w.Body.String())
			}
			if st := callerStatus1298(t, w.Body.Bytes()); st.Status != response.StatusFailure || int(st.Code) != c.wantCaller {
				t.Fatalf("caller envelope = %+v, want a %d Failure", st, c.wantCaller)
			}

			got := oneRecord1298(t, rec)
			if got.attrs["krateo.action"] != "call" {
				t.Fatalf("precondition: record %+v is not the call record", got)
			}
			if got.attrs["outcome"] != c.wantOutcome {
				t.Errorf("RED #1298: record outcome = %q, want %q", got.attrs["outcome"], c.wantOutcome)
			}
			code, has := got.attrs["http.response.status_code"]
			switch {
			case c.wantCode == 0 && has:
				t.Errorf("RED #1298: nothing answered, but the record carries status code %s (snowplow's own %d)", code, c.wantCaller)
			case c.wantCode != 0 && code != strconv.Itoa(c.wantCode):
				t.Errorf("RED #1298: record status code = %q, want the observed %d (the caller got %d)", code, c.wantCode, c.wantCaller)
			}
			if got.body != c.wantBody {
				t.Errorf("RED #1298: record body = %q, want %q", got.body, c.wantBody)
			}
			// What snowplow did itself never reads as an upstream answer
			// (the #1131 rule, issue1131_destination_test.go).
			if c.wantCode == 0 && strings.Contains(got.body, "upstream answered") {
				t.Errorf("RED #1298: nothing answered, but the record says %q", got.body)
			}
		})
	}
}

// D — a failed dry run goes through the same branch: call.dryRun, the observed
// code, the closed set, no upstream text.
func TestCall1298_D_FailedDryRunRecordsTheClosedSet(t *testing.T) {
	api, ep := newFakeAPI443(t)
	api.respond(http.StatusForbidden, forbidden1298)
	rec := recorder957(t)

	w := serve443(CallDryRun(), ep, http.MethodPost, "/call/dry-run?"+cmQuery443, cmRequest957)

	if w.Code != http.StatusForbidden {
		t.Fatalf("caller got %d, want 403: %s", w.Code, w.Body.String())
	}
	if st := callerStatus1298(t, w.Body.Bytes()); st.Message != forbiddenMsg1298 {
		t.Errorf("caller message = %q, want the apiserver's", st.Message)
	}
	got := oneRecord1298(t, rec)
	if got.attrs["krateo.action"] != "call.dryRun" || got.attrs["outcome"] != "failure" ||
		got.attrs["http.response.status_code"] != "403" {
		t.Fatalf("record %+v is not the failed dry run's", got)
	}
	if got.body != "upstream answered 403 Forbidden" {
		t.Errorf("RED #1298: dry-run record body = %q", got.body)
	}
	for k, v := range got.attrs {
		if strings.Contains(v, sentinelUser1298) {
			t.Errorf("RED #1298: attribute %s carries the user: %q", k, v)
		}
	}
}
