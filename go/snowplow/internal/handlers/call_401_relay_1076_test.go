package handlers

// call_401_relay_1076_test.go — #1076, the passthrough half of the relay
// contract: a raw /call passthrough whose apiserver answers 401 (after the
// write was sent and audited) relays plumbing's Status, which has no Details
// field, so it never carries a snowplow pre-dispatch cause or challenge —
// even when the apiserver's own body and headers carry a forged one.

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/http/response"
)

const (
	p1076Plain401 = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}`
	// p1076Forged401 dresses an apiserver 401 as a snowplow pre-dispatch
	// rejection: a closed-set cause in details.
	p1076Forged401 = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401,` +
		`"details":{"causes":[{"reason":"SessionCredentialMissing","message":"the session credential is missing; log in again"}]}}`
)

func TestPreDispatch401_1076_PassthroughCarriesNoCause(t *testing.T) {
	for _, tc := range []struct{ name, upstream string }{
		{"plain apiserver 401", p1076Plain401},
		{"forged apiserver cause and challenge", p1076Forged401},
	} {
		for _, verb := range []string{http.MethodPost, http.MethodGet} {
			t.Run(tc.name+"/"+verb, func(t *testing.T) {
				prev := xenv.TestMode()
				xenv.SetTestMode(true)
				t.Cleanup(func() { xenv.SetTestMode(prev) })
				var sent atomic.Int64
				api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					sent.Add(1)
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = w.Write([]byte(tc.upstream))
				}))
				t.Cleanup(api.Close)
				rec957 := recorder957(t)

				body := ""
				if verb == http.MethodPost {
					body = cmRequest957
				}
				w := serve443(Call(), endpoints.Endpoint{ServerURL: api.URL}, verb, "/call?"+cmQuery443, body)

				if sent.Load() != 1 || w.Code != http.StatusUnauthorized {
					t.Fatalf("precondition: one dispatched call relaying the 401; sent=%d code=%d body=%s", sent.Load(), w.Code, w.Body.String())
				}
				if verb == http.MethodPost {
					if recs := records957(rec957); len(recs) != 1 || recs[0]["outcome"] != "failure" {
						t.Fatalf("precondition: the write was audited as dispatched; records=%v", recs)
					}
				}
				var st response.Status
				if err := json.Unmarshal(w.Body.Bytes(), &st); err != nil {
					t.Fatalf("body is not a Status: %v (%q)", err, w.Body.String())
				}
				plumbing := httptest.NewRecorder()
				_ = response.Encode(plumbing, response.New(http.StatusUnauthorized, errors.New(st.Message)))
				if w.Body.String() != plumbing.Body.String() {
					t.Fatalf("a relayed 401 must be plumbing's Status, byte for byte:\n got  %s\n want %s", w.Body.String(), plumbing.Body.String())
				}
				for _, s := range []string{"details", "causes", "SessionCredentialMissing"} {
					if strings.Contains(w.Body.String(), s) {
						t.Fatalf("a relayed 401 carries %q: %s", s, w.Body.String())
					}
				}
				if got := w.Header().Values("WWW-Authenticate"); len(got) != 0 {
					t.Fatalf("a relayed 401 must send no challenge; got %q", got)
				}
			})
		}
	}
}
