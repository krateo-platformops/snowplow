// main_audit_410_test.go — AUDIT-4 (krateo-platformops/installer#410)
// criterion 4: authentication failures are a RECORDED decision, not an
// unstated omission. The decision (owner, 2026-10-25) is OUT: a request whose
// token fails the UserConfig middleware is answered 401 before any handler runs
// and leaves no audit record, because recording it would make the trail
// writable by anyone able to send a bad token. Those requests stay visible in
// the gateway access log. docs/architecture/audit-coverage.md states it.
//
// This arm pins the decision on the REAL write route table with the REAL
// middleware: if an emit is ever added on the authentication-failure path, it
// turns red and the inventory has to change with it. The control proves the
// same route table records a write that does authenticate.
package main

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
	"go.opentelemetry.io/otel/log/logtest"
)

func countRecords410(rec *logtest.Recorder) int {
	n := 0
	for _, recs := range rec.Result() {
		n += len(recs)
	}
	return n
}

func do443Req(method, target string) *http.Request {
	req := httptest.NewRequest(method, target, bytes.NewReader([]byte(cm443)))
	req.Header.Set("Content-Type", "application/json")
	return req
}

func serve410(h http.Handler, req *http.Request) int {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w.Code
}

func token410(t *testing.T, key *rsa.PrivateKey, d time.Duration) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: "alice", Groups: []string{"devs"}, Duration: d,
		KeyID: "kid-410", PrivateKey: key,
	})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	return tok
}

func TestAudit410_C4_AuthenticationFailure_IsNotAudited(t *testing.T) {
	rec := logtest.NewRecorder()
	audit.SetDefault(audit.New(rec.Logger("test-410")))
	t.Cleanup(func() { audit.SetDefault(nil) })

	trusted, _ := rsa.GenerateKey(rand.Reader, 2048)
	stranger, _ := rsa.GenerateKey(rand.Reader, 2048)
	got, ep := fakeAPIMain443(t)

	mux := http.NewServeMux()
	mountCallWriteRoutes(mux, use.NewChain(audit.Middleware()),
		middleware.UserConfig(jwtutil.NewStaticKeySource(&trusted.PublicKey), "krateo-system"))

	cases := map[string]string{
		"no header":        "",
		"not bearer":       "Basic YWxpY2U6cHc=",
		"garbage token":    "Bearer not-a-jwt",
		"untrusted signer": "Bearer " + token410(t, stranger, time.Hour),
		"expired, trusted": "Bearer " + token410(t, trusted, -time.Hour),
	}
	for name, authz := range cases {
		for _, m := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
			req := do443Req(m, "/call"+cmQ443)
			if authz != "" {
				req.Header.Set("Authorization", authz)
			}
			w := serve410(mux, req)
			if w != http.StatusUnauthorized {
				t.Fatalf("%s %s: code %d, want 401", name, m, w)
			}
		}
	}
	if n := countRecords410(rec); n != 0 {
		t.Fatalf("authentication failures emitted %d audit records, want 0 (the documented exclusion)", n)
	}
	if n := len(got()); n != 0 {
		t.Fatalf("precondition: %d apiserver requests behind a failed authentication, want 0", n)
	}

	// Control: the same route table, an authenticated caller — one record.
	ok := http.NewServeMux()
	mountCallWriteRoutes(ok, use.NewChain(audit.Middleware()), testUserConfig443(ep))
	if code := serve410(ok, do443Req(http.MethodPost, "/call"+cmQ443)); code != http.StatusOK {
		t.Fatalf("control: authenticated POST /call → %d, want 200", code)
	}
	if n := countRecords410(rec); n != 1 {
		t.Fatalf("control: an authenticated write emitted %d records, want 1", n)
	}
}
