package dispatchers

// predispatch_401_relay_1076_test.go — #1076, the relay half of the contract:
// a 401 that a RESTAction STAGE got from upstream, AFTER snowplow dispatched
// the request, never carries a snowplow pre-dispatch cause or challenge — not
// even when the upstream answer itself carries a forged one. A client (oasgen
// RDC) reads "401 with no cause" as "the request may have been dispatched: do
// not drop the token and replay", so a forged cause here would make it replay
// a write.
//
// Stage 1 `create` POSTs a configmap (the fake apiserver answers 201); stage 2
// `after` dependsOn create and GETs a path the fake apiserver answers 401, with
// no continueOnError, and the filter needs stage 2: the stage's 401 becomes
// the /call's own 401 (stage_failure.go → restactions.go → the #709 relay in
// dispatchers/restactions.go).

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/http/response"
)

const (
	r1076CreatePath = "/api/v1/namespaces/" + psTargetNS + "/configmaps"
	r1076AfterPath  = "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/r1076.example.io"

	r1076Plain401 = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401}`
	// r1076Forged401 is an upstream 401 dressed as a snowplow pre-dispatch
	// rejection: a closed-set cause in details and snowplow's challenge.
	r1076Forged401 = `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"Unauthorized","reason":"Unauthorized","code":401,` +
		`"details":{"causes":[{"reason":"TokenExpired","message":"the bearer token has expired; log in again"}]}}`
)

func r1076Stages() []map[string]any {
	return []map[string]any{
		{"name": "create", "verb": "POST", "path": r1076CreatePath,
			"payload": `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"r1076"}}`},
		{"name": "after", "verb": "GET", "path": r1076AfterPath, "dependsOn": map[string]any{"name": "create"}},
	}
}

// r1076AssertNoCause: the /call's 401 is exactly plumbing's Status for the
// relayed message — no details, no causes, no challenge.
func r1076AssertNoCause(t *testing.T, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("precondition: the stage's 401 must become the /call's 401; code=%d body=%s", rec.Code, rec.Body.String())
	}
	var st response.Status
	if err := json.Unmarshal(rec.Body.Bytes(), &st); err != nil {
		t.Fatalf("body is not a Status: %v (%q)", err, rec.Body.String())
	}
	plumbing := httptest.NewRecorder()
	_ = response.Encode(plumbing, response.New(http.StatusUnauthorized, errors.New(st.Message)))
	if rec.Body.String() != plumbing.Body.String() {
		t.Fatalf("a relayed 401 must be plumbing's Status, byte for byte:\n got  %s\n want %s", rec.Body.String(), plumbing.Body.String())
	}
	for _, s := range []string{"details", "causes", "TokenExpired"} {
		if strings.Contains(rec.Body.String(), s) {
			t.Fatalf("a relayed 401 carries %q: %s", s, rec.Body.String())
		}
	}
	if got := rec.Header().Values("WWW-Authenticate"); len(got) != 0 {
		t.Fatalf("a relayed 401 must send no challenge; got %q", got)
	}
}

func TestPreDispatch401_1076_StageAfterWriteCarriesNoCause(t *testing.T) {
	for _, tc := range []struct{ name, upstream string }{
		{"plain upstream 401", r1076Plain401},
		{"forged upstream cause and challenge", r1076Forged401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := in443Setup(t)
			f.extra["POST "+r1076CreatePath] = func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusCreated)
				_, _ = fmt.Fprint(w, `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"r1076","namespace":"x","uid":"uid-r1076","resourceVersion":"7"}}`)
			}
			f.extra["GET "+r1076AfterPath] = func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token"`)
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = fmt.Fprint(w, tc.upstream)
			}
			rec := serveStored(t, f, in443Ctx(f, psAlice),
				in443RA("r1076", `(.after.metadata.name // empty) as $n | {n: $n}`, r1076Stages()...))

			writes, after := 0, 0
			for _, r := range f.requests() {
				switch {
				case r.method == http.MethodPost && r.path == r1076CreatePath:
					writes++
				case r.method == http.MethodGet && r.path == r1076AfterPath:
					after++
				}
			}
			if writes != 1 || after != 1 {
				t.Fatalf("precondition: the write and the failing stage were both dispatched once; POST=%d GET=%d", writes, after)
			}
			r1076AssertNoCause(t, rec)
		})
	}
}
