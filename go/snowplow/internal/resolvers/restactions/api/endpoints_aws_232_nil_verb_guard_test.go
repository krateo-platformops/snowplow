//go:build unit
// +build unit

package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
)

// endpoints_aws_232_nil_verb_guard_test.go — snowplow#232.
//
// The owned external fetch (external_fetch.go: httpFetchAllowingNonJSON)
// hands opts.RequestInfo to plumbing's SigV4 signer on the AWS-auth branch
// — via the client builder (httpClientForEndpoint → HTTPClientForEndpoint →
// ComputeAwsSignature). plumbing dereferences RequestInfo.Verb UNGUARDED
// (http/request/util.go:134 — `method := *ex.Verb`; it nil-checks the
// struct pointer but not the Verb field). The fetch already defaults a nil
// Verb to GET for the actual request (external_fetch.go: `verb :=
// ptr.Deref(opts.Verb, http.MethodGet)`), but never carries that default
// onto RequestInfo.Verb, so an AWS-auth endpoint reached with a nil Verb
// panics instead of signing a GET.
//
// Dormant on 057 (no AWS-auth endpoint exists there), but one config away:
// any path that reaches an AWS endpoint without populating Verb fires it.
// This drives the REAL owned fetch path with a nil Verb — it PANICS on
// pre-fix code and, with the guard, signs a GET and completes normally.
func TestIssue232_NilVerb_AwsEndpoint_DoesNotPanic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)

	// Same AWS-auth shape as ARM 10's delegation-parity case (SigV4 with a
	// CA), minus the CA — the panic is on the header-signing path and does
	// not need TLS.
	ep := &endpoints.Endpoint{
		ServerURL:    srv.URL,
		AwsAccessKey: "AKIAEXAMPLE",
		AwsSecretKey: "secret",
		AwsRegion:    "eu-west-1",
		AwsService:   "execute-api",
	}
	if !ep.HasAwsAuth() {
		t.Fatalf("test setup: endpoint is not AWS-auth, cannot reach the panic path")
	}

	// The unguarded second precondition: RequestInfo with a nil Verb.
	opts := httpcall.RequestOptions{
		RequestInfo: httpcall.RequestInfo{
			Path: "/anything",
			Verb: nil,
		},
		Endpoint: ep,
	}

	// Pre-fix: this call panics inside plumbing's ComputeAwsSignature
	// (util.go:134), reached via the AWS client builder. Post-fix: the nil
	// Verb is defaulted to GET, the request is signed and completes with the
	// server's 2xx JSON body.
	status, jsonBytes, _, err := httpFetchAllowingNonJSON(context.Background(), opts)
	if err != nil {
		t.Fatalf("#232: owned AWS fetch with nil Verb returned a hard error: %v", err)
	}
	if status == nil || status.Code != http.StatusOK {
		t.Fatalf("#232: expected a 200 status envelope, got %+v", status)
	}
	if string(jsonBytes) != `{"ok":true}` {
		t.Fatalf("#232: expected the server JSON body, got %q", string(jsonBytes))
	}
}
