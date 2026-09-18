//go:build unit
// +build unit

package api

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
)

// endpoints_tls_229_silent_drop_test.go — ARM 11 for snowplow#229 / audit A1:
// the MIRROR IMAGE of the basic-auth field mismatch, and the one that fails
// SILENTLY.
//
// Numbered 11 because 8 (CA shape detection), 9 (token + username + empty
// password) and 10 (delegation parity) are taken.
//
// THE SHAPE: Password set, Username EMPTY, no token, CA present, no client
// certs.
//
//	plumbing  v1.14.2  endpoints/types.go:29-31    HasBasicAuth() = len(Password) != 0  → TRUE
//	client-go v0.33.0  transport/config.go:105-107 HasBasicAuth() = len(Username) != 0  → FALSE
//
// plumbing therefore sends `Authorization: Basic base64(":password")` today.
// Handing the same credentials to transport.Config yields NO basic wrapper
// (client-go sees an empty Username) and NO bearer wrapper (there is no token),
// so the request would go out with NO Authorization header at all.
//
// WHY THIS IS WORSE THAN THE BUG ARM 9 COVERS. Arm 9's shape fails LOUDLY — a
// client-construction error, immediate and traceable. This one produces a
// well-formed request that the server rejects as unauthenticated, with nothing
// anywhere saying a credential was dropped. The next engineer chases a
// server-side permissions problem that does not exist.
//
// WHY EXCLUSION RATHER THAN A GATE: the shape is not expressible in
// transport.Config at all, because the two libraries disagree about which field
// defines basic auth. Selecting either library's predicate changes nothing. So
// the endpoint is delegated to plumbing and keeps exactly the behaviour it has
// today — the same principle as the AWS exclusion: the owned branch owns only
// what it can reproduce exactly.
//
// TWO ASSERTIONS, at the two levels that can independently break:
//
//  1. ROUTING — endpointNeedsOwnedCAClient must be FALSE for this shape, pinned
//     directly the way ARM 7 pins the predicate. This is the assertion that
//     survives a later refactor of the builder's internals.
//  2. THE WIRE — the request must actually carry `Authorization: Basic` with
//     plumbing's exact `:password` encoding. Routing alone is not enough: it
//     says which branch was taken, not that the credential arrived.
//
// RED CONTROL (captured): drop the `!(ep.HasBasicAuth() && ep.Username == "")`
// conjunct from endpointNeedsOwnedCAClient and this arm fails with NO
// Authorization header on the wire — the silent drop, reproduced.
//
// THE WIRE HALF RUNS OVER PLAIN HTTP, AND THAT IS ITSELF A FINDING. Driving it
// against a private-CA TLS server FAILS with "x509: certificate signed by
// unknown authority" — because this shape is DELEGATED, and plumbing's early
// return still discards its CA. That is the unfixed remainder of the class
// stated in the commit message: delegating preserves today's behaviour, which
// for a private-CA basic-auth endpoint is still broken upstream. We are not
// fixing that here and not making it worse; the exclusion exists to stop a
// LOUD upstream failure being converted into a SILENT credential drop. So this
// arm isolates the credential question from the TLS question: the CA stays on
// the endpoint (it is what makes the shape real and drives the routing), while
// the server is plain HTTP so the handshake is not the variable under test.
func TestIssue229_Arm11_PasswordOnlyBasicAuthIsNotSilentlyDropped(t *testing.T) {
	// A valid CA, present on the endpoint so HasCA() holds and the routing
	// assertion is meaningful.
	_, caPEM := tlsServerWithCA(t, func(http.ResponseWriter, *http.Request) {})

	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)

	ep := &endpoints.Endpoint{
		ServerURL:                srv.URL,
		Password:                 "s3cret",
		Username:                 "", // EMPTY — the whole point
		CertificateAuthorityData: string(caPEM),
	}

	// (1) ROUTING: this shape must NOT be owned.
	if endpointNeedsOwnedCAClient(ep) {
		t.Fatalf("ARM11 FAIL: a password-set/username-EMPTY endpoint with a CA must be DELEGATED "+
			"to plumbing, not handled by the owned builder. client-go keys basic auth off "+
			"Username, so the owned path would install neither a basic nor a bearer wrapper and "+
			"the credential would vanish with no error anywhere. ep=%+v", ep)
	}

	// (2) THE WIRE: the credential must actually arrive.
	cli, err := httpClientForEndpoint(ep, &httpcall.RequestInfo{Verb: ptr.To(http.MethodGet)})
	if err != nil {
		t.Fatalf("ARM11 FAIL: building a client for this shape errored: %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("ARM11 FAIL: request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if gotAuth == "" {
		t.Fatalf("ARM11 FAIL: THE SILENT DROP — the request reached the server with NO " +
			"Authorization header. plumbing sends Basic base64(\":password\") for this shape " +
			"today; the owned path installs no wrapper at all because client-go keys basic auth " +
			"off Username. This is exactly why the shape must be excluded rather than gated")
	}
	if !strings.HasPrefix(gotAuth, "Basic ") {
		t.Fatalf("ARM11 FAIL: want a Basic credential on the wire (plumbing's classification); "+
			"got %q", gotAuth)
	}

	// Plumbing's exact encoding: empty username, colon, password.
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte(":s3cret"))
	if gotAuth != want {
		t.Fatalf("ARM11 FAIL: the credential on the wire is not the one plumbing sends today.\n"+
			" got: %q\nwant: %q", gotAuth, want)
	}
}
