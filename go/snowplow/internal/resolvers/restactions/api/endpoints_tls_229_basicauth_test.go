//go:build unit
// +build unit

package api

import (
	"net/http"
	"strings"
	"testing"
)

// endpoints_tls_229_basicauth_test.go — ARM 9 for snowplow#229 / audit A1:
// the CROSS-LIBRARY basic-auth definition mismatch.
//
// THE HAZARD, and why it is an OUTAGE rather than a degradation: the two
// libraries key "this endpoint uses basic auth" off DIFFERENT fields,
//
//	plumbing  v1.14.2  endpoints/types.go:29-31    HasBasicAuth() = len(Password) != 0  ← PASSWORD
//	client-go v0.33.0  transport/config.go:105-107 HasBasicAuth() = len(Username) != 0  ← USERNAME
//
// and client-go HARD-ERRORS on the overlap (transport/round_trippers.go:50-51,
// "username/password or bearer token may be set, but not both"). So an endpoint
// carrying a Token AND a Username with an EMPTY Password works TODAY — plumbing
// sees no password, classifies it token-auth, and sends the bearer token — but
// copying both credentials unconditionally into transport.Config would make
// transport.New refuse to build a client at all. Every request through that
// endpoint would fail, where today every request succeeds.
//
// httpClientForEndpoint therefore copies the basic-auth pair only when
// PLUMBING's predicate says so (ep.HasBasicAuth(), i.e. non-empty password),
// preserving plumbing's classification exactly. Adopting client-go's would
// silently change WHICH endpoints count as basic auth — a different, and
// unrequested, behavioural change.
//
// WHY THIS ARM IS NEEDED AT ALL: no other arm in this package sets a Username
// alongside a Token, so this shape is invisible to every one of them. Arms 1-8
// stay green against the unconditional-copy version of the code; only this one
// goes red.
//
// TWO ASSERTIONS, because either alone is passable by a wrong fix:
//
//  1. the request SUCCEEDS — catches the hard error (the outage);
//  2. the wire carries the BEARER token and NO Basic credential — catches a
//     "fix" that silences the error by dropping the token and sending basic
//     auth instead, which would change the presented IDENTITY. Arm 3's
//     identity-neutrality claim must hold for this shape too.
func TestIssue229_Arm9_TokenPlusUsernameEmptyPasswordStaysBearer(t *testing.T) {
	var gotAuth, gotVerb string
	srv, caPEM := tlsServerWithCA(t, sarHandler(&gotAuth, &gotVerb))

	// The exact hazardous shape: token-auth + CA (so this endpoint IS routed
	// to the owned branch — endpointNeedsOwnedCAClient is true), PLUS a
	// username with NO password. plumbing calls this token-auth; client-go
	// would call it basic auth.
	ep := saShapedEndpoint(srv.URL, caPEM)
	ep.Username = "someone"
	ep.Password = ""

	st, jsonBytes, err := drive(t, ep, http.MethodPost, sarPayload)

	// (1) It must still work. A client-go hard error surfaces here.
	if err != nil {
		t.Fatalf("ARM9 FAIL: a Token + Username + EMPTY Password endpoint must still build a "+
			"working client — plumbing classifies it token-auth and it succeeds today. Copying "+
			"Username/Password unconditionally makes client-go refuse with "+
			"\"username/password or bearer token may be set, but not both\", a total outage for "+
			"this endpoint. err: %v", err)
	}
	if st == nil || jsonBytes == nil {
		t.Fatalf("ARM9 FAIL: expected a response envelope and body; st=%+v body=%q", st, string(jsonBytes))
	}
	if !strings.Contains(string(jsonBytes), `"allowed":true`) {
		t.Fatalf("ARM9 FAIL: body did not carry the server's bytes, got %q", string(jsonBytes))
	}

	// (2) The presented identity must be the BEARER token, unchanged.
	want := "Bearer " + saTestToken
	if gotAuth != want {
		t.Fatalf("ARM9 FAIL: wrong credential on the wire.\n got: %q\nwant: %q\n"+
			"The username must NOT be promoted to a Basic credential: plumbing's semantics are "+
			"preserved, so an empty password means token-auth", gotAuth, want)
	}
	if strings.HasPrefix(gotAuth, "Basic ") {
		t.Fatalf("ARM9 FAIL: Basic auth was sent for an endpoint plumbing classifies as "+
			"token-auth; got %q", gotAuth)
	}
}
