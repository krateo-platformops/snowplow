//go:build unit
// +build unit

package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/ptr"
)

// endpoints_tls_229_delegation_parity_test.go — ARM 10 for snowplow#229 /
// audit A1: "DELEGATES VERBATIM for every other shape" is a CHECKED claim,
// not a comment.
//
// A1 introduces a SECOND client-construction path. The risk that creates is not
// the shape it was built for — arms 1-5 cover that — but the shapes it was NOT
// built for: if the owned builder quietly diverges on one of them, every
// endpoint of that shape changes behaviour with nothing pointing at the cause.
// ARM 7 pins WHICH shapes route where (the predicate); this arm pins that for
// the shapes routed AWAY, the two builders actually agree.
//
// THE NON-OWNED SHAPES, each a deliberate exclusion in endpointNeedsOwnedCAClient:
//
//	no CA                  — nothing to install; plumbing's system-root client is
//	                         already correct.
//	cert-auth + CA         — plumbing DOES take its CA branch. This is the entire
//	                         live `<user>-clientconfig` population (all 7 Secrets
//	                         on 057) and MUST stay byte-identical.
//	Insecure + CA          — the operator explicitly asked to skip verification;
//	                         installing a root pool would override that choice.
//	AWS auth + CA          — SigV4 is unexported in plumbing; delegating leaves
//	                         this shape exactly as broken as it is today, which is
//	                         status quo and no regression.
//	CA in an unrecognised  — caPEMBytes returns ok=false and the builder delegates
//	encoding                 rather than build a client with an EMPTY root pool,
//	                         because an empty pool silently falls back to the
//	                         system roots — the very failure this file removes.
//
// WHAT IS ASSERTED, per shape: the owned entry point and the delegate agree on
// (a) error-vs-success, with the SAME error text when both fail, and (b) when
// both succeed, what reaches the wire — same status, same body, same
// Authorization header — driven against the same server. (b) is what makes this
// more than a smoke test: a builder that silently dropped the bearer wrapper, or
// added one, passes (a) and fails (b).
//
// The drive is over plain HTTP deliberately. DELEGATION EQUIVALENCE is the
// property under test and TLS is not the variable; for the shape where TLS IS
// the variable (cert-auth against a real private CA) ARM 6 already drives a real
// handshake end to end. Plain HTTP is also what lets this arm cover insecure and
// AWS-auth, which cannot be driven against a self-signed server without changing
// the very thing being compared.
func TestIssue229_Arm10_DelegationParityForNonOwnedShapes(t *testing.T) {
	// A CA that IS valid PEM, so "unrecognised encoding" below is the only
	// deliberately malformed one.
	_, caPEM := tlsServerWithCA(t, func(http.ResponseWriter, *http.Request) {})

	cases := []struct {
		name string
		ep   func(serverURL string) *endpoints.Endpoint
		// ownedPredicate is what endpointNeedsOwnedCAClient must return for this
		// shape. There are TWO delegation sites, reached differently: a shape the
		// PREDICATE rejects delegates at endpoints_tls.go:84, while an OWNED shape
		// whose CA is in an unrecognised encoding delegates deeper, at :95, after
		// caPEMBytes fails. Both must reproduce plumbing's behaviour exactly, so
		// both are covered here — and pinning the expected value in BOTH directions
		// is what stops a case drifting into the other branch and silently
		// measuring nothing.
		ownedPredicate bool
	}{
		{
			name: "no CA at all",
			ep: func(u string) *endpoints.Endpoint {
				return &endpoints.Endpoint{ServerURL: u, Token: saTestToken}
			},
			ownedPredicate: false,
		},
		{
			name: "cert-auth with a CA (the live clientconfig population)",
			ep: func(u string) *endpoints.Endpoint {
				certPEM, keyPEM := selfSignedClientPair(t)
				return &endpoints.Endpoint{
					ServerURL:                u,
					CertificateAuthorityData: string(caPEM),
					ClientCertificateData:    string(certPEM),
					ClientKeyData:            string(keyPEM),
				}
			},
			ownedPredicate: false,
		},
		{
			name: "insecure with a CA",
			ep: func(u string) *endpoints.Endpoint {
				return &endpoints.Endpoint{
					ServerURL:                u,
					Token:                    saTestToken,
					CertificateAuthorityData: string(caPEM),
					Insecure:                 true,
				}
			},
			ownedPredicate: false,
		},
		{
			name: "AWS auth with a CA (SigV4 unexported in plumbing)",
			ep: func(u string) *endpoints.Endpoint {
				return &endpoints.Endpoint{
					ServerURL:                u,
					CertificateAuthorityData: string(caPEM),
					AwsAccessKey:             "AKIAEXAMPLE",
					AwsSecretKey:             "secret",
					AwsRegion:                "eu-west-1",
					AwsService:               "execute-api",
				}
			},
			ownedPredicate: false,
		},
		{
			name: "CA present but in an unrecognised encoding",
			ep: func(u string) *endpoints.Endpoint {
				return &endpoints.Endpoint{
					ServerURL:                u,
					Token:                    saTestToken,
					CertificateAuthorityData: "!!! not pem and not base64 !!!",
				}
			},
			// OWNED by the predicate, but delegated INTERNALLY when caPEMBytes
			// cannot recognise the CA — the :95 site. Building a client with an
			// empty root pool here would silently fall back to the system roots,
			// which is the exact failure this file exists to remove, so the only
			// correct behaviour is plumbing's.
			ownedPredicate: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ownedAuth, delegateAuth string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Header.Get("X-A1-Parity-Caller") {
				case "owned":
					ownedAuth = r.Header.Get("Authorization")
				case "delegate":
					delegateAuth = r.Header.Get("Authorization")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"parity":true}`))
			}))
			t.Cleanup(srv.Close)

			// Pin WHICH delegation site this case exercises, in both directions.
			// A case that drifted into the other branch would still satisfy the
			// parity assertions below while no longer testing the site it was
			// written for.
			if got := endpointNeedsOwnedCAClient(tc.ep(srv.URL)); got != tc.ownedPredicate {
				t.Fatalf("ARM10 FAIL [%s]: endpointNeedsOwnedCAClient = %v, want %v. Either the "+
					"routing predicate changed or this case is miscategorised — reconcile with "+
					"ARM 7 before touching the assertions below", tc.name, got, tc.ownedPredicate)
			}

			// A POPULATED RequestInfo, as every production call site has.
			// plumbing dereferences ex.Verb unguarded on the AWS path
			// (http/request/util.go:134 — it nil-checks ex but not ex.Verb),
			// so an empty one panics for that shape through BOTH builders
			// alike. Pre-existing and upstream, tracked as snowplow#232 and
			// dormant there (057 has no AWS-auth endpoint). Noted here so the
			// next reader does not mistake it for a divergence introduced by
			// this fix.
			ri := &httpcall.RequestInfo{Verb: ptr.To(http.MethodGet)}
			ownedCli, ownedErr := httpClientForEndpoint(tc.ep(srv.URL), ri)
			delegateCli, delegateErr := httpcall.HTTPClientForEndpoint(tc.ep(srv.URL), ri)

			// (a) construction-outcome parity.
			if (ownedErr != nil) != (delegateErr != nil) {
				t.Fatalf("ARM10 FAIL [%s]: construction outcome DIVERGED — owned err=%v, "+
					"delegate err=%v. A non-owned shape must reach plumbing untouched, so the "+
					"two must fail and succeed together", tc.name, ownedErr, delegateErr)
			}
			if ownedErr != nil {
				if ownedErr.Error() != delegateErr.Error() {
					t.Fatalf("ARM10 FAIL [%s]: both failed but with DIFFERENT errors — the owned "+
						"path is not delegating verbatim.\n   owned: %v\ndelegate: %v",
						tc.name, ownedErr, delegateErr)
				}
				return
			}

			// (b) wire parity.
			ownedStatus, ownedBody := a1DriveWith(t, ownedCli, srv.URL, "owned")
			delegateStatus, delegateBody := a1DriveWith(t, delegateCli, srv.URL, "delegate")

			if ownedStatus != delegateStatus {
				t.Fatalf("ARM10 FAIL [%s]: status diverged — owned %d, delegate %d",
					tc.name, ownedStatus, delegateStatus)
			}
			if ownedBody != delegateBody {
				t.Fatalf("ARM10 FAIL [%s]: body diverged — owned %q, delegate %q",
					tc.name, ownedBody, delegateBody)
			}
			if ownedAuth != delegateAuth {
				t.Fatalf("ARM10 FAIL [%s]: the CREDENTIAL on the wire diverged — owned %q, "+
					"delegate %q. A non-owned shape must present exactly what plumbing presents "+
					"today; changing it changes the identity of every endpoint of this shape",
					tc.name, ownedAuth, delegateAuth)
			}
		})
	}
}

// a1DriveWith issues the same request through cli and returns status + body.
func a1DriveWith(t *testing.T, cli *http.Client, url, caller string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("build request (%s): %v", caller, err)
	}
	req.Header.Set("X-A1-Parity-Caller", caller)
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("drive (%s): %v", caller, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body (%s): %v", caller, err)
	}
	return resp.StatusCode, string(body)
}
