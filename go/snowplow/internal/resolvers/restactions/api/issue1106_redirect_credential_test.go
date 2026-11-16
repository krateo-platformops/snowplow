package api

// issue1106_redirect_credential_test.go — #1106: a named endpointRef's
// credential never reaches a host other than the endpoint's own.
//
//   A  GET, PUT and POST × 301/302/303/307/308 to another host: 0 requests and
//      0 credentials reach it; the endpoint got the one request; the stage
//      fails with #729's 502 naming the redirect; the token is in no output.
//   B  a same-host redirect is still followed, with the credential and (307,
//      308) the full body.
//   C  every credential kind: bearer, basic (client-go and plumbing branches),
//      client certificate, AWS signature — 0 requests at the other host.
//   D  a credential in a stage header (X-Api-Key, X-Auth-Token, PRIVATE-TOKEN)
//      on an endpoint with no credential of its own: 0 requests elsewhere,
//      another host or another port of the same hostname.
//   F  the refusal holds with the wire dump on (debug Secret on both builder
//      branches, the verbose wire dump).
// The transport backstop and the 10-hop bound are pinned in internal/credhttp
// (endpoint_redirect_1106_test.go).

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	httpcall "github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
)

var x1106RedirectCodes = []int{
	http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
	http.StatusTemporaryRedirect, http.StatusPermanentRedirect,
}

func TestIssue1106_A_CrossHostRedirectNeverReachesTheOtherHost(t *testing.T) {
	for _, verb := range []string{http.MethodGet, http.MethodPut, http.MethodPost} {
		for _, code := range x1106RedirectCodes {
			name := verb + "_" + strconv.Itoa(code)
			t.Run(name, func(t *testing.T) {
				other := newX1106Srv(t, false, x1106OK)
				ep := newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, x1106OtherURL(other.srv)+"/stolen", code)
				})
				x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL)})
				o := x1106Resolve(t, x1106Extras, x1106Write("w", verb, `${ "/zd/tickets/" + (.ticketId|tostring) }`))

				if hits := other.all(); len(hits) != 0 {
					t.Fatalf("RED: %d request(s) reached the other host; first %s with the endpoint token=%v, body bytes=%d",
						len(hits), hits[0].Method, hits[0].Auth == "Bearer "+x1106Token, len(hits[0].Body))
				}
				if hits := ep.all(); len(hits) != 1 || hits[0].Auth != "Bearer "+x1106Token {
					t.Fatalf("PRE: the endpoint must get the one request with its own token, got %d", len(hits))
				}
				x1106WantReason(t, o.outcomes, "w", StageReasonError)
				if o.failure == nil || o.failure.Code != http.StatusBadGateway ||
					!strings.Contains(o.failure.Message, strconv.Itoa(code)+" "+http.StatusText(code)+", a redirect snowplow does not follow") {
					t.Fatalf("the refusal must be the stage's 502 naming the %d redirect (#729), got %+v", code, o.failure)
				}
				x1106NoSecretIn(t, name, o, x1106Token)
			})
		}
	}
}

func TestIssue1106_B_SameHostRedirectIsFollowed(t *testing.T) {
	for _, tc := range []struct {
		verb     string
		code     int
		wantVerb string
		wantBody bool
	}{
		{http.MethodGet, http.StatusFound, http.MethodGet, false},
		{http.MethodPut, http.StatusTemporaryRedirect, http.MethodPut, true},
		{http.MethodPost, http.StatusPermanentRedirect, http.MethodPost, true},
		{http.MethodPost, http.StatusSeeOther, http.MethodGet, false},
	} {
		t.Run(tc.verb+"_"+strconv.Itoa(tc.code), func(t *testing.T) {
			ep := newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasPrefix(r.URL.Path, "/moved/") {
					x1106OK(w, r)
					return
				}
				http.Redirect(w, r, "/moved"+r.URL.Path, tc.code)
			})
			x1106Env(t, "", map[string]map[string]string{x1106Ref: x1106Secret(ep.srv.URL)})
			o := x1106Resolve(t, x1106Extras, x1106Write("w", tc.verb, `${ "/zd/tickets/" + (.ticketId|tostring) }`))
			hits := ep.all()
			if len(hits) != 2 {
				t.Fatalf("RED: a same-host redirect must be followed: %d request(s), want 2 (outcomes %s)", len(hits), o.outcomes)
			}
			h := hits[1]
			if h.Path != "/moved/zd/tickets/42" || h.Method != tc.wantVerb || h.Auth != "Bearer "+x1106Token {
				t.Fatalf("the followed hop: %s %s token=%v, want %s /moved/zd/tickets/42 with the token", h.Method, h.Path, h.Auth == "Bearer "+x1106Token, tc.wantVerb)
			}
			if tc.wantBody != strings.Contains(h.Body, x1106Body) {
				t.Fatalf("the followed hop's body carried the payload=%v, want %v", strings.Contains(h.Body, x1106Body), tc.wantBody)
			}
			if o.outcomes != "[]" {
				t.Fatalf("a followed same-host redirect is a success: header %s", o.outcomes)
			}
		})
	}
}

// x1106ClientCert is a self-signed client certificate and key, PEM.
func x1106ClientCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1106), Subject: pkix.Name{CommonName: "x1106-client"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &k.PublicKey, k)
	if err != nil {
		t.Fatal(err)
	}
	kd, err := x509.MarshalECPrivateKey(k)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kd})
}

// x1106TLSRequestingClientCert is a TLS host that asks for (and records) a
// client certificate.
func x1106TLSRequestingClientCert(t *testing.T, fn http.HandlerFunc) *x1106Srv {
	t.Helper()
	z := &x1106Srv{}
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit := x1106Hit{Method: r.Method, Path: r.URL.Path, Auth: r.Header.Get("Authorization")}
		if r.TLS != nil {
			hit.PeerCerts = len(r.TLS.PeerCertificates)
		}
		z.mu.Lock()
		z.hits = append(z.hits, hit)
		z.mu.Unlock()
		fn(w, r)
	}))
	s.TLS = &tls.Config{ClientAuth: tls.RequestClientCert}
	s.StartTLS()
	t.Cleanup(s.Close)
	z.srv = s
	return z
}

func TestIssue1106_C_EveryCredentialKindStaysOnItsHost(t *testing.T) {
	certPEM, keyPEM := x1106ClientCert(t)
	b64 := func(b []byte) string { return base64.StdEncoding.EncodeToString(b) }
	for _, tc := range []struct {
		name string
		ep   func(server, ca string) *endpoints.Endpoint
	}{
		{"bearer", func(s, _ string) *endpoints.Endpoint {
			return &endpoints.Endpoint{ServerURL: s, Token: x1106Token, Insecure: true}
		}},
		{"basic_clientgo", func(s, _ string) *endpoints.Endpoint {
			return &endpoints.Endpoint{ServerURL: s, Username: "svc", Password: x1106Token, Insecure: true}
		}},
		{"basic_plumbing_password_only", func(s, _ string) *endpoints.Endpoint {
			return &endpoints.Endpoint{ServerURL: s, Password: x1106Token, Insecure: true}
		}},
		{"client_certificate", func(s, ca string) *endpoints.Endpoint {
			return &endpoints.Endpoint{ServerURL: s, CertificateAuthorityData: b64([]byte(ca)),
				ClientCertificateData: b64(certPEM), ClientKeyData: b64(keyPEM)}
		}},
		{"aws_signature", func(s, _ string) *endpoints.Endpoint {
			return &endpoints.Endpoint{ServerURL: s, AwsAccessKey: "AKIDX1106", AwsSecretKey: x1106Token,
				AwsRegion: "eu-west-1", AwsService: "execute-api", Insecure: true}
		}},
	} {
		for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
			t.Run(tc.name+"_"+strconv.Itoa(code), func(t *testing.T) {
				other := x1106TLSRequestingClientCert(t, x1106OK)
				api := newTLSRedirector(t, code, x1106OtherURL(other.srv)+"/stolen")
				ep := tc.ep(api.URL, caOf(api))
				res, body, _, _ := httpFetchAllowingNonJSON(context.Background(), httpcall.RequestOptions{
					RequestInfo: httpcall.RequestInfo{Path: "/zd/tickets/42", Verb: ptr.To(http.MethodPut), Payload: ptr.To(x1106Body)},
					Endpoint:    ep,
				})
				if hits := other.all(); len(hits) != 0 {
					t.Fatalf("RED: %d request(s) reached the other host (authorization present=%v, client certs presented=%d)",
						len(hits), hits[0].Auth != "", hits[0].PeerCerts)
				}
				if res == nil || res.Status != response.StatusFailure || res.Code != code || body != nil {
					t.Fatalf("want the %d itself as a Failure and no body, got %+v body=%q", code, res, body)
				}
			})
		}
	}
}

// D — a credential snowplow cannot see: an endpoint Secret with no credential,
// and a stage header carrying one under a name net/http does not strip on a
// cross-host redirect. The hop is refused whatever the headers.
func TestIssue1106_D_StageHeaderCredentialNeverLeavesTheOrigin(t *testing.T) {
	// X-Zq7-Tenant-Cred is deliberately odd: nothing in the stack strips it by
	// name (plumbing's guard strips Authorization alone, #1135).
	for _, h := range []string{"X-Api-Key", "X-Auth-Token", "PRIVATE-TOKEN", "X-Zq7-Tenant-Cred"} {
		for _, target := range []string{"other_host", "same_hostname_other_port"} {
			t.Run(h+"_"+target, func(t *testing.T) {
				other := newX1106Srv(t, false, x1106OK)
				to := x1106OtherURL(other.srv)
				if target == "same_hostname_other_port" {
					to = other.srv.URL
				}
				ep := newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, to+"/stolen", http.StatusFound)
				})
				x1106Env(t, "", map[string]map[string]string{x1106Ref: {"server-url": ep.srv.URL}})
				s := x1106Stage("r", http.MethodGet, "/zd/tickets/42")
				s.Headers = []string{h + ": " + x1106Token}
				o := x1106Resolve(t, x1106Extras, s)
				if hits := ep.all(); len(hits) != 1 {
					t.Fatalf("PRE: the endpoint gets the one request, got %d", len(hits))
				}
				if hits := other.all(); len(hits) != 0 {
					t.Fatalf("RED: the %s header followed the redirect to %s (%d request(s))", h, target, len(hits))
				}
				x1106WantReason(t, o.outcomes, "r", StageReasonError)
			})
		}
	}
}

// F — the wire dump never displaces the redirect policy (the oasgen #379
// trap: a debug wrapper assigned back onto the client bypassed its checks).
// The refusal is applied by credhttp.Send, outermost on whatever client the
// builders and wrapEndpointClient composed, the dump included.
func TestIssue1106_F_DebugAndVerboseKeepTheRefusal(t *testing.T) {
	for _, tc := range []struct {
		name    string
		secret  func(url string) map[string]string
		verbose bool
	}{
		{"secret_debug_owned", func(u string) map[string]string { return x1106Secret(u, "debug", "true") }, false},
		{"secret_debug_plumbing_branch", func(u string) map[string]string {
			return map[string]string{"server-url": u, "password": x1106Token, "debug": "true"}
		}, false},
		{"verbose_wire_dump", func(u string) map[string]string { return x1106Secret(u) }, true},
	} {
		for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
			t.Run(tc.name+"_"+strconv.Itoa(code), func(t *testing.T) {
				if tc.verbose {
					t.Setenv(envVerboseWireDump, "true")
				}
				other := newX1106Srv(t, false, x1106OK)
				ep := newX1106Srv(t, false, func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, x1106OtherURL(other.srv)+"/stolen", code)
				})
				x1106Env(t, "", map[string]map[string]string{x1106Ref: tc.secret(ep.srv.URL)})
				o := x1106ResolveWith(t, x1106Opts{verbose: tc.verbose}, x1106Extras,
					x1106Write("w", http.MethodPut, `${ "/zd/tickets/" + (.ticketId|tostring) }`))
				if !strings.Contains(o.logs, `"msg":"request details"`) {
					t.Fatal("PRE: the wire dump must be on for this arm")
				}
				if hits := other.all(); len(hits) != 0 {
					t.Fatalf("RED: with the dump on, %d request(s) reached the other host (authorization present=%v)", len(hits), hits[0].Auth != "")
				}
				x1106WantReason(t, o.outcomes, "w", StageReasonError)
			})
		}
	}
}
