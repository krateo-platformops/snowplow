// predispatch_401_1076_test.go — #1076: every 401 snowplow writes BEFORE
// dispatch carries `details.causes[0].reason` from one closed set, plus a
// WWW-Authenticate challenge; plumbing's six Status fields stay byte-identical.
//
// Driven through the REAL middleware.UserConfig and middleware.RefreshAuth with
// real RS256 JWTs. The wire contract is asserted as LITERALS (reasons,
// sentences, challenge), never through the package constants, so renaming or
// swapping a constant is caught. The six plumbing fields are compared against
// what UPSTREAM use.UserConfig writes for the same request, byte for byte.
//
// The relayed-401 half of the contract (a stage or passthrough 401 never
// carries a cause, even a forged one) lives where the relays are:
// dispatchers/predispatch_401_relay_1076_test.go and
// handlers/call_401_relay_1076_test.go.
package middleware_test

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"

	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

// p1076Challenge is the challenge each token cause sends, as literals: a bare
// `Bearer` when no token was presented (RFC 6750 §3.1: no error code), the
// invalid_token challenge for a token that was presented and refused.
var p1076Challenge = map[string]string{
	"TokenMissing": `Bearer`,
	"TokenInvalid": `Bearer error="invalid_token"`,
	"TokenExpired": `Bearer error="invalid_token"`,
}

// p1076Sentence is the fixed human sentence each token cause carries, as
// literals.
var p1076Sentence = map[string]string{
	"TokenMissing": "no bearer token was presented; log in",
	"TokenInvalid": "the bearer token is not valid; log in again",
	"TokenExpired": "the bearer token has expired; log in again",
}

// p1076Want is the exact wire body for a pre-dispatch 401: plumbing's body for
// the same Status, with details appended after its six fields.
func p1076Want(plumbingBody, reason string) string {
	return strings.TrimSuffix(plumbingBody, "}\n") +
		`,"details":{"causes":[{"reason":"` + reason + `","message":"` + p1076Sentence[reason] + `"}]}}` + "\n"
}

// p1076Serve drives req through mw and reports whether the next handler ran.
func p1076Serve(mw func(http.Handler) http.Handler, req *http.Request) (*httptest.ResponseRecorder, bool) {
	reached := false
	h := use.NewChain(mw).Then(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, reached
}

// p1076AssertCause: a 401 that did not reach the handler, carrying exactly the
// cause, its challenge, plumbing's Content-Type and no other header, and a
// body that a plumbing response.Status reader decodes to the same six fields.
// Headers are read from rec.Result(), i.e. what was WRITTEN with the status
// line: a header set after WriteHeader never reaches a real client, and the
// recorder's live Header() map would still show it.
func p1076AssertCause(t *testing.T, rec *httptest.ResponseRecorder, reached bool, plumbingBody, reason string) {
	t.Helper()
	if reached || rec.Code != http.StatusUnauthorized {
		t.Fatalf("want a 401 that never reaches the handler; code=%d reached=%v", rec.Code, reached)
	}
	if got, want := rec.Body.String(), p1076Want(plumbingBody, reason); got != want {
		t.Fatalf("body:\n got  %q\n want %q", got, want)
	}
	if got := rec.Result().Header.Values("WWW-Authenticate"); len(got) != 1 || got[0] != p1076Challenge[reason] {
		t.Fatalf("WWW-Authenticate (as written) = %q, want exactly [%q]", got, p1076Challenge[reason])
	}
	if rec.Result().Header.Get("Content-Type") != "application/json" || len(rec.Result().Header) != 2 {
		t.Fatalf("headers = %v, want Content-Type + WWW-Authenticate only", rec.Result().Header)
	}
	var asPlumbing, plumbing response.Status
	if err := json.Unmarshal(rec.Body.Bytes(), &asPlumbing); err != nil {
		t.Fatalf("a plumbing Status reader cannot decode the body: %v", err)
	}
	_ = json.Unmarshal([]byte(plumbingBody), &plumbing)
	if asPlumbing != plumbing {
		t.Fatalf("a plumbing Status reader sees %+v, want %+v", asPlumbing, plumbing)
	}
}

func p1076OtherKeyToken(t *testing.T, kid string, dur time.Duration) string {
	t.Helper()
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: "bob", Groups: []string{"krateo:users"}, Duration: dur, KeyID: kid, PrivateKey: other,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// UserConfig — each token branch carries its cause and the challenge, and its
// first six Status fields are exactly what upstream use.UserConfig writes for
// the same request.
func TestPreDispatch401_1076_UserConfigTokenCauses(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	upstream := use.UserConfig(testKeys, testAuthnNS)
	snowplow := middleware.UserConfig(testKeys, testAuthnNS)

	withAuthz := func(v string) func(*testing.T) *http.Request {
		return func(*testing.T) *http.Request {
			r := httptest.NewRequest(http.MethodPost, "/call?x=1", nil)
			if v != "" {
				r.Header.Set("Authorization", v)
			}
			return r
		}
	}
	for _, tc := range []struct {
		name   string
		mk     func(*testing.T) *http.Request
		reason string
	}{
		{"no Authorization header", withAuthz(""), "TokenMissing"},
		{"Basic scheme", withAuthz("Basic dXNlcjpwYXNz"), "TokenMissing"},
		{"no space", withAuthz("BearerToken123"), "TokenMissing"},
		{"garbage bearer", withAuthz("Bearer not.a.real.jwt"), "TokenInvalid"},
		// An empty bearer presents no token: TokenMissing, as on RefreshAuth
		// (the message stays upstream's, which the comparison pins).
		{"empty bearer", withAuthz("Bearer "), "TokenMissing"},
		{"whitespace bearer", withAuthz("Bearer  "), "TokenInvalid"},
		{"wrong signing key", func(t *testing.T) *http.Request {
			return withAuthz("Bearer " + p1076OtherKeyToken(t, testKeyID, time.Hour))(t)
		}, "TokenInvalid"},
		// An expired token signed by the WRONG key is a bad signature, never
		// TokenExpired: an expiry is only believed once the signature holds.
		{"expired and wrong signing key", func(t *testing.T) *http.Request {
			return withAuthz("Bearer " + p1076OtherKeyToken(t, testKeyID, -time.Hour))(t)
		}, "TokenInvalid"},
		{"expired", func(t *testing.T) *http.Request {
			return withAuthz("Bearer " + helperMakeToken(t, testUsername, -time.Hour))(t)
		}, "TokenExpired"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := tc.mk(t)
			up := httptest.NewRecorder()
			upstream(http.NotFoundHandler()).ServeHTTP(up, req.Clone(req.Context()))
			if up.Code != http.StatusUnauthorized || up.Header().Get("WWW-Authenticate") != "" {
				t.Fatalf("precondition: upstream answers a plain 401; code=%d", up.Code)
			}
			rec, reached := p1076Serve(snowplow, req)
			p1076AssertCause(t, rec, reached, up.Body.String(), tc.reason)
		})
	}
}

// RefreshAuth (/refreshes, the RUM ingest and the /debug surface) — the same
// contract on its header-or-cookie transport.
func TestPreDispatch401_1076_RefreshAuthTokenCauses(t *testing.T) {
	t.Setenv("REFRESH_SESSION_COOKIE", "krateo-session")
	mw := middleware.RefreshAuth(refreshAuthTestKeys)
	req := func(authz, cookie string) func(*testing.T) *http.Request {
		return func(*testing.T) *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
			if authz != "" {
				r.Header.Set("Authorization", authz)
			}
			if cookie != "" {
				r.AddCookie(&http.Cookie{Name: "krateo-session", Value: cookie})
			}
			return r
		}
	}
	tok := func(d time.Duration) func(*testing.T) string {
		return func(t *testing.T) string { return mintRefreshToken(t, "userA", d) }
	}
	for _, tc := range []struct {
		name    string
		mk      func(*testing.T) *http.Request
		message string
		reason  string
	}{
		{"no header, no cookie", req("", ""), "missing credentials: no bearer header or session cookie", "TokenMissing"},
		{"Basic header, no cookie", req("Basic dXNlcjpwYXNz", ""), "missing credentials: no bearer header or session cookie", "TokenMissing"},
		{"empty bearer, no cookie", req("Bearer ", ""), "missing credentials: no bearer header or session cookie", "TokenMissing"},
		{"garbage bearer", req("Bearer not.a.real.jwt", ""), "token is invalid", "TokenInvalid"},
		{"garbage cookie", req("", "not.a.real.jwt"), "token is invalid", "TokenInvalid"},
		{"wrong signing key", func(t *testing.T) *http.Request {
			return req("Bearer "+p1076OtherKeyToken(t, refreshAuthTestKeyID, time.Hour), "")(t)
		}, "token is invalid", "TokenInvalid"},
		{"expired bearer", func(t *testing.T) *http.Request { return req("Bearer "+tok(-time.Hour)(t), "")(t) }, "token is expired", "TokenExpired"},
		{"expired cookie", func(t *testing.T) *http.Request { return req("", tok(-time.Hour)(t))(t) }, "token is expired", "TokenExpired"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plumbing := httptest.NewRecorder()
			_ = response.Unauthorized(plumbing, errors.New(tc.message))
			rec, reached := p1076Serve(mw, tc.mk(t))
			p1076AssertCause(t, rec, reached, plumbing.Body.String(), tc.reason)
		})
	}
}

// The token causes never leak onto a request that passes: a valid token
// reaches the handler with no challenge.
func TestPreDispatch401_1076_ValidTokenCarriesNoChallenge(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
	r.Header.Set("Authorization", "Bearer "+mintRefreshToken(t, "userA", time.Hour))
	rec, reached := p1076Serve(middleware.RefreshAuth(refreshAuthTestKeys), r)
	if !reached || rec.Code != http.StatusOK || rec.Header().Get("WWW-Authenticate") != "" {
		t.Fatalf("valid token: reached=%v code=%d hdr=%q", reached, rec.Code, rec.Header().Get("WWW-Authenticate"))
	}
}

// An unresolvable signing key is OUR fault: it stays plumbing's 503, byte for
// byte, with no cause and no challenge — on both middlewares, for a key source
// that fails outright and for the REAL JWKS source with an unknown kid (#1076
// leaves that 503 alone until krateo-platformops/plumbing#63).
func TestPreDispatch401_1076_KeyUnavailableIs503WithNoCause(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	pub := &refreshAuthTestPrivateKey.PublicKey
	jwks := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{
			"kty": "RSA", "use": "sig", "alg": "RS256", "kid": "known",
			"n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
			"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
		}}})
	}))
	defer jwks.Close()
	unknownKid, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: "userA", Groups: []string{"devs"}, Duration: time.Hour, KeyID: "unknown", PrivateKey: refreshAuthTestPrivateKey,
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, src := range []struct {
		name string
		keys func() jwtutil.KeySource
		tok  string
	}{
		{"failing source", func() jwtutil.KeySource { return failingKeySource{} }, mintRefreshToken(t, "userA", time.Hour)},
		{"JWKS, unknown kid", func() jwtutil.KeySource { return jwtutil.NewJWKSKeySource(jwks.URL) }, unknownKid},
	} {
		for _, mw := range []struct {
			name string
			mk   func(jwtutil.KeySource) func(http.Handler) http.Handler
		}{
			{"UserConfig", func(k jwtutil.KeySource) func(http.Handler) http.Handler {
				return middleware.UserConfig(k, testAuthnNS)
			}},
			{"RefreshAuth", middleware.RefreshAuth},
		} {
			t.Run(src.name+"/"+mw.name, func(t *testing.T) {
				r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
				r.Header.Set("Authorization", "Bearer "+src.tok)
				rec, reached := p1076Serve(mw.mk(src.keys()), r)
				if reached || rec.Code != http.StatusServiceUnavailable {
					t.Fatalf("want 503; code=%d reached=%v body=%s", rec.Code, reached, rec.Body.String())
				}
				var st response.Status
				_ = json.Unmarshal(rec.Body.Bytes(), &st)
				plumbing := httptest.NewRecorder()
				_ = response.ServiceUnavailable(plumbing, errors.New(st.Message))
				if rec.Body.String() != plumbing.Body.String() || strings.Contains(rec.Body.String(), "causes") {
					t.Fatalf("the 503 must stay plumbing's, byte for byte:\n got  %s\n want %s", rec.Body.String(), plumbing.Body.String())
				}
				if got := rec.Header().Values("WWW-Authenticate"); len(got) != 0 {
					t.Fatalf("a 503 sends no challenge; got %q", got)
				}
			})
		}
	}
}
