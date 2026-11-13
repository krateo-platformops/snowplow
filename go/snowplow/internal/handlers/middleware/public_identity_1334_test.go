package middleware_test

// public_identity_1334_test.go — #1334, the auth middleware's half of the
// "Must never be possible" list, driven through the REAL UserConfigOrPublic with
// real RS256 tokens:
//
//   - MN1: a token that is invalid, expired, signed by a retired key, from an
//     unknown issuer, or whose session credential is gone (revoked) NEVER falls
//     back to the public identity: it gets the SAME 401 as with the public
//     identity off, and nothing is dispatched.
//   - MN2: a token-less request cannot name any other identity — not through
//     Impersonate-* or X-Remote-* headers, a session cookie carrying a VALID
//     token, query parameters or extras. It is the public identity, exactly,
//     and no clientconfig Secret is read for it.
//   - MN3 (the middleware's part): the only credential the context carries for
//     the public identity is the impersonating config for exactly user
//     krateo:public + group system:unauthenticated; its clientconfig endpoint
//     holds nothing to dial with. When snowplow may not impersonate it, the
//     request is 503 and nothing runs — never the ServiceAccount.
//   - Opt-in: with no public identity the token-less request is the 401
//     TokenMissing it always was, byte for byte.

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"
	"k8s.io/client-go/rest"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

const i1334SAToken = "sa-token-1334"

// i1334Grant installs the impersonation capability the chart grants with
// publicIdentity.enabled: users [krateo:public] and groups
// [system:unauthenticated], by name. granted=false installs a reviewer that
// refuses every principal (the grant is missing).
func i1334Grant(t *testing.T, granted bool) {
	t.Helper()
	cache.ResetImpersonationCapabilityForTest()
	t.Cleanup(cache.ResetImpersonationCapabilityForTest)
	t.Cleanup(cache.SetImpersonationAvailableForTest(true))
	cache.SetImpersonationReviewer(func(_ context.Context, resource, _, name string) (bool, error) {
		if !granted {
			return false, nil
		}
		return (resource == cache.ImpersonateResourceUsers && name == middleware.PublicUsername) ||
			(resource == cache.ImpersonateResourceGroups && name == middleware.PublicGroup), nil
	})
}

// i1334Public is the public identity over a stand-in ServiceAccount.
func i1334Public() *middleware.PublicIdentity {
	return middleware.NewPublicIdentity(
		func() (*endpoints.Endpoint, error) {
			return &endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: i1334SAToken}, nil
		},
		func() (*rest.Config, error) {
			return &rest.Config{Host: "https://10.96.0.1:443", BearerToken: i1334SAToken}, nil
		})
}

// i1334SecretReads counts the clientconfig re-reads the middleware sends (the
// cache is off, so every authenticated request re-reads its Secret), answering
// NotFound for every one.
func i1334SecretReads(t *testing.T) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404,"message":"secrets not found"}`))
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(credhttp.SetInClusterConfigForTest(func() (*rest.Config, error) {
		return &rest.Config{Host: srv.URL, BearerToken: "tok-sa"}, nil
	}))
	return &n
}

type i1334Seen struct {
	reached bool
	user    jwtutil.UserInfo
	userErr error
	ep      endpoints.Endpoint
	token   string
	ctx     context.Context
}

func i1334Serve(mw func(http.Handler) http.Handler, req *http.Request) (*httptest.ResponseRecorder, *i1334Seen) {
	seen := &i1334Seen{}
	h := use.NewChain(mw).Then(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.reached = true
		seen.ctx = r.Context()
		seen.user, seen.userErr = xcontext.UserInfo(r.Context())
		seen.ep, _ = xcontext.UserConfig(r.Context())
		seen.token, _ = xcontext.AccessToken(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, seen
}

func i1334ForeignToken(t *testing.T, kid string, dur time.Duration) string {
	t.Helper()
	other, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: "mallory", Groups: []string{"system:masters"}, Duration: dur, KeyID: kid, PrivateKey: other,
	})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

// MN1 — every presented-but-bad credential keeps its 401 (byte-identical to the
// public identity being off), never reaches the handler, never counts as a
// public serve.
func TestIssue1334_BadCredentialNeverFallsBackToPublic(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	i1334Grant(t, true)
	secretReads := i1334SecretReads(t)
	off := middleware.UserConfig(testKeys, testAuthnNS)
	on := middleware.UserConfigOrPublic(testKeys, testAuthnNS, i1334Public())

	withAuthz := func(v string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
		r.Header["Authorization"] = []string{v}
		return r
	}
	cases := []struct {
		name  string
		authz string
	}{
		{"empty Authorization value", ""},
		{"Basic scheme", "Basic dXNlcjpwYXNz"},
		{"no space", "BearerToken123"},
		{"empty bearer", "Bearer "},
		{"whitespace bearer", "Bearer  "},
		{"garbage bearer", "Bearer not.a.real.jwt"},
		{"unknown issuer (foreign key, foreign kid)", "Bearer " + i1334ForeignToken(t, "kid-of-another-issuer", time.Hour)},
		{"forged (foreign key, our kid)", "Bearer " + i1334ForeignToken(t, testKeyID, time.Hour)},
		{"expired", "Bearer " + helperMakeToken(t, testUsername, -time.Hour)},
		{"expired and forged", "Bearer " + i1334ForeignToken(t, testKeyID, -time.Hour)},
		// A VALID signature whose session credential Secret is gone: the
		// revoked session. The re-read answers NotFound: 401.
		{"revoked session (clientconfig gone)", "Bearer " + helperMakeToken(t, testUsername, time.Hour)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := middleware.PublicIdentityOutcome("served")
			want, _ := i1334Serve(off, withAuthz(tc.authz))
			got, seen := i1334Serve(on, withAuthz(tc.authz))
			if want.Code != http.StatusUnauthorized {
				t.Fatalf("precondition: with the public identity off this credential is refused 401; got %d", want.Code)
			}
			if seen.reached || got.Code != http.StatusUnauthorized {
				t.Fatalf("MUST NEVER: a bad credential (%s) reached the handler or was not 401 with the public identity on; code=%d reached=%v user=%+v",
					tc.name, got.Code, seen.reached, seen.user)
			}
			if got.Body.String() != want.Body.String() ||
				!slices.Equal(got.Result().Header.Values("WWW-Authenticate"), want.Result().Header.Values("WWW-Authenticate")) {
				t.Fatalf("the public identity changed the 401 for %s:\n on  %s\n off %s", tc.name, got.Body.String(), want.Body.String())
			}
			if middleware.PublicIdentityOutcome("served") != before {
				t.Fatalf("a bad credential (%s) was counted as a public serve", tc.name)
			}
		})
	}
	// The retired-key class (KeyNotPublished) needs a JWKS source.
	t.Run("retired signing key", func(t *testing.T) {
		retired, current := knpKey(t), knpKey(t)
		authn := &knpJWKS{}
		authn.publish(map[string]*rsa.PublicKey{"key-2": &current.PublicKey})
		keys := jwtutil.NewJWKSKeySource(authn.serve(t))
		tok := knpMint(t, retired, "key-1", knpIssuedBefore())
		r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
		r.Header.Set("Authorization", "Bearer "+tok)
		got, seen := i1334Serve(middleware.UserConfigOrPublic(keys, testAuthnNS, i1334Public()), r)
		if seen.reached || got.Code != http.StatusUnauthorized || knpCause(got.Body.Bytes()) != "KeyNotPublished" {
			t.Fatalf("MUST NEVER: a token signed by a retired key was not refused 401 KeyNotPublished; code=%d reached=%v body=%s",
				got.Code, seen.reached, got.Body.String())
		}
	})
	if secretReads.Load() == 0 {
		t.Fatal("instrument: the revoked-session case never re-read its Secret, so it did not test what it names")
	}
}

// MN2 — a token-less request is the public identity, whatever it carries.
func TestIssue1334_TokenlessRequestCannotNameAnotherIdentity(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("REFRESH_SESSION_COOKIE", "krateo-session")
	i1334Grant(t, true)
	secretReads := i1334SecretReads(t)
	on := middleware.UserConfigOrPublic(testKeys, testAuthnNS, i1334Public())
	valid := helperMakeToken(t, testUsername, time.Hour)

	for _, tc := range []struct {
		name string
		mk   func() *http.Request
	}{
		{"nothing", func() *http.Request { return httptest.NewRequest(http.MethodGet, "/call?x=1", nil) }},
		{"impersonation headers", func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
			r.Header.Set("Impersonate-User", testUsername)
			r.Header.Add("Impersonate-Group", "system:masters")
			r.Header.Add("Impersonate-Group", "system:authenticated")
			r.Header.Set("Impersonate-Uid", "uid-1")
			r.Header.Set("Impersonate-Extra-Scopes", "admin")
			return r
		}},
		{"proxy identity headers", func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
			r.Header.Set("X-Remote-User", testUsername)
			r.Header.Set("X-Remote-Group", "system:masters")
			r.Header.Set("X-Forwarded-User", testUsername)
			r.Header.Set("X-Auth-Request-User", testUsername)
			return r
		}},
		{"session cookie with a VALID token", func() *http.Request {
			r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
			r.AddCookie(&http.Cookie{Name: "krateo-session", Value: valid})
			r.AddCookie(&http.Cookie{Name: "access_token", Value: valid})
			return r
		}},
		{"query parameters and extras", func() *http.Request {
			return httptest.NewRequest(http.MethodGet,
				`/call?x=1&username=alice&user=alice&as=alice&access_token=`+valid+
					`&extras=%7B%22username%22%3A%22alice%22%2C%22groups%22%3A%5B%22system%3Amasters%22%5D%7D`, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec, seen := i1334Serve(on, tc.mk())
			if !seen.reached || rec.Code != http.StatusOK {
				t.Fatalf("a token-less request with the public identity on must reach the handler; code=%d body=%s", rec.Code, rec.Body.String())
			}
			if seen.userErr != nil || seen.user.Username != "krateo:public" ||
				!slices.Equal(seen.user.Groups, []string{"system:unauthenticated"}) ||
				seen.user.Act != nil || seen.user.Principal != "" {
				t.Fatalf("MUST NEVER: a token-less request named another identity: got %+v (err=%v); want exactly krateo:public [system:unauthenticated]",
					seen.user, seen.userErr)
			}
			if seen.token != "" {
				t.Fatalf("MUST NEVER: a token-less request carries an access token %q downstream", seen.token)
			}
		})
	}
	if n := secretReads.Load(); n != 0 {
		t.Fatalf("MUST NEVER: %d clientconfig Secret read(s) for the public identity; credsecret.For(krateo:public) names a real user's Secret (krateo-public-clientconfig)", n)
	}
}

// MN3 (middleware) — the public identity's only credential is the impersonating
// config for exactly that identity; its clientconfig endpoint has nothing to
// dial with.
func TestIssue1334_PublicIdentityCarriesOnlyTheImpersonatingCredential(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	i1334Grant(t, true)
	_, seen := i1334Serve(middleware.UserConfigOrPublic(testKeys, testAuthnNS, i1334Public()),
		httptest.NewRequest(http.MethodGet, "/call?x=1", nil))
	if !seen.reached {
		t.Fatal("precondition: the token-less request reaches the handler")
	}
	// plumbing's xcontext.UserConfig getter rewrites ServerURL outside test
	// mode, so only the credential fields are pinned: none may be set.
	if cred := redactEP(seen.ep); slices.Contains([]bool{cred["token"], cred["cert"], cred["key"], cred["password"], cred["user"], cred["aws"]}, true) {
		t.Fatalf("MUST NEVER: the public identity's clientconfig endpoint carries a credential: %v", cred)
	}
	rc, ok := cache.ImpersonatingConfigFor(seen.ctx)
	if !ok {
		t.Fatal("MUST NEVER: the public identity's context carries no impersonating config; a dial would fall back")
	}
	if rc.Impersonate.UserName != "krateo:public" || !slices.Equal(rc.Impersonate.Groups, []string{"system:unauthenticated"}) ||
		rc.Impersonate.UID != "" || len(rc.Impersonate.Extra) != 0 {
		t.Fatalf("MUST NEVER: the public identity is impersonated as %+v; want exactly krateo:public [system:unauthenticated]", rc.Impersonate)
	}
	got, err := cache.ClientConfigFor(seen.ctx, seen.ep)
	if err != nil || got != rc {
		t.Fatalf("every client-go read of the public identity must use the impersonating config; ClientConfigFor = %p (%v), want %p", got, err, rc)
	}
	if !cache.ImpersonatedDialFromContext(seen.ctx) {
		t.Fatal("the public identity's context must carry the impersonated-dial marker (branch E impersonates or refuses)")
	}
	if cache.RequesterCredentialFor(seen.ctx) {
		t.Fatal("the public identity has no clientconfig credential; RequesterCredentialFor must say so")
	}
}

// MN3 (middleware) — with the impersonation not granted, a token-less request
// is 503 and never runs; nothing falls back to the ServiceAccount.
func TestIssue1334_PublicIdentityUnavailableIs503NeverTheServiceAccount(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T)
		pub   *middleware.PublicIdentity
	}{
		{"impersonation not granted", func(t *testing.T) { i1334Grant(t, false) }, i1334Public()},
		{"impersonation unavailable at startup", func(t *testing.T) {
			cache.ResetImpersonationCapabilityForTest()
			t.Cleanup(cache.ResetImpersonationCapabilityForTest)
			t.Cleanup(cache.SetImpersonationAvailableForTest(false))
		}, i1334Public()},
		{"no ServiceAccount credential", func(t *testing.T) { i1334Grant(t, true) }, middleware.NewPublicIdentity(
			func() (*endpoints.Endpoint, error) { return nil, context.Canceled },
			func() (*rest.Config, error) { return nil, context.Canceled })},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.setup(t)
			before := middleware.PublicIdentityOutcome("unavailable")
			rec, seen := i1334Serve(middleware.UserConfigOrPublic(testKeys, testAuthnNS, tc.pub),
				httptest.NewRequest(http.MethodGet, "/call?x=1", nil))
			if seen.reached || rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("MUST NEVER: the public identity ran without its impersonation; code=%d reached=%v user=%+v", rec.Code, seen.reached, seen.user)
			}
			if middleware.PublicIdentityOutcome("unavailable") != before+1 {
				t.Fatal("the refusal was not counted as unavailable")
			}
		})
	}
}

// Opt-in — off, a token-less request is UserConfig's 401 TokenMissing, byte for
// byte, through either constructor.
func TestIssue1334_PublicIdentityOff_TokenlessIs401(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	i1334Grant(t, true)
	want, _ := i1334Serve(middleware.UserConfig(testKeys, testAuthnNS), httptest.NewRequest(http.MethodGet, "/call?x=1", nil))
	got, seen := i1334Serve(middleware.UserConfigOrPublic(testKeys, testAuthnNS, nil), httptest.NewRequest(http.MethodGet, "/call?x=1", nil))
	if want.Code != http.StatusUnauthorized || knpCause(want.Body.Bytes()) != "TokenMissing" {
		t.Fatalf("precondition: UserConfig answers a token-less request 401 TokenMissing; got %d %s", want.Code, want.Body.String())
	}
	if seen.reached || got.Code != want.Code || got.Body.String() != want.Body.String() {
		t.Fatalf("public identity off: a token-less request must be UserConfig's 401; got %d reached=%v %s", got.Code, seen.reached, got.Body.String())
	}
	// A valid token is untouched by the public identity being on.
	r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
	r.Header.Set("Authorization", "Bearer "+helperMakeToken(t, testUsername, time.Hour))
	_, s := i1334Serve(middleware.UserConfigOrPublic(testKeys, testAuthnNS, i1334Public()), r)
	if s.reached && s.user.Username == "krateo:public" {
		t.Fatal("a request with a valid token was served as the public identity")
	}
}

func redactEP(ep endpoints.Endpoint) map[string]bool {
	return map[string]bool{
		"server": ep.ServerURL != "", "token": ep.Token != "", "cert": ep.ClientCertificateData != "",
		"key": ep.ClientKeyData != "", "password": ep.Password != "", "user": ep.Username != "",
		"aws": ep.AwsAccessKey != "" || ep.AwsSecretKey != "",
	}
}
