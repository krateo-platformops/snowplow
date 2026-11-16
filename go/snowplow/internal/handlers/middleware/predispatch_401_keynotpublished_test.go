// predispatch_401_keynotpublished_test.go — plumbing v1.36.0 (plumbing#63,
// #107): a token whose signing key authn no longer publishes is a pre-dispatch
// 401 with its OWN cause, KeyNotPublished, on both middlewares.
//
// jwtutil reports a retired key as ErrTokenInvalid AND ErrKeyNotPublished, and
// never as ErrKeyUnavailable. So the cause depends on tokenRejected testing
// ErrKeyNotPublished before its generic TokenInvalid branch: these tests fail
// with "TokenInvalid" if that branch is removed or placed after the default.
//
// Driven through the REAL middleware.UserConfig and middleware.RefreshAuth,
// against the REAL jwtutil.JWKSKeySource reading an httptest JWKS endpoint, with
// real RS256 tokens. The wire contract is asserted as literals (p1076Challenge,
// p1076Sentence), and UserConfig's six Status fields against upstream
// use.UserConfig for the same request.
//
// The neighbours that must NOT move are pinned here too: a kid the cached set
// may predate, a token without `iat`, and a JWKS whose latest fetch failed stay
// plumbing's 503 with no cause; a bad signature under a published kid stays
// TokenInvalid.
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
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"

	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

// knpJWKS is an authn whose published key set can be swapped, or made to fail,
// between requests.
type knpJWKS struct {
	mu   sync.Mutex
	keys map[string]*rsa.PublicKey
	down bool
}

func (a *knpJWKS) publish(keys map[string]*rsa.PublicKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.keys, a.down = keys, false
}

func (a *knpJWKS) fail() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.down = true
}

func (a *knpJWKS) serve(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		a.mu.Lock()
		defer a.mu.Unlock()
		if a.down {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		var keys []any
		for kid, pub := range a.keys {
			keys = append(keys, map[string]any{
				"kty": "RSA", "use": "sig", "alg": "RS256", "kid": kid,
				"n": base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
				"e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": keys})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func knpKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// knpMint signs a Krateo token as authn does (jwtutil.CreateToken's claims),
// except that the issue time is chosen: jwtutil tells a retired key from a new
// one by `iat`, and CreateToken always stamps "now". A nil iat leaves the claim
// out.
func knpMint(t *testing.T, key *rsa.PrivateKey, kid string, iat *time.Time) string {
	t.Helper()
	rc := jwt.RegisteredClaims{
		Issuer:    "krateo.io",
		Subject:   "userA",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}
	if iat != nil {
		rc.IssuedAt = jwt.NewNumericDate(*iat)
		rc.NotBefore = jwt.NewNumericDate(*iat)
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, jwtutil.KrateoClaims{
		UserInfo:         jwtutil.UserInfo{Username: "userA", Groups: []string{"devs"}},
		RegisteredClaims: rc,
	})
	tok.Header["kid"] = kid
	s, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// knpIssuedBefore is an issue time well before any key-set fetch in a test,
// beyond jwtutil's 5s leeway.
func knpIssuedBefore() *time.Time {
	at := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	return &at
}

// knpMiddlewares are the two gates that call jwtutil.ValidateWithKeySource.
var knpMiddlewares = []struct {
	name string
	mk   func(jwtutil.KeySource) func(http.Handler) http.Handler
}{
	{"UserConfig", func(k jwtutil.KeySource) func(http.Handler) http.Handler {
		return middleware.UserConfig(k, testAuthnNS)
	}},
	{"RefreshAuth", middleware.RefreshAuth},
}

func knpRequest(token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	return r
}

// knpCause is details.causes[0].reason of a snowplow body, or "".
func knpCause(body []byte) string {
	var st struct {
		Details *struct {
			Causes []struct {
				Reason string `json:"reason"`
			} `json:"causes"`
		} `json:"details"`
	}
	if json.Unmarshal(body, &st) != nil || st.Details == nil || len(st.Details.Causes) == 0 {
		return ""
	}
	return st.Details.Causes[0].Reason
}

// The wire contract: a retired key is a 401 KeyNotPublished with the
// invalid_token challenge; its six Status fields are exactly upstream
// use.UserConfig's (UserConfig) and plumbing's Unauthorized for the
// validator's error (RefreshAuth), whose message is that error's text.
func TestPreDispatch401_KeyNotPublished_RetiredKeyIsItsOwnCause(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("REFRESH_SESSION_COOKIE", "krateo-session")
	retired, current := knpKey(t), knpKey(t)
	authn := &knpJWKS{}
	authn.publish(map[string]*rsa.PublicKey{"key-2": &current.PublicKey})
	tok := knpMint(t, retired, "key-1", knpIssuedBefore())

	// One source for every request below: the first validation fetches the
	// set, and the refresh floor (30s) keeps every later request on that same
	// fetch, so the error text (which names the fetch time) is identical.
	keys := jwtutil.NewJWKSKeySource(authn.serve(t))
	_, verr := jwtutil.ValidateWithKeySource(keys, tok)
	if !errors.Is(verr, jwtutil.ErrKeyNotPublished) || !errors.Is(verr, jwtutil.ErrTokenInvalid) || errors.Is(verr, jwtutil.ErrKeyUnavailable) {
		t.Fatalf("precondition: a retired key is ErrKeyNotPublished AND ErrTokenInvalid, never ErrKeyUnavailable; got %v", verr)
	}

	t.Run("UserConfig", func(t *testing.T) {
		req := knpRequest(tok)
		up := httptest.NewRecorder()
		use.UserConfig(keys, testAuthnNS)(http.NotFoundHandler()).ServeHTTP(up, req.Clone(req.Context()))
		if up.Code != http.StatusUnauthorized {
			t.Fatalf("precondition: upstream answers 401 for a retired key; code=%d body=%s", up.Code, up.Body.String())
		}
		rec, reached := p1076Serve(middleware.UserConfig(keys, testAuthnNS), req)
		p1076AssertCause(t, rec, reached, up.Body.String(), "KeyNotPublished")
	})
	t.Run("RefreshAuth", func(t *testing.T) {
		plumbing := httptest.NewRecorder()
		_ = response.Unauthorized(plumbing, verr)
		rec, reached := p1076Serve(middleware.RefreshAuth(keys), knpRequest(tok))
		p1076AssertCause(t, rec, reached, plumbing.Body.String(), "KeyNotPublished")
	})
	t.Run("RefreshAuth/cookie", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
		r.AddCookie(&http.Cookie{Name: "krateo-session", Value: tok})
		plumbing := httptest.NewRecorder()
		_ = response.Unauthorized(plumbing, verr)
		rec, reached := p1076Serve(middleware.RefreshAuth(keys), r)
		p1076AssertCause(t, rec, reached, plumbing.Body.String(), "KeyNotPublished")
	})
}

// The revocation as it happens: a token that passes while authn publishes its
// key gets KeyNotPublished once authn stops publishing it and snowplow's set is
// refreshed.
func TestPreDispatch401_KeyNotPublished_RotationRevokesTheOldKey(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	for _, mw := range knpMiddlewares {
		t.Run(mw.name, func(t *testing.T) {
			oldKey, newKey := knpKey(t), knpKey(t)
			authn := &knpJWKS{}
			authn.publish(map[string]*rsa.PublicKey{"key-1": &oldKey.PublicKey})
			// A TTL and a refresh floor of 1ns: every request refetches, as one
			// past jwt.cacheTTL does in production.
			keys := jwtutil.NewJWKSKeySource(authn.serve(t),
				jwtutil.WithJWKSCacheTTL(time.Nanosecond), jwtutil.WithJWKSMinRefreshInterval(time.Nanosecond))
			tok := knpMint(t, oldKey, "key-1", knpIssuedBefore())

			// RefreshAuth reaches the handler. UserConfig gets past the token
			// and fails later on the credential Secret (no informer, no
			// in-cluster config here): it never answers a token cause.
			rec, reached := p1076Serve(mw.mk(keys), knpRequest(tok))
			if c := knpCause(rec.Body.Bytes()); strings.HasPrefix(c, "Token") || c == "KeyNotPublished" || rec.Code == http.StatusServiceUnavailable ||
				(mw.name == "RefreshAuth" && (!reached || rec.Code != http.StatusOK)) {
				t.Fatalf("while key-1 is published the token must validate; code=%d reached=%v body=%s", rec.Code, reached, rec.Body.String())
			}

			authn.publish(map[string]*rsa.PublicKey{"key-2": &newKey.PublicKey})
			rec, reached = p1076Serve(mw.mk(keys), knpRequest(tok))
			if reached || rec.Code != http.StatusUnauthorized || knpCause(rec.Body.Bytes()) != "KeyNotPublished" {
				t.Fatalf("after key-1 is retired: want 401 KeyNotPublished; code=%d reached=%v body=%s", rec.Code, reached, rec.Body.String())
			}
			if got := rec.Result().Header.Get("WWW-Authenticate"); got != `Bearer error="invalid_token"` {
				t.Fatalf("challenge = %q", got)
			}
		})
	}
}

// What must NOT become KeyNotPublished. An absent kid that may be a key the
// set predates, or about which the latest fetch says nothing, stays OUR fault:
// plumbing's 503, byte for byte, no cause, no challenge. A bad signature under
// a published kid stays TokenInvalid.
func TestPreDispatch401_KeyNotPublished_NeighboursKeepTheirAnswer(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	current, other := knpKey(t), knpKey(t)
	now := time.Now().Truncate(time.Second)

	type tc struct {
		name   string
		keys   func(t *testing.T) jwtutil.KeySource
		token  func(t *testing.T) string
		code   int
		reason string // "" for the causeless 503
	}
	published := func(t *testing.T) jwtutil.KeySource {
		a := &knpJWKS{}
		a.publish(map[string]*rsa.PublicKey{"key-2": &current.PublicKey})
		return jwtutil.NewJWKSKeySource(a.serve(t))
	}
	for _, c := range []tc{
		// A token issued at the fetch, as one signed with a new key at the
		// rotation moment is: the set may predate its key.
		{"absent kid, issued now", published, func(t *testing.T) string { return knpMint(t, other, "key-3", &now) },
			http.StatusServiceUnavailable, ""},
		{"absent kid, no iat", published, func(t *testing.T) string { return knpMint(t, other, "key-3", nil) },
			http.StatusServiceUnavailable, ""},
		// The latest fetch failed after a good one: an absent kid says nothing.
		{"absent kid, latest fetch failed", func(t *testing.T) jwtutil.KeySource {
			a := &knpJWKS{}
			a.publish(map[string]*rsa.PublicKey{"key-2": &current.PublicKey})
			keys := jwtutil.NewJWKSKeySource(a.serve(t),
				jwtutil.WithJWKSCacheTTL(time.Nanosecond), jwtutil.WithJWKSMinRefreshInterval(time.Nanosecond))
			if _, err := keys.PublicKey("key-2"); err != nil {
				t.Fatalf("precondition: first fetch: %v", err)
			}
			a.fail()
			return keys
		}, func(t *testing.T) string { return knpMint(t, other, "key-3", knpIssuedBefore()) },
			http.StatusServiceUnavailable, ""},
		{"JWKS never reachable", func(t *testing.T) jwtutil.KeySource {
			a := &knpJWKS{}
			a.fail()
			return jwtutil.NewJWKSKeySource(a.serve(t))
		}, func(t *testing.T) string { return knpMint(t, other, "key-3", knpIssuedBefore()) },
			http.StatusServiceUnavailable, ""},
		// A published kid, a signature from another key: TokenInvalid, even with
		// an old iat.
		{"bad signature under a published kid", published, func(t *testing.T) string { return knpMint(t, other, "key-2", knpIssuedBefore()) },
			http.StatusUnauthorized, "TokenInvalid"},
	} {
		for _, mw := range knpMiddlewares {
			t.Run(c.name+"/"+mw.name, func(t *testing.T) {
				rec, reached := p1076Serve(mw.mk(c.keys(t)), knpRequest(c.token(t)))
				if reached || rec.Code != c.code {
					t.Fatalf("want %d; code=%d reached=%v body=%s", c.code, rec.Code, reached, rec.Body.String())
				}
				if c.reason != "" {
					if got := knpCause(rec.Body.Bytes()); got != c.reason {
						t.Fatalf("cause = %q, want %q; body=%s", got, c.reason, rec.Body.String())
					}
					return
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
