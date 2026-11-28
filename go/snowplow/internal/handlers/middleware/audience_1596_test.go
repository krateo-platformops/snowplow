package middleware_test

// audience_1596_test.go — #1596: snowplow is called by a name
// (snowplow.krateo-system), and a token authn issued for other recipients only
// is a 401 TokenAudienceMismatch, on both validating middlewares.
//
//   - aud absent (every sign-in, step-up and service-account token): accepted;
//   - aud exactly ours, or ours among others: accepted;
//   - aud naming only others, or an empty list: refused, with the cause, the
//     invalid_token challenge, a message that names no audience and no
//     identity, and the exact body the frontend is promised;
//   - the cause is chosen BEFORE TokenInvalid (ErrAudienceMismatch wraps
//     ErrTokenInvalid);
//   - the refusal is counted by the token's actor class, never by a name.
//
// Driven through the real UserConfig (real Secrets informer) and RefreshAuth.

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/jwtutil"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

const a1596Name = "snowplow.krateo-system"

// a1596WantBody is the 401 body the frontend gets, verbatim.
const a1596WantBody = `{"kind":"Status","apiVersion":"v1","status":"Failure","message":"token is invalid: token is for another audience","reason":"Unauthorized","code":401,"details":{"causes":[{"reason":"TokenAudienceMismatch","message":"the bearer token was issued for another recipient; this is a configuration fault, not a login problem"}]}}` + "\n"

// a1596Token signs a token for su994User with aud exactly as given (nil: no
// claim; a []string{}: an empty list) and the given principal ("" : none).
func a1596Token(t *testing.T, aud any, principal string) string {
	t.Helper()
	claims := map[string]any{
		"username": su994User, "groups": []string{su994Group},
		"iss": "krateo.io", "iat": time.Now().Unix(), "nbf": time.Now().Unix(),
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	if aud != nil {
		claims["aud"] = aud
	}
	if principal != "" {
		claims["principal"] = principal
	}
	hdr, _ := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": testKeyID})
	body, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	signing := base64.RawURLEncoding.EncodeToString(hdr) + "." + base64.RawURLEncoding.EncodeToString(body)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, testPrivateKey, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

type a1596Route struct {
	name  string
	serve func(t *testing.T, tok string, opts ...jwtutil.ValidateOption) (*httptest.ResponseRecorder, bool)
}

func a1596Routes() []a1596Route {
	do := func(mw func(http.Handler) http.Handler, tok string) (*httptest.ResponseRecorder, bool) {
		r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
		r.Header.Set("Authorization", "Bearer "+tok)
		return p1076Serve(mw, r)
	}
	return []a1596Route{
		{"UserConfig", func(t *testing.T, tok string, opts ...jwtutil.ValidateOption) (*httptest.ResponseRecorder, bool) {
			su994Informer(t, su994Secret(credsecret.SessionNames(su994User)[0], su994Session))
			su994APIServer(t)
			return do(middleware.UserConfig(testKeys, testAuthnNS, opts...), tok)
		}},
		{"RefreshAuth", func(t *testing.T, tok string, opts ...jwtutil.ValidateOption) (*httptest.ResponseRecorder, bool) {
			return do(middleware.RefreshAuth(testKeys, opts...), tok)
		}},
	}
}

func TestAudience1596_AcceptedAndRefused(t *testing.T) {
	cases := []struct {
		name    string
		aud     any
		refused bool
	}{
		{"aud absent", nil, false},
		{"exactly ours", []string{a1596Name}, false},
		{"ours among others", []string{"agentiko.krateo-system", a1596Name, "other.ns"}, false},
		{"ours as a bare string", a1596Name, false},
		{"only others", []string{"agentiko.krateo-system", "other.ns"}, true},
		{"a near miss", []string{"snowplow.other-ns"}, true},
		// The match is exact: no case folding, no trailing dot.
		{"the name in another case", []string{"SNOWPLOW.KRATEO-SYSTEM"}, true},
		{"the name with a trailing dot", []string{a1596Name + "."}, true},
		{"an empty list", []string{}, true},
	}
	for _, rt := range a1596Routes() {
		for _, tc := range cases {
			t.Run(rt.name+"/"+tc.name, func(t *testing.T) {
				rec, reached := rt.serve(t, a1596Token(t, tc.aud, ""), middleware.AudienceOptions(a1596Name)...)
				if !tc.refused {
					if rec.Code != http.StatusOK || !reached {
						t.Fatalf("want accepted; code=%d reached=%v body=%s", rec.Code, reached, rec.Body.String())
					}
					return
				}
				if reached || rec.Code != http.StatusUnauthorized {
					t.Fatalf("want a 401 that never reaches the handler; code=%d reached=%v", rec.Code, reached)
				}
				if got := rec.Body.String(); got != a1596WantBody {
					t.Fatalf("body:\n got  %q\n want %q", got, a1596WantBody)
				}
				if got := rec.Result().Header.Values("WWW-Authenticate"); len(got) != 1 || got[0] != `Bearer error="invalid_token"` {
					t.Fatalf("challenge = %q, want Bearer error=\"invalid_token\"", got)
				}
				// No service, no audience value, no identity anywhere in the body.
				for _, leak := range []string{"snowplow", "krateo-system", "agentiko", "other.ns", su994User, su994Group} {
					if strings.Contains(rec.Body.String(), leak) {
						t.Fatalf("the 401 names %q: %s", leak, rec.Body.String())
					}
				}
			})
		}
	}
}

// Without a configured audience nothing is checked: the same tokens that are
// refused above are served (the chart always configures one).
func TestAudience1596_NoNameChecksNothing(t *testing.T) {
	for _, rt := range a1596Routes() {
		t.Run(rt.name, func(t *testing.T) {
			rec, reached := rt.serve(t, a1596Token(t, []string{"other.ns"}, ""), middleware.AudienceOptions("")...)
			if rec.Code != http.StatusOK || !reached {
				t.Fatalf("code=%d reached=%v body=%s", rec.Code, reached, rec.Body.String())
			}
		})
	}
}

// The ordering: the validator's error is BOTH ErrTokenInvalid and
// ErrAudienceMismatch, and the answer is the mismatch cause, not TokenInvalid.
// A token that is merely invalid still answers TokenInvalid.
func TestAudience1596_MismatchWinsOverTokenInvalid(t *testing.T) {
	tok := a1596Token(t, []string{"other.ns"}, "")
	_, err := jwtutil.ValidateWithKeySource(testKeys, tok, middleware.AudienceOptions(a1596Name)...)
	if !errors.Is(err, jwtutil.ErrTokenInvalid) || !errors.Is(err, jwtutil.ErrAudienceMismatch) {
		t.Fatalf("precondition: the validator's error must be both; got %v", err)
	}
	for _, rt := range a1596Routes() {
		t.Run(rt.name, func(t *testing.T) {
			rec, _ := rt.serve(t, tok, middleware.AudienceOptions(a1596Name)...)
			if !strings.Contains(rec.Body.String(), `"reason":"TokenAudienceMismatch"`) || strings.Contains(rec.Body.String(), `"reason":"TokenInvalid"`) {
				t.Fatalf("want TokenAudienceMismatch, not TokenInvalid: %s", rec.Body.String())
			}
			bad, _ := rt.serve(t, tok[:len(tok)-4]+"AAAA", middleware.AudienceOptions(a1596Name)...)
			if !strings.Contains(bad.Body.String(), `"reason":"TokenInvalid"`) {
				t.Fatalf("a bad signature must stay TokenInvalid: %s", bad.Body.String())
			}
		})
	}
}

// The refusal is counted by the token's signed actor class, and an accepted
// token is not counted.
func TestAudience1596_RefusalIsCountedByActorClass(t *testing.T) {
	opts := middleware.AudienceOptions(a1596Name)
	rt := a1596Routes()[1]
	before := map[string]int64{"agent": middleware.AudienceMismatchCount("agent"), "unknown": middleware.AudienceMismatchCount("unknown")}
	rt.serve(t, a1596Token(t, []string{"other.ns"}, "agent"), opts...)
	rt.serve(t, a1596Token(t, []string{"other.ns"}, ""), opts...)
	rt.serve(t, a1596Token(t, []string{a1596Name}, "agent"), opts...)
	rt.serve(t, a1596Token(t, nil, ""), opts...)
	if got := middleware.AudienceMismatchCount("agent") - before["agent"]; got != 1 {
		t.Errorf("agent count moved by %d, want 1", got)
	}
	if got := middleware.AudienceMismatchCount("unknown") - before["unknown"]; got != 1 {
		t.Errorf("unknown count moved by %d, want 1", got)
	}
}
