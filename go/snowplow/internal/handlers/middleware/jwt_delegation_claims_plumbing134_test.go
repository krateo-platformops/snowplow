// jwt_delegation_claims_plumbing134_test.go — what plumbing v1.34.0's token
// validation changes at snowplow's two JWT gates, middleware.UserConfig and
// middleware.RefreshAuth.
//
// plumbing v1.30.0 (#98) and v1.34.0 (#104) give jwtutil.UserInfo two optional
// claims, RFC 8693's `act` and `principal`, and jwtutil.ValidateWithKeySource
// now decodes both. Before, the verifier ignored them: any correctly signed
// token was accepted whatever `act` held. Now:
//
//   - a well-formed `act` chain is accepted, and the token's identity is still
//     its subject: snowplow reads the subject's credential Secret and places
//     the subject in the request context, never the actor;
//   - `principal` is lenient: an unknown or non-string kind is accepted;
//   - a signed token whose `act` is malformed — not an object, a link with no
//     `sub`, or a chain deeper than 8 — is NEW-REFUSED with ErrTokenInvalid,
//     so both gates answer 401 TokenInvalid where v1.28.1 served the subject.
//
// authn's jwtutil.CreateToken refuses to sign such a chain, so no token authn
// issues is refused by this. The arms pin the refusal deliberately, so a
// later plumbing that loosens or tightens it shows up here.
//
// The tokens are signed by hand (RS256 over a CreateToken token's own header
// and claims, with extra claims added), because CreateToken refuses the
// malformed chains. The control arm re-signs with no extra claim and must be
// accepted, so a 401 below is the claim's refusal, not a bad signature.
package middleware_test

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

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

const (
	dlgSubject  = "dlg-erin"
	dlgActor    = "dlg-agent"
	dlgGroup    = "dlg-devs"
	dlgSubjURL  = "https://subject.dlg.example/k8s"
	dlgActorURL = "https://actor.dlg.example/k8s"
)

// dlgToken signs, with key under kid, the claims CreateToken issues for
// dlgSubject plus extra (each key set, replacing any CreateToken wrote).
func dlgToken(t *testing.T, key *rsa.PrivateKey, kid string, extra map[string]any) string {
	t.Helper()
	base, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: dlgSubject, Groups: []string{dlgGroup}, Duration: time.Hour, KeyID: kid, PrivateKey: key,
	})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	parts := strings.Split(base, ".")
	if len(parts) != 3 {
		t.Fatalf("CreateToken returned %d segments", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("decode claims: %v", err)
	}
	claims := map[string]any{}
	if err := json.Unmarshal(raw, &claims); err != nil {
		t.Fatalf("unmarshal claims: %v", err)
	}
	for k, v := range extra {
		claims[k] = v
	}
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("marshal claims: %v", err)
	}
	signing := parts[0] + "." + base64.RawURLEncoding.EncodeToString(payload)
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// dlgChain is an `act` chain of depth links, outermost first, each naming an
// actor.
func dlgChain(depth int) map[string]any {
	var link map[string]any
	for i := depth; i >= 1; i-- {
		next := map[string]any{"sub": dlgActor + "-" + string(rune('a'+i-1))}
		if link != nil {
			next["act"] = link
		}
		link = next
	}
	return link
}

// The claims each arm adds; nil adds none.
var (
	dlgAccepted = []struct {
		name  string
		extra map[string]any
	}{
		{"control: re-signed, no extra claim", nil},
		{"delegated: act names the actor, principals known", map[string]any{
			"principal": "human",
			"act":       map[string]any{"sub": dlgActor, "principal": "agent", "act": map[string]any{"sub": dlgActor + "-root", "principal": "controller"}},
		}},
		{"act null", map[string]any{"act": nil}},
		{"act chain exactly 8 deep", map[string]any{"act": dlgChain(8)}},
		{"principal unknown string", map[string]any{"principal": "robot"}},
		{"principal not a string", map[string]any{"principal": 7}},
		{"actor principal unknown", map[string]any{"act": map[string]any{"sub": dlgActor, "principal": "robot"}}},
	}
	dlgRefused = []struct {
		name  string
		extra map[string]any
	}{
		{"act is a string", map[string]any{"act": dlgActor}},
		{"act is an array", map[string]any{"act": []any{map[string]any{"sub": dlgActor}}}},
		{"act has no sub", map[string]any{"act": map[string]any{}}},
		{"act sub empty", map[string]any{"act": map[string]any{"sub": ""}}},
		{"nested act link has no sub", map[string]any{"act": map[string]any{"sub": dlgActor, "act": map[string]any{"principal": "agent"}}}},
		{"act chain 9 deep", map[string]any{"act": dlgChain(9)}},
	}
)

// UserConfig: an accepted token reads the SUBJECT's credential Secret and puts
// the subject in the request context — even with the actor's own Secret right
// there in the informer.
func TestUserConfig_DelegationClaims_IdentityStaysTheSubject(t *testing.T) {
	su994Informer(t,
		su994Secret(credsecret.SessionNames(dlgSubject)[0], dlgSubjURL),
		su994Secret(credsecret.SessionNames(dlgActor)[0], dlgActorURL))
	asked := su994APIServer(t)

	for _, tc := range dlgAccepted {
		t.Run(tc.name, func(t *testing.T) {
			res := su994Serve(t, dlgToken(t, testPrivateKey, testKeyID, tc.extra))
			if res.code != http.StatusOK || !res.reached {
				t.Fatalf("want 200 reaching the handler; code=%d body=%s", res.code, res.body)
			}
			if res.ep.ServerURL != dlgSubjURL {
				t.Fatalf("credential read = %q, want the subject's %q (never the actor's)", res.ep.ServerURL, dlgSubjURL)
			}
			if res.user.Username != dlgSubject || len(res.user.Groups) != 1 || res.user.Groups[0] != dlgGroup {
				t.Fatalf("ctx identity = %q %v, want the subject %q [%s]", res.user.Username, res.user.Groups, dlgSubject, dlgGroup)
			}
		})
	}
	if got := asked(); len(got) != 0 {
		t.Fatalf("every read was an informer hit; the apiserver must not be asked, got %v", got)
	}
}

// UserConfig: a signed token with a malformed `act` is a 401 TokenInvalid that
// never reaches the handler, with exactly the six Status fields upstream
// use.UserConfig writes for it.
func TestUserConfig_DelegationClaims_MalformedActIs401TokenInvalid(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false")
	upstream := use.UserConfig(testKeys, testAuthnNS)
	snowplow := middleware.UserConfig(testKeys, testAuthnNS)
	for _, tc := range dlgRefused {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/call?x=1", nil)
			req.Header.Set("Authorization", "Bearer "+dlgToken(t, testPrivateKey, testKeyID, tc.extra))
			up := httptest.NewRecorder()
			upstream(http.NotFoundHandler()).ServeHTTP(up, req.Clone(req.Context()))
			if up.Code != http.StatusUnauthorized {
				t.Fatalf("precondition: upstream use.UserConfig refuses the token 401; code=%d body=%s", up.Code, up.Body.String())
			}
			rec, reached := p1076Serve(snowplow, req)
			p1076AssertCause(t, rec, reached, up.Body.String(), "TokenInvalid")
		})
	}
}

// RefreshAuth: the same two answers on the header-or-cookie gate.
func TestRefreshAuth_DelegationClaims(t *testing.T) {
	t.Setenv("REFRESH_SESSION_COOKIE", "krateo-session")
	for _, tc := range dlgAccepted {
		t.Run("accepted/"+tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
			req.Header.Set("Authorization", "Bearer "+dlgToken(t, refreshAuthTestPrivateKey, refreshAuthTestKeyID, tc.extra))
			var user string
			var reached bool
			h := middleware.RefreshAuth(refreshAuthTestKeys)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reached = true
				if ui, err := xcontext.UserInfo(r.Context()); err == nil {
					user = ui.Username
				}
				w.WriteHeader(http.StatusOK)
			}))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK || !reached || user != dlgSubject {
				t.Fatalf("want 200 as the subject %q; code=%d reached=%v user=%q body=%s", dlgSubject, rec.Code, reached, user, rec.Body.String())
			}
		})
	}
	plumbing := httptest.NewRecorder()
	_ = response.Unauthorized(plumbing, errors.New(jwtutil.ErrTokenInvalid.Error()))
	for _, tc := range dlgRefused {
		t.Run("refused/"+tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/debug/vars", nil)
			req.AddCookie(&http.Cookie{Name: "krateo-session", Value: dlgToken(t, refreshAuthTestPrivateKey, refreshAuthTestKeyID, tc.extra)})
			rec, reached := p1076Serve(middleware.RefreshAuth(refreshAuthTestKeys), req)
			p1076AssertCause(t, rec, reached, plumbing.Body.String(), "TokenInvalid")
		})
	}
}
