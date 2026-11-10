// credential_status_1029_test.go — #1029: a missing per-user credential answers
// a 401 whose Status names the cause, so the portal can tell "step up again"
// from "log in again" without parsing text.
//
// Driven through the REAL middleware.UserConfig with real RS256 JWTs, the real
// Secrets informer and the real apiserver re-read against a fake apiserver (the
// #994 harness in userconfig_stepup_994_test.go). The wire contract is asserted
// as LITERALS, never through the package constants, so renaming or swapping a
// constant is caught.
package middleware_test

import (
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/plumbing/server/use"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
)

const c1029Challenge = `Bearer error="insufficient_user_authentication"`

type c1029Body struct {
	Kind       string `json:"kind"`
	APIVersion string `json:"apiVersion"`
	Status     string `json:"status"`
	Message    string `json:"message"`
	Reason     string `json:"reason"`
	Code       int    `json:"code"`
	Details    *struct {
		Causes []struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"causes"`
	} `json:"details"`
}

// c1029Serve drives one request through the real middleware and returns the
// recorder and whether the next handler was reached.
func c1029Serve(t *testing.T, tok string) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	reached := false
	terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusOK)
	})
	wrapped := use.NewChain(middleware.UserConfig(testKeys, testAuthnNS)).Then(terminal)
	r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()
	wrapped.ServeHTTP(rec, r)
	return rec, reached
}

func c1029Decode(t *testing.T, rec *httptest.ResponseRecorder) c1029Body {
	t.Helper()
	var b c1029Body
	if err := json.Unmarshal(rec.Body.Bytes(), &b); err != nil {
		t.Fatalf("the body is not a JSON Status: %v (%q)", err, rec.Body.String())
	}
	return b
}

// c1029AssertPlumbingCompatible: every field plumbing's Status has is
// byte-identical to what response.Unauthorized writes for the same message, in
// the same order, so a reader of `message` (or of any other plumbing field)
// sees exactly what it saw before #1029. The cause is appended after them.
func c1029AssertPlumbingCompatible(t *testing.T, rec *httptest.ResponseRecorder, b c1029Body) {
	t.Helper()
	want := httptest.NewRecorder()
	_ = response.Unauthorized(want, errors.New(b.Message))
	prefix := strings.TrimSuffix(want.Body.String(), "}\n")
	if !strings.HasPrefix(rec.Body.String(), prefix+`,"details":{"causes":[`) {
		t.Fatalf("the plumbing fields are not byte-compatible:\n got  %s\n want %s,\"details\":...", rec.Body.String(), prefix)
	}
	if got := rec.Result().Header.Get("Content-Type"); got != want.Header().Get("Content-Type") {
		t.Fatalf("Content-Type = %q, want plumbing's %q", got, want.Header().Get("Content-Type"))
	}
	var asPlumbing response.Status
	if err := json.Unmarshal(rec.Body.Bytes(), &asPlumbing); err != nil {
		t.Fatalf("a plumbing response.Status reader cannot decode the body: %v", err)
	}
	if asPlumbing != *response.New(http.StatusUnauthorized, errors.New(b.Message)) {
		t.Fatalf("a plumbing response.Status reader sees %+v, want %+v", asPlumbing, *response.New(http.StatusUnauthorized, errors.New(b.Message)))
	}
}

// A krateo:loa:2 token whose step-up Secret is missing: 401, cause
// StepUpRequired, and the bare RFC 9470 challenge.
func TestCredentialStatus1029_StepUpMissingIsStepUpRequired(t *testing.T) {
	session := su994Secret(credsecret.SessionName(su994User), su994Session)
	su994Informer(t, session)
	su994APIServer(t, session)

	rec, reached := c1029Serve(t, su994Token(t, su994Group, credsecret.StepUpGroup))
	if reached || rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 without reaching the handler; code=%d reached=%v", rec.Code, reached)
	}
	b := c1029Decode(t, rec)
	if b.Details == nil || len(b.Details.Causes) != 1 || b.Details.Causes[0].Reason != "StepUpRequired" {
		t.Fatalf("want details.causes[0].reason=StepUpRequired; body=%s", rec.Body.String())
	}
	if got := rec.Result().Header.Values("WWW-Authenticate"); len(got) != 1 || got[0] != c1029Challenge {
		t.Fatalf("WWW-Authenticate = %q, want exactly [%q]", got, c1029Challenge)
	}
	if strings.Contains(rec.Result().Header.Get("WWW-Authenticate"), "acr_values") {
		t.Fatal("the challenge must not carry acr_values (krateo:loa:2 is not an IdP ACR value)")
	}
	if !strings.Contains(b.Message, "not found") {
		t.Fatalf("message must stay the re-read's NotFound text; got %q", b.Message)
	}
	c1029AssertPlumbingCompatible(t, rec, b)
}

// Any other token whose session Secret is missing: 401, cause
// SessionCredentialMissing, never StepUpRequired, and the RFC 6750
// invalid_token challenge (#1076), never the step-up one — even with a step-up
// Secret for the same user sitting in the informer.
func TestCredentialStatus1029_SessionMissingIsSessionCredentialMissing(t *testing.T) {
	stepUp := su994Secret(credsecret.StepUpName(su994User), su994StepUp)
	su994Informer(t, stepUp)
	su994APIServer(t, stepUp)

	rec, reached := c1029Serve(t, su994Token(t, su994Group))
	if reached || rec.Code != http.StatusUnauthorized {
		t.Fatalf("want 401 without reaching the handler; code=%d reached=%v", rec.Code, reached)
	}
	b := c1029Decode(t, rec)
	if b.Details == nil || len(b.Details.Causes) != 1 || b.Details.Causes[0].Reason != "SessionCredentialMissing" {
		t.Fatalf("want details.causes[0].reason=SessionCredentialMissing; body=%s", rec.Body.String())
	}
	if got := rec.Result().Header.Values("WWW-Authenticate"); len(got) != 1 || got[0] != `Bearer error="invalid_token"` {
		t.Fatalf("the session case must send exactly the invalid_token challenge (#1076); got %q", got)
	}
	if strings.Contains(rec.Body.String(), "StepUpRequired") {
		t.Fatalf("the session case must never name StepUpRequired: %s", rec.Body.String())
	}
	c1029AssertPlumbingCompatible(t, rec, b)
}

// A re-read that fails for another reason (403, 5xx) is not "missing": it stays
// plumbing's 500, byte-identical, with no cause and no challenge — for a
// step-up token and for a session token alike.
func TestCredentialStatus1029_ReReadErrorIs500WithNoCause(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stepUp bool
		code   int
	}{
		{"stepup/403", true, http.StatusForbidden},
		{"stepup/500", true, http.StatusInternalServerError},
		{"session/403", false, http.StatusForbidden},
		{"session/500", false, http.StatusInternalServerError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			groups := []string{su994Group}
			want := credsecret.SessionName(su994User)
			if tc.stepUp {
				groups = append(groups, credsecret.StepUpGroup)
				want = credsecret.StepUpName(su994User)
			}
			su994Informer(t)
			su994APIServerFailing(t, map[string]int{want: tc.code})

			rec, reached := c1029Serve(t, su994Token(t, groups...))
			if reached || rec.Code != http.StatusInternalServerError {
				t.Fatalf("a %d re-read must answer 500; code=%d reached=%v body=%s", tc.code, rec.Code, reached, rec.Body.String())
			}
			b := c1029Decode(t, rec)
			if b.Details != nil || strings.Contains(rec.Body.String(), "causes") {
				t.Fatalf("a re-read error must carry no cause: %s", rec.Body.String())
			}
			if got := rec.Header().Values("WWW-Authenticate"); len(got) != 0 {
				t.Fatalf("a re-read error must send no challenge; got %q", got)
			}
			plumbing := httptest.NewRecorder()
			_ = response.InternalError(plumbing, errors.New(b.Message))
			if rec.Body.String() != plumbing.Body.String() {
				t.Fatalf("the 500 must stay plumbing's, byte for byte:\n got  %s\n want %s", rec.Body.String(), plumbing.Body.String())
			}
		})
	}
}

// ── Reviewer arms (#1031 security review: R-DOC, R-NM, R-ID, R-INF) ──────────
// They pin three properties the arms above did not hold: the cause follows
// credsecret.For's EXACT group match (R1), the cause message carries no
// identity (R2/R2b), and the informer paths never produce the cause shape (R6).
// R-DOC also keeps the integration doc's section 2 examples byte-equal to the
// wire.

// c1029DocExample is one of the doc's ```http examples: its status line, its
// header lines and its body.
type c1029DocExample struct {
	statusLine string
	headers    []string
	body       string
}

// c1029DocBlocks returns the doc's ```http examples (section 2), keyed by the
// details.causes[0].reason of their body (#1076: one example per cause).
func c1029DocBlocks(t *testing.T) map[string]c1029DocExample {
	t.Helper()
	b, err := os.ReadFile("../../../docs/howto-frontend-credential-401-causes.md")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]c1029DocExample{}
	for _, blk := range strings.Split(string(b), "```http\n")[1:] {
		blk = blk[:strings.Index(blk, "```")]
		var ex c1029DocExample
		for _, ln := range strings.Split(blk, "\n") {
			switch {
			case strings.HasPrefix(ln, "HTTP/"):
				ex.statusLine = ln
			case strings.HasPrefix(ln, "{"):
				ex.body = ln
			case strings.Contains(ln, ": "):
				ex.headers = append(ex.headers, ln)
			}
		}
		var st c1029Body
		if err := json.Unmarshal([]byte(ex.body), &st); err != nil || st.Details == nil || len(st.Details.Causes) != 1 {
			t.Fatalf("doc example has no single cause: %q", ex.body)
		}
		if _, dup := out[st.Details.Causes[0].Reason]; dup {
			t.Fatalf("two doc examples for %s", st.Details.Causes[0].Reason)
		}
		out[st.Details.Causes[0].Reason] = ex
	}
	return out
}

// R-DOC — the doc's examples are the wire, byte for byte (status line, every
// header, body), once the doc's example user is substituted: one example for
// each cause of the #1076 closed set, and no other.
func TestCredentialStatus1029_DocExamplesEqualTheWire(t *testing.T) {
	examples := c1029DocBlocks(t)
	sub := func(s string) string { return strings.ReplaceAll(s, "alice", su994User) }
	bearer := func(v string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
		if v != "" {
			r.Header.Set("Authorization", v)
		}
		return r
	}
	tokenServe := func(r *http.Request) func(*testing.T) *httptest.ResponseRecorder {
		return func(t *testing.T) *httptest.ResponseRecorder {
			t.Setenv("CACHE_ENABLED", "false")
			rec, _ := p1076Serve(middleware.UserConfig(testKeys, testAuthnNS), r)
			return rec
		}
	}
	// wireSub replaces, in a doc example, the values the wire cannot share
	// with the doc: KeyNotPublished's message names the JWKS URL, the time
	// the set was fetched and the token's issue time (plumbing's error text).
	// Each is checked against what the test set up before it is substituted.
	var wireSub []string
	cases := map[string]func(*testing.T) *httptest.ResponseRecorder{
		"KeyNotPublished": func(t *testing.T) *httptest.ResponseRecorder {
			t.Setenv("CACHE_ENABLED", "false")
			authn := &knpJWKS{}
			authn.publish(map[string]*rsa.PublicKey{"key-2": &knpKey(t).PublicKey})
			url := authn.serve(t)
			iat := knpIssuedBefore()
			before := time.Now().Add(-time.Second)
			rec, _ := p1076Serve(middleware.UserConfig(jwtutil.NewJWKSKeySource(url), testAuthnNS),
				bearer("Bearer "+knpMint(t, knpKey(t), "key-1", iat)))
			after := time.Now().Add(time.Second)
			m := regexp.MustCompile(`, fetched at (\S+), holds`).FindStringSubmatch(rec.Body.String())
			if m == nil {
				t.Fatalf("no fetch time in %s", rec.Body.String())
			}
			fetched, err := time.Parse(time.RFC3339, m[1])
			if err != nil || fetched.Before(before.Truncate(time.Second)) || fetched.After(after) {
				t.Fatalf("fetch time %q is not this test's fetch [%s, %s]", m[1], before, after)
			}
			wireSub = []string{
				"http://authn.example/.well-known/jwks.json", url,
				"2026-11-10T09:30:00Z", m[1],
				"2026-11-10T09:00:00Z", iat.UTC().Format(time.RFC3339),
			}
			return rec
		},
		"TokenMissing": tokenServe(bearer("")),
		"TokenInvalid": tokenServe(bearer("Bearer not.a.real.jwt")),
		"TokenExpired": func(t *testing.T) *httptest.ResponseRecorder {
			return tokenServe(bearer("Bearer " + helperMakeToken(t, testUsername, -time.Hour)))(t)
		},
		"StepUpRequired": func(t *testing.T) *httptest.ResponseRecorder {
			session := su994Secret(credsecret.SessionName(su994User), su994Session)
			su994Informer(t, session)
			su994APIServer(t, session)
			rec, _ := c1029Serve(t, su994Token(t, su994Group, credsecret.StepUpGroup))
			return rec
		},
		"SessionCredentialMissing": func(t *testing.T) *httptest.ResponseRecorder {
			stepUp := su994Secret(credsecret.StepUpName(su994User), su994StepUp)
			su994Informer(t, stepUp)
			su994APIServer(t, stepUp)
			rec, _ := c1029Serve(t, su994Token(t, su994Group))
			return rec
		},
	}
	if len(examples) != len(cases) {
		t.Fatalf("the doc has %d examples, want one per cause (%d)", len(examples), len(cases))
	}
	for reason, serve := range cases {
		t.Run(reason, func(t *testing.T) {
			ex, ok := examples[reason]
			if !ok {
				t.Fatalf("the doc has no example for %s", reason)
			}
			wireSub = nil
			rec := serve(t)
			if ex.statusLine != "HTTP/1.1 401 Unauthorized" || rec.Code != http.StatusUnauthorized {
				t.Fatalf("status: doc %q wire %d", ex.statusLine, rec.Code)
			}
			body := sub(ex.body)
			if wireSub != nil {
				for i := 0; i < len(wireSub); i += 2 {
					if strings.Count(body, wireSub[i]) != 1 {
						t.Fatalf("the doc example must name %q exactly once: %s", wireSub[i], body)
					}
				}
				body = strings.NewReplacer(wireSub...).Replace(body)
			}
			if got, want := rec.Body.String(), body+"\n"; got != want {
				t.Fatalf("body differs from doc:\n got  %q\n want %q", got, want)
			}
			if len(ex.headers) != len(rec.Result().Header) {
				t.Fatalf("doc headers %q, wire headers %v", ex.headers, rec.Result().Header)
			}
			for _, h := range ex.headers {
				k, v, _ := strings.Cut(h, ": ")
				if got := rec.Result().Header.Values(k); len(got) != 1 || got[0] != v {
					t.Fatalf("header %q: doc %q wire %q", k, v, got)
				}
			}
		})
	}
}

// R-NM — a token whose groups only resemble the marker reads the session Secret
// (credsecret.For) and gets the session cause, never StepUpRequired.
func TestCredentialStatus1029_NearMissGroupIsSessionCause(t *testing.T) {
	for _, g := range []string{"x-krateo:loa:2", "krateo:loa:2 ", "KRATEO:LOA:2", "krateo:loa:20"} {
		t.Run(g, func(t *testing.T) {
			stepUp := su994Secret(credsecret.StepUpName(su994User), su994StepUp)
			su994Informer(t, stepUp)
			su994APIServer(t, stepUp)
			rec, _ := c1029Serve(t, su994Token(t, su994Group, g))
			if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), `"reason":"SessionCredentialMissing"`) ||
				rec.Result().Header.Get("WWW-Authenticate") != `Bearer error="invalid_token"` {
				t.Fatalf("group %q: want the session cause; code=%d hdr=%q body=%s",
					g, rec.Code, rec.Result().Header.Get("WWW-Authenticate"), rec.Body.String())
			}
		})
	}
}

// R-ID — neither cause message carries identity.
func TestCredentialStatus1029_CauseMessageCarriesNoIdentity(t *testing.T) {
	for _, stepup := range []bool{true, false} {
		t.Run(map[bool]string{true: "stepup", false: "session"}[stepup], func(t *testing.T) {
			groups := []string{su994Group}
			have := su994Secret(credsecret.StepUpName(su994User), su994StepUp)
			if stepup {
				groups = append(groups, credsecret.StepUpGroup)
				have = su994Secret(credsecret.SessionName(su994User), su994Session)
			}
			su994Informer(t, have)
			su994APIServer(t, have)
			rec, _ := c1029Serve(t, su994Token(t, groups...))
			b := c1029Decode(t, rec)
			if b.Details == nil || len(b.Details.Causes) != 1 {
				t.Fatalf("no cause: %s", rec.Body.String())
			}
			if m := b.Details.Causes[0].Message; strings.Contains(m, su994User) || strings.Contains(m, "clientconfig") {
				t.Fatalf("cause message carries identity: %q", m)
			}
		})
	}
}

// R-INF — the informer paths never produce the cause shape: a hit reaches the
// handler; a malformed Secret in the snapshot is plumbing's 500, with no cause
// and no challenge.
func TestCredentialStatus1029_InformerPathsNeverCarryACause(t *testing.T) {
	for _, stepup := range []bool{true, false} {
		groups := []string{su994Group}
		name := credsecret.SessionName(su994User)
		if stepup {
			groups = append(groups, credsecret.StepUpGroup)
			name = credsecret.StepUpName(su994User)
		}
		kind := map[bool]string{true: "stepup", false: "session"}[stepup]
		t.Run(kind+"/hit", func(t *testing.T) {
			s := su994Secret(name, su994Session)
			su994Informer(t, s)
			su994APIServer(t)
			rec, reached := c1029Serve(t, su994Token(t, groups...))
			if !reached || rec.Code != http.StatusOK || rec.Header().Get("WWW-Authenticate") != "" {
				t.Fatalf("cache hit: reached=%v code=%d body=%s", reached, rec.Code, rec.Body.String())
			}
		})
		t.Run(kind+"/malformed", func(t *testing.T) {
			s := su994Secret(name, su994Session)
			delete(s.Data, "server-url")
			su994Informer(t, s)
			su994APIServer(t, s)
			rec, reached := c1029Serve(t, su994Token(t, groups...))
			if reached || rec.Code != http.StatusInternalServerError || strings.Contains(rec.Body.String(), "causes") ||
				rec.Header().Get("WWW-Authenticate") != "" {
				t.Fatalf("malformed: reached=%v code=%d hdr=%q body=%s",
					reached, rec.Code, rec.Header().Get("WWW-Authenticate"), rec.Body.String())
			}
		})
	}
}
