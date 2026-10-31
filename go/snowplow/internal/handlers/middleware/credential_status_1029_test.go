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
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/krateo-platformops/plumbing/http/response"
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
	if got := rec.Header().Get("Content-Type"); got != want.Header().Get("Content-Type") {
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
	if got := rec.Header().Values("WWW-Authenticate"); len(got) != 1 || got[0] != c1029Challenge {
		t.Fatalf("WWW-Authenticate = %q, want exactly [%q]", got, c1029Challenge)
	}
	if strings.Contains(rec.Header().Get("WWW-Authenticate"), "acr_values") {
		t.Fatal("the challenge must not carry acr_values (krateo:loa:2 is not an IdP ACR value)")
	}
	if !strings.Contains(b.Message, "not found") {
		t.Fatalf("message must stay the re-read's NotFound text; got %q", b.Message)
	}
	c1029AssertPlumbingCompatible(t, rec, b)
}

// Any other token whose session Secret is missing: 401, cause
// SessionCredentialMissing, never StepUpRequired, and no challenge — even with
// a step-up Secret for the same user sitting in the informer.
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
	if got := rec.Header().Values("WWW-Authenticate"); len(got) != 0 {
		t.Fatalf("the session case must send no WWW-Authenticate challenge; got %q", got)
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

// c1029DocBlocks returns the bodies and header lines of the doc's ```http
// examples (sections 2a and 2b), in order.
func c1029DocBlocks(t *testing.T) (bodies []string, headers [][]string) {
	t.Helper()
	b, err := os.ReadFile("../../../docs/howto-frontend-credential-401-causes.md")
	if err != nil {
		t.Fatal(err)
	}
	for _, blk := range strings.Split(string(b), "```http\n")[1:] {
		blk = blk[:strings.Index(blk, "```")]
		var hs []string
		for _, ln := range strings.Split(blk, "\n") {
			if strings.HasPrefix(ln, "{") {
				bodies = append(bodies, ln)
			} else if strings.Contains(ln, ": ") {
				hs = append(hs, ln)
			}
		}
		headers = append(headers, hs)
	}
	return
}

// R-DOC — the doc's examples are the wire, byte for byte, once the doc's
// example user is substituted.
func TestCredentialStatus1029_DocExamplesEqualTheWire(t *testing.T) {
	bodies, headers := c1029DocBlocks(t)
	if len(bodies) != 2 || len(headers) != 2 {
		t.Fatalf("expected 2 doc examples, got %d bodies %d header sets", len(bodies), len(headers))
	}
	sub := func(s string) string { return strings.ReplaceAll(s, "alice", su994User) }

	t.Run("2a", func(t *testing.T) {
		session := su994Secret(credsecret.SessionName(su994User), su994Session)
		su994Informer(t, session)
		su994APIServer(t, session)
		rec, _ := c1029Serve(t, su994Token(t, su994Group, credsecret.StepUpGroup))
		if got, want := rec.Body.String(), sub(bodies[0])+"\n"; got != want {
			t.Fatalf("2a body differs from doc:\n got  %q\n want %q", got, want)
		}
		if len(headers[0]) != 2 {
			t.Fatalf("2a doc headers: %q", headers[0])
		}
		for _, h := range headers[0] {
			k, v, _ := strings.Cut(h, ": ")
			if rec.Header().Get(k) != v {
				t.Fatalf("2a header %q: doc %q wire %q", k, v, rec.Header().Get(k))
			}
		}
		if rec.Code != http.StatusUnauthorized || len(rec.Header()) != 2 {
			t.Fatalf("2a code=%d headers=%v", rec.Code, rec.Header())
		}
	})
	t.Run("2b", func(t *testing.T) {
		stepUp := su994Secret(credsecret.StepUpName(su994User), su994StepUp)
		su994Informer(t, stepUp)
		su994APIServer(t, stepUp)
		rec, _ := c1029Serve(t, su994Token(t, su994Group))
		if got, want := rec.Body.String(), sub(bodies[1])+"\n"; got != want {
			t.Fatalf("2b body differs from doc:\n got  %q\n want %q", got, want)
		}
		if len(headers[1]) != 1 {
			t.Fatalf("2b doc headers: %q", headers[1])
		}
		if rec.Code != http.StatusUnauthorized || len(rec.Header()) != 1 || rec.Header().Get("Content-Type") != "application/json" {
			t.Fatalf("2b code=%d headers=%v", rec.Code, rec.Header())
		}
	})
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
				rec.Header().Get("WWW-Authenticate") != "" {
				t.Fatalf("group %q: want the session cause; code=%d hdr=%q body=%s",
					g, rec.Code, rec.Header().Get("WWW-Authenticate"), rec.Body.String())
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
