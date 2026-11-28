package main

// main_audit_audience_1596_test.go — #1596: a token that VERIFIED but was
// issued for other recipients is refused 401 TokenAudienceMismatch, and the
// audited route writes exactly ONE row: denied, reason audience_mismatch
// (audit v0.17.0), no status code, naming the verified sender and its signed
// class. An accepted token (no aud, ours, ours among others) writes no such
// row. Through the real route table and both validating middlewares:
// UserConfig (POST /call, GET /export) and RefreshAuth (the debug lane).

import (
	"log/slog"
	"net/http"
	"testing"
	"time"

	krateoaudit "github.com/krateo-platformops/audit"
	"github.com/krateo-platformops/audit/audittest"
	"github.com/krateo-platformops/plumbing/jwtutil"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/support/audit"
)

const aud1596 = "snowplow.krateo-system"

func aud1596Attrs(t *testing.T, rec *audittest.Recorder) []map[string]string {
	t.Helper()
	var out []map[string]string
	for _, r := range rec.Records() {
		a := map[string]string{}
		for _, kv := range audittest.Attributes(r) {
			a[string(kv.Key)] = kv.Value.Emit()
		}
		out = append(out, a)
	}
	return out
}

func aud1596Refused(a map[string]string) bool {
	return a[krateoaudit.KeyDenialReason] == audit.DenialAudienceMismatch
}

func TestAudit_Audience1596_UserConfigRefusalIsOneRow(t *testing.T) {
	for _, route := range []struct{ name, method, target, body, action string }{
		{"POST /call", http.MethodPost, "/call?" + e2eCMQuery, e2eCM, "call"},
		{"GET /export", http.MethodGet, "/export?format=json&" + e2eCMQuery, "", "export"},
	} {
		for _, tc := range []struct {
			name    string
			aud     []string
			refused bool
		}{
			{"only others", []string{"agentiko.krateo-system", "other.ns"}, true},
			{"no aud", nil, false},
			{"exactly ours", []string{aud1596}, false},
			{"ours among others", []string{"other.ns", aud1596}, false},
		} {
			t.Run(route.name+", "+tc.name, func(t *testing.T) {
				login := e2eLogin{groups: []string{"devs"}, seed: []string{credsecret.SessionNames(e2eUser)[0]},
					checkAudience: aud1596, tokenAudience: tc.aud}
				h, tok, rec := e2eEnvWith(t, login, nil)
				before := audit.DesignErrorCount("answered")
				code := e2eDo(h, tok, route.method, route.target, route.body)
				if d := audit.DesignErrorCount("answered") - before; d != 0 {
					t.Errorf("the design-error counter moved by %d", d)
				}
				if !tc.refused {
					if code == http.StatusUnauthorized {
						t.Fatalf("an accepted token was refused: %d", code)
					}
					for _, a := range aud1596Attrs(t, rec) {
						if aud1596Refused(a) {
							t.Fatalf("an accepted token wrote an audience_mismatch row")
						}
					}
					return
				}
				if code != http.StatusUnauthorized {
					t.Fatalf("answered %d, want 401", code)
				}
				a, body := e2eRow(t, rec.Records())
				for k, want := range map[string]string{
					krateoaudit.KeyAction:       route.action,
					krateoaudit.KeyOutcome:      krateoaudit.OutcomeDenied,
					krateoaudit.KeyDenialReason: "audience_mismatch",
					krateoaudit.KeyUserName:     e2eUser, // the verified sender, from the signed claims
					krateoaudit.KeyActorClass:   "human", // its signed principal
				} {
					if a[k] != want {
						t.Errorf("%s = %q, want %q", k, a[k], want)
					}
				}
				if got, ok := a[krateoaudit.KeyHTTPStatusCode]; ok {
					t.Errorf("http.response.status_code %q: no authority answered", got)
				}
				if body != audit.MessageRefused {
					t.Errorf("body %q, want %q", body, audit.MessageRefused)
				}
				// The row names no audience, ours or theirs.
				for k, v := range a {
					for _, aud := range append([]string{aud1596}, tc.aud...) {
						if v == aud {
							t.Errorf("%s names an audience", k)
						}
					}
				}
				audittest.AssertConformant(t, audit.Writer, rec.Records()[0], audittest.AllowIncomplete(krateoaudit.RuleTarget))
			})
		}
	}
}

// A token that does NOT verify (a bad signature) is refused 401 and never a row (no caller is marked verified),
// even with an aud that would mismatch.
func TestAudit_Audience1596_AnUnverifiedTokenIsNeverARow(t *testing.T) {
	login := e2eLogin{groups: []string{"devs"}, seed: []string{credsecret.SessionNames(e2eUser)[0]},
		checkAudience: aud1596, tokenAudience: []string{"other.ns"}}
	h, tok, rec := e2eEnvWith(t, login, nil)
	if code := e2eDo(h, tok[:len(tok)-4]+"AAAA", http.MethodPost, "/call?"+e2eCMQuery, e2eCM); code != http.StatusUnauthorized {
		t.Fatalf("answered %d, want 401", code)
	}
	if n := len(rec.Records()); n != 0 {
		t.Fatalf("%d rows for a token that did not verify, want none", n)
	}
}

// RefreshAuth, through the real debug lane (the audited POST
// /debug/shadow-parity): the same one row, before the RBAC gate is reached.
func TestAudit_Audience1596_RefreshAuthRefusalIsOneRow(t *testing.T) {
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	t.Setenv("RESOLVED_CACHE_ENABLED", "true")
	i1362Watcher(t, i1362World(t)...)
	audit.SetNamespaceReader(e2eNamespaces{})

	for _, tc := range []struct {
		name    string
		aud     []string
		refused bool
	}{
		{"only others", []string{"other.ns"}, true},
		{"no aud", nil, false},
		{"ours among others", []string{"other.ns", aud1596}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := audittest.NewRecorder(t)
			audit.SetDefault(audit.New(rec.Logger(audit.Scope)))
			t.Cleanup(func() { audit.SetDefault(nil) })
			mux := http.NewServeMux()
			registerDebugRoutes(newRouteTable(mux), newRequestChain(slog.New(slog.DiscardHandler)), debugAuthTestKeys,
				middleware.AudienceOptions(aud1596)...)
			tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
				Username: i1362Operator.user, Groups: i1362Operator.groups, Duration: time.Hour,
				KeyID: debugAuthTestKeyID, PrivateKey: debugAuthTestPrivateKey, Principal: jwtutil.PrincipalHuman,
				Audience: tc.aud,
			})
			if err != nil {
				t.Fatal(err)
			}
			w := i1362Drive(t, mux, http.MethodPost, handlers.ShadowParityPath+"?enabled=true", tok)
			rows := aud1596Attrs(t, rec)
			if !tc.refused {
				if w.Code == http.StatusUnauthorized {
					t.Fatalf("an accepted token was refused")
				}
				for _, a := range rows {
					if aud1596Refused(a) {
						t.Fatal("an accepted token wrote an audience_mismatch row")
					}
				}
				return
			}
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("answered %d, want 401: %s", w.Code, w.Body.String())
			}
			if len(rows) != 1 {
				t.Fatalf("%d rows, want exactly 1", len(rows))
			}
			a := rows[0]
			for k, want := range map[string]string{
				krateoaudit.KeyAction:       "debug.shadowParity",
				krateoaudit.KeyURLPath:      handlers.ShadowParityPath,
				krateoaudit.KeyOutcome:      krateoaudit.OutcomeDenied,
				krateoaudit.KeyDenialReason: "audience_mismatch",
				krateoaudit.KeyUserName:     i1362Operator.user,
				krateoaudit.KeyActorClass:   "human",
			} {
				if a[k] != want {
					t.Errorf("%s = %q, want %q", k, a[k], want)
				}
			}
			if _, ok := a[krateoaudit.KeyHTTPStatusCode]; ok {
				t.Error("a status code on a refusal no authority answered")
			}
			audittest.AssertConformant(t, audit.Writer, rec.Records()[0])
		})
	}
}
