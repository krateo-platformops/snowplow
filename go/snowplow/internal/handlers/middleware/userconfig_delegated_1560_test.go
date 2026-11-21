// userconfig_delegated_1560_test.go — #1560: a verified token that carries
// `act` reads its DELEGATED credential (authn#167), never the person's session
// Secret, through the REAL middleware.UserConfig, the real Secrets informer
// (fake clientset, so the #510 transform runs) and the real apiserver re-read
// against a recording fake apiserver (the #994 harness).
//
//   - the delegated Secret is read from the informer, and on a miss from the
//     apiserver; the person's session Secret is right there and never used;
//   - an informer hit that is not held (a stale copy after a re-exchange) is
//     re-read from the apiserver exactly once, and served when that read is
//     held; refused with the mapped cause only when it is not;
//   - each outcome maps to its 401 cause: absent and not-owned are
//     DelegatedCredentialMissing, expired and mismatch are
//     DelegatedCredentialMismatch, with the invalid_token challenge;
//   - the Secret carries no labels, as authn writes it;
//   - a token without `act` reads exactly what it read before.
package middleware_test

import (
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	corev1 "k8s.io/api/core/v1"

	"github.com/krateo-platformops/snowplow/internal/credsecret"
	"github.com/krateo-platformops/snowplow/internal/testsupport/delegatedcred"
)

const (
	d1560Actor      = "d1560-agent"
	d1560Delegated  = "https://delegated.d1560.example/k8s"
	d1560Fresh      = "https://fresh.d1560.example/k8s"
	d1560OtherGroup = "d1560-other"
)

// d1560Token is a verified delegated token for su994User, acting through
// actor, with groups as issued.
func d1560Token(t *testing.T, actor string, groups ...string) string {
	t.Helper()
	tok, err := jwtutil.CreateToken(jwtutil.CreateTokenOptions{
		Username: su994User, Groups: groups, Duration: time.Hour,
		KeyID: testKeyID, PrivateKey: testPrivateKey,
		Act: &jwtutil.Actor{Sub: actor, Principal: "agent"},
	})
	if err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	return tok
}

// d1560Secret is su994User's delegated credential for actor, its certificate
// carrying groups, valid from nb to na (zero: valid now), served at url.
func d1560Secret(t *testing.T, actor, url string, groups []string, nb, na time.Time) *corev1.Secret {
	t.Helper()
	s := delegatedcred.Secret(t, delegatedcred.Spec{Namespace: su994SecretsNS, Username: su994User, Actor: actor,
		Groups: groups, NotBefore: nb, NotAfter: na, ServerURL: url, Token: "tok-delegated"})
	if len(s.Labels) != 0 {
		t.Fatalf("PRE: authn writes a delegated Secret with no labels; the fixture has %v", s.Labels)
	}
	return s
}

func d1560Name() string { return endpoints.DelegatedSecretName(su994User, d1560Actor) }

func d1560Cause(t *testing.T, body string) string {
	t.Helper()
	for _, c := range []string{"DelegatedCredentialMissing", "DelegatedCredentialMismatch", "SessionCredentialMissing", "StepUpRequired"} {
		if strings.Contains(body, `"reason":"`+c+`"`) {
			return c
		}
	}
	return ""
}

// The delegated Secret, from the informer: the session Secret of the same
// person is in the informer too and is never read.
func TestUserConfig1560_DelegatedTokenReadsTheDelegatedSecretFromTheInformer(t *testing.T) {
	su994Informer(t,
		su994Secret(credsecret.SessionNames(su994User)[0], su994Session),
		d1560Secret(t, d1560Actor, d1560Delegated, []string{su994Group}, time.Time{}, time.Time{}))
	asked := su994APIServer(t)

	res := su994Serve(t, d1560Token(t, d1560Actor, su994Group))
	if res.code != http.StatusOK || !res.reached {
		t.Fatalf("want 200 from the informer hit; code=%d body=%s", res.code, res.body)
	}
	if res.ep.ServerURL != d1560Delegated {
		t.Fatalf("a token with act must read its delegated credential: endpoint=%q, want %q (session %q)", res.ep.ServerURL, d1560Delegated, su994Session)
	}
	if res.user.Username != su994User || res.user.Act == nil || res.user.Act.Sub != d1560Actor {
		t.Fatalf("ctx identity = %+v, want the subject %q acting through %q", res.user, su994User, d1560Actor)
	}
	if got := asked(); len(got) != 0 {
		t.Fatalf("an informer hit must not ask the apiserver; asked %v", got)
	}
}

// The delegated Secret, from the apiserver on an informer miss: exactly one
// GET, for the delegated name only.
func TestUserConfig1560_DelegatedTokenReadsTheDelegatedSecretFromTheAPIServer(t *testing.T) {
	session := su994Secret(credsecret.SessionNames(su994User)[0], su994Session)
	su994Informer(t, session)
	asked := su994APIServer(t, session, d1560Secret(t, d1560Actor, d1560Delegated, []string{su994Group}, time.Time{}, time.Time{}))

	res := su994Serve(t, d1560Token(t, d1560Actor, su994Group))
	if res.code != http.StatusOK || res.ep.ServerURL != d1560Delegated {
		t.Fatalf("want 200 with the delegated credential; code=%d endpoint=%q body=%s", res.code, res.ep.ServerURL, res.body)
	}
	if got := asked(); !slices.Equal(got, []string{d1560Name()}) {
		t.Fatalf("the apiserver re-read asked for %v, want exactly one GET of the delegated name", got)
	}
}

// After a re-exchange the informer can still hold the previous version of the
// Secret. An informer hit that is not held is re-read from the apiserver ONCE;
// a held re-read is served (no 401), and a re-read that is not held either is
// refused with the cause of what the apiserver holds.
func TestUserConfig1560_StaleInformerCopyIsReReadOnce(t *testing.T) {
	now := time.Now()
	fresh := func() *corev1.Secret {
		return d1560Secret(t, d1560Actor, d1560Fresh, []string{su994Group}, time.Time{}, time.Time{})
	}
	staleGroups := func() *corev1.Secret {
		return d1560Secret(t, d1560Actor, d1560Delegated, []string{d1560OtherGroup}, time.Time{}, time.Time{})
	}
	staleExpired := func() *corev1.Secret {
		return d1560Secret(t, d1560Actor, d1560Delegated, []string{su994Group}, now.Add(-20*time.Minute), now.Add(-10*time.Minute))
	}
	for _, tc := range []struct {
		name      string
		informer  *corev1.Secret
		apiserver *corev1.Secret // nil: absent there
		wantURL   string         // "" : refused
		wantCause string
	}{
		{"stale groups in the informer, fresh on the apiserver", staleGroups(), fresh(), d1560Fresh, ""},
		{"expired in the informer, fresh on the apiserver", staleExpired(), fresh(), d1560Fresh, ""},
		{"both hold other groups", staleGroups(), staleGroups(), "", "DelegatedCredentialMismatch"},
		{"both expired", staleExpired(), staleExpired(), "", "DelegatedCredentialMismatch"},
		{"stale in the informer, gone from the apiserver", staleGroups(), nil, "", "DelegatedCredentialMissing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session := su994Secret(credsecret.SessionNames(su994User)[0], su994Session)
			su994Informer(t, session, tc.informer)
			have := []*corev1.Secret{session}
			if tc.apiserver != nil {
				have = append(have, tc.apiserver)
			}
			asked := su994APIServer(t, have...)

			res := su994Serve(t, d1560Token(t, d1560Actor, su994Group))
			if tc.wantURL != "" {
				if res.code != http.StatusOK || res.ep.ServerURL != tc.wantURL {
					t.Fatalf("want 200 with the apiserver's fresh credential %q; code=%d endpoint=%q body=%s", tc.wantURL, res.code, res.ep.ServerURL, res.body)
				}
			} else {
				if res.code != http.StatusUnauthorized || res.reached || d1560Cause(t, res.body) != tc.wantCause {
					t.Fatalf("want 401 %s without reaching the handler; code=%d reached=%v body=%s", tc.wantCause, res.code, res.reached, res.body)
				}
			}
			if got := asked(); !slices.Equal(got, []string{d1560Name()}) {
				t.Fatalf("the stale hit must be re-read exactly once, for the delegated name only; asked %v", got)
			}
		})
	}
}

// Each outcome of the check maps to its 401 cause, read from the apiserver
// (the informer is empty of delegated Secrets). The person's session Secret
// is present in both places and is never served instead.
func TestUserConfig1560_EachOutcomeMapsToItsCause(t *testing.T) {
	now := time.Now()
	notOwned := d1560Secret(t, d1560Actor, d1560Delegated, []string{su994Group}, time.Time{}, time.Time{})
	notOwned.Annotations[endpoints.ActorAnnotation] = "d1560-someone-else"
	for _, tc := range []struct {
		name  string
		have  *corev1.Secret
		cause string
	}{
		{"absent", nil, "DelegatedCredentialMissing"},
		{"not-owned (another actor's annotation)", notOwned, "DelegatedCredentialMissing"},
		{"expired", d1560Secret(t, d1560Actor, d1560Delegated, []string{su994Group}, now.Add(-20*time.Minute), now.Add(-10*time.Minute)), "DelegatedCredentialMismatch"},
		{"mismatch (other groups)", d1560Secret(t, d1560Actor, d1560Delegated, []string{d1560OtherGroup}, time.Time{}, time.Time{}), "DelegatedCredentialMismatch"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session := su994Secret(credsecret.SessionNames(su994User)[0], su994Session)
			su994Informer(t, session)
			have := []*corev1.Secret{session}
			if tc.have != nil {
				have = append(have, tc.have)
			}
			su994APIServer(t, have...)

			rec, reached := c1029Serve(t, d1560Token(t, d1560Actor, su994Group))
			if reached || rec.Code != http.StatusUnauthorized {
				t.Fatalf("want 401 without reaching the handler; code=%d reached=%v body=%s", rec.Code, reached, rec.Body.String())
			}
			b := c1029Decode(t, rec)
			if b.Details == nil || len(b.Details.Causes) != 1 || b.Details.Causes[0].Reason != tc.cause {
				t.Fatalf("want details.causes[0].reason=%s; body=%s", tc.cause, rec.Body.String())
			}
			if got := rec.Result().Header.Values("WWW-Authenticate"); len(got) != 1 || got[0] != `Bearer error="invalid_token"` {
				t.Fatalf("WWW-Authenticate = %q, want exactly the invalid_token challenge", got)
			}
			if m := b.Details.Causes[0].Message; strings.Contains(m, su994User) || strings.Contains(m, d1560Actor) || strings.Contains(m, "delegated-") {
				t.Fatalf("the cause message carries identity: %q", m)
			}
			c1029AssertPlumbingCompatible(t, rec, b)
		})
	}
}

// A token without `act` is unchanged: it reads the session Secret, even with
// the person's delegated Secret in the informer.
func TestUserConfig1560_TokenWithoutActIsUnchanged(t *testing.T) {
	su994Informer(t,
		su994Secret(credsecret.SessionNames(su994User)[0], su994Session),
		d1560Secret(t, d1560Actor, d1560Delegated, []string{su994Group}, time.Time{}, time.Time{}))
	asked := su994APIServer(t)

	res := su994Serve(t, su994Token(t, su994Group))
	if res.code != http.StatusOK || res.ep.ServerURL != su994Session {
		t.Fatalf("a token without act must read the session Secret; code=%d endpoint=%q body=%s", res.code, res.ep.ServerURL, res.body)
	}
	if got := asked(); len(got) != 0 {
		t.Fatalf("an informer hit must not ask the apiserver; asked %v", got)
	}
}
