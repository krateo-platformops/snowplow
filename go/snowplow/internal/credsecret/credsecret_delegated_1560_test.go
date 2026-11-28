package credsecret

// credsecret_delegated_1560_test.go — #1560: the delegated credential
// (authn#167) in the one selector.
//
//   - a token with `act` selects endpoints.DelegatedSecretName(username,
//     outermost act.sub, groups): the groups name it, but never pick a kind
//     (no step-up branch);
//   - a token without `act` selects exactly what For selected before;
//   - Check maps every endpoints.CheckDelegatedCredential outcome, and a
//     label-less Secret (authn writes none on a delegated one) is held;
//   - the delegated name is reserved (#1025) and is neither a session nor a
//     step-up name, so learned identities' S1 (IsSessionName) never sees it;
//   - the log scrubber removes a delegated name, quoted or not.

import (
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	corev1 "k8s.io/api/core/v1"

	"github.com/krateo-platformops/snowplow/internal/redact"
	"github.com/krateo-platformops/snowplow/internal/testsupport/delegatedcred"
)

const (
	d1560User  = "d1560-erin"
	d1560Actor = "d1560-agent"
	d1560NS    = "krateo-system"
)

var d1560Groups = []string{"d1560-devs", "d1560-ops"}

func TestSelect1560_ActSelectsTheDelegatedCredential(t *testing.T) {
	cases := []struct {
		name string
		act  *jwtutil.Actor
		grps []string
	}{
		{"one actor", &jwtutil.Actor{Sub: d1560Actor}, d1560Groups},
		{"a chain: the outermost actor names it", &jwtutil.Actor{Sub: d1560Actor, Act: &jwtutil.Actor{Sub: "d1560-root"}}, d1560Groups},
		{"actor principal set", &jwtutil.Actor{Sub: d1560Actor, Principal: "agent"}, d1560Groups},
		{"a step-up group does not make it step-up", &jwtutil.Actor{Sub: d1560Actor}, append([]string{StepUpGroup}, d1560Groups...)},
		{"no groups", &jwtutil.Actor{Sub: d1560Actor}, nil},
	}
	for _, c := range cases {
		want := []string{endpoints.DelegatedSecretName(d1560User, d1560Actor, c.grps)}
		got := Select(jwtutil.UserInfo{Username: d1560User, Groups: c.grps, Act: c.act})
		if got.Kind != KindDelegated || !slices.Equal(got.Names, want) {
			t.Errorf("%s: Select = %v %q, want delegated %q", c.name, got.Kind, got.Names, want)
		}
		for _, n := range got.Names {
			if IsSessionName(n) || IsStepUpName(n) {
				t.Errorf("%s: delegated name %q reads as a session or step-up name", c.name, n)
			}
		}
	}
	// Another actor for the same person is another Secret: two actors never
	// share a credential.
	want := Select(jwtutil.UserInfo{Username: d1560User, Groups: d1560Groups, Act: &jwtutil.Actor{Sub: d1560Actor}}).Names
	other := Select(jwtutil.UserInfo{Username: d1560User, Groups: d1560Groups, Act: &jwtutil.Actor{Sub: d1560Actor + "-2"}})
	if other.Names[0] == want[0] {
		t.Fatal("two actors of one person select the same delegated Secret")
	}
}

// A token without `act` is unchanged by #1560: Select's names are For's, and
// its kind follows the step-up group alone.
func TestSelect1560_TokenWithoutActIsUnchanged(t *testing.T) {
	for _, u := range []string{"alice", "john.doe", "Alice@Example.com"} {
		for _, g := range [][]string{nil, {"devs"}, {"devs", StepUpGroup}, {"x-krateo:loa:2"}} {
			got := Select(jwtutil.UserInfo{Username: u, Groups: g})
			wantKind := KindSession
			if IsStepUp(g) {
				wantKind = KindStepUp
			}
			if got.Kind != wantKind || !slices.Equal(got.Names, For(u, g)) {
				t.Errorf("Select(%q, %v) = %v %q, want %v %q", u, g, got.Kind, got.Names, wantKind, For(u, g))
			}
		}
	}
}

// Check maps each outcome of endpoints.CheckDelegatedCredential.
func TestCheck1560_EveryOutcome(t *testing.T) {
	now := time.Now()
	cred := Select(jwtutil.UserInfo{Username: d1560User, Groups: d1560Groups, Act: &jwtutil.Actor{Sub: d1560Actor}})
	base := func(mut func(*delegatedcred.Spec)) delegatedcred.Spec {
		s := delegatedcred.Spec{Namespace: d1560NS, Username: d1560User, Actor: d1560Actor, Groups: d1560Groups,
			ServerURL: "https://k8s.d1560.example", Token: "tok"}
		if mut != nil {
			mut(&s)
		}
		return s
	}
	held := delegatedcred.Secret(t, base(nil))
	if len(held.Labels) != 0 {
		t.Fatalf("PRE: the fixture must carry no labels, as authn writes it; got %v", held.Labels)
	}
	reordered := delegatedcred.Secret(t, base(func(s *delegatedcred.Spec) { s.Groups = []string{d1560Groups[1], d1560Groups[0]} }))
	otherUserAnn := delegatedcred.Secret(t, base(nil))
	otherUserAnn.Annotations[endpoints.UsernameAnnotation] = "d1560-mallory"
	otherActorAnn := delegatedcred.Secret(t, base(nil))
	otherActorAnn.Annotations[endpoints.ActorAnnotation] = "d1560-other-agent"
	otherCN := delegatedcred.Secret(t, base(nil))
	otherCN.Data["client-certificate-data"], _ = delegatedcred.Certificate(t, "d1560-mallory", d1560Groups, now.Add(-time.Minute), now.Add(time.Hour))
	noCert := delegatedcred.Secret(t, base(nil))
	delete(noCert.Data, "client-certificate-data")
	expired := delegatedcred.Secret(t, base(func(s *delegatedcred.Spec) {
		s.NotBefore, s.NotAfter = now.Add(-20*time.Minute), now.Add(-10*time.Minute)
	}))
	notYet := delegatedcred.Secret(t, base(func(s *delegatedcred.Spec) {
		s.NotBefore, s.NotAfter = now.Add(5*time.Minute), now.Add(15*time.Minute)
	}))
	fewerGroups := delegatedcred.Secret(t, base(func(s *delegatedcred.Spec) { s.Groups = d1560Groups[:1] }))
	moreGroups := delegatedcred.Secret(t, base(func(s *delegatedcred.Spec) { s.Groups = append([]string{"d1560-admins"}, d1560Groups...) }))

	for _, c := range []struct {
		name string
		got  Outcome
		want Outcome
	}{
		{"held (label-less, as authn writes it)", cred.Check(held, now), Held},
		{"groups in another order", cred.Check(reordered, now), Held},
		{"absent", cred.Check(nil, now), NotOwned},
		{"username annotation names someone else", cred.Check(otherUserAnn, now), NotOwned},
		{"actor annotation names another actor", cred.Check(otherActorAnn, now), NotOwned},
		{"certificate CN is someone else", cred.Check(otherCN, now), NotOwned},
		{"no certificate", cred.Check(noCert, now), NotOwned},
		{"expired certificate", cred.Check(expired, now), Expired},
		{"certificate not yet valid", cred.Check(notYet, now), Expired},
		{"certificate has fewer groups than the token", cred.Check(fewerGroups, now), Mismatch},
		{"certificate has more groups than the token", cred.Check(moreGroups, now), Mismatch},
	} {
		if c.got != c.want {
			t.Errorf("%s: Check = %v, want %v", c.name, c.got, c.want)
		}
	}

	// A session token's Check is OwnsSecret, exactly as before: the delegated
	// Secret, annotated with the same username, does not make it read one
	// (Select never names it), and a session Secret it owns is held.
	sess := Select(jwtutil.UserInfo{Username: d1560User, Groups: d1560Groups})
	if slices.Contains(sess.Names, held.Name) {
		t.Fatal("a token without act names the delegated Secret")
	}
	owned := held.DeepCopy()
	owned.Name = sess.Names[0]
	if got := sess.Check(owned, now); got != Held {
		t.Fatalf("a session Secret owned by the user: Check = %v, want held", got)
	}
}

// #1025 — a delegated name is reserved: no named endpointRef may resolve to
// one. It is not a session name, so learned identities' S1 (which selects by
// IsSessionName) never reads it as a login.
func TestIsPerUserName1560_ReservesDelegatedNames(t *testing.T) {
	for _, pair := range [][2]string{{d1560User, d1560Actor}, {"alice", "agentiko"}, {"", ""}, {"a\x00b", "c"}} {
		n := endpoints.DelegatedSecretName(pair[0], pair[1], d1560Groups)
		if !IsPerUserName(n) || !IsDelegatedName(n) || IsSessionName(n) || IsStepUpName(n) {
			t.Errorf("%q: perUser=%v delegated=%v session=%v stepup=%v; want true,true,false,false",
				n, IsPerUserName(n), IsDelegatedName(n), IsSessionName(n), IsStepUpName(n))
		}
	}
	for _, other := range []string{"delegated-", "delegated-abc", "delegated-" + strings.Repeat("A", 64), "x-delegated-" + strings.Repeat("a", 64)} {
		if IsPerUserName(other) {
			t.Errorf("IsPerUserName(%q) = true; not a delegated name's shape", other)
		}
	}
}

// The delegated name is a guessable hash of (username, actor): the log
// scrubber removes it from error text, quoted (an apiserver NotFound, JSON
// escaped or not) or bare.
func TestRedact1560_ScrubsDelegatedNames(t *testing.T) {
	n := endpoints.DelegatedSecretName(d1560User, d1560Actor, d1560Groups)
	for _, s := range []string{
		`secrets "` + n + `" not found`,
		`{"message":"secrets \"` + n + `\" not found"}`,
		`GET /api/v1/namespaces/krateo-system/secrets/` + n + `: 403`,
		n,
	} {
		got := redact.ErrorText(s)
		if strings.Contains(got, n) || !strings.Contains(got, redact.Redacted) {
			t.Errorf("ErrorText(%q) = %q: the delegated name survives", s, got)
		}
	}
	// The no-quote fast path of the log handler must not skip a bare name.
	a := redact.ScrubLogAttr(nil, slog.String("err", "reading "+n+" failed"))
	if strings.Contains(a.Value.String(), n) {
		t.Fatalf("ScrubLogAttr kept a bare delegated name: %q", a.Value.String())
	}
	// Text with no name and no quote is returned untouched.
	if got := redact.ErrorText("delegated credentials are rotated"); got != "delegated credentials are rotated" {
		t.Fatalf("ErrorText changed text holding no name: %q", got)
	}
}

// #1596 - the groups are part of the name. One person with two tokens of
// different groups through one actor reads two different Secrets; the order of
// the groups and repeats in them don't change the name; the name keeps its
// shape; and the Secret proven the token's is the one named for its groups,
// while another token's is refused.
func TestSelect1596_TheTokensGroupsNameTheDelegatedCredential(t *testing.T) {
	sel := func(groups []string) Credential {
		return Select(jwtutil.UserInfo{Username: d1560User, Groups: groups, Act: &jwtutil.Actor{Sub: d1560Actor}})
	}
	a, b := sel([]string{"g-devs"}), sel([]string{"g-devs", "g-ops"})
	if a.Names[0] == b.Names[0] {
		t.Fatal("two tokens of one person and actor with different groups select the same delegated Secret")
	}
	base := sel([]string{"g-ops", "g-devs"}).Names[0]
	for name, g := range map[string][]string{
		"reordered":            {"g-devs", "g-ops"},
		"repeated":             {"g-devs", "g-ops", "g-devs", "g-ops"},
		"reordered + repeated": {"g-ops", "g-devs", "g-ops"},
	} {
		if got := sel(g).Names[0]; got != base {
			t.Errorf("%s: the name changed with the order or repeats of the groups", name)
		}
	}
	for _, c := range []Credential{a, b, sel(nil)} {
		n := c.Names[0]
		if len(n) != len("delegated-")+64 || !IsDelegatedName(n) || !IsPerUserName(n) {
			t.Errorf("%q: not delegated- and 64 hex", n)
		}
	}

	// CheckDelegatedCredential path: each token holds its own Secret, and is
	// refused the other's.
	now := time.Now()
	secretFor := func(groups []string) *corev1.Secret {
		return delegatedcred.Secret(t, delegatedcred.Spec{Namespace: d1560NS, Username: d1560User, Actor: d1560Actor,
			Groups: groups, ServerURL: "https://k8s.example", Token: "tok"})
	}
	sa, sb := secretFor([]string{"g-devs"}), secretFor([]string{"g-devs", "g-ops"})
	if sa.Name != a.Names[0] || sb.Name != b.Names[0] {
		t.Fatal("the fixture's Secret is not named as Select names it")
	}
	if got := a.Check(sa, now); got != Held {
		t.Errorf("a token's own Secret: Check = %v, want held", got)
	}
	if got := b.Check(sb, now); got != Held {
		t.Errorf("a token's own Secret: Check = %v, want held", got)
	}
	if got := a.Check(sb, now); got == Held {
		t.Error("a token read another token's Secret (other groups) as its own")
	}
	if got := b.Check(sa, now); got == Held {
		t.Error("a token read another token's Secret (other groups) as its own")
	}
}
