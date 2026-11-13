package credsecret

import (
	"slices"
	"testing"
)

// TestFor_SelectsByTheStepUpGroupOnly pins the #994 selection and the #1382
// order: the step-up names for groups carrying krateo:loa:2, the session names
// otherwise; the exact name first, then the legacy (DNS-1123) one where it
// differs. The expected names are literal fixtures, not computed, so a change
// in plumbing's derivation shows here.
func TestFor_SelectsByTheStepUpGroupOnly(t *testing.T) {
	cases := []struct {
		name     string
		username string
		groups   []string
		want     []string
	}{
		{"no groups", "alice", nil, []string{"alice-clientconfig"}},
		{"ordinary groups", "alice", []string{"devs", "krateo:loa:1"}, []string{"alice-clientconfig"}},
		{"step-up group", "alice", []string{"devs", StepUpGroup}, []string{"alice-clientconfig-stepup"}},
		{"step-up group alone", "alice", []string{StepUpGroup}, []string{"alice-clientconfig-stepup"}},
		{"a group that only contains the marker", "alice", []string{"x-krateo:loa:2"}, []string{"alice-clientconfig"}},
		{"dotted username: exact, then legacy", "john.doe", nil, []string{"john.doe-clientconfig", "johndoe-clientconfig"}},
		{"dotted username, step-up", "john.doe", []string{StepUpGroup}, []string{"john.doe-clientconfig-stepup", "johndoe-clientconfig-stepup"}},
		{"no exact name: legacy only", "Alice@Example.com", []string{StepUpGroup}, []string{"aliceexamplecom-clientconfig-stepup"}},
	}
	for _, c := range cases {
		if got := For(c.username, c.groups); !slices.Equal(got, c.want) {
			t.Errorf("%s: For(%q, %v) = %q, want %q", c.name, c.username, c.groups, got, c.want)
		}
	}
}

// TestNames_AreDisjoint is the collision authn's second contract change fixed:
// no username's session name is another username's step-up name, and the
// predicates classify each kind exactly, for exact and legacy names alike.
func TestNames_AreDisjoint(t *testing.T) {
	users := []string{"bob", "bob-stepup", "bob-clientconfig", "alice@example.com", "john.doe", "Mixed.Case"}
	session := map[string]bool{}
	for _, u := range users {
		for _, n := range SessionNames(u) {
			session[n] = true
		}
	}
	for _, u := range users {
		for _, su := range StepUpNames(u) {
			if session[su] {
				t.Errorf("step-up name %q of %q is also a session name", su, u)
			}
			if IsSessionName(su) || !IsStepUpName(su) || !IsPerUserName(su) {
				t.Errorf("step-up name %q: session=%v stepup=%v perUser=%v; want false,true,true",
					su, IsSessionName(su), IsStepUpName(su), IsPerUserName(su))
			}
		}
		for _, s := range SessionNames(u) {
			if !IsSessionName(s) || IsStepUpName(s) || !IsPerUserName(s) {
				t.Errorf("session name %q: session=%v stepup=%v perUser=%v; want true,false,true",
					s, IsSessionName(s), IsStepUpName(s), IsPerUserName(s))
			}
		}
		if got, want := len(AllNames(u)), len(SessionNames(u))+len(StepUpNames(u)); got != want {
			t.Errorf("AllNames(%q) has %d names, want %d", u, got, want)
		}
	}
	for _, other := range []string{"render-endpoint", "x-clientconfigs", "clientconfig-stepup-x"} {
		if IsPerUserName(other) {
			t.Errorf("IsPerUserName(%q) = true; not a per-user credential name", other)
		}
	}
}

// TestStepUpNames_PairWithSessionNames: each step-up name is its session
// name's step-up form, in the same order, so the exact-then-legacy order of
// the two kinds can never drift apart.
func TestStepUpNames_PairWithSessionNames(t *testing.T) {
	for _, u := range []string{"bob", "john.doe", "Alice@Example.com", "a.b.c"} {
		s, su := SessionNames(u), StepUpNames(u)
		if len(s) != len(su) || len(s) == 0 {
			t.Fatalf("%q: %d session names, %d step-up names", u, len(s), len(su))
		}
		for i := range s {
			if su[i] != s[i]+"-stepup" {
				t.Errorf("%q: step-up name %d is %q, want %q", u, i, su[i], s[i]+"-stepup")
			}
		}
	}
}
