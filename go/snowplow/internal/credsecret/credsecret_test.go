package credsecret

import "testing"

// TestFor_SelectsByTheStepUpGroupOnly pins the #994 selection: the step-up
// Secret for groups carrying krateo:loa:2, the session Secret otherwise, both
// built from the username's DNS-1123 form (authn's storage naming).
func TestFor_SelectsByTheStepUpGroupOnly(t *testing.T) {
	cases := []struct {
		name     string
		username string
		groups   []string
		want     string
	}{
		{"no groups", "alice", nil, "alice-clientconfig"},
		{"ordinary groups", "alice", []string{"devs", "krateo:loa:1"}, "alice-clientconfig"},
		{"step-up group", "alice", []string{"devs", StepUpGroup}, "alice-clientconfig-stepup"},
		{"step-up group alone", "alice", []string{StepUpGroup}, "alice-clientconfig-stepup"},
		{"a group that only contains the marker", "alice", []string{"x-krateo:loa:2"}, "alice-clientconfig"},
		{"dns1123 username (authn: kubeutil.MakeDNS1123Compatible)", "Alice@Example.com", []string{StepUpGroup}, "aliceexamplecom-clientconfig-stepup"},
	}
	for _, c := range cases {
		if got := For(c.username, c.groups); got != c.want {
			t.Errorf("%s: For(%q, %v) = %q, want %q", c.name, c.username, c.groups, got, c.want)
		}
	}
}

// TestNames_AreDisjoint is the collision authn's second contract change fixed:
// no username's session name is another username's step-up name, and the
// predicates classify each kind exactly.
func TestNames_AreDisjoint(t *testing.T) {
	users := []string{"bob", "bob-stepup", "bob-clientconfig", "alice@example.com"}
	session := map[string]bool{}
	for _, u := range users {
		session[SessionName(u)] = true
	}
	for _, u := range users {
		su := StepUpName(u)
		if session[su] {
			t.Errorf("step-up name %q of %q is also a session name", su, u)
		}
		if IsSessionName(su) || !IsStepUpName(su) || !IsPerUserName(su) {
			t.Errorf("step-up name %q: session=%v stepup=%v perUser=%v; want false,true,true",
				su, IsSessionName(su), IsStepUpName(su), IsPerUserName(su))
		}
		s := SessionName(u)
		if !IsSessionName(s) || IsStepUpName(s) || !IsPerUserName(s) {
			t.Errorf("session name %q: session=%v stepup=%v perUser=%v; want true,false,true",
				s, IsSessionName(s), IsStepUpName(s), IsPerUserName(s))
		}
	}
	for _, other := range []string{"render-endpoint", "x-clientconfigs", "clientconfig-stepup-x"} {
		if IsPerUserName(other) {
			t.Errorf("IsPerUserName(%q) = true; not a per-user credential name", other)
		}
	}
}
