// issue1334_effective_groups_test.go — #1334: WithAuthenticatedGroup is the
// apiserver's effective group set, and for every AUTHENTICATED identity it is
// byte-identical to the pre-#1334 function, so no key, digest, sub-gen fold or
// learned class key moves on rollout.
//
// The apiserver parity table (the same inputs through k8s.io/apiserver's own
// impersonation filter and group adder) lives in internal/rbac
// (issue1334_apiserver_group_parity_test.go), where the evaluator is reachable
// too.

package cache

import (
	"reflect"
	"sort"
	"strings"
	"testing"
)

// preIssue1334WithAuthenticatedGroup is a FROZEN copy of the function as it was
// on main 6a18a554 (rbac_shift_hook.go), before #1334. It is the byte-identity
// reference for authenticated identities only.
func preIssue1334WithAuthenticatedGroup(groups []string) []string {
	for _, g := range groups {
		if g == "system:authenticated" {
			return groups
		}
	}
	out := make([]string, 0, len(groups)+1)
	out = append(out, groups...)
	return append(out, "system:authenticated")
}

// authenticatedCorpus1334 is every authenticated identity shape snowplow mints a
// key for: a JWT user with and without groups, one already presenting
// system:authenticated (in any position), a ServiceAccount, and the prewarm
// group representative (Username == "", pickRepresentativeFromSubjects).
var authenticatedCorpus1334 = []struct {
	username string
	groups   []string
}{
	{"cyberjoker", nil},
	{"cyberjoker", []string{}},
	{"admin", []string{"system:masters"}},
	{"alice", []string{"devs", "portal", "krateo:viewers"}},
	{"bob", []string{"system:authenticated"}},
	{"bob", []string{"devs", "system:authenticated", "ops"}},
	{"system:serviceaccount:krateo-system:snowplow", nil},
	{"system:serviceaccount:ns1:sa1", []string{"system:serviceaccounts", "system:serviceaccounts:ns1"}},
	{"", []string{"devs"}},
	{"", []string{"system:authenticated"}},
	{"", nil},
	{"krateo:public", nil}, // the #1334 username WITHOUT system:unauthenticated is an ordinary authenticated user
}

func TestIssue1334_AuthenticatedIdentities_ByteIdenticalToPre1334(t *testing.T) {
	for _, id := range authenticatedCorpus1334 {
		old := preIssue1334WithAuthenticatedGroup(id.groups)
		got := WithAuthenticatedGroup(id.username, id.groups)
		// Exact slice equality: order, length and nil-ness (DeepEqual separates a
		// nil from an empty slice) — every fold downstream is a pure function of it.
		if !reflect.DeepEqual(got, old) {
			t.Errorf("(%q, %q): WithAuthenticatedGroup = %q, pre-#1334 = %q — an authenticated identity's key would move",
				id.username, id.groups, got, old)
		}
		// The sub-gen fold, the reseed predicates (Rotated, IdentityAuthorisedForGVR)
		// and the learned class key all derive from it.
		if a, b := foldSubjects(id.username, got), foldSubjects(id.username, old); !reflect.DeepEqual(a, b) {
			t.Errorf("(%q, %q): folded subjects moved: %v vs %v", id.username, id.groups, a, b)
		}
		oldClass := func() string {
			g := append([]string(nil), old...)
			sort.Strings(g)
			return id.username + "\x00" + strings.Join(g, "\x00")
		}()
		if k := LearnedClassKey(id.username, id.groups); k != oldClass {
			t.Errorf("(%q, %q): LearnedClassKey moved", id.username, id.groups)
		}
	}
}

// TestIssue1334_UnauthenticatedIdentity_NoAuthenticatedFold: an identity that
// presents system:unauthenticated folds no system:authenticated subject, so a
// rotation of a system:authenticated binding does not count as rotating it,
// and its learned class key differs from the same username's authenticated
// class.
func TestIssue1334_UnauthenticatedIdentity_NoAuthenticatedFold(t *testing.T) {
	pub, pubGroups := "krateo:public", []string{"system:unauthenticated"}
	if got := WithAuthenticatedGroup(pub, pubGroups); !reflect.DeepEqual(got, pubGroups) {
		t.Fatalf("WithAuthenticatedGroup(%q, %q) = %q, want the groups unchanged (apiserver impersonation.go:126-144)",
			pub, pubGroups, got)
	}
	rotated := RotatedSubjectSet{set: map[subjectKey]struct{}{
		{Kind: subjectKindGroup, Name: systemAuthenticatedGroup}: {},
	}}
	if rotated.Rotated(pub, pubGroups) {
		t.Errorf("a system:authenticated rotation must not rotate %q + system:unauthenticated (its key never folds it)", pub)
	}
	if !rotated.Rotated("alice", nil) {
		t.Errorf("control: a system:authenticated rotation must rotate an authenticated user")
	}
	if LearnedClassKey(pub, pubGroups) == LearnedClassKey(pub, nil) {
		t.Errorf("the unauthenticated and authenticated classes of %q must not share a learned class key", pub)
	}
	// system:anonymous: the apiserver adds system:unauthenticated, never
	// system:authenticated (impersonation.go:145-157).
	if got := WithAuthenticatedGroup("system:anonymous", nil); !reflect.DeepEqual(got, []string{"system:unauthenticated"}) {
		t.Errorf("WithAuthenticatedGroup(system:anonymous, nil) = %q, want [system:unauthenticated]", got)
	}
}
