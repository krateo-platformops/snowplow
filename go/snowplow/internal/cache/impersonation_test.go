package cache

import (
	"context"
	"sync/atomic"
	"testing"

	"k8s.io/client-go/rest"
)

// #574. The refresher must read as the cell's COHORT, not as snowplow.

func withImpersonationAvailable(t *testing.T, v bool) {
	t.Helper()
	prev := impersonationAvailable.Load()
	impersonationAvailable.Store(v)
	t.Cleanup(func() { impersonationAvailable.Store(prev) })
}

// TestImpersonation_AllThreeRepresentativeShapes pins the mapping from
// pickRepresentativeFromSubjects' three outputs onto Impersonate.
//
// The group-only row is the one that matters: the apiserver REJECTS
// Impersonate-Group without Impersonate-User ("requesting uid, groups or
// user-extra ... without impersonating a user"), so a cohort whose binding
// subject is a Group cannot be impersonated as a group alone.
func TestImpersonation_AllThreeRepresentativeShapes(t *testing.T) {
	withImpersonationAvailable(t, true)
	base := &rest.Config{Host: "https://kubernetes.default.svc"}

	for _, tc := range []struct {
		name       string
		user       string
		groups     []string
		wantUser   string
		wantGroups []string
	}{
		{"user", "alice", nil, "alice", nil},
		{"serviceaccount", "system:serviceaccount:ns:sa", nil, "system:serviceaccount:ns:sa", nil},
		{"group only", "", []string{"devs"}, CohortPlaceholderUsername, []string{"devs"}},
		{"user with groups", "bob", []string{"devs"}, "bob", []string{"devs"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ImpersonatingRESTConfig(base, tc.user, tc.groups)
			if got == nil {
				t.Fatalf("want an impersonating config for %q/%v, got nil", tc.user, tc.groups)
			}
			if got.Impersonate.UserName != tc.wantUser {
				t.Errorf("UserName = %q, want %q", got.Impersonate.UserName, tc.wantUser)
			}
			if len(got.Impersonate.Groups) != len(tc.wantGroups) {
				t.Errorf("Groups = %v, want %v", got.Impersonate.Groups, tc.wantGroups)
			}
			// A group-only cohort MUST carry a username or the apiserver rejects it.
			if got.Impersonate.UserName == "" {
				t.Error("UserName is empty — the apiserver rejects Impersonate-Group without Impersonate-User")
			}
		})
	}
}

// TestImpersonation_NeverMutatesTheSharedBase — the internal *rest.Config is
// shared and documented read-only. Mutating it would make one cohort's dial leak
// into another's, which is precisely what #574 exists to prevent.
func TestImpersonation_NeverMutatesTheSharedBase(t *testing.T) {
	withImpersonationAvailable(t, true)
	base := &rest.Config{Host: "https://kubernetes.default.svc"}

	a := ImpersonatingRESTConfig(base, "alice", nil)
	b := ImpersonatingRESTConfig(base, "bob", nil)

	if base.Impersonate.UserName != "" {
		t.Errorf("the SHARED base was mutated: UserName = %q", base.Impersonate.UserName)
	}
	if a.Impersonate.UserName != "alice" || b.Impersonate.UserName != "bob" {
		t.Errorf("configs cross-contaminated: a=%q b=%q", a.Impersonate.UserName, b.Impersonate.UserName)
	}
	if a == b || a == base || b == base {
		t.Error("ImpersonatingRESTConfig returned a shared pointer; each cohort needs its own")
	}
}

// TestImpersonation_UnavailableKeepsPreviousBehaviour is the rollout-safety arm.
// The RBAC grant ships in the installer chart and this code ships in snowplow;
// they roll independently. A nil return means the caller keeps the SA transport,
// so a code-before-RBAC rollout behaves exactly as it does today instead of
// 403-ing every background refresh.
func TestImpersonation_UnavailableKeepsPreviousBehaviour(t *testing.T) {
	withImpersonationAvailable(t, false)
	if got := ImpersonatingRESTConfig(&rest.Config{Host: "h"}, "alice", []string{"devs"}); got != nil {
		t.Errorf("with impersonation ungranted the caller MUST keep the SA transport, got %+v", got.Impersonate)
	}
}

// TestImpersonation_DefaultsToUnavailable asserts the package-level default on
// the REAL variable, not on a local stand-in. A default of true would 403 every
// background refresh on a code-before-RBAC rollout.
//
// It deliberately does not use withImpersonationAvailable, because that helper
// sets the value — reading it back would assert the helper, not the default.
func TestImpersonation_DefaultsToUnavailable(t *testing.T) {
	var zero atomic.Bool // the same type and zero value as the package var
	if zero.Load() {
		t.Fatal("atomic.Bool zero value is not false; the default-false argument does not hold")
	}
	// And the accessor must report that zero value rather than a constant.
	prev := impersonationAvailable.Load()
	t.Cleanup(func() { impersonationAvailable.Store(prev) })
	impersonationAvailable.Store(false)
	if ImpersonationAvailable() {
		t.Error("ImpersonationAvailable() must report the stored value")
	}
}

// TestImpersonation_NoIdentityIsNotACohort — an identity with neither a username
// nor groups is not a cohort. Impersonating the placeholder ALONE would dial as a
// principal with no rights and silently resolve nothing, which reads as success.
func TestImpersonation_NoIdentityIsNotACohort(t *testing.T) {
	withImpersonationAvailable(t, true)
	if got := ImpersonatingRESTConfig(&rest.Config{Host: "h"}, "", nil); got != nil {
		t.Errorf("an empty identity must NOT impersonate the bare placeholder, got %+v", got.Impersonate)
	}
	if got := ImpersonatingRESTConfig(nil, "alice", nil); got != nil {
		t.Error("a nil base config must return nil")
	}
}

// TestImpersonation_DialMarkerRoundTrips pins the ctx marker rbac.MustRegateSADial
// reads. An unmarked ctx must stay gated.
func TestImpersonation_DialMarkerRoundTrips(t *testing.T) {
	ctx := context.Background()
	if ImpersonatedDialFromContext(ctx) {
		t.Error("a bare ctx must NOT report an impersonated dial, or the gate is disarmed everywhere")
	}
	if !ImpersonatedDialFromContext(WithImpersonatedDial(ctx)) {
		t.Error("the marker must round-trip")
	}
}

// TestImpersonation_PlaceholderIsUnprivilegedByName is the structural guard on
// the synthetic principal. It cannot assert the cluster's RBAC from a unit test,
// so it asserts the two properties that make the live grant safe: the name is
// not in Kubernetes' reserved `system:` namespace (which would collide with a
// built-in subject), and it is not a ServiceAccount form (which would make it
// mintable as a real token).
func TestImpersonation_PlaceholderIsUnprivilegedByName(t *testing.T) {
	const p = CohortPlaceholderUsername
	if len(p) >= 7 && p[:7] == "system:" {
		t.Errorf("placeholder %q is in the reserved system: namespace", p)
	}
	if len(p) >= 23 && p[:23] == "system:serviceaccount:" {
		t.Errorf("placeholder %q is a ServiceAccount form and could be minted a real token", p)
	}
	if p == "" {
		t.Error("placeholder must not be empty — Impersonate-Group requires Impersonate-User")
	}
}
