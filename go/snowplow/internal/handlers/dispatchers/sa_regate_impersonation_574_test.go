package dispatchers

import (
	"context"
	"testing"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"k8s.io/client-go/rest"
)

// #574 — the SA re-gate must stand down on an impersonated dial.
//
// WHY THIS ARM LIVES IN dispatchers AND NOT IN internal/rbac: that package's
// TestMain refuses to run the whole package unless RBAC_TEST_ALLOW_DESTRUCTIVE=1
// (it creates and tears down CRDs). A test placed there reports "ok" having
// executed NOTHING — which is exactly what happened to the first version of this
// arm, and it survived a mutation that deleted the behaviour it was written to
// guard. dispatchers imports both cache and rbac and runs untagged.
func TestIssue574_RegateStandsDownOnImpersonatedDial(t *testing.T) {
	// A REAL narrowing subject is required, not just a background ctx: with no
	// UserInfo, ServesUnnarrowed returns true (sa_regate.go) and the gate never
	// fires at all — so an arm built on a bare ctx would be testing nothing.
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}}),
	)
	base := cache.WithInternalRESTConfig(
		cache.WithBackgroundResolve(ctx),
		&rest.Config{Host: "https://kubernetes.default.svc"},
	)

	// PRECONDITION, non-vacuous by construction: if this ctx were not re-gated
	// today, the arm below could pass against a gate that never fires at all.
	if !rbac.MustRegateSADial(base) {
		t.Fatal("precondition: a background SA dial under a real narrowing subject must be re-gated " +
			"BEFORE #574, or this arm cannot demonstrate any change in behaviour")
	}

	if rbac.MustRegateSADial(cache.WithImpersonatedDial(base)) {
		t.Error("an IMPERSONATED dial must NOT be re-gated. The gate exists because an SA dial is " +
			"BROADER than the ctx identity; under impersonation the apiserver evaluates exactly that " +
			"identity, which is strictly narrower. Leaving it armed makes #574 a no-op — the refresh " +
			"still fails closed and the 650-denials-per-15-minutes stays exactly where it was")
	}
}
