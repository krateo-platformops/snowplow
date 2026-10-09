// impersonation.go — #574: the background refresher reads as the cell's COHORT,
// not as snowplow.
//
// THE PROBLEM THIS SOLVES. A refresh re-resolves a cell under that cell's
// representative identity (resolve_populate.go installs it via WithUserInfo) but
// dials the apiserver with snowplow's own ServiceAccount transport. Identity says
// cohort X, transport says snowplow. rbac.MustRegateSADial exists ONLY to stop
// that mismatch caching SA-breadth rows under a narrow cohort key — it is a
// symptom, not a policy, and it costs ~650 refused refreshes per 15 minutes on
// krateo-057.
//
// WHY IMPERSONATION NARROWS RATHER THAN WIDENS. snowplow's SA can already `get`
// AND `list` secrets cluster-wide (measured on 057). So the refresher reads today
// with a credential that can read anything. Reading as the cohort is STRICTLY LESS
// privilege on the refresh path. The only capability added is the `impersonate`
// verb itself.
//
// WHY THE GRANT IS SAFE TO BOUND. Impersonating a user does NOT inherit that
// user's group memberships — the apiserver uses exactly the groups supplied.
// Verified on 057 against a User CR carrying groups:["admins"], where admins is
// bound to cluster-admin:
//
//	--as=admin                      list secrets -> no
//	--as=admin --as-group=admins    list secrets -> yes
//
// So `impersonate: users` cannot escalate and may be granted broadly (1000 users
// cannot be enumerated), while ALL escalation risk is confined to
// `impersonate: groups`, which the installer's ClusterRole allowlists by
// resourceNames and which MUST NOT include a privileged group.
//
// THE ALLOWLIST IS NOT HERE, DELIBERATELY. snowplow holds no list. It always
// impersonates the representative; whether that is permitted is the apiserver's
// decision against the shipped ClusterRole. A cohort whose group is absent from
// resourceNames gets a 403, the Put declines exactly as it does today, and the
// cell expires at TTL and refills on read. That keeps "who may be refreshed" a
// reviewable RBAC change rather than a constant in this package, and leaves
// nothing to drift out of step.
package cache

import (
	"context"
	"sync/atomic"

	"k8s.io/client-go/rest"
)

// CohortPlaceholderUsername stands in for a cohort whose binding subject is a
// GROUP and which therefore has no username of its own
// (pickRepresentativeFromSubjects returns SubjectIdentity{Groups: ...}).
//
// IT EXISTS BECAUSE THE APISERVER REQUIRES IT: Impersonate-Group is rejected
// without Impersonate-User —
//
//	"requesting uid, groups or user-extra ... without impersonating a user"
//
// so a group-only cohort cannot be impersonated as a group alone.
//
// IT MUST HAVE NO RBAC OF ITS OWN. Verified on krateo-057: a principal with no
// bindings answers "no" to pods, secrets and compositiondefinitions, and
// placeholder+group reproduces a real group member's answers exactly. Its only
// unavoidable addition is the system:authenticated group the apiserver attaches
// to any authenticated request, which on a stock cluster is system:basic-user,
// system:discovery and system:public-info-viewer — self-SAR, discovery and
// version info.
//
// The `snowplow:` prefix is deliberate: `system:` is reserved by Kubernetes, and
// a prefix no real authenticator mints keeps this principal unforgeable from
// outside. TestImpersonation_PlaceholderHasNoBindings is the structural guard
// that a future RoleBinding to it is caught.
const CohortPlaceholderUsername = "snowplow:cohort-placeholder"

// impersonationAvailable records whether the apiserver will accept an
// impersonated dial from snowplow's ServiceAccount. It is probed ONCE at startup
// (a SelfSubjectAccessReview) rather than inferred from a failed read, because
// inferring it would mean discovering the answer separately on every cohort.
//
// DEFAULT FALSE IS LOAD-BEARING. The RBAC grant ships in the installer chart and
// the code ships in snowplow; they roll independently. If the code lands first,
// every impersonated read would 403 and background refresh would stop ENTIRELY —
// strictly worse than the partial failure it replaces. Defaulting to false means
// a code-only rollout behaves exactly as today.
var impersonationAvailable atomic.Bool

// SetImpersonationAvailable records the startup probe's answer. Called once from
// main; a false value keeps the pre-#574 behaviour.
func SetImpersonationAvailable(v bool) { impersonationAvailable.Store(v) }

// ImpersonationAvailable reports the probe's answer.
func ImpersonationAvailable() bool { return impersonationAvailable.Load() }

type impersonatedDialKey struct{}

// WithImpersonatedDial marks a ctx whose apiserver transport impersonates the
// cell's representative. rbac.MustRegateSADial reads it: the re-gate exists
// because an SA dial is BROADER than the ctx identity, and under impersonation
// it is exactly that identity, so the premise is gone. Any dial that is NOT
// marked keeps the gate.
func WithImpersonatedDial(ctx context.Context) context.Context {
	return context.WithValue(ctx, impersonatedDialKey{}, true)
}

// ImpersonatedDialFromContext reports whether the transport on ctx impersonates
// the ctx identity.
func ImpersonatedDialFromContext(ctx context.Context) bool {
	v, _ := ctx.Value(impersonatedDialKey{}).(bool)
	return v
}

// ImpersonatingRESTConfig returns a COPY of base that dials as (username,
// groups), or nil when impersonation is unavailable or the identity is not one
// that can be impersonated.
//
// A nil return means "caller must keep the existing SA behaviour", so the call
// site needs no capability check of its own.
//
// COPY, NEVER MUTATE. The internal *rest.Config is shared and documented
// read-only (internal_client.go). rest.CopyConfig gives a deep-enough copy that
// setting Impersonate cannot leak into another cohort's dial — the bug this
// would otherwise have is one cohort reading as another, which is exactly what
// #574 exists to prevent.
//
// THE THREE REPRESENTATIVE SHAPES, from pickRepresentativeFromSubjects:
//
//	{Username: alice}                        -> UserName: alice
//	{Username: system:serviceaccount:ns:sa}  -> UserName: system:serviceaccount:ns:sa
//	{Groups: [devs]}  (no username)          -> UserName: placeholder, Groups: [devs]
//
// An identity with NEITHER a username nor groups is not a cohort and returns nil
// rather than impersonating the placeholder alone, which would dial as a
// principal with no rights and silently resolve nothing.
func ImpersonatingRESTConfig(base *rest.Config, username string, groups []string) *rest.Config {
	if base == nil || !impersonationAvailable.Load() {
		return nil
	}
	if username == "" && len(groups) == 0 {
		return nil
	}
	user := username
	if user == "" {
		// Group-only cohort: the apiserver rejects Impersonate-Group without
		// Impersonate-User, so stand a principal with no rights in front of it.
		user = CohortPlaceholderUsername
	}
	cfg := rest.CopyConfig(base)
	cfg.Impersonate = rest.ImpersonationConfig{
		UserName: user,
		Groups:   groups,
	}
	return cfg
}
