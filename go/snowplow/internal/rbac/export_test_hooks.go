package rbac

import (
	"context"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/snowplow/internal/cache"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/client-go/kubernetes"
)

// export_for_test.go — Ship 1.1 S1 drift-guard hooks.
//
// Exposes the SA-username parser + the synthetic-SA-group expansion to the
// cross-package parity test in internal/rbac/evaltest, which asserts the
// cache/ package's byte-faithful COPIES (cache.ParseCohortSAUsernameForTest /
// cache.CohortEffectiveGroupsForTest) stay equal to these ORIGINALS. Cache/
// cannot import rbac/ (cycle), so it carries copies; this guard fails the
// moment a copy silently re-diverges from the original — the exact bug class
// Ship 1.1 fixed (CohortNSACL had drifted from EvaluateRBAC on the SA path).
//
// Test-only: these are thin pass-throughs to the unexported originals. They
// add zero production surface (the funcs are never called by non-test code).

// ParseServiceAccountUsernameForTest exposes parseServiceAccountUsername.
func ParseServiceAccountUsernameForTest(u string) (string, string, bool) {
	return parseServiceAccountUsername(u)
}

// EffectiveGroupsForTest exposes effectiveGroups, adapted to a (groups,
// isSA, saNS) signature so the parity test can compare it directly against
// cache.CohortEffectiveGroupsForTest. It constructs the EvaluateOptions the
// original reads (only Groups is consulted by effectiveGroups).
func EffectiveGroupsForTest(groups []string, isSA bool, saNS string) []string {
	return effectiveGroups(EvaluateOptions{Groups: groups}, isSA, saNS)
}

// CanonicalGroupsHashForTest exposes canonicalGroupsHash (groups_hash.go)
// to the cross-package M9 collision/order/purity falsifier in evaltest.
// The hasher is the single source of truth for the memo key's GroupsHash
// dimension; the falsifier asserts its contract directly (length-prefix
// framing defeats the ["a","bc"]/["ab","c"] alias; order independence;
// nil==empty!=[""]; caller slice unmutated) WITHOUT routing through
// EvaluateRBAC's observable behaviour, so a regression in the hasher is
// caught at the unit boundary. Thin pass-through; zero production surface.
func CanonicalGroupsHashForTest(groups []string) uint64 {
	return canonicalGroupsHash(groups)
}

// SelectCRBCandidatesForTest exposes selectCRBCandidates to the
// cross-package v7 Step 2A falsifiers in internal/rbac/evaltest (F-C0
// behavior-preservation). Thin pass-through; zero production surface.
func SelectCRBCandidatesForTest(snap *cache.RBACSnapshot, opts EvaluateOptions) []*rbacv1.ClusterRoleBinding {
	return selectCRBCandidates(snap, opts)
}

// SelectRBCandidatesForTest exposes the per-namespace selectRBCandidates
// to the v7 Step 2A falsifiers (F-C0 ordered-output behavior-preservation
// across the routeRBSubjects extraction; F-C1 per-ns union reference).
// Thin pass-through; zero production surface.
func SelectRBCandidatesForTest(snap *cache.RBACSnapshot, ns string, opts EvaluateOptions) []*rbacv1.RoleBinding {
	return selectRBCandidates(snap, ns, opts)
}

// SelectRBCandidatesAllNSForTest exposes the all-namespace
// selectRBCandidatesAllNS to the v7 Step 2A F-C1 union-equivalence falsifier.
// Thin pass-through; zero production surface. It also makes
// selectRBCandidatesAllNS a used symbol for the unused-symbol linter — no
// serving path calls it in Step A (its first real consumer is the Step B
// requester-profile builder).
func SelectRBCandidatesAllNSForTest(snap *cache.RBACSnapshot, opts EvaluateOptions) []*rbacv1.RoleBinding {
	return selectRBCandidatesAllNS(snap, opts)
}

// LookupRoleRefRulesForTest exposes the PURE, side-effect-free
// lookupRoleRefRules (evaluate.go) to the v7 Step 2B F-D1 dark-purity /
// resolve-fidelity falsifiers. It is the exact resolve half the requester
// profile uses; the falsifier asserts calling it records NO snapshot miss.
// Thin pass-through; zero production surface.
func LookupRoleRefRulesForTest(snap *cache.RBACSnapshot, namespace string, ref rbacv1.RoleRef) ([]rbacv1.PolicyRule, bool) {
	return lookupRoleRefRules(snap, namespace, ref)
}

// RoleRefPermitsForTest exposes roleRefPermits (the served-path resolve +
// rulesPermit + miss-accounting) to the F-D1 behavior-preserving falsifier,
// which asserts the miss bump stayed at this call site with the pre-split
// selectivity. log is unused by roleRefPermits (reserved), so nil is safe.
// Thin pass-through; zero production surface.
func RoleRefPermitsForTest(snap *cache.RBACSnapshot, namespace string, ref rbacv1.RoleRef, opts EvaluateOptions) (bool, error) {
	return roleRefPermits(snap, namespace, ref, opts, nil)
}

// ResetRequesterProfileMemoForTest clears the v7 Step 2B requester-profile memo
// singleton and its counters. TEST-ONLY (F-D7) — production code MUST NOT call
// it. Thin pass-through; zero production surface.
func ResetRequesterProfileMemoForTest() {
	resetRequesterProfileMemo()
}

// RequesterProfileMemoStatsForTest returns (hits, misses, stores, refused,
// entries) for the requester-profile memo. TEST-ONLY (F-D7). The memo emits no
// expvar/log (it is dark); this is its only observable surface, for tests.
func RequesterProfileMemoStatsForTest() (hits, misses, stores, refused uint64, entries int) {
	return requesterProfileMemoStats()
}

// SetSARClientsetForTest overrides the SAR-baseline clientset factory
// (rbac.go's sarClientsetForEndpoint) so the hermetic cache=off L5 test
// can inject a fake authorization clientset instead of building a real
// one from the user endpoint. Returns a restore func; production code
// MUST NOT call it. The factory receives the endpoint the SAR path
// resolved from ctx and returns the kubernetes.Interface whose
// AuthorizationV1().SelfSubjectAccessReviews() the SAR path calls.
func SetSARClientsetForTest(f func(ctx context.Context, ep endpoints.Endpoint) (kubernetes.Interface, error)) (restore func()) {
	prev := sarClientsetForEndpoint
	sarClientsetForEndpoint = f
	return func() { sarClientsetForEndpoint = prev }
}
