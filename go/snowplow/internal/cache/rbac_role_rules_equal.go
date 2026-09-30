// rbac_role_rules_equal.go — #257 C4: the order-insensitive PolicyRule
// set-equality predicate.
//
// SECURITY-LOAD-BEARING. This predicate decides whether a Role/ClusterRole
// UPDATE's rules are UNCHANGED, and therefore whether the per-subject sub-gen
// bump (the L1 key rotation) for every bound subject can be SKIPPED. A wrong
// "equal" skips a rotation that a real rule change required → a stale grant or
// (worse) a stale REVOKE served under the old key → a LEAK. So it
// canonicalizes-then-compares ALL FIVE PolicyRule dimensions
// {verbs, apiGroups, resources, resourceNames, nonResourceURLs}, each
// order-insensitive (both the rule order and each rule's field-slice order are
// apiserver-unstable across writes).
//
// bump-when-in-doubt is the correct fail-safe bias (a spurious rotation is a
// perf cost; a missed grant/revoke is a leak). This PURE comparison cannot see
// the "in doubt" cases — an unparseable object, a mixed-kind event, a nil side —
// so the CALLER (the skip decision) treats those as NOT-equal (⇒ bump). Keeping
// the fail-safe at the call site keeps this function a total, side-effect-free
// equality on two rule slices.

package cache

import (
	"slices"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
)

// policyRulesEqual reports whether two PolicyRule sets are EQUAL
// order-insensitively. Multiset semantics: [r, r] and [r] are NOT equal.
func policyRulesEqual(a, b []rbacv1.PolicyRule) bool {
	if len(a) != len(b) {
		return false
	}
	return slices.Equal(canonRules(a), canonRules(b))
}

// canonRules renders each rule to its canonical string and sorts the multiset,
// so a permutation of the rule list compares equal.
func canonRules(rules []rbacv1.PolicyRule) []string {
	out := make([]string, 0, len(rules))
	for _, r := range rules {
		out = append(out, canonRule(r))
	}
	slices.Sort(out)
	return out
}

// canonRule renders one PolicyRule to a canonical string over ALL FIVE
// dimensions (C4), each field-slice cloned+sorted so a reordered-but-equal rule
// renders identically. TWO distinct C0 control chars are used so NO value can
// inject either level: \x1e (RS) separates the five DIMENSIONS, \x1f (US) joins
// items WITHIN a dimension (sortedJoin). Both are illegal / percent-encoded in
// every value class {verb, apiGroup, resource, resourceName, nonResourceURL} —
// critically, the intra-slice join is NOT "," because a comma is a legal RFC 3986
// sub-delim in a nonResourceURL path, so a comma-join would collide
// ["/a","/b"] with ["/a,/b"] and read a revoke as a no-op (a leak).
func canonRule(r rbacv1.PolicyRule) string {
	return sortedJoin(r.Verbs) + "\x1e" +
		sortedJoin(r.APIGroups) + "\x1e" +
		sortedJoin(r.Resources) + "\x1e" +
		sortedJoin(r.ResourceNames) + "\x1e" +
		sortedJoin(r.NonResourceURLs)
}

func sortedJoin(ss []string) string {
	c := slices.Clone(ss)
	slices.Sort(c)
	return strings.Join(c, "\x1f")
}
