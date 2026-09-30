// rbac_role_rules_equal_falsifier_257_test.go — #257 C4 canonicalization
// falsifier for policyRulesEqual. Per-dimension: each dimension REORDER compares
// EQUAL; each dimension CHANGE compares NOT-equal (its own bump arm). The
// resourceNames-narrowing arm is the leak trap C4 exists to guard.

package cache

import (
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
)

func c4Rule() rbacv1.PolicyRule {
	return rbacv1.PolicyRule{
		Verbs:           []string{"get", "list"},
		APIGroups:       []string{"", "apps"},
		Resources:       []string{"pods", "deployments"},
		ResourceNames:   []string{"alpha", "beta"},
		NonResourceURLs: []string{"/healthz", "/metrics"},
	}
}

// TestPolicyRulesEqual_C4_PerDimension — the control matrix over all 5 dims.
func TestPolicyRulesEqual_C4_PerDimension(t *testing.T) {
	base := []rbacv1.PolicyRule{c4Rule()}

	// Identical → equal.
	if !policyRulesEqual(base, []rbacv1.PolicyRule{c4Rule()}) {
		t.Fatal("C4: identical rule sets must be equal")
	}

	// Each dimension REORDER-ONLY → still equal (order-insensitive).
	reordered := c4Rule()
	reordered.Verbs = []string{"list", "get"}
	reordered.APIGroups = []string{"apps", ""}
	reordered.Resources = []string{"deployments", "pods"}
	reordered.ResourceNames = []string{"beta", "alpha"}
	reordered.NonResourceURLs = []string{"/metrics", "/healthz"}
	if !policyRulesEqual(base, []rbacv1.PolicyRule{reordered}) {
		t.Fatal("C4: a pure reorder of EVERY field-slice must compare EQUAL — order-insensitive")
	}

	// Each dimension CHANGE → NOT equal (its own bump arm). A missed one = leak.
	dims := []struct {
		name   string
		mutate func(r *rbacv1.PolicyRule)
	}{
		{"verbs grant (add delete)", func(r *rbacv1.PolicyRule) { r.Verbs = append(slicesClone(r.Verbs), "delete") }},
		{"apiGroups change (add batch)", func(r *rbacv1.PolicyRule) { r.APIGroups = append(slicesClone(r.APIGroups), "batch") }},
		{"resources change (add secrets)", func(r *rbacv1.PolicyRule) { r.Resources = append(slicesClone(r.Resources), "secrets") }},
		{"resourceNames NARROWING (remove beta)", func(r *rbacv1.PolicyRule) { r.ResourceNames = []string{"alpha"} }},
		{"nonResourceURLs change (add /x)", func(r *rbacv1.PolicyRule) { r.NonResourceURLs = append(slicesClone(r.NonResourceURLs), "/x") }},
	}
	for _, d := range dims {
		t.Run(d.name, func(t *testing.T) {
			m := c4Rule()
			d.mutate(&m)
			if policyRulesEqual(base, []rbacv1.PolicyRule{m}) {
				t.Fatalf("C4 LEAK-TRAP: a %s must compare NOT-equal (a suppressed rotation on this change is a stale grant/revoke served under the old key). policyRulesEqual ignored the dimension.", d.name)
			}
		})
	}
}

// TestPolicyRulesEqual_RuleOrderAndMultiset — rule-list order-insensitivity and
// multiset (duplicate) semantics.
func TestPolicyRulesEqual_RuleOrderAndMultiset(t *testing.T) {
	rA := rbacv1.PolicyRule{Verbs: []string{"get"}, APIGroups: []string{""}, Resources: []string{"pods"}}
	rB := rbacv1.PolicyRule{Verbs: []string{"watch"}, APIGroups: []string{"apps"}, Resources: []string{"deployments"}}
	if !policyRulesEqual([]rbacv1.PolicyRule{rA, rB}, []rbacv1.PolicyRule{rB, rA}) {
		t.Fatal("rule-list order must not matter")
	}
	if policyRulesEqual([]rbacv1.PolicyRule{rA, rA}, []rbacv1.PolicyRule{rA}) {
		t.Fatal("multiset: [rA, rA] must NOT equal [rA] (length differs)")
	}
	if policyRulesEqual([]rbacv1.PolicyRule{rA, rA}, []rbacv1.PolicyRule{rA, rB}) {
		t.Fatal("multiset: [rA, rA] must NOT equal [rA, rB]")
	}
}

func slicesClone(s []string) []string { return append([]string(nil), s...) }

// TestPolicyRulesEqual_C4_NonResourceURLComma_NoCollision — the intra-slice
// separator must not COLLIDE with a value. Commas are legal RFC 3986 sub-delims
// in URL paths, so a comma-join renders two nonResourceURLs ["/a","/b"]
// identically to one comma-bearing path ["/a,/b"]. Those grants DIFFER (two URLs
// vs one path — a revoke of /a and /b), so they MUST compare NOT-equal. RED on a
// "," join; GREEN once the join uses a control char no value can contain.
func TestPolicyRulesEqual_C4_NonResourceURLComma_NoCollision(t *testing.T) {
	twoURLs := rbacv1.PolicyRule{NonResourceURLs: []string{"/a", "/b"}}
	oneCommaPath := rbacv1.PolicyRule{NonResourceURLs: []string{"/a,/b"}}
	if policyRulesEqual([]rbacv1.PolicyRule{twoURLs}, []rbacv1.PolicyRule{oneCommaPath}) {
		t.Fatalf("#257 C4 LEAK: nonResourceURLs [\"/a\",\"/b\"] and [\"/a,/b\"] must be NOT-equal — the intra-slice separator collided with a legal comma in a URL path, reading a REVOKE (two URLs -> one path) as no-change = a missed key rotation = leak.")
	}
}
