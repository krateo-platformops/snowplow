// shadow_parity_test.go — hermetic falsifiers for v7 Step 2C (projection P,
// digest, shareable / name-ambiguity classification). These build R
// (rbac.RequesterProfile) and D (AccessDomain) directly from exported fields —
// no cluster, no corpus, no network — so they run in the default -race suite.
//
// The corpus halves (F-D4-offline, F-proj offline half) live in
// shadow_parity_corpus_test.go behind the `accessdomainmeasure` build tag.
//
// RED-first control matrix (each arm fails when its one defect is injected):
//   - F-D7b:            digest is not deterministic / not order-independent.
//   - F-proj-golden:    projectClass returns a wrong per-class answer.
//   - shareable arm:    nameAmbiguous / escape rule dropped or inverted.

package dispatchers

import (
	"sync"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/rbac"
	rbacv1 "k8s.io/api/rbac/v1"
)

// rule is a terse PolicyRule constructor for the golden matrix.
func rule(verbs, groups, resources, names []string) rbacv1.PolicyRule {
	return rbacv1.PolicyRule{Verbs: verbs, APIGroups: groups, Resources: resources, ResourceNames: names}
}

func clusterProfile(rules ...rbacv1.PolicyRule) *rbac.RequesterProfile {
	return &rbac.RequesterProfile{ClusterRules: rules, NamespacedRules: map[string][]rbacv1.PolicyRule{}}
}

func nsProfile(byNS map[string][]rbacv1.PolicyRule) *rbac.RequesterProfile {
	return &rbac.RequesterProfile{NamespacedRules: byNS}
}

// ───────────────────────── F-proj-golden ─────────────────────────
// A hand-built (R, D) matrix asserting the projected answer per class equals the
// expected answer: exact booleans (incl. resourceNames scoping), nsset TOP vs
// namespace-set vs empty, and the wildcard gate.
func TestFProjGolden_PerClassAnswers(t *testing.T) {
	getPods := clusterProfile(rule([]string{"get"}, []string{""}, []string{"pods"}, nil))
	getPodByName := clusterProfile(rule([]string{"get"}, []string{""}, []string{"pods"}, []string{"pod1"}))
	listDepCluster := clusterProfile(rule([]string{"list"}, []string{"apps"}, []string{"deployments"}, nil))
	listDepInAB := nsProfile(map[string][]rbacv1.PolicyRule{
		"ns-b": {rule([]string{"list"}, []string{"apps"}, []string{"deployments"}, nil)},
		"ns-a": {rule([]string{"list"}, []string{"apps"}, []string{"deployments"}, nil)},
		"ns-x": {rule([]string{"list"}, []string{"batch"}, []string{"jobs"}, nil)}, // non-matching
	})
	empty := clusterProfile()

	tests := []struct {
		name  string
		r     *rbac.RequesterProfile
		class AccessClass
		want  ClassAnswer
	}{
		{
			name:  "exact get pods permit",
			r:     getPods,
			class: AccessClass{Kind: ClassExact, Verb: "get", Group: "", Resource: "pods", Namespace: "ns1", Name: "anypod"},
			want:  ClassAnswer{Kind: AnswerBool, Permit: true},
		},
		{
			name:  "exact delete pods deny (verb not granted)",
			r:     getPods,
			class: AccessClass{Kind: ClassExact, Verb: "delete", Group: "", Resource: "pods", Namespace: "ns1", Name: "anypod"},
			want:  ClassAnswer{Kind: AnswerBool, Permit: false},
		},
		{
			name:  "exact resourceNames get pod1 permit",
			r:     getPodByName,
			class: AccessClass{Kind: ClassExact, Verb: "get", Group: "", Resource: "pods", Namespace: "ns1", Name: "pod1"},
			want:  ClassAnswer{Kind: AnswerBool, Permit: true},
		},
		{
			name:  "exact resourceNames get pod2 deny (name not in list)",
			r:     getPodByName,
			class: AccessClass{Kind: ClassExact, Verb: "get", Group: "", Resource: "pods", Namespace: "ns1", Name: "pod2"},
			want:  ClassAnswer{Kind: AnswerBool, Permit: false},
		},
		{
			name:  "nsset TOP via cluster grant",
			r:     listDepCluster,
			class: AccessClass{Kind: ClassNamespaceSet, Verb: "list", Group: "apps", Resource: "deployments"},
			want:  ClassAnswer{Kind: AnswerNamespaceSet, ClusterPermit: true},
		},
		{
			name:  "nsset namespace-set sorted",
			r:     listDepInAB,
			class: AccessClass{Kind: ClassNamespaceSet, Verb: "list", Group: "apps", Resource: "deployments"},
			want:  ClassAnswer{Kind: AnswerNamespaceSet, Namespaces: []string{"ns-a", "ns-b"}},
		},
		{
			name:  "nsset empty set",
			r:     empty,
			class: AccessClass{Kind: ClassNamespaceSet, Verb: "list", Group: "apps", Resource: "deployments"},
			want:  ClassAnswer{Kind: AnswerNamespaceSet},
		},
		{
			name:  "wildcard is gated (not a fabricated permit)",
			r:     listDepCluster,
			class: AccessClass{Kind: ClassWildcard, Verb: "list", Group: AccessWildcard, Resource: "deployments"},
			want:  ClassAnswer{Kind: AnswerWildcardGated},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := projectClass(tc.r, tc.class)
			if got.Kind != tc.want.Kind {
				t.Fatalf("Kind: got %v want %v", got.Kind, tc.want.Kind)
			}
			if got.Permit != tc.want.Permit {
				t.Fatalf("Permit: got %v want %v", got.Permit, tc.want.Permit)
			}
			if got.ClusterPermit != tc.want.ClusterPermit {
				t.Fatalf("ClusterPermit: got %v want %v", got.ClusterPermit, tc.want.ClusterPermit)
			}
			if !equalStrs(got.Namespaces, tc.want.Namespaces) {
				t.Fatalf("Namespaces: got %v want %v", got.Namespaces, tc.want.Namespaces)
			}
		})
	}
}

// TestFProjGolden_NamespaceSetIsSorted asserts the set-valued answer is always
// sorted (the property the digest's order-independence rests on).
func TestFProjGolden_NamespaceSetIsSorted(t *testing.T) {
	r := nsProfile(map[string][]rbacv1.PolicyRule{
		"zeta":  {rule([]string{"list"}, []string{""}, []string{"configmaps"}, nil)},
		"alpha": {rule([]string{"list"}, []string{""}, []string{"configmaps"}, nil)},
		"mid":   {rule([]string{"list"}, []string{""}, []string{"configmaps"}, nil)},
	})
	ans := projectClass(r, AccessClass{Kind: ClassNamespaceSet, Verb: "list", Group: "", Resource: "configmaps"})
	want := []string{"alpha", "mid", "zeta"}
	if !equalStrs(ans.Namespaces, want) {
		t.Fatalf("namespace set not sorted: got %v want %v", ans.Namespaces, want)
	}
}

// ───────────────────────── F-D7b (digest determinism) ─────────────────────────

// A domain with an nsset class whose answer is a multi-namespace set — the
// map-iteration-order-sensitive case. Classes pre-sorted by canonical.
func multiNSDomainAndProfile() (AccessDomain, *rbac.RequesterProfile) {
	d := domain(
		[]AccessClass{
			{Kind: ClassExact, Verb: "get", Group: "", Resource: "pods", Namespace: "ns-a", Name: "p1"},
			{Kind: ClassNamespaceSet, Verb: "list", Group: "apps", Resource: "deployments"},
		},
		nil,
	)
	r := &rbac.RequesterProfile{
		ClusterRules: []rbacv1.PolicyRule{rule([]string{"get"}, []string{""}, []string{"pods"}, nil)},
		NamespacedRules: map[string][]rbacv1.PolicyRule{
			"ns-3": {rule([]string{"list"}, []string{"apps"}, []string{"deployments"}, nil)},
			"ns-1": {rule([]string{"list"}, []string{"apps"}, []string{"deployments"}, nil)},
			"ns-2": {rule([]string{"list"}, []string{"apps"}, []string{"deployments"}, nil)},
		},
	}
	return d, r
}

// TestFD7b_DigestDeterministicAcrossRuns: same (R, D) → same digest across many
// runs, despite Go's randomized map iteration (namespace set + NamespacedRules
// walk). RED if the encoding leaked map order.
func TestFD7b_DigestDeterministicAcrossRuns(t *testing.T) {
	d, r := multiNSDomainAndProfile()
	first, _ := ComputeProjectionDigest(r, d)
	for i := 0; i < 500; i++ {
		got, _ := ComputeProjectionDigest(r, d)
		if got != first {
			t.Fatalf("digest not deterministic on run %d: %s != %s", i, got, first)
		}
	}
}

// TestFD7b_DigestDeterministicAcrossGoroutines: N concurrent goroutines compute
// the same digest. Also exercises -race on the pure functions (no shared state).
func TestFD7b_DigestDeterministicAcrossGoroutines(t *testing.T) {
	d, r := multiNSDomainAndProfile()
	want, _ := ComputeProjectionDigest(r, d)

	const n = 64
	var wg sync.WaitGroup
	got := make([]string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i], _ = ComputeProjectionDigest(r, d)
		}(i)
	}
	wg.Wait()
	for i, g := range got {
		if g != want {
			t.Fatalf("goroutine %d digest %s != %s", i, g, want)
		}
	}
}

// TestFD7b_TwoDifferentPsDiffer: two identities with different grants over the
// same D produce different digests (the discrimination the digest exists for).
func TestFD7b_TwoDifferentPsDiffer(t *testing.T) {
	d := domain([]AccessClass{
		{Kind: ClassExact, Verb: "get", Group: "", Resource: "secrets", Namespace: "ns1", Name: "s1"},
	}, nil)

	rAllow := clusterProfile(rule([]string{"get"}, []string{""}, []string{"secrets"}, nil))
	rDeny := clusterProfile(rule([]string{"get"}, []string{""}, []string{"configmaps"}, nil))

	dg1, _ := ComputeProjectionDigest(rAllow, d)
	dg2, _ := ComputeProjectionDigest(rDeny, d)
	if dg1 == dg2 {
		t.Fatalf("digests must differ for differing access: both %s", dg1)
	}
}

// TestFD7b_SetOrderIndependent: two identities whose namespace GRANT SETS are
// equal but whose NamespacedRules were populated in a different insertion/content
// order still produce the same digest (the set is what matters, not order).
func TestFD7b_SetOrderIndependent(t *testing.T) {
	d := domain([]AccessClass{
		{Kind: ClassNamespaceSet, Verb: "list", Group: "", Resource: "configmaps"},
	}, nil)
	lr := rule([]string{"list"}, []string{""}, []string{"configmaps"}, nil)

	r1 := nsProfile(map[string][]rbacv1.PolicyRule{"a": {lr}, "b": {lr}, "c": {lr}})
	r2 := nsProfile(map[string][]rbacv1.PolicyRule{"c": {lr}, "b": {lr}, "a": {lr}})

	dg1, _ := ComputeProjectionDigest(r1, d)
	dg2, _ := ComputeProjectionDigest(r2, d)
	if dg1 != dg2 {
		t.Fatalf("equal grant sets must yield equal digest: %s != %s", dg1, dg2)
	}

	// A genuinely different set (extra namespace) must differ.
	r3 := nsProfile(map[string][]rbacv1.PolicyRule{"a": {lr}, "b": {lr}, "c": {lr}, "d": {lr}})
	dg3, _ := ComputeProjectionDigest(r3, d)
	if dg3 == dg1 {
		t.Fatalf("different grant set must yield different digest: both %s", dg1)
	}
}

// ───────────────────── shareable / name-ambiguity classification ─────────────

func TestShareable_ClassificationArm(t *testing.T) {
	esc := AccessEscape{Verb: "create", Group: "apps", Resource: "deployments", Step: "s", Reason: "r"}

	tests := []struct {
		name string
		d    AccessDomain
		want bool
	}{
		// name-specific verb + name-free ⇒ NOT shareable.
		{"exact get name-free not shareable",
			domain([]AccessClass{{Kind: ClassExact, Verb: "get", Group: "", Resource: "pods", Namespace: "ns1", Name: ""}}, nil), false},
		{"nsset get not shareable",
			domain([]AccessClass{{Kind: ClassNamespaceSet, Verb: "get", Group: "", Resource: "pods"}}, nil), false},
		{"wildcard get not shareable",
			domain([]AccessClass{{Kind: ClassWildcard, Verb: "get", Group: AccessWildcard, Resource: "pods"}}, nil), false},
		{"wildcard verb-star not shareable",
			domain([]AccessClass{{Kind: ClassWildcard, Verb: AccessWildcard, Group: AccessWildcard, Resource: AccessWildcard}}, nil), false},
		{"exact update/patch/delete name-free not shareable",
			domain([]AccessClass{
				{Kind: ClassExact, Verb: "update", Group: "", Resource: "pods", Name: ""},
				{Kind: ClassExact, Verb: "patch", Group: "", Resource: "pods", Name: ""},
				{Kind: ClassExact, Verb: "delete", Group: "", Resource: "pods", Name: ""},
			}, nil), false},

		// name-pinned / collection-verb ⇒ shareable.
		{"exact get name-pinned shareable",
			domain([]AccessClass{{Kind: ClassExact, Verb: "get", Group: "", Resource: "pods", Namespace: "ns1", Name: "pod1"}}, nil), true},
		{"exact list name-free shareable (collection verb)",
			domain([]AccessClass{{Kind: ClassExact, Verb: "list", Group: "", Resource: "pods", Name: ""}}, nil), true},
		{"nsset list shareable",
			domain([]AccessClass{{Kind: ClassNamespaceSet, Verb: "list", Group: "apps", Resource: "deployments"}}, nil), true},
		{"wildcard list shareable (collection verb)",
			domain([]AccessClass{{Kind: ClassWildcard, Verb: "list", Group: AccessWildcard, Resource: "deployments"}}, nil), true},
		{"nsset create shareable",
			domain([]AccessClass{{Kind: ClassNamespaceSet, Verb: "create", Group: "apps", Resource: "deployments"}}, nil), true},

		// escapes ⇒ NOT shareable regardless of classes.
		{"escape not shareable",
			domain([]AccessClass{{Kind: ClassExact, Verb: "list", Group: "", Resource: "pods", Name: "x"}}, []AccessEscape{esc}), false},

		// empty domain: vacuously shareable (no ambiguous class, no escape).
		{"empty domain shareable", domain(nil, nil), true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Shareable(tc.d); got != tc.want {
				t.Fatalf("Shareable=%v want %v", got, tc.want)
			}
		})
	}
}

// TestNotShareableReasons_Tally: per-reason tally is the Step-2E counter shape.
func TestNotShareableReasons_Tally(t *testing.T) {
	d := domain(
		[]AccessClass{
			{Kind: ClassExact, Verb: "get", Group: "", Resource: "pods", Name: ""},            // name-ambiguous
			{Kind: ClassNamespaceSet, Verb: "delete", Group: "apps", Resource: "deployments"}, // name-ambiguous
			{Kind: ClassExact, Verb: "list", Group: "", Resource: "pods", Name: "keep"},       // shareable class
		},
		[]AccessEscape{{Verb: "create", Group: "apps", Resource: "deployments", Step: "s", Reason: "r"}},
	)
	got := NotShareableReasons(d)
	if got.NameAmbiguous != 2 {
		t.Fatalf("NameAmbiguous tally: got %d want 2", got.NameAmbiguous)
	}
	if got.Escape != 1 {
		t.Fatalf("Escape tally: got %d want 1", got.Escape)
	}
	if !got.Any() || Shareable(d) {
		t.Fatalf("tally.Any()=%v Shareable=%v — expected not-shareable", got.Any(), Shareable(d))
	}

	clean := domain([]AccessClass{{Kind: ClassExact, Verb: "list", Group: "", Resource: "pods", Name: ""}}, nil)
	if ct := NotShareableReasons(clean); ct.Any() {
		t.Fatalf("clean domain tally should be empty, got %+v", ct)
	}
}

// TestProjection_WildcardGatedFlag asserts the projection flags a wildcard cell.
func TestProjection_WildcardGatedFlag(t *testing.T) {
	dWild := domain([]AccessClass{{Kind: ClassWildcard, Verb: "list", Group: AccessWildcard, Resource: "x"}}, nil)
	if p := ProjectRequesterProfile(clusterProfile(), dWild); !p.WildcardGated {
		t.Fatalf("expected WildcardGated=true for a domain with a ClassWildcard")
	} else if DigestTrustworthy(p) {
		t.Fatalf("DigestTrustworthy must be false for a wildcard-gated projection")
	} else if !Shareable(dWild) {
		// The trap the sharing contract guards: a collection-verb wildcard cell is
		// Shareable(D)==true yet its digest is NOT trustworthy.
		t.Fatalf("collection-verb wildcard cell expected Shareable(D)==true (contract trap)")
	}
	dNoWild := domain([]AccessClass{{Kind: ClassExact, Verb: "list", Group: "", Resource: "x", Name: ""}}, nil)
	if p := ProjectRequesterProfile(clusterProfile(), dNoWild); p.WildcardGated {
		t.Fatalf("expected WildcardGated=false for a domain with no ClassWildcard")
	} else if !DigestTrustworthy(p) {
		t.Fatalf("DigestTrustworthy must be true for a non-wildcard projection")
	}
	dEsc := domain(nil, []AccessEscape{{Verb: "create", Group: "g", Resource: "r", Step: "s", Reason: "x"}})
	if p := ProjectRequesterProfile(clusterProfile(), dEsc); !p.HasEscape {
		t.Fatalf("expected HasEscape=true for a domain with an escape")
	}
}

// ───────────────────────── helpers ─────────────────────────

// domain builds an AccessDomain in the SAME canonical-sorted order the deriver's
// builder produces, so ComputeProjectionDigest sees classes in sorted order.
func domain(classes []AccessClass, escapes []AccessEscape) AccessDomain {
	b := newAccessBuilder()
	for _, c := range classes {
		b.add(c)
	}
	for _, e := range escapes {
		b.escape(e)
	}
	return b.build()
}

func equalStrs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
