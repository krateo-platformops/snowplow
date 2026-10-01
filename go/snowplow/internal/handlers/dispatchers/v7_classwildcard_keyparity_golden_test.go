package dispatchers

// v7_classwildcard_keyparity_golden_test.go — #180/#254 ClassWildcard key-parity
// golden (arch's 8-arm spec; TL's black-box encoding ruling).
//
// CONTRACT this pins: a collection-verb ClassWildcard digest MUST discriminate
// identities by their concrete granted (group,resource,namespace) set under the
// class; identical grants MUST collapse (the share rate); and a cell's served
// bytes must track the digest (ARM1-OUTPUT, separate — needs the resolve harness).
//
// ASSERTION DISCIPLINE (TL ruling):
//   - PRIMARY assertion is the OBSERVABLE contract: ComputeProjectionDigest(R,D)
//     .digest ==/!= as the RAW rbacv1.PolicyRule oracle predicts. The oracle is
//     the grants the test CONSTRUCTS — NEVER the deriver's/projection's own output.
//   - canonical() is used ONLY in the on-failure diff ("diff before hash": show
//     WHICH class answer collapsed/diverged), never as the assertion.
//   - NO future ClassAnswer.Atoms field — a compile-RED test is uncommittable and
//     would pin #180 to one internal encoding. #180 may pick any encoding.
//
// RED STATE (current gated code): projectClass returns the CONSTANT
// AnswerWildcardGated for every ClassWildcard (shadow_parity.go:243), so every
// identity's wildcard-class digest is EQUAL. Therefore:
//   - must-DIFFER arms ARM1/ARM3/ARM4/ARM5 are RED NOW (they fail — the leak);
//   - must-EQUAL ARM2 passes VACUOUSLY (everything equal) — marked so the green is
//     not read as coverage;
//   - ARM6 (shareability) / ARM7 (namespace-set ⊤-vs-enumerated) use
//     ALREADY-implemented projections and are GREEN-now standing guards.
// #180's enumerate projection flips the must-DIFFER arms GREEN and makes ARM2 real.
//
// D is built HERMETICALLY by direct AccessClass construction — arch's accepted
// fallback (noted). The real deriver (DeriveRESTActionAccessDomain over a
// UAF-ResourcesFrom step, access_domain.go:418) emits the same ClassWildcard(v,G,*);
// the deriver has its own tests, so this golden pins the PROJECTION contract.

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	rbacv1 "k8s.io/api/rbac/v1"
)

const kpGroup = "widgets.example.io" // group G used across the arms

func kpRule(verbs, groups, resources []string) rbacv1.PolicyRule {
	return rbacv1.PolicyRule{Verbs: verbs, APIGroups: groups, Resources: resources}
}

// kpR builds a RequesterProfile DIRECTLY from raw rules — the INDEPENDENT oracle
// (never BuildRequesterProfile / the deriver). cluster → ClusterRules (a matching
// ClusterRoleBinding's reach); ns[k] → NamespacedRules[k] (a RoleBinding in k).
func kpR(cluster []rbacv1.PolicyRule, ns map[string][]rbacv1.PolicyRule) *rbac.RequesterProfile {
	if ns == nil {
		ns = map[string][]rbacv1.PolicyRule{}
	}
	return &rbac.RequesterProfile{ClusterRules: cluster, NamespacedRules: ns}
}

// kpWildcardD is the hermetic ClassWildcard(verb,G,*) domain (the deriver's shape
// for a UAF-ResourcesFrom step).
func kpWildcardD(verb, group string) AccessDomain {
	return AccessDomain{Classes: []AccessClass{{Kind: ClassWildcard, Verb: verb, Group: group, Resource: AccessWildcard}}}
}

func kpNsSetD(verb, group, resource string) AccessDomain {
	return AccessDomain{Classes: []AccessClass{{Kind: ClassNamespaceSet, Verb: verb, Group: group, Resource: resource}}}
}

// kpDiff renders the per-class canonical answers for A vs B — the pre-hash DIFF
// shown ONLY on failure, so a RED names which class answer collapsed/diverged.
func kpDiff(rA, rB *rbac.RequesterProfile, d AccessDomain) string {
	_, pa := ComputeProjectionDigest(rA, d)
	_, pb := ComputeProjectionDigest(rB, d)
	var b strings.Builder
	for i := range d.Classes {
		fmt.Fprintf(&b, "\n  class[%d] %s:\n    A=%q\n    B=%q", i, d.Classes[i].String(), pa.Answers[i].canonical(), pb.Answers[i].canonical())
	}
	return b.String()
}

// ARM 1 — KEY-PARITY (RED NOW). Two identities whose RAW grants differ in-scope
// (A: list widgets in G; B: list flexes in G; neither holds the other's resource,
// neither holds "*") MUST get different wildcard digests. RED now: both collapse to
// AnswerWildcardGated.
func TestGoldenV7_ARM1_KeyParity_ResourceSetDiffers(t *testing.T) {
	d := kpWildcardD("list", kpGroup)
	rA := kpR([]rbacv1.PolicyRule{kpRule([]string{"list"}, []string{kpGroup}, []string{"widgets"})}, nil)
	rB := kpR([]rbacv1.PolicyRule{kpRule([]string{"list"}, []string{kpGroup}, []string{"flexes"})}, nil)
	// Oracle (raw rules): A grants widgets∉B, B grants flexes∉A ⇒ MUST differ.
	dgA, _ := ComputeProjectionDigest(rA, d)
	dgB, _ := ComputeProjectionDigest(rB, d)
	if dgA == dgB {
		t.Fatalf("ARM1 RED (expected until #180): ClassWildcard digest fails to discriminate A{list widgets} from B{list flexes} — the AnswerWildcardGated sentinel collapses both ⇒ false-share leak once Step 3 shares the cell.%s", kpDiff(rA, rB, d))
	}
}

// ARM 2 — OVER-DISCRIMINATION guard (VACUOUS UNTIL #180). Identical in-scope grants
// ⇒ SAME digest. Passes VACUOUSLY today (the sentinel makes ALL wildcard digests
// equal) — NOT coverage until #180; post-fix it guards the enumerate projection
// against OVER-discriminating (which would silently kill the v7 share rate).
func TestGoldenV7_ARM2_OverDiscrimination_IdenticalGrantsCollapse(t *testing.T) {
	d := kpWildcardD("list", kpGroup)
	mk := func() *rbac.RequesterProfile {
		return kpR([]rbacv1.PolicyRule{kpRule([]string{"list"}, []string{kpGroup}, []string{"widgets"})}, nil)
	}
	dgA, _ := ComputeProjectionDigest(mk(), d)
	dgB, _ := ComputeProjectionDigest(mk(), d)
	if dgA != dgB {
		t.Fatalf("ARM2: identical grants must collapse to one digest (the share rate); got different.%s", kpDiff(mk(), mk(), d))
	}
	t.Log("ARM2 is VACUOUS until #180: the AnswerWildcardGated sentinel makes all wildcard digests equal, so this green is not yet coverage.")
}

// ARM 3 — PARTIAL-OVERLAP across groups×resources (RED NOW, K>1×M>1). A and B share
// some atoms and differ on others; MUST differ on the set difference.
func TestGoldenV7_ARM3_PartialOverlap_DiffersOnSetDifference(t *testing.T) {
	const g1, g2 = "g1.example.io", "g2.example.io"
	d := AccessDomain{Classes: []AccessClass{
		{Kind: ClassWildcard, Verb: "list", Group: g1, Resource: AccessWildcard},
		{Kind: ClassWildcard, Verb: "list", Group: g2, Resource: AccessWildcard},
	}}
	// A: g1/{widgets,flexes}, g2/{x} ; B: g1/{widgets}, g2/{x,y} — share
	// {g1/widgets, g2/x}, differ on A:g1/flexes vs B:g2/y.
	rA := kpR([]rbacv1.PolicyRule{
		kpRule([]string{"list"}, []string{g1}, []string{"widgets", "flexes"}),
		kpRule([]string{"list"}, []string{g2}, []string{"x"}),
	}, nil)
	rB := kpR([]rbacv1.PolicyRule{
		kpRule([]string{"list"}, []string{g1}, []string{"widgets"}),
		kpRule([]string{"list"}, []string{g2}, []string{"x", "y"}),
	}, nil)
	dgA, _ := ComputeProjectionDigest(rA, d)
	dgB, _ := ComputeProjectionDigest(rB, d)
	if dgA == dgB {
		t.Fatalf("ARM3 RED (until #180): partial-overlap grants must differ on the set difference (A has g1/flexes, B has g2/y); the sentinel collapses them.%s", kpDiff(rA, rB, d))
	}
}

// ARM 4 — "*"-PRESERVED-LITERAL (RED NOW). A grants Resources:["*"] in G (grant-all);
// B enumerates the same CURRENT concretes. MUST differ (they diverge on any
// future/undiscovered resource = a real authz difference).
func TestGoldenV7_ARM4_ResourceStarVsEnumeration_Differs(t *testing.T) {
	d := kpWildcardD("list", kpGroup)
	rStar := kpR([]rbacv1.PolicyRule{kpRule([]string{"list"}, []string{kpGroup}, []string{"*"})}, nil)
	rEnum := kpR([]rbacv1.PolicyRule{kpRule([]string{"list"}, []string{kpGroup}, []string{"widgets", "flexes", "gauges"})}, nil)
	dgStar, _ := ComputeProjectionDigest(rStar, d)
	dgEnum, _ := ComputeProjectionDigest(rEnum, d)
	if dgStar == dgEnum {
		t.Fatalf("ARM4 RED (until #180): rule Resources:[\"*\"] (grant-all) must digest differently from an explicit enumeration (they diverge on any future resource); the sentinel collapses them.%s", kpDiff(rStar, rEnum, d))
	}
}

// ARM 5 — NAMESPACE-SCOPE DISCRIMINATION (RED NOW). Same (group,resource) grant,
// different NAMESPACE scope: A via a RoleBinding in N1 ONLY (NamespacedRules[N1]);
// B via a ClusterRoleBinding (ClusterRules, all namespaces). MUST differ (A admits
// only N1 objects, B all). RED now: the ClassWildcard sentinel ignores the ns
// dimension entirely (and would stay RED under a resource-only enumerate).
func TestGoldenV7_ARM5_NamespaceScope_Differs(t *testing.T) {
	const n1 = "tenant-1"
	d := kpWildcardD("list", kpGroup)
	rNs := kpR(nil, map[string][]rbacv1.PolicyRule{n1: {kpRule([]string{"list"}, []string{kpGroup}, []string{"widgets"})}})
	rCluster := kpR([]rbacv1.PolicyRule{kpRule([]string{"list"}, []string{kpGroup}, []string{"widgets"})}, nil)
	dgNs, _ := ComputeProjectionDigest(rNs, d)
	dgCluster, _ := ComputeProjectionDigest(rCluster, d)
	if dgNs == dgCluster {
		t.Fatalf("ARM5 RED (until #180): a RoleBinding-in-%s grant must digest differently from a ClusterRoleBinding (all-ns) grant for the same (group,resource); the wildcard sentinel ignores the namespace dimension.%s", n1, kpDiff(rNs, rCluster, d))
	}
}

// ARM 5b — NAMESPACE-IDENTITY DISCRIMINATION (RED NOW; arch strengthening). Two
// DIFFERENT single-namespace scopes — A via a RoleBinding in N1, B via a
// RoleBinding in N2, same (group,resource) — MUST differ (A admits only N1
// objects, B only N2). This catches an enumerate that records "has-a-namespace-
// answer" but not WHICH namespace: ARM5 is {N1}-vs-⊤ and ARM7 is set-vs-⊤, so
// neither pins single-vs-single. RED now: the wildcard sentinel ignores the
// namespace entirely; GREEN only under a (group,resource,ns-answer) enumerate
// whose ns-answer records the concrete namespace set.
func TestGoldenV7_ARM5b_NamespaceIdentity_Differs(t *testing.T) {
	const n1, n2 = "tenant-1", "tenant-2"
	d := kpWildcardD("list", kpGroup)
	rN1 := kpR(nil, map[string][]rbacv1.PolicyRule{n1: {kpRule([]string{"list"}, []string{kpGroup}, []string{"widgets"})}})
	rN2 := kpR(nil, map[string][]rbacv1.PolicyRule{n2: {kpRule([]string{"list"}, []string{kpGroup}, []string{"widgets"})}})
	dgN1, _ := ComputeProjectionDigest(rN1, d)
	dgN2, _ := ComputeProjectionDigest(rN2, d)
	if dgN1 == dgN2 {
		t.Fatalf("ARM5b RED (until #180): a grant scoped to %s must digest differently from one scoped to %s for the same (group,resource) — an enumerate that records 'has a namespace' but not WHICH would collide; the wildcard sentinel ignores it.%s", n1, n2, kpDiff(rN1, rN2, d))
	}
}

// ARM 6 — SHAREABILITY (GREEN-now standing guard; freshness-audit dependency). A
// NAME-SPECIFIC-verb (get) ClassWildcard is name-ambiguous ⇒ NOT shareable. The
// name-dimension safety RESTS on this classification; prove it, don't assume. RED
// if a name-specific wildcard ever became shareable.
func TestGoldenV7_ARM6_Shareability_NameSpecificWildcardNotShareable(t *testing.T) {
	if d := kpWildcardD("get", kpGroup); Shareable(d) { // get = name-specific verb
		t.Fatalf("ARM6: a name-specific-verb (get) ClassWildcard must be NOT shareable (name-ambiguous); Shareable(D)=true — the name-dimension leak guard is broken. %s", d.String())
	}
	// Control: a collection-verb (list) wildcard IS shareable (nothing else ambiguous).
	if d := kpWildcardD("list", kpGroup); !Shareable(d) {
		t.Fatalf("ARM6 control: a collection-verb (list) ClassWildcard must be shareable; Shareable=false. %s", d.String())
	}
}

// ARM 7 — NS-"*"-PRESERVED (GREEN-now COMPONENT guard; the ns-analog of ARM4). A
// cluster-wide grant (ClusterPermit ⊤) MUST digest differently from an explicit
// ns-set listing all CURRENT namespaces (they diverge on any FUTURE namespace = a
// real authz difference). This exercises a PLAIN ClassNamespaceSet — the
// ALREADY-implemented AnswerNamespaceSet (⊤ vs Namespaces) encoding that the
// WILDCARD atom's ns-answer will REUSE at #180. So it is GREEN now and guards that
// encoding as a COMPONENT; it does NOT prove the wildcard-atom ns-"*" INTEGRATION
// today (that, like ARM2's over-discrimination, is only testable once #180's
// enumerate populates the wildcard atoms). RED if ⊤ ever collapsed into the set.
func TestGoldenV7_ARM7_NamespaceStarVsEnumeration_Differs(t *testing.T) {
	const r = "widgets"
	d := kpNsSetD("list", kpGroup, r)
	rTop := kpR([]rbacv1.PolicyRule{kpRule([]string{"list"}, []string{kpGroup}, []string{r})}, nil)
	rEnum := kpR(nil, map[string][]rbacv1.PolicyRule{
		"ns-a": {kpRule([]string{"list"}, []string{kpGroup}, []string{r})},
		"ns-b": {kpRule([]string{"list"}, []string{kpGroup}, []string{r})},
	})
	dgTop, _ := ComputeProjectionDigest(rTop, d)
	dgEnum, _ := ComputeProjectionDigest(rEnum, d)
	if dgTop == dgEnum {
		t.Fatalf("ARM7: a cluster-wide (⊤) namespace grant must digest differently from an enumerated ns-set (they diverge on any future namespace); ⊤ collapsed into the set.%s", kpDiff(rTop, rEnum, d))
	}
}

// ARM 8 — PER-ATOM NS-MAPPING (RED NOW; pm condition-6 completeness, REQUIRED).
// Two identities with the SAME resource-set {widgets,gadgets} AND the SAME
// namespace-union {n1, n2}, but a DIFFERENT resource→namespace MAPPING, MUST
// differ:
//
//	A: widgets @ n1 + gadgets @ n2
//	B: widgets @ n2 + gadgets @ n1   (swapped)
//
// This is the arm that forces the ns-answer to be PER-ATOM. A WRONG DOMAIN-LEVEL
// ns encoding (one ns-set for the whole cell — resource-set {widgets,gadgets} ×
// ns-set {n1,n2}) COLLIDES here and LEAKS: A serves widgets only in n1 while B
// serves widgets only in n2. RED now (sentinel), RED under ANY domain-level-ns
// encoding, GREEN only under the correct per-atom (for EACH atom compute its
// ns-answer). ARM5/ARM5b are single-atom (domain==per-atom there) and ARM3 is
// multi-atom but all cluster-scoped, so neither pins per-atom — this does.
func TestGoldenV7_ARM8_PerAtomNsMapping_Differs(t *testing.T) {
	const n1, n2 = "tenant-1", "tenant-2"
	const g1 = "g1.example.io"
	d := kpWildcardD("list", g1)
	// A: widgets@n1, gadgets@n2 (both via RoleBindings).
	rA := kpR(nil, map[string][]rbacv1.PolicyRule{
		n1: {kpRule([]string{"list"}, []string{g1}, []string{"widgets"})},
		n2: {kpRule([]string{"list"}, []string{g1}, []string{"gadgets"})},
	})
	// B: widgets@n2, gadgets@n1 — same resource-set + same ns-union, swapped mapping.
	rB := kpR(nil, map[string][]rbacv1.PolicyRule{
		n1: {kpRule([]string{"list"}, []string{g1}, []string{"gadgets"})},
		n2: {kpRule([]string{"list"}, []string{g1}, []string{"widgets"})},
	})
	dgA, _ := ComputeProjectionDigest(rA, d)
	dgB, _ := ComputeProjectionDigest(rB, d)
	if dgA == dgB {
		t.Fatalf("ARM8 RED (until #180 per-atom ns): same resource-set {widgets,gadgets} + same ns-union {%s,%s} but a SWAPPED resource→ns mapping (A: widgets@%s,gadgets@%s; B: widgets@%s,gadgets@%s) must digest differently — a DOMAIN-LEVEL ns encoding (one ns-set for the whole cell) collides here and leaks; the ns-answer must be PER-ATOM.%s", n1, n2, n1, n2, n2, n1, kpDiff(rA, rB, d))
	}
}

// ARM 1-OUTPUT — RESOLVE-OUTPUT LEAK INVARIANT (RED NOW; pm condition 2). The
// digest/serve coupling, in arch's implication form: digest(A)==digest(B) ⟹
// serve(A)==serve(B). It links key-parity to a REAL served-content difference —
// not just a theoretical key diff. Two co-bound identities (same portal group =
// same dispatch binding) with DIVERGENT per-object grants (alice permitted in
// tenant-a, bob in tenant-b) are served DIFFERENT UAF-narrowed content, while
// their ClassWildcard digest COLLAPSES to the gated sentinel today ⇒ a shared
// cell would serve alice's rows to bob. The serve side is the REAL EvaluateRBAC
// refilter over the same published snapshot the digest's R is built from — serve
// is INDEPENDENT of the digest's R-projection (not two views of one object), so a
// divergence is genuine output evidence. RED today (equal gated digest + divergent
// serve); GREEN at #180 (enumerate makes digest(A)≠digest(B) ⇒ premise false ⇒ no
// shared cell ⇒ no leak). Uses the a1 two-tenant harness (configmaps get/list,
// alice→tenant-a / bob→tenant-b) — divergence is RBAC-produced, not a fixture
// literal.
func TestGoldenV7_ARM1OUTPUT_LeakInvariant_EqualDigestImpliesEqualServe(t *testing.T) {
	a1BuildTwoTenantWatcher(t)
	snap := cache.RBACSnapshotForTest()
	if snap == nil {
		t.Fatal("ARM1-OUTPUT precondition: no published RBAC snapshot")
	}

	// SERVE = the real UAF refilter keep-set (RBAC-filtered namespaces) per
	// identity, computed via EvaluateRBAC exactly as refilter.go's per-object
	// evalSingle does — INDEPENDENT of the projection's R.
	serve := func(user string) []string {
		t.Helper()
		ctx := a1UserCtx(user)
		kept := []string{}
		for _, ns := range []string{a1TenantA, a1TenantB} {
			allowed, _, err := rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
				Username: user, Groups: []string{a1Group},
				Verb: "list", Group: "", Resource: "configmaps", Namespace: ns,
			})
			if err != nil {
				t.Fatalf("ARM1-OUTPUT serve EvaluateRBAC(%s,%s): %v", user, ns, err)
			}
			if allowed {
				kept = append(kept, ns)
			}
		}
		slices.Sort(kept)
		return kept
	}
	serveA, serveB := serve(a1Alice), serve(a1Bob)
	if len(serveA) == 0 || len(serveB) == 0 || slices.Equal(serveA, serveB) {
		t.Fatalf("ARM1-OUTPUT precondition: the two identities' served content must genuinely diverge (else the arm is vacuous); got alice=%v bob=%v", serveA, serveB)
	}

	// DIGEST over a ClassWildcard (the resourcesFrom-UAF shape): each identity's R
	// (built from the SAME snapshot) projects the wildcard to the gated sentinel →
	// equal digest TODAY.
	d := kpWildcardD("list", "") // core group (configmaps)
	rA := rbac.BuildRequesterProfile(snap, rbac.EvaluateOptions{Username: a1Alice, Groups: []string{a1Group}})
	rB := rbac.BuildRequesterProfile(snap, rbac.EvaluateOptions{Username: a1Bob, Groups: []string{a1Group}})
	dgA, _ := ComputeProjectionDigest(rA, d)
	dgB, _ := ComputeProjectionDigest(rB, d)

	if dgA == dgB && !slices.Equal(serveA, serveB) {
		t.Fatalf("ARM1-OUTPUT RED (until #180): equal ClassWildcard digest (%s…) but DIFFERENT served content (alice=%v, bob=%v) — the gated wildcard would share ONE cell across identities whose UAF-narrowed output differs, so bob is served alice's rows (the real content leak key-parity alone can't prove). #180's enumerate must make digest(A)≠digest(B).%s",
			dgA[:12], serveA, serveB, kpDiff(rA, rB, d))
	}
}
