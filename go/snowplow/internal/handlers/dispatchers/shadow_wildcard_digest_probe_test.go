// shadow_wildcard_digest_probe_test.go — v7 #368 falsifiers for the 4th dark
// detector (cross-identity wildcard digest-collision probe).
//
// RED-first control matrix (each arm fires on exactly its defect; the companions
// are single-defect controls). Each arm is proven RED by a neuter documented inline:
//
//	F-368(a) NOT-VACUOUS — a wrong SHAREABLE/ungated projection (collapses two
//	         different-access identities to one digest) fires collision_total. RED
//	         neuter: make fingerprintOver ignore the evaluator (return a constant) —
//	         the independent oracle is what catches a projection bug the digest hides.
//	F-368(b) NAMESPACE — a RoleBinding-N1 identity vs a ClusterRoleBinding (all-ns)
//	         identity, checks in N1 AND N2, collapsed to one digest → collision. RED
//	         neuter: drop namespace from the coordinate → the two look identical.
//	F-368(c) CONVERSE (union-drift) — two IDENTICAL-access identities observed at
//	         different times (growing union) stay collision 0. RED neuter: store a
//	         pre-hashed fingerprint over each observation's union instead of
//	         recomputing over the current union (fix (b)) → false collision.
//	F-368(d) DENOMINATOR — one identity → observed_total 0 (not certified).
//	F-368(e) DARK — observe() is byte-identical for cache.ComputeKey (no key fold);
//	         resolvedKeyVersion stays v6 (the probe is dispatchers-only, counters-only).
//	F-368(f) GATED — a gated wildcard cell with two different-access identities keeps
//	         collision_total 0 and fires the expvar-only gated diagnostic instead.
//	F-368(wire) — runShadowParityHook actually CALLS the probe (seam-is-a-floor).

package dispatchers

import (
	"context"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	rbacv1 "k8s.io/api/rbac/v1"
)

// ───────────────────────────── #368 harness ─────────────────────────────

// wcDomain is a collection-verb ClassWildcard-bearing cell (the UAF-list shape).
func wcDomain() AccessDomain {
	return domain([]AccessClass{{Kind: ClassWildcard, Verb: "list", Group: "*", Resource: "*"}}, nil)
}

// wcProfileCluster permits (verb,group,resource) in EVERY namespace (a ClusterRoleBinding).
func wcProfileCluster(verb, group, resource string) *rbac.RequesterProfile {
	return &rbac.RequesterProfile{ClusterRules: []rbacv1.PolicyRule{
		{Verbs: []string{verb}, APIGroups: []string{group}, Resources: []string{resource}},
	}}
}

// wcProfileNS permits (verb,group,resource) in ONE namespace only (a RoleBinding).
func wcProfileNS(ns, verb, group, resource string) *rbac.RequesterProfile {
	return &rbac.RequesterProfile{NamespacedRules: map[string][]rbacv1.PolicyRule{
		ns: {{Verbs: []string{verb}, APIGroups: []string{group}, Resources: []string{resource}}},
	}}
}

// wcProfileDeny permits nothing.
func wcProfileDeny() *rbac.RequesterProfile { return &rbac.RequesterProfile{} }

// withProfiles overrides the requester-profile seam so each identity's evaluator
// access is exactly the given profile (the fix-(b) recompute consults this).
func withProfiles(t *testing.T, m map[string]*rbac.RequesterProfile) {
	t.Helper()
	prev := shadowProfileFor
	shadowProfileFor = func(_ *cache.RBACSnapshot, id rbac.EvaluateOptions) *rbac.RequesterProfile {
		if p, ok := m[id.Username]; ok {
			return p
		}
		return wcProfileDeny()
	}
	t.Cleanup(func() { shadowProfileFor = prev })
}

// wcSnap returns a non-nil snapshot. Its content is irrelevant here because every
// arm overrides shadowProfileFor to return a controlled profile (ignoring snap); the
// probe only needs snap != nil to pass its guard.
func wcSnap() *cache.RBACSnapshot { return &cache.RBACSnapshot{} }

func wcSC(user, digest string, shareable, gated bool) *shadowContext {
	return &shadowContext{
		domain:        wcDomain(),
		identity:      rbac.EvaluateOptions{Username: user},
		digest:        digest,
		shareable:     shareable,
		wildcardGated: gated,
	}
}

func wcOpts(verb, group, resource, ns string) rbac.EvaluateOptions {
	return rbac.EvaluateOptions{Verb: verb, Group: group, Resource: resource, Namespace: ns}
}

// wcCertified is the certification triad (code doc + #368 issue): certified ⇔
// collision==0 AND observed>0 AND evicted==0. Any drop (evicted>0) VOIDS the window.
func wcCertified() bool {
	return shadowWildcardDigestCollisionTotal.Load() == 0 &&
		shadowWildcardDigestObservedTotal.Load() > 0 &&
		shadowWildcardDigestEvictedTotal.Load() == 0
}

// ───────────────────────────── arms ─────────────────────────────

func TestF368a_WrongShareableProjection_FiresCollision(t *testing.T) {
	resetWildcardDigestProbeForTest()
	withProfiles(t, map[string]*rbac.RequesterProfile{
		"alice": wcProfileCluster("list", "", "pods"), // permits
		"bob":   wcProfileDeny(),                      // denies — DIFFERENT access
	})
	snap := wcSnap()
	opts := wcOpts("list", "", "pods", "default")
	// WRONG, ungated, SHAREABLE projection: both identities collapse to one digest.
	const sameDigest = "wrong-ungated-same-digest"
	wildcardProbe.observe(snap, wcSC("alice", sameDigest, true, false), opts)
	wildcardProbe.observe(snap, wcSC("bob", sameDigest, true, false), opts)

	if got := shadowWildcardDigestCollisionTotal.Load(); got != 1 {
		t.Fatalf("#368(a): a wrong SHAREABLE projection collapsing two different-access identities must fire collision_total once; got %d", got)
	}
	if got := shadowWildcardDigestObservedTotal.Load(); got != 1 {
		t.Fatalf("#368(a): observed_total (denominator) must be 1; got %d", got)
	}
	if got := shadowWildcardGatedDigestCollisionTotal.Load(); got != 0 {
		t.Fatalf("#368(a): the gated diagnostic must stay 0 for an ungated cell; got %d", got)
	}
}

func TestF368b_NamespaceScopeCollision(t *testing.T) {
	resetWildcardDigestProbeForTest()
	withProfiles(t, map[string]*rbac.RequesterProfile{
		"alice": wcProfileNS("n1", "list", "", "pods"), // RoleBinding in n1 ONLY
		"bob":   wcProfileCluster("list", "", "pods"),  // ClusterRoleBinding (all ns)
	})
	snap := wcSnap()
	const sameDigest = "namespace-blind-same-digest" // a namespace-blind projection collapsed them
	// Observations span BOTH namespaces (required: a set limited to n1 would miss it).
	wildcardProbe.observe(snap, wcSC("alice", sameDigest, true, false), wcOpts("list", "", "pods", "n1"))
	wildcardProbe.observe(snap, wcSC("alice", sameDigest, true, false), wcOpts("list", "", "pods", "n2"))
	wildcardProbe.observe(snap, wcSC("bob", sameDigest, true, false), wcOpts("list", "", "pods", "n1"))
	wildcardProbe.observe(snap, wcSC("bob", sameDigest, true, false), wcOpts("list", "", "pods", "n2"))

	// alice denies (list,"",pods,n2); bob permits it → fingerprints differ over the
	// cross-identity union {n1,n2} even though the digest collapsed them.
	if got := shadowWildcardDigestCollisionTotal.Load(); got != 1 {
		t.Fatalf("#368(b): a namespace-blind projection must fire collision_total (the cross-namespace break); got %d", got)
	}
}

func TestF368c_Converse_IdenticalAccessNoDrift(t *testing.T) {
	resetWildcardDigestProbeForTest()
	// IDENTICAL access (both cluster-permit). A correct projection gives them the
	// SAME digest; they must NOT collide despite being observed at different times
	// over different accumulated unions (the union-drift guard, fix (b)).
	withProfiles(t, map[string]*rbac.RequesterProfile{
		"alice": wcProfileCluster("list", "", "pods"),
		"bob":   wcProfileCluster("list", "", "pods"),
	})
	snap := wcSnap()
	const sameDigest = "correct-same-access-same-digest"
	wildcardProbe.observe(snap, wcSC("alice", sameDigest, true, false), wcOpts("list", "", "pods", "n1")) // union {n1}
	wildcardProbe.observe(snap, wcSC("bob", sameDigest, true, false), wcOpts("list", "", "pods", "n1"))   // ids {alice,bob}
	wildcardProbe.observe(snap, wcSC("bob", sameDigest, true, false), wcOpts("list", "", "pods", "n2"))   // union grows {n1,n2}

	if got := shadowWildcardDigestCollisionTotal.Load(); got != 0 {
		t.Fatalf("#368(c): identical-access identities must NOT collide (fix-(b) recompute over the current union removes union-drift); got %d", got)
	}
	// ...but the denominator still counts them (a real, certified observation).
	if got := shadowWildcardDigestObservedTotal.Load(); got != 1 {
		t.Fatalf("#368(c): observed_total must be 1 (two identities, no collision = the certified state); got %d", got)
	}
}

func TestF368d_Denominator_OneIdentityNotCertified(t *testing.T) {
	resetWildcardDigestProbeForTest()
	withProfiles(t, map[string]*rbac.RequesterProfile{"alice": wcProfileCluster("list", "", "pods")})
	snap := wcSnap()
	wildcardProbe.observe(snap, wcSC("alice", "d", true, false), wcOpts("list", "", "pods", "n1"))

	if got := shadowWildcardDigestObservedTotal.Load(); got != 0 {
		t.Fatalf("#368(d): a single identity must leave observed_total 0 (≥2 identities required = not-yet-exercised, NOT certified); got %d", got)
	}
	if got := shadowWildcardDigestCollisionTotal.Load(); got != 0 {
		t.Fatalf("#368(d): a single identity cannot collide; got %d", got)
	}
}

func TestF368e_Dark_NoKeyEffect(t *testing.T) {
	resetWildcardDigestProbeForTest()
	withProfiles(t, map[string]*rbac.RequesterProfile{
		"alice": wcProfileCluster("list", "", "pods"),
		"bob":   wcProfileDeny(),
	})
	snap := wcSnap()
	in := cache.ResolvedKeyInputs{CacheEntryClass: "widgets", Namespace: "n1", Name: "w"}
	before := cache.ComputeKey(in)
	// Drive a collision — the most active probe path.
	wildcardProbe.observe(snap, wcSC("alice", "same", true, false), wcOpts("list", "", "pods", "n1"))
	wildcardProbe.observe(snap, wcSC("bob", "same", true, false), wcOpts("list", "", "pods", "n1"))
	if after := cache.ComputeKey(in); after != before {
		t.Fatalf("#368(e) DARK: the probe must not perturb ComputeKey; %q -> %q", before, after)
	}
	// Sanity: the probe DID run (collision fired), so the no-key-effect is non-vacuous.
	if shadowWildcardDigestCollisionTotal.Load() == 0 {
		t.Fatalf("#368(e): precondition — the probe must have fired a collision for the dark check to be meaningful")
	}
}

func TestF368f_GatedCell_DiagnosticNotDetector(t *testing.T) {
	resetWildcardDigestProbeForTest()
	withProfiles(t, map[string]*rbac.RequesterProfile{
		"alice": wcProfileCluster("list", "", "pods"),
		"bob":   wcProfileDeny(),
	})
	snap := wcSnap()
	opts := wcOpts("list", "", "pods", "default")
	// GATED wildcard cell (today's state): different-access identities, one digest.
	wildcardProbe.observe(snap, wcSC("alice", "gated-digest", true, true), opts)
	wildcardProbe.observe(snap, wcSC("bob", "gated-digest", true, true), opts)

	if got := shadowWildcardDigestCollisionTotal.Load(); got != 0 {
		t.Fatalf("#368(f): a GATED cell must NOT touch the detector collision_total (it is never shared/can't leak); got %d", got)
	}
	if got := shadowWildcardDigestObservedTotal.Load(); got != 0 {
		t.Fatalf("#368(f): a GATED cell must NOT touch the detector denominator; got %d", got)
	}
	if got := shadowWildcardGatedDigestCollisionTotal.Load(); got != 1 {
		t.Fatalf("#368(f): a gated-cell collision must fire the expvar-only gated diagnostic; got %d", got)
	}
}

func TestF368wire_HookCallsProbe(t *testing.T) {
	rbac.SetShadowParityEnabled(true)
	t.Cleanup(func() { rbac.SetShadowParityEnabled(false) })
	resetWildcardDigestProbeForTest()
	withProfiles(t, map[string]*rbac.RequesterProfile{
		"alice": wcProfileCluster("list", "", "pods"),
		"bob":   wcProfileDeny(),
	})
	snap := wcSnap()
	// Drive the REAL hook (not observe directly): a gated wildcard cell, two
	// different-access identities. If the probe is wired into runShadowParityHook,
	// the gated diagnostic fires. Proves the seam is not dead code.
	for _, u := range []string{"alice", "bob"} {
		sc := wcSC(u, "gated-digest", true, true)
		ctx := context.WithValue(context.Background(), shadowCtxKey, sc)
		opts := wcOpts("list", "", "pods", "default")
		opts.Username = u
		runShadowParityHook(ctx, snap, opts, true)
	}
	if got := shadowWildcardGatedDigestCollisionTotal.Load(); got == 0 {
		t.Fatalf("WIRING (#368): runShadowParityHook did not call the wildcard digest probe (seam-is-a-floor: the probe can be correct yet never invoked)")
	}
}

// TestF368g_EvictionObservableVoidsCertification (TL condition): a cap eviction must
// be OBSERVABLE (evicted_total) so collision_total==0 can't be misread as certified
// while the LRU silently dropped a pair that WOULD have collided. Forces the entry
// cap to 1, thrashes it, and shows the alice/bob collision (same wrong digest,
// different access) is MISSED because alice's entry was evicted before bob arrived —
// with evicted_total recording it. Certification rule: evicted_total>0 VOIDS the window.
func TestF368g_EvictionObservableVoidsCertification(t *testing.T) {
	resetWildcardDigestProbeForTest()
	prev := maxWildcardDigestEntries
	maxWildcardDigestEntries = 1 // tiny cap → any second (cell,digest) evicts the first
	t.Cleanup(func() { maxWildcardDigestEntries = prev })

	withProfiles(t, map[string]*rbac.RequesterProfile{
		"alice": wcProfileCluster("list", "", "pods"), // permits
		"bob":   wcProfileDeny(),                      // denies — a real collision pair with alice under one digest
		"carol": wcProfileDeny(),                      // filler to force the eviction
	})
	snap := wcSnap()
	opts := wcOpts("list", "", "pods", "default")

	wildcardProbe.observe(snap, wcSC("alice", "dWrong", true, false), opts)                             // entry(cell,dWrong)
	wildcardProbe.observe(snap, wcSC("carol", "dOther", true, false), wcOpts("list", "", "pods", "n9")) // evicts (cell,dWrong)
	wildcardProbe.observe(snap, wcSC("bob", "dWrong", true, false), opts)                               // re-creates (cell,dWrong) fresh — alice gone

	if got := shadowWildcardDigestEvictedTotal.Load(); got == 0 {
		t.Fatalf("#368(g1/LRU): LRU eviction of a tracked (cell,digest) must bump evicted_total (make the false-negative OBSERVABLE); got 0")
	}
	if got := shadowWildcardDigestCollisionTotal.Load(); got != 0 {
		t.Fatalf("#368(g1/LRU): the evicted-pair collision must NOT be counted — this documents the cap false-negative; got %d", got)
	}
	// RED-provable: remove the evicted++ in evictLRULocked → this arm REDs.
}

// TestF368g2_CoordCapTruncationObservable — drop path (b): a NEW coordinate dropped
// because the per-entry coordUnion cap is hit must bump evicted_total. Without this,
// a coord-cap false-negative (a dimension the oracle never fingerprinted) would read
// clean. RED-provable: remove the coord-cap else-branch evicted++ → this arm REDs.
func TestF368g2_CoordCapTruncationObservable(t *testing.T) {
	resetWildcardDigestProbeForTest()
	prev := maxWildcardCoordsPerEntry
	maxWildcardCoordsPerEntry = 1
	t.Cleanup(func() { maxWildcardCoordsPerEntry = prev })

	withProfiles(t, map[string]*rbac.RequesterProfile{"alice": wcProfileCluster("list", "", "pods")})
	snap := wcSnap()
	// Same (cell,digest,identity); two DIFFERENT coordinates → the 2nd is dropped.
	wildcardProbe.observe(snap, wcSC("alice", "d", true, false), wcOpts("list", "", "pods", "n1"))
	wildcardProbe.observe(snap, wcSC("alice", "d", true, false), wcOpts("list", "", "pods", "n2"))

	if got := shadowWildcardDigestEvictedTotal.Load(); got == 0 {
		t.Fatalf("#368(g2/coord-cap): a coordUnion-cap truncation drops an observation and MUST bump evicted_total; got 0")
	}
}

// TestF368g3_IdentityCapTruncationObservable — drop path (c): a NEW identity dropped
// because the per-entry identities cap is hit must bump evicted_total (that dropped
// identity could be the colliding one). RED-provable: remove the identity-cap
// else-branch evicted++ → this arm REDs.
func TestF368g3_IdentityCapTruncationObservable(t *testing.T) {
	resetWildcardDigestProbeForTest()
	prev := maxWildcardIdentityPerEntry
	maxWildcardIdentityPerEntry = 1
	t.Cleanup(func() { maxWildcardIdentityPerEntry = prev })

	withProfiles(t, map[string]*rbac.RequesterProfile{
		"alice": wcProfileCluster("list", "", "pods"),
		"bob":   wcProfileDeny(),
	})
	snap := wcSnap()
	opts := wcOpts("list", "", "pods", "default")
	// Same (cell,digest); two DIFFERENT identities → the 2nd is dropped (cap=1).
	wildcardProbe.observe(snap, wcSC("alice", "d", true, false), opts)
	wildcardProbe.observe(snap, wcSC("bob", "d", true, false), opts)

	if got := shadowWildcardDigestEvictedTotal.Load(); got == 0 {
		t.Fatalf("#368(g3/identity-cap): an identities-cap truncation drops an identity and MUST bump evicted_total; got 0")
	}
}

// TestF368g4_EvictionVoidsCertTriad — a window that WOULD certify (a clean pair:
// collision==0 ∧ observed>0) is VOIDED by any eviction (evicted>0). Proves the triad
// is mechanically enforced, not just documented.
func TestF368g4_EvictionVoidsCertTriad(t *testing.T) {
	resetWildcardDigestProbeForTest()
	withProfiles(t, map[string]*rbac.RequesterProfile{
		"alice": wcProfileCluster("list", "", "pods"), // identical access → same digest, no collision
		"bob":   wcProfileCluster("list", "", "pods"),
	})
	snap := wcSnap()
	opts := wcOpts("list", "", "pods", "n1")
	// A clean, certified-looking pair: observed>0, collision==0, evicted==0.
	wildcardProbe.observe(snap, wcSC("alice", "dClean", true, false), opts)
	wildcardProbe.observe(snap, wcSC("bob", "dClean", true, false), opts)
	if !wcCertified() {
		t.Fatalf("precondition: a clean ≥2-identity same-access pair must be certified (collision=%d observed=%d evicted=%d)",
			shadowWildcardDigestCollisionTotal.Load(), shadowWildcardDigestObservedTotal.Load(), shadowWildcardDigestEvictedTotal.Load())
	}
	// Now force a drop elsewhere (identity-cap on a different digest) → evicted>0.
	prev := maxWildcardIdentityPerEntry
	maxWildcardIdentityPerEntry = 1
	t.Cleanup(func() { maxWildcardIdentityPerEntry = prev })
	wildcardProbe.observe(snap, wcSC("alice", "dOther", true, false), opts)
	wildcardProbe.observe(snap, wcSC("bob", "dOther", true, false), opts) // dropped → evicted>0
	if wcCertified() {
		t.Fatalf("#368(g4): evicted_total>0 must VOID certification even with collision==0 ∧ observed>0 (evicted=%d)",
			shadowWildcardDigestEvictedTotal.Load())
	}
}
