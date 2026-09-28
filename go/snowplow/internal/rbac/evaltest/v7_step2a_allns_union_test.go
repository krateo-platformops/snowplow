// v7_step2a_allns_union_test.go — v7 Step 2A F-C1.
//
// F-C1 (all-namespace union equivalence): selectRBCandidatesAllNS(snap, id)
// (pointer set) MUST equal the union over EVERY namespace of
// selectRBCandidates(snap, ns, id). The per-namespace selector is the
// trusted path (it, in turn, is guarded behavior-preserving by F-C0), so
// this proves the new flat all-namespace reverse index + selectRBCandidatesAllNS
// agree with it across the whole identity matrix.
//
// RED-first history: selectRBCandidatesAllNS did not exist before the
// implementation, so this file did not compile (compile-RED). A behavioural
// RED was also demonstrated by dropping one namespace's flat-index appends —
// the union then omits that namespace's RBs and F-C1 fails (see the dev
// report). This file stays GREEN only against the correct index.
package evaltest

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/rbac"

	rbacv1 "k8s.io/api/rbac/v1"
)

// rbPtrSet builds a pointer-keyed set (order-independent), the correct
// equality for the all-namespace union (an RB lives in exactly one
// namespace, so the per-ns union has no cross-ns duplicate; the all-ns
// function pointer-dedups multi-subject landings).
func rbPtrSet(rbs []*rbacv1.RoleBinding) map[*rbacv1.RoleBinding]struct{} {
	m := make(map[*rbacv1.RoleBinding]struct{}, len(rbs))
	for _, r := range rbs {
		m[r] = struct{}{}
	}
	return m
}

func TestV7Step2A_FC1_SelectRBCandidatesAllNS_UnionEquivalence(t *testing.T) {
	snap := buildV7CandidateSnapshot(t)
	idents := v7CandidateIdentities()

	// Union over EVERY namespace actually present in the snapshot (not just
	// the fixed query list) so a namespace the fixture forgot to list still
	// counts against the all-ns result.
	var allNamespaces []string
	for ns := range snap.RoleBindingsByNS {
		allNamespaces = append(allNamespaces, ns)
	}

	for _, id := range idents {
		opts := rbac.EvaluateOptions{Username: id.Username, Groups: id.Groups}

		// Trusted union over per-ns selectRBCandidates.
		unionSet := map[*rbacv1.RoleBinding]struct{}{}
		for _, ns := range allNamespaces {
			for _, rb := range rbac.SelectRBCandidatesForTest(snap, ns, opts) {
				unionSet[rb] = struct{}{}
			}
		}

		// The all-namespace function under test.
		allNS := rbac.SelectRBCandidatesAllNSForTest(snap, opts)
		allNSSet := rbPtrSet(allNS)

		// Pointer-dedup invariant: the returned slice has no duplicate.
		if len(allNSSet) != len(allNS) {
			t.Errorf("F-C1 user=%q: selectRBCandidatesAllNS returned duplicates: %d entries, %d distinct",
				id.Username, len(allNS), len(allNSSet))
		}

		// Set equality (both directions), reported by ns/name identity.
		var missingFromAllNS, extraInAllNS []string
		for rb := range unionSet {
			if _, ok := allNSSet[rb]; !ok {
				missingFromAllNS = append(missingFromAllNS, rb.Namespace+"/"+rb.Name)
			}
		}
		for rb := range allNSSet {
			if _, ok := unionSet[rb]; !ok {
				extraInAllNS = append(extraInAllNS, rb.Namespace+"/"+rb.Name)
			}
		}
		if len(missingFromAllNS) > 0 || len(extraInAllNS) > 0 {
			t.Errorf("F-C1 UNION MISMATCH user=%q:\n  union size=%d allNS size=%d\n  missing from allNS (under-inclusion): %v\n  extra in allNS (over-inclusion): %v",
				id.Username, len(unionSet), len(allNSSet),
				sortedStrings(missingFromAllNS), sortedStrings(extraInAllNS))
		}
	}
}

// TestV7Step2A_FC1_CrossNamespaceGather proves the all-namespace index
// actually GATHERS across namespaces, not merely agrees per-ns: alice has a
// RoleBinding in BOTH ns-a and ns-b, so her all-ns candidate set must
// contain RBs from both namespaces. (A per-ns-only index would satisfy the
// union check trivially if the fixture had a single-namespace subject; this
// arm removes that escape.)
func TestV7Step2A_FC1_CrossNamespaceGather(t *testing.T) {
	snap := buildV7CandidateSnapshot(t)
	opts := rbac.EvaluateOptions{Username: "alice", Groups: []string{"devs", "system:authenticated"}}

	allNS := rbac.SelectRBCandidatesAllNSForTest(snap, opts)

	sawNS := map[string]bool{}
	for _, rb := range allNS {
		sawNS[rb.Namespace] = true
	}
	for _, want := range []string{"ns-a", "ns-b"} {
		if !sawNS[want] {
			t.Errorf("F-C1 cross-ns gather: alice's all-ns candidates missing an RB from namespace %q; saw namespaces %v",
				want, sawNS)
		}
	}
}
