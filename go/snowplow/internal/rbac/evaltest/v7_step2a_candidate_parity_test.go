// v7_step2a_candidate_parity_test.go — v7 Step 2A falsifiers.
//
// Step 2A is a PURE ADDITIVE, DARK change: it adds the all-namespace
// RoleBinding-by-subject reverse index to the RBAC snapshot, extracts the
// shared routeRBSubjects routing out of selectRBCandidates, and adds the
// all-namespace selectRBCandidatesAllNS analogue. NOTHING is wired to
// serving; no cache key changes; no RBAC verdict changes.
//
// This file carries:
//
//   - F-C0 (the LOAD-BEARING behavior-preserving arm, resolves R2-M2):
//     selectRBCandidates must return the byte/identity-IDENTICAL ORDERED
//     candidate slice after the routeRBSubjects extraction as before it.
//     The reference (referenceSelectRBCandidatesOriginal) is a faithful,
//     frozen copy of the pre-extraction evaluate.go:512-543 body (WITH the
//     nil-map guards the extraction drops) — so it IS pre-change behavior.
//     A reorder / drop / add in the extraction, or a behavior change from
//     dropping the nil-map guards, goes RED. A captured sha256 golden over
//     the whole matrix is asserted too, so a drift of BOTH impl and
//     reference together is still caught.
//
//   - No-serving-change proof: a verdict+matchedBindingUID sha256 golden
//     over an identity×coordinate matrix, captured from pre-change
//     EvaluateRBAC. Because matchedBindingUID is first-match over the
//     ORDERED candidate set, this arm proves candidate ORDER preservation
//     end-to-end through the served evaluator, not just set-equality.
//
// F-C1 (all-namespace union equivalence) lives in
// v7_step2a_allns_union_test.go — it depends on selectRBCandidatesAllNS,
// which does not exist pre-implementation.
package evaltest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ─────────────────────────────────────────────────────────────────────
// Shared candidate fixture — multi-namespace, every Subject.Kind, dedup
// (multi-landing), catch-all, SA-subject-ns ≠ RB-ns, a subject present in
// two namespaces (so the all-namespace union must gather across ns), and
// an empty (no-RB) namespace.
// ─────────────────────────────────────────────────────────────────────

func rbWithUID(ns, name, uid string, subs ...rbacv1.Subject) *rbacv1.RoleBinding {
	rb := roleBinding(ns, name, "Role", name+"-role", subs...)
	rb.UID = types.UID(uid)
	return rb
}

// buildV7CandidateSnapshot returns a directly-constructed snapshot with
// its subject indexes built via cache.RebuildSubjectIndexesForTest (which
// runs the production rebuildSubjectIndexes, populating BOTH the per-ns and
// the new all-namespace flat maps). No watcher is needed: the routing
// functions read only the snapshot's index fields.
func buildV7CandidateSnapshot(t *testing.T) *cache.RBACSnapshot {
	t.Helper()
	snap := &cache.RBACSnapshot{
		RoleBindingsByNS:   map[string][]*rbacv1.RoleBinding{},
		ClusterRolesByName: map[string]*rbacv1.ClusterRole{},
		RolesByNSName:      map[string]*rbacv1.Role{},
	}

	// Cluster-wide bindings — every Kind + a multi-subject dedup case.
	snap.ClusterRoleBindings = []*rbacv1.ClusterRoleBinding{
		clusterRoleBinding("crb-user-alice", "r", userSubject("alice")),
		clusterRoleBinding("crb-group-devs", "r", groupSubject("devs")),
		clusterRoleBinding("crb-sysauth", "r", groupSubject("system:authenticated")),
		clusterRoleBinding("crb-sa-sys-sp", "r", saSubject("krateo-system", "snowplow")),
		clusterRoleBinding("crb-catchall", "r", rbacv1.Subject{Kind: "FutureKind", Name: "x"}),
		clusterRoleBinding("crb-multi", "r",
			userSubject("alice"),     // alice as User
			groupSubject("devs"),     // and via devs group  → dedup at lookup
			saSubject("ns-a", "sa1"), // and via SA
		),
	}

	// ns-a
	snap.RoleBindingsByNS["ns-a"] = []*rbacv1.RoleBinding{
		rbWithUID("ns-a", "rb-a-user-alice", "uid-a-alice", userSubject("alice")),
		rbWithUID("ns-a", "rb-a-group-devs", "uid-a-devs", groupSubject("devs")),
		rbWithUID("ns-a", "rb-a-sysauth", "uid-a-sysauth", groupSubject("system:authenticated")),
		rbWithUID("ns-a", "rb-a-sa-nsA-sa1", "uid-a-sa1", saSubject("ns-a", "sa1")),
		rbWithUID("ns-a", "rb-a-catchall", "uid-a-catch", rbacv1.Subject{Kind: "FutureKind", Name: "y"}),
		// multi-landing: alice as User AND via devs group → dedup preserves
		// first-occurrence order.
		rbWithUID("ns-a", "rb-a-multi", "uid-a-multi", userSubject("alice"), groupSubject("devs")),
		// SA synthetic group: system:serviceaccounts:ns-a matches any SA in ns-a.
		rbWithUID("ns-a", "rb-a-sasyngroup", "uid-a-sasyn", groupSubject("system:serviceaccounts:ns-a")),
	}
	// ns-b — alice ALSO has a binding here (union must gather ns-a + ns-b);
	// an SA subject whose namespace (ns-a) differs from the RB's ns (ns-b);
	// the all-SAs synthetic group.
	snap.RoleBindingsByNS["ns-b"] = []*rbacv1.RoleBinding{
		rbWithUID("ns-b", "rb-b-user-alice", "uid-b-alice", userSubject("alice")),
		rbWithUID("ns-b", "rb-b-group-ops", "uid-b-ops", groupSubject("ops")),
		rbWithUID("ns-b", "rb-b-sa-nsA-sa1", "uid-b-sa1", saSubject("ns-a", "sa1")),
		rbWithUID("ns-b", "rb-b-allsas", "uid-b-allsas", groupSubject("system:serviceaccounts")),
	}
	// ns-c
	snap.RoleBindingsByNS["ns-c"] = []*rbacv1.RoleBinding{
		rbWithUID("ns-c", "rb-c-group-devs", "uid-c-devs", groupSubject("devs")),
		rbWithUID("ns-c", "rb-c-catchall", "uid-c-catch", rbacv1.Subject{Kind: "OtherKind", Name: "z"}),
		rbWithUID("ns-c", "rb-c-sa-nsC-sa2", "uid-c-sa2", saSubject("ns-c", "sa2")),
	}
	// "ns-empty" has no RBs on purpose (queried below → nil candidate set).

	cache.RebuildSubjectIndexesForTest(snap)
	return snap
}

// v7CandidateIdentities is the identity matrix for F-C0/F-C1. Ordered and
// fixed so the golden is deterministic.
type v7Ident struct {
	Username string
	Groups   []string
}

func v7CandidateIdentities() []v7Ident {
	return []v7Ident{
		{Username: "alice", Groups: []string{"devs", "system:authenticated"}},
		{Username: "alice", Groups: nil}, // user-only + implicit system:authenticated add
		{Username: "bob", Groups: []string{"ops", "system:authenticated"}},
		{Username: "system:serviceaccount:ns-a:sa1", Groups: []string{"system:authenticated"}},
		{Username: "system:serviceaccount:ns-c:sa2", Groups: []string{"system:authenticated"}},
		{Username: "system:serviceaccount:krateo-system:snowplow", Groups: []string{"system:authenticated"}},
		{Username: "carol", Groups: []string{"system:authenticated"}}, // sysauth + catch-all only
		{Username: "dave", Groups: []string{"devs", "ops", "system:authenticated"}},
		{Username: "", Groups: nil}, // unauthenticated: catch-all only, no system:authenticated
	}
}

// v7CandidateNamespaces — including an empty namespace with no RBs.
func v7CandidateNamespaces() []string {
	return []string{"ns-a", "ns-b", "ns-c", "ns-empty"}
}

// referenceSelectRBCandidatesOriginal is a FROZEN, faithful copy of the
// pre-extraction selectRBCandidates body (evaluate.go:512-543 at
// origin/main a6b9d348), INCLUDING the nil-map guards the extraction drops.
// It is pre-change behavior expressed as code. SA-username parsing and the
// synthetic-group expansion route through the exported drift-guard hooks so
// the reference stays byte-faithful to the production helpers.
func referenceSelectRBCandidatesOriginal(snap *cache.RBACSnapshot, ns string, username string, groups []string) []*rbacv1.RoleBinding {
	if snap == nil || ns == "" {
		return nil
	}
	saNS, saName, isSA := rbac.ParseServiceAccountUsernameForTest(username)
	effGroups := rbac.EffectiveGroupsForTest(groups, isSA, saNS)

	seen := make(map[*rbacv1.RoleBinding]struct{})
	var out []*rbacv1.RoleBinding
	add := func(rbs []*rbacv1.RoleBinding) {
		for _, r := range rbs {
			if _, ok := seen[r]; ok {
				continue
			}
			seen[r] = struct{}{}
			out = append(out, r)
		}
	}

	if userInner := snap.RBsByUserByNS[ns]; userInner != nil && username != "" {
		add(userInner[username])
	}
	if groupInner := snap.RBsByGroupByNS[ns]; groupInner != nil {
		for _, g := range effGroups {
			add(groupInner[g])
		}
		if username != "" {
			add(groupInner["system:authenticated"])
		}
	}
	if isSA {
		if saInner := snap.RBsByServiceAccountByNS[ns]; saInner != nil {
			add(saInner[saNS+"/"+saName])
		}
	}
	add(snap.RBsCatchAllByNS[ns])

	return out
}

func rbIDs(rbs []*rbacv1.RoleBinding) []string {
	out := make([]string, len(rbs))
	for i, rb := range rbs {
		out[i] = rb.Namespace + "/" + rb.Name
	}
	return out
}

func crbIDs(crbs []*rbacv1.ClusterRoleBinding) []string {
	out := make([]string, len(crbs))
	for i, c := range crbs {
		out[i] = c.Name
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────
// F-C0 — routeRBSubjects extraction is behavior-preserving (ORDERED).
// ─────────────────────────────────────────────────────────────────────

// fc0MatrixGolden is the sha256 (hex) of the ordered candidate matrix over
// buildV7CandidateSnapshot × v7CandidateIdentities × v7CandidateNamespaces
// (RB) + × identities (CRB). Captured from pre-change code
// (origin/main a6b9d348). A mismatch means the extraction perturbed the
// ordered candidate set — F-C0 RED.
const fc0MatrixGolden = "679b100758afd0b1bc9c11ab17afcdd13ed53dff020329638a512f152ebc46cb"

func TestV7Step2A_FC0_SelectRBCandidatesOrderedGolden(t *testing.T) {
	snap := buildV7CandidateSnapshot(t)
	idents := v7CandidateIdentities()
	namespaces := v7CandidateNamespaces()

	var b strings.Builder
	for _, id := range idents {
		opts := rbac.EvaluateOptions{Username: id.Username, Groups: id.Groups}

		// CRB anchor (selectCRBCandidates is NOT refactored in Step A; it is
		// a stability anchor here).
		gotCRB := rbac.SelectCRBCandidatesForTest(snap, opts)
		fmt.Fprintf(&b, "CRB|%s|%v\n", id.Username, crbIDs(gotCRB))

		for _, ns := range namespaces {
			got := rbac.SelectRBCandidatesForTest(snap, ns, opts)
			ref := referenceSelectRBCandidatesOriginal(snap, ns, id.Username, id.Groups)

			// LOAD-BEARING: ordered, identity-identical vs the frozen
			// pre-change reference. Pointer-identity is asserted directly so
			// this is not merely a name compare.
			if len(got) != len(ref) {
				t.Errorf("F-C0 length divergence user=%q ns=%q: got %d, ref %d\n  got=%v\n  ref=%v",
					id.Username, ns, len(got), len(ref), rbIDs(got), rbIDs(ref))
			} else {
				for i := range got {
					if got[i] != ref[i] {
						t.Errorf("F-C0 ORDER/identity divergence user=%q ns=%q pos=%d: got %s, ref %s\n  got=%v\n  ref=%v",
							id.Username, ns, i, got[i].Namespace+"/"+got[i].Name,
							ref[i].Namespace+"/"+ref[i].Name, rbIDs(got), rbIDs(ref))
						break
					}
				}
			}
			fmt.Fprintf(&b, "RB|%s|%s|%v\n", id.Username, ns, rbIDs(got))
		}
	}

	sum := sha256.Sum256([]byte(b.String()))
	got := hex.EncodeToString(sum[:])
	if fc0MatrixGolden == "REPLACE_FC0" {
		t.Fatalf("F-C0 CAPTURE: fc0MatrixGolden = %q", got)
	}
	if got != fc0MatrixGolden {
		t.Errorf("F-C0 matrix golden mismatch:\n  got  %s\n  want %s\n(the ordered candidate matrix changed — extraction is NOT behavior-preserving)", got, fc0MatrixGolden)
	}
}

// ─────────────────────────────────────────────────────────────────────
// No-serving-change proof — EvaluateRBAC verdict+UID table byte-identical.
// ─────────────────────────────────────────────────────────────────────

// verdictTableGolden is the sha256 (hex) of the (allowed, matchedBindingUID)
// table over the identity×coordinate matrix below, captured from pre-change
// EvaluateRBAC (origin/main a6b9d348). matchedBindingUID is first-match over
// the ORDERED candidate set, so this arm proves the served evaluator's
// verdict AND candidate ordering are untouched by Step 2A.
const verdictTableGolden = "930a9e2908dd12e3538bf279a1431d544a1052d9172524a5c02b5c4d0faeb8b3"

func TestV7Step2A_NoServingChange_VerdictTableGolden(t *testing.T) {
	crbAdmin := clusterRole("cr-admin", rule([]string{"*"}, []string{"*"}, []string{"*"}))
	crReader := clusterRole("cr-reader",
		rule([]string{"templates.krateo.io"}, []string{"restactions"}, []string{"get", "list"}))
	roleEditor := role("ns-a", "role-editor",
		rule([]string{""}, []string{"configmaps"}, []string{"get", "list", "create"}))

	crbAliceAdmin := clusterRoleBinding("crb-alice-admin", "cr-admin", userSubject("alice"))
	crbAliceAdmin.UID = types.UID("U1")
	crbDevsReader := clusterRoleBinding("crb-devs-reader", "cr-reader", groupSubject("devs"))
	crbDevsReader.UID = types.UID("U2")

	rbBobEditor := roleBinding("ns-a", "rb-bob-editor", "Role", "role-editor", userSubject("bob"))
	rbBobEditor.UID = types.UID("U3")
	rbOpsReader := roleBinding("ns-b", "rb-ops-reader", "ClusterRole", "cr-reader", groupSubject("ops"))
	rbOpsReader.UID = types.UID("U4")
	rbSa1Editor := roleBinding("ns-a", "rb-sa1-editor", "Role", "role-editor", saSubject("ns-a", "sa1"))
	rbSa1Editor.UID = types.UID("U5")

	newTestWatcher(t,
		crbAdmin, crReader, roleEditor,
		crbAliceAdmin, crbDevsReader,
		rbBobEditor, rbOpsReader, rbSa1Editor,
	)

	idents := []v7Ident{
		{Username: "alice", Groups: []string{"system:authenticated"}},
		{Username: "bob", Groups: []string{"system:authenticated"}},
		{Username: "dave", Groups: []string{"devs", "system:authenticated"}},
		{Username: "erin", Groups: []string{"ops", "system:authenticated"}},
		{Username: "system:serviceaccount:ns-a:sa1", Groups: []string{"system:authenticated"}},
		{Username: "carol", Groups: []string{"system:authenticated"}},
		{Username: "", Groups: nil},
	}
	verbs := []string{"get", "list", "create", "delete"}
	grs := []struct{ group, resource string }{
		{"templates.krateo.io", "restactions"},
		{"", "configmaps"},
	}
	namespaces := []string{"ns-a", "ns-b", ""}
	names := []string{"", "foo"}

	var b strings.Builder
	for _, id := range idents {
		for _, v := range verbs {
			for _, gr := range grs {
				for _, ns := range namespaces {
					for _, nm := range names {
						allowed, uid, err := rbac.EvaluateRBAC(context.Background(), rbac.EvaluateOptions{
							Username:  id.Username,
							Groups:    id.Groups,
							Verb:      v,
							Group:     gr.group,
							Resource:  gr.resource,
							Namespace: ns,
							Name:      nm,
						})
						if err != nil {
							t.Fatalf("EvaluateRBAC(user=%q v=%s gr=%s/%s ns=%q name=%q): %v",
								id.Username, v, gr.group, gr.resource, ns, nm, err)
						}
						fmt.Fprintf(&b, "%s|%v|%s|%s|%s|%s|%s|%v|%s\n",
							id.Username, id.Groups, v, gr.group, gr.resource, ns, nm, allowed, uid)
					}
				}
			}
		}
	}

	sum := sha256.Sum256([]byte(b.String()))
	got := hex.EncodeToString(sum[:])
	if verdictTableGolden == "REPLACE_VERDICT" {
		t.Fatalf("VERDICT CAPTURE: verdictTableGolden = %q", got)
	}
	if got != verdictTableGolden {
		t.Errorf("no-serving-change VIOLATION: verdict+UID table golden mismatch:\n  got  %s\n  want %s", got, verdictTableGolden)
	}
}

// sortedStrings is a tiny determinism helper (kept local to avoid touching
// production sort helpers). Used by F-C1's set comparison.
func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}
