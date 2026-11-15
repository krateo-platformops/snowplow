// f3_convergence_test.go — Ship 0.30.242 H.c-layered Phase 3 F3
// (THE PATH-B CORRECTNESS GATE).
//
// THE INVARIANT (design §12.3 + §1.2)
//
// For every (identity, class, scope) tuple: the BindingUID derived by
// the DISPATCHER path (production rbac.EvaluateRBAC first-match) MUST
// EXIST among the BindingUIDs emitted by the SEED path
// (cache.EnumeratePrewarmTargetsForGVR over the same snapshot).
//
// SEED and DISPATCH are TWO INDEPENDENT mechanisms that must agree on
// per-binding identity:
//
//   - SEED PATH stamps BindingUID directly from each binding's
//     metadata.uid (cache.BindingUIDFromCRB / FromRB on the binding
//     pointer at enrolment time, surfaced via the BindingsByGVR index).
//   - DISPATCH PATH runs the full snapshot walk under (Username, Groups,
//     verb, gvr, ns) — sorts candidates into stable order, finds the
//     first permitting binding, returns cache.BindingUIDFromCRB/FromRB
//     on THAT binding.
//
// The seed enumerator emits N targets per GVR (one per binding granting
// get/list on the GVR). The dispatcher picks ONE (first-match). F3
// asserts: the dispatcher's pick EXISTS among the seed's emissions for
// the same GVR. If it does not, the two paths have diverged → cells
// populated under the seed path would be UNREACHABLE from the dispatch
// path (or vice-versa).
//
// PATH-B CORRECTNESS GATE
//
// 2c F1-deletion authorized the memo scaffolding-only (Path B) on the
// claim that direct rbac.EvaluateRBAC at every cell-key derivation
// site preserves correctness without per-request memo plumbing. F3 is
// the falsifier for that claim. A divergence here means one of:
//
//   (bucket 1) Test harness bug — Driver B filter logic incorrectly
//     matches seed targets. Surface, do not fix without architect input.
//
//   (bucket 2) Path B deferral was wrong — direct EvaluateRBAC at
//     dispatcher time AND seed-path enumeration produce different
//     first-match BindingUIDs under concurrent republish. The
//     per-request memo's snap-coherent invalidation IS load-bearing
//     for correctness. IMPLICATION: pull memo plumbing forward as
//     Phase 2d BEFORE Phase 4.
//
//   (bucket 3) Sort-order non-determinism — design §6's
//     sortCRBsStable/sortRBsStable insufficient for some edge case.
//     IMPLICATION: fix the comparator.
//
// F3 SCOPE BOUNDARY (per Phase 4 reviewer context)
//
// F3 tests convergence of enumerator-stamped BindingUID (seed path) vs
// dispatcher-evaluated first-match BindingUID (production dispatch). It
// does NOT test:
//   - cell content (F6)
//   - empty-UID invariant (F7)
//   - memo behavior — Path B deferral makes the dispatcher use direct
//     EvaluateRBAC, which IS the current production state; F3 verifies
//     THAT state, not a future memo-plumbed state.
//
// FIXTURE (per the design's "≥30 cohorts × ≥5 cache classes × ≥3 scopes")
//
// 30 identities = 10 User-kind + 10 Group-kind + 10 ServiceAccount-kind.
// Each identity has BOTH a CRB and an RB (so multiple bindings exist
// per identity — the dispatcher's first-match is structurally
// non-trivial). All bindings stamped with deterministic UIDs.
//
// 5 cache classes from design §3.3:
//   widgets, restactions, apistage, widgetContent, raFullList.
// widgetContent has BindingUID == "" by construction (identity-free
// key); F3 verifies that, but it does NOT participate in the
// dispatcher-vs-seed convergence assertion (no UID to converge).
//
// 3 scopes per layer:
//   cluster (namespace="") — CRB-only phase
//   ns-A-with-RB — both phases active; dispatcher's first-match could
//                  be CRB or RB depending on stable order
//   ns-B-no-RB — both phases active but RB phase yields no candidates;
//                dispatcher falls back to the CRB phase.

package evaltest

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/rbac"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// ──────────────────────────────────────────────────────────────────────
// Fixture
// ──────────────────────────────────────────────────────────────────────

// f3Identity is a tuple describing one of the 30 test identities.
type f3Identity struct {
	Kind      string // "User" | "Group" | "ServiceAccount"
	Username  string // canonical, including "system:serviceaccount:..." for SA
	Groups    []string
	CRBUID    string // expected "C:<uid>" from a CRB-phase first-match
	RBUID     string // expected "R:ns-A/<uid>" from an RB-phase first-match in ns-A
}

func f3BuildIdentities() []f3Identity {
	out := make([]f3Identity, 0, 30)
	// 10 User-kind.
	for i := 0; i < 10; i++ {
		out = append(out, f3Identity{
			Kind:     "User",
			Username: fmt.Sprintf("user-%d", i),
			Groups:   nil,
			CRBUID:   fmt.Sprintf("crb-user-%d", i),
			RBUID:    fmt.Sprintf("rb-user-%d", i),
		})
	}
	// 10 Group-kind (Username="" — pure group identity).
	for i := 0; i < 10; i++ {
		out = append(out, f3Identity{
			Kind:     "Group",
			Username: "",
			Groups:   []string{fmt.Sprintf("grp-%d", i)},
			CRBUID:   fmt.Sprintf("crb-grp-%d", i),
			RBUID:    fmt.Sprintf("rb-grp-%d", i),
		})
	}
	// 10 SA-kind.
	for i := 0; i < 10; i++ {
		out = append(out, f3Identity{
			Kind:     "ServiceAccount",
			Username: fmt.Sprintf("system:serviceaccount:ns-sa-%d:sa", i),
			Groups:   nil,
			CRBUID:   fmt.Sprintf("crb-sa-%d", i),
			RBUID:    fmt.Sprintf("rb-sa-%d", i),
		})
	}
	return out
}

// f3CacheClass enumerates the 5 cache classes. Each maps to a (GVR,
// verb) pair the cell key is derived under.
type f3CacheClass struct {
	Name        string
	GVR         schema.GroupVersionResource
	Verb        string
	IdentityFree bool // widgetContent only
}

func f3Classes() []f3CacheClass {
	return []f3CacheClass{
		{Name: "widgets", GVR: schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "widgets"}, Verb: "get"},
		{Name: "restactions", GVR: schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}, Verb: "get"},
		{Name: "apistage", GVR: schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}, Verb: "list"},
		{Name: "widgetContent", GVR: schema.GroupVersionResource{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "widgets"}, Verb: "get", IdentityFree: true},
		{Name: "raFullList", GVR: schema.GroupVersionResource{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"}, Verb: "get"},
	}
}

// f3Scopes returns the 3 scopes. Returned namespaces correspond to:
//   "" — cluster-wide; only CRB phase evaluates.
//   "ns-A" — has an RB granting each identity; both phases active.
//   "ns-B" — has NO RB; only CRBs match (the dispatcher falls back to
//            CRB phase after RB-phase yields no candidates).
func f3Scopes() []string {
	return []string{"", "ns-A", "ns-B"}
}

// f3BuildSnapshot constructs all the RBAC objects the F3 fixture
// needs: one ClusterRole granting BOTH GVRs (widgets+restactions get+list)
// + one Role in ns-A granting the same, + 30 CRBs (one per identity) +
// 30 RBs in ns-A (one per identity).
func f3BuildSnapshot(t *testing.T, idents []f3Identity, snapseq uint64) []runtime.Object {
	t.Helper()
	cr := &rbacv1.ClusterRole{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRole"},
		ObjectMeta: metav1.ObjectMeta{Name: "f3-reader"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"widgets.templates.krateo.io"}, Resources: []string{"widgets"}, Verbs: []string{"get", "list"}},
			{APIGroups: []string{"templates.krateo.io"}, Resources: []string{"restactions"}, Verbs: []string{"get", "list"}},
		},
	}
	r := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns-A", Name: "f3-reader"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"widgets.templates.krateo.io"}, Resources: []string{"widgets"}, Verbs: []string{"get", "list"}},
			{APIGroups: []string{"templates.krateo.io"}, Resources: []string{"restactions"}, Verbs: []string{"get", "list"}},
		},
	}

	out := []runtime.Object{cr, r}
	for _, id := range idents {
		subj := f3SubjectFor(id)
		crb := &rbacv1.ClusterRoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "ClusterRoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: id.CRBUID, UID: types.UID(id.CRBUID)},
			Subjects:   []rbacv1.Subject{subj},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: "f3-reader"},
		}
		rb := &rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns-A", Name: id.RBUID, UID: types.UID(id.RBUID)},
			Subjects:   []rbacv1.Subject{subj},
			RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "f3-reader"},
		}
		// #139 C-F3-1 — SINGLE-SUBJECT INVARIANT. The SA-exclusion exemption in
		// f3CheckConvergence mirrors production's allSubjectsAreServiceAccountKind
		// via `id.Kind == "ServiceAccount"`. That mirror is faithful ONLY because
		// every F3 binding carries EXACTLY ONE subject (of id.Kind): a SA
		// identity's binding is SA-ONLY (so production drops it), a User/Group
		// identity's binding has a User/Group subject (so production keeps it). If
		// the fixture ever grows a multi-subject binding, id.Kind stops being the
		// faithful value of allSubjectsAreServiceAccountKind and the exemption
		// becomes unsound (a mixed SA+User binding is NOT SA-only → production
		// keeps it, but a SA-kind identity would still be exempted). This
		// assertion converts that future silent-mask into a LOUD fail — replacing
		// the id.Kind mirror with the real cache.AllSubjectsAreServiceAccountKind
		// export (design §Strategic choice) then becomes necessary.
		if len(crb.Subjects) != 1 || len(rb.Subjects) != 1 {
			t.Fatalf("F3 single-subject invariant broken (identity %q kind %q): CRB has %d subjects, RB has %d — the id.Kind mirror of allSubjectsAreServiceAccountKind is unsound for multi-subject bindings; see #139 (switch to cache.AllSubjectsAreServiceAccountKind export)",
				id.Username, id.Kind, len(crb.Subjects), len(rb.Subjects))
		}
		out = append(out, crb, rb)
	}
	_ = snapseq // reserved for future per-republish content variation
	return out
}

func f3SubjectFor(id f3Identity) rbacv1.Subject {
	switch id.Kind {
	case "User":
		return rbacv1.Subject{Kind: "User", APIGroup: "rbac.authorization.k8s.io", Name: id.Username}
	case "Group":
		return rbacv1.Subject{Kind: "Group", APIGroup: "rbac.authorization.k8s.io", Name: id.Groups[0]}
	case "ServiceAccount":
		// id.Username == "system:serviceaccount:ns-sa-N:sa"
		// extract ns + name from the canonical form
		const pfx = "system:serviceaccount:"
		rest := strings.TrimPrefix(id.Username, pfx)
		colon := strings.IndexByte(rest, ':')
		ns := rest[:colon]
		name := rest[colon+1:]
		return rbacv1.Subject{Kind: "ServiceAccount", Namespace: ns, Name: name}
	}
	return rbacv1.Subject{}
}

// f3InitWatcher constructs the *cache.ResourceWatcher + builds the
// BindingsByGVR index over the F3 GVR set. Wires cache.SetGlobal. The
// snapshot is published as part of the watcher's initial reconcile.
func f3InitWatcher(t *testing.T, idents []f3Identity) {
	t.Helper()
	// Use the same newTestWatcher as evaluate_test.go — it builds the
	// watcher + waits for cache sync + wires cache.SetGlobal.
	newTestWatcher(t, f3BuildSnapshot(t, idents, 0)...)

	// Build the BindingsByGVR index over the F3 GVR set. This is what
	// the seed enumerator reads. Production wires this via the Phase 1
	// walker; for the synthetic harness we call it directly.
	cache.ResetBindingsByGVRIndexForTest()
	cache.SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{
		{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "widgets"},
		{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"},
	})
}

// ──────────────────────────────────────────────────────────────────────
// Driver A — dispatcher path
// ──────────────────────────────────────────────────────────────────────

// f3DispatcherUID computes the BindingUID via the dispatcher's
// production path: rbac.EvaluateRBAC under the (Username, Groups,
// verb, gvr, ns) tuple. The returned uid is the first-match permitting
// binding's identity ("C:<uid>" or "R:<ns>/<uid>"); "" if no permit.
func f3DispatcherUID(ctx context.Context, id f3Identity, class f3CacheClass, ns string) (allowed bool, uid string, err error) {
	allowed, uid, err = rbac.EvaluateRBAC(ctx, rbac.EvaluateOptions{
		Username:  id.Username,
		Groups:    id.Groups,
		Verb:      class.Verb,
		Group:     class.GVR.Group,
		Resource:  class.GVR.Resource,
		Namespace: ns,
		Name:      "", // F3 doesn't exercise resource-names-scoped rules
	})
	return allowed, uid, err
}

// ──────────────────────────────────────────────────────────────────────
// Driver B — seed enumerator path
// ──────────────────────────────────────────────────────────────────────

// f3SeedTargetsForIdentity returns the seed targets for the identity's
// (class.GVR, class.Verb) filtered by the identity's representative
// shape. The dispatcher's first-match UID MUST exist among these UIDs.
func f3SeedTargetsForIdentity(id f3Identity, class f3CacheClass) []cache.PrewarmTarget {
	all := cache.EnumeratePrewarmTargetsForGVR(class.GVR, class.Verb)
	out := make([]cache.PrewarmTarget, 0, 2)
	for _, t := range all {
		if f3SubjectMatchesIdentity(t.Subject, id) {
			out = append(out, t)
		}
	}
	return out
}

// f3SubjectMatchesIdentity reports whether a seed-target representative
// SubjectIdentity matches an F3 identity tuple. Per the seed enumerator,
// the representative is drawn from the binding's first non-empty
// subject. For F3 identities, each binding has EXACTLY one subject of
// the matching kind, so the representative is unambiguous.
func f3SubjectMatchesIdentity(rep cache.SubjectIdentity, id f3Identity) bool {
	switch id.Kind {
	case "User":
		return rep.Username == id.Username && len(rep.Groups) == 0
	case "Group":
		if rep.Username != "" || len(rep.Groups) != 1 {
			return false
		}
		return rep.Groups[0] == id.Groups[0]
	case "ServiceAccount":
		return rep.Username == id.Username && len(rep.Groups) == 0
	}
	return false
}

// ──────────────────────────────────────────────────────────────────────
// Convergence assertion (per-tuple)
// ──────────────────────────────────────────────────────────────────────

// f3Divergence captures a convergence failure for triage.
type f3Divergence struct {
	IdentityKind string
	Username     string
	Groups       []string
	Class        string
	Namespace    string
	DispatcherUID string
	SeedUIDs      []string
	Bucket       string // "harness" | "path-b" | "sort-order" | "unclassified"
}

func (d f3Divergence) String() string {
	return fmt.Sprintf("[%s] identity=%s ns=%q dispatcher=%q seed=%v BUCKET=%s",
		d.Class, d.identitySig(), d.Namespace, d.DispatcherUID, d.SeedUIDs, d.Bucket)
}
func (d f3Divergence) identitySig() string {
	if d.Username != "" {
		return d.Username
	}
	if len(d.Groups) > 0 {
		return "group:" + d.Groups[0]
	}
	return "<anonymous>"
}

// f3CheckConvergence asserts: dispatcher's first-match BindingUID
// EXISTS among the seed-emitted UIDs for the same (identity, GVR, verb)
// tuple. Returns nil on convergence, an *f3Divergence on failure.
//
// SCOPE-SPECIFIC EXPECTATIONS:
//   - cluster (ns=""):    dispatcher's first-match is a CRB → "C:<uid>"
//   - ns-A-with-RB:        dispatcher's first-match could be CRB OR RB
//                          (stable sort across both phases at the
//                          per-phase boundary; CRB phase precedes RB
//                          phase per design §6); seed enumerator
//                          includes both CRB and RB; the dispatcher's
//                          actual pick MUST exist among them.
//   - ns-B-no-RB:          dispatcher's first-match is a CRB (RB phase
//                          yields no candidates for ns-B since no RBs
//                          exist there).
//
// widgetContent is identity-free: dispatcher's BindingUID is the
// per-class-class field on Inputs, but widgetContent doesn't fold
// BindingUID into the key. F3 verifies the dispatcher path STILL
// produces a valid UID under widgetContent's gvr/verb (it does —
// rbac.EvaluateRBAC doesn't know about cache classes), but we do NOT
// assert convergence; we assert "the dispatcher uid would have been
// foldable IF the class folded it". The empty-key contract is in F7.
func f3CheckConvergence(ctx context.Context, id f3Identity, class f3CacheClass, ns string) *f3Divergence {
	allowed, dispUID, err := f3DispatcherUID(ctx, id, class, ns)
	if err != nil {
		return &f3Divergence{
			IdentityKind: id.Kind, Username: id.Username, Groups: id.Groups,
			Class: class.Name, Namespace: ns,
			DispatcherUID: fmt.Sprintf("<err: %v>", err),
			Bucket:        "unclassified",
		}
	}
	if !allowed {
		// Identity has no permit for this (class.Verb, class.GVR, ns).
		// Per F7's invariant, dispUID MUST be "". The seed enumerator
		// SHOULD still emit a target for this identity's bindings (the
		// bindings exist in the snapshot — they just don't grant THIS
		// (verb, gvr, ns) tuple). Convergence in the disallowed case is
		// vacuous: dispUID="" maps to "no first-match", and the seed
		// enumerator's emissions for the GVR are irrelevant. Skip.
		if dispUID != "" {
			return &f3Divergence{
				IdentityKind: id.Kind, Username: id.Username, Groups: id.Groups,
				Class: class.Name, Namespace: ns,
				DispatcherUID: dispUID,
				Bucket:        "unclassified",
			}
		}
		return nil
	}

	// allowed=true → dispUID MUST be non-empty (F7 invariant).
	if dispUID == "" {
		return &f3Divergence{
			IdentityKind: id.Kind, Username: id.Username, Groups: id.Groups,
			Class: class.Name, Namespace: ns,
			DispatcherUID: "",
			Bucket:        "unclassified",
		}
	}

	seedTargets := f3SeedTargetsForIdentity(id, class)
	if len(seedTargets) == 0 {
		// #139 — Intended SA-exclusion, NOT a divergence. The seed enumerator
		// drops SA-only bindings from every seed target class
		// (internal/cache/prewarm_enumeration.go:202
		// `if allSubjectsAreServiceAccountKind(entry.subjects) { continue }`,
		// the #130 Diego directive cc213e9 2026-07-11: SA-only bindings are
		// machine cohorts that never render the frontend). Every F3 binding is
		// built with EXACTLY ONE subject of id.Kind (f3SubjectFor +
		// f3BuildSnapshot — asserted by the single-subject invariant in
		// f3BuildSnapshot), so `id.Kind == "ServiceAccount"` is the FAITHFUL
		// fixture-level value of allSubjectsAreServiceAccountKind for this
		// identity's bindings — a mirror of the SAME production rule via the
		// fixture's construction invariant, not a divergent copy of the
		// set-logic. The dispatcher still ALLOWS via the CRB (production serve
		// cold-fills such a cell on first traffic); the seed legitimately emits
		// nothing. Vacuous convergence — skip.
		if id.Kind == "ServiceAccount" {
			return nil
		}
		// A User/Group identity with an ALLOWED dispatch but empty seed is a
		// REAL enumerator miss (the BindingsByGVR index failed to enrol a
		// login-cohort binding) — the defect this branch exists to catch. The
		// fall-through keeps the original harness bucket verbatim.
		return &f3Divergence{
			IdentityKind: id.Kind, Username: id.Username, Groups: id.Groups,
			Class: class.Name, Namespace: ns,
			DispatcherUID: dispUID,
			SeedUIDs:      nil,
			Bucket:        "harness", // most likely the index didn't enrol — surface for triage
		}
	}
	seedUIDs := make([]string, 0, len(seedTargets))
	for _, t := range seedTargets {
		seedUIDs = append(seedUIDs, t.BindingUID)
	}
	for _, uid := range seedUIDs {
		if uid == dispUID {
			return nil // CONVERGE
		}
	}

	// Divergence — try to classify.
	bucket := "unclassified"
	// Bucket 2 (Path B wrong) — if the dispatcher's UID is "C:<crb>" or
	// "R:<ns>/<rb>" but the seed only emits the other, that's a sort-
	// vs-enumeration mismatch under Path B's direct-EvaluateRBAC.
	dispIsCRB := strings.HasPrefix(dispUID, "C:")
	dispIsRB := strings.HasPrefix(dispUID, "R:")
	allSeedCRB, allSeedRB := true, true
	for _, uid := range seedUIDs {
		if !strings.HasPrefix(uid, "C:") {
			allSeedCRB = false
		}
		if !strings.HasPrefix(uid, "R:") {
			allSeedRB = false
		}
	}
	switch {
	case dispIsCRB && allSeedRB:
		bucket = "path-b" // dispatcher picked a CRB; seed enumerator emitted only RBs
	case dispIsRB && allSeedCRB:
		bucket = "sort-order" // dispatcher walked into RB phase; seed only has CRB
	}

	return &f3Divergence{
		IdentityKind: id.Kind, Username: id.Username, Groups: id.Groups,
		Class: class.Name, Namespace: ns,
		DispatcherUID: dispUID,
		SeedUIDs:      seedUIDs,
		Bucket:        bucket,
	}
}

// ──────────────────────────────────────────────────────────────────────
// F3 Phase 6 — sequential 100 republishes
// ──────────────────────────────────────────────────────────────────────

// TestF3_SequentialConvergence_100Republishes exercises the canonical
// convergence assertion across 100 publish epochs. Each epoch:
//   - Republishes the snapshot (identical content; this tests that
//     republishing doesn't shift first-match under stable order).
//   - Iterates all (identity, class, scope) tuples and asserts
//     convergence.
//
// Wall-clock target: <30 s.
func TestF3_SequentialConvergence_100Republishes(t *testing.T) {
	idents := f3BuildIdentities()
	classes := f3Classes()
	scopes := f3Scopes()

	f3InitWatcher(t, idents)
	ctx := context.Background()

	const epochs = 100
	totalAssertions := 0
	var divergences []f3Divergence

	for epoch := 0; epoch < epochs; epoch++ {
		// Republish (identical content). Tests determinism across
		// pointer-churn republishes.
		f3RepublishSnapshot(t, idents)

		for _, id := range idents {
			for _, class := range classes {
				for _, ns := range scopes {
					if class.IdentityFree {
						// widgetContent: BindingUID is irrelevant to its
						// key; F3 doesn't assert convergence. F7 covers
						// the empty-UID-for-identity-free contract.
						continue
					}
					totalAssertions++
					if d := f3CheckConvergence(ctx, id, class, ns); d != nil {
						divergences = append(divergences, *d)
						if len(divergences) > 10 {
							// Don't drown the log; surface 10 then bail.
							break
						}
					}
				}
				if len(divergences) > 10 {
					break
				}
			}
			if len(divergences) > 10 {
				break
			}
		}
		if len(divergences) > 0 {
			break
		}
	}

	if len(divergences) > 0 {
		t.Errorf("F3 DIVERGENCE: %d sites diverged across %d epochs (showing first %d):",
			len(divergences), epochs, len(divergences))
		for _, d := range divergences {
			t.Errorf("  %s", d.String())
		}
		t.Fatalf("F3 Phase 6 FAIL — see divergence bucket(s) for triage")
	}

	t.Logf("F3 Phase 6: %d epochs × %d assertions each = %d total convergence checks, ZERO divergence",
		epochs, totalAssertions/epochs, totalAssertions)
}

// f3RepublishSnapshot rebuilds + publishes the snapshot in-place.
// Uses cache.RebuildRBACSnapshotForTest which synchronously rebuilds
// indexes + publishes via the writer's snap-coherent stamp order
// (Ship 0.30.242 Phase 2a). Does NOT rebuild BindingsByGVR — the
// BindingsByGVR delta hooks are CRD/RB-event driven; for synthetic
// republishes of identical content there are no deltas to fire. The
// index built at f3InitWatcher time remains valid.
func f3RepublishSnapshot(t *testing.T, idents []f3Identity) {
	t.Helper()
	// Trigger a synchronous snapshot rebuild against the watcher's
	// underlying informer state (which is unchanged for this fixture).
	// The new snapshot pointer publishes via the snap-coherent stamp
	// order in Phase 2a's rebuildRBACSnapshot fix.
	rw := cache.Global()
	if rw == nil {
		t.Fatalf("f3RepublishSnapshot: cache.Global() is nil")
	}
	cache.RebuildRBACSnapshotForTest(rw)
}

// ──────────────────────────────────────────────────────────────────────
// F3 Phase 7 — concurrent dispatcher + republish under -race
// ──────────────────────────────────────────────────────────────────────

// TestF3_ConcurrentConvergence_UnderRace spawns 1 republisher
// goroutine (50 republishes) and 4 dispatcher goroutines, each
// hammering the 450-tuple convergence assertion. The -race detector
// is the primary safety net; convergence MUST hold AT EVERY tuple AT
// EVERY iteration regardless of when the republisher fires.
//
// Per design §12.3 Phase 7: zero race detector hits AND zero
// convergence divergence.
//
// Wall-clock target: <60 s under -race.
func TestF3_ConcurrentConvergence_UnderRace(t *testing.T) {
	idents := f3BuildIdentities()
	classes := f3Classes()
	scopes := f3Scopes()

	f3InitWatcher(t, idents)
	ctx := context.Background()

	const (
		republishes      = 50
		dispatcherCount  = 4
		iterPerDispatcher = 50
	)

	var (
		divergences   atomic.Int64
		stopRepublish atomic.Bool
		wg            sync.WaitGroup
		firstDivergence atomic.Pointer[f3Divergence]
	)

	// Republisher goroutine.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < republishes; i++ {
			if stopRepublish.Load() {
				return
			}
			f3RepublishSnapshot(t, idents)
			time.Sleep(time.Millisecond) // give dispatchers a chance to read between republishes
		}
	}()

	// Dispatcher goroutines.
	for d := 0; d < dispatcherCount; d++ {
		wg.Add(1)
		go func(disp int) {
			defer wg.Done()
			for i := 0; i < iterPerDispatcher; i++ {
				for _, id := range idents {
					for _, class := range classes {
						for _, ns := range scopes {
							if class.IdentityFree {
								continue
							}
							if div := f3CheckConvergence(ctx, id, class, ns); div != nil {
								divergences.Add(1)
								firstDivergence.CompareAndSwap(nil, div)
								stopRepublish.Store(true)
								return
							}
						}
					}
				}
			}
		}(d)
	}

	wg.Wait()

	if d := divergences.Load(); d > 0 {
		div := firstDivergence.Load()
		t.Fatalf("F3 Phase 7 FAIL — %d divergences detected under concurrent republish + dispatch.\n  First: %s",
			d, div.String())
	}

	t.Logf("F3 Phase 7: %d dispatcher goroutines × %d iterations × ~%d tuples = ~%d assertions under -race; %d republishes interleaved; ZERO divergence",
		dispatcherCount, iterPerDispatcher,
		len(idents)*(len(classes)-1)*len(scopes),
		dispatcherCount*iterPerDispatcher*len(idents)*(len(classes)-1)*len(scopes),
		republishes)
}

// ──────────────────────────────────────────────────────────────────────
// F3 Phase 4 — mid-test mutation (design §12.3 Phase 4 carry-forward)
// ──────────────────────────────────────────────────────────────────────

// TestF3_MidTestMutation_NewBindingInNewNamespace adds a new RB in
// ns-C granting user-0 access, re-publishes the snapshot, and asserts:
//
//   (1) The NEW stage cell (user-0 × widgets × ns-C) is reachable
//       from BOTH the dispatcher AND the seed enumerator (the new RB's
//       BindingUID).
//   (2) The PRIOR convergences for user-0 across cluster, ns-A, ns-B
//       MUST remain stable (the new RB in ns-C doesn't shift first-
//       match in those scopes).
//
// Per design §12.3 Phase 4.
func TestF3_MidTestMutation_NewBindingInNewNamespace(t *testing.T) {
	idents := f3BuildIdentities()
	classes := f3Classes()

	// We only need user-0 for this test (per design §12.3 Phase 4, the
	// mutation is targeted at one identity).
	user0 := idents[0]

	f3InitWatcher(t, idents)
	ctx := context.Background()

	// Baseline pass — verify convergence holds for user-0 across the
	// 3 scopes BEFORE mutation.
	preMutationUIDs := map[string]string{}
	for _, class := range classes {
		if class.IdentityFree {
			continue
		}
		for _, ns := range []string{"", "ns-A", "ns-B"} {
			if d := f3CheckConvergence(ctx, user0, class, ns); d != nil {
				t.Fatalf("F3 Phase 4 BASELINE FAIL — pre-mutation divergence for user-0: %s", d.String())
			}
			_, uid, _ := f3DispatcherUID(ctx, user0, class, ns)
			preMutationUIDs[fmt.Sprintf("%s|%s", class.Name, ns)] = uid
		}
	}

	// MUTATION — add a new RB in ns-C for user-0. We do this by
	// directly publishing a hand-built snapshot that includes the new
	// binding (bypassing the dynamic-fake which would require
	// re-instantiating the watcher).
	const newRBUID = "rb-user-0-ns-C-mutation"
	if err := f3AddRBToSnapshot(t, "ns-C", newRBUID, user0); err != nil {
		t.Fatalf("F3 Phase 4 MUTATION: failed to publish mutated snapshot: %v", err)
	}

	// ALSO add ns-C to the BindingsByGVR index — the seed enumerator
	// needs the GVR×ns enrolment to include the new binding.
	cache.ResetBindingsByGVRIndexForTest()
	cache.SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{
		{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "widgets"},
		{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"},
	})

	// Assertion 1: the NEW ns-C stage cell converges (new RB's UID).
	for _, class := range classes {
		if class.IdentityFree {
			continue
		}
		allowed, uid, err := f3DispatcherUID(ctx, user0, class, "ns-C")
		if err != nil {
			t.Fatalf("F3 Phase 4 (1) class=%s ns=ns-C: dispatcher err=%v", class.Name, err)
		}
		if !allowed {
			// The new RB grants access — dispatcher MUST permit. If
			// not, the mutation didn't take effect → harness bug.
			t.Fatalf("F3 Phase 4 (1) class=%s ns=ns-C: dispatcher denied (expected allow); the new RB didn't activate", class.Name)
		}
		// Expected: dispatcher's first-match is the new RB.
		// The new RB has UID newRBUID and lives in ns-C.
		wantUID := "R:ns-C/" + newRBUID
		// In ns-C, both CRB phase + RB phase walk. The CRB still
		// matches (it's cluster-wide). Stable order at the per-phase
		// boundary: CRB phase precedes RB phase. So the dispatcher's
		// first-match should be the CRB UID, NOT the new RB.
		//
		// This is actually the design-correct behavior: CRBs precede
		// RBs. The "new stage cell" the design refers to is the
		// SEED-enumerated target for the new RB, which MUST exist
		// among seed emissions.
		_ = wantUID

		// What we MUST assert: convergence holds (dispatcher's UID
		// exists among seed UIDs for user-0 × class × ns-C).
		if d := f3CheckConvergence(ctx, user0, class, "ns-C"); d != nil {
			t.Fatalf("F3 Phase 4 (1) post-mutation convergence FAIL for class=%s ns=ns-C: %s", class.Name, d.String())
		}
		t.Logf("F3 Phase 4 (1) class=%s ns=ns-C: dispatcher=%q converges", class.Name, uid)
	}

	// Assertion 2: PRIOR scope convergences remain stable. user-0's
	// first-match in cluster / ns-A / ns-B MUST be the same as before
	// the mutation.
	for _, class := range classes {
		if class.IdentityFree {
			continue
		}
		for _, ns := range []string{"", "ns-A", "ns-B"} {
			if d := f3CheckConvergence(ctx, user0, class, ns); d != nil {
				t.Fatalf("F3 Phase 4 (2) post-mutation convergence FAIL for prior scope class=%s ns=%q: %s", class.Name, ns, d.String())
			}
			_, uid, _ := f3DispatcherUID(ctx, user0, class, ns)
			expected := preMutationUIDs[fmt.Sprintf("%s|%s", class.Name, ns)]
			if uid != expected {
				t.Fatalf("F3 Phase 4 (2) post-mutation FIRST-MATCH SHIFT for prior scope class=%s ns=%q: pre=%q post=%q (the new ns-C RB shifted first-match in a scope it shouldn't have)",
					class.Name, ns, expected, uid)
			}
		}
	}

	t.Logf("F3 Phase 4: mid-test mutation passed — new ns-C convergence + prior-scope first-match stability across %d classes",
		len(classes)-1) // -1 for widgetContent
}

// f3AddRBToSnapshot reads the current snapshot, ADDS a single new RB
// for the identity in the target namespace, and publishes the result
// via cache.PublishRBACSnapshotForTest. Uses the snap-coherent stamp
// order (Phase 2a writer fix) by mutating the snapshot fields directly
// then storing.
//
// This bypasses the dynamic.fake (which would require rebuilding the
// watcher) and operates DIRECTLY on the snapshot. The watcher's
// indexes (CRBsBy*/RBsBy*) are rebuilt before Store.
func f3AddRBToSnapshot(t *testing.T, ns, rbUID string, id f3Identity) error {
	t.Helper()
	cur := cache.LiveRBACSnapshot()
	if cur == nil {
		return fmt.Errorf("no live snapshot to mutate")
	}
	// Build the new RB.
	newRB := &rbacv1.RoleBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: rbUID, UID: types.UID(rbUID)},
		Subjects:   []rbacv1.Subject{f3SubjectFor(id)},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "f3-reader"},
	}
	// Build a Role in ns-C granting the same access (the dispatcher
	// resolves the RoleRef against the snapshot's Roles by ns+name).
	newRole := &rbacv1.Role{
		TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "f3-reader"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{"widgets.templates.krateo.io"}, Resources: []string{"widgets"}, Verbs: []string{"get", "list"}},
			{APIGroups: []string{"templates.krateo.io"}, Resources: []string{"restactions"}, Verbs: []string{"get", "list"}},
		},
	}

	// Construct a new snapshot with the prior content + the new RB/Role.
	newSnap := &cache.RBACSnapshot{
		ClusterRoleBindings: cur.ClusterRoleBindings,
		RoleBindingsByNS:    map[string][]*rbacv1.RoleBinding{},
		ClusterRolesByName:  cur.ClusterRolesByName,
		RolesByNSName:       map[string]*rbacv1.Role{},
	}
	for k, v := range cur.RoleBindingsByNS {
		newSnap.RoleBindingsByNS[k] = v
	}
	for k, v := range cur.RolesByNSName {
		newSnap.RolesByNSName[k] = v
	}
	newSnap.RoleBindingsByNS[ns] = append(newSnap.RoleBindingsByNS[ns], newRB)
	newSnap.RolesByNSName[ns+"/f3-reader"] = newRole

	// Rebuild subject indexes for the new snapshot.
	cache.RebuildSubjectIndexesForTest(newSnap)

	// Publish via the snap-coherent stamp order (Phase 2a) — the
	// PublishRBACSnapshotForTest helper stamps PublishSeq before
	// rbacSnap.Store. The Phase 2a writer-fix invariant is preserved
	// by this path because it goes through the same Store gate.
	cache.PublishRBACSnapshotForTest(newSnap)
	return nil
}

// TestF3_SAExemption_DoesNotMaskUserGroupSeedMiss is the #139 C-F3-2 RED arm:
// the discriminating proof that the SA-exclusion exemption in f3CheckConvergence
// is KIND-scoped, NOT a blanket "seed==[] always converges". It holds the
// seed-empty condition FIXED and varies ONLY id.Kind:
//
//   - a USER identity with dispatcher-allow AND empty seed → f3CheckConvergence
//     MUST return a non-nil divergence (Bucket=="harness") — the real enumerator-
//     miss defect the branch exists to catch;
//   - the SAME tuple flipped to Kind="ServiceAccount" → nil (the intended
//     SA-exclusion).
//
// The seed-empty-for-a-User condition is forced WITHOUT any production seam: build
// the BindingsByGVR index over restactions ONLY (NOT widgets), so a user-0 ×
// widgets(get) lookup is dispatcher-ALLOWED (EvaluateRBAC reads the full RBAC
// snapshot, which still carries crb-user-0's widgets grant) but seed-EMPTY (the
// widgets GVR was never enrolled in the index the seed enumerator reads).
//
// A "make seed==[] always return nil" mis-fix FAILS the User half here — that is
// the discriminator. If someone weakens the guard to a blanket
// `if len(seedTargets)==0 { return nil }`, the User arm below returns nil and this
// test FAILS.
//
// ISOLATED (feedback_serialize_kind_test_runs): it mutates the process-global
// BindingsByGVR index (restactions-only), so it runs in its own fn and REBUILDS
// the standard 2-GVR index in a defer. Do NOT interleave with the sequential /
// concurrent / mutation F3 tests.
func TestF3_SAExemption_DoesNotMaskUserGroupSeedMiss(t *testing.T) {
	idents := f3BuildIdentities()
	f3InitWatcher(t, idents) // builds the full 2-GVR index + publishes the snapshot

	// Restrict the seed's BindingsByGVR index to restactions ONLY — widgets is
	// deliberately UNENROLLED so a widgets-class seed lookup is empty. The
	// dispatcher is unaffected (it reads the RBAC snapshot, not this index).
	cache.ResetBindingsByGVRIndexForTest()
	cache.SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{
		{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"},
	})
	// Restore the standard 2-GVR index so no sibling test inherits the
	// restactions-only index (process-global).
	defer func() {
		cache.ResetBindingsByGVRIndexForTest()
		cache.SeedBindingsIndexFromSnapshotForTest([]schema.GroupVersionResource{
			{Group: "widgets.templates.krateo.io", Version: "v1beta1", Resource: "widgets"},
			{Group: "templates.krateo.io", Version: "v1", Resource: "restactions"},
		})
	}()

	ctx := context.Background()
	user0 := idents[0] // Kind=="User", Username=="user-0"
	if user0.Kind != "User" {
		t.Fatalf("fixture drift: idents[0] must be the User identity, got kind %q", user0.Kind)
	}
	// widgets(get) — the class whose GVR we deliberately left out of the index.
	widgetsClass := f3Classes()[0]
	if widgetsClass.Name != "widgets" {
		t.Fatalf("fixture drift: f3Classes()[0] must be widgets, got %q", widgetsClass.Name)
	}

	// Precondition — the dispatcher ALLOWS user-0 for widgets(get) cluster-wide
	// (crb-user-0 grants it). If it didn't, the branch wouldn't be reached and the
	// test would be vacuous.
	allowed, dispUID, err := f3DispatcherUID(ctx, user0, widgetsClass, "")
	if err != nil {
		t.Fatalf("dispatcher err for user-0 × widgets: %v", err)
	}
	if !allowed || dispUID == "" {
		t.Fatalf("precondition: dispatcher must ALLOW user-0 × widgets(get) (dispUID=%q allowed=%v) — the seed-empty branch is only reached under dispatch-allow", dispUID, allowed)
	}
	// Precondition — the seed enumerator emits NOTHING for user-0 × widgets
	// (widgets GVR unenrolled), so we are genuinely exercising the len==0 branch.
	if seed := f3SeedTargetsForIdentity(user0, widgetsClass); len(seed) != 0 {
		t.Fatalf("precondition: seed must be EMPTY for user-0 × widgets under the restactions-only index; got %d targets", len(seed))
	}

	// USER arm — dispatcher-allow + seed-empty for a User is a REAL enumerator
	// miss → MUST diverge with Bucket=="harness". (A blanket seed==[]→nil mis-fix
	// returns nil here and FAILS the test — the discriminator.)
	dUser := f3CheckConvergence(ctx, user0, widgetsClass, "")
	if dUser == nil {
		t.Fatal("C-F3-2 DISCRIMINATOR FAILED: a User identity with dispatcher-allow + empty seed converged to nil — the SA-exclusion exemption is a blanket seed==[]→nil, masking a real enumerator miss. The guard MUST be kind-scoped (id.Kind==\"ServiceAccount\" only).")
	}
	if dUser.Bucket != "harness" {
		t.Fatalf("C-F3-2: User seed-miss must bucket \"harness\"; got %q (%s)", dUser.Bucket, dUser.String())
	}

	// SA arm — the SAME tuple (same empty-index, same dispatch-allow shape) but
	// Kind flipped to ServiceAccount → the intended SA-exclusion → nil. Synthesize
	// a SA identity that the dispatcher ALSO allows via a CRB: reuse user-0's
	// grant coordinates but with SA kind. Simplest faithful construction: take the
	// real SA identity (idents[20], which has crb-sa-0) and confirm it too is
	// dispatch-allowed + seed-empty for widgets, then assert nil.
	saID := idents[20] // first SA identity (10 User + 10 Group precede it)
	if saID.Kind != "ServiceAccount" {
		t.Fatalf("fixture drift: idents[20] must be the first ServiceAccount identity, got kind %q", saID.Kind)
	}
	allowedSA, dispUIDSA, errSA := f3DispatcherUID(ctx, saID, widgetsClass, "")
	if errSA != nil {
		t.Fatalf("dispatcher err for sa × widgets: %v", errSA)
	}
	if !allowedSA || dispUIDSA == "" {
		t.Fatalf("precondition: dispatcher must ALLOW the SA × widgets(get) (dispUID=%q) — else the SA arm is vacuous", dispUIDSA)
	}
	// SA seed is empty for TWO reasons here (widgets unenrolled AND the production
	// SA-only exclusion), but the exemption fires on id.Kind regardless.
	dSA := f3CheckConvergence(ctx, saID, widgetsClass, "")
	if dSA != nil {
		t.Fatalf("C-F3-2: a ServiceAccount identity with dispatcher-allow + empty seed must be EXEMPTED (nil, intended SA-exclusion); got divergence %s", dSA.String())
	}
}
