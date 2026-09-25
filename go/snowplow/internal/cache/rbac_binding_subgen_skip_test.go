// rbac_binding_subgen_skip_test.go — #253 arms for "an UPDATE that changed
// nothing RBAC-relevant records no sub-generation bump".
//
// WHY THESE AND NOT A SINGLE "NO-OPS ARE SKIPPED" TEST. The skip is a
// SECURITY-ADJACENT narrowing: every bump it drops is a key rotation that will
// no longer happen. A suite that only proves no-ops are skipped would pass just
// as happily on a predicate that skipped EVERYTHING. So the arms come in two
// directions, and the ones that carry the risk are the MUST-BUMP ones.
//
// Every arm drives the REAL onBindingUpdate against a built index — the same
// asCRB/asRB normalisation and the same two independent old/new branches the
// informer path uses — then drains the pending set exactly as
// rebuildRBACSnapshot does. A seam below onBindingUpdate could not show that
// the predicate survives normalisation.
//
// THE CONTROL MATRIX. Each arm is the UNIQUE detector of one defect; the
// verification runs each defect alone against the whole suite and checks that
// exactly the named arm goes red (see the #253 control-matrix run):
//
//	arm                                  defect it alone catches
//	GenuineSubjectEditStillBumps         subjects dropped from the predicate
//	RoleRefRetargetStillBumps            roleRef dropped from the predicate
//	ReorderedSubjectsAreUnchanged        positional instead of multiset compare
//	AnnotationOnlyChangeDoesNotBump      skip keyed on resourceVersion  <-- the
//	                                     production defect shape: an annotation
//	                                     write advances the RV, so an RV-keyed
//	                                     skip catches NONE of it
//	RelistSyncDoesNotBump                skip suppressed when old RV == new RV
//	AmbiguousClassificationBumps         incomparable sides treated as equal
//
// plus two structural arms — the index delta must still run on a skipped event,
// and onBindingAdd/onBindingDelete must still bump unconditionally.

package cache

import (
	"sync"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// skipArmSubGenBumps drives one UPDATE through the real hook from a known floor
// and returns how many per-subject sub-generation bumps it produced. The bump
// is deferred to snapshot-publish (#118 (c)-v2 GAP-2), so the pending set is
// drained the way rebuildRBACSnapshot drains it before reading the counter.
func skipArmSubGenBumps(t *testing.T, oldObj, newObj interface{}) uint64 {
	t.Helper()
	ResetRBACSubGenForTest()
	ResetPendingSubGenBumpsForTest()
	ResetBindingNoopCountersForTest()

	onBindingUpdate(oldObj, newObj)
	flushPendingSubGenBumps()

	return RBACSubGenBumpsTotal()
}

// skipArmAnnotated returns a copy of the binding at a NEW resourceVersion with
// one extra annotation — nothing else touched. This is the exact production
// shape measured on 057 against 1.12.13: annotate, then un-annotate, a
// RoleBinding whose subjects and roleRef never move.
func skipArmAnnotated(crb *rbacv1.ClusterRoleBinding, rv, key, value string) *rbacv1.ClusterRoleBinding {
	out := crb.DeepCopy()
	out.ResourceVersion = rv
	if out.Annotations == nil {
		out.Annotations = map[string]string{}
	}
	out.Annotations[key] = value
	return out
}

// ---------------------------------------------------------------------------
// ARM 1 — the security-load-bearing direction. A genuine subject-set edit MUST
// still rotate every affected subject's key.
// ---------------------------------------------------------------------------

func TestSubGenSkip_GenuineSubjectEditStillBumps(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)
	t.Cleanup(ResetBindingNoopCountersForTest)

	cases := []struct {
		name       string
		old, new   *rbacv1.ClusterRoleBinding
		mustMove   []subjectKey
		wantNoopCt uint64
		why        string
	}{
		{
			name: "subject ADDED",
			old:  noopArmCRB("arm1-add", "100", "role-a", noopArmUser("alice")),
			new:  noopArmCRB("arm1-add", "101", "role-a", noopArmUser("alice"), noopArmUser("bob")),
			mustMove: []subjectKey{
				{Kind: subjectKindUser, Name: "bob"},
			},
			why: "bob genuinely GAINED the grant. If his key does not rotate he keeps serving from a " +
				"cell resolved under the old, narrower scope — the grant silently does not take effect",
		},
		{
			name: "subject REMOVED",
			old:  noopArmCRB("arm1-del", "100", "role-a", noopArmUser("alice"), noopArmUser("bob")),
			new:  noopArmCRB("arm1-del", "101", "role-a", noopArmUser("alice")),
			mustMove: []subjectKey{
				{Kind: subjectKindUser, Name: "bob"},
			},
			why: "bob LOST the grant — the revoke case. A missed bump here leaves him reading a cell " +
				"resolved under a scope he no longer holds until TTL: a cross-tenant read",
		},
		{
			name: "subject SWAPPED (same count, different member)",
			old:  noopArmCRB("arm1-swap", "100", "role-a", noopArmUser("alice")),
			new:  noopArmCRB("arm1-swap", "101", "role-a", noopArmUser("carol")),
			mustMove: []subjectKey{
				{Kind: subjectKindUser, Name: "alice"},
				{Kind: subjectKindUser, Name: "carol"},
			},
			why: "equal LENGTH must never be mistaken for equal membership — alice was revoked and " +
				"carol granted in a single write, and BOTH keys must rotate",
		},
		{
			name: "GROUP subject added (the subject kinds must be symmetric)",
			old:  noopArmCRB("arm1-grp", "100", "role-a", noopArmUser("alice")),
			new:  noopArmCRB("arm1-grp", "101", "role-a", noopArmUser("alice"), noopArmGroup("devs")),
			mustMove: []subjectKey{
				{Kind: subjectKindGroup, Name: "devs"},
			},
			why: "a Group grant is as load-bearing as a User grant; a predicate that compared only " +
				"User subjects would pass every other row of this arm",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := skipArmSubGenBumps(t, tc.old, tc.new); got == 0 {
				t.Fatalf("a genuine subject-set edit recorded ZERO sub-generation bumps.\n  %s", tc.why)
			}
			for _, s := range tc.mustMove {
				if subGenValue(s) == 0 {
					t.Errorf("%s/%s sub-generation did not move.\n  %s", s.Kind, s.Name, tc.why)
				}
			}
			if got := RBACBindingSemanticNoopUpdatesTotal(); got != tc.wantNoopCt {
				t.Errorf("semantic_noop_updates_total = %d; want %d — a genuine edit must not be "+
					"classified as a no-op by the counter either", got, tc.wantNoopCt)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ARM 2 — a roleRef retarget MUST still bump. Same subjects, different rules:
// the effective permission changed without the subject list moving at all.
// ---------------------------------------------------------------------------

func TestSubGenSkip_RoleRefRetargetStillBumps(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)
	t.Cleanup(ResetBindingNoopCountersForTest)

	t.Run("ClusterRoleBinding retargeted to a different ClusterRole", func(t *testing.T) {
		old := noopArmCRB("arm2-crb", "100", "role-a", noopArmUser("alice"))
		new := noopArmCRB("arm2-crb", "101", "role-b", noopArmUser("alice"))

		if got := skipArmSubGenBumps(t, old, new); got == 0 {
			t.Fatal("a roleRef retarget recorded ZERO sub-generation bumps. alice's subject list never " +
				"moved, but she is now bound to a DIFFERENT rule set — her effective scope changed and " +
				"her key must rotate. A predicate that compared only subjects would skip this")
		}
		if subGenValue(subjectKey{Kind: subjectKindUser, Name: "alice"}) == 0 {
			t.Error("alice's sub-generation did not move on a roleRef retarget")
		}
	})

	t.Run("RoleBinding moved to a different namespace's Role", func(t *testing.T) {
		// Same name, same roleRef struct, same subjects — only the binding's
		// NAMESPACE differs, which for a namespace-scoped "Role" roleRef means
		// a different role entirely.
		old := noopArmRB("arm2-rb", "ns1", "200", "role-a", noopArmUser("alice"))
		new := noopArmRB("arm2-rb", "ns2", "201", "role-a", noopArmUser("alice"))

		if got := skipArmSubGenBumps(t, old, new); got == 0 {
			t.Fatal("a namespace move recorded ZERO bumps. A \"Role\" roleRef is namespace-scoped, so " +
				"ns1/role-a and ns2/role-a are different rule sets; dropping the namespace from the " +
				"predicate would skip a real permission change")
		}
	})
}

// ---------------------------------------------------------------------------
// ARM 3 — a subject list reordered with identical members is UNCHANGED. The
// apiserver makes no promise about subject order, so a positional comparison
// would call every reorder a real change and the fix would skip almost nothing.
// ---------------------------------------------------------------------------

func TestSubGenSkip_ReorderedSubjectsAreUnchanged(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)
	t.Cleanup(ResetBindingNoopCountersForTest)

	old := noopArmCRB("arm3", "100", "role-a",
		noopArmUser("alice"), noopArmGroup("devs"), noopArmUser("bob"))
	new := noopArmCRB("arm3", "101", "role-a",
		noopArmUser("bob"), noopArmUser("alice"), noopArmGroup("devs"))

	if got := skipArmSubGenBumps(t, old, new); got != 0 {
		t.Errorf("a pure REORDER of an identical subject set recorded %d sub-generation bumps; want 0. "+
			"The comparison must be order-insensitive (a sorted multiset), not positional — otherwise "+
			"every apiserver rewrite that happens to shuffle the list rotates every key for nothing", got)
	}
	if got := RBACBindingSemanticNoopUpdatesTotal(); got != 1 {
		t.Errorf("semantic_noop_updates_total = %d; want 1 — the counter and the skip must agree, or "+
			"the shipped counter stops describing the shipped behaviour", got)
	}

	t.Run("a DUPLICATE is not the same multiset as a single entry", func(t *testing.T) {
		// Multiset, not set: [alice,alice] vs [alice] must NOT be skipped. This
		// is the conservative direction — it can only over-bump.
		dupOld := noopArmCRB("arm3-dup", "100", "role-a", noopArmUser("alice"), noopArmUser("alice"))
		dupNew := noopArmCRB("arm3-dup", "101", "role-a", noopArmUser("alice"))
		if got := skipArmSubGenBumps(t, dupOld, dupNew); got == 0 {
			t.Error("a multiplicity change was skipped. The comparison must stay a MULTISET: collapsing " +
				"to a set is a widening, and widening the skip is the direction that loses bumps")
		}
	})
}

// ---------------------------------------------------------------------------
// ARM 4 — THE PRODUCTION SHAPE. An annotation-only write advances the
// resourceVersion while touching nothing RBAC-relevant. Measured on 057 against
// 1.12.13: annotate then un-annotate a binding and rbac_subgen_bumps_total went
// 0 -> 1 -> 2 while rbac_binding_noop_updates_total (same-RV) stayed at 0
// THROUGHOUT. An RV-keyed skip would have caught NONE of it.
// ---------------------------------------------------------------------------

func TestSubGenSkip_AnnotationOnlyChangeDoesNotBump(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)
	t.Cleanup(ResetBindingNoopCountersForTest)

	base := noopArmCRB("arm4", "100", "role-a", noopArmUser("alice"), noopArmGroup("devs"))
	annotated := skipArmAnnotated(base, "101", "krateo.io/touched", "1")
	unannotated := base.DeepCopy()
	unannotated.ResourceVersion = "102"

	ResetRBACSubGenForTest()
	ResetPendingSubGenBumpsForTest()
	ResetBindingNoopCountersForTest()

	// Annotate, then un-annotate — the two writes actually performed on 057.
	onBindingUpdate(base, annotated)
	onBindingUpdate(annotated, unannotated)
	flushPendingSubGenBumps()

	if got := RBACSubGenBumpsTotal(); got != 0 {
		t.Errorf("rbac_subgen_bumps_total = %d after an annotate + un-annotate cycle; want 0.\n"+
			"  This is the exact write pair measured on 057 against 1.12.13, where it read 2. "+
			"Subjects and roleRef never moved; only metadata.resourceVersion did", got)
	}
	if got := subGenValue(subjectKey{Kind: subjectKindUser, Name: "alice"}); got != 0 {
		t.Errorf("alice's sub-generation = %d after pure annotation churn; want 0 — her L1 key rotated "+
			"for a metadata write", got)
	}

	// The two counters are the load-bearing part of this arm: they pin that the
	// fix could NOT have been keyed on resourceVersion, and they keep the
	// effect measurable in production after the fix ships.
	if got := RBACBindingSemanticNoopUpdatesTotal(); got != 2 {
		t.Errorf("semantic_noop_updates_total = %d; want 2 — both writes must still be CLASSIFIED as "+
			"semantic no-ops. The counters are what makes the skip's effect observable on a live "+
			"cluster; a fix that stopped counting would be unmeasurable", got)
	}
	if got := RBACBindingNoopUpdatesTotal(); got != 0 {
		t.Errorf("noop_updates_total (same-RV) = %d; want 0.\n"+
			"  THIS IS THE LINE THAT DECIDED THE DESIGN. An annotation write advances the "+
			"resourceVersion, so the same-RV signal never fires for this traffic. A skip keyed on RV "+
			"equality would have skipped NEITHER of these two bumps — a 100%% miss rate. The skip "+
			"must key on the SEMANTIC comparison and never on the resourceVersion", got)
	}
}

// ---------------------------------------------------------------------------
// ARM 5 — a relist / DeltaFIFO Sync. The informer dispatches these as
// OnUpdate(old, new) with old and new being the SAME object at the SAME
// resourceVersion. One watch re-establishment delivers one of these per
// binding, and before this fix each one rotated every subject's key.
// ---------------------------------------------------------------------------

func TestSubGenSkip_RelistSyncDoesNotBump(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)
	t.Cleanup(ResetBindingNoopCountersForTest)

	crb := noopArmCRB("arm5", "100", "role-a", noopArmUser("alice"), noopArmGroup("devs"))

	if got := skipArmSubGenBumps(t, crb, crb); got != 0 {
		t.Errorf("a relist Sync (old == new, identical resourceVersion) recorded %d sub-generation "+
			"bumps; want 0. This is the highest-volume no-op there is: one per binding per watch "+
			"re-establishment", got)
	}
	if got := RBACBindingNoopUpdatesTotal(); got != 1 {
		t.Errorf("noop_updates_total = %d; want 1 — the relist must still be COUNTED even though its "+
			"bump is now skipped", got)
	}
	if got := RBACBindingSemanticNoopUpdatesTotal(); got != 1 {
		t.Errorf("semantic_noop_updates_total = %d; want 1 — it is a superset of noop_updates_total "+
			"and that relation must survive the fix", got)
	}

	t.Run("RoleBinding relist takes the same path", func(t *testing.T) {
		rb := noopArmRB("arm5-rb", "ns1", "200", "role-a", noopArmUser("alice"))
		if got := skipArmSubGenBumps(t, rb, rb); got != 0 {
			t.Errorf("a namespaced relist Sync recorded %d bumps; want 0 — the asRB branch must be "+
				"skipped identically to the asCRB one", got)
		}
	})
}

// ---------------------------------------------------------------------------
// ARM 6 — BUMP WHEN IN DOUBT. An event the classifier cannot compare must never
// be skipped: a missed bump is a stale RBAC scope, a spurious one is waste.
// ---------------------------------------------------------------------------

func TestSubGenSkip_AmbiguousClassificationBumps(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)
	t.Cleanup(ResetBindingNoopCountersForTest)

	cases := []struct {
		name     string
		old, new interface{}
		why      string
	}{
		{
			name: "MIXED kinds: old ClusterRoleBinding, new RoleBinding",
			// Identical subjects, identical roleRef fields, identical RV — every
			// field-level comparison says "unchanged". Only the kind mismatch
			// says the comparison is meaningless.
			old: noopArmCRB("arm6-mixed", "100", "role-a", noopArmUser("alice")),
			new: noopArmRB("arm6-mixed", "ns1", "100", "role-a", noopArmUser("alice")),
			why: "a cluster-wide grant and a namespaced one are not the same grant; there is no " +
				"meaningful old-vs-new comparison across kinds, so this must bump rather than guess",
		},
		{
			name: "OLD side unnormalisable",
			old:  "not-a-binding",
			new:  noopArmCRB("arm6-oldbad", "100", "role-a", noopArmUser("alice")),
			why: "the old side was dropped to the drift canary, so what alice previously held is " +
				"UNKNOWN. Skipping would bet that it was identical",
		},
		{
			name: "NEW side unnormalisable",
			old:  noopArmCRB("arm6-newbad", "100", "role-a", noopArmUser("alice")),
			new:  "not-a-binding",
			why: "the new side is unknown; alice's grant may have been rewritten to anything, and the " +
				"index already dropped the enrol. Her key must rotate",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := skipArmSubGenBumps(t, tc.old, tc.new); got == 0 {
				t.Errorf("an AMBIGUOUS classification recorded ZERO bumps.\n  %s", tc.why)
			}
			if subGenValue(subjectKey{Kind: subjectKindUser, Name: "alice"}) == 0 {
				t.Errorf("alice's sub-generation did not move.\n  %s", tc.why)
			}
		})
	}

	t.Run("BOTH sides unnormalisable records nothing but is not a skip", func(t *testing.T) {
		// Neither side yields a subject, so there is nothing to bump — but the
		// event must reach that state by having no subjects, never by being
		// classified as unchanged. Both sides bumped the drift canary.
		before := BindingsIndexDeltaSkippedNonTyped()
		if got := skipArmSubGenBumps(t, "not-a-binding", "also-not-a-binding"); got != 0 {
			t.Errorf("bumps = %d; want 0 — there are no subjects on either side to bump", got)
		}
		if got := BindingsIndexDeltaSkippedNonTyped(); got != before+2 {
			t.Errorf("drift canary moved by %d; want 2 — both sides must still be reported as dropped",
				got-before)
		}
	})
}

// ---------------------------------------------------------------------------
// STRUCTURAL — the index delta must STILL RUN on a skipped event. Only the
// recordPendingSubGenBumps calls are conditional; a "while we're here" widening
// that also skipped applyBindingDelete/applyBindingAdd would leave the index
// stale against a snapshot that moved underneath it.
// ---------------------------------------------------------------------------

func TestSubGenSkip_IndexDeltaStillRunsOnASkippedEvent(t *testing.T) {
	ResetBindingsByGVRIndexForTest()
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)

	compGVR := gr("composition.krateo.io", "compositions")

	// The binding's role grants NOTHING at build time, so the subject is not in
	// the compositions bucket.
	crb, _ := crbRuleUID("arm7-binding", "uid-arm7",
		rbacv1.Subject{Kind: "Group", Name: "devs"}, nil)
	crb.ResourceVersion = "100"
	roleNoGrant := &rbacv1.ClusterRole{}
	roleNoGrant.Name = crb.RoleRef.Name
	PublishRBACSnapshotForTest(buildSnap(
		[]*rbacv1.ClusterRoleBinding{crb}, []*rbacv1.ClusterRole{roleNoGrant}))
	BuildBindingsByGVRIndex([]schema.GroupVersionResource{compGVR})

	if got := targetRepresentativeLabels(EnumeratePrewarmTargetsForGVR(compGVR, "list")); len(got) != 0 {
		t.Fatalf("setup: want no prewarm targets before the role grants anything, got %v", got)
	}

	// The ROLE now grants compositions. The binding object itself is untouched,
	// so the very next UPDATE for it is a semantic no-op — and that no-op must
	// still re-derive the binding's bucket membership against the new snapshot.
	roleGrants := &rbacv1.ClusterRole{}
	roleGrants.Name = crb.RoleRef.Name
	roleGrants.Rules = getListRules("composition.krateo.io", "compositions")
	PublishRBACSnapshotForTest(buildSnap(
		[]*rbacv1.ClusterRoleBinding{crb}, []*rbacv1.ClusterRole{roleGrants}))

	if got := skipArmSubGenBumps(t, crb, crb); got != 0 {
		t.Fatalf("setup: the event under test must be a SKIPPED one, but it recorded %d bumps", got)
	}
	if got := targetRepresentativeLabels(EnumeratePrewarmTargetsForGVR(compGVR, "list")); !equalSorted(got, []string{"group:devs"}) {
		t.Errorf("after a SKIPPED no-op UPDATE the index reads %v; want [group:devs].\n"+
			"  The skip covers ONLY the sub-generation bumps. applyBindingDelete + applyBindingAdd must "+
			"still run on every event: the binding's bucket membership is derived from the SNAPSHOT, "+
			"which can move while the binding object does not", got)
	}
}

// ---------------------------------------------------------------------------
// STRUCTURAL — onBindingAdd and onBindingDelete are untouched by #253. A create
// and a delete are real RBAC changes; neither has an old-vs-new comparison to
// make, and a DELETE is the revoke path.
// ---------------------------------------------------------------------------

func TestSubGenSkip_AddAndDeleteStillBumpUnconditionally(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)

	crb := noopArmCRB("arm8", "100", "role-a", noopArmUser("alice"))

	for _, tc := range []struct {
		name string
		fire func()
		why  string
	}{
		{
			name: "onBindingAdd",
			fire: func() { onBindingAdd(crb) },
			why:  "a create is a real grant, and an ADD has no old side to compare against at all",
		},
		{
			name: "onBindingDelete",
			fire: func() { onBindingDelete(crb) },
			why:  "a delete is the REVOKE path — the security-load-bearing one",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ResetRBACSubGenForTest()
			ResetPendingSubGenBumpsForTest()

			tc.fire()
			flushPendingSubGenBumps()

			if got := RBACSubGenBumpsTotal(); got == 0 {
				t.Errorf("%s recorded ZERO sub-generation bumps.\n  %s", tc.name, tc.why)
			}
			if subGenValue(subjectKey{Kind: subjectKindUser, Name: "alice"}) == 0 {
				t.Errorf("alice's sub-generation did not move on %s.\n  %s", tc.name, tc.why)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// -race — the skip reads both sides on the RBAC informer processor goroutine
// and the index's own concurrency test already drives these hooks from several
// goroutines at once. subjectSetsEqual sorts CLONES of the live slices the
// caller is holding; sorting them in place would be a data race on a slice the
// index also references, and only a concurrent arm can catch that.
// ---------------------------------------------------------------------------

func TestSubGenSkip_ConcurrentMixedTrafficAccounting(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)
	t.Cleanup(ResetBindingNoopCountersForTest)

	ResetRBACSubGenForTest()
	ResetPendingSubGenBumpsForTest()
	ResetBindingNoopCountersForTest()

	const (
		goroutines = 8
		perG       = 50
	)

	// A no-op and a genuine edit per iteration, so the arm cannot pass by
	// skipping everything OR by bumping everything.
	relist := noopArmCRB("conc-skip-relist", "100", "role-a", noopArmUser("noop-alice"))
	churnOld := noopArmCRB("conc-skip-churn", "100", "role-a", noopArmUser("noop-alice"))
	churnNew := skipArmAnnotated(churnOld, "101", "krateo.io/touched", "1")
	realOld := noopArmCRB("conc-skip-real", "100", "role-a", noopArmUser("real-alice"))
	realNew := noopArmCRB("conc-skip-real", "101", "role-a", noopArmUser("real-alice"), noopArmUser("real-bob"))

	var start sync.WaitGroup
	start.Add(1)
	var done sync.WaitGroup
	for range goroutines {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			for range perG {
				onBindingUpdate(relist, relist)
				onBindingUpdate(churnOld, churnNew)
				onBindingUpdate(realOld, realNew)
			}
		}()
	}
	start.Done()
	done.Wait()

	flushPendingSubGenBumps()

	// Only the genuine-edit subjects may have rotated. The pending set dedupes,
	// so the exact bump count is not fixed — but WHICH subjects moved is.
	for _, s := range []subjectKey{
		{Kind: subjectKindUser, Name: "real-alice"},
		{Kind: subjectKindUser, Name: "real-bob"},
	} {
		if subGenValue(s) == 0 {
			t.Errorf("%s did not rotate under concurrent mixed traffic — the genuine edit was lost", s.Name)
		}
	}
	if got := subGenValue(subjectKey{Kind: subjectKindUser, Name: "noop-alice"}); got != 0 {
		t.Errorf("noop-alice rotated %d times under concurrent traffic; want 0 — every event naming "+
			"her was a relist or an annotation-only churn", got)
	}

	const wantNoops = uint64(goroutines * perG)
	if got := RBACBindingNoopUpdatesTotal(); got != wantNoops {
		t.Errorf("noop_updates_total = %d; want %d — the same-RV relists must still be counted exactly "+
			"once per event after the fix", got, wantNoops)
	}
	if got := RBACBindingSemanticNoopUpdatesTotal(); got != 2*wantNoops {
		t.Errorf("semantic_noop_updates_total = %d; want %d — the relist AND the annotation churn are "+
			"both semantic no-ops, and the genuine edits contribute zero", got, 2*wantNoops)
	}
}
