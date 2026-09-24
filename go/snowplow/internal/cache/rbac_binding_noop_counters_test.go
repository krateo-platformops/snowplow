// rbac_binding_noop_counters_test.go — #247 binding-update no-op arms.
//
// Every arm drives the REAL onBindingUpdate hook against a built index, not
// recordBindingUpdateNoop directly: the classification has to survive the same
// asCRB/asRB normalisation and the same two independent old/new branches the
// informer path uses, and a seam below that could not show it does.
//
// The classification matrix these pin (rows = event, columns = counter):
//
//	event                                   noop  semantic
//	same object, same RV (relist Sync)       +1      +1     superset holds
//	same subjects+roleRef, DIFFERENT RV       0      +1     the metadata-churn row
//	same subjects REORDERED, different RV     0      +1     the one most easily got wrong
//	subject added / removed                   0       0
//	roleRef retargeted                        0       0
//	old CRB, new RB (mixed kinds)             0       0
//	either side unnormalisable                0       0
//
// The two rows that carry the decision are rows 2-3: if the difference between
// the counters is large, an RV-keyed fix would miss most of the waste.

package cache

import (
	"encoding/json"
	"sync"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// noopArmCRB builds a ClusterRoleBinding with an explicit resourceVersion and
// subject list. The UID is fixed per name so the index treats old and new as
// the same binding, exactly as the informer would.
func noopArmCRB(name, rv string, roleName string, subs ...rbacv1.Subject) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			UID:             types.UID("uid-" + name),
			ResourceVersion: rv,
		},
		Subjects: subs,
		RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "ClusterRole", Name: roleName},
	}
}

func noopArmRB(name, namespace, rv string, roleName string, subs ...rbacv1.Subject) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			UID:             types.UID("uid-" + name),
			ResourceVersion: rv,
		},
		Subjects: subs,
		RoleRef:  rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: roleName},
	}
}

func noopArmUser(n string) rbacv1.Subject  { return rbacv1.Subject{Kind: "User", Name: n} }
func noopArmGroup(n string) rbacv1.Subject { return rbacv1.Subject{Kind: "Group", Name: n} }

// activateBindingDeltaHooksForTest publishes an empty snapshot and builds the
// index so deltaActive() is true and onBindingUpdate does real work. Without
// this the hook returns at its first line and every arm would pass vacuously.
func activateBindingDeltaHooksForTest(t *testing.T) {
	t.Helper()
	ResetBindingsByGVRIndexForTest()
	PublishRBACSnapshotForTest(buildSnap(nil, nil))
	BuildBindingsByGVRIndex([]schema.GroupVersionResource{gr("composition.krateo.io", "compositions")})
	if !BindingsByGVRIndexBuilt() {
		t.Fatal("setup: index not built — onBindingUpdate would no-op and every arm would be vacuous")
	}
}

// TestBindingNoopCounters_ClassificationMatrix walks the whole matrix in one
// table, asserting the DELTA each event produces on both counters.
func TestBindingNoopCounters_ClassificationMatrix(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)

	cases := []struct {
		name             string
		old, new         interface{}
		wantNoop         uint64
		wantSemanticNoop uint64
		why              string
	}{
		{
			name:             "relist Sync: identical object, identical RV",
			old:              noopArmCRB("relist", "100", "role-a", noopArmUser("alice")),
			new:              noopArmCRB("relist", "100", "role-a", noopArmUser("alice")),
			wantNoop:         1,
			wantSemanticNoop: 1,
			why: "a DeltaFIFO Replace dispatches OnUpdate(old,new) with the same object; " +
				"same RV must count as relist fan-out AND, being byte-identical, as a semantic no-op — " +
				"semantic_noop is a SUPERSET and a same-RV event that raised only one of them breaks it",
		},
		{
			name:             "metadata churn: same subjects + roleRef, DIFFERENT RV",
			old:              noopArmCRB("churn", "100", "role-a", noopArmUser("alice"), noopArmGroup("devs")),
			new:              noopArmCRB("churn", "101", "role-a", noopArmUser("alice"), noopArmGroup("devs")),
			wantNoop:         0,
			wantSemanticNoop: 1,
			why: "the object was genuinely rewritten (labels/annotations) but nothing RBAC-relevant moved. " +
				"This is the row that decides whether a fix may key on resourceVersion: if these are common, " +
				"an RV check would miss them entirely",
		},
		{
			name:             "same subjects in a DIFFERENT ORDER, different RV",
			old:              noopArmCRB("reorder", "100", "role-a", noopArmUser("alice"), noopArmGroup("devs"), noopArmUser("bob")),
			new:              noopArmCRB("reorder", "101", "role-a", noopArmUser("bob"), noopArmUser("alice"), noopArmGroup("devs")),
			wantNoop:         0,
			wantSemanticNoop: 1,
			why: "the apiserver does not promise subject order is stable. A positional comparison would call " +
				"this a real change and silently deflate the semantic-no-op count — the failure mode most " +
				"likely to be got wrong",
		},
		{
			name:             "subject ADDED",
			old:              noopArmCRB("subadd", "100", "role-a", noopArmUser("alice")),
			new:              noopArmCRB("subadd", "101", "role-a", noopArmUser("alice"), noopArmUser("bob")),
			wantNoop:         0,
			wantSemanticNoop: 0,
			why:              "bob genuinely gained the grant; the key MUST rotate and this is not a no-op",
		},
		{
			name:             "subject REMOVED",
			old:              noopArmCRB("subdel", "100", "role-a", noopArmUser("alice"), noopArmUser("bob")),
			new:              noopArmCRB("subdel", "101", "role-a", noopArmUser("alice")),
			wantNoop:         0,
			wantSemanticNoop: 0,
			why:              "bob lost the grant — the revoke case, the security-load-bearing one",
		},
		{
			name:             "subject SWAPPED (same count, different member)",
			old:              noopArmCRB("subswap", "100", "role-a", noopArmUser("alice")),
			new:              noopArmCRB("subswap", "101", "role-a", noopArmUser("carol")),
			wantNoop:         0,
			wantSemanticNoop: 0,
			why:              "equal LENGTH must not be mistaken for equal membership",
		},
		{
			name:             "roleRef RETARGETED",
			old:              noopArmCRB("refchg", "100", "role-a", noopArmUser("alice")),
			new:              noopArmCRB("refchg", "101", "role-b", noopArmUser("alice")),
			wantNoop:         0,
			wantSemanticNoop: 0,
			why:              "the same subject now gets a different rule set — a real permission change",
		},
		{
			name:             "MIXED kinds: old ClusterRoleBinding, new RoleBinding",
			old:              noopArmCRB("mixed", "100", "role-a", noopArmUser("alice")),
			new:              noopArmRB("mixed", "ns1", "100", "role-a", noopArmUser("alice")),
			wantNoop:         0,
			wantSemanticNoop: 0,
			why: "there is no meaningful old-vs-new comparison across kinds; guessing either way would " +
				"contaminate the number this instrument exists to produce",
		},
		{
			name:             "old side UNNORMALISABLE",
			old:              "not-a-binding",
			new:              noopArmCRB("halfdrop", "100", "role-a", noopArmUser("alice")),
			wantNoop:         0,
			wantSemanticNoop: 0,
			why: "the dropped side already bumped the drift canary in its own branch; it must not also " +
				"be counted as a no-op",
		},
		{
			name:             "BOTH sides unnormalisable",
			old:              "not-a-binding",
			new:              "also-not-a-binding",
			wantNoop:         0,
			wantSemanticNoop: 0,
			why:              "nothing to compare",
		},
		{
			name:             "RoleBinding relist Sync (the namespaced kind takes the same path)",
			old:              noopArmRB("rb-relist", "ns1", "200", "role-a", noopArmUser("alice")),
			new:              noopArmRB("rb-relist", "ns1", "200", "role-a", noopArmUser("alice")),
			wantNoop:         1,
			wantSemanticNoop: 1,
			why:              "the asRB branch must classify identically to the asCRB one",
		},
		{
			name:             "empty RVs must NOT count as a relist",
			old:              noopArmCRB("nover", "", "role-a", noopArmUser("alice")),
			new:              noopArmCRB("nover", "", "role-a", noopArmUser("alice")),
			wantNoop:         0,
			wantSemanticNoop: 1,
			why: "the apiserver always stamps a resourceVersion, so two empty RVs mean a fabricated object, " +
				"never watch instability — counting it would let a synthetic event masquerade as a relist. " +
				"It is still a semantic no-op, which is what keeps that counter the honest superset",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ResetBindingNoopCountersForTest()

			onBindingUpdate(tc.old, tc.new)

			if got := RBACBindingNoopUpdatesTotal(); got != tc.wantNoop {
				t.Errorf("noop_updates_total = %d; want %d\n  %s", got, tc.wantNoop, tc.why)
			}
			if got := RBACBindingSemanticNoopUpdatesTotal(); got != tc.wantSemanticNoop {
				t.Errorf("semantic_noop_updates_total = %d; want %d\n  %s", got, tc.wantSemanticNoop, tc.why)
			}
			// The superset relation is structural, not per-case: assert it on
			// every row rather than trusting the table's own numbers.
			if RBACBindingNoopUpdatesTotal() > RBACBindingSemanticNoopUpdatesTotal() {
				t.Errorf("noop (%d) > semantic_noop (%d) — semantic_noop must be a SUPERSET; "+
					"an identical resourceVersion means identical content, so it cannot be a "+
					"relist no-op without also being a semantic one",
					RBACBindingNoopUpdatesTotal(), RBACBindingSemanticNoopUpdatesTotal())
			}
		})
	}
}

// TestBindingNoopCounters_BumpsStillFireOnEveryNoop is the no-behaviour-change
// arm. The brief is explicit that the instrument must not start skipping bumps;
// a relist no-op must STILL rotate every subject's sub-generation exactly as
// before, so the number we are about to measure describes today's system and
// not a system this commit quietly changed.
func TestBindingNoopCounters_BumpsStillFireOnEveryNoop(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)

	ResetBindingNoopCountersForTest()
	ResetRBACSubGenForTest()
	ResetPendingSubGenBumpsForTest()
	t.Cleanup(ResetRBACSubGenForTest)
	t.Cleanup(ResetPendingSubGenBumpsForTest)

	crb := noopArmCRB("still-bumps", "100", "role-a", noopArmUser("alice"), noopArmGroup("devs"))
	onBindingUpdate(crb, crb) // the pure relist case

	if got := RBACBindingNoopUpdatesTotal(); got != 1 {
		t.Fatalf("setup: noop_updates_total = %d; want 1 — this arm must be measuring the no-op path", got)
	}

	// The bump is deferred to snapshot-publish (#118 (c)-v2 GAP-2), so the
	// subjects sit in the pending set until a flush. Drain it the way
	// rebuildRBACSnapshot does.
	flushPendingSubGenBumps()

	if got := RBACSubGenBumpsTotal(); got == 0 {
		t.Error("a relist no-op recorded ZERO sub-generation bumps. This commit is instrument-only: " +
			"the bump must still fire on the no-op path, or the counters would be measuring a system " +
			"this change already altered")
	}
	if got := subGenValue(subjectKey{Kind: subjectKindUser, Name: "alice"}); got == 0 {
		t.Error("alice's sub-generation did not move on a no-op UPDATE — the traced behaviour " +
			"(every UPDATE rotates every subject's key) must be intact for the measurement to mean anything")
	}
	if got := subGenValue(subjectKey{Kind: subjectKindGroup, Name: "devs"}); got == 0 {
		t.Error("the devs group's sub-generation did not move on a no-op UPDATE")
	}
}

// TestBindingNoopCounters_ConcurrentUpdatesAreCountedExactlyOnce is the -race
// arm. onBindingUpdate runs on the RBAC informer processor goroutine, and the
// index's own concurrency test already drives these hooks from several
// goroutines at once; the counters must not lose or double an event under that.
//
// It also pins "once per EVENT, not once per branch": the classification sits
// after two independent old/new branches, and a count placed inside either
// branch would read 2N here instead of N.
func TestBindingNoopCounters_ConcurrentUpdatesAreCountedExactlyOnce(t *testing.T) {
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)

	ResetBindingNoopCountersForTest()

	const (
		goroutines = 8
		perG       = 50
	)
	// One relist no-op and one genuine subject change per iteration, so the
	// arm counts a MIXTURE and cannot pass by counting every call.
	relist := noopArmCRB("conc-relist", "100", "role-a", noopArmUser("alice"))
	realOld := noopArmCRB("conc-real", "100", "role-a", noopArmUser("alice"))
	realNew := noopArmCRB("conc-real", "101", "role-a", noopArmUser("alice"), noopArmUser("bob"))

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
				onBindingUpdate(realOld, realNew)
			}
		}()
	}
	start.Done()
	done.Wait()

	const want = uint64(goroutines * perG)
	if got := RBACBindingNoopUpdatesTotal(); got != want {
		t.Errorf("noop_updates_total = %d; want %d (%d goroutines x %d relist events). "+
			"2x would mean the classification is inside the per-branch old/new blocks and counts "+
			"an event twice; less would mean a lost add",
			got, want, goroutines, perG)
	}
	if got := RBACBindingSemanticNoopUpdatesTotal(); got != want {
		t.Errorf("semantic_noop_updates_total = %d; want %d — the genuine subject-add events must "+
			"contribute ZERO, so this must equal the relist count exactly", got, want)
	}
}

// TestBindingNoopExpvar_ZeroIsPublishedNotOmitted — the zero-readability arm,
// same contract as the sub-generation counters. A `0` here is the answer "no
// no-op updates", and it is only admissible if the key is present while the
// value is 0.
func TestBindingNoopExpvar_ZeroIsPublishedNotOmitted(t *testing.T) {
	RegisterRBACBindingNoopExpvar()
	RegisterRBACBindingNoopExpvar() // idempotent: expvar.Publish panics on a duplicate key.

	ResetBindingNoopCountersForTest()
	t.Cleanup(ResetBindingNoopCountersForTest)

	doc := scrapeDebugVarsForSubGenTest(t)
	for _, key := range []string{
		"snowplow_rbac_binding_noop_updates_total",
		"snowplow_rbac_binding_semantic_noop_updates_total",
	} {
		raw, present := doc[key]
		if !present {
			t.Fatalf("/debug/vars has NO %q key while the value is 0 — a zero that is omitted from the "+
				"payload makes \"no no-op updates\" indistinguishable from \"not instrumented\"", key)
		}
		var v float64
		if err := json.Unmarshal(raw, &v); err != nil {
			t.Fatalf("%s is not a JSON number: %v (raw: %s)", key, err, raw)
		}
		if v != 0 {
			t.Errorf("%s = %v at the floor; want 0", key, v)
		}
	}

	// And they must TRACK — a Func returning a constant 0 passes presence alone.
	activateBindingDeltaHooksForTest(t)
	t.Cleanup(ResetBindingsByGVRIndexForTest)
	t.Cleanup(ResetRBACSubGenForTest)
	crb := noopArmCRB("expvar-relist", "100", "role-a", noopArmUser("alice"))
	onBindingUpdate(crb, crb)

	doc = scrapeDebugVarsForSubGenTest(t)
	for key, want := range map[string]float64{
		"snowplow_rbac_binding_noop_updates_total":          1,
		"snowplow_rbac_binding_semantic_noop_updates_total": 1,
	} {
		var v float64
		if err := json.Unmarshal(doc[key], &v); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if v != want {
			t.Errorf("%s = %v after one relist no-op; want %v — the expvar.Func must read the live "+
				"counter at scrape time", key, v, want)
		}
	}
}
