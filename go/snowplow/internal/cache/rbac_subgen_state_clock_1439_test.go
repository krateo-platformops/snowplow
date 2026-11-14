// rbac_subgen_state_clock_1439_test.go — #1439: the sub-generation is the number
// of the last RBAC state change that touched the identity.
//
// These arms pin the two halves of the fold on the exact values, from a zeroed
// clock (the end-to-end arms, on the bytes served, are in
// handlers/dispatchers/issue1439_rbac_state_naming_test.go):
//
//   - a bump writes ONE new state of the process-wide clock into every subject
//     it names (not +1 per subject);
//   - an identity reads the LATEST state over its subjects (not their sum);
//   - the #682 boot-window epoch is one more state of the same clock.
package cache

import "testing"

func TestIssue1439_BumpWritesOneStateAndTheFoldIsTheLatest(t *testing.T) {
	ResetRBACSubGenForTest()
	t.Cleanup(ResetRBACSubGenForTest)

	user := func(n string) subjectKey { return subjectKey{Kind: subjectKindUser, Name: n} }
	group := func(n string) subjectKey { return subjectKey{Kind: subjectKindGroup, Name: n} }
	read := func(u string, groups ...string) uint64 { return RBACSubGenForSubject(u, groups) }

	if got := read("carol", "team"); got != 0 {
		t.Fatalf("an identity none of whose subjects was touched must read 0; got %d", got)
	}

	// State 1 and 2: carol's own history (a binding granted, then revoked).
	BumpSubjectSubGens([]subjectKey{user("carol")})
	BumpSubjectSubGens([]subjectKey{user("carol")})
	if got := read("carol", "team"); got != 2 {
		t.Fatalf("carol must read state 2 (her last own change); got %d", got)
	}
	if got := read("dave", "team"); got != 0 {
		t.Fatalf("dave shares no bumped subject and must still read 0; got %d", got)
	}

	// An empty flush is not a state.
	BumpSubjectSubGens(nil)

	// State 3: one publish touching the shared group AND an unrelated user. Both
	// get the SAME number.
	BumpSubjectSubGens([]subjectKey{group("team"), user("erin")})
	if g, e := subGenValue(group("team")), subGenValue(user("erin")); g != 3 || e != 3 {
		t.Fatalf("every subject of one bump must hold that bump's state 3; group=%d user=%d", g, e)
	}
	// The latest of {carol: 2, team: 3} is 3 — a sum would read 5; the latest of
	// {dave: untouched, team: 3} is 3 too: one state, one value.
	if c, d := read("carol", "team"), read("dave", "team"); c != 3 || d != 3 {
		t.Fatalf("both holders of the group must read state 3 after the shared change; carol=%d dave=%d", c, d)
	}

	// State 4: the shared group again. Dave now reads 4. Carol read 2 before
	// state 3 (above) — the value "2" was hers alone and no identity reads it
	// after it: under +1-per-bump dave would read 2 here.
	BumpSubjectSubGens([]subjectKey{group("team")})
	if d := read("dave", "team"); d != 4 {
		t.Fatalf("dave must read state 4; got %d", d)
	}

	// A ServiceAccount identity folds its SA counter the same way.
	BumpSubjectSubGens([]subjectKey{{Kind: subjectKindServiceAccount, Name: "runner", Namespace: "ci"}})
	if got := read("system:serviceaccount:ci:runner", "team"); got != 5 {
		t.Fatalf("the SA identity must read state 5 (its own change, later than the group's 4); got %d", got)
	}
}

func TestIssue1439_EpochIsOneStateOfTheSameClock(t *testing.T) {
	ResetRBACSubGenForTest()
	t.Cleanup(ResetRBACSubGenForTest)

	group := subjectKey{Kind: subjectKindGroup, Name: "team"}
	BumpSubjectSubGens([]subjectKey{group}) // state 1
	if got := RBACSubGenForSubject("carol", []string{"team"}); got != 1 {
		t.Fatalf("precondition: carol reads state 1; got %d", got)
	}

	// The boot-window rotation: an RBAC event was noted before the first index
	// build. It takes state 2 and every identity moves to it, touched or not.
	rbacEventSeenPreBuild.Store(true)
	if !rotateIfRBACEventSeenPreBuild() {
		t.Fatalf("the noted pre-build event must rotate")
	}
	if got := RBACSubGenEpoch(); got != 1 {
		t.Fatalf("RBACSubGenEpoch counts the boot-window rotations and must read 1; got %d", got)
	}
	if c, o := RBACSubGenForSubject("carol", []string{"team"}), RBACSubGenForSubject("outsider", nil); c != 2 || o != 2 {
		t.Fatalf("every identity must read the epoch's state 2; carol=%d outsider=%d", c, o)
	}
	// No second rotation without a new note.
	if rotateIfRBACEventSeenPreBuild() {
		t.Fatalf("a rotation with no noted event must not happen")
	}
	// A later per-subject bump is a later state than the epoch's.
	BumpSubjectSubGens([]subjectKey{group}) // state 3
	if c, o := RBACSubGenForSubject("carol", []string{"team"}), RBACSubGenForSubject("outsider", nil); c != 3 || o != 2 {
		t.Fatalf("carol must read state 3 and the outsider stay on the epoch's state 2; carol=%d outsider=%d", c, o)
	}
}
