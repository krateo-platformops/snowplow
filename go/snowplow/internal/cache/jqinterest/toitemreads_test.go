package jqinterest

import (
	"testing"

	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

// TestIssue666_ToItemReadsWitnessesAreRedByConstruction: every #666 arm is
// RED through its fixed witness ALONE (no fuzz, no seed), and the witness is
// silent with every rule in place. So the arm is RED on the default path and
// under -tags jqinterest_exhaustive alike, whatever chains a seed reaches.
//
// toItemReads rules NOT pinned by a mutation, and why:
//   - apiVersion/kind of the envelope read nothing: constant for a GVR;
//   - .items.<key> reads nothing: an error on an array, the array's kind
//     is membership;
//   - a kind/shape read of the envelope or of .items reads nothing: the
//     envelope is always an object with the same keys (E6) and the items
//     array's kind and length are membership (ADD/DELETE always mark);
//   - any other envelope key makes the edge All. Under E6 an informer-served
//     envelope has no other key, and neither does the emulated one, so a
//     mutation that read nothing there could not be told apart: it is not
//     pinnable by a differential arm;
//   - itemExcl dropping an exclusion only reads MORE (never unsound), and an
//     exclusion relative to the items array that starts with [] cannot be
//     produced (del() takes key-only paths).
func TestIssue666_ToItemReadsWitnessesAreRedByConstruction(t *testing.T) {
	want := map[string]int{
		mutItemsWholeDropped: 2, mutEnvelopeWholeDropped: 1, mutEnvelopeIterDropped: 1,
		mutItemsElemDropped: 1, mutItemExclMisapplied: 1,
	}
	got := map[string]int{}
	for _, a := range soundnessArms() {
		if _, ok := want[a.mutation]; !ok {
			continue
		}
		got[a.mutation]++
		in := Analyze(a.chain, jqsupport.ModuleLoader())
		if in.All || len(in.Reads) == 0 {
			t.Errorf("%s: the arm must be narrowed and read something with every rule in place: %s", a.name, in)
		}
		if witnessCounterexample(a) {
			t.Errorf("%s: the witness fires with every rule in place", a.name)
		}
		testMutations = map[string]bool{a.mutation: true}
		mut := Analyze(a.chain, jqsupport.ModuleLoader())
		red := witnessCounterexample(a)
		testMutations = nil
		if !red {
			t.Errorf("%s: the witness does not fire under %q (interest %s)", a.name, a.mutation, mut)
			continue
		}
		t.Logf("RED by witness under %-22s %s: %s -> %s", a.mutation, a.name, in, mut)
	}
	for m, n := range want {
		if got[m] != n {
			t.Errorf("mutation %q: %d arms, want %d", m, got[m], n)
		}
	}
}
