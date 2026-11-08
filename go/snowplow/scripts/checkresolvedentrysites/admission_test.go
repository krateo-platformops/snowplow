package main

// admission_test.go — site-class 6's own discrimination proof (#1233): the
// checker flags a putCoreLocked call without a top-level !admitWrite return
// before it, including the shapes that only look guarded (an admission after the
// insertion, in a branch the write can skip, in a closure, behind &&), and an
// index insertion or LRU push outside putCoreLocked; it accepts putCoreLocked
// itself and an admitted write.

import (
	"os/exec"
	"strings"
	"testing"
)

func TestGate_AdmissionFlagsEveryUnadmittedStoreWrite(t *testing.T) {
	out, err := exec.Command("go", "run", ".", "-admission-only", "./testdata/admission").CombinedOutput()
	s := string(out)
	if err == nil {
		t.Fatalf("GATE DID NOT DISCRIMINATE: the checker exited 0.\n%s", s)
	}
	for _, m := range []string{
		"store.go:42  putUnadmitted calls putCoreLocked without returning on !admitWrite(...) first",
		"store.go:47  putAdmittedLate calls putCoreLocked without returning on !admitWrite(...) first",
		"store.go:55  insertByHand pushes onto the store's LRU order",
		"store.go:56  insertByHand inserts into the store's index",
		// The #1243 review's mutant: the admission in a branch an unpinned write skips.
		"store.go:67  putAdmittedInABranch calls putCoreLocked without returning on !admitWrite(...) first",
		"store.go:78  putAdmittedInAClosure calls putCoreLocked without returning on !admitWrite(...) first",
		"store.go:86  putAdmittedBehindAnd calls putCoreLocked without returning on !admitWrite(...) first",
	} {
		if !strings.Contains(s, m) {
			t.Errorf("expected the checker to flag %q.\nOutput:\n%s", m, s)
		}
	}
	for _, accepted := range []string{"store.go:27 ", "store.go:28 ", "store.go:36 "} {
		if strings.Contains(s, accepted) {
			t.Errorf("the checker false-flagged %s (putCoreLocked itself or an admitted write).\nOutput:\n%s", accepted, s)
		}
	}
	if !strings.Contains(s, "ERROR: 7 store write(s) are not admitted") {
		t.Errorf("expected exactly 7 findings.\nOutput:\n%s", s)
	}
}
