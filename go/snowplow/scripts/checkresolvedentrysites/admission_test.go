package main

// admission_test.go — site-class 6's own discrimination proof (#1233): the
// checker flags a putCoreLocked call without a prior !admitWrite return, one
// admitted only after the insertion, and an index insertion or LRU push outside
// putCoreLocked; it accepts putCoreLocked itself and an admitted write.

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
		"store.go:39  putUnadmitted calls putCoreLocked without returning on !admitWrite(...) first",
		"store.go:44  putAdmittedLate calls putCoreLocked without returning on !admitWrite(...) first",
		"store.go:52  insertByHand pushes onto the store's LRU order",
		"store.go:53  insertByHand inserts into the store's index",
	} {
		if !strings.Contains(s, m) {
			t.Errorf("expected the checker to flag %q.\nOutput:\n%s", m, s)
		}
	}
	for _, accepted := range []string{"store.go:24 ", "store.go:25 ", "store.go:33 "} {
		if strings.Contains(s, accepted) {
			t.Errorf("the checker false-flagged %s (putCoreLocked itself or an admitted write).\nOutput:\n%s", accepted, s)
		}
	}
	if !strings.Contains(s, "ERROR: 4 store write(s) are not admitted") {
		t.Errorf("expected exactly 4 findings.\nOutput:\n%s", s)
	}
}
