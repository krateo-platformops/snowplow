package main

// verdict_test.go — site-class 4's own discrimination proof (#912 re-review):
// the typed checker must flag an ungated ResolvedEntry write reached directly,
// through a method value and through an ad-hoc interface over the store (the
// entry built with new() or copied, without a literal), and must not flag the
// verdict-gated write.

import (
	"os/exec"
	"strings"
	"testing"
)

func TestGate_VerdictFlagsEveryUngatedResolvedEntryWrite(t *testing.T) {
	out, err := exec.Command("go", "run", ".", "-verdict-only", "./testdata/verdict").CombinedOutput()
	if err == nil {
		t.Fatalf("GATE DID NOT DISCRIMINATE: the checker exited 0 on the verdict fixture.\n%s", out)
	}
	s := string(out)
	for _, m := range []string{
		"put_sites.go:18  .PutIfGen(",      // direct
		"put_sites.go:22  .Put(",           // method value
		"put_sites.go:31  .PutThenRemark(", // ad-hoc interface, copied entry
	} {
		if !strings.Contains(s, m) {
			t.Errorf("expected the checker to flag %q.\nOutput:\n%s", m, s)
		}
	}
	if strings.Contains(s, "put_sites.go:14") {
		t.Errorf("the checker false-flagged the verdict-gated write.\nOutput:\n%s", s)
	}
	if !strings.Contains(s, "ERROR: 3 resolve-output write(s)") {
		t.Errorf("expected exactly 3 findings.\nOutput:\n%s", s)
	}
}
