package main

// verdict_test.go — site-class 4's own discrimination proof (#912 re-review):
// the checker must FLAG a ResolvedEntry Put site whose function never reads the
// shared Put-gate verdict (and an annotation with no reason), and accept a
// gated site and a named one.

import (
	"os/exec"
	"strings"
	"testing"
)

func TestGate_VerdictFlagsAPutSiteThatBypassesTheVerdict(t *testing.T) {
	out, err := exec.Command("go", "run", ".", "-verdict-only", "testdata/verdict").CombinedOutput()
	if err == nil {
		t.Fatalf("GATE DID NOT DISCRIMINATE: the checker exited 0 on the verdict fixture.\n%s", out)
	}
	s := string(out)
	for _, m := range []string{"put_sites.go:25", "put_sites.go:31"} { // bypassSite, emptyReasonSite
		if !strings.Contains(s, m) {
			t.Errorf("expected the checker to flag %s.\nOutput:\n%s", m, s)
		}
	}
	for _, m := range []string{"put_sites.go:14", "put_sites.go:20"} { // gatedSite, namedSite
		if strings.Contains(s, m) {
			t.Errorf("the checker false-flagged the well-formed site %s.\nOutput:\n%s", m, s)
		}
	}
	if !strings.Contains(s, "never calls PutGateVerdict") || !strings.Contains(s, "with NO reason") {
		t.Errorf("expected both diagnostics.\nOutput:\n%s", s)
	}
}
