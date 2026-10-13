package main

// servecell_test.go — site-class 3's own discrimination proof (#643 PR-B): the
// typed checker must FLAG every unrouted (*cache.ResolvedCacheStore) body read
// in testdata/servecell — a direct call, a method value, an empty-reason waiver
// — and must NOT flag the cache.ServeCell read or the reasoned probe. If the
// check regressed (e.g. stopped resolving method values, or matched on a name
// instead of the type), this goes RED.

import (
	"os/exec"
	"strings"
	"testing"
)

func TestServeCellGate_FlagsUnroutedReads(t *testing.T) {
	out, err := exec.Command("go", "run", ".", "-servecell-only", "./testdata/servecell").CombinedOutput()
	if err == nil {
		t.Fatalf("GATE DID NOT DISCRIMINATE: exited 0 on the servecell fixture.\n%s", out)
	}
	s := string(out)
	for _, m := range []string{
		"servecell.go:19  (*ResolvedCacheStore).Get is not routed",        // direct unrouted call
		"servecell.go:23  (*ResolvedCacheStore).GetNoTouch is not routed", // method value
		"servecell.go:29  (*ResolvedCacheStore).GetNoTouch has a //serve-probe: waiver with NO reason",
	} {
		if !strings.Contains(s, m) {
			t.Errorf("expected the checker to flag %q.\nOutput:\n%s", m, s)
		}
	}
	for _, m := range []string{"servecell.go:14", "servecell.go:34"} { // routed read, reasoned probe
		if strings.Contains(s, m) {
			t.Errorf("checker FALSE-flagged %s.\nOutput:\n%s", m, s)
		}
	}
	if !strings.Contains(s, "ERROR: 3 L1 store read(s)") {
		t.Errorf("expected exactly 3 findings.\nOutput:\n%s", s)
	}
}
