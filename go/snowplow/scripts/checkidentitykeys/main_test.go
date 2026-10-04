// main_test.go — the #449 identity-key gate's own discrimination proof.
//
// testdata/red is a miniature tree with the sources of truth intact (R0 passes)
// and one site per rule shaped like the defect it exists for: an identity-bound
// key hashed with no class (#435), hand-assembled identity fields (pre-#449), a
// forged class (direct, forwarded, through a test seam), a second class
// derivation outside rbac.IdentityClassOf, a reuse key without
// the class (#432), an identity-free class given an identity, and an
// unregistered store. The checker must flag exactly those lines and none of the
// well-formed sites beside them; and it must pass on the real tree.
package main

import (
	"os/exec"
	"strings"
	"testing"
)

func TestGate_FlagsRedFixture(t *testing.T) {
	out, err := exec.Command("go", "run", ".", "testdata/red").CombinedOutput()
	if err == nil {
		t.Fatalf("GATE DID NOT DISCRIMINATE: checker exited 0 on the RED fixture.\n%s", out)
	}
	s := string(out)
	mustFlag := map[string]string{
		"R1 internal/app/sites.go:42": "identity-bound key hashed with no class (#435 shape)",
		"R1 internal/app/sites.go:83": "identity-free class given an identity",
		"R2 internal/app/sites.go:49": "identity dimension in a literal",
		"R2 internal/app/sites.go:52": "identity dimension assigned directly",
		"R3 internal/app/sites.go:59": "forged class",
		"R3 internal/app/sites.go:65": "forged class through a forwarding builder",
		"R3 internal/app/sites.go:70": "production call of the identity test seam",
		"R3 internal/app/sites.go:88": "a second class derivation",
		"R4 internal/app/sites.go:76": "reuse key without the class (#432 shape)",
		"R5 internal/cache/key.go:66": "unregistered store",
	}
	for site, what := range mustFlag {
		if !strings.Contains(s, site+":") {
			t.Errorf("expected the checker to flag %s (%s); it did not.\nOutput:\n%s", site, what, s)
		}
	}
	// The well-formed sites (goodDispatch / goodRAFullList / goodRehash /
	// goodContent / goodMemo, lines 11-37) and the intact sources of truth
	// must not be flagged.
	flagged := 0
	for _, line := range strings.Split(s, "\n") {
		if !strings.HasPrefix(line, "R") {
			continue
		}
		flagged++
		if strings.HasPrefix(line, "R0") {
			t.Errorf("checker FALSE-flagged an intact source of truth: %s", line)
		}
		if n := lineNo(line); n >= 10 && n <= 37 {
			t.Errorf("checker FALSE-flagged a well-formed site: %s", line)
		}
	}
	if flagged != len(mustFlag) {
		t.Errorf("checker flagged %d sites, want exactly %d.\nOutput:\n%s", flagged, len(mustFlag), s)
	}
}

func TestGate_ProductionTreeIsClean(t *testing.T) {
	out, err := exec.Command("go", "run", ".", "../..").CombinedOutput()
	if err != nil {
		t.Fatalf("the identity-key gate fails on the production tree:\n%s", out)
	}
}

func lineNo(line string) int {
	i := strings.Index(line, "sites.go:")
	if i < 0 {
		return -1
	}
	n := 0
	for _, c := range line[i+len("sites.go:"):] {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int(c-'0')
	}
	return n
}
