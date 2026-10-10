// main_test.go — the #449 identity-key gate's own discrimination proof.
//
// testdata/red is a miniature tree with the sources of truth intact (R0 passes)
// and one site per rule shaped like the defect it exists for: an identity-bound
// key hashed with no class (#435), hand-assembled identity fields (pre-#449),
// an IdentityClass edited between IdentityClassOf and SetIdentity and pointers
// to identity fields (reviewer-424 D3/D4), a
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
		"R1 internal/app/sites.go:42":  "identity-bound key hashed with no class (#435 shape)",
		"R1 internal/app/sites.go:83":  "identity-free class given an identity",
		"R2 internal/app/sites.go:49":  "identity dimension in a literal",
		"R2 internal/app/sites.go:52":  "identity dimension assigned directly",
		"R3 internal/app/sites.go:59":  "forged class",
		"R3 internal/app/sites.go:65":  "forged class through a forwarding builder",
		"R3 internal/app/sites.go:70":  "production call of the identity test seam",
		"R2 internal/app/sites.go:90":  "IdentityClass field edited before SetIdentity (review D4)",
		"R2 internal/app/sites.go:100": "address of a ResolvedKeyInputs identity field (review D3)",
		"R2 internal/app/sites.go:108": "address of an IdentityClass field",
		"R3 internal/app/sites.go:113": "a second class derivation",
		"R4 internal/app/sites.go:76":  "reuse key without the class (#432 shape)",
		"R5 internal/cache/key.go:73":  "unregistered store",

		// #180 — the SECOND dimension category. A scope dimension is a function of
		// (requester, access domain), so it cannot come from IdentityClassOf and
		// gets its own branch, writer and derivation. S0/S2/S3 are the analogues
		// of R0/R2/R3, and these are one site per rule.
		"S0 internal/cache/key.go:90":  "a scope derivation that cannot see the access domain",
		"S2 internal/app/sites.go:119": "scope dimension assigned directly",
		"S2 internal/app/sites.go:125": "scope dimension set in a literal",
		"S3 internal/app/sites.go:131": "a second scope derivation",
		// The two scope fixtures are ALSO R1 violations and that is correct, not a
		// false positive: class "widgets" is identity-bound, so those sites owe an
		// identity as well as a scope. Asserting them keeps the exact-count arm
		// honest instead of loosening it.
		"R1 internal/app/sites.go:120": "scope fixture also hashes identity-bound inputs with no class",
		"R1 internal/app/sites.go:126": "scope fixture also hashes identity-bound inputs with no class",
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
		// #180 added a SECOND rule family (S*). Counting only "R" would make the
		// exact-count arm silently blind to every scope finding — the arm would
		// still pass while proving nothing about the new category.
		if !strings.HasPrefix(line, "R") && !strings.HasPrefix(line, "S") {
			continue
		}
		flagged++
		if strings.HasPrefix(line, "R0") {
			t.Errorf("checker FALSE-flagged an intact source of truth: %s", line)
		}
		if n := lineNo(line); n >= 10 && n <= 37 && !strings.Contains(line, "key.go") {
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
