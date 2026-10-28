package main

// verdict_test.go — site-class 4's own discrimination proof (#912 re-review,
// delta-3 item 3).
//
// Rule A: outside the cache package, the typed checker must flag an ungated
// ResolvedEntry write reached through any receiver with an ungated write's name
// (an ad-hoc interface, a wrapper's method value, an entry built with new() or
// copied), and must not flag the verdict-gated write.
//
// Rule B: inside the package that declares ResolvedCacheStore, it must flag an
// ungated primitive called from a function without a verdict, or with one it
// does not return on first, and an ungated write under an exported name; it
// must accept the gated write and PutContentIfGen.

import (
	"os/exec"
	"strings"
	"testing"
)

func runVerdictOnly(t *testing.T, fixture string) string {
	t.Helper()
	out, err := exec.Command("go", "run", ".", "-verdict-only", fixture).CombinedOutput()
	if err == nil {
		t.Fatalf("GATE DID NOT DISCRIMINATE: the checker exited 0 on %s.\n%s", fixture, out)
	}
	return string(out)
}

func TestGate_VerdictFlagsEveryUngatedResolvedEntryWrite(t *testing.T) {
	s := runVerdictOnly(t, "./testdata/verdict")
	for _, m := range []string{
		"put_sites.go:24  .PutIfGen(",      // direct, on an ad-hoc interface
		"put_sites.go:32  .Put(",           // a wrapper's method value
		"put_sites.go:40  .PutThenRemark(", // ad-hoc interface, copied entry
	} {
		if !strings.Contains(s, m) {
			t.Errorf("expected the checker to flag %q.\nOutput:\n%s", m, s)
		}
	}
	if strings.Contains(s, "put_sites.go:16") {
		t.Errorf("the checker false-flagged the verdict-gated write.\nOutput:\n%s", s)
	}
	if !strings.Contains(s, "ERROR: 3 resolve-output write(s)") {
		t.Errorf("expected exactly 3 findings.\nOutput:\n%s", s)
	}
}

func TestGate_VerdictInsideTheStorePackage(t *testing.T) {
	s := runVerdictOnly(t, "./testdata/verdictcache")
	for _, m := range []string{
		"store.go:42  .put(...) is an ungated store write in SeedForSomething, which takes no PutVerdict",
		"store.go:48  .put(...) is an ungated store write in WriteWithVerdict, whose first statement does not return on !v.Store()",
		"store.go:52  .Put(...) is an ungated store write declared on the store under an exported name",
	} {
		if !strings.Contains(s, m) {
			t.Errorf("expected the checker to flag %q.\nOutput:\n%s", m, s)
		}
	}
	for _, accepted := range []string{"store.go:21", "store.go:24", "store.go:32", "store.go:37"} {
		if strings.Contains(s, accepted+" ") {
			t.Errorf("the checker false-flagged %s (a primitive, the gated write or PutContentIfGen).\nOutput:\n%s", accepted, s)
		}
	}
	if !strings.Contains(s, "ERROR: 3 resolve-output write(s)") {
		t.Errorf("expected exactly 3 findings.\nOutput:\n%s", s)
	}
}
