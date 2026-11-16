package main

import (
	"strings"
	"testing"
)

// The self-test is the guard's positive control, so CI's go test runs it too.
func TestSelfTest_TheGoodChainPassesAndEveryDefectIsCaught(t *testing.T) {
	if bad := selfTest(); len(bad) != 0 {
		t.Fatalf("self-test: %q", bad)
	}
}

// The tree itself: the guard must see the real chain and find nothing.
func TestTheRepositoryPublishesOnlyThroughTheSlot(t *testing.T) {
	srcs, err := parseDir("../..")
	if err != nil {
		t.Fatal(err)
	}
	if len(srcs) < 100 {
		t.Fatalf("PRECONDITION: only %d files scanned", len(srcs))
	}
	if f := check(srcs); len(f) != 0 {
		t.Fatalf("findings on the tree:\n%s", strings.Join(f, "\n"))
	}
}

// Zero files is a failure, never a pass.
func TestAnEmptyScanFails(t *testing.T) {
	if f := check(nil); len(f) == 0 {
		t.Fatal("an empty scan reported no findings: a broken guard would pass")
	}
}
