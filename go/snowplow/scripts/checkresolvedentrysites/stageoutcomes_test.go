package main

// stageoutcomes_test.go — site-class 5's own discrimination proof (#911
// re-review): a dispatcher Put site that drops the stage outcomes (and a waiver
// with no reason) is flagged; a site that keeps them, and a reasoned waiver,
// are not.

import (
	"strings"
	"testing"
)

func TestGate_StageOutcomesFlagsADispatcherPutThatDropsThem(t *testing.T) {
	findings, sites, err := checkStageOutcomesTree("testdata/stageoutcomes")
	if err != nil {
		t.Fatal(err)
	}
	if sites != 4 {
		t.Fatalf("PRE: the fixture has 4 dispatcher Put sites, the walk saw %d", sites)
	}
	got := map[int]string{}
	for _, f := range findings {
		if !strings.HasSuffix(f.file, "put_sites.go") {
			t.Fatalf("unexpected finding in %s", f.file)
		}
		got[f.line] = f.kind
	}
	want := map[int]string{
		21: "missing-stage-outcomes",      // droppedSite
		27: "empty-stage-outcomes-waiver", // emptyReasonSite
	}
	if len(got) != len(want) {
		t.Fatalf("GATE DID NOT DISCRIMINATE: findings %v, want %v", got, want)
	}
	for line, kind := range want {
		if got[line] != kind {
			t.Errorf("line %d: finding %q, want %q (all: %v)", line, got[line], kind, got)
		}
	}
}
