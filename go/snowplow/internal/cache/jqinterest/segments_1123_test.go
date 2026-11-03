package jqinterest

import (
	"testing"

	"github.com/krateo-platformops/snowplow/internal/pathtemplate"
)

// #1123 — a stage path may declare a multi-segment value with segments(E). The
// analysis must read it as a function of its argument, under the name the
// renderer defines, or every RA that adopts the declared form loses its
// read-set narrowing to "unknown function".
func TestIssue1123_SegmentsIsAGenericBuiltin(t *testing.T) {
	if !genericBuiltins[pathtemplate.SegmentsFunc+"/1"] {
		t.Fatalf("%s/1 is not a generic builtin of the analysis", pathtemplate.SegmentsFunc)
	}
}
