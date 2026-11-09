package api

// issue1264_stage_outcomes_summary_test.go — #1264: SummarizeStageOutcomes, the
// parser that turns an X-Snowplow-Stage-Outcomes value into the Server span's
// snowplow.stages.* attributes.
//
// THE ORACLE IS NOT THE PARSER. Each collector arm builds the outcomes it
// expects by hand (register / ran / fail / skip, the same calls the
// orchestrator makes) and states the count and the reasons it put there; the
// parser reads the value HeaderValue rendered from that collector.
//
//   - S1 a resolve with failed, skipped and not-run stages: the count is the
//     number of stages that did not succeed and the reasons are their distinct
//     codes, sorted (a stage with two failing items reports its ranked reason
//     once; two stages with one reason report it once);
//   - S2 every stage succeeded: [] reads failed=0, no reasons;
//   - S3 an aborted resolve: the resolve-level row counts, as in the header;
//   - S4 the truncated summary: its N, and no reasons (the header has none);
//   - S5 the closed set: an unexpected reason is folded to "other", never
//     passed raw; every reason code the package declares is in the set
//     (read from the source, so a new code cannot be left out), and (#1288)
//     classified by the arm's own table: a failure counts in failed and
//     names its reason, a deliberate non-execution counts in NotExecuted;
//   - S6 no header, or a value of neither shape: no summary.

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

func s1264Summary(t *testing.T, v string) StageOutcomeSummary {
	t.Helper()
	s, ok := SummarizeStageOutcomes(v)
	if !ok {
		t.Fatalf("SummarizeStageOutcomes(%q): ok=false, want a summary", v)
	}
	return s
}

func s1264Want(t *testing.T, what string, got StageOutcomeSummary, failed int, reasons ...string) {
	t.Helper()
	if got.Failed != failed || !slices.Equal(got.Reasons, reasons) {
		t.Fatalf("%s: summary = {failed:%d reasons:%q}, want {failed:%d reasons:%q}",
			what, got.Failed, got.Reasons, failed, reasons)
	}
}

func TestIssue1264_S1_FailedStagesCountAndSortedDistinctReasons(t *testing.T) {
	o := newStageOutcomes()
	o.register([]string{"ok", "forbidden", "notfound-a", "notfound-b", "skipped", "never"})
	o.ran("ok")
	// Two items of one stage fail with different reasons: the ranked one is
	// the stage's outcome, and the stage counts once.
	o.fail("forbidden", StageReasonNotFound)
	o.fail("forbidden", StageReasonForbidden)
	o.fail("notfound-a", StageReasonNotFound)
	o.fail("notfound-b", StageReasonNotFound)
	o.skip("skipped", "forbidden")
	// "never" is registered and never ran: NotRun.
	v := o.HeaderValue()
	got := s1264Summary(t, v)
	// 5 stages did not succeed; "ok" did.
	s1264Want(t, "header "+v, got, 5,
		StageReasonForbidden, StageReasonNotFound, StageReasonNotRun, StageReasonSkipped)
	for _, r := range got.Reasons {
		if strings.Contains(r, "never") || strings.Contains(r, "forbidden") || strings.Contains(r, "skipped") {
			t.Fatalf("reason %q carries a stage name", r)
		}
	}
}

func TestIssue1264_S2_AllSucceededIsZeroWithNoReasons(t *testing.T) {
	o := newStageOutcomes()
	o.register([]string{"a", "b"})
	o.ran("a")
	o.ran("b")
	v := o.HeaderValue()
	if v != "[]" {
		t.Fatalf("SETUP: an all-success collector rendered %q, want []", v)
	}
	got := s1264Summary(t, v)
	s1264Want(t, "all succeeded", got, 0)
	if got.Reasons != nil {
		t.Fatalf("all succeeded: Reasons = %#v, want nil (no attribute)", got.Reasons)
	}
}

func TestIssue1264_S3_AbortedResolveCountsTheResolveRow(t *testing.T) {
	ctx, o := WithStageOutcomes(context.Background())
	abortedResolve(ctx, []*templates.API{{Name: "a"}, {Name: "b"}})
	v := o.HeaderValue()
	// The header lists the resolve-level Aborted row and both stages NotRun.
	s1264Want(t, "aborted "+v, s1264Summary(t, v), 3, StageReasonAborted, StageReasonNotRun)
}

func TestIssue1264_S4_TruncatedSummaryKeepsItsCountAndNamesNoReason(t *testing.T) {
	o := newStageOutcomes()
	names := make([]string, 0, 120)
	for i := 0; i < 120; i++ {
		names = append(names, fmt.Sprintf("a-deliberately-long-stage-name-for-the-header-bound-%03d", i))
	}
	o.register(names)
	for _, n := range names {
		o.fail(n, StageReasonForbidden)
	}
	v := o.HeaderValue()
	if !strings.Contains(v, `"truncated":true`) {
		t.Fatalf("SETUP: 120 failed stages rendered %d bytes without the truncated summary", len(v))
	}
	got := s1264Summary(t, v)
	s1264Want(t, "truncated "+v, got, 120)
	if got.Reasons != nil {
		t.Fatalf("truncated: Reasons = %#v, want nil: the summary names none, and none is invented", got.Reasons)
	}
}

func TestIssue1264_S5_ReasonsComeFromTheClosedSet(t *testing.T) {
	const raw = "<script>alice@example.com</script>"
	v := `[{"name":"x","ok":false,"reason":"` + raw + `"},` +
		`{"name":"y","ok":false,"reason":"Forbidden"},` +
		`{"name":"z","ok":false,"reason":"AlsoUnknown"},` +
		`{"name":"w","ok":false}]`
	got := s1264Summary(t, v)
	s1264Want(t, "unknown reasons", got, 4, StageReasonForbidden, StageReasonOther)
	for _, r := range got.Reasons {
		if r == raw || r == "AlsoUnknown" || r == "" {
			t.Fatalf("reason %q passed through raw", r)
		}
	}

	// Every StageReason* code this package declares is in the closed set: a
	// code added later without joining it would read "other" on every span.
	// #1288: and every one is classified, here, by hand: a failure (failed 1,
	// its reason named) or a deliberate non-execution (not_executed 1, failed
	// 0, no reason named). A new code must be added to s1264ReasonTable with
	// its class, or this arm fails.
	for n, c := range s1264ReasonTable(t) {
		if !stageReasonKnown(c.code) {
			t.Errorf("%s (%q) is not in the closed set: a span would read it as %q", n, c.code, StageReasonOther)
		}
		got := s1264Summary(t, `[{"name":"s","ok":false,"reason":"`+c.code+`"}]`)
		if c.notExecuted {
			s1264Want(t, n, got, 0)
			if got.NotExecuted != 1 {
				t.Errorf("%s: NotExecuted = %d, want 1 (a deliberate non-execution)", n, got.NotExecuted)
			}
			continue
		}
		s1264Want(t, n, got, 1, c.code)
		if got.NotExecuted != 0 {
			t.Errorf("%s: NotExecuted = %d, want 0 (a failure)", n, got.NotExecuted)
		}
	}
}

// s1264Reason is one declared reason code and its expected #1288 class.
type s1264Reason struct {
	code        string
	notExecuted bool // a deliberate non-execution, not a failure
}

// s1264ReasonTable is every StageReason* constant the package declares, by
// name, with its value and class. It fails when the package declares a code
// the table does not classify (or the table names one the package dropped).
func s1264ReasonTable(t *testing.T) map[string]s1264Reason {
	t.Helper()
	table := map[string]s1264Reason{
		"StageReasonNotExecuted":        {StageReasonNotExecuted, true},
		"StageReasonWriteRefused":       {StageReasonWriteRefused, true},
		"StageReasonForbidden":          {StageReasonForbidden, false},
		"StageReasonNotFound":           {StageReasonNotFound, false},
		"StageReasonUnauthorized":       {StageReasonUnauthorized, false},
		"StageReasonNotRun":             {StageReasonNotRun, false},
		"StageReasonError":              {StageReasonError, false},
		"StageReasonSkipped":            {StageReasonSkipped, false},
		"StageReasonAborted":            {StageReasonAborted, false},
		"StageReasonInvalidPath":        {StageReasonInvalidPath, false},
		"StageReasonTemplateError":      {StageReasonTemplateError, false},
		"StageReasonNotSent":            {StageReasonNotSent, false},
		"StageReasonTimeout":            {StageReasonTimeout, false},
		"StageReasonDestinationRefused": {StageReasonDestinationRefused, false},
		"StageReasonUAFOnNonReadVerb":   {StageReasonUAFOnNonReadVerb, false}, // #1281: a misconfigured stage
	}
	declared := s1264DeclaredReasonConsts(t)
	var tableNames []string
	for n := range table {
		tableNames = append(tableNames, n)
	}
	sort.Strings(tableNames)
	if !slices.Equal(declared, tableNames) {
		t.Fatalf("the package declares reason codes %v; the table classifies %v: add the new code to s1264ReasonTable with its class", declared, tableNames)
	}
	return table
}

// s1264DeclaredReasonConsts is every StageReason* constant the package's
// non-test source declares, except StageReasonOther (the fold value itself).
func s1264DeclaredReasonConsts(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var out []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range af.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, sp := range gd.Specs {
				for _, n := range sp.(*ast.ValueSpec).Names {
					if strings.HasPrefix(n.Name, "StageReason") && n.Name != "StageReasonOther" {
						out = append(out, n.Name)
					}
				}
			}
		}
	}
	sort.Strings(out)
	if len(out) == 0 {
		t.Fatal("SETUP: found no StageReason* constants in the package source")
	}
	return out
}

func TestIssue1264_S6_NoHeaderOrAnUnreadableValueIsNoSummary(t *testing.T) {
	for _, v := range []string{"", "not json", `{"truncated":false,"failed":2}`, `{"failed":3}`,
		`{"truncated":true}`, `{"truncated":true,"failed":-1}`, `"[]"`} {
		if s, ok := SummarizeStageOutcomes(v); ok {
			t.Errorf("SummarizeStageOutcomes(%q) = %+v, ok; want no summary", v, s)
		}
	}
}
