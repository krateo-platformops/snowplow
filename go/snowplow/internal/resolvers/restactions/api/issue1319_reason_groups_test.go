package api

// issue1319_reason_groups_test.go — #1319 contract addition: every stage
// reason code of the closed set is in exactly one client group ("cut /
// failed", "denied" or "by design"), and the frontend integration doc lists
// exactly those groups, so neither can drift.
//
//   G1 the closed set, read from the CODE (every StageReason* constant
//      declared in this package, the classes map and the rank map), equals
//      the set StageReasonGroups places, and every placement is one of the
//      three groups; an unknown code reads "cut / failed";
//   G2 the doc's three tables list exactly StageReasonGroups.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// g1319ReasonConsts returns the names of every StageReason* constant declared
// in this package's non-test files, except StageReasonOther (the span's fold
// value, never sent in a header).
func g1319ReasonConsts(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	fset := token.NewFileSet()
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		for _, d := range af.Decls {
			gd, ok := d.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}
			for _, sp := range gd.Specs {
				for _, n := range sp.(*ast.ValueSpec).Names {
					if strings.HasPrefix(n.Name, "StageReason") && n.Name != "StageReasonOther" {
						names = append(names, n.Name)
					}
				}
			}
		}
	}
	sort.Strings(names)
	return names
}

func g1319Keys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestIssue1319_G1_EveryReasonCodeIsInExactlyOneGroup(t *testing.T) {
	consts := g1319ReasonConsts(t)
	if len(consts) == 0 {
		t.Fatal("SETUP: found no StageReason* constant")
	}
	// A code declared as a constant but missing from the groups: the counts
	// differ (the constants' values are the classes map's keys, checked below).
	if len(consts) != len(StageReasonGroups) {
		t.Fatalf("G1 RED: %d StageReason* constants %v, but StageReasonGroups places %d codes %v — place every new code in a group",
			len(consts), consts, len(StageReasonGroups), g1319Keys(StageReasonGroups))
	}
	groups, classes, ranks := g1319Keys(StageReasonGroups), g1319Keys(stageReasonClasses), g1319Keys(reasonRank)
	if strings.Join(groups, ",") != strings.Join(classes, ",") {
		t.Fatalf("G1 RED: the closed set (stageReasonClasses) %v differs from StageReasonGroups %v", classes, groups)
	}
	// reasonRank holds the codes fail() records (NotRun, Skipped and Aborted
	// are rendered, never recorded): each must be placed too.
	for _, code := range ranks {
		if _, ok := StageReasonGroups[code]; !ok {
			t.Fatalf("G1 RED: the ranked code %s is in no group", code)
		}
	}
	for code, g := range StageReasonGroups {
		if g != StageGroupCutFailed && g != StageGroupDenied && g != StageGroupByDesign {
			t.Fatalf("G1 RED: %s is in group %q, want one of the three groups", code, g)
		}
		// The by-design group is exactly the #1288 classifier's.
		if (g == StageGroupByDesign) != stageNotExecutedByDesign(code) {
			t.Fatalf("G1 RED: %s is in %q but stageNotExecutedByDesign=%v", code, g, stageNotExecutedByDesign(code))
		}
	}
	if g := StageReasonGroup("SomeFutureCode"); g != StageGroupCutFailed {
		t.Fatalf("G1 RED: an unknown code reads %q, want %q", g, StageGroupCutFailed)
	}
}

func TestIssue1319_G2_TheDocListsExactlyTheGroups(t *testing.T) {
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "docs", "howto-frontend-widget-stage-outcomes.md"))
	if err != nil {
		t.Fatalf("read the integration doc: %v", err)
	}
	row := regexp.MustCompile("^\\| `([A-Za-z]+)` \\|")
	doc := map[string]string{}
	group := ""
	for _, line := range strings.Split(string(b), "\n") {
		switch {
		case strings.HasPrefix(line, "### "):
			group = strings.TrimPrefix(line, "### ")
		case strings.HasPrefix(line, "## "):
			group = ""
		}
		m := row.FindStringSubmatch(line)
		if m == nil || group == "" {
			continue
		}
		if prev, dup := doc[m[1]]; dup {
			t.Fatalf("G2 RED: the doc lists %s twice (%q and %q)", m[1], prev, group)
		}
		doc[m[1]] = group
	}
	for code, g := range StageReasonGroups {
		if doc[code] != g {
			t.Fatalf("G2 RED: the doc puts %s in %q, the code in %q", code, doc[code], g)
		}
	}
	if len(doc) != len(StageReasonGroups) {
		t.Fatalf("G2 RED: the doc's group tables list %d codes %v, the code %d %v",
			len(doc), g1319Keys(doc), len(StageReasonGroups), g1319Keys(StageReasonGroups))
	}
}
