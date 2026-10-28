// stageoutcomes.go — site-class 5 (#911 re-review): every dispatcher Put site
// of a resolve output carries the stage outcomes, or says why it cannot hold a
// RESTAction answer.
//
// THE DEFECT CLASS: X-Snowplow-Stage-Outcomes on a CACHED answer is the value
// the resolve that stored the cell kept on the entry (#904). It is kept by hand
// at each restactions-class Put site (the /call dispatcher, the cohort seed, the
// refresher), exactly the "stamp ALL sites of a class, or none" shape #118(d)
// found: a new Put site that forgets the field silently answers [] ("all
// succeeded") for a resolve whose write was skipped. This gate makes that a hard
// failure:
//
//   - every `ResolvedEntry{...}` literal under internal/handlers/dispatchers
//     must set StageOutcomes as a keyed element, OR
//   - carry `// stage-outcomes-waiver: <reason>` on its line or in the comment
//     block directly above it — a cell of a class that never answers a
//     RESTAction /call (widgets, widgetContent). The reason is mandatory.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// stageOutcomesField is the entry field the site-class 5 invariant is keyed on.
const stageOutcomesField = "StageOutcomes"

// stageOutcomesWaiverPrefix exempts a literal of a class that never answers a
// RESTAction /call.
const stageOutcomesWaiverPrefix = "stage-outcomes-waiver:"

// stageOutcomesScope is the path segment of the dispatcher Put sites.
const stageOutcomesScope = "/handlers/dispatchers/"

// checkStageOutcomesTree walks root and returns the site-class 5 findings and
// the number of in-scope Put sites.
func checkStageOutcomesTree(root string) ([]entryFinding, int, error) {
	fset := token.NewFileSet()
	var findings []entryFinding
	sites := 0
	// #nosec G304,G703 -- dev-time AST lint tool; the walked path is a filepath.WalkDir result under the CLI-arg target dir, not untrusted input.
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".claude", "vendor", "testdata":
				if path != root {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") ||
			!strings.Contains(filepath.ToSlash(path), stageOutcomesScope) {
			return nil
		}
		src, perr := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution) //nosec G304,G703 -- dev-time AST lint tool; path is a filepath.WalkDir result
		if perr != nil {
			return fmt.Errorf("parse %s: %w", path, perr)
		}
		commentByLine := map[int]string{}
		for _, cg := range src.Comments {
			for _, c := range cg.List {
				commentByLine[fset.Position(c.Pos()).Line] = strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
			}
		}
		ast.Inspect(src, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok || !isResolvedEntry(cl.Type) {
				return true
			}
			sites++
			if hasKeyedField(cl, stageOutcomesField) {
				return true
			}
			pos := fset.Position(cl.Pos())
			reason, waived := stageOutcomesWaiver(commentByLine, pos.Line)
			switch {
			case !waived:
				findings = append(findings, entryFinding{file: pos.Filename, line: pos.Line, kind: "missing-stage-outcomes"})
			case reason == "":
				findings = append(findings, entryFinding{file: pos.Filename, line: pos.Line, kind: "empty-stage-outcomes-waiver"})
			}
			return true
		})
		return nil
	})
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].file != findings[j].file {
			return findings[i].file < findings[j].file
		}
		return findings[i].line < findings[j].line
	})
	return findings, sites, err
}

// stageOutcomesWaiver returns (reason, true) when the waiver sits on line (a
// trailing comment) or in the contiguous comment block directly above it.
func stageOutcomesWaiver(commentByLine map[int]string, line int) (string, bool) {
	for ln := line; ; ln-- {
		txt, ok := commentByLine[ln]
		if !ok {
			if ln == line {
				continue
			}
			return "", false
		}
		if i := strings.Index(txt, stageOutcomesWaiverPrefix); i >= 0 {
			return strings.TrimSpace(txt[i+len(stageOutcomesWaiverPrefix):]), true
		}
	}
}

func reportStageOutcomesFindings(findings []entryFinding) {
	fmt.Fprintf(os.Stderr, "\nERROR: %d dispatcher ResolvedEntry Put site(s) neither set %s nor carry a //%s annotation.\n",
		len(findings), stageOutcomesField, stageOutcomesWaiverPrefix)
	fmt.Fprintf(os.Stderr, "       A cached RESTAction answer carries the header of the resolve that stored it (#904):\n")
	fmt.Fprintf(os.Stderr, "       a Put site that drops the outcomes answers [] (\"all succeeded\") for a failed resolve.\n\n")
	for _, f := range findings {
		switch f.kind {
		case "empty-stage-outcomes-waiver":
			fmt.Fprintf(os.Stderr, "  %s:%d  ResolvedEntry{...} has a //%s annotation with NO reason\n", f.file, f.line, stageOutcomesWaiverPrefix)
		default:
			fmt.Fprintf(os.Stderr, "  %s:%d  ResolvedEntry{...} does not set %s and has no `//%s <why this cell never answers a RESTAction /call>`\n",
				f.file, f.line, stageOutcomesField, stageOutcomesWaiverPrefix)
		}
	}
}
