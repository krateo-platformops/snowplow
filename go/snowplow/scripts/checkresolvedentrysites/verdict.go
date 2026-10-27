// verdict.go — site-class 4 (#912 re-review): every ResolvedEntry Put site
// decides through THE shared Put-gate verdict (cache.PutGateVerdict).
//
// THE DEFECT CLASS (#903 F1 → #915 → #912 N1): the rule "a resolve whose
// RESTAction declares a write is never stored" was wired into the Put sites by
// hand, one site at a time; each new path that stored a resolve (the opt-in
// external-TTL Put, the apiref memo) was found missing it after the fact. With
// one verdict that combines every sink-derived signal, the remaining risk is a
// NEW Put site that never reads it. This gate makes that a hard failure:
//
//   - every function that builds a `ResolvedEntry{...}` literal must call
//     PutGateVerdict (bare or package-qualified) somewhere in its body, OR
//   - the literal carries `//put-verdict: <reason>` on its line or the line
//     above, naming where its verdict is taken (the seed's
//     declineSeedPutOnError, the raFullList gates in apiref) or why the cell is
//     not a resolve output (a GET-only content cell). The reason is mandatory.
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

// verdictFunc is the shared Put-gate verdict every Put site must read.
const verdictFunc = "PutGateVerdict"

// verdictWaiverPrefix exempts a literal whose verdict is taken elsewhere.
const verdictWaiverPrefix = "put-verdict:"

type verdictFinding struct {
	file string
	line int
	kind string // "missing-verdict-or-waiver" | "empty-verdict-waiver-reason"
}

// checkVerdictTree walks root (skipping .claude, vendor, testdata and _test.go
// files) and returns the verdict findings and the number of Put sites seen.
func checkVerdictTree(root string) ([]verdictFinding, int, error) {
	fset := token.NewFileSet()
	var findings []verdictFinding
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
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, n, perr := checkVerdictFile(fset, path)
		if perr != nil {
			return perr
		}
		findings = append(findings, f...)
		sites += n
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

// checkVerdictFile checks one file: per function declaration, whether it calls
// the verdict, and every ResolvedEntry literal inside it.
func checkVerdictFile(fset *token.FileSet, path string) ([]verdictFinding, int, error) {
	src, err := parser.ParseFile(fset, path, nil, parser.ParseComments|parser.SkipObjectResolution) //nosec G304,G703 -- dev-time AST lint tool; path is a filepath.WalkDir result
	if err != nil {
		return nil, 0, fmt.Errorf("parse %s: %w", path, err)
	}
	commentByLine := map[int]string{}
	for _, cg := range src.Comments {
		for _, c := range cg.List {
			commentByLine[fset.Position(c.Pos()).Line] = strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
		}
	}
	var findings []verdictFinding
	sites := 0
	for _, decl := range src.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		readsVerdict := false
		var lits []*ast.CompositeLit
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CallExpr:
				if calleeNameIs(node.Fun, verdictFunc) {
					readsVerdict = true
				}
			case *ast.CompositeLit:
				if isResolvedEntry(node.Type) {
					lits = append(lits, node)
				}
			}
			return true
		})
		for _, lit := range lits {
			sites++
			if readsVerdict {
				continue
			}
			pos := fset.Position(lit.Pos())
			reason, waived := prefixedReason(commentByLine, pos.Line, verdictWaiverPrefix)
			switch {
			case !waived:
				findings = append(findings, verdictFinding{file: pos.Filename, line: pos.Line, kind: "missing-verdict-or-waiver"})
			case reason == "":
				findings = append(findings, verdictFinding{file: pos.Filename, line: pos.Line, kind: "empty-verdict-waiver-reason"})
			}
		}
	}
	return findings, sites, nil
}

// prefixedReason returns (reason, true) when an annotation with prefix sits on
// line (a trailing comment) or anywhere in the contiguous comment block
// directly above it (Put sites already carry other waivers there).
func prefixedReason(commentByLine map[int]string, line int, prefix string) (string, bool) {
	for ln := line; ; ln-- {
		txt, ok := commentByLine[ln]
		if !ok {
			if ln == line {
				continue // no trailing comment: look at the block above
			}
			return "", false
		}
		if i := strings.Index(txt, prefix); i >= 0 {
			return strings.TrimSpace(txt[i+len(prefix):]), true
		}
	}
}

func reportVerdictFindings(findings []verdictFinding) {
	fmt.Fprintf(os.Stderr, "ERROR: %d ResolvedEntry Put site(s) do not read the shared Put-gate verdict (cache.%s).\n",
		len(findings), verdictFunc)
	fmt.Fprintf(os.Stderr, "       Every Put site decides through ONE verdict, so a rule such as \"a resolve that\n")
	fmt.Fprintf(os.Stderr, "       declares a write is never stored\" (#903 F1) holds on every path (#915, #912 N1).\n\n")
	for _, f := range findings {
		switch f.kind {
		case "empty-verdict-waiver-reason":
			fmt.Fprintf(os.Stderr, "  %s:%d  ResolvedEntry{...} has a //%s annotation with NO reason\n", f.file, f.line, verdictWaiverPrefix)
		default:
			fmt.Fprintf(os.Stderr, "  %s:%d  ResolvedEntry{...} in a function that never calls %s, and no `//%s <where the verdict is taken>`\n",
				f.file, f.line, verdictFunc, verdictWaiverPrefix)
		}
	}
}
