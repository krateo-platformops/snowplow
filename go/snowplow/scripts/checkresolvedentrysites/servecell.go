package main

// servecell.go — site-class 3 (#643 PR-B): every L1 BODY read on the resolve
// path is routed through cache.ServeCell.
//
// THE DEFECT CLASS. A declined-external mark folds in the cached L1 cells its
// resolve served its inputs from (cache/served_cell.go): when such a cell is
// re-Put by the refresher, the mark retires and the now-cacheable cell is
// warmed. A body read that bypasses ServeCell is not noted, so a mark taken from
// it never learns its input was refreshed and holds a cacheable cell cold —
// SILENTLY: nothing reads red (the #533 "a missing edge reads as green" lesson).
//
// THE RULE (typed, go/packages — not a name heuristic). Under the scanned
// resolve-path packages, every reference to (*cache.ResolvedCacheStore).Get or
// (*cache.ResolvedCacheStore).GetNoTouch — a call or a method value — must
// EITHER not exist (route the read through cache.ServeCell) OR carry an inline
// `//serve-probe: <reason>` annotation on its line or the line above, naming
// why the read serves no body (a warmth probe, a populate worker's re-check).
// A bare `//serve-probe:` with no reason is itself flagged.

import (
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

const serveProbePrefix = "serve-probe:"

// serveCellScope is the resolve path under the module root: the RESTAction and
// widget resolvers (restactions.Resolve, apiref.Resolve and everything they
// reach). The dispatchers nested-call handler (ResolveNestedCall) reads no L1
// body — it calls objects.Get and the resolvers above — so it is not in scope.
var serveCellScope = []string{"./internal/resolvers/..."}

type serveFinding struct {
	file   string
	line   int
	method string
	kind   string // "unrouted" | "empty-reason"
}

func isStoreRead(fn *types.Func) bool {
	if fn == nil || (fn.Name() != "Get" && fn.Name() != "GetNoTouch") {
		return false
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return false
	}
	ptr, ok := sig.Recv().Type().(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := ptr.Elem().(*types.Named)
	if !ok || named.Obj().Name() != "ResolvedCacheStore" || named.Obj().Pkg() == nil {
		return false
	}
	return strings.HasSuffix(named.Obj().Pkg().Path(), "/internal/cache")
}

// checkServeCellSites loads patterns (typed) and returns every unrouted store
// read, plus the count of waived probe sites.
func checkServeCellSites(dir string, patterns []string) ([]serveFinding, int, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
			packages.NeedTypes | packages.NeedTypesInfo,
		Dir: dir,
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		return nil, 0, err
	}
	var loadErrs []string
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		for _, e := range p.Errors {
			loadErrs = append(loadErrs, e.Error())
		}
	})
	if len(loadErrs) > 0 {
		return nil, 0, fmt.Errorf("load: %s", strings.Join(loadErrs, "; "))
	}

	var findings []serveFinding
	probes := 0
	for _, p := range pkgs {
		for _, f := range p.Syntax {
			fname := p.Fset.Position(f.Pos()).Filename
			if strings.HasSuffix(fname, "_test.go") {
				continue
			}
			commentByLine := map[int]string{}
			for _, cg := range f.Comments {
				for _, c := range cg.List {
					commentByLine[p.Fset.Position(c.Pos()).Line] = strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
				}
			}
			ast.Inspect(f, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				s, ok := p.TypesInfo.Selections[sel]
				if !ok {
					return true
				}
				fn, _ := s.Obj().(*types.Func)
				if !isStoreRead(fn) {
					return true
				}
				pos := p.Fset.Position(sel.Pos())
				reason, waived := "", false
				for _, ln := range []int{pos.Line, pos.Line - 1} {
					if txt, ok := commentByLine[ln]; ok {
						if i := strings.Index(txt, serveProbePrefix); i >= 0 {
							reason, waived = strings.TrimSpace(txt[i+len(serveProbePrefix):]), true
							break
						}
					}
				}
				switch {
				case !waived:
					findings = append(findings, serveFinding{file: pos.Filename, line: pos.Line, method: fn.Name(), kind: "unrouted"})
				case reason == "":
					findings = append(findings, serveFinding{file: pos.Filename, line: pos.Line, method: fn.Name(), kind: "empty-reason"})
				default:
					probes++
				}
				return true
			})
		}
	}
	sort.Slice(findings, func(i, j int) bool {
		if findings[i].file != findings[j].file {
			return findings[i].file < findings[j].file
		}
		return findings[i].line < findings[j].line
	})
	return findings, probes, nil
}

func reportServeFindings(findings []serveFinding) {
	fmt.Fprintf(os.Stderr, "\nERROR: %d L1 store read(s) on the resolve path bypass cache.ServeCell.\n", len(findings))
	fmt.Fprintf(os.Stderr, "       A declined-external mark (#643) folds in the cells its resolve SERVED; an\n")
	fmt.Fprintf(os.Stderr, "       unrouted read is not noted, so a mark taken from it never retires when the\n")
	fmt.Fprintf(os.Stderr, "       cell is refreshed and holds a cacheable cell cold — silently.\n\n")
	for _, f := range findings {
		switch f.kind {
		case "empty-reason":
			fmt.Fprintf(os.Stderr, "  %s:%d  (*ResolvedCacheStore).%s has a //%s waiver with NO reason\n", f.file, f.line, f.method, serveProbePrefix)
		default:
			fmt.Fprintf(os.Stderr, "  %s:%d  (*ResolvedCacheStore).%s is not routed through cache.ServeCell and has no //%s <reason>\n",
				f.file, f.line, f.method, serveProbePrefix)
		}
	}
	fmt.Fprintln(os.Stderr, "\n       Read the body through cache.ServeCell(ctx, store, key, touch, covers...), or, for a")
	fmt.Fprintf(os.Stderr, "       read that serves no body, annotate it: //%s <why no body is served>\n", serveProbePrefix)
}
