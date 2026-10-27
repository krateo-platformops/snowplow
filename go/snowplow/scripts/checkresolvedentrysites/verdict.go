// verdict.go — site-class 4 (#912 re-review): a resolve output is written to
// L1 only through a write that takes the Put-gate verdict.
//
// THE DEFECT CLASS (#903 F1 → #915 → #912 N1/F-A/F-B): the rule "a resolve
// whose RESTAction declares a write is never stored" was wired into the Put
// sites by hand, one site at a time, and each site decided "store" by default
// for a decline reason it did not recognise. The fix makes the TYPE enforce it:
// the resolve-output writes (cache/put_gated.go: PutResolvedIfGen,
// PutResolvedThenRemark, PutResolvedEndingKnownStale,
// ReplaceResolvedIfGenRefresh, PutRAFullList(IfGen)) take a cache.PutVerdict and
// store nothing unless it allows the Put, and a PutVerdict can only be made by
// cache.PutGateVerdict. What is left to enforce is that the resolve-output
// packages never reach around them, and this gate does that:
//
//	in internal/handlers/dispatchers and internal/resolvers/widgets/apiref
//	(production files), no reference — a call or a method value — to a method
//	named Put, PutIfGen, PutThenRemark, PutEndingKnownStale, ReplaceIfGen or
//	ReplaceIfGenRefresh that takes a *cache.ResolvedEntry, on ANY receiver (the
//	store, an interface, a wrapper).
//
// Typed (go/packages), so an entry built without a literal (new(), a copied
// entry), a method value, or an ad-hoc interface over the store is caught as
// well as a direct call. There is no waiver: the GET-only content cells that
// legitimately write ungated live outside these packages.
package main

import (
	"fmt"
	"go/ast"
	"go/types"
	"os"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// verdictScope is the resolve-output code under the module root.
var verdictScope = []string{"./internal/handlers/dispatchers/...", "./internal/resolvers/widgets/apiref/..."}

// ungatedWrites are the store writes that take no verdict.
var ungatedWrites = map[string]bool{
	"Put": true, "PutIfGen": true, "PutThenRemark": true, "PutEndingKnownStale": true,
	"ReplaceIfGen": true, "ReplaceIfGenRefresh": true,
}

type verdictFinding struct {
	file   string
	line   int
	method string
}

// isUngatedEntryWrite reports a method named in ungatedWrites that takes a
// *cache.ResolvedEntry, whatever its receiver.
func isUngatedEntryWrite(fn *types.Func) bool {
	if fn == nil || !ungatedWrites[fn.Name()] {
		return false
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return false
	}
	for i := 0; i < sig.Params().Len(); i++ {
		if ptr, ok := sig.Params().At(i).Type().(*types.Pointer); ok {
			if named, ok := ptr.Elem().(*types.Named); ok && named.Obj().Name() == "ResolvedEntry" &&
				named.Obj().Pkg() != nil && strings.HasSuffix(named.Obj().Pkg().Path(), "/internal/cache") {
				return true
			}
		}
	}
	return false
}

// checkVerdictSites loads patterns (typed) and returns every ungated write of a
// ResolvedEntry, plus the number of production files scanned.
func checkVerdictSites(dir string, patterns []string) ([]verdictFinding, int, error) {
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
	var findings []verdictFinding
	files := 0
	for _, p := range pkgs {
		for _, f := range p.Syntax {
			if strings.HasSuffix(p.Fset.Position(f.Pos()).Filename, "_test.go") {
				continue
			}
			files++
			ast.Inspect(f, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				s, ok := p.TypesInfo.Selections[sel]
				if !ok {
					return true
				}
				if fn, _ := s.Obj().(*types.Func); isUngatedEntryWrite(fn) {
					pos := p.Fset.Position(sel.Pos())
					findings = append(findings, verdictFinding{file: pos.Filename, line: pos.Line, method: fn.Name()})
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
	return findings, files, nil
}

func reportVerdictFindings(findings []verdictFinding) {
	fmt.Fprintf(os.Stderr, "\nERROR: %d resolve-output write(s) bypass the Put-gate verdict.\n", len(findings))
	fmt.Fprintf(os.Stderr, "       A resolve output is stored only through a write that takes cache.PutVerdict and\n")
	fmt.Fprintf(os.Stderr, "       refuses unless it allows the Put (cache/put_gated.go), so a decline can never be\n")
	fmt.Fprintf(os.Stderr, "       stored by a site that mishandled its reason (#903 F1, #915, #912).\n\n")
	for _, f := range findings {
		fmt.Fprintf(os.Stderr, "  %s:%d  .%s(...) writes a ResolvedEntry without a verdict — use the PutResolved*/ReplaceResolved* write\n",
			f.file, f.line, f.method)
	}
}
