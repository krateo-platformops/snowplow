// verdict.go — site-class 4 (#912 re-review, delta-3 item 3): a resolve output
// is written to L1 only through a write that takes the Put-gate verdict.
//
// THE DEFECT CLASS (#903 F1 → #915 → #912 N1/F-A/F-B): the rule "a resolve
// whose RESTAction declares a write is never stored" was wired into the Put
// sites by hand, one site at a time, and each site decided "store" by default
// for a decline reason it did not recognise. The fix makes the TYPE and the
// COMPILER enforce it:
//   - every exported ResolvedEntry write of the cache package
//     (cache/put_gated.go, PutRAFullList(IfGen)) takes a cache.PutVerdict and
//     stores nothing unless it allows the Put, and a PutVerdict can only be
//     made by cache.PutGateVerdict;
//   - the ungated store methods are UNEXPORTED, so no other package can reach
//     one; PutContentIfGen, the one exported write without a verdict, writes
//     only a GET-only content cell (an apistage entry under its own key).
//
// What is left for this gate:
//
//	RULE A — outside the cache package, in the resolve-output packages
//	(internal/handlers/dispatchers, internal/resolvers/widgets/apiref): no
//	reference — a call or a method value — to a method named Put, PutIfGen,
//	PutThenRemark, PutEndingKnownStale, ReplaceIfGen or ReplaceIfGenRefresh that
//	takes a *cache.ResolvedEntry, on ANY receiver (an interface, a wrapper).
//
//	RULE B — inside the package that declares ResolvedCacheStore: an exported
//	helper cannot re-open the escape the compiler closed. Every call to an
//	ungated store primitive (put, putPlain, putIfGen, putThenRemark,
//	putEndingKnownStale, replaceIfGen, replaceIfGenRefresh) outside the
//	primitives themselves sits in a function that takes a PutVerdict and whose
//	FIRST statement returns when that verdict does not allow the Put
//	(`if … !v.Store() … { return … }`), or in PutContentIfGen. And no method on
//	ResolvedCacheStore carries an exported ungated name.
//
// Typed (go/packages), so an entry built without a literal (new(), a copied
// entry), a method value, or an ad-hoc interface over the store is caught as
// well as a direct call. There is no waiver.
package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"sort"
	"strings"

	"golang.org/x/tools/go/packages"
)

// verdictScope is the code under the module root the gate scans: the
// resolve-output packages (rule A) and the cache package (rule B).
var verdictScope = []string{"./internal/handlers/dispatchers/...", "./internal/resolvers/widgets/apiref/...", "./internal/cache"}

// ungatedWrites are the exported names of the store writes that take no
// verdict (rule A; rule B forbids declaring one on the store).
var ungatedWrites = map[string]bool{
	"Put": true, "PutIfGen": true, "PutThenRemark": true, "PutEndingKnownStale": true,
	"ReplaceIfGen": true, "ReplaceIfGenRefresh": true,
}

// ungatedPrimitives are the cache package's unexported ungated store writes
// (rule B).
var ungatedPrimitives = map[string]bool{
	"put": true, "putPlain": true, "putIfGen": true, "putThenRemark": true,
	"putEndingKnownStale": true, "replaceIfGen": true, "replaceIfGenRefresh": true,
}

// contentWrite is the one exported write without a verdict (rule B exempts it
// by name; TestIssue912_V3 pins that it writes nothing but a content cell).
const contentWrite = "PutContentIfGen"

const storeType = "ResolvedCacheStore"

type verdictFinding struct {
	file   string
	line   int
	method string
	why    string // "" — rule A; otherwise rule B's reason
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

// storeMethodOf reports the method name when fn is a method of p's
// ResolvedCacheStore.
func storeMethodOf(p *types.Package, fn *types.Func) (string, bool) {
	if fn == nil || fn.Pkg() != p {
		return "", false
	}
	sig, ok := fn.Type().(*types.Signature)
	if !ok || sig.Recv() == nil {
		return "", false
	}
	t := sig.Recv().Type()
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	if named, ok := t.(*types.Named); ok && named.Obj().Name() == storeType {
		return fn.Name(), true
	}
	return "", false
}

// verdictParam returns the name of fd's parameter of p's PutVerdict type.
func verdictParam(info *types.Info, p *types.Package, fd *ast.FuncDecl) string {
	for _, field := range fd.Type.Params.List {
		named, ok := info.TypeOf(field.Type).(*types.Named)
		if !ok || named.Obj().Pkg() != p || named.Obj().Name() != "PutVerdict" {
			continue
		}
		for _, n := range field.Names {
			return n.Name
		}
	}
	return ""
}

// guardsOnStore reports whether fd's first statement is an if whose condition
// contains !<v>.Store() and whose body ends in a return.
func guardsOnStore(fd *ast.FuncDecl, v string) bool {
	if fd.Body == nil || len(fd.Body.List) == 0 {
		return false
	}
	ifs, ok := fd.Body.List[0].(*ast.IfStmt)
	if !ok || ifs.Init != nil || len(ifs.Body.List) == 0 {
		return false
	}
	if _, ok := ifs.Body.List[len(ifs.Body.List)-1].(*ast.ReturnStmt); !ok {
		return false
	}
	found := false
	ast.Inspect(ifs.Cond, func(n ast.Node) bool {
		u, ok := n.(*ast.UnaryExpr)
		if !ok || u.Op != token.NOT {
			return true
		}
		call, ok := u.X.(*ast.CallExpr)
		if !ok || len(call.Args) != 0 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Store" {
			return true
		}
		if id, ok := sel.X.(*ast.Ident); ok && id.Name == v {
			found = true
		}
		return true
	})
	return found
}

// checkStorePackage applies rule B to one file of the package that declares
// ResolvedCacheStore.
func checkStorePackage(p *packages.Package, f *ast.File) []verdictFinding {
	var out []verdictFinding
	at := func(pos token.Pos, method, why string) {
		ps := p.Fset.Position(pos)
		out = append(out, verdictFinding{file: ps.Filename, line: ps.Line, method: method, why: why})
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		fn, _ := p.TypesInfo.Defs[fd.Name].(*types.Func)
		name, isStoreMethod := storeMethodOf(p.Types, fn)
		if isStoreMethod && ungatedWrites[name] {
			at(fd.Name.Pos(), name, "declared on the store under an exported name (the ungated writes stay unexported)")
			continue
		}
		if isStoreMethod && ungatedPrimitives[name] {
			continue // the primitives themselves
		}
		v := verdictParam(p.TypesInfo, p.Types, fd)
		guarded := v != "" && guardsOnStore(fd, v)
		exempt := isStoreMethod && name == contentWrite
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			s, ok := p.TypesInfo.Selections[sel]
			if !ok {
				return true
			}
			callee, _ := s.Obj().(*types.Func)
			m, ok := storeMethodOf(p.Types, callee)
			if !ok || !ungatedPrimitives[m] || guarded || exempt {
				return true
			}
			why := fmt.Sprintf("in %s, which takes no PutVerdict", fd.Name.Name)
			if v != "" {
				why = fmt.Sprintf("in %s, whose first statement does not return on !%s.Store()", fd.Name.Name, v)
			}
			at(sel.Pos(), m, why)
			return true
		})
	}
	return out
}

// checkVerdictSites loads patterns (typed) and returns every rule A and rule B
// finding, plus the number of production files scanned.
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
		declaresStore := p.Types != nil && p.Types.Scope().Lookup(storeType) != nil
		for _, f := range p.Syntax {
			if strings.HasSuffix(p.Fset.Position(f.Pos()).Filename, "_test.go") {
				continue
			}
			files++
			if declaresStore {
				findings = append(findings, checkStorePackage(p, f)...)
				continue
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
		if f.why == "" {
			fmt.Fprintf(os.Stderr, "  %s:%d  .%s(...) writes a ResolvedEntry without a verdict — use the PutResolved*/ReplaceResolved* write\n",
				f.file, f.line, f.method)
			continue
		}
		fmt.Fprintf(os.Stderr, "  %s:%d  .%s(...) is an ungated store write %s — take the verdict and return on !v.Store() first\n",
			f.file, f.line, f.method, f.why)
	}
}
