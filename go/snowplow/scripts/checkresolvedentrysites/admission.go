// admission.go — site-class 6 (#1233): every write of the store is admitted by
// the one check, admitWrite.
//
// THE RULE (Diego, 2026-11-07): while the RBAC snapshot is incomplete, no writer
// stores a shared cache entry, and the rule sits at the single place that decides
// a Put, not at each writer. In the cache package that place is admitWrite, and
// every cell enters the store through putCoreLocked. So, in the package that
// declares ResolvedCacheStore:
//
//	RULE C1 — every function that calls putCoreLocked has, among its own top-level
//	statements before the one holding that call, an `if` whose condition is
//	!admitWrite(...) (alone or as an operand of ||) and whose body ends in a
//	return. A guard inside a branch, a closure or behind && does not count.
//
//	RULE C2 — no function but putCoreLocked inserts into the store's index (an
//	assignment to an element of a ResolvedCacheStore's `index` field) or pushes
//	onto its LRU order (a PushFront on its `order` field).
//
// So a new store write either goes through a function that admits, or it is
// flagged here: it cannot skip the rule by calling putCoreLocked directly, nor
// by inserting into the index by hand. There is no waiver.
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

// admissionScope is the code under the module root the gate scans.
var admissionScope = []string{"./internal/cache"}

const (
	coreInsert   = "putCoreLocked"
	admitCheck   = "admitWrite"
	indexField   = "index"
	orderField   = "order"
	pushFrontFun = "PushFront"
)

type admissionFinding struct {
	file string
	line int
	why  string
}

// storeFieldOf reports the field name when sel selects a field of p's
// ResolvedCacheStore.
func storeFieldOf(p *packages.Package, sel *ast.SelectorExpr) (string, bool) {
	s, ok := p.TypesInfo.Selections[sel]
	if !ok || s.Kind() != types.FieldVal {
		return "", false
	}
	t := s.Recv()
	if ptr, ok := t.(*types.Pointer); ok {
		t = ptr.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok || named.Obj().Pkg() != p.Types || named.Obj().Name() != storeType {
		return "", false
	}
	return sel.Sel.Name, true
}

// admitsBefore reports whether body has, as one of its OWN statements before
// the one that holds pos, an if whose condition cannot be true without having
// called admitWrite and found it false: !admitWrite(...) itself, or an || chain
// with it as an operand. The if's body must end in a return. A guard nested in
// another statement (a branch the write can skip, a closure whose return leaves
// only the closure) or one behind && does not count (#1243 review).
func admitsBefore(body *ast.BlockStmt, pos token.Pos) bool {
	for _, st := range body.List {
		if st.End() > pos {
			return false // reached the statement that holds the call
		}
		ifs, ok := st.(*ast.IfStmt)
		if !ok || ifs.Init != nil || len(ifs.Body.List) == 0 {
			continue
		}
		if _, ok := ifs.Body.List[len(ifs.Body.List)-1].(*ast.ReturnStmt); !ok {
			continue
		}
		if refusesUnlessAdmitted(ifs.Cond) {
			return true
		}
	}
	return false
}

// refusesUnlessAdmitted reports whether cond is !admitWrite(...) or an || chain
// that has it as an operand, so the if is taken whenever admitWrite is false.
func refusesUnlessAdmitted(cond ast.Expr) bool {
	switch e := cond.(type) {
	case *ast.ParenExpr:
		return refusesUnlessAdmitted(e.X)
	case *ast.BinaryExpr:
		return e.Op == token.LOR && (refusesUnlessAdmitted(e.X) || refusesUnlessAdmitted(e.Y))
	case *ast.UnaryExpr:
		if e.Op != token.NOT {
			return false
		}
		call, ok := e.X.(*ast.CallExpr)
		if !ok {
			return false
		}
		id, ok := call.Fun.(*ast.Ident)
		return ok && id.Name == admitCheck
	}
	return false
}

// checkAdmissionFile applies rules C1 and C2 to one file of the package that
// declares ResolvedCacheStore.
func checkAdmissionFile(p *packages.Package, f *ast.File) []admissionFinding {
	var out []admissionFinding
	at := func(pos token.Pos, why string) {
		ps := p.Fset.Position(pos)
		out = append(out, admissionFinding{file: ps.Filename, line: ps.Line, why: why})
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		isCore := fd.Name.Name == coreInsert
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.CallExpr:
				sel, ok := n.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if sel.Sel.Name == coreInsert && !admitsBefore(fd.Body, n.Pos()) {
					at(n.Pos(), fmt.Sprintf("%s calls %s without returning on !%s(...) first", fd.Name.Name, coreInsert, admitCheck))
				}
				if sel.Sel.Name == pushFrontFun && !isCore {
					if inner, ok := sel.X.(*ast.SelectorExpr); ok {
						if field, ok := storeFieldOf(p, inner); ok && field == orderField {
							at(n.Pos(), fmt.Sprintf("%s pushes onto the store's LRU order; only %s inserts a cell", fd.Name.Name, coreInsert))
						}
					}
				}
			case *ast.AssignStmt:
				if isCore {
					return true
				}
				for _, lhs := range n.Lhs {
					ix, ok := lhs.(*ast.IndexExpr)
					if !ok {
						continue
					}
					sel, ok := ix.X.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					if field, ok := storeFieldOf(p, sel); ok && field == indexField {
						at(n.Pos(), fmt.Sprintf("%s inserts into the store's index; only %s inserts a cell", fd.Name.Name, coreInsert))
					}
				}
			}
			return true
		})
	}
	return out
}

// checkAdmissionSites loads patterns (typed) and returns every rule C finding,
// plus the number of production files scanned in the store package.
func checkAdmissionSites(dir string, patterns []string) ([]admissionFinding, int, error) {
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
	var findings []admissionFinding
	files := 0
	for _, p := range pkgs {
		if p.Types == nil || p.Types.Scope().Lookup(storeType) == nil {
			continue
		}
		for _, f := range p.Syntax {
			if strings.HasSuffix(p.Fset.Position(f.Pos()).Filename, "_test.go") {
				continue
			}
			files++
			findings = append(findings, checkAdmissionFile(p, f)...)
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

func reportAdmissionFindings(findings []admissionFinding) {
	fmt.Fprintf(os.Stderr, "\nERROR: %d store write(s) are not admitted by %s.\n", len(findings), admitCheck)
	fmt.Fprintf(os.Stderr, "       While the RBAC snapshot is incomplete no writer stores a shared cache entry\n")
	fmt.Fprintf(os.Stderr, "       (#1233). The rule is the store's: every function that inserts a cell through\n")
	fmt.Fprintf(os.Stderr, "       %s returns on !%s(...) first, and nothing else inserts one.\n\n", coreInsert, admitCheck)
	for _, f := range findings {
		fmt.Fprintf(os.Stderr, "  %s:%d  %s\n", f.file, f.line, f.why)
	}
}
