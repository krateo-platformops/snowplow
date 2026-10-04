// widget_content_put_callsites_450_test.go — #450 / #456 (reviewer-416
// condition at 7567c9f9): EVERY populateWidgetContentL1 call site must hand it
// a ctx built by withWidgetContentPutSinks in the same function, with no
// reassignment in between.
//
// WHY A CALL-SITE ARM. TestIssue450_WithWidgetContentPutSinks_InstallsEveryGateSink
// pins the helper against populate's gates, and the drain arms exercise
// iterateApiRefPages. Nothing exercised the page-1 site (phase1Walker.walk):
// removing its helper call left the whole package GREEN. That is the exact
// #450 bug class (a Put path whose ctx lacks the gate sinks), just on the other
// site. A real-resolve page-1 arm is not cheap: walk calls widgets.Resolve with
// no seam, and Resolve's tail crdschema.ValidateObjectStatus needs discovery
// plus a CRD GET, so a hermetic page-1 resolve errors and the walk skips the
// Put before any gate runs.
//
// THE RULE, checked over the package's non-test sources (every build tag; files
// are parsed directly):
//  1. populateWidgetContentL1 is only ever CALLED (never taken as a value), and
//     its ctx argument is a plain identifier;
//  2. in the enclosing function, the LAST assignment to that identifier before
//     the call (by source position) is `x = withWidgetContentPutSinks(...)`
//     (or `:=`), so any reassignment between the helper and the populate fails;
//  3. that assignment sits in a block that encloses the call, so a helper
//     install guarded by an `if` (or in a sibling branch) does not count.
// A new call site that does not follow the rule fails. The arm also asserts it
// found both current sites, so a broken scanner cannot pass vacuously.

package dispatchers

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

const (
	populateFn   = "populateWidgetContentL1"
	putSinksFn   = "withWidgetContentPutSinks"
	callSitesArm = "#450 C1 CALL-SITE"
)

type ctxAssign struct {
	pos    token.Pos
	rhs    ast.Expr
	blocks []*ast.BlockStmt // enclosing blocks, outermost first
}

// enclosingBlocks returns the BlockStmts on stack, outermost first.
func enclosingBlocks(stack []ast.Node) []*ast.BlockStmt {
	var out []*ast.BlockStmt
	for _, n := range stack {
		if b, ok := n.(*ast.BlockStmt); ok {
			out = append(out, b)
		}
	}
	return out
}

func isCallTo(e ast.Expr, name string) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	id, ok := call.Fun.(*ast.Ident)
	return ok && id.Name == name
}

func TestIssue450_EveryPopulateCallSiteUsesTheHelperCtx(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var sites []string

	for _, fn := range files {
		if strings.HasSuffix(fn, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, fn, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", fn, err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			funcName := fd.Name.Name
			if fd.Recv != nil && len(fd.Recv.List) > 0 {
				funcName = "(method) " + funcName
			}

			type popCall struct {
				call   *ast.CallExpr
				blocks []*ast.BlockStmt
			}
			var calls []popCall
			assigns := map[string][]ctxAssign{}
			calledIdents := map[*ast.Ident]bool{}

			var stack []ast.Node
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return true
				}
				switch x := n.(type) {
				case *ast.CallExpr:
					if id, ok := x.Fun.(*ast.Ident); ok && id.Name == populateFn {
						calledIdents[id] = true
						calls = append(calls, popCall{call: x, blocks: enclosingBlocks(stack)})
					}
				case *ast.AssignStmt:
					for i, lhs := range x.Lhs {
						id, ok := lhs.(*ast.Ident)
						if !ok {
							continue
						}
						var rhs ast.Expr
						if len(x.Rhs) == len(x.Lhs) {
							rhs = x.Rhs[i]
						} // multi-value form (a, b = f()): rhs stays nil → never the helper
						assigns[id.Name] = append(assigns[id.Name], ctxAssign{pos: x.Pos(), rhs: rhs, blocks: enclosingBlocks(stack)})
					}
				case *ast.ValueSpec:
					for i, id := range x.Names {
						var rhs ast.Expr
						if i < len(x.Values) {
							rhs = x.Values[i]
						}
						assigns[id.Name] = append(assigns[id.Name], ctxAssign{pos: x.Pos(), rhs: rhs, blocks: enclosingBlocks(stack)})
					}
				}
				stack = append(stack, n)
				return true
			})

			// Rule 1a: populate is never used as a value (it would escape this check).
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if id, ok := n.(*ast.Ident); ok && id.Name == populateFn && !calledIdents[id] {
					t.Errorf("%s: %s uses %s as a value at %s — every use must be a direct call so its ctx can be checked",
						callSitesArm, funcName, populateFn, fset.Position(id.Pos()))
				}
				return true
			})

			for _, pc := range calls {
				where := fset.Position(pc.call.Pos())
				site := filepath.Base(where.Filename) + ":" + fd.Name.Name
				sites = append(sites, site)
				if len(pc.call.Args) == 0 {
					t.Errorf("%s: %s calls %s with no arguments", callSitesArm, site, populateFn)
					continue
				}
				// Rule 1b: the ctx argument is a plain identifier.
				ctxID, ok := pc.call.Args[0].(*ast.Ident)
				if !ok {
					t.Errorf("%s: %s (%s) passes a non-identifier ctx to %s — build it with %s into a variable first",
						callSitesArm, site, where, populateFn, putSinksFn)
					continue
				}
				// Rule 2: the last assignment before the call is the helper.
				var last *ctxAssign
				for i := range assigns[ctxID.Name] {
					a := &assigns[ctxID.Name][i]
					if a.pos < pc.call.Pos() && (last == nil || a.pos > last.pos) {
						last = a
					}
				}
				if last == nil {
					t.Errorf("%s: %s (%s) passes %q to %s, but it is never assigned in %s (a parameter?) — the "+
						"Put gates would read whatever sinks the caller happened to install",
						callSitesArm, site, where, ctxID.Name, populateFn, fd.Name.Name)
					continue
				}
				if !isCallTo(last.rhs, putSinksFn) {
					t.Errorf("%s: %s (%s) — the last assignment to %q before %s (at %s) is not %s(...); the "+
						"widgetContent Put gates on this path would not see their sinks (the #450 bug class)",
						callSitesArm, site, where, ctxID.Name, populateFn, fset.Position(last.pos), putSinksFn)
					continue
				}
				// Rule 3: the helper assignment's block encloses the call.
				inner := last.blocks[len(last.blocks)-1]
				enclosing := false
				for _, b := range pc.blocks {
					if b == inner {
						enclosing = true
						break
					}
				}
				if !enclosing {
					t.Errorf("%s: %s (%s) — the %s install at %s is in a block that does not enclose the %s "+
						"call (e.g. under an if), so some paths reach the Put without it",
						callSitesArm, site, where, putSinksFn, fset.Position(last.pos), populateFn)
				}
			}
		}
	}

	sort.Strings(sites)
	t.Logf("%s sites found: %v", populateFn, sites)
	for _, want := range []string{"phase1_walk.go:walk", "phase1_walk_pagination.go:iterateApiRefPages"} {
		found := false
		for _, s := range sites {
			if s == want {
				found = true
			}
		}
		if !found {
			t.Errorf("PRECONDITION: expected a %s call site %s — the scanner (or the code) moved; update this arm",
				populateFn, want)
		}
	}
}
