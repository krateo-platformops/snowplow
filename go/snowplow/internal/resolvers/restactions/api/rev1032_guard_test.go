package api

// rev1032_guard_test.go — #1025 / #1032 review: the STRUCTURAL guard that
// keeps the one rule in front of every Secret lookup.
//
// The #1025 rule (refusePerUserRef) runs once, at the top of
// resolveStageEndpoint, before the stage branches on userAccessFilter;
// resolveOne itself does not re-check. That is only safe while:
//   - resolveOne has no caller other than resolveStageEndpoint (a new direct
//     caller would look up a ref the rule never saw), and
//   - in resolveStageEndpoint, refusePerUserRef is an unconditional
//     top-level `if err := ...; err != nil` statement that precedes every
//     resolveOne reference (moving it below the UAF branches would put the
//     caller-supplied UAF branch's resolveOne in front of it).
// This test parses the package's non-test files and fails on either. Same
// allow-list idea as #443's gatedAtCaller guard (inline443_enum_test.go).
// Adopted from the #1032 reviewer's sketch.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

func TestRev1032_ResolveOneOnlyBehindRefusePerUserRef(t *testing.T) {
	fset := token.NewFileSet()
	ents, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"resolveStageEndpoint": true}
	sawResolveOne := 0
	for _, e := range ents {
		n := e.Name()
		if !strings.HasSuffix(n, ".go") || strings.HasSuffix(n, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, n, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			var refusePos []token.Pos
			var oneCalls []token.Pos
			ast.Inspect(fd.Body, func(x ast.Node) bool {
				sel, ok := x.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "resolveOne":
					oneCalls = append(oneCalls, sel.Pos())
				case "refusePerUserRef":
					refusePos = append(refusePos, sel.Pos())
				}
				return true
			})
			if len(oneCalls) == 0 {
				continue
			}
			sawResolveOne += len(oneCalls)
			if !allowed[fd.Name.Name] {
				t.Errorf("%s: %s references resolveOne; only resolveStageEndpoint may (it applies the #1025 rule first)",
					fset.Position(oneCalls[0]), fd.Name.Name)
				continue
			}
			if len(refusePos) == 0 {
				t.Errorf("%s: no refusePerUserRef call", fd.Name.Name)
				continue
			}
			// The refusal must be a TOP-LEVEL statement of the body (unconditional)
			// and precede every resolveOne reference.
			top := false
			for _, st := range fd.Body.List {
				if st.Pos() <= refusePos[0] && refusePos[0] < st.End() {
					if is, ok := st.(*ast.IfStmt); ok && is.Init != nil && is.Init.Pos() <= refusePos[0] && refusePos[0] < is.Init.End() {
						top = true
					}
				}
			}
			if !top {
				t.Errorf("%s: refusePerUserRef is not an unconditional top-level `if err := ...; err != nil` statement", fd.Name.Name)
			}
			for _, p := range oneCalls {
				if p < refusePos[0] {
					t.Errorf("%s: resolveOne referenced before refusePerUserRef", fset.Position(p))
				}
			}
		}
	}
	if sawResolveOne == 0 {
		t.Fatal("found no resolveOne reference: the guard is not looking at the right package")
	}
}
