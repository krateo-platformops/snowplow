package api

// rev1032_guard_test.go — #1025 / #1032 review: the STRUCTURAL guard that
// keeps the one rule in front of every Secret lookup.
//
// The #1025 rule (refusePerUserRef) runs once per stage, in
// refuseStageEndpointRef, which runStage calls for EVERY stage before it plans
// the stage and before it branches on userAccessFilter (#1477 review B1: the
// rule is decided whatever the data); resolveOne itself does not re-check. That
// is only safe while:
//   - resolveOne has no caller other than resolveStageEndpoint (a new direct
//     caller would look up a ref the rule never saw);
//   - resolveStageEndpoint has no production caller other than runStage, and
//     takes its ref only from r.stageRef (never renders one itself);
//   - in runStage, `if r.refuseStageEndpointRef(...) {` is an unconditional
//     top-level statement that precedes every resolveStageEndpoint reference;
//   - in refuseStageEndpointRef, refusePerUserRef is an unconditional
//     top-level `if err := ...; err != nil` statement, and the only writer of
//     r.stageRef in the package.
// This test parses the package's non-test files and fails on any of them.
// Same allow-list idea as #443's gatedAtCaller guard (inline443_enum_test.go).
// Adopted from the #1032 reviewer's sketch.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

type rev1032Func struct {
	file string
	decl *ast.FuncDecl
}

// rev1032Funcs parses the package's non-test files and returns every function
// declaration by name (methods by method name).
func rev1032Funcs(t *testing.T) (*token.FileSet, map[string][]rev1032Func) {
	t.Helper()
	fset := token.NewFileSet()
	ents, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string][]rev1032Func{}
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
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Body != nil {
				out[fd.Name.Name] = append(out[fd.Name.Name], rev1032Func{file: n, decl: fd})
			}
		}
	}
	return fset, out
}

// selectorRefs returns the positions of every `x.<name>` in body.
func selectorRefs(body ast.Node, name string) []token.Pos {
	var out []token.Pos
	ast.Inspect(body, func(x ast.Node) bool {
		if sel, ok := x.(*ast.SelectorExpr); ok && sel.Sel.Name == name {
			out = append(out, sel.Pos())
		}
		return true
	})
	return out
}

// ifEndsReturningTrue reports whether the top-level `if` of body holding pos
// has a body whose last statement is `return true` (the refusal stops the
// stage: a gate that falls through refuses nothing).
func ifEndsReturningTrue(body *ast.BlockStmt, pos token.Pos) bool {
	for _, st := range body.List {
		is, ok := st.(*ast.IfStmt)
		if !ok || pos < st.Pos() || pos >= st.End() || len(is.Body.List) == 0 {
			continue
		}
		ret, ok := is.Body.List[len(is.Body.List)-1].(*ast.ReturnStmt)
		if !ok || len(ret.Results) != 1 {
			return false
		}
		id, ok := ret.Results[0].(*ast.Ident)
		return ok && id.Name == "true"
	}
	return false
}

// topLevelIfHolds reports whether pos lies in the init or the condition of a
// top-level `if` statement of body.
func topLevelIfHolds(body *ast.BlockStmt, pos token.Pos) bool {
	for _, st := range body.List {
		is, ok := st.(*ast.IfStmt)
		if !ok || pos < st.Pos() || pos >= st.End() {
			continue
		}
		if is.Init != nil && is.Init.Pos() <= pos && pos < is.Init.End() {
			return true
		}
		if is.Cond != nil && is.Cond.Pos() <= pos && pos < is.Cond.End() {
			return true
		}
	}
	return false
}

func TestRev1032_ResolveOneOnlyBehindRefusePerUserRef(t *testing.T) {
	fset, funcs := rev1032Funcs(t)
	sawResolveOne, sawStageEndpoint := 0, 0
	var resolveStageEndpointCallers []string
	for name, fs := range funcs {
		for _, fn := range fs {
			if refs := selectorRefs(fn.decl.Body, "resolveOne"); len(refs) > 0 {
				sawResolveOne += len(refs)
				if name != "resolveStageEndpoint" {
					t.Errorf("%s: %s references resolveOne; only resolveStageEndpoint may (runStage applies the #1025 rule before calling it)",
						fset.Position(refs[0]), name)
				}
			}
			if refs := selectorRefs(fn.decl.Body, "resolveStageEndpoint"); len(refs) > 0 {
				sawStageEndpoint += len(refs)
				resolveStageEndpointCallers = append(resolveStageEndpointCallers, name)
			}
			if name != "refuseStageEndpointRef" {
				ast.Inspect(fn.decl.Body, func(x ast.Node) bool {
					as, ok := x.(*ast.AssignStmt)
					if !ok {
						return true
					}
					for _, l := range as.Lhs {
						if sel, ok := l.(*ast.SelectorExpr); ok && sel.Sel.Name == "stageRef" {
							t.Errorf("%s: %s writes r.stageRef; only refuseStageEndpointRef may (it applies the #1025 rule to what it writes)",
								fset.Position(sel.Pos()), name)
						}
					}
					return true
				})
			}
		}
	}
	if sawResolveOne == 0 || sawStageEndpoint == 0 {
		t.Fatal("found no resolveOne / resolveStageEndpoint reference: the guard is not looking at the right package")
	}
	for _, c := range resolveStageEndpointCallers {
		if c != "runStage" {
			t.Errorf("%s references resolveStageEndpoint; only runStage may (it runs refuseStageEndpointRef first)", c)
		}
	}

	// resolveStageEndpoint takes its ref from r.stageRef and renders none.
	for _, fn := range funcs["resolveStageEndpoint"] {
		if refs := selectorRefs(fn.decl.Body, "evalEndpointRef"); len(refs) > 0 {
			t.Errorf("%s: resolveStageEndpoint renders an endpointRef itself; it must use r.stageRef, which the #1025 rule saw",
				fset.Position(refs[0]))
		}
		if len(selectorRefs(fn.decl.Body, "stageRef")) == 0 {
			t.Error("resolveStageEndpoint does not read r.stageRef")
		}
	}

	// refuseStageEndpointRef: the rule is an unconditional top-level
	// `if err := ...; err != nil`.
	refusers := funcs["refuseStageEndpointRef"]
	if len(refusers) != 1 {
		t.Fatalf("want exactly one refuseStageEndpointRef, got %d", len(refusers))
	}
	rf := refusers[0].decl
	rule := selectorRefs(rf.Body, "refusePerUserRef")
	if len(rule) == 0 {
		t.Fatal("refuseStageEndpointRef: no refusePerUserRef call")
	}
	inInit := false
	for _, st := range rf.Body.List {
		if is, ok := st.(*ast.IfStmt); ok && is.Init != nil && is.Init.Pos() <= rule[0] && rule[0] < is.Init.End() {
			inInit = true
		}
	}
	if !inInit {
		t.Error("refuseStageEndpointRef: refusePerUserRef is not an unconditional top-level `if err := ...; err != nil` statement")
	}
	if !ifEndsReturningTrue(rf.Body, rule[0]) {
		t.Error("refuseStageEndpointRef: the refusal's `if` body does not end in `return true`")
	}

	// runStage: `if r.refuseStageEndpointRef(...) {` is top-level and precedes
	// every resolveStageEndpoint reference (and the plan).
	for _, fn := range funcs["runStage"] {
		gate := selectorRefs(fn.decl.Body, "refuseStageEndpointRef")
		if len(gate) == 0 {
			t.Fatal("runStage: no refuseStageEndpointRef call")
		}
		if !topLevelIfHolds(fn.decl.Body, gate[0]) {
			t.Error("runStage: refuseStageEndpointRef is not an unconditional top-level `if` statement")
		}
		if !ifEndsReturningTrue(fn.decl.Body, gate[0]) {
			t.Error("runStage: the refuseStageEndpointRef gate's body does not end in `return true` (the stage must stop)")
		}
		for _, name := range []string{"resolveStageEndpoint", "planStage"} {
			for _, p := range selectorRefs(fn.decl.Body, name) {
				if p < gate[0] {
					t.Errorf("%s: runStage references %s before refuseStageEndpointRef", fset.Position(p), name)
				}
			}
		}
	}
}
