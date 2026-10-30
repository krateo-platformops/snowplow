package cache

// store_swap_k24_single_walker_test.go — #928 round 3: the single-walker
// invariant that H1d and H1e's equivalence rests on, pinned at the source.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// k24Ref is one production reference to a walker function.
type k24Ref struct {
	file, fn string // the file (module-relative) and the declared function holding it
	call     bool   // the reference is the callee of a call
	goStmt   bool   // ... and that call is a go statement
	loop     bool   // an enclosing for/range loop
	lit      bool   // an enclosing function literal
}

// TestStoreSwap_K24_TheForcedPassHasOneWalker: the forced pass runs on ONE
// goroutine for the process's life. judgePending's and supersede's flag
// clears (#928 delta H1d, H1e) would be equivalent outside l.mu only because
// the only other writer of the same log's flag is that same pass's later
// setPending; a second concurrent forced pass would void that argument. The
// clears are under l.mu regardless, so a break is not a defect by itself, but
// this arm says so first.
//
// The invariant, read from the production source (no _test.go files):
//   - forcedVerify is called from one place, runStoreVerificationWalker's own
//     loop, on its goroutine (no go statement, no function literal);
//   - runStoreVerificationWalker is started from one place, a go statement in
//     StartStoreVerification, outside any loop or function literal;
//   - StartStoreVerification is called from one place in the module, main in
//     main.go, outside any loop or function literal.
func TestStoreSwap_K24_TheForcedPassHasOneWalker(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..")) // the module root (go/snowplow)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{"forcedVerify": true, "runStoreVerificationWalker": true, "StartStoreVerification": true}
	refs := map[string][]k24Ref{}
	fset := token.NewFileSet()
	files := 0
	err = filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if de.IsDir() {
			switch de.Name() {
			case "testdata", "vendor", ".git":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		files++
		rel, _ := filepath.Rel(root, path)
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			var stack []ast.Node
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if n == nil {
					stack = stack[:len(stack)-1]
					return true
				}
				stack = append(stack, n)
				var name string
				switch x := n.(type) {
				case *ast.Ident:
					name = x.Name
				case *ast.SelectorExpr:
					name = x.Sel.Name
				default:
					return true
				}
				if !names[name] {
					return true
				}
				refs[name] = append(refs[name], k24Classify(rel, fd.Name.Name, stack))
				// A selector's Sel ident is the same reference: do not
				// descend into it. The pop for this node still runs.
				_, isSel := n.(*ast.SelectorExpr)
				if isSel {
					stack = stack[:len(stack)-1]
					return false
				}
				return true
			})
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 100 {
		t.Fatalf("the scan read %d files: it is not looking at the production tree", files)
	}
	one := func(name string) (k24Ref, bool) {
		got := refs[name]
		if len(got) != 1 {
			t.Errorf("K24 RED: %s has %d production references, want exactly 1: %+v", name, len(got), got)
			return k24Ref{}, false
		}
		return got[0], true
	}
	if r, ok := one("forcedVerify"); ok && (r.fn != "runStoreVerificationWalker" || !r.call || r.goStmt || r.lit) {
		t.Errorf("K24 RED: forcedVerify's one reference is %+v; want a plain call on runStoreVerificationWalker's goroutine", r)
	}
	if r, ok := one("runStoreVerificationWalker"); ok && (r.fn != "StartStoreVerification" || !r.call || !r.goStmt || r.loop || r.lit) {
		t.Errorf("K24 RED: runStoreVerificationWalker's one reference is %+v; want one go statement in StartStoreVerification, outside any loop or literal", r)
	}
	if r, ok := one("StartStoreVerification"); ok && (r.file != "main.go" || r.fn != "main" || !r.call || r.goStmt || r.loop || r.lit) {
		t.Errorf("K24 RED: StartStoreVerification's one reference is %+v; want one plain call in main.go's main, outside any loop or literal", r)
	}
}

// k24Classify reads the reference at the top of stack (a node whose parent,
// if a call, has it as Fun).
func k24Classify(file, fn string, stack []ast.Node) k24Ref {
	r := k24Ref{file: file, fn: fn}
	ref := stack[len(stack)-1]
	if len(stack) >= 2 {
		if c, ok := stack[len(stack)-2].(*ast.CallExpr); ok && c.Fun == ref {
			r.call = true
			if len(stack) >= 3 {
				_, r.goStmt = stack[len(stack)-3].(*ast.GoStmt)
			}
		}
	}
	for _, n := range stack[:len(stack)-1] {
		switch n.(type) {
		case *ast.ForStmt, *ast.RangeStmt:
			r.loop = true
		case *ast.FuncLit:
			r.lit = true
		}
	}
	return r
}
