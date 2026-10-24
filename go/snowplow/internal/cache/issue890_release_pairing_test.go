//go:build unit || integration

// issue890_release_pairing_test.go — #890/#893 structural guard: every production
// function that opens an L1 resolve scope (cache.WithL1KeyContext /
// WithL1KeyContextFromEpoch) also releases it (Deps().ReleaseUnstored) in the
// same function. A new resolve entry that forgets the release would bring back
// the leak for its declines and errors; this fails until its author adds it.

package cache

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

func TestIssue890_EveryL1ResolveScopeIsReleased(t *testing.T) {
	root := ".." // internal/
	fset := token.NewFileSet()
	var opened, unreleased []string
	err := filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
		if err != nil || de.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		if filepath.Base(filepath.Dir(path)) == "cache" {
			return nil // the definitions themselves
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		ast.Inspect(f, func(n ast.Node) bool {
			var body *ast.BlockStmt
			switch fn := n.(type) {
			case *ast.FuncDecl:
				body = fn.Body
			case *ast.FuncLit:
				body = fn.Body
			default:
				return true
			}
			if body == nil {
				return true
			}
			opens, releases := false, false
			ast.Inspect(body, func(m ast.Node) bool {
				if _, nested := m.(*ast.FuncLit); nested && m != n {
					// A nested closure is judged on its own (its defer runs at its return)
					// — except that a release deferred through a closure counts for the
					// enclosing function: look inside for the release only.
					ast.Inspect(m, func(k ast.Node) bool {
						if sel, ok := k.(*ast.SelectorExpr); ok && sel.Sel.Name == "ReleaseUnstored" {
							releases = true
						}
						return true
					})
					return false
				}
				sel, ok := m.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				switch sel.Sel.Name {
				case "WithL1KeyContext", "WithL1KeyContextFromEpoch":
					if x, ok := sel.X.(*ast.Ident); ok && x.Name == "cache" {
						opens = true
					}
				case "ReleaseUnstored":
					releases = true
				}
				return true
			})
			if opens {
				pos := fset.Position(n.Pos()).String()
				opened = append(opened, pos)
				if !releases {
					unreleased = append(unreleased, pos)
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(opened) == 0 {
		t.Fatal("the scan found no L1 resolve scope at all — it is not looking at the production tree")
	}
	if len(unreleased) > 0 {
		t.Fatalf("#890/#893: %d function(s) open an L1 resolve scope without releasing it (defer cache.Deps().ReleaseUnstored(key)):\n  %s",
			len(unreleased), strings.Join(unreleased, "\n  "))
	}
	t.Logf("%d production L1 resolve scopes, all released", len(opened))
}
