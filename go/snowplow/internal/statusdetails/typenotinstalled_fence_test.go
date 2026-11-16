package statusdetails

// typenotinstalled_fence_test.go: a *TypeNotInstalledError is created in ONE
// place in the module, objects.Get's typeNotInstalled. That is what makes
// "recognised by its Go type" mean "snowplow decided it": if another package
// built one (from a status's details, say), an upstream could again choose
// what snowplow answers (#1438 review, B1).
//
// The fence reads every non-test Go file of the module and finds each
// composite literal or new() of the type.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestTypeNotInstalledError_IsCreatedOnlyByObjectsGet(t *testing.T) {
	root, err := filepath.Abs("../..") // go/snowplow
	if err != nil {
		t.Fatal(err)
	}
	isType := func(e ast.Expr) bool {
		switch x := e.(type) {
		case *ast.SelectorExpr: // statusdetails.TypeNotInstalledError
			return x.Sel.Name == "TypeNotInstalledError"
		case *ast.Ident: // inside this package
			return x.Name == "TypeNotInstalledError"
		}
		return false
	}
	var sites []string
	files := 0
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			if n := d.Name(); n == "vendor" || n == "node_modules" || n == "testdata" || (strings.HasPrefix(n, ".") && path != root) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
		if perr != nil {
			return perr
		}
		files++
		rel, _ := filepath.Rel(root, path)
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CompositeLit:
				if isType(x.Type) {
					sites = append(sites, filepath.ToSlash(rel))
				}
			case *ast.CallExpr:
				if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "new" && len(x.Args) == 1 && isType(x.Args[0]) {
					sites = append(sites, filepath.ToSlash(rel))
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if files < 100 {
		t.Fatalf("PRECONDITION: read only %d Go files under %s; the fence must read the whole module", files, root)
	}
	sort.Strings(sites)
	if len(sites) != 1 || sites[0] != "internal/objects/get.go" {
		t.Fatalf("a TypeNotInstalledError is created at %v, want exactly [internal/objects/get.go]", sites)
	}
}
