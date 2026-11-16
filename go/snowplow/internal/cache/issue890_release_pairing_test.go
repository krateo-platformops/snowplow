//go:build unit || integration

// issue890_release_pairing_test.go — #890/#893 structural guard: every production
// function that opens an L1 resolve scope (cache.WithL1KeyContext /
// WithL1KeyContextFromEpoch) also releases it (Deps().ReleaseUnstored) in the
// same function. A new resolve entry that forgets the release would bring back
// the leak for its declines and errors; this fails until its author adds it.
//
// The call is resolved by IMPORT PATH, not by the identifier "cache": an aliased
// import (snowplowcache.WithL1KeyContext) or a dot import is a scope too, and so
// is an unqualified call inside package cache itself. The whole module is
// scanned (every non-test .go file outside testdata / vendor).

package cache

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const cacheImportPath890 = "github.com/krateo-platformops/snowplow/internal/cache"

var l1ScopeOpeners890 = map[string]bool{"WithL1KeyContext": true, "WithL1KeyContextFromEpoch": true}

// l1ScopeViolations890 returns the functions in f that open an L1 resolve scope,
// and those of them that do not release it. inCachePkg marks a file of package
// cache itself, where the openers are called unqualified.
func l1ScopeViolations890(fset *token.FileSet, f *ast.File, inCachePkg bool) (opened, unreleased []string) {
	qualifiers := map[string]bool{}
	unqualified := inCachePkg
	for _, imp := range f.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		if p != cacheImportPath890 {
			continue
		}
		switch {
		case imp.Name == nil:
			qualifiers["cache"] = true
		case imp.Name.Name == ".":
			unqualified = true
		case imp.Name.Name != "_":
			qualifiers[imp.Name.Name] = true
		}
	}
	if len(qualifiers) == 0 && !unqualified {
		return nil, nil
	}
	isOpen := func(call *ast.CallExpr) bool {
		switch fn := call.Fun.(type) {
		case *ast.SelectorExpr:
			x, ok := fn.X.(*ast.Ident)
			return ok && qualifiers[x.Name] && l1ScopeOpeners890[fn.Sel.Name]
		case *ast.Ident:
			return unqualified && l1ScopeOpeners890[fn.Name]
		}
		return false
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
			if lit, nested := m.(*ast.FuncLit); nested && lit != n {
				// A nested closure is judged on its own; a release deferred through a
				// closure still counts for the enclosing function.
				ast.Inspect(lit, func(k ast.Node) bool {
					if sel, ok := k.(*ast.SelectorExpr); ok && sel.Sel.Name == "ReleaseUnstored" {
						releases = true
					}
					return true
				})
				return false
			}
			switch x := m.(type) {
			case *ast.CallExpr:
				if isOpen(x) {
					opens = true
				}
			case *ast.SelectorExpr:
				if x.Sel.Name == "ReleaseUnstored" {
					releases = true
				}
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
	return opened, unreleased
}

func TestIssue890_EveryL1ResolveScopeIsReleased(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..")) // the module root (go/snowplow)
	if err != nil {
		t.Fatal(err)
	}
	cacheDir := filepath.Join(root, "internal", "cache")
	fset := token.NewFileSet()
	var opened, unreleased []string
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
		o, u := l1ScopeViolations890(fset, f, filepath.Dir(path) == cacheDir)
		opened = append(opened, o...)
		unreleased = append(unreleased, u...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(opened) == 0 {
		t.Fatalf("the scan of %d files found no L1 resolve scope at all — it is not looking at the production tree", files)
	}
	if len(unreleased) > 0 {
		t.Fatalf("#890/#893: %d function(s) open an L1 resolve scope without releasing it (defer cache.Deps().ReleaseUnstored(ctx, key)):\n  %s",
			len(unreleased), strings.Join(unreleased, "\n  "))
	}
	t.Logf("%d files, %d production L1 resolve scopes, all released", files, len(opened))
}

// TestIssue890_ReleaseGuardSeesAliasesAndInPackageCalls — the reviewer's F-b
// probes: an aliased import, a dot import and an unqualified call inside package
// cache are scopes too; a released one passes.
func TestIssue890_ReleaseGuardSeesAliasesAndInPackageCalls(t *testing.T) {
	cases := []struct {
		name, src  string
		inCachePkg bool
		want       int // unreleased
	}{
		{"alias_unreleased", `package handlers
import snowplowcache "github.com/krateo-platformops/snowplow/internal/cache"
func plurals(ctx context.Context) { ctx = snowplowcache.WithL1KeyContext(ctx, "k"); _ = ctx }`, false, 1},
		{"alias_released", `package handlers
import snowplowcache "github.com/krateo-platformops/snowplow/internal/cache"
func plurals(ctx context.Context) {
	ctx = snowplowcache.WithL1KeyContext(ctx, "k")
	defer snowplowcache.Deps().ReleaseUnstored(ctx, "k")
}`, false, 0},
		{"dot_import_unreleased", `package handlers
import . "github.com/krateo-platformops/snowplow/internal/cache"
func f(ctx context.Context) { ctx = WithL1KeyContextFromEpoch(ctx, "k", 0); _ = ctx }`, false, 1},
		{"in_cache_package_unreleased", `package cache
func f(ctx context.Context) { ctx = WithL1KeyContext(ctx, "k"); _ = ctx }`, true, 1},
		{"other_package_named_cache_is_not_ours", `package handlers
import cache "example.com/other/cache"
func f(ctx context.Context) { ctx = cache.WithL1KeyContext(ctx, "k"); _ = ctx }`, false, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, c.name+".go", c.src, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, u := l1ScopeViolations890(fset, f, c.inCachePkg)
			if len(u) != c.want {
				t.Fatalf("#890 F-b RED: the guard reported %d unreleased scope(s) %v, want %d", len(u), u, c.want)
			}
		})
	}
}
