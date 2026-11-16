package credhttp

// resolve_lists_988_guard_test.go — #988 structural guard: the marker and the
// reason each have ONE production writer, so the counter's labels stay closed.
//
//   - MarkResolve is called only by cache.WithFallthroughScope: the marker IS a
//     resolve scope, so every resolve origin is marked by scoping its ctx and
//     no other code can make a non-resolve request count.
//   - WithResolveReason is called only by cache.WithFallthroughReason, which
//     takes a typed cache.FallthroughReason: the reason label is the closed
//     enum (plus "unattributed"), never an ad-hoc string.

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

func TestIssue988_MarkerAndReasonHaveOneWriterEach(t *testing.T) {
	root := moduleRoot(t)
	allowed := map[string]string{
		"MarkResolve":       "internal/cache/fallthrough_ctx.go",
		"WithResolveReason": "internal/cache/fallthrough_meter.go",
	}
	const self = "github.com/krateo-platformops/snowplow/internal/credhttp"
	seen := map[string][]string{}
	fset := token.NewFileSet()
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" || d.Name() == "vendor" || strings.HasPrefix(d.Name(), ".") && p != root {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		local := ""
		for _, im := range f.Imports {
			if strings.Trim(im.Path.Value, `"`) == self {
				local = "credhttp"
				if im.Name != nil {
					local = im.Name.Name
				}
			}
		}
		inPkg := filepath.ToSlash(filepath.Dir(rel)) == "internal/credhttp"
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			name := ""
			switch fn := call.Fun.(type) {
			case *ast.SelectorExpr:
				if id, ok := fn.X.(*ast.Ident); ok && local != "" && id.Name == local {
					name = fn.Sel.Name
				}
			case *ast.Ident:
				if inPkg {
					name = fn.Name
				}
			}
			if _, tracked := allowed[name]; tracked {
				seen[name] = append(seen[name], filepath.ToSlash(rel))
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for name, file := range allowed {
		sites := seen[name]
		sort.Strings(sites)
		if len(sites) != 1 || sites[0] != file {
			t.Errorf("%s is called from %v; its one production writer is %s", name, sites, file)
		}
	}
}
