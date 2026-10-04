// inline443_enum_test.go — #443 part 2, the ENUMERATION GUARD for the inert
// (dry-run) flag.
//
// The inline dry-run resolve persists nothing because ONE ctx flag
// (cache.Inert) is checked inside the cache package's own ctx-taking mutators
// and at a few registration and egress points (choke points (a)–(n)). Those
// checks only hold while the resolve path keeps calling the ctx-taking forms.
// This guard type-checks the resolve-path packages and fails on any use of a
// ctx-LESS side-effecting entry point, so a future edit cannot quietly bypass
// the flag:
//
//   - (*cache.ResourceWatcher).EnsureResourceType (use EnsureResourceTypeFor);
//   - (*cache.ResolvedCacheStore).Get / Put / PutRAFullList (ctx-less; Get
//     stamps warmth, Put/PutRAFullList persist);
//   - cache.RegisterClusterListKey, cache.AddNavigationDiscoveredGroup, and any
//     cache.Enqueue* / cache.Publish* function;
//   - a `go` statement whose body uses context.Background (it would drop the
//     flag on the floor).
//
// Scope: internal/resolvers/..., internal/objects/... and
// handlers/dispatchers/nested_call.go. Every remaining use must be on the
// allow-list below, and each allow-listed function must itself contain a
// cache.Inert check (or be outside any request path), which this test also
// verifies — so deleting a gate fails here.
package dispatchers

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const cachePkgPath443 = "github.com/krateo-platformops/snowplow/internal/cache"

// allow443 lists the ctx-less uses that are acceptable, keyed by
// "<file base>:<enclosing func>:<symbol>". needsInert says the enclosing
// function must contain a cache.Inert(...) call (the gate that makes the use
// safe under the flag).
var allow443 = map[string]struct {
	why        string
	needsInert bool
}{
	// (c) — lazyRegisterInnerCallPaths returns at its head under the flag.
	"resolve.go:lazyRegisterInnerCallPaths:AddNavigationDiscoveredGroup": {"(c) early return under cache.Inert", true},
	// (e) — readContent picks GetNoTouch under the flag; Get is the live-/call warmth stamp.
	"apistage.go:apistageContentServe:Get": {"(e) GetNoTouch under cache.Inert", true},
	// (e) — raFullListServe reads with GetNoTouch under the flag.
	"ra_full_list.go:raFullListServe:Get": {"(e) GetNoTouch under cache.Inert", true},
	// (d) — the async populate returns at its head under the flag, before the
	// goroutine that detaches to context.Background.
	"cluster_list.go:populateClusterListCellAsync:go+context.Background": {"(d) early return under cache.Inert", true},
	"cluster_list.go:populateClusterListCellSync:RegisterClusterListKey": {"(d) only reached after a PutIfGen that refuses under cache.Inert", false},
}

func TestS443_InertEnumerationGuard(t *testing.T) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
			packages.NeedTypes | packages.NeedTypesInfo,
		Dir:   "../../..",
		Tests: false,
	}
	pkgs, err := packages.Load(cfg,
		"./internal/resolvers/...", "./internal/objects/...", "./internal/handlers/dispatchers")
	if err != nil {
		t.Fatalf("packages.Load: %v", err)
	}
	var hits []string
	inertFuncs := map[string]bool{} // "<file>:<func>" containing cache.Inert(
	for _, p := range pkgs {
		for _, e := range p.Errors {
			t.Fatalf("load %s: %v", p.PkgPath, e)
		}
		for _, f := range p.Syntax {
			base := filepath.Base(p.Fset.Position(f.Pos()).Filename)
			if strings.HasSuffix(base, "_test.go") {
				continue
			}
			if p.PkgPath == "github.com/krateo-platformops/snowplow/internal/handlers/dispatchers" && base != "nested_call.go" {
				continue
			}
			if base == "cluster_list_prewarm.go" { // prewarm-only, never on a resolve path
				continue
			}
			for _, d := range f.Decls {
				fn, ok := d.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				where := base + ":" + fn.Name.Name
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					switch x := n.(type) {
					case *ast.CallExpr:
						if isCachePkgFunc443(p.TypesInfo, x.Fun, "Inert") {
							inertFuncs[where] = true
						}
					case *ast.SelectorExpr:
						if sym := forbidden443(p.TypesInfo, x); sym != "" {
							hits = append(hits, where+":"+sym+" @ "+p.Fset.Position(x.Pos()).String())
						}
					case *ast.GoStmt:
						if usesContextBackground443(p.TypesInfo, x) {
							hits = append(hits, where+":go+context.Background @ "+p.Fset.Position(x.Pos()).String())
						}
					}
					return true
				})
			}
		}
	}
	sort.Strings(hits)
	seen := map[string]bool{}
	for _, h := range hits {
		key := h[:strings.Index(h, " @ ")]
		a, ok := allow443[key]
		if !ok {
			t.Errorf("ctx-less side-effect entry point on the resolve path: %s — use the ctx-taking form "+
				"(EnsureResourceTypeFor / PutIfGen / GetNoTouch …) or gate it on cache.Inert and allow-list it", h)
			continue
		}
		seen[key] = true
		fnKey := key[:strings.LastIndex(key, ":")]
		if a.needsInert && !inertFuncs[fnKey] {
			t.Errorf("%s is allow-listed (%s) but %s no longer contains a cache.Inert check", h, a.why, fnKey)
		}
	}
	for k := range allow443 {
		if !seen[k] {
			t.Errorf("stale allow-list entry %q: no such use any more — remove it", k)
		}
	}
	if len(inertFuncs) == 0 {
		t.Fatal("guard saw no cache.Inert call at all — the type-check scope is wrong (the arm cannot fail)")
	}
}

func isCachePkgFunc443(info *types.Info, fun ast.Expr, name string) bool {
	sel, ok := fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	obj, ok := info.Uses[sel.Sel].(*types.Func)
	return ok && obj.Pkg() != nil && obj.Pkg().Path() == cachePkgPath443
}

// forbidden443 returns the symbol name when sel selects a forbidden ctx-less
// entry point (as a call or a method value), else "".
func forbidden443(info *types.Info, sel *ast.SelectorExpr) string {
	obj, ok := info.Uses[sel.Sel].(*types.Func)
	if !ok || obj.Pkg() == nil || obj.Pkg().Path() != cachePkgPath443 {
		return ""
	}
	name := obj.Name()
	sig, _ := obj.Type().(*types.Signature)
	if sig != nil && sig.Recv() != nil {
		recv := sig.Recv().Type()
		if p, ok := recv.(*types.Pointer); ok {
			recv = p.Elem()
		}
		named, _ := recv.(*types.Named)
		if named == nil {
			return ""
		}
		switch named.Obj().Name() {
		case "ResourceWatcher":
			if name == "EnsureResourceType" {
				return name
			}
		case "ResolvedCacheStore":
			switch name {
			case "Get", "Put", "PutRAFullList":
				return name
			}
		}
		return ""
	}
	switch {
	case name == "RegisterClusterListKey", name == "AddNavigationDiscoveredGroup",
		strings.HasPrefix(name, "Enqueue"), strings.HasPrefix(name, "Publish"):
		return name
	}
	return ""
}

func usesContextBackground443(info *types.Info, g *ast.GoStmt) bool {
	found := false
	ast.Inspect(g, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Background" {
			return true
		}
		if obj, ok := info.Uses[sel.Sel].(*types.Func); ok && obj.Pkg() != nil && obj.Pkg().Path() == "context" {
			found = true
		}
		return !found
	})
	return found
}
