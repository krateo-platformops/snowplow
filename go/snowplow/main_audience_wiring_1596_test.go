package main

// main_audience_wiring_1596_test.go — #1596: every place snowplow validates
// authn's tokens passes the audience options. UserConfig, UserConfigOrPublic
// and RefreshAuth take them variadically (so a forgotten argument compiles),
// which this test closes: in the non-test files of this package, each call of
// the three must spread an options slice (`aud...`).

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"
)

func TestIssue1596_EveryTokenValidationSitePassesTheAudience(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	validating := map[string]bool{"UserConfig": true, "UserConfigOrPublic": true, "RefreshAuth": true}
	sites := 0
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		af, err := parser.ParseFile(fset, f, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(af, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if x, ok := sel.X.(*ast.Ident); !ok || x.Name != "middleware" || !validating[sel.Sel.Name] {
				return true
			}
			sites++
			if !call.Ellipsis.IsValid() {
				t.Errorf("%s: middleware.%s is called without the audience options (spread an []jwtutil.ValidateOption)",
					fset.Position(call.Pos()), sel.Sel.Name)
			}
			return true
		})
	}
	if sites < 5 {
		t.Fatalf("found %d validating call sites, want at least 5 (UserConfigOrPublic, UserConfig in public_identity.go; RefreshAuth in main.go twice and debug_routes.go): the scan has stopped seeing them", sites)
	}
}
