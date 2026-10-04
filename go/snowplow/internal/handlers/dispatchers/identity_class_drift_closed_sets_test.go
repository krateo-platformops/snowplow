package dispatchers

// identity_class_drift_closed_sets_test.go — #448 (F8 attribute hygiene).
//
// snowplow_l1_identity_class_drift_declined_total reaches OTLP as one series per
// {site, reason} of the CLOSED sets IdentityClassDriftSites x
// IdentityClassDriftReasons. A new call site or a new reason that is not added
// to those lists still ticks the expvar map but never reaches ClickStack, which
// is a silent hole. This arm derives both sets from source and requires them to
// equal the declared lists in both directions.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestIdentityClassDrift448_CallSitesUseTheClosedSets(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sites := map[string]bool{}
	reasons := map[string]bool{}
	calls := 0
	for _, pkg := range pkgs {
		for _, f := range pkg.Files {
			ast.Inspect(f, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CallExpr:
					id, ok := x.Fun.(*ast.Ident)
					if !ok || id.Name != "noteIdentityClassDrift" || len(x.Args) != 2 {
						return true
					}
					calls++
					lit, ok := x.Args[0].(*ast.BasicLit)
					if !ok || lit.Kind != token.STRING {
						t.Errorf("%s: noteIdentityClassDrift site must be a string literal (closed set)", fset.Position(x.Pos()))
						return true
					}
					v, _ := strconv.Unquote(lit.Value)
					sites[v] = true
				case *ast.FuncDecl:
					if x.Name.Name != "identityClassDrift" && x.Name.Name != "identityClassDriftCtx" {
						return true
					}
					ast.Inspect(x.Body, func(m ast.Node) bool {
						r, ok := m.(*ast.ReturnStmt)
						if !ok || len(r.Results) != 1 {
							return true
						}
						if lit, ok := r.Results[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
							if v, _ := strconv.Unquote(lit.Value); v != "" {
								reasons[v] = true
							}
						}
						return true
					})
				}
				return true
			})
		}
	}
	if calls < 5 {
		t.Fatalf("non-exercise guard: found %d noteIdentityClassDrift call sites, want >= 5", calls)
	}
	eq := func(what string, got map[string]bool, want []string) {
		var g []string
		for k := range got {
			g = append(g, k)
		}
		w := append([]string(nil), want...)
		sort.Strings(g)
		sort.Strings(w)
		if strings.Join(g, ",") != strings.Join(w, ",") {
			t.Errorf("#448: %s derived from source = %v, declared closed set = %v — update the declared list so every value reaches OTLP", what, g, w)
		}
	}
	eq("sites", sites, IdentityClassDriftSites)
	eq("reasons", reasons, IdentityClassDriftReasons)
}

func TestIdentityClassDrift448_CellsCoverTheClosedProduct(t *testing.T) {
	read := func() map[string]int64 {
		cells := IdentityClassDriftDeclinedCells()
		if want := len(IdentityClassDriftSites) * len(IdentityClassDriftReasons); len(cells) != want {
			t.Fatalf("cells = %d, want the full closed product %d", len(cells), want)
		}
		out := map[string]int64{}
		for _, c := range cells {
			out[c.Site+"/"+c.Reason] = c.Count
		}
		return out
	}
	// Deltas, not a reset: refresher goroutines left by other arms may tick
	// other pairs concurrently.
	before := read()
	NoteIdentityClassDriftForTest("seed", "rbac_subgen")
	NoteIdentityClassDriftForTest("seed", "rbac_subgen")
	NoteIdentityClassDriftForTest("widgets", "no_identity")
	after := read()
	if d := after["seed/rbac_subgen"] - before["seed/rbac_subgen"]; d != 2 {
		t.Fatalf("seed/rbac_subgen delta = %d, want 2 (cells read the wrong counter)", d)
	}
	if d := after["widgets/no_identity"] - before["widgets/no_identity"]; d != 1 {
		t.Fatalf("widgets/no_identity delta = %d, want 1 (cells read the wrong counter)", d)
	}
	if after["seed/rbac_subgen"] != identityClassDriftDeclinedForTest("seed", "rbac_subgen") {
		t.Fatalf("cells and the expvar map disagree")
	}
}
