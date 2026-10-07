// refresh_class_stamp_ast_test.go — M16 (coverage-audit): a build-time AST
// invariant over the dispatcher serve sites. EVERY setRefreshKeyHeader /
// setRefreshKeyHeaderUnlessExternal / setRefreshKeyHeaderIfArmable call whose
// `class` argument is a STRING
// LITERAL must stamp a class in the SubscriptionCoordinates.Class allowlist
// (DeriveSubscriptionKey's switch). A literal outside the allowlist would make
// the frontend arm a /refreshes subscription under a class DeriveSubscriptionKey
// fails-closed on (default: no key) — the header + the subscription decoder
// would silently disagree and the live-refresh channel would never fire for
// that response.
//
// The class the dispatcher stamps and the class the subscription decoder accepts
// are two ends of ONE contract that is expressed as two hand-written literal
// sets (the serve-site literals here, the switch cases in
// refresh_subscription.go). This scanner pins them: it fails the build if a
// serve site stamps a class the switch does not accept — the exact drift a
// case-typo ("widgetcontent" vs "widgetContent") introduces.
//
// RED arm: TestM16_RED_TypoClassIsFlagged feeds the SAME classifier a synthetic
// call-site stamping "widgetcontent" (lowercase typo) and asserts it is flagged
// — proving the scanner discriminates a wrong literal, not just rubber-stamps.

package dispatchers

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// refreshClassAllowlist is the SubscriptionCoordinates.Class allowlist =
// DeriveSubscriptionKey's accepted set (refresh_subscription.go switch):
// cache.CacheEntryClass{WidgetContent,Apistage,RAFullList} + the classWidgets /
// classRestActions string values. Kept as literals here (this is a literal-vs-
// literal contract check); a divergence from the switch is what the scanner
// exists to catch.
var refreshClassAllowlist = map[string]struct{}{
	"widgets":       {},
	"widgetContent": {},
	"restactions":   {},
	"apistage":      {},
	"raFullList":    {},
}

// setRefreshKeyHeaderClassArgIndex maps the stamping helpers to the 0-based
// index of their `class` string argument. A new stamping helper MUST be added
// here or its serve sites silently leave the invariant's scope (the two cold
// tails did exactly that when #548 moved them onto setRefreshKeyHeaderIfArmable;
// the `total` floor below only catches a scanner that matches NOTHING).
var setRefreshKeyHeaderClassArgIndex = map[string]int{
	"setRefreshKeyHeader":               2, // (wri, key, class)
	"setRefreshKeyHeaderUnlessExternal": 2, // (wri, key, class, externalTTL)
	"setRefreshKeyHeaderIfArmable":      2, // (wri, key, class, armable) — #548
}

// scanRefreshClassLiterals walks a parsed file and returns every string-literal
// class argument passed to a stamping helper, tagged with its position.
type classStamp struct {
	fn    string
	class string
	pos   string
}

func scanRefreshClassLiterals(t *testing.T, fset *token.FileSet, file *ast.File) []classStamp {
	t.Helper()
	var out []classStamp
	ast.Inspect(file, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		ident, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		idx, tracked := setRefreshKeyHeaderClassArgIndex[ident.Name]
		if !tracked || idx >= len(call.Args) {
			return true
		}
		lit, ok := call.Args[idx].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			// A non-literal class arg (e.g. a variable) is out of scope — this
			// scanner pins LITERAL serve sites; a variable is checked by the
			// class-derivation tests. The prod dispatcher sites all use literals.
			return true
		}
		unq, err := strconv.Unquote(lit.Value)
		if err != nil {
			return true
		}
		out = append(out, classStamp{fn: ident.Name, class: unq, pos: fset.Position(lit.Pos()).String()})
		return true
	})
	return out
}

// TestM16_RefreshClassStampsAreInAllowlist scans EVERY .go file in this package
// and asserts every literal class stamped at a serve site is in the allowlist.
func TestM16_RefreshClassStampsAreInAllowlist(t *testing.T) {
	fset := token.NewFileSet()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read package dir: %v", err)
	}

	total := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		// Scan production files only — a _test.go RED fixture deliberately
		// stamps a bad literal (TestM16_RED_TypoClassIsFlagged) and must not
		// fail the production invariant.
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(".", name)
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", path, perr)
		}
		for _, s := range scanRefreshClassLiterals(t, fset, f) {
			total++
			if _, ok := refreshClassAllowlist[s.class]; !ok {
				t.Errorf("M16: %s stamps refresh-class %q at %s — NOT in the SubscriptionCoordinates.Class allowlist %v; the frontend would arm a subscription DeriveSubscriptionKey fails-closed on",
					s.fn, s.class, s.pos, keysOf(refreshClassAllowlist))
			}
		}
	}

	// Sanity: the scanner actually found the known prod serve sites (guards
	// against a silent zero-match pass if the helper names ever change).
	if total < 3 {
		t.Fatalf("M16: expected to scan >=3 literal refresh-class serve sites (restactions x2, widgetContent, widgets x2); found %d — the scanner matched nothing, so the invariant is not actually being enforced", total)
	}
}

// TestM16_RED_TypoClassIsFlagged proves the scanner is DISCRIMINATING: a
// synthetic serve site stamping "widgetcontent" (the lowercase typo of the real
// "widgetContent") is flagged as NOT in the allowlist. The GREEN prod scan above
// passes only because every real literal is correct; this arm shows a wrong one
// is caught.
func TestM16_RED_TypoClassIsFlagged(t *testing.T) {
	const src = `package dispatchers
func serveTypo(wri any, key string) {
	setRefreshKeyHeader(wri, key, "widgetcontent") // TYPO: lowercase c
}
func serveGood(wri any, key string) {
	setRefreshKeyHeader(wri, key, "widgetContent") // correct
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "synthetic_typo.go", src, 0)
	if err != nil {
		t.Fatalf("parse synthetic: %v", err)
	}
	stamps := scanRefreshClassLiterals(t, fset, f)
	if len(stamps) != 2 {
		t.Fatalf("expected 2 stamps in the synthetic source; got %d (%v)", len(stamps), stamps)
	}

	var flagged, accepted []string
	for _, s := range stamps {
		if _, ok := refreshClassAllowlist[s.class]; ok {
			accepted = append(accepted, s.class)
		} else {
			flagged = append(flagged, s.class)
		}
	}
	if len(flagged) != 1 || flagged[0] != "widgetcontent" {
		t.Fatalf("RED arm: the typo \"widgetcontent\" must be flagged (not in allowlist); flagged=%v", flagged)
	}
	if len(accepted) != 1 || accepted[0] != "widgetContent" {
		t.Fatalf("RED arm control: the correct \"widgetContent\" must be accepted; accepted=%v", accepted)
	}
}

func keysOf(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
