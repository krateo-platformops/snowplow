package credsecret

// no_local_derivation_1382_test.go — #1382: the names of a user's credential
// Secrets are plumbing's (endpoints.ExactSecretName, SessionSecretNames,
// SecretName, ClientConfigSuffix, StepUpSuffix), never rebuilt in snowplow. A
// local copy drifts: authn#140 changed the name, and a reader still building
// MakeDNS1123Compatible(u)+"-clientconfig" would read only the legacy name,
// or another user's.
//
// The fence parses every non-test .go file of the module and fails on:
//   - a string literal that spells either suffix ("-clientconfig",
//     "-stepup"), whatever it is for: a constant, a Sprintf format, a
//     concatenation. The exemptions below RECOGNISE such a name in text (a
//     redaction pattern, a metric's help); each must still match, so it
//     cannot outlive the code it excuses;
//   - a `+` that has kubeutil.MakeDNS1123Compatible(...) or
//     endpoints.SecretName(...) / ExactSecretName(...) as an operand: a
//     derived name with something appended;
//   - a `+` that has endpoints.ClientConfigSuffix or endpoints.StepUpSuffix as
//     an operand, outside credsecret.StepUpNames: the one place the step-up
//     form is put together from plumbing's session names and suffixes, since
//     plumbing names only the session Secret.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// suffixLiterals are the spellings no literal may carry.
var suffixLiterals = []string{"-clientconfig", "-stepup"}

// derivationExempt maps "<module-relative file>|<literal>" to why that
// literal is not a derivation. Each must still match.
var derivationExempt = map[string]string{
	"internal/redact/redact.go|" + `(\\?")[^"]*-clientconfig[^"]*"`:                                                                "a redaction pattern that RECOGNISES a quoted credential Secret name in an error, to scrub it",
	"internal/handlers/dispatchers/widgets_resolve_error.go|" + `-clientconfig\b`:                                                  "an identity-shape pattern that RECOGNISES a credential Secret name in a dependency message, to withhold it",
	"internal/metrics/metrics.go|Every *-clientconfig Secret the registry sees: the denominator of the from_secrets alarm (#262).": "a metric's help text",
}

// suffixConsts are plumbing's suffix constants; concatenating one builds a name.
var suffixConsts = map[string]bool{"ClientConfigSuffix": true, "StepUpSuffix": true}

// derivingCalls are the calls whose result, with something appended, is a
// locally derived credential Secret name.
var derivingCalls = map[string]map[string]bool{
	"github.com/krateo-platformops/plumbing/kubeutil":  {"MakeDNS1123Compatible": true},
	"github.com/krateo-platformops/plumbing/endpoints": {"SecretName": true, "ExactSecretName": true},
}

const (
	importEndpoints = "github.com/krateo-platformops/plumbing/endpoints"
	// stepUpNamesSite is the one function allowed to append a suffix constant.
	stepUpNamesSite = "internal/credsecret/credsecret.go|StepUpNames"
)

// derivationViolations scans one parsed file. rel is its module-relative
// path; used collects the exemptions that matched.
func derivationViolations(fset *token.FileSet, rel string, f *ast.File, used map[string]bool) []string {
	imports := map[string]string{}
	for _, im := range f.Imports {
		p, _ := strconv.Unquote(im.Path.Value)
		name := p[strings.LastIndex(p, "/")+1:]
		if im.Name != nil {
			name = im.Name.Name
		}
		imports[name] = p
	}
	// pkgSel reports the import path and name of a selector pkg.Name.
	pkgSel := func(e ast.Expr) (string, string, bool) {
		sel, ok := e.(*ast.SelectorExpr)
		if !ok {
			return "", "", false
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok {
			return "", "", false
		}
		p, ok := imports[id.Name]
		return p, sel.Sel.Name, ok
	}
	deriving := func(e ast.Expr) bool {
		call, ok := ast.Unparen(e).(*ast.CallExpr)
		if !ok {
			return false
		}
		p, name, ok := pkgSel(call.Fun)
		return ok && derivingCalls[p][name]
	}
	suffixConst := func(e ast.Expr) bool {
		p, name, ok := pkgSel(ast.Unparen(e))
		return ok && p == importEndpoints && suffixConsts[name]
	}

	var bad []string
	at := func(n ast.Node) string { return fset.Position(n.Pos()).String() }
	var walk func(fn string) func(ast.Node) bool
	walk = func(fn string) func(ast.Node) bool {
		return func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.BasicLit:
				if x.Kind != token.STRING {
					return true
				}
				v, err := strconv.Unquote(x.Value)
				if err != nil {
					return true
				}
				for _, s := range suffixLiterals {
					if !strings.Contains(v, s) {
						continue
					}
					key := rel + "|" + v
					if _, ok := derivationExempt[key]; ok {
						used[key] = true
						break
					}
					bad = append(bad, at(x)+": the literal "+strconv.Quote(v)+" spells a credential Secret suffix; take the name from credsecret / plumbing's endpoints")
					break
				}
			case *ast.BinaryExpr:
				if x.Op != token.ADD {
					return true
				}
				if deriving(x.X) || deriving(x.Y) {
					bad = append(bad, at(x)+": a derived name with something appended; take the credential Secret names from credsecret / plumbing's endpoints")
				}
				if (suffixConst(x.X) || suffixConst(x.Y)) && rel+"|"+fn != stepUpNamesSite {
					bad = append(bad, at(x)+": a plumbing suffix constant concatenated outside credsecret.StepUpNames; take the names from credsecret")
				}
			}
			return true
		}
	}
	for _, d := range f.Decls {
		fn := ""
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil {
			fn = fd.Name.Name
		}
		ast.Inspect(d, walk(fn))
	}
	return bad
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..")
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found from %s: %v", file, err)
	}
	return root
}

func TestIssue1382_NoLocalClientconfigDerivation(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	used := map[string]bool{}
	var bad []string
	scanned := 0
	sawStepUpNames := false
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if n := d.Name(); p != root && (strings.HasPrefix(n, ".") || n == "testdata" || n == "vendor") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		scanned++
		bad = append(bad, derivationViolations(fset, rel, f, used)...)
		if rel == "internal/credsecret/credsecret.go" {
			for _, d := range f.Decls {
				if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "StepUpNames" {
					sawStepUpNames = true
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 100 {
		t.Fatalf("scanned only %d files: the walk is broken and would pass by finding nothing", scanned)
	}
	if !sawStepUpNames {
		t.Errorf("credsecret.StepUpNames is gone: the exemption %s names nothing", stepUpNamesSite)
	}
	sort.Strings(bad)
	for _, b := range bad {
		t.Error(b)
	}
	for k, why := range derivationExempt {
		if !used[k] {
			t.Errorf("stale exemption %q (%s): it matches nothing, remove it", k, why)
		}
	}
}

// The fence must bite on each shape of a local derivation, and let the
// sanctioned ones through.
func TestIssue1382_FenceCatchesEveryShape(t *testing.T) {
	const hdr = "package x\nimport (\n\"fmt\"\n\"strings\"\n\"github.com/krateo-platformops/plumbing/kubeutil\"\nep \"github.com/krateo-platformops/plumbing/endpoints\"\n)\nvar _ = fmt.Sprint\nvar _ = strings.Cut\nvar _ = kubeutil.MakeDNS1123Compatible\nvar _ = ep.SecretName\n"
	cases := map[string]struct {
		rel, src string
		wantBad  bool
	}{
		"const suffix":          {"internal/x/x.go", hdr + "const S = \"-clientconfig\"\n", true},
		"sprintf":               {"internal/x/x.go", hdr + "func N(u string) string { return fmt.Sprintf(\"%s-clientconfig\", u) }\n", true},
		"stepup literal":        {"internal/x/x.go", hdr + "func N(u string) string { return ep.SecretName(u) + \"-stepup\" }\n", true},
		"dns1123 plus":          {"internal/x/x.go", hdr + "func N(u, s string) string { return kubeutil.MakeDNS1123Compatible(u) + s }\n", true},
		"exact plus":            {"internal/x/x.go", hdr + "func N(u string) string { n, _ := ep.ExactSecretName(u); return n }\nfunc M(u, s string) string { return ep.SecretName(u) + s }\n", true},
		"suffix const plus":     {"internal/x/x.go", hdr + "func N(u string) string { return u + ep.ClientConfigSuffix }\n", true},
		"suffix const trimmed":  {"internal/x/x.go", hdr + "func N(n string) bool { return strings.HasSuffix(n, ep.StepUpSuffix) }\n", false},
		"stepup site":           {"internal/credsecret/credsecret.go", hdr + "func StepUpNames(n string) string { return strings.TrimSuffix(n, ep.ClientConfigSuffix) + ep.StepUpSuffix }\n", false},
		"stepup site elsewhere": {"internal/credsecret/credsecret.go", hdr + "func Other(n string) string { return n + ep.StepUpSuffix }\n", true},
		"plumbing names":        {"internal/x/x.go", hdr + "func N(u string) []string { return ep.SessionSecretNames(u) }\n", false},
		"dns1123 alone":         {"internal/x/x.go", hdr + "func N(u string) string { return kubeutil.MakeDNS1123Compatible(u) }\n", false},
	}
	for name, c := range cases {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "x.go", c.src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		bad := derivationViolations(fset, c.rel, f, map[string]bool{})
		if got := len(bad) > 0; got != c.wantBad {
			t.Errorf("%s: flagged=%v, want %v (%v)", name, got, c.wantBad, bad)
		}
	}
}
