package credhttp

// browser_fetch_guard_test.go: on js/wasm every client plumbing builds leaves
// through the browser's fetch with fetch's default credentials mode
// (same-origin), so snowplow passes each through PlumbingClient, which sets
// "omit" (egress_js.go). This walks every non-test .go file in the module, of
// every build, and fails on:
//   - a function that builds more plumbing clients (request.HTTPClientForEndpoint,
//     dialguard.Client) than it passes through PlumbingClient;
//   - a JWKS key source (jwtutil.NewJWKSKeySource) given no client of its
//     own (jwtutil.WithJWKSHTTPClient): plumbing would build one inside,
//     where PlumbingClient cannot reach it.
//
// The fetch-mode tests (egress_js_test.go, restactions/api's
// endpoints_tls_js_test.go) send on each of those clients and check the
// options fetch was called with; this keeps a new call site from being
// missed.

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

const (
	importDialguard = "github.com/krateo-platformops/plumbing/dialguard"
	importJWTUtil   = "github.com/krateo-platformops/plumbing/jwtutil"
)

type plumbingClientFinding struct {
	fn   string
	pos  token.Position
	what string
}

// plumbingClientScan returns every place in f that sends on a plumbing client
// without PlumbingClient, and how many plumbing client builders it saw.
func plumbingClientScan(fset *token.FileSet, f *ast.File, rel string) (out []plumbingClientFinding, builders int) {
	imports := map[string]string{}
	for _, im := range f.Imports {
		p, _ := strconv.Unquote(im.Path.Value)
		name := p[strings.LastIndex(p, "/")+1:]
		if im.Name != nil {
			name = im.Name.Name
		}
		imports[name] = p
	}
	inCredhttp := strings.HasPrefix(rel, "internal/credhttp/")
	callee := func(call *ast.CallExpr) (pkg, name string) {
		switch f := call.Fun.(type) {
		case *ast.SelectorExpr:
			if id, ok := f.X.(*ast.Ident); ok {
				return imports[id.Name], f.Sel.Name
			}
		case *ast.Ident:
			if inCredhttp {
				return importCredhttp, f.Name
			}
		}
		return "", ""
	}
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		built, wrapped := 0, 0
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch pkg, name := callee(call); {
			case pkg == importRequest && name == "HTTPClientForEndpoint", pkg == importDialguard && name == "Client":
				built++
			case pkg == importCredhttp && name == "PlumbingClient":
				wrapped++
			case pkg == importJWTUtil && name == "NewJWKSKeySource":
				hasClient := false
				for _, a := range call.Args {
					if ac, ok := a.(*ast.CallExpr); ok {
						if p, n := callee(ac); p == importJWTUtil && n == "WithJWKSHTTPClient" {
							hasClient = true
						}
					}
				}
				if !hasClient {
					out = append(out, plumbingClientFinding{fd.Name.Name, fset.Position(call.Pos()),
						"jwtutil.NewJWKSKeySource without jwtutil.WithJWKSHTTPClient: plumbing builds the client inside, where PlumbingClient cannot reach it"})
				}
			}
			return true
		})
		builders += built
		if built > wrapped {
			out = append(out, plumbingClientFinding{fd.Name.Name, fset.Position(fd.Pos()),
				strconv.Itoa(built) + " plumbing client(s) built, " + strconv.Itoa(wrapped) + " passed through credhttp.PlumbingClient"})
		}
	}
	return out, builders
}

func TestIssue1359_EveryPlumbingClientGoesThroughPlumbingClient(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	var bad []string
	scanned, builders := 0, 0
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
		rel = filepath.ToSlash(rel)
		f, perr := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if perr != nil {
			return perr
		}
		scanned++
		found, n := plumbingClientScan(fset, f, rel)
		builders += n
		for _, r := range found {
			bad = append(bad, r.pos.String()+" (in "+r.fn+"): "+r.what)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Non-empty control: the /call passthrough (1), the stage fetch's plumbing
	// branch (2) and the JWKS client (1) are known builders.
	if scanned < 100 || builders < 4 {
		t.Fatalf("scanned %d files and found %d plumbing client builders: the walk is broken", scanned, builders)
	}
	for _, b := range bad {
		t.Error(b)
	}
}

// The guard must bite on every shape it claims to catch, and pass the
// wrapped ones.
func TestIssue1359_PlumbingClientGuardDetectsEveryShape(t *testing.T) {
	scan := func(body, rel string) []plumbingClientFinding {
		t.Helper()
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "x.go", "package p\n"+body+"\n", parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%v\n%s", err, body)
		}
		got, _ := plumbingClientScan(fset, f, rel)
		return got
	}
	const req = `import (r "github.com/krateo-platformops/plumbing/http/request"; "github.com/krateo-platformops/plumbing/dialguard"; "github.com/krateo-platformops/plumbing/jwtutil"; "github.com/krateo-platformops/snowplow/internal/credhttp");`
	for name, body := range map[string]string{
		"endpointClient":     req + ` func f() { c, _ := r.HTTPClientForEndpoint(nil, nil); _ = c }`,
		"dialguardClient":    req + ` func f() { _ = dialguard.Client(0) }`,
		"oneOfTwoWrapped":    req + ` func f() { a, _ := r.HTTPClientForEndpoint(nil, nil); b, _ := r.HTTPClientForEndpoint(nil, nil); _, _ = credhttp.PlumbingClient(a), b }`,
		"jwksWithoutClient":  req + ` func f() { _ = jwtutil.NewJWKSKeySource("u", jwtutil.WithJWKSCacheTTL(0)) }`,
		"unqualifiedOutside": req + ` func f() { c, _ := r.HTTPClientForEndpoint(nil, nil); _ = PlumbingClient(c) }`,
	} {
		if got := scan(body, "internal/other/x.go"); len(got) == 0 {
			t.Errorf("%s: guard did not flag %q", name, body)
		}
	}
	for name, body := range map[string]string{
		"endpointClient": req + ` func f() { c, _ := r.HTTPClientForEndpoint(nil, nil); _ = credhttp.PlumbingClient(c) }`,
		"jwks":           req + ` func f() { _ = jwtutil.NewJWKSKeySource("u", jwtutil.WithJWKSHTTPClient(credhttp.PlumbingClient(dialguard.Client(0)))) }`,
	} {
		if got := scan(body, "internal/other/x.go"); len(got) != 0 {
			t.Errorf("%s: wrapped form flagged: %+v", name, got)
		}
	}
	// PlumbingClient unqualified is credhttp's only inside credhttp.
	if got := scan(req+` func f() { c, _ := r.HTTPClientForEndpoint(nil, nil); _ = PlumbingClient(c) }`, "internal/credhttp/x.go"); len(got) != 0 {
		t.Errorf("PlumbingClient inside credhttp: want it allowed, got %+v", got)
	}
}
