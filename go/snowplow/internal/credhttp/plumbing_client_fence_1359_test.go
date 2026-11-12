package credhttp

// plumbing_client_fence_1359_test.go — #1359: only credhttp builds the clients
// plumbing builds, and hands each out (plumbing_clients.go, do.go), so every
// client snowplow sends on is on plumbing's guarded path
// (dialguard.RoundTripper, which on js/wasm sets fetch's redirect mode "error"
// and credentials mode "omit"). A client built anywhere else could skip it.
//
// The fence, type-checked over every build of the module (loadWholeModule: the
// server build, rbac_narrow_baseline and js/wasm), on every non-test file,
// test support included. Outside internal/credhttp nothing may:
//   - import plumbing/dialguard, the package that builds plumbing's transport
//     and clients;
//   - refer to a function of plumbing/http/request or plumbing/jwtutil that
//     builds, takes or holds an *http.Client, an http.RoundTripper or an
//     *http.Transport (HTTPClientForEndpoint, NewJWKSKeySource,
//     WithJWKSHTTPClient), or that sends on a client it builds inside
//     (request.Do).
//
// A reference is any use of the name: a call, a package-level var, a func
// literal, a function value (mk := dialguard.Client), a nil argument. Types
// (request.RequestInfo, ...) stay usable. Every exported function of
// plumbing/http/request is classified, so a plumbing bump that adds one fails
// here until it is.

import (
	"go/ast"
	"go/types"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const (
	importDialguard = "github.com/krateo-platformops/plumbing/dialguard"
	importJWTUtil   = "github.com/krateo-platformops/plumbing/jwtutil"
)

// sendsInside are the fenced functions whose signature shows no client: they
// build one inside and send on it.
var sendsInside = map[string]bool{importRequest + ".Do": true}

// requestFuncsAllowed are plumbing/http/request's exported functions that
// neither build a client nor send: usable anywhere.
var requestFuncsAllowed = map[string]string{
	"ComputeAwsHeaders":   "computes the AWS signature headers; sends nothing (external_fetch.go adds them to a request credhttp sends)",
	"ComputeAwsSignature": "computes the AWS signature; sends nothing",
}

// fenceAllowed: (package path relative to the module, fenced package) → reason.
// Each must still match.
var fenceAllowed = map[[2]string]string{
	{"internal/testsupport/jsfetch", importDialguard}: "js/wasm test support, imported only by tests: the probe that observes whether net/http sends dialguard.Transport through fetch. It builds no client snowplow sends on.",
}

// holdsClient reports whether t is, points to, or holds in a field (two
// levels deep) an *http.Client, an http.RoundTripper or an *http.Transport.
// Function types are not entered: an option func that takes the key source
// does not hand out its client.
func holdsClient(t types.Type, depth int) bool {
	if depth > 2 {
		return false
	}
	switch u := t.(type) {
	case *types.Pointer:
		return holdsClient(u.Elem(), depth)
	case *types.Slice:
		return holdsClient(u.Elem(), depth)
	case *types.Named:
		if o := u.Obj(); o.Pkg() != nil && o.Pkg().Path() == importHTTP {
			switch o.Name() {
			case "Client", "RoundTripper", "Transport":
				return true
			}
		}
		return holdsClient(u.Underlying(), depth)
	case *types.Struct:
		for i := 0; i < u.NumFields(); i++ {
			if holdsClient(u.Field(i).Type(), depth+1) {
				return true
			}
		}
	}
	return false
}

// fencedFunc reports whether f, a package-level function of plumbing's, is
// fenced to credhttp.
func fencedFunc(f *types.Func) bool {
	if sendsInside[f.Pkg().Path()+"."+f.Name()] {
		return true
	}
	sig := f.Type().(*types.Signature)
	for _, tup := range []*types.Tuple{sig.Params(), sig.Results()} {
		for i := 0; i < tup.Len(); i++ {
			if holdsClient(tup.At(i).Type(), 0) {
				return true
			}
		}
	}
	return false
}

// fenced reports whether a use of obj is fenced to credhttp, and names it.
func fenced(obj types.Object) (string, bool) {
	if obj == nil || obj.Pkg() == nil {
		return "", false
	}
	switch obj.Pkg().Path() {
	case importDialguard:
		return "dialguard." + obj.Name(), true
	case importRequest, importJWTUtil:
		f, ok := obj.(*types.Func)
		if !ok || f.Type().(*types.Signature).Recv() != nil || !fencedFunc(f) {
			return "", false
		}
		return obj.Pkg().Name() + "." + obj.Name(), true
	}
	return "", false
}

type fenceHit struct {
	pkg, decl, what, pos string
	fencedPkg            string
}

// fenceScan returns every fenced import and reference in pkgs outside
// credhttp.
func fenceScan(pkgs []*packages.Package, modulePath string) []fenceHit {
	credhttpPath := modulePath + "/internal/credhttp"
	var out []fenceHit
	for _, p := range pkgs {
		if p.PkgPath == credhttpPath || p.TypesInfo == nil {
			continue
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(p.PkgPath, modulePath), "/")
		for _, f := range p.Syntax {
			for _, im := range f.Imports {
				if path, _ := strconv.Unquote(im.Path.Value); path == importDialguard {
					out = append(out, fenceHit{rel, "import", "import of plumbing/dialguard", p.Fset.Position(im.Pos()).String(), importDialguard})
				}
			}
			for _, d := range f.Decls {
				decl := declName(d)
				ast.Inspect(d, func(n ast.Node) bool {
					id, ok := n.(*ast.Ident)
					if !ok {
						return true
					}
					obj := p.TypesInfo.Uses[id]
					if what, bad := fenced(obj); bad {
						out = append(out, fenceHit{rel, decl, what, p.Fset.Position(id.Pos()).String(), obj.Pkg().Path()})
					}
					return true
				})
			}
		}
	}
	return out
}

func TestIssue1359_OnlyCredhttpBuildsPlumbingClients(t *testing.T) {
	if testing.Short() {
		t.Skip("type-checks the whole module")
	}
	root, err := filepath.Abs(moduleRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	modulePath := modulePathOf(t, root)
	pkgs, _ := loadWholeModule(t, root)
	if len(pkgs) < 20 {
		t.Fatalf("NON-VACUITY: only %d packages loaded", len(pkgs))
	}

	used := map[[2]string]bool{}
	var bad []string
	for _, h := range fenceScan(pkgs, modulePath) {
		k := [2]string{h.pkg, h.fencedPkg}
		if _, ok := fenceAllowed[k]; ok {
			used[k] = true
			continue
		}
		bad = append(bad, h.pos+": "+h.what+" (in "+h.decl+"): only internal/credhttp builds plumbing's clients (plumbing_clients.go, #1359)")
	}
	sort.Strings(bad)
	for _, b := range dedupe(bad) {
		t.Error(b)
	}
	for k := range fenceAllowed {
		if !used[k] {
			t.Errorf("stale fence exemption %v: it matches nothing; remove it", k)
		}
	}

	// Non-vacuity and completeness, from credhttp itself, which must refer to
	// the builders it hands out.
	var credhttp *packages.Package
	for _, p := range pkgs {
		if p.PkgPath == modulePath+"/internal/credhttp" {
			credhttp = p
			break
		}
	}
	if credhttp == nil {
		t.Fatal("NON-VACUITY: internal/credhttp was not loaded")
	}
	seen := map[string]bool{}
	for _, obj := range credhttp.TypesInfo.Uses {
		if what, ok := fenced(obj); ok {
			seen[what] = true
		}
	}
	for _, want := range []string{"dialguard.Client", "dialguard.Transport", "request.HTTPClientForEndpoint", "jwtutil.NewJWKSKeySource", "jwtutil.WithJWKSHTTPClient"} {
		if !seen[want] {
			t.Errorf("NON-VACUITY: credhttp's references do not include the fenced %s (saw %v): the classification is blind to it", want, seen)
		}
	}
	for _, imp := range credhttp.Types.Imports() {
		if imp.Path() != importRequest {
			continue
		}
		scope := imp.Scope()
		for _, name := range scope.Names() {
			f, ok := scope.Lookup(name).(*types.Func)
			if !ok || !f.Exported() {
				continue
			}
			if _, allowed := requestFuncsAllowed[name]; allowed == fencedFunc(f) {
				t.Errorf("plumbing/http/request.%s is classified neither as fenced nor as allowed (or as both): add it to requestFuncsAllowed or sendsInside", name)
			}
		}
	}
}

func dedupe(xs []string) []string {
	var out []string
	for i, x := range xs {
		if i == 0 || x != xs[i-1] {
			out = append(out, x)
		}
	}
	return out
}

// fenceShapeFixture is type-checked as a package inside the module, outside
// credhttp, through an overlay. Every top-level declaration named bad* must be
// flagged, every one named ok* must not. The first six are the shapes the
// #1359 re-review used to get past the AST walk this fence replaced.
const fenceShapeFixture = `package fencefixture

import (
	"net/http"

	"github.com/krateo-platformops/plumbing/dialguard"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/jwtutil"
)

// 1. a package-level var
var badPackageVar = dialguard.Client(0)

// 2. a package-level func literal
var badFuncLiteral = func() *http.Client { return dialguard.Client(0) }

// 3. a client literal on plumbing's transport
var badTransportLiteral = &http.Client{Transport: dialguard.Transport()}

// 4. a decoy: a client is built and something else is handed out
func badDecoy() *http.Client {
	c, _ := request.HTTPClientForEndpoint(nil, nil)
	_ = c
	return &http.Client{}
}

// 5. a function-value alias
func badAlias() *http.Client {
	mk := dialguard.Client
	return mk(0)
}

// 6. a nil client, which falls back to plumbing's own
var badJWKSNilClient = jwtutil.WithJWKSHTTPClient(nil)

var badJWKSDefaultClient = jwtutil.NewJWKSKeySource("https://authn.invalid/.well-known/jwks.json")
var badDoValue = request.Do
var badEndpointBuilderValue = request.HTTPClientForEndpoint

var okRequestType request.RequestInfo
var okSigner = request.ComputeAwsSignature
var okJWKSOption = jwtutil.WithJWKSCacheTTL(0)
var okJWKSURL = jwtutil.JWKSURL("https://authn.invalid")
var okValidate = jwtutil.ValidateWithKeySource
`

func TestIssue1359_FenceCatchesEveryShape(t *testing.T) {
	root, err := filepath.Abs(moduleRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "internal", "credhttp", "internal", "fencefixture")
	pkgs := loadTyped(t, root, map[string][]byte{filepath.Join(dir, "x.go"): []byte(fenceShapeFixture)}, nil, "./internal/credhttp/internal/fencefixture")
	if len(pkgs) != 1 || pkgs[0].Types == nil {
		t.Fatalf("fence fixture did not load: %v", pkgs)
	}
	flagged := map[string]bool{}
	for _, h := range fenceScan(pkgs, modulePathOf(t, root)) {
		flagged[h.decl] = true
	}
	if !flagged["import"] {
		t.Error("the fence did not flag the import of plumbing/dialguard")
	}
	nBad := 0
	for _, name := range pkgs[0].Types.Scope().Names() {
		switch {
		case strings.HasPrefix(name, "bad"):
			nBad++
			if !flagged[name] {
				t.Errorf("the fence did not flag %s", name)
			}
		case strings.HasPrefix(name, "ok") && flagged[name]:
			t.Errorf("the fence flagged the allowed %s", name)
		}
	}
	if nBad != 9 {
		t.Errorf("fixture scope lists %d bad* cases, want 9: the fixture did not load whole", nBad)
	}
}
