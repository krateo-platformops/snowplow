package credhttp

// plumbing_client_fence_1359_test.go — #1359: only credhttp builds the clients
// plumbing builds, and hands each out (plumbing_clients.go, do.go, send.go), so
// every client snowplow sends on is on plumbing's guarded path
// (dialguard.RoundTripper, which on js/wasm sets fetch's redirect mode "error"
// and credentials mode "omit"). A client built anywhere else could skip it.
//
// The fence, type-checked over every build of the module (loadWholeModule: the
// server build, rbac_narrow_baseline and js/wasm), on every non-test file,
// test support included. Outside internal/credhttp nothing may refer to:
//   - anything in plumbing/dialguard, the package that builds plumbing's
//     transport and clients (a blank import of it included);
//   - a function or method of a plumbing package that imports dialguard
//     (clientPackages) that builds, takes or returns an *http.Client, an
//     http.RoundTripper or an *http.Transport, or that sends on a client it
//     builds inside (sendsInside);
//   - a literal or new() of such a package's type that holds a client
//     (util.RetryClient{}, jwtutil.JWKSKeySource{}).
//
// A reference is any use of the name: a call, a package-level var, a func
// literal, a function or method value (mk := dialguard.Client), a nil
// argument. Types themselves stay usable (request.RequestInfo, ...).
//
// A plumbing bump cannot open a hole unseen: every exported function and
// method of every clientPackages package is classified, fenced or allowed with
// a reason, and the linked plumbing packages that import dialguard must be
// exactly clientPackages. A new jwtutil.FetchJWKS(url) with a client inside,
// or a new package that imports dialguard, fails here until it is classified.

import (
	"go/ast"
	"go/types"
	"os"
	"os/exec"
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
	importHTTPUtil  = "github.com/krateo-platformops/plumbing/http/util"
	plumbingModule  = "github.com/krateo-platformops/plumbing"
)

// clientPackages are the linked plumbing packages, other than dialguard, that
// import dialguard: each builds clients on it somewhere. TestIssue1359_Only...
// checks the set against `go list -deps` of every build.
var clientPackages = map[string]bool{importRequest: true, importJWTUtil: true, importHTTPUtil: true}

// sendsInside are the fenced functions and methods whose signature shows no
// client: they build one inside and send on it. Keyed as plumbingKey.
var sendsInside = map[string]bool{
	importRequest + ".Do":              true, // HTTPClientForEndpoint, then sends
	importHTTPUtil + ".RetryClient.Do": true, // a nil Client becomes dialguard.Client(0)
}

// plumbingAllowed are the exported functions and methods of clientPackages
// that neither build a client nor send on one they built: usable anywhere.
// Keyed as plumbingKey. Each must still exist.
var plumbingAllowed = map[string]string{
	importRequest + ".ComputeAwsHeaders":   "computes the AWS signature headers; sends nothing (external_fetch.go adds them to a request credhttp sends)",
	importRequest + ".ComputeAwsSignature": "computes the AWS signature; sends nothing",

	importJWTUtil + ".CreateToken":                 "signs a token; sends nothing",
	importJWTUtil + ".ExtractUserInfo":             "decodes a token; sends nothing",
	importJWTUtil + ".JWKSURL":                     "joins a URL; sends nothing",
	importJWTUtil + ".NewStaticKeySource":          "a key source over one key it is given; no client",
	importJWTUtil + ".ParseRSAPrivateKeyFromPEM":   "parses a key; sends nothing",
	importJWTUtil + ".ParseRSAPublicKeyFromPEM":    "parses a key; sends nothing",
	importJWTUtil + ".Validate":                    "verifies against a key it is given; sends nothing",
	importJWTUtil + ".ValidateWithKeySource":       "asks a KeySource for the key; the only fetching one, a JWKSKeySource, can be built only by credhttp (NewJWKSKeySource and its literal are fenced)",
	importJWTUtil + ".ValidateClaimsWithKeySource": "as ValidateWithKeySource",
	importJWTUtil + ".WithJWKSCacheTTL":            "a key-source option that sets no client",
	importJWTUtil + ".WithJWKSMinRefreshInterval":  "a key-source option that sets no client",
	importJWTUtil + ".WithJWKSRequestTimeout":      "a key-source option that sets the timeout of the client the source already has",
	importJWTUtil + ".JWKSKeySource.PublicKey":     "fetches with the source's own client, which credhttp built: a JWKSKeySource can be built only by credhttp (NewJWKSKeySource and its literal are fenced)",
	importJWTUtil + ".StaticKeySource.PublicKey":   "returns the key it holds; sends nothing",
	importJWTUtil + ".UserInfo.Actor":              "reads a decoded claim; sends nothing",
	importJWTUtil + ".UserInfo.ActorPrincipal":     "reads a decoded claim; sends nothing",
	importJWTUtil + ".UserInfo.DelegationChain":    "reads a decoded claim; sends nothing",
	importJWTUtil + ".Principal.Known":             "reads a decoded claim; sends nothing",
	importJWTUtil + ".Principal.UnmarshalJSON":     "decodes a claim; sends nothing",
}

// fenceAllowed: (package path relative to the module, what) → reason. Each
// must still match.
var fenceAllowed = map[[2]string]string{
	{"internal/testsupport/jsfetch", "dialguard.Transport"}: "js/wasm test support, imported only by tests: the probe that observes whether net/http sends dialguard.Transport through fetch. It builds no client snowplow sends on.",
}

// plumbingKey is f's key in sendsInside and plumbingAllowed: the import path,
// the receiver's type name for a method, and the name.
func plumbingKey(f *types.Func) string {
	key := f.Pkg().Path() + "."
	if recv := f.Type().(*types.Signature).Recv(); recv != nil {
		t := recv.Type()
		if p, ok := t.(*types.Pointer); ok {
			t = p.Elem()
		}
		if n, ok := t.(*types.Named); ok {
			key += n.Obj().Name() + "."
		}
	}
	return key + f.Name()
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

// fencedFunc reports whether f, a function or method of a clientPackages
// package, is fenced to credhttp: it is in sendsInside, or a parameter or a
// result (not the receiver) holds a client.
func fencedFunc(f *types.Func) bool {
	if sendsInside[plumbingKey(f)] {
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

// shortName is obj as the fence reports it: "dialguard.Client",
// "util.RetryClient.Do".
func shortName(obj types.Object) string {
	if f, ok := obj.(*types.Func); ok {
		return obj.Pkg().Name() + strings.TrimPrefix(plumbingKey(f), f.Pkg().Path())
	}
	return obj.Pkg().Name() + "." + obj.Name()
}

// fenced reports whether a use of obj is fenced to credhttp, and names it.
func fenced(obj types.Object) (string, bool) {
	if obj == nil || obj.Pkg() == nil {
		return "", false
	}
	switch path := obj.Pkg().Path(); {
	case path == importDialguard:
		return shortName(obj), true
	case clientPackages[path]:
		f, ok := obj.(*types.Func)
		if !ok || !fencedFunc(f) {
			return "", false
		}
		return shortName(obj), true
	}
	return "", false
}

// fencedLiteral reports whether a value of type t built outside credhttp (a
// composite literal, new) is fenced: a type of dialguard's or of a
// clientPackages package that holds a client.
func fencedLiteral(t types.Type) (string, bool) {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := t.(*types.Named)
	if !ok || n.Obj().Pkg() == nil {
		return "", false
	}
	path := n.Obj().Pkg().Path()
	if (path == importDialguard || clientPackages[path]) && holdsClient(n, 0) {
		return "a " + n.Obj().Pkg().Name() + "." + n.Obj().Name() + " built outside credhttp", true
	}
	return "", false
}

type fenceHit struct {
	pkg, decl, what, pos string
}

// fenceScan returns every fenced reference, literal and blank import in pkgs
// outside credhttp.
func fenceScan(pkgs []*packages.Package, modulePath string) []fenceHit {
	credhttpPath := modulePath + "/internal/credhttp"
	var out []fenceHit
	for _, p := range pkgs {
		if p.PkgPath == credhttpPath || p.TypesInfo == nil {
			continue
		}
		rel := strings.TrimPrefix(strings.TrimPrefix(p.PkgPath, modulePath), "/")
		add := func(decl, what string, pos interface{ String() string }) {
			out = append(out, fenceHit{rel, decl, what, pos.String()})
		}
		for _, f := range p.Syntax {
			// Every other import of dialguard is caught through what it
			// refers to (an unused import does not compile); a blank one
			// refers to nothing.
			for _, im := range f.Imports {
				if path, _ := strconv.Unquote(im.Path.Value); path == importDialguard && im.Name != nil && im.Name.Name == "_" {
					add("import", "blank import of plumbing/dialguard", p.Fset.Position(im.Pos()))
				}
			}
			for _, d := range f.Decls {
				decl := declName(d)
				ast.Inspect(d, func(n ast.Node) bool {
					switch x := n.(type) {
					case *ast.Ident:
						if what, bad := fenced(p.TypesInfo.Uses[x]); bad {
							add(decl, what, p.Fset.Position(x.Pos()))
						}
					case *ast.CompositeLit:
						if tv, ok := p.TypesInfo.Types[x]; ok {
							if what, bad := fencedLiteral(tv.Type); bad {
								add(decl, what, p.Fset.Position(x.Pos()))
							}
						}
					case *ast.CallExpr:
						if id, ok := x.Fun.(*ast.Ident); ok && len(x.Args) == 1 {
							if _, builtin := p.TypesInfo.Uses[id].(*types.Builtin); builtin && id.Name == "new" {
								if tv, ok := p.TypesInfo.Types[x.Args[0]]; ok {
									if what, bad := fencedLiteral(tv.Type); bad {
										add(decl, what, p.Fset.Position(x.Pos()))
									}
								}
							}
						}
					}
					return true
				})
			}
		}
	}
	return out
}

// plumbingPackagesImportingDialguard is every plumbing package, other than
// dialguard, that the module's non-test code links in build b and that imports
// dialguard (go list -deps).
func plumbingPackagesImportingDialguard(t *testing.T, root string, b moduleBuild) map[string]bool {
	t.Helper()
	args := append([]string{"list", "-deps", "-f", "{{.ImportPath}} {{join .Imports \",\"}}"}, b.flags...)
	cmd := exec.Command("go", append(args, "./...")...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(), b.env...)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list %v (env %v): %v", args, b.env, err)
	}
	got := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		path, imports, _ := strings.Cut(line, " ")
		if path == importDialguard || !strings.HasPrefix(path, plumbingModule+"/") {
			continue
		}
		for _, imp := range strings.Split(imports, ",") {
			if imp == importDialguard {
				got[path] = true
			}
		}
	}
	return got
}

func TestIssue1359_OnlyCredhttpBuildsPlumbingClients(t *testing.T) {
	if testing.Short() {
		t.Skip("type-checks the whole module")
	}
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("no go command on PATH")
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
		k := [2]string{h.pkg, h.what}
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

	// The linked plumbing packages that build clients on dialguard are exactly
	// clientPackages, in every build.
	for _, b := range moduleTagSets {
		got := plumbingPackagesImportingDialguard(t, root, b)
		for path := range got {
			if !clientPackages[path] {
				t.Errorf("build %v: the linked plumbing package %s imports dialguard and is not in clientPackages: classify its functions and methods", b, path)
			}
		}
		if b.env == nil && b.flags == nil {
			for path := range clientPackages {
				if !got[path] {
					t.Errorf("clientPackages lists %s, which the server build does not link or which no longer imports dialguard: remove it", path)
				}
			}
		}
	}

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
	// Non-vacuity: credhttp refers to the builders it hands out.
	seen := map[string]bool{}
	for _, obj := range credhttp.TypesInfo.Uses {
		if what, ok := fenced(obj); ok {
			seen[what] = true
		}
	}
	for _, want := range []string{"dialguard.Client", "dialguard.Transport", "request.HTTPClientForEndpoint", "jwtutil.NewJWKSKeySource", "jwtutil.WithJWKSHTTPClient", "util.NewRetryClient"} {
		if !seen[want] {
			t.Errorf("NON-VACUITY: credhttp's references do not include the fenced %s (saw %v): the classification is blind to it", want, seen)
		}
	}

	// Completeness: every exported function and method of every
	// clientPackages package is fenced or allowed, never both, never neither.
	found := map[string]bool{}
	plumbingAllowedLeft := map[string]bool{}
	for k := range plumbingAllowed {
		plumbingAllowedLeft[k] = true
	}
	for _, imp := range allImports(credhttp.Types) {
		if !clientPackages[imp.Path()] {
			continue
		}
		found[imp.Path()] = true
		for _, f := range exportedFuncsAndMethods(imp) {
			key := plumbingKey(f)
			_, allowed := plumbingAllowed[key]
			switch fenced := fencedFunc(f); {
			case allowed && fenced:
				t.Errorf("%s is both fenced and in plumbingAllowed: remove it from plumbingAllowed", key)
			case !allowed && !fenced:
				t.Errorf("%s is classified neither as fenced nor as allowed: add it to plumbingAllowed with a reason, or to sendsInside", key)
			}
			delete(plumbingAllowedLeft, key)
		}
	}
	for path := range clientPackages {
		if !found[path] {
			t.Errorf("NON-VACUITY: %s is not reachable from credhttp's imports, so its functions were not classified", path)
		}
	}
	for key := range plumbingAllowedLeft {
		t.Errorf("stale plumbingAllowed entry %s: no such exported function or method", key)
	}
}

// allImports is every package pkg imports, directly or not.
func allImports(pkg *types.Package) []*types.Package {
	seen := map[string]bool{}
	var out []*types.Package
	var walk func(p *types.Package)
	walk = func(p *types.Package) {
		for _, imp := range p.Imports() {
			if !seen[imp.Path()] {
				seen[imp.Path()] = true
				out = append(out, imp)
				walk(imp)
			}
		}
	}
	walk(pkg)
	return out
}

// exportedFuncsAndMethods is every exported package-level function of pkg and
// every exported method (pointer method set) of its exported named types.
func exportedFuncsAndMethods(pkg *types.Package) []*types.Func {
	var out []*types.Func
	scope := pkg.Scope()
	for _, name := range scope.Names() {
		switch o := scope.Lookup(name).(type) {
		case *types.Func:
			if o.Exported() {
				out = append(out, o)
			}
		case *types.TypeName:
			if !o.Exported() {
				continue
			}
			ms := types.NewMethodSet(types.NewPointer(o.Type()))
			for i := 0; i < ms.Len(); i++ {
				if f, ok := ms.At(i).Obj().(*types.Func); ok && f.Exported() && f.Pkg() == pkg {
					out = append(out, f)
				}
			}
		}
	}
	return out
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
	"github.com/krateo-platformops/plumbing/http/util"
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
var badRetryClient = util.NewRetryClient(nil)
var badRetryClientLiteral = &util.RetryClient{}
var badRetryClientNew = new(util.RetryClient)
var badJWKSKeySourceLiteral = &jwtutil.JWKSKeySource{}
func badRetryDo(rc *util.RetryClient) { _, _ = rc.Do(nil) }

var okRequestType request.RequestInfo
var okSigner = request.ComputeAwsSignature
var okJWKSOption = jwtutil.WithJWKSCacheTTL(0)
var okJWKSURL = jwtutil.JWKSURL("https://authn.invalid")
var okValidate = jwtutil.ValidateWithKeySource
func okPublicKey(s *jwtutil.JWKSKeySource) { _, _ = s.PublicKey("") }
`

// fenceBlankImportFixture is a second file of the fixture package: a blank
// import refers to nothing, so the fence flags the import itself.
const fenceBlankImportFixture = `package fencefixture

import _ "github.com/krateo-platformops/plumbing/dialguard"
`

func TestIssue1359_FenceCatchesEveryShape(t *testing.T) {
	root, err := filepath.Abs(moduleRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "internal", "credhttp", "internal", "fencefixture")
	pkgs := loadTyped(t, root, map[string][]byte{
		filepath.Join(dir, "x.go"): []byte(fenceShapeFixture),
		filepath.Join(dir, "y.go"): []byte(fenceBlankImportFixture),
	}, nil, "./internal/credhttp/internal/fencefixture")
	if len(pkgs) != 1 || pkgs[0].Types == nil {
		t.Fatalf("fence fixture did not load: %v", pkgs)
	}
	flagged := map[string]bool{}
	for _, h := range fenceScan(pkgs, modulePathOf(t, root)) {
		flagged[h.decl] = true
	}
	if !flagged["import"] {
		t.Error("the fence did not flag the blank import of plumbing/dialguard")
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
	if nBad != 14 {
		t.Errorf("fixture scope lists %d bad* cases, want 14: the fixture did not load whole", nBad)
	}
}
