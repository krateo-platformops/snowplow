package credhttp

// ratelimit_920_typed_guard_test.go — #920 structural gate, the typed half.
//
// A *rest.Config built from nothing never had noClientRateLimit applied, so
// every client built from it gets client-go's 5/s limiter. guard_test.go
// refuses the syntactic shapes (a rest.Config literal, new, a zero value, and
// the constructors it names). This gate closes the rest by TYPE: outside
// credhttp, no non-test code may refer to a function (or a function-typed
// variable or field) defined outside this module whose results are, or
// contain, a rest.Config. That covers every third-party config source by what
// it returns, not by its name: clientcmd.RESTConfigFromKubeConfig,
// clientcmd's ClientConfig interface method, rest.AnonymousClientConfig,
// dynamic.ConfigFor, plumbing's kubeconfig.NewClientConfig, and any added
// later. A reference rather than a call is checked, so taking such a function
// as a value (var f = clientcmd.RESTConfigFromKubeConfig) is refused too.
//
// Allowed: rest.CopyConfig (a copy keeps the no-limiter setting of the config
// it copies) and anything defined in this module. A module function that
// returns a *rest.Config is allowed because its own body is checked by this
// gate and by guard_test.go: it can only hand on a config that came from
// credhttp, a copy of one, or a shape one of the two gates refuses.

import (
	"bufio"
	"go/ast"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const restPkg = "k8s.io/client-go/rest"

// restConfigIn reports whether t is, or contains, rest.Config: through
// pointers, slices, arrays, maps, channels, struct fields (exported or not)
// and result tuples. A function or interface type does not contain the values
// it would produce; a function-typed reference is judged by its own results.
func restConfigIn(t types.Type) bool {
	return restConfigInSeen(t, map[types.Type]bool{})
}

func restConfigInSeen(t types.Type, seen map[types.Type]bool) bool {
	t = types.Unalias(t)
	if seen[t] {
		return false
	}
	seen[t] = true
	switch x := t.(type) {
	case *types.Named:
		if o := x.Obj(); o.Pkg() != nil && o.Pkg().Path() == restPkg && o.Name() == "Config" {
			return true
		}
		return restConfigInSeen(x.Underlying(), seen)
	case *types.Pointer:
		return restConfigInSeen(x.Elem(), seen)
	case *types.Slice:
		return restConfigInSeen(x.Elem(), seen)
	case *types.Array:
		return restConfigInSeen(x.Elem(), seen)
	case *types.Chan:
		return restConfigInSeen(x.Elem(), seen)
	case *types.Map:
		return restConfigInSeen(x.Key(), seen) || restConfigInSeen(x.Elem(), seen)
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if restConfigInSeen(x.Field(i).Type(), seen) {
				return true
			}
		}
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			if restConfigInSeen(x.At(i).Type(), seen) {
				return true
			}
		}
	}
	return false
}

// returnsRESTConfig reports whether obj is a function, or a function-typed
// variable or field, whose results are or contain rest.Config.
//
// It judges the GENERIC ORIGIN of obj, not its instantiation: a generic
// container (atomic.Pointer[T].Load, a typed cache) whose result is a
// rest.Config only through its type argument hands back a value the caller
// stored, so the config came from this module, not from the container.
func returnsRESTConfig(obj types.Object) bool {
	switch o := obj.(type) {
	case *types.Func:
		obj = o.Origin()
	case *types.Var:
		obj = o.Origin()
	default:
		return false
	}
	sig, ok := obj.Type().Underlying().(*types.Signature)
	return ok && restConfigIn(sig.Results())
}

type typedHit struct {
	pos  string // file:line:col
	fn   string // enclosing top-level declaration
	what string // pkg.Name of the referenced object
}

// typedConfigSources returns every reference, in pkgs outside credhttp, to an
// object defined outside modulePath that returns a rest.Config, except
// rest.CopyConfig. allowedSeen counts the rest.CopyConfig references it let
// through (the non-vacuity witness: the scan does see such calls).
func typedConfigSources(pkgs []*packages.Package, modulePath string) (hits []typedHit, allowedSeen int) {
	credhttpPath := modulePath + "/internal/credhttp"
	for _, p := range pkgs {
		if p.PkgPath == credhttpPath || p.TypesInfo == nil {
			continue
		}
		for _, f := range p.Syntax {
			for _, d := range f.Decls {
				encl := declName(d)
				ast.Inspect(d, func(n ast.Node) bool {
					id, ok := n.(*ast.Ident)
					if !ok {
						return true
					}
					obj := p.TypesInfo.Uses[id]
					if obj == nil || obj.Pkg() == nil {
						return true
					}
					path := obj.Pkg().Path()
					if path == modulePath || strings.HasPrefix(path, modulePath+"/") || !returnsRESTConfig(obj) {
						return true
					}
					if path == restPkg && obj.Name() == "CopyConfig" {
						allowedSeen++
						return true
					}
					hits = append(hits, typedHit{
						pos:  p.Fset.Position(id.Pos()).String(),
						fn:   encl,
						what: obj.Pkg().Name() + "." + obj.Name(),
					})
					return true
				})
			}
		}
	}
	sort.Slice(hits, func(i, j int) bool { return hits[i].pos < hits[j].pos })
	return hits, allowedSeen
}

func declName(d ast.Decl) string {
	switch d := d.(type) {
	case *ast.FuncDecl:
		return d.Name.Name
	case *ast.GenDecl:
		for _, sp := range d.Specs {
			switch sp := sp.(type) {
			case *ast.ValueSpec:
				if len(sp.Names) > 0 {
					return sp.Names[0].Name
				}
			case *ast.TypeSpec:
				return sp.Name.Name
			}
		}
	}
	return ""
}

// modulePathOf reads the module path from root/go.mod.
func modulePathOf(t *testing.T, root string) string {
	t.Helper()
	f, err := os.Open(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(strings.TrimSpace(sc.Text()), "module "); ok {
			return strings.TrimSpace(rest)
		}
	}
	t.Fatal("no module line in go.mod")
	return ""
}

func loadTyped(t *testing.T, root string, overlay map[string][]byte, patterns ...string) []*packages.Package {
	t.Helper()
	cfg := &packages.Config{
		Mode:    packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:     root,
		Overlay: overlay,
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pkgs {
		for _, e := range p.Errors {
			t.Fatalf("load %s: %v", p.PkgPath, e)
		}
	}
	return pkgs
}

func TestIssue920_NoRESTConfigFromOutsideTheModuleOutsideCredhttp(t *testing.T) {
	if testing.Short() {
		t.Skip("type-checks the whole module")
	}
	root, err := filepath.Abs(moduleRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	pkgs := loadTyped(t, root, nil, "./...")
	if len(pkgs) < 20 {
		t.Fatalf("NON-VACUITY: only %d packages loaded", len(pkgs))
	}
	hits, copies := typedConfigSources(pkgs, modulePathOf(t, root))
	for _, h := range hits {
		t.Errorf("%s: %s (in %s) returns a *rest.Config built outside internal/credhttp; it never had noClientRateLimit applied (#920)", h.pos, h.what, h.fn)
	}
	// impersonation.go and streaming_list.go copy a credhttp config today; a
	// scan that sees none of them is not looking at the calls.
	if copies == 0 {
		t.Errorf("NON-VACUITY: the typed scan saw no rest.CopyConfig reference; it is blind to calls that return a *rest.Config")
	}
}

// typedShapeFixture is type-checked as a package inside the module (through
// an overlay, nothing is written to disk). Every function named bad* must be
// flagged, every function named ok* must not.
const typedShapeFixture = `package typedfixture

import (
	"context"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/kubeconfig"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func badRESTConfigFromKubeConfig(b []byte) { _, _ = clientcmd.RESTConfigFromKubeConfig(b) }

var badFuncValue = clientcmd.RESTConfigFromKubeConfig

func badInterfaceMethod(cc clientcmd.ClientConfig) { _, _ = cc.ClientConfig() }

func badBuildConfigFromFlags() { _, _ = clientcmd.BuildConfigFromFlags("", "") }

func badPlumbingKubeconfig(ep endpoints.Endpoint) { _, _ = kubeconfig.NewClientConfig(context.Background(), ep) }

func badAnonymous(rc *rest.Config) { _ = rest.AnonymousClientConfig(rc) }

func badConfigFor(rc *rest.Config) { _ = dynamic.ConfigFor(rc) }

func badInCluster() { _, _ = rest.InClusterConfig() }

func okCopyConfig(rc *rest.Config) { _ = rest.CopyConfig(rc) }

func okCredhttp() { _, _ = credhttp.InClusterConfig() }

func local() *rest.Config { return nil }

func okModuleInternal() { _ = local() }

func okRESTClientMethod(c *rest.RESTClient) { _ = c.Get() }

func okGenericContainer(p *atomic.Pointer[holder]) { _ = p.Load() }

type holder struct{ rc *rest.Config }

var (
	containsStruct holder
	containsNested []map[string]*holder
	containsFunc   func() *rest.Config
)
`

func typedShapeCases(t *testing.T) {
	t.Helper()
	root, err := filepath.Abs(moduleRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "internal", "credhttp", "internal", "typedfixture")
	pkgs := loadTyped(t, root, map[string][]byte{filepath.Join(dir, "x.go"): []byte(typedShapeFixture)}, "./internal/credhttp/internal/typedfixture")
	if len(pkgs) != 1 || pkgs[0].Types == nil {
		t.Fatalf("typed fixture did not load: %v", pkgs)
	}
	hits, copies := typedConfigSources(pkgs, modulePathOf(t, root))
	flagged := map[string]bool{}
	for _, h := range hits {
		flagged[h.fn] = true
	}
	scope := pkgs[0].Types.Scope()
	for _, name := range scope.Names() {
		switch {
		case strings.HasPrefix(name, "bad") && !flagged[name]:
			t.Errorf("typed guard did not flag %s", name)
		case strings.HasPrefix(name, "ok") && flagged[name]:
			t.Errorf("typed guard flagged the allowed %s", name)
		}
	}
	if copies != 1 {
		t.Errorf("typed guard counted %d rest.CopyConfig references in the fixture, want 1", copies)
	}
	for name, want := range map[string]bool{"containsStruct": true, "containsNested": true, "containsFunc": false} {
		if got := restConfigIn(scope.Lookup(name).Type()); got != want {
			t.Errorf("restConfigIn(%s) = %v, want %v", name, got, want)
		}
	}
}
