package credhttp

// ratelimit_920_typed_guard_test.go — #920 structural gate, the typed half.
//
// The guarantee: every client built from a *rest.Config is built by
// credhttp, and every credhttp builder applies noClientRateLimit to whatever
// config it is handed (proven per builder by the behavioural test). Then where
// a config came from does not matter. Rule 4 is what makes that hold. Rules
// 1-3 refuse configs built from nothing, as defence in depth: a config built
// that way never had noClientRateLimit applied (nor #723's redirect refusal).
// They cannot see every allocation. Reflection and decoders can make one
// through a pointer-typed path (reflect.New over a *rest.Config's Elem,
// json.Unmarshal into a struct with a *rest.Config field), and a module
// function CAN create a config that way. Rule 4 makes such a config harmless:
// nothing outside credhttp can turn it into a client.
//
// All four rules are TYPED. They run over every non-test file of the module
// outside credhttp, under every build-tag set the module uses:
//
//  1. BY VALUE. No type expression and no generic type argument (inferred or
//     explicit) may contain a rest.Config by value: restConfigIn without the
//     pointer step. This covers a literal, new, make, a variable, an array or
//     map element, a field, a conversion, a generic that makes a T, and
//     reflect.TypeFor[T]. *rest.Config and a *rc dereference stay legal. A
//     composite literal whose type is elided (the {..} inside
//     []*rest.Config{{..}}) is judged by its own type.
//  2. NO ADDRESS OF A CONFIG POINTER. &x, where x is a *rest.Config (or
//     reaches one through slices, arrays, maps or pointers): json.Unmarshal(b,
//     &rc).
//  3. NO THIRD-PARTY SOURCE. No reference to a function, or a function-typed
//     variable or field, defined outside this module whose declared results
//     are or contain a rest.Config, except rest.CopyConfig:
//     clientcmd.RESTConfigFromKubeConfig, clientcmd's ClientConfig method,
//     rest.AnonymousClientConfig, dynamic.ConfigFor, plumbing's
//     kubeconfig.NewClientConfig, called or taken as a value.
//  4. NO THIRD-PARTY CONSUMER. No reference to a function, or a
//     function-typed variable or field, defined outside this module whose
//     declared parameters or receiver contain a rest.Config (through pointers
//     and struct fields), except rest.CopyConfig. Only credhttp turns a config
//     into a client: client-go's constructors, plumbing's endpoints.FromSecret
//     (use credhttp.FromSecret) and methods on rest.Config itself are refused.
//
// Rules 3 and 4 judge a generic by what its OWN code declares (its origin): a
// parameter or result that is a config only through a type parameter is the
// caller's value (atomic.Pointer[T].Store / Load), and an instantiation whose
// type argument holds a config by value is refused by rule 1.

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

const restPkg = "k8s.io/client-go/rest"

func isRESTConfig(t types.Type) bool {
	n, ok := types.Unalias(t).(*types.Named)
	if !ok {
		return false
	}
	o := n.Obj()
	return o.Pkg() != nil && o.Pkg().Path() == restPkg && o.Name() == "Config"
}

// restConfigIn reports whether t is, or contains, rest.Config: through
// pointers, slices, arrays, maps, channels, struct fields (exported or not)
// and result tuples. A function or interface type does not contain the values
// it would produce; a function-typed reference is judged by its own results.
func restConfigIn(t types.Type) bool {
	return restConfigWalk(t, true, map[types.Type]bool{})
}

// restConfigByValue is restConfigIn without the pointer step: whether a value
// of type t holds a rest.Config itself rather than a pointer to one.
func restConfigByValue(t types.Type) bool {
	return restConfigWalk(t, false, map[types.Type]bool{})
}

func restConfigWalk(t types.Type, throughPointers bool, seen map[types.Type]bool) bool {
	t = types.Unalias(t)
	if seen[t] {
		return false
	}
	seen[t] = true
	if isRESTConfig(t) {
		return true
	}
	switch x := t.(type) {
	case *types.Named:
		return restConfigWalk(x.Underlying(), throughPointers, seen)
	case *types.Pointer:
		return throughPointers && restConfigWalk(x.Elem(), throughPointers, seen)
	case *types.Slice:
		return restConfigWalk(x.Elem(), throughPointers, seen)
	case *types.Array:
		return restConfigWalk(x.Elem(), throughPointers, seen)
	case *types.Chan:
		return restConfigWalk(x.Elem(), throughPointers, seen)
	case *types.Map:
		return restConfigWalk(x.Key(), throughPointers, seen) || restConfigWalk(x.Elem(), throughPointers, seen)
	case *types.Struct:
		for i := 0; i < x.NumFields(); i++ {
			if restConfigWalk(x.Field(i).Type(), throughPointers, seen) {
				return true
			}
		}
	case *types.Tuple:
		for i := 0; i < x.Len(); i++ {
			if restConfigWalk(x.At(i).Type(), throughPointers, seen) {
				return true
			}
		}
	}
	return false
}

// reachesConfigPointer reports whether t is a *rest.Config, or reaches one
// through slices, arrays, maps and pointers (not struct fields: &opts of a
// struct that carries an rc is how the module passes options).
func reachesConfigPointer(t types.Type) bool {
	for {
		switch x := types.Unalias(t).(type) {
		case *types.Pointer:
			if isRESTConfig(x.Elem()) {
				return true
			}
			t = x.Elem()
		case *types.Slice:
			t = x.Elem()
		case *types.Array:
			t = x.Elem()
		case *types.Map:
			t = x.Elem()
		default:
			return false
		}
	}
}

// returnsRESTConfig reports whether obj is a function, or a function-typed
// variable or field, whose OWN declared results (its generic origin) are or
// contain rest.Config.
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

// consumesRESTConfig reports whether obj is a function, or a function-typed
// variable or field, whose OWN declared parameters or receiver (its generic
// origin) are or contain rest.Config.
func consumesRESTConfig(obj types.Object) bool {
	switch o := obj.(type) {
	case *types.Func:
		obj = o.Origin()
	case *types.Var:
		obj = o.Origin()
	default:
		return false
	}
	sig, ok := obj.Type().Underlying().(*types.Signature)
	if !ok {
		return false
	}
	if r := sig.Recv(); r != nil && restConfigIn(r.Type()) {
		return true
	}
	return restConfigIn(sig.Params())
}

type typedHit struct {
	pos  string // file:line:col
	fn   string // enclosing top-level declaration
	what string
}

// typedConfigSources applies the three rules to pkgs, skipping credhttp.
// allowedSeen counts the rest.CopyConfig references rule 3 let through (the
// non-vacuity witness: the scan does see calls that return a *rest.Config).
func typedConfigSources(pkgs []*packages.Package, modulePath string) (hits []typedHit, allowedSeen int) {
	credhttpPath := modulePath + "/internal/credhttp"
	seenHit := map[string]bool{}
	add := func(p *packages.Package, pos token.Pos, fn, what string) {
		h := typedHit{pos: p.Fset.Position(pos).String(), fn: fn, what: what}
		if k := h.pos + "|" + h.what; !seenHit[k] {
			seenHit[k] = true
			hits = append(hits, h)
		}
	}
	for _, p := range pkgs {
		if p.PkgPath == credhttpPath || p.TypesInfo == nil {
			continue
		}
		info := p.TypesInfo
		for _, f := range p.Syntax {
			for _, d := range f.Decls {
				encl := declName(d)
				var stack []ast.Node
				ast.Inspect(d, func(n ast.Node) bool {
					if n == nil {
						stack = stack[:len(stack)-1]
						return true
					}
					var parent ast.Node
					if len(stack) > 0 {
						parent = stack[len(stack)-1]
					}
					stack = append(stack, n)

					switch x := n.(type) {
					case *ast.CompositeLit:
						// Rule 1 for a literal whose type is elided: judge
						// the literal's own type (an implicit &T{} too).
						if x.Type == nil {
							if t := info.TypeOf(x); t != nil {
								if ptr, ok := types.Unalias(t).(*types.Pointer); ok {
									t = ptr.Elem()
								}
								if restConfigByValue(t) {
									add(p, x.Pos(), encl, "elided composite literal of "+types.TypeString(t, nil)+" holds a rest.Config by value")
								}
							}
						}
					case *ast.UnaryExpr:
						// Rule 2.
						if x.Op == token.AND {
							if t := info.TypeOf(x.X); t != nil && reachesConfigPointer(t) {
								add(p, x.Pos(), encl, "&x of a *rest.Config ("+types.TypeString(t, nil)+") lets a decoder or reflection allocate a config")
							}
						}
					case *ast.Ident:
						// Rule 1 for generic type arguments, inferred or not.
						if inst, ok := info.Instances[x]; ok {
							for i := 0; i < inst.TypeArgs.Len(); i++ {
								if a := inst.TypeArgs.At(i); restConfigByValue(a) {
									add(p, x.Pos(), encl, "generic "+x.Name+" instantiated with "+types.TypeString(a, nil)+", which holds a rest.Config by value")
								}
							}
						}
						// Rules 3 and 4: an object defined outside this module.
						if obj := info.Uses[x]; obj != nil && obj.Pkg() != nil {
							path := obj.Pkg().Path()
							if path != modulePath && !strings.HasPrefix(path, modulePath+"/") {
								isCopy := path == restPkg && obj.Name() == "CopyConfig"
								name := obj.Pkg().Name() + "." + obj.Name()
								if returnsRESTConfig(obj) {
									if isCopy {
										allowedSeen++
									} else {
										add(p, x.Pos(), encl, name+" returns a *rest.Config built outside internal/credhttp")
									}
								}
								if consumesRESTConfig(obj) && !isCopy {
									add(p, x.Pos(), encl, name+" takes a *rest.Config: only internal/credhttp turns a config into a client")
								}
							}
						}
					}
					// Rule 1 for type expressions. The rest.Config inside a
					// *rest.Config pointer type is not a value.
					if e, ok := n.(ast.Expr); ok {
						if tv, ok := info.Types[e]; ok && tv.IsType() && restConfigByValue(tv.Type) {
							if star, ok := parent.(*ast.StarExpr); ok && star.X == e {
								return true
							}
							add(p, e.Pos(), encl, "type "+types.TypeString(tv.Type, nil)+" holds a rest.Config by value")
							stack = stack[:len(stack)-1] // not descending: Inspect sends no nil for n
							return false
						}
					}
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

func loadTyped(t *testing.T, root string, overlay map[string][]byte, buildFlags []string, patterns ...string) []*packages.Package {
	t.Helper()
	cfg := &packages.Config{
		Mode:       packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo,
		Dir:        root,
		Overlay:    overlay,
		BuildFlags: buildFlags,
	}
	pkgs, err := packages.Load(cfg, patterns...)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pkgs {
		for _, e := range p.Errors {
			t.Fatalf("load %s (flags %v): %v", p.PkgPath, buildFlags, e)
		}
	}
	return pkgs
}

// moduleTagSets are the build-tag sets under which the module's non-test
// files are type-checked. The default set is the production build;
// rbac_narrow_baseline swaps in the *_baseline.go files. A tagged file in
// none of them that is a standalone program (//go:build ignore, run with go
// run) is loaded on its own. A file that declares nothing but imports holds
// no code to check (apis/generate.go: go:generate directives and a blank
// import of a codegen program, which no build can type-check). Any other
// non-test file still unchecked fails the test, so a new tag is added here
// rather than skipped silently (F6).
var moduleTagSets = [][]string{nil, {"-tags=rbac_narrow_baseline"}}

// importsOnly reports whether the file at path declares nothing but imports.
func importsOnly(t *testing.T, path string) bool {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range f.Decls {
		if g, ok := d.(*ast.GenDecl); !ok || g.Tok != token.IMPORT {
			return false
		}
	}
	return true
}

// loadWholeModule type-checks every non-test .go file of the module.
// The returned coverage maps each non-test file to how it was checked:
// "tags:<n>" (the first moduleTagSets entry that compiled it), "standalone"
// or "imports-only".
func loadWholeModule(t *testing.T, root string) ([]*packages.Package, map[string]string) {
	t.Helper()
	var all []*packages.Package
	covered := map[string]string{}
	note := func(pkgs []*packages.Package, how string) {
		for _, p := range pkgs {
			all = append(all, p)
			for _, f := range p.GoFiles {
				if covered[f] == "" {
					covered[f] = how
				}
			}
		}
	}
	for i, flags := range moduleTagSets {
		note(loadTyped(t, root, nil, flags, "./..."), "tags:"+strconv.Itoa(i))
	}
	var missing []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" || d.Name() == "vendor" || strings.HasPrefix(d.Name(), ".") && path != root {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go") && covered[path] == "" {
			missing = append(missing, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// A file named on its own is a package by itself, whatever its build
	// constraint: right for a standalone program, a type error for a member
	// of a multi-file package (which needs its tag set above).
	for _, f := range missing {
		if importsOnly(t, f) {
			covered[f] = "imports-only"
			continue
		}
		note(loadTyped(t, root, nil, nil, f), "standalone")
	}
	for _, f := range missing {
		if covered[f] == "" {
			t.Errorf("%s is not type-checked by the #920 typed gate: add its build tags to moduleTagSets", f)
		}
	}
	return all, covered
}

func TestIssue920_NoRESTConfigFromOutsideTheModuleOutsideCredhttp(t *testing.T) {
	if testing.Short() {
		t.Skip("type-checks the whole module")
	}
	root, err := filepath.Abs(moduleRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	pkgs, covered := loadWholeModule(t, root)
	if len(pkgs) < 20 {
		t.Fatalf("NON-VACUITY: only %d packages loaded", len(pkgs))
	}
	// F6 non-vacuity: each way of TYPE-CHECKING a file is exercised today. The
	// *_baseline.go files compile only under rbac_narrow_baseline, and
	// scripts/lint's programs only on their own. "imports-only" is not
	// required: it only accepts files, so its absence is no defect (F8).
	byHow := map[string]int{}
	for _, how := range covered {
		byHow[how]++
	}
	for _, how := range []string{"tags:0", "tags:1", "standalone"} {
		if byHow[how] == 0 {
			t.Errorf("NON-VACUITY: no non-test file was type-checked as %q (coverage %v)", how, byHow)
		}
	}
	t.Logf("non-test files type-checked: %v", byHow)
	hits, copies := typedConfigSources(pkgs, modulePathOf(t, root))
	for _, h := range hits {
		t.Errorf("%s: %s (in %s); outside internal/credhttp a config never had noClientRateLimit applied (#920)", h.pos, h.what, h.fn)
	}
	// impersonation.go and streaming_list.go copy a credhttp config today; a
	// scan that sees none of them is not looking at the calls.
	if copies == 0 {
		t.Errorf("NON-VACUITY: the typed scan saw no rest.CopyConfig reference; it is blind to calls that return a *rest.Config")
	}
}

// typedShapeFixture is type-checked as a package inside the module (through
// an overlay, nothing is written to disk). Every top-level declaration named
// bad* must be flagged, every one named ok* must not.
const typedShapeFixture = `package typedfixture

import (
	"context"
	"encoding/json"
	"reflect"
	"sync/atomic"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/kubeconfig"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	k8srest "k8s.io/client-go/rest"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Rule 3: third-party sources.
func badRESTConfigFromKubeConfig(b []byte) { _, _ = clientcmd.RESTConfigFromKubeConfig(b) }

var badFuncValue = clientcmd.RESTConfigFromKubeConfig

func badInterfaceMethod(cc clientcmd.ClientConfig) { _, _ = cc.ClientConfig() }

func badBuildConfigFromFlags() { _, _ = clientcmd.BuildConfigFromFlags("", "") }

func badPlumbingKubeconfig(ep endpoints.Endpoint) { _, _ = kubeconfig.NewClientConfig(context.Background(), ep) }

func badAnonymous(rc *rest.Config) { _ = rest.AnonymousClientConfig(rc) }

func badConfigFor(rc *rest.Config) { _ = dynamic.ConfigFor(rc) }

func badInCluster() { _, _ = rest.InClusterConfig() }

// Rule 1: a rest.Config by value.
func badLiteral() *rest.Config { return &rest.Config{Host: "h"} }

func badAliasedLiteral() k8srest.Config { return k8srest.Config{} }

func badNew() *rest.Config { return new(rest.Config) }

func badVar() *rest.Config { var c rest.Config; return &c }

type badField struct{ c rest.Config }

type badType rest.Config

type badAlias = rest.Config

func badMake() *rest.Config { cs := make([]rest.Config, 1); return &cs[0] } // H1

func badArrayVar() *rest.Config { var a [1]rest.Config; return &a[0] } // H2

func badMapLiteral() *rest.Config { m := map[string]rest.Config{}; c := m["x"]; return &c } // H3

func badReflectTypeFor() *rest.Config { // H4
	return reflect.New(reflect.TypeFor[rest.Config]()).Interface().(*rest.Config)
}

func newT[T any]() *T { return new(T) }

func badGenericNew() *rest.Config { return newT[rest.Config]() }

func badInferredInstance(cs []*rest.Config) { _ = firstOf(derefAll(cs)) }

func derefAll(cs []*rest.Config) []rest.Config { return nil }

func firstOf[T any](xs []T) T { var z T; return z }

func badElidedInSlice() []*rest.Config { return []*rest.Config{{Host: "h"}} }

func badElidedInMap() map[string]*rest.Config { return map[string]*rest.Config{"a": {Host: "h"}} }

// Rule 2: &x of a config pointer.
func badDecode(b []byte) *rest.Config { var rc *rest.Config; _ = json.Unmarshal(b, &rc); return rc } // H5

func badDecodeSlice(b []byte) []*rest.Config { var rcs []*rest.Config; _ = json.Unmarshal(b, &rcs); return rcs }

// Rule 4: a third-party consumer of a config.
func badConsumerFromSecret(rc *rest.Config) { _, _ = endpoints.FromSecret(context.Background(), rc, "s", "n") }

var badConsumerValue = endpoints.FromSecret

func badConsumerClientset(rc *rest.Config) { _, _ = kubernetes.NewForConfig(rc) }

func badConsumerMethod(rc *rest.Config) { rc.Wrap(nil) }

// H6-H9: reflection and decoders allocate a config through a pointer-typed
// path that rules 1-3 do not see. Handing it to a third party is refused
// (rule 4); handing it to credhttp is allowed, because credhttp applies
// noClientRateLimit to whatever it is given (the okH* twins).
func h6() *rest.Config {
	return reflect.New(reflect.TypeFor[*rest.Config]().Elem()).Interface().(*rest.Config)
}

func h7() *rest.Config {
	return reflect.New(reflect.TypeOf((*rest.Config)(nil)).Elem()).Interface().(*rest.Config)
}

func h8(b []byte) *rest.Config {
	var s struct{ RC *rest.Config }
	_ = json.Unmarshal(b, &s)
	return s.RC
}

func h9() *rest.Config {
	return reflect.New(reflect.TypeOf(rest.CopyConfig).Out(0).Elem()).Interface().(*rest.Config)
}

func badH6Consumed() { _, _ = endpoints.FromSecret(context.Background(), h6(), "s", "n") }

func badH7Consumed() { _, _ = kubernetes.NewForConfig(h7()) }

func badH8Consumed(b []byte) { _, _ = endpoints.FromSecret(context.Background(), h8(b), "s", "n") }

func badH9Consumed() { _, _ = dynamic.NewForConfig(h9()) }

func okH6ToCredhttp() { _, _ = credhttp.FromSecret(context.Background(), h6(), "s", "n") }

func okH7ToCredhttp() { _, _ = credhttp.Kubernetes(h7()) }

func okH8ToCredhttp(b []byte) { _, _ = credhttp.FromSecret(context.Background(), h8(b), "s", "n") }

func okH9ToCredhttp() { _, _ = credhttp.Dynamic(h9()) }

func okAtomicStore(p *atomic.Pointer[holder]) { p.Store(&holder{}) }

// Allowed.
func okCopyConfig(rc *rest.Config) *rest.Config { return rest.CopyConfig(rc) }

func okDerefCopy(rc *rest.Config) *rest.Config { cp := *rc; cp.Host = "h"; return &cp }

func okCredhttp() { _, _ = credhttp.InClusterConfig() }

func local() *rest.Config { return nil }

func okModuleInternal() { _ = local() }

func okRESTClientMethod(c *rest.RESTClient) { _ = c.Get() }

func okGenericContainer(p *atomic.Pointer[holder]) { _ = p.Load() }

func okPointerSlice(cs []*rest.Config) []*rest.Config { return append([]*rest.Config(nil), cs...) }

func okAddrOfOptions(o *holder) *holder { h := *o; return &h }

type okHolderType struct{ rc *rest.Config }

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
	pkgs := loadTyped(t, root, map[string][]byte{filepath.Join(dir, "x.go"): []byte(typedShapeFixture)}, nil, "./internal/credhttp/internal/typedfixture")
	if len(pkgs) != 1 || pkgs[0].Types == nil {
		t.Fatalf("typed fixture did not load: %v", pkgs)
	}
	hits, copies := typedConfigSources(pkgs, modulePathOf(t, root))
	flagged := map[string]bool{}
	for _, h := range hits {
		flagged[h.fn] = true
	}
	scope := pkgs[0].Types.Scope()
	nBad := 0
	for _, name := range scope.Names() {
		switch {
		case strings.HasPrefix(name, "bad"):
			nBad++
			if !flagged[name] {
				t.Errorf("typed guard did not flag %s", name)
			}
		case strings.HasPrefix(name, "ok") && flagged[name]:
			t.Errorf("typed guard flagged the allowed %s: %v", name, hitsIn(hits, name))
		}
	}
	if nBad < 25 {
		t.Errorf("fixture scope lists only %d bad* cases; the fixture did not load whole", nBad)
	}
	// okCopyConfig calls it; h9 takes it as a value.
	if copies != 2 {
		t.Errorf("typed guard counted %d rest.CopyConfig references in the fixture, want 2", copies)
	}
	for name, want := range map[string][2]bool{
		// name: {restConfigIn, restConfigByValue}
		"containsStruct": {true, false},
		"containsNested": {true, false},
		"containsFunc":   {false, false},
	} {
		typ := scope.Lookup(name).Type()
		if got := [2]bool{restConfigIn(typ), restConfigByValue(typ)}; got != want {
			t.Errorf("{restConfigIn, restConfigByValue}(%s) = %v, want %v", name, got, want)
		}
	}
}

func hitsIn(hits []typedHit, fn string) []string {
	var out []string
	for _, h := range hits {
		if h.fn == fn {
			out = append(out, h.what)
		}
	}
	return out
}
