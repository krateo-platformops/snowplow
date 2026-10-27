package credhttp

// ratelimit_920_guard_test.go — #920 structural gate, the in-package half.
// guard_test.go keeps every client and every *rest.Config inside this package
// (and, for #920, refuses a rest.Config built from nothing or a QPS / Burst /
// RateLimiter assignment anywhere else). This file checks the inside: every
// function here that builds a REST client or a *rest.Config applies
// noClientRateLimit, directly or through a function of this package that does
// (Harden). A builder added without it, or the call dropped from one (or from
// Harden), turns this RED.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// buildsRESTClientOrConfig reports whether pkgPath.name returns a REST client
// (where client-go installs its rate limiter) or a *rest.Config (from which
// one is later built). An *http.Client has no rate limiter, so
// rest.HTTPClientFor, transport.New and plumbing's request helpers are not in
// this set.
func buildsRESTClientOrConfig(pkgPath, name string) bool {
	switch {
	case strings.HasPrefix(pkgPath, "k8s.io/client-go/"),
		strings.HasPrefix(pkgPath, "k8s.io/apiextensions-apiserver/pkg/client/"):
		if forConfigRe.MatchString(name) {
			return true
		}
	}
	switch pkgPath {
	case "k8s.io/client-go/rest":
		switch name {
		case "InClusterConfig", "RESTClientFor", "RESTClientForConfigAndClient",
			"UnversionedRESTClientFor", "UnversionedRESTClientForConfigAndClient":
			return true
		}
	case "k8s.io/client-go/tools/clientcmd":
		return strings.Contains(name, "ClientConfig") || strings.HasPrefix(name, "BuildConfig")
	case "github.com/krateo-platformops/plumbing/kubeconfig":
		return name == "NewClientConfig"
	}
	return false
}

const noLimitHelper = "noClientRateLimit"

type builderFn struct {
	builds string          // the first REST-client/config builder it references
	calls  map[string]bool // package-local functions (or func vars) it calls
}

// scanBuilders parses the non-test files of dir and returns every top-level
// function with what it builds and what it calls.
func scanBuilders(t *testing.T, fset *token.FileSet, files map[string]string) map[string]*builderFn {
	t.Helper()
	fns := map[string]*builderFn{}
	sources := map[string]string{} // package-level var → the builder it holds
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		f, err := parser.ParseFile(fset, n, files[n], parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		imports := map[string]string{}
		for _, im := range f.Imports {
			p, _ := strconv.Unquote(im.Path.Value)
			name := p[strings.LastIndex(p, "/")+1:]
			if im.Name != nil {
				name = im.Name.Name
			}
			imports[name] = p
		}
		builder := func(n ast.Node) string {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && buildsRESTClientOrConfig(imports[id.Name], sel.Sel.Name) {
					return id.Name + "." + sel.Sel.Name
				}
			}
			return ""
		}
		for _, d := range f.Decls {
			switch d := d.(type) {
			case *ast.GenDecl:
				// A package-level var holding a builder (the inClusterConfig
				// seam) builds wherever it is called.
				for _, sp := range d.Specs {
					vs, ok := sp.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for i, v := range vs.Values {
						if b := builder(v); b != "" && i < len(vs.Names) {
							sources[vs.Names[i].Name] = b
						}
					}
				}
			case *ast.FuncDecl:
				if d.Recv != nil || d.Body == nil {
					continue
				}
				fi := &builderFn{calls: map[string]bool{}}
				ast.Inspect(d.Body, func(n ast.Node) bool {
					if b := builder(n); b != "" && fi.builds == "" {
						fi.builds = b
					}
					if c, ok := n.(*ast.CallExpr); ok {
						if id, ok := c.Fun.(*ast.Ident); ok {
							fi.calls[id.Name] = true
						}
					}
					return true
				})
				fns[d.Name.Name] = fi
			}
		}
	}
	for _, fi := range fns {
		for v := range fi.calls {
			if b, ok := sources[v]; ok && fi.builds == "" {
				fi.builds = v + " (= " + b + ")"
			}
		}
	}
	return fns
}

// helperMissingSettings returns which of the two settings client-go reads to
// decide "no limiter" the helper no longer makes: QPS assigned a negative
// constant, and RateLimiter assigned nil.
func helperMissingSettings(t *testing.T, files map[string]string) []string {
	t.Helper()
	var qpsNegative, limiterNil bool
	found := false
	for n, src := range files {
		f, err := parser.ParseFile(token.NewFileSet(), n, src, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || fd.Name.Name != noLimitHelper || fd.Body == nil {
				continue
			}
			found = true
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				as, ok := n.(*ast.AssignStmt)
				if !ok || len(as.Lhs) != len(as.Rhs) {
					return true
				}
				for i, lhs := range as.Lhs {
					sel, ok := lhs.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					switch rhs := as.Rhs[i]; sel.Sel.Name {
					case "QPS":
						if u, ok := rhs.(*ast.UnaryExpr); ok && u.Op == token.SUB {
							if _, ok := u.X.(*ast.BasicLit); ok {
								qpsNegative = true
							}
						}
					case "RateLimiter":
						if id, ok := rhs.(*ast.Ident); ok && id.Name == "nil" {
							limiterNil = true
						}
					}
				}
				return true
			})
		}
	}
	if !found {
		return []string{"anything (it has no body)"}
	}
	var missing []string
	if !qpsNegative {
		missing = append(missing, "QPS to a negative constant")
	}
	if !limiterNil {
		missing = append(missing, "RateLimiter to nil")
	}
	return missing
}

// builderViolations returns, sorted, every builder that never applies the
// helper, and the names of all builders found.
func builderViolations(fns map[string]*builderFn) (bad, builders []string) {
	applies := map[string]bool{}
	for changed := true; changed; {
		changed = false
		for name, fi := range fns {
			if applies[name] {
				continue
			}
			for c := range fi.calls {
				if c == noLimitHelper || applies[c] {
					applies[name] = true
					changed = true
					break
				}
			}
		}
	}
	for name, fi := range fns {
		if fi.builds == "" {
			continue
		}
		builders = append(builders, name)
		if !applies[name] {
			bad = append(bad, "credhttp."+name+" builds "+fi.builds+" but never applies "+noLimitHelper+
				": its clients get client-go's 5/s limiter (#920)")
		}
	}
	sort.Strings(bad)
	sort.Strings(builders)
	return bad, builders
}

func TestIssue920_EveryCredhttpBuilderAppliesNoClientRateLimit(t *testing.T) {
	dir := filepath.Join(moduleRoot(t), "internal", "credhttp")
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, e := range ents {
		if !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[filepath.Join(dir, e.Name())] = string(b)
	}
	fset := token.NewFileSet()
	fns := scanBuilders(t, fset, files)
	if _, ok := fns[noLimitHelper]; !ok {
		t.Fatalf("%s is gone from internal/credhttp: #920 has no single place that removes the client-side rate limit", noLimitHelper)
	}
	for _, missing := range helperMissingSettings(t, files) {
		t.Errorf("%s no longer sets %s: client-go would install its 5/s limiter (#920)", noLimitHelper, missing)
	}
	bad, builders := builderViolations(fns)
	for _, b := range bad {
		t.Error(b)
	}
	// The scan must classify the builders this package is known to have, or a
	// broken walk would pass by finding none.
	for _, want := range []string{"InClusterConfig", "NewClientConfig", "Dynamic", "Metadata", "Kubernetes", "APIExtensions", "Discovery", "RESTClient"} {
		if i := sort.SearchStrings(builders, want); i == len(builders) || builders[i] != want {
			t.Errorf("the scan did not classify credhttp.%s as a builder (found %v): the gate is blind", want, builders)
		}
	}
}

// The gate must bite on each way a builder can skip the helper.
func TestIssue920_BuilderGateDetectsEveryShape(t *testing.T) {
	const helperSrc = "package credhttp\nimport \"k8s.io/client-go/rest\"\nfunc noClientRateLimit(rc *rest.Config) { rc.QPS = -1 }\n"
	cases := map[string]struct {
		src     string
		wantBad bool
	}{
		"direct":          {"package credhttp\nimport \"k8s.io/client-go/dynamic\"\nfunc D(rc *rest.Config) { cfg := dynamic.ConfigFor(rc); noClientRateLimit(cfg); dynamic.NewForConfigAndClient(cfg, nil) }\n", false},
		"missing":         {"package credhttp\nimport \"k8s.io/client-go/dynamic\"\nfunc D(rc *rest.Config) { dynamic.NewForConfigAndClient(rc, nil) }\n", true},
		"viaHarden":       {"package credhttp\nimport \"k8s.io/client-go/rest\"\nfunc Harden(rc *rest.Config) *rest.Config { noClientRateLimit(rc); return rc }\nfunc I() { rc, _ := rest.InClusterConfig(); Harden(rc) }\n", false},
		"hardenDropped":   {"package credhttp\nimport \"k8s.io/client-go/rest\"\nfunc Harden(rc *rest.Config) *rest.Config { return rc }\nfunc I() { rc, _ := rest.InClusterConfig(); Harden(rc) }\n", true},
		"seamVar":         {"package credhttp\nimport \"k8s.io/client-go/rest\"\nvar src = rest.InClusterConfig\nfunc I() { src() }\n", true},
		"aliasedClientGo": {"package credhttp\nimport k \"k8s.io/client-go/kubernetes\"\nfunc K(rc *rest.Config) { k.NewForConfigAndClient(rc, nil) }\n", true},
		"restClient":      {"package credhttp\nimport \"k8s.io/client-go/rest\"\nfunc R(rc *rest.Config) { rest.RESTClientForConfigAndClient(rc, nil) }\n", true},
		"kubeconfig":      {"package credhttp\nimport \"github.com/krateo-platformops/plumbing/kubeconfig\"\nfunc N() { kubeconfig.NewClientConfig(nil, x) }\n", true},
		"httpOnly":        {"package credhttp\nimport \"k8s.io/client-go/rest\"\nfunc H(rc *rest.Config) { rest.HTTPClientFor(rc) }\n", false},
	}
	for name, c := range cases {
		fns := scanBuilders(t, token.NewFileSet(), map[string]string{"helper.go": helperSrc, "x.go": c.src})
		bad, _ := builderViolations(fns)
		if got := len(bad) > 0; got != c.wantBad {
			t.Errorf("%s: flagged=%v, want %v (%v)", name, got, c.wantBad, bad)
		}
	}
}
