package credhttp

// guard_test.go: the #723/#724 structural guard. Every *http.Client, transport,
// client-go client or *rest.Config that the module builds must come from this
// package, so it carries the never-follow-a-redirect policy. The guard walks
// every non-test .go file in the module and fails on any reference to a
// constructor that bypasses credhttp. A NEW credentialed client added by any
// other route turns this test RED.
//
// Exemptions are listed in `allowed` with the reason. Each one must still
// match something, so an exemption cannot outlive the code it excuses.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// forbidden reports whether the reference pkgPath.name builds a client, a
// transport or a *rest.Config outside credhttp.
func forbidden(pkgPath, name string) bool {
	switch {
	case strings.HasPrefix(pkgPath, "k8s.io/client-go/"),
		strings.HasPrefix(pkgPath, "k8s.io/apiextensions-apiserver/pkg/client/"):
		// Every generated clientset/typed client/dynamic/metadata/discovery
		// constructor is named New*ForConfig*.
		if forConfigRe.MatchString(name) {
			return true
		}
	}
	switch pkgPath {
	case "k8s.io/client-go/rest":
		switch name {
		case "InClusterConfig", "HTTPClientFor", "TransportFor", "RESTClientFor",
			"RESTClientForConfigAndClient", "UnversionedRESTClientFor",
			"UnversionedRESTClientForConfigAndClient":
			return true
		}
	case "k8s.io/client-go/transport":
		return name == "New"
	case "k8s.io/client-go/tools/clientcmd":
		return strings.Contains(name, "ClientConfig") || strings.HasPrefix(name, "BuildConfig")
	case "github.com/krateo-platformops/plumbing/http/request":
		return name == "HTTPClientForEndpoint" || name == "Do"
	case "github.com/krateo-platformops/plumbing/kubeconfig":
		return name == "NewClientConfig"
	case "net/http":
		switch name {
		case "DefaultClient", "Get", "Head", "Post", "PostForm":
			return true
		}
	case "sigs.k8s.io/controller-runtime", "sigs.k8s.io/controller-runtime/pkg/client",
		"sigs.k8s.io/controller-runtime/pkg/manager", "sigs.k8s.io/controller-runtime/pkg/cluster":
		return strings.HasPrefix(name, "New")
	}
	return false
}

var forConfigRe = regexp.MustCompile(`^New.*ForConfig`)

// allowed: (module-relative file, enclosing func, reference) → reason.
// "http.Client{}" stands for a composite literal of net/http.Client.
var allowed = map[[3]string]string{
	{"internal/resolvers/restactions/api/endpoints_tls.go", "buildEndpointClient", "request.HTTPClientForEndpoint"}: "endpoint client; its single caller httpClientForEndpoint applies credhttp.Client by provenance (SA dial / held credential). A named endpointRef carries its own credential and keeps the default redirect behaviour.",
	{"internal/resolvers/restactions/api/endpoints_tls.go", "buildEndpointClient", "transport.New"}:                 "same as above",
	{"internal/resolvers/restactions/api/endpoints_tls.go", "buildEndpointClient", "http.Client{}"}:                 "same as above",
	{"internal/handlers/call.go", "ServeHTTP", "request.Do"}:                                                        "RESIDUAL #726: the /call raw passthrough dials the caller's own clientconfig through plumbing's request.Do, which builds its client internally (no CheckRedirect seam). Tracked in its own issue.",
}

type ref struct {
	file, fn, what string
	pos            token.Position
}

func scanFile(fset *token.FileSet, f *ast.File, rel string) []ref {
	imports := map[string]string{} // local name → path
	for _, im := range f.Imports {
		p, _ := strconv.Unquote(im.Path.Value)
		name := p[strings.LastIndex(p, "/")+1:]
		if im.Name != nil {
			name = im.Name.Name
		}
		imports[name] = p
	}
	var out []ref
	var fn string
	var stack []ast.Node
	ast.Inspect(f, func(n ast.Node) bool {
		if n == nil {
			stack = stack[:len(stack)-1]
			return true
		}
		var parent ast.Node
		if len(stack) > 0 {
			parent = stack[len(stack)-1]
		}
		stack = append(stack, n)
		if fd, ok := n.(*ast.FuncDecl); ok {
			fn = fd.Name.Name
		}
		switch x := n.(type) {
		case *ast.SelectorExpr:
			id, ok := x.X.(*ast.Ident)
			if !ok || id.Obj != nil {
				return true
			}
			p, ok := imports[id.Name]
			if ok && forbidden(p, x.Sel.Name) {
				out = append(out, ref{rel, fn, p[strings.LastIndex(p, "/")+1:] + "." + x.Sel.Name, fset.Position(x.Pos())})
			}
		case *ast.CompositeLit:
			sel, ok := x.Type.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			id, ok := sel.X.(*ast.Ident)
			if !ok || imports[id.Name] != "net/http" || (sel.Sel.Name != "Client" && sel.Sel.Name != "Transport") {
				return true
			}
			// An http.Client literal handed straight to credhttp.Client is hardened.
			if u, ok := parent.(*ast.UnaryExpr); ok && u.X == x && len(stack) >= 3 {
				parent = stack[len(stack)-3]
			}
			if c, ok := parent.(*ast.CallExpr); ok {
				if s, ok := c.Fun.(*ast.SelectorExpr); ok {
					if pid, ok := s.X.(*ast.Ident); ok && imports[pid.Name] == "github.com/krateo-platformops/snowplow/internal/credhttp" && s.Sel.Name == "Client" {
						return true
					}
				}
			}
			out = append(out, ref{rel, fn, "http." + sel.Sel.Name + "{}", fset.Position(x.Pos())})
		}
		return true
	})
	return out
}

func moduleRoot(t *testing.T) string {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(file), "..", "..")
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("module root not found from %s: %v", file, err)
	}
	return root
}

func TestIssue723_724_NoCredentialedClientBypassesCredhttp(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	used := map[[3]string]bool{}
	var bad []string
	scanned := 0
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
		if strings.HasPrefix(rel, "internal/credhttp/") {
			return nil
		}
		f, perr := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if perr != nil {
			return perr
		}
		scanned++
		for _, r := range scanFile(fset, f, rel) {
			k := [3]string{r.file, r.fn, r.what}
			if _, ok := allowed[k]; ok {
				used[k] = true
				continue
			}
			bad = append(bad, r.pos.String()+": "+r.what+" (in "+r.fn+") builds a client/transport/rest.Config outside internal/credhttp")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if scanned < 100 {
		t.Fatalf("scanned only %d files; the walk is broken", scanned)
	}
	sort.Strings(bad)
	for _, b := range bad {
		t.Error(b)
	}
	for k := range allowed {
		if !used[k] {
			t.Errorf("stale exemption %v: it matches nothing; remove it", k)
		}
	}
}

// The guard must bite on every shape it claims to catch.
func TestIssue723_724_GuardDetectsEveryShape(t *testing.T) {
	cases := map[string]string{
		"dynamic":    `import "k8s.io/client-go/dynamic"; func f() { dynamic.NewForConfig(nil) }`,
		"aliased":    `import k8sdynamic "k8s.io/client-go/dynamic"; func f() { k8sdynamic.NewForConfigAndClient(nil, nil) }`,
		"kubernetes": `import "k8s.io/client-go/kubernetes"; func f() { kubernetes.NewForConfig(nil) }`,
		"typed":      `import authv1 "k8s.io/client-go/kubernetes/typed/authorization/v1"; func f() { authv1.NewForConfig(nil) }`,
		"discovery":  `import "k8s.io/client-go/discovery"; func f() { discovery.NewDiscoveryClientForConfig(nil) }`,
		"metadata":   `import "k8s.io/client-go/metadata"; func f() { metadata.NewForConfig(nil) }`,
		"incluster":  `import "k8s.io/client-go/rest"; var x = rest.InClusterConfig`,
		"restclient": `import "k8s.io/client-go/rest"; func f() { rest.RESTClientFor(nil) }`,
		"httpclient": `import "k8s.io/client-go/rest"; func f() { rest.HTTPClientFor(nil) }`,
		"transport":  `import "k8s.io/client-go/transport"; func f() { transport.New(nil) }`,
		"plumbing":   `import "github.com/krateo-platformops/plumbing/http/request"; func f() { request.HTTPClientForEndpoint(nil, nil) }`,
		"kubeconfig": `import "github.com/krateo-platformops/plumbing/kubeconfig"; func f() { kubeconfig.NewClientConfig(nil, x) }`,
		"literal":    `import "net/http"; var c = &http.Client{}`,
		"transportL": `import "net/http"; var c = &http.Transport{}`,
		"default":    `import "net/http"; var c = http.DefaultClient`,
		"apiext":     `import apiext "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"; func f() { apiext.NewForConfig(nil) }`,
	}
	for name, body := range cases {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "x.go", "package p\n"+body+"\n", parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got := scanFile(fset, f, "x.go"); len(got) == 0 {
			t.Errorf("%s: guard did not flag %q", name, body)
		}
	}
	// And it must NOT flag the hardened forms.
	ok := `import ("net/http"; "github.com/krateo-platformops/snowplow/internal/credhttp"); var c = credhttp.Client(&http.Client{})`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "x.go", "package p\n"+ok+"\n", parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	if got := scanFile(fset, f, "x.go"); len(got) != 0 {
		t.Errorf("hardened literal flagged: %v", got)
	}
}
