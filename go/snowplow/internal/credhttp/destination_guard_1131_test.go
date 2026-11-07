package credhttp

// destination_guard_1131_test.go — #1131: no dial that can carry a named endpoint
// leaves the guarded set (destination.go). "A check in a builder that some call
// sites bypass looks complete and isn't": core-provider found 5 dial paths where
// it expected 3.
//
// It walks every non-test .go file in the module, this package included (the
// #723 guard skips it, and this one is about what it does), and fails on:
//   - an http.Transport literal with no DialContext;
//   - an http.Client literal with no Transport, unless it goes straight into
//     credhttp.Client, which gives it guardedTransport;
//   - a net.Dialer literal with no Control;
//   - a client-go transport.Config literal with no DialHolder;
//   - a zero value of any of those, http.DefaultClient, http.DefaultTransport,
//     and net/http's package-level Get, Head, Post and PostForm;
//   - net.Dial and its family, and tls.Dial: a dial with no dialer to guard;
//   - plumbing's HTTPClientForEndpoint or Do anywhere but the two builders whose
//     chain the behavioural arms send on.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// destinationExempt: (file, enclosing func, what) → reason. Each must still match.
var destinationExempt = map[[3]string]string{
	{"internal/credhttp/endpoint_clients.go", "endpointIdle", "http.DefaultTransport"}:                              "reads IdleConnTimeout, the value client-go's transports take from it. No dial.",
	{"internal/credhttp/do.go", "Do", "request.HTTPClientForEndpoint"}:                                              "/call: plumbing's transport dials through dialguard (TestIssue1131_EveryDeniedSpellingIsRefused, plumbing chain).",
	{"internal/resolvers/restactions/api/endpoints_tls.go", "buildEndpointClient", "request.HTTPClientForEndpoint"}: "the plumbing branch of a stage fetch, the same transport.",
}

const (
	importHTTP      = "net/http"
	importNet       = "net"
	importTLS       = "crypto/tls"
	importTransport = "k8s.io/client-go/transport"
	importRequest   = "github.com/krateo-platformops/plumbing/http/request"
	importCredhttp  = "github.com/krateo-platformops/snowplow/internal/credhttp"
)

// destinationType names the guarded types and the field that makes a literal of
// one guarded.
func destinationType(e ast.Expr, imports map[string]string) (name, field string, ok bool) {
	sel, isSel := e.(*ast.SelectorExpr)
	if !isSel {
		return "", "", false
	}
	id, isID := sel.X.(*ast.Ident)
	if !isID {
		return "", "", false
	}
	switch imports[id.Name] + "." + sel.Sel.Name {
	case importHTTP + ".Transport":
		return "http.Transport", "DialContext", true
	case importHTTP + ".Client":
		return "http.Client", "Transport", true
	case importNet + ".Dialer":
		return "net.Dialer", "Control", true
	case importTransport + ".Config":
		return "transport.Config", "DialHolder", true
	}
	return "", "", false
}

// destinationReference reports a reference that bypasses the guarded set by name.
func destinationReference(pkg, name string) (string, bool) {
	switch pkg {
	case importHTTP:
		switch name {
		case "DefaultClient", "DefaultTransport", "Get", "Head", "Post", "PostForm":
			return "http." + name, true
		}
	case importNet:
		switch name {
		case "Dial", "DialTimeout", "DialTCP", "DialIP", "DialUnix", "DialUDP":
			return "net." + name, true
		}
	case importTLS:
		if name == "Dial" || name == "DialWithDialer" {
			return "tls." + name, true
		}
	case importRequest:
		if name == "HTTPClientForEndpoint" || name == "Do" {
			return "request." + name, true
		}
	}
	return "", false
}

func hasKey(lit *ast.CompositeLit, field string) bool {
	for _, el := range lit.Elts {
		if kv, ok := el.(*ast.KeyValueExpr); ok {
			if id, ok := kv.Key.(*ast.Ident); ok && id.Name == field {
				return true
			}
		}
	}
	return false
}

// handedToCredhttpClient reports whether the literal at the top of stack is the
// argument of credhttp.Client (from another package) or of Client (from this one).
func handedToCredhttpClient(stack []ast.Node, imports map[string]string, inCredhttp bool) bool {
	i := len(stack) - 2
	if i >= 0 {
		if _, ok := stack[i].(*ast.UnaryExpr); ok {
			i--
		}
	}
	if i < 0 {
		return false
	}
	call, ok := stack[i].(*ast.CallExpr)
	if !ok {
		return false
	}
	switch f := call.Fun.(type) {
	case *ast.SelectorExpr:
		id, ok := f.X.(*ast.Ident)
		return ok && imports[id.Name] == importCredhttp && f.Sel.Name == "Client"
	case *ast.Ident:
		return inCredhttp && f.Name == "Client"
	}
	return false
}

type destinationFinding struct {
	what string
	fn   string
	pos  token.Position
}

// destinationScan returns everything in f that leaves the guarded set, and how
// many guarded-type literals it saw (so a scan that finds nothing can be told
// from a scan that looked at nothing).
func destinationScan(fset *token.FileSet, f *ast.File, rel string) (out []destinationFinding, literals int) {
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
		if _, top := parent.(*ast.File); top {
			fn = ""
			if fd, ok := n.(*ast.FuncDecl); ok {
				fn = fd.Name.Name
			}
		}
		add := func(what string, p token.Pos) {
			out = append(out, destinationFinding{what, fn, fset.Position(p)})
		}
		switch x := n.(type) {
		case *ast.CompositeLit:
			name, field, ok := destinationType(x.Type, imports)
			if !ok {
				return true
			}
			literals++
			if hasKey(x, field) || (name == "http.Client" && handedToCredhttpClient(stack, imports, inCredhttp)) {
				return true
			}
			add(name+"{} without "+field, x.Pos())
		case *ast.CallExpr:
			if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "new" && len(x.Args) == 1 {
				if name, _, ok := destinationType(x.Args[0], imports); ok {
					add("new("+name+")", x.Pos())
				}
			}
		case *ast.ValueSpec:
			if name, _, ok := destinationType(x.Type, imports); ok && len(x.Values) == 0 {
				add("zero "+name, x.Pos())
			}
		case *ast.Field:
			if name, _, ok := destinationType(x.Type, imports); ok {
				add("by-value "+name, x.Pos())
			}
		case *ast.TypeSpec:
			if name, _, ok := destinationType(x.Type, imports); ok {
				add("renamed "+name, x.Pos())
			}
		case *ast.SelectorExpr:
			if id, ok := x.X.(*ast.Ident); ok {
				if what, bad := destinationReference(imports[id.Name], x.Sel.Name); bad {
					add(what, x.Pos())
				}
			}
		}
		return true
	})
	return out, literals
}

func TestIssue1131_NoDialLeavesTheGuardedSet(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	used := map[[3]string]bool{}
	var bad []string
	scanned, literals := 0, 0
	sawDialer := false
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
		found, n := destinationScan(fset, f, rel)
		literals += n
		if rel == "internal/credhttp/budget.go" && n > 0 {
			sawDialer = true
		}
		for _, r := range found {
			k := [3]string{rel, r.fn, r.what}
			if _, ok := destinationExempt[k]; ok {
				used[k] = true
				continue
			}
			bad = append(bad, r.pos.String()+": "+r.what+" (in "+r.fn+") leaves the set of guarded dials (#1131, destination.go)")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Non-empty controls: nothing found must mean nothing was there.
	if scanned < 100 || !sawDialer || literals < 3 {
		t.Fatalf("scanned %d files and %d guarded literals (budget.go's dialer seen: %v): the walk is broken", scanned, literals, sawDialer)
	}
	sort.Strings(bad)
	for _, b := range bad {
		t.Error(b)
	}
	for k := range destinationExempt {
		if !used[k] {
			t.Errorf("stale exemption %v: it matches nothing; remove it", k)
		}
	}
}

// The guard must bite on every shape it claims to catch.
func TestIssue1131_GuardDetectsEveryShape(t *testing.T) {
	for name, body := range map[string]string{
		"transport":        `import "net/http"; var t = &http.Transport{Proxy: nil}`,
		"transportValue":   `import "net/http"; var t = http.Transport{}`,
		"aliased":          `import nh "net/http"; var t = &nh.Transport{}`,
		"client":           `import "net/http"; var c = &http.Client{}`,
		"clientTimeout":    `import ("net/http"; "time"); var c = &http.Client{Timeout: time.Second}`,
		"dialer":           `import "net"; var d = &net.Dialer{}`,
		"dialerKeepAlive":  `import ("net"; "time"); var d = net.Dialer{KeepAlive: time.Second}`,
		"transportConfig":  `import "k8s.io/client-go/transport"; var c = &transport.Config{}`,
		"defaultClient":    `import "net/http"; var c = http.DefaultClient`,
		"defaultTransport": `import "net/http"; var c = http.DefaultTransport`,
		"get":              `import "net/http"; func f() { http.Get("u") }`,
		"netDial":          `import "net"; func f() { net.Dial("tcp", "a") }`,
		"netDialTimeout":   `import "net"; func f() { net.DialTimeout("tcp", "a", 0) }`,
		"tlsDial":          `import "crypto/tls"; func f() { tls.Dial("tcp", "a", nil) }`,
		"newDialer":        `import "net"; var d = new(net.Dialer)`,
		"newClient":        `import "net/http"; var c = new(http.Client)`,
		"zeroDialer":       `import "net"; func f() { var d net.Dialer; _ = d }`,
		"fieldClient":      `import "net/http"; type T struct{ c http.Client }`,
		"renamedDialer":    `import "net"; type D net.Dialer`,
		"plumbingBuilder":  `import "github.com/krateo-platformops/plumbing/http/request"; func f() { request.HTTPClientForEndpoint(nil, nil) }`,
		"plumbingDo":       `import "github.com/krateo-platformops/plumbing/http/request"; func f() { request.Do(nil, x) }`,
	} {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "x.go", "package p\n"+body+"\n", parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got, _ := destinationScan(fset, f, "x.go"); len(got) == 0 {
			t.Errorf("%s: guard did not flag %q", name, body)
		}
	}
	// And it must NOT flag the guarded forms.
	for name, body := range map[string]string{
		"transport":     `import "net/http"; var t = &http.Transport{DialContext: d}`,
		"client":        `import "net/http"; var c = &http.Client{Transport: t}`,
		"credhttp":      `import ("net/http"; "github.com/krateo-platformops/snowplow/internal/credhttp"); var c = credhttp.Client(&http.Client{Timeout: 1})`,
		"dialer":        `import ("net"; "github.com/krateo-platformops/plumbing/dialguard"); var d = &net.Dialer{Control: dialguard.Control}`,
		"config":        `import "k8s.io/client-go/transport"; var c = &transport.Config{DialHolder: h}`,
		"pointerField":  `import "net/http"; type T struct{ c *http.Client }`,
		"aResolverDial": `import "net"; var r = net.Resolver{}`,
	} {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, "x.go", "package p\n"+body+"\n", parser.SkipObjectResolution)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got, _ := destinationScan(fset, f, "x.go"); len(got) != 0 {
			t.Errorf("%s: guarded form flagged: %+v", name, got)
		}
	}
	// Client(...) unqualified is credhttp's only inside credhttp.
	body := "package p\nimport \"net/http\"\nvar c = Client(&http.Client{})\n"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "x.go", body, parser.SkipObjectResolution)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := destinationScan(fset, f, "internal/other/x.go"); len(got) != 1 {
		t.Errorf("an unqualified Client outside credhttp: want it flagged, got %+v", got)
	}
	if got, _ := destinationScan(fset, f, "internal/credhttp/x.go"); len(got) != 0 {
		t.Errorf("Client inside credhttp: want it allowed, got %+v", got)
	}
}
