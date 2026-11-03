package credhttp

// send_guard_1106_test.go — #1106's structural guard: every endpoint request
// leaves through Send, so every one carries the cross-origin redirect refusal
// (SameOriginOnly) outermost.
//
// The policy is enforced at the one place that sends, not at a builder or at
// client construction (oasgen's host-policy conformance §5(a)): a builder guard
// protects the paths that use the builder, and a NEW send site would be
// silently unguarded — an unguarded credentialed request succeeds, so nothing
// would surface it. This test makes "the set of guarded sends" and "the set of
// endpoint sends" the same by construction:
//   - every function that obtains an endpoint client (httpClientForEndpoint, or
//     plumbing's request.HTTPClientForEndpoint) sends through Send in the same
//     function, except buildEndpointClient, the builder whose single caller is
//     httpClientForEndpoint;
//   - control: at least one such site and one Send call are found, so a scan
//     of the wrong tree cannot pass with a meaningless zero.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestIssue1106_EveryEndpointRequestLeavesThroughSend(t *testing.T) {
	root := moduleRoot(t)
	fset := token.NewFileSet()
	var sites, sendCalls int
	var bad []string
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
		f, perr := parser.ParseFile(fset, p, nil, parser.SkipObjectResolution)
		if perr != nil {
			return perr
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		inCredhttp := strings.HasPrefix(rel, "internal/credhttp/")
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			obtains, sends := false, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch fun := call.Fun.(type) {
				case *ast.Ident:
					if fun.Name == "httpClientForEndpoint" {
						obtains = true
					}
					if inCredhttp && fun.Name == "Send" {
						sends = true
					}
				case *ast.SelectorExpr:
					if fun.Sel.Name == "HTTPClientForEndpoint" {
						obtains = true
					}
					if id, ok := fun.X.(*ast.Ident); ok && id.Name == "credhttp" && fun.Sel.Name == "Send" {
						sends = true
					}
				}
				return true
			})
			if sends {
				sendCalls++
			}
			if !obtains {
				continue
			}
			sites++
			if fn.Name.Name == "buildEndpointClient" && rel == "internal/resolvers/restactions/api/endpoints_tls.go" {
				continue // the builder; its single caller is httpClientForEndpoint
			}
			if fn.Name.Name == "httpClientForEndpoint" && rel == "internal/resolvers/restactions/api/endpoints_tls.go" {
				continue // the builder's wrapper; its callers are checked here
			}
			if !sends {
				bad = append(bad, rel+": "+fn.Name.Name)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if sites < 3 || sendCalls < 2 {
		t.Fatalf("control: the scan found %d endpoint-client sites and %d Send calls; it walked the wrong tree", sites, sendCalls)
	}
	sort.Strings(bad)
	for _, b := range bad {
		t.Errorf("RED: %s obtains an endpoint client and does not send through credhttp.Send: its request would follow a cross-origin redirect (#1106)", b)
	}
}
