// main_public_identity_routes_1334_test.go — #1334: which routes accept the
// public identity, through the REAL route table (mountCallerIdentityRoutes, the
// one main() mounts them with) and the real auth middleware.
//
// GET /rbac enumerates a RESTAction's read set under snowplow's own
// ServiceAccount, never under the caller's credential. Serving it token-less
// would run that ServiceAccount enumeration for an anonymous caller, so a
// token-less request to it must stay 401 TokenMissing with the public identity
// on. Every other identity route takes the public path; the arm checks that
// for GET /call, so it shows a route that does take the public identity
// answers differently (here 503: the impersonation is not granted in this
// test, so nothing is dispatched).
package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/server/use"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"k8s.io/client-go/rest"
)

func routesPublic1334(t *testing.T) *http.ServeMux {
	t.Helper()
	t.Setenv("CACHE_ENABLED", "false")
	cache.ResetRouteScopeRegistryForTest()
	t.Cleanup(cache.ResetRouteScopeRegistryForTest)
	cache.ResetImpersonationCapabilityForTest()
	t.Cleanup(cache.ResetImpersonationCapabilityForTest)
	public := middleware.NewPublicIdentity(
		func() (*endpoints.Endpoint, error) {
			return &endpoints.Endpoint{ServerURL: "https://kubernetes.default.svc", Token: "sa-1334"}, nil
		},
		func() (*rest.Config, error) {
			return &rest.Config{Host: "https://10.96.0.1:443", BearerToken: "sa-1334"}, nil
		})
	mux := http.NewServeMux()
	mountCallerIdentityRoutes(newRouteTable(mux), use.NewChain(), debugAuthTestKeys, "krateo-system", public)
	return mux
}

func causeOf1334(body []byte) string {
	var st struct {
		Details *struct {
			Causes []struct {
				Reason string `json:"reason"`
			} `json:"causes"`
		} `json:"details"`
	}
	if json.Unmarshal(body, &st) != nil || st.Details == nil || len(st.Details.Causes) == 0 {
		return ""
	}
	return st.Details.Causes[0].Reason
}

func TestIssue1334_RouteTable_RBACNeverServesThePublicIdentity(t *testing.T) {
	mux := routesPublic1334(t)

	rbac := httptest.NewRecorder()
	mux.ServeHTTP(rbac, httptest.NewRequest(http.MethodGet, "/rbac?apiVersion=templates.krateo.io/v1&resource=restactions&namespace=ns&name=ra", nil))
	if rbac.Code != http.StatusUnauthorized || causeOf1334(rbac.Body.Bytes()) != "TokenMissing" {
		t.Fatalf("MUST NEVER: a token-less GET /rbac answered %d (cause %q) with the public identity on; want 401 TokenMissing — "+
			"its enumeration runs under snowplow's ServiceAccount. body=%s", rbac.Code, causeOf1334(rbac.Body.Bytes()), rbac.Body.String())
	}

	// CONTROL: a route that takes the public identity answers otherwise for the
	// same token-less request (503: the impersonation is not granted here).
	call := httptest.NewRecorder()
	mux.ServeHTTP(call, httptest.NewRequest(http.MethodGet, "/call?apiVersion=v1&resource=configmaps&namespace=krateo-public&name=page", nil))
	if call.Code != http.StatusServiceUnavailable {
		t.Fatalf("CONTROL: a token-less GET /call with the public identity on answered %d, want 503 (the public path, impersonation not granted); "+
			"the arm cannot tell the public path from the 401. body=%s", call.Code, call.Body.String())
	}
}

// main() mounts the identity routes only through mountCallerIdentityRoutes, so
// the arm above tests the route table production serves.
func TestIssue1334_MainMountsIdentityRoutesThroughTheTestedFunction(t *testing.T) {
	f, err := parser.ParseFile(token.NewFileSet(), "main.go", nil, 0)
	if err != nil {
		t.Fatalf("parse main.go: %v", err)
	}
	calls, direct := 0, 0
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "main" || fd.Recv != nil {
			continue
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			switch fn := c.Fun.(type) {
			case *ast.Ident:
				if fn.Name == "mountCallerIdentityRoutes" {
					calls++
				}
			case *ast.SelectorExpr:
				if id, ok := fn.X.(*ast.Ident); ok && id.Name == "middleware" &&
					(fn.Sel.Name == "UserConfig" || fn.Sel.Name == "UserConfigOrPublic") {
					direct++
				}
			}
			return true
		})
	}
	if calls != 1 || direct != 0 {
		t.Fatalf("main() must mount the identity routes through mountCallerIdentityRoutes once and build no UserConfig itself: calls=%d direct=%d", calls, direct)
	}
}
