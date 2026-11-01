// dispatch_seams_testshim_test.go — test-only swappers for the H1 ServeHTTP
// orchestration seams (dispatch_seams.go). The setters live in a _test.go file
// so production restactions.go / widgets.go / helpers.go never expose a setter;
// the seams are reassignable ONLY from tests. Mirrors
// resolve_populate_testshim_test.go (resolveOnceFn) and
// nested_call_testshim_test.go (api.nestedCallResolver).

package dispatchers

import (
	"context"
	"net/http"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/objects"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
	"github.com/krateo-platformops/snowplow/internal/resolvers/widgets"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// setFetchObjectForTest swaps fetchObjectFn and returns a restore the caller
// MUST defer/Cleanup. Restores the production fetchObject.
func setFetchObjectForTest(fn func(req *http.Request) objects.Result) func() {
	prev := fetchObjectFn
	fetchObjectFn = fn
	return func() { fetchObjectFn = prev }
}

// setCheckDispatchRBACForTest swaps the cache=on dispatch RBAC gate verdict and
// returns a restore. Records every (gvr, namespace) it is asked so a test can
// assert the gate ran (or did NOT run) and in what order. The gate takes the
// dispatched object (#1046); these stubs only ever read its namespace.
func setCheckDispatchRBACForTest(fn func(ctx context.Context, gvr schema.GroupVersionResource, namespace string) bool) func() {
	prev := checkDispatchRBACFn
	checkDispatchRBACFn = func(ctx context.Context, gvr schema.GroupVersionResource, obj rbac.ObjectNamer) bool {
		return fn(ctx, gvr, obj.GetNamespace())
	}
	return func() { checkDispatchRBACFn = prev }
}

// setRestactionsResolveForTest swaps the top-level RESTAction resolve seam and
// returns a restore.
func setRestactionsResolveForTest(fn func(ctx context.Context, opts restactions.ResolveOptions) (*templatesv1.RESTAction, error)) func() {
	prev := restactionsResolveFn
	restactionsResolveFn = fn
	return func() { restactionsResolveFn = prev }
}

// setWidgetsResolveForTest swaps the top-level Widget resolve seam and returns a
// restore.
func setWidgetsResolveForTest(fn func(ctx context.Context, opts widgets.ResolveOptions) (*widgets.Widget, error)) func() {
	prev := widgetsResolveFn
	widgetsResolveFn = fn
	return func() { widgetsResolveFn = prev }
}
