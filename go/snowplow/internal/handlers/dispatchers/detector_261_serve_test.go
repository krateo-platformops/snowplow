// detector_261_serve_test.go — the serve helpers of the #261 serve-time
// detector arms (implicit_group_detector_261_test.go). They lived in the
// case-3 detector's test file, removed with that detector (#1449).

package dispatchers

import (
	"context"
	"net/http/httptest"
	"testing"

	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/resolvers/restactions"
)

// c3NonUAFResolve is a resolver seam that returns a plain (NON-UAF) RESTAction,
// so the serve path's Put is ACCEPTED and a later serve can HIT the cell.
func c3NonUAFResolve() func(context.Context, restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
	return func(_ context.Context, _ restactions.ResolveOptions) (*templatesv1.RESTAction, error) {
		out := &templatesv1.RESTAction{}
		out.SetName(h1RAName)
		out.SetNamespace(h1NS)
		return out, nil
	}
}

// c3Serve drives one full dispatch through the REAL RESTAction handler and
// returns the response recorder. cr is the fetched CR (non-UAF).
func c3Serve(t *testing.T, ctx context.Context) *httptest.ResponseRecorder {
	t.Helper()
	restore := installRAFakes(t, a1RAUnstructured(false), func() bool { return true }, c3NonUAFResolve())
	defer restore()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/call", nil).WithContext(ctx)
	RESTAction().ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("dispatch must serve 200; got %d body=%s", rec.Code, rec.Body.String())
	}
	return rec
}
