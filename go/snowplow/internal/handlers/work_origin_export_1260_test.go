package handlers

// work_origin_export_1260_test.go — #1260: an export's re-dispatch through the
// /call lane (exportHandler.dispatchCall, the production builder) names itself
// "export", though it keeps the request's context values.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

func TestIssue1260_ExportRedispatchNamesExport(t *testing.T) {
	var got string
	var ok bool
	h := &exportHandler{call: http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got, ok = cache.WorkOrigin(r.Context())
	})}
	req := httptest.NewRequest(http.MethodGet, "/export?format=csv", nil)
	req = req.WithContext(cache.WithRequestWork(req.Context()))
	h.dispatchCall(req, 0, 0, &stageOutcomeSet{})
	if !ok || got != cache.NonCustomerExport {
		t.Fatalf("#1260: an export re-dispatch's ctx has work origin (%q, %v), want %q", got, ok, cache.NonCustomerExport)
	}
}
