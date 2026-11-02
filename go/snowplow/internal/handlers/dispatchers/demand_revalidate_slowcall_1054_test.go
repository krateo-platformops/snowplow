package dispatchers

// demand_revalidate_slowcall_1054_test.go — #1054: a demand revalidation runs
// on a detached clone of an already-served request (setDemandTemplate). The
// clone must not carry that request's slow-call record, or the revalidation's
// later work would be charged to a request that has already been answered.

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/slowcall"
)

func TestS1054_DemandTemplateDropsTheRequestsRecord(t *testing.T) {
	mon := slowcall.NewMonitorForTest(time.Hour, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	const key = "s1054-demand-template-key"
	t.Cleanup(func() { demandTemplates.Delete(key) })
	var had, kept bool
	h := mon.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		had = slowcall.RecordingForTest(r.Context())
		tpl := setDemandTemplate(key, r)
		tpl.mu.Lock()
		kept = slowcall.RecordingForTest(tpl.req.Context())
		tpl.mu.Unlock()
	}))
	req := httptest.NewRequest(http.MethodGet, "/call", nil)
	req.Pattern = "GET /call"
	h.ServeHTTP(httptest.NewRecorder(), req)
	if !had {
		t.Fatalf("PRECONDITION: the request carried no record")
	}
	if kept {
		t.Fatalf("DETACH RED: the demand-revalidation template carries the served request's slow-call record")
	}
}
