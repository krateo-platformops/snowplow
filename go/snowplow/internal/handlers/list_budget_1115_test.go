package handlers_test

// list_budget_1115_test.go — #1115 review B1: /list's discovery and per-GVR
// LISTs are this customer request's network exchanges. client-go's discovery
// runs on its own context.TODO(), so the budget is bound to the request's
// client (credhttp.BindRequestBudget). A spent budget sends nothing and
// answers 504 Timeout; with budget left /list answers as before.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	xenv "github.com/krateo-platformops/plumbing/env"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/handlers"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

func TestList1115_DiscoveryAndListsRunInsideTheBudget(t *testing.T) {
	prev := xenv.TestMode()
	xenv.SetTestMode(true)
	f := &fakeAPI517{}
	srv := httptest.NewServer(http.HandlerFunc(f.handler))
	t.Cleanup(func() {
		srv.Close()
		xenv.SetTestMode(prev)
	})
	serve := func(arrival time.Time) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/list?category="+cat517+"&ns="+ns517, nil)
		ctx := xcontext.BuildContext(context.Background(),
			xcontext.WithAccessToken("test-token-1115"),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}}),
			xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL}),
		)
		rec := httptest.NewRecorder()
		handlers.List().ServeHTTP(rec, req.WithContext(pagebudget.WithCustomer(ctx, arrival)))
		return rec
	}
	hits := func() int {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.hits)
	}

	if rec := serve(time.Now()); rec.Code != http.StatusOK || hits() == 0 {
		t.Fatalf("control: /list answered %d after %d apiserver hits; want 200 and >0: %s", rec.Code, hits(), rec.Body.String())
	}
	before := hits()
	rec := serve(time.Now().Add(-2 * pagebudget.PageMaxGoal))
	if n := hits() - before; n != 0 {
		t.Fatalf("a /list with a SPENT budget sent %d apiserver request(s); want 0", n)
	}
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("spent budget: /list answered %d, want 504: %s", rec.Code, rec.Body.String())
	}
}
