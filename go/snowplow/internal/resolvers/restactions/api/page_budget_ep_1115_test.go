package api

// page_budget_ep_1115_test.go — #1122 review R3 (every path reads Timeout):
// the endpointRef Secret read is a network exchange of the request too. With
// the budget spent nothing is sent, and the stage outcome reads Timeout, not
// the truncating exit's Error.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/jwtutil"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"k8s.io/client-go/rest"
)

func TestBudget1115_R3_EndpointResolutionCutReadsTimeout(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserInfo(jwtutil.UserInfo{Username: "cyberjoker", Groups: []string{"devs"}}))
	ctx = pagebudget.WithCustomer(ctx, time.Now().Add(-2*pagebudget.PageMaxGoal))
	ctx, outcomes := WithStageOutcomes(ctx)
	_, _ = ResolveWithFailure(ctx, ResolveOptions{
		RC: &rest.Config{Host: srv.URL}, AuthnNS: u904AuthnNS,
		Items:               []*templates.API{u904Stage("only", http.MethodGet, "/x")},
		RESTActionNamespace: "ns-1115", RESTActionName: "ra-1115-ep",
	})
	if n.Load() != 0 {
		t.Fatalf("%d request(s) sent with a spent budget", n.Load())
	}
	if raw := outcomes.HeaderValue(); !strings.Contains(raw, `"name":"only","ok":false,"reason":"Timeout"`) {
		t.Fatalf("stage outcomes %s: want only failed with reason Timeout", raw)
	}
}
