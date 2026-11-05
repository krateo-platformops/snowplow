package objects

// review1122_budget_test.go — the #1122 reviewer's B1 arm, adopted (#1115):
// objects.getFromAPIServer (the apiserver fall-through of a widget/CR GET on a
// customer /call) carries the request's page budget. A request whose budget is
// already spent is never sent (credhttp's BudgetTransport answers not_sent),
// and the result is the 504 Timeout envelope.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	templatesv1 "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func review1122Server(t *testing.T) (*httptest.Server, *atomic.Int32) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x","namespace":"ns"}}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func spentBudgetCtx(srvURL string) context.Context {
	ctx := xcontext.BuildContext(context.Background(),
		xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srvURL, Token: "t"}))
	ctx = cache.WithInternalRESTConfig(ctx, &rest.Config{Host: srvURL})
	return pagebudget.WithCustomer(ctx, time.Now().Add(-2*pagebudget.PageMaxGoal))
}

// The fall-through itself.
func TestReview1122_ObjectsFallthroughCarriesTheBudget(t *testing.T) {
	srv, n := review1122Server(t)
	ctx := spentBudgetCtx(srv.URL)
	res := getFromAPIServer(ctx, templatesv1.ObjectReference{
		Reference: templatesv1.Reference{Name: "x", Namespace: "ns"}, APIVersion: "v1", Resource: "configmaps",
	})
	t.Logf("requests received=%d err=%v outcome=%v", n.Load(), res.Err, pagebudget.Outcome(ctx))
	if n.Load() != 0 {
		t.Fatalf("a customer request with a SPENT budget sent %d request(s) to the apiserver: the budget never reached the round trip", n.Load())
	}
	if res.Err == nil || res.Err.Code != http.StatusGatewayTimeout || pagebudget.Outcome(ctx) != pagebudget.NotSent {
		t.Fatalf("result %+v outcome %v, want a 504 and not_sent", res.Err, pagebudget.Outcome(ctx))
	}
}

// Control: the same client chain, with the request ctx passed through, honours it.
func TestReview1122_ObjectsFallthroughControl(t *testing.T) {
	srv, n := review1122Server(t)
	ctx := spentBudgetCtx(srv.URL)
	dc, err := credhttp.Dynamic(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	_, err = dc.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace("ns").Get(ctx, "x", metav1.GetOptions{})
	t.Logf("control: requests received=%d err=%v outcome=%v", n.Load(), err, pagebudget.Outcome(ctx))
	if n.Load() != 0 || err == nil {
		t.Fatalf("control: the budgeted ctx was not honoured (%d sent, err %v)", n.Load(), err)
	}
}
