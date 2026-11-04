package evaltest

// sar_budget_1115_test.go — #1115 review B1: the cache-off SAR baseline
// (rbac.go selfSubjectAccessReview) is a network exchange of the customer
// request and runs inside its page budget. Driven through rbac.UserCan over
// the real credhttp clientset chain (BudgetTransport) against an httptest
// apiserver: a spent budget sends no SelfSubjectAccessReview and denies; with
// budget left the review is sent as before.

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/jwtutil"
	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/rbac"
	authv1 "k8s.io/api/authorization/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

func TestSAR1115_BaselineRunsInsideTheBudget(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(authv1.SelfSubjectAccessReview{Status: authv1.SubjectAccessReviewStatus{Allowed: true}})
	}))
	t.Cleanup(srv.Close)
	restore := rbac.SetSARClientsetForTest(func(_ context.Context, _ endpoints.Endpoint) (kubernetes.Interface, error) {
		return credhttp.Kubernetes(&rest.Config{Host: srv.URL})
	})
	t.Cleanup(restore)
	t.Setenv("CACHE_ENABLED", "false")
	t.Setenv("TEST_MODE", "true")

	check := func(arrival time.Time) bool {
		ctx := xcontext.BuildContext(context.Background(),
			xcontext.WithUserInfo(jwtutil.UserInfo{Username: "alice", Groups: []string{"devs"}}),
			xcontext.WithUserConfig(endpoints.Endpoint{ServerURL: srv.URL, Token: "alice"}))
		return rbac.UserCan(pagebudget.WithCustomer(ctx, arrival), rbac.UserCanOptions{
			Verb: "get", GroupResource: schema.GroupResource{Resource: "configmaps"}, Namespace: "ns", Name: "x",
		})
	}
	if !check(time.Now()) || n.Load() != 1 {
		t.Fatalf("control: allowed=false or %d reviews sent; want allowed and 1", n.Load())
	}
	if check(time.Now().Add(-2*pagebudget.PageMaxGoal)) || n.Load() != 1 {
		t.Fatalf("a request with a SPENT budget sent the SelfSubjectAccessReview (%d total, want still 1) or was allowed", n.Load())
	}
}
