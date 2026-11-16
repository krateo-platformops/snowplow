package api

// discovery_budget_1115_test.go — #1122 re-review N1: the discovery stage's
// group/version read and the /rbac read-set enumeration send on the request's
// ctx (client-go's discovery helpers send on context.TODO()), so a spent page
// budget sends nothing and the stage reason reads Timeout. The shared
// discovery client memoises the client, not results: without a budget every
// call is a GET (the control).

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	httpcall "github.com/krateo-platformops/plumbing/http/request"
	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"k8s.io/client-go/rest"
)

// countingDiscoveryServer answers /api, /apis and every group/version with a
// minimal discovery document, counting requests.
func countingDiscoveryServer(t *testing.T) (*rest.Config, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api":
			_, _ = io.WriteString(w, `{"kind":"APIVersions","versions":["v1"]}`)
		case "/apis":
			_, _ = io.WriteString(w, `{"kind":"APIGroupList","apiVersion":"v1","groups":[{"name":"composition.krateo.io","versions":[{"groupVersion":"composition.krateo.io/v1","version":"v1"}]}]}`)
		default:
			_, _ = io.WriteString(w, `{"kind":"APIResourceList","apiVersion":"v1","groupVersion":"composition.krateo.io/v1","resources":[{"name":"fireworksapps","singularName":"fireworksapp","namespaced":true,"kind":"FireworksApp","verbs":["get","list","watch"]}]}`)
		}
	}))
	t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL, BearerToken: "fake-sa-jwt", TLSClientConfig: rest.TLSClientConfig{CAData: pemEncodeCert(srv.Certificate())}}, &n
}

func spentBudget(ctx context.Context) context.Context {
	return pagebudget.WithCustomer(ctx, time.Now().Add(-2*pagebudget.PageMaxGoal))
}

func TestDiscovery1115_N1_GroupVersionStageRunsInsideTheBudget(t *testing.T) {
	rc, n := countingDiscoveryServer(t)
	withDiscoverySARESTConfig(t, rc)
	call := httpcall.RequestOptions{RequestInfo: httpcall.RequestInfo{Path: "/apis/composition.krateo.io/v1"}}

	// Control: no budget, two calls, two GETs (the client is memoised, not the
	// answer).
	for range 2 {
		if _, served, err := dispatchViaDiscovery(ownClusterCtx(context.Background()), call); err != nil || !served {
			t.Fatalf("control: served=%v err=%v", served, err)
		}
	}
	if n.Load() != 2 {
		t.Fatalf("control: %d requests for 2 calls, want 2", n.Load())
	}

	_, _, err := dispatchViaDiscovery(spentBudget(ownClusterCtx(context.Background())), call)
	if n.Load() != 2 {
		t.Fatalf("a group/version discovery stage with a SPENT budget sent %d request(s)", n.Load()-2)
	}
	var be *pagebudget.ExhaustedError
	if !errors.As(err, &be) {
		t.Fatalf("err = %v, want the page budget's ExhaustedError", err)
	}
	if got := stageReasonOf(itemErrorValue(err)); got != StageReasonTimeout {
		t.Fatalf("stage reason %q, want Timeout", got)
	}
}

func TestDiscovery1115_N1_RBACReadSetRunsInsideTheBudget(t *testing.T) {
	rc, n := countingDiscoveryServer(t)
	withInspectSARESTConfig(t, rc)
	ra := &templates.RESTAction{Spec: templates.RESTActionSpec{API: []*templates.API{
		{Name: "apps", Path: "/apis/composition.krateo.io/v1/fireworksapps"},
		{Name: "uaf", Path: "/apis/composition.krateo.io/v1/fireworksapps", UserAccessFilter: &templates.UserAccessFilterSpec{
			Verb: "list", Group: "composition.krateo.io", Resource: "fireworksapps"}},
	}}}

	// Control: the read set resolves with requests sent.
	if _, _, err := InspectReadSet(context.Background(), ra, nil); err != nil || n.Load() == 0 {
		t.Fatalf("control: err=%v after %d requests", err, n.Load())
	}
	before := n.Load()
	_, _, _ = InspectReadSet(spentBudget(context.Background()), ra, nil)
	if sent := n.Load() - before; sent != 0 {
		t.Fatalf("/rbac enumeration with a SPENT budget sent %d discovery request(s), want 0", sent)
	}
}
