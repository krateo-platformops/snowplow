package cache

// plurals_budget_1115_test.go — #1122 third review F2: the plurals resolver's
// discovery miss (PluralFor / KindForGVR, behind /plurals, widget
// resourcesRefs and the CRD schema resolver) runs inside the request's page
// budget. Its client is built per miss and bound to the request's budget
// (credhttp.BindRequestBudget), since client-go's ServerResourcesForGroupVersion
// sends on context.TODO(). A spent budget sends nothing; a request without a
// budget (background work) resolves as before.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

func pluralsBudgetServer(t *testing.T) (*rest.Config, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"kind":"APIResourceList","groupVersion":"budget1115.example.io/v1","resources":[{"name":"probes","singularName":"probe","namespaced":true,"kind":"Probe","verbs":["get","list"]}]}`)
	}))
	t.Cleanup(srv.Close)
	return &rest.Config{Host: srv.URL}, &n
}

func TestPlurals1115_DiscoveryMissRunsInsideTheBudget(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	ResetPluralsStoreForTest()
	t.Cleanup(ResetPluralsStoreForTest)
	gvk := schema.GroupVersionKind{Group: "budget1115.example.io", Version: "v1", Kind: "Probe"}
	gvr := schema.GroupVersionResource{Group: "budget1115.example.io", Version: "v1", Resource: "probes"}
	spent := pagebudget.WithCustomer(context.Background(), time.Now().Add(-2*pagebudget.PageMaxGoal))

	rc, n := pluralsBudgetServer(t)
	if _, err := PluralFor(spent, gvk, rc); err == nil || n.Load() != 0 {
		t.Fatalf("PluralFor with a SPENT budget: err %v after %d request(s); want an error and 0 sent", err, n.Load())
	}
	if _, err := KindForGVR(spent, gvr, rc); err == nil || n.Load() != 0 {
		t.Fatalf("KindForGVR with a SPENT budget: err %v after %d request(s); want an error and 0 sent", err, n.Load())
	}

	// Control: no budget — the same miss resolves with one request, and the
	// failed budgeted attempts cached nothing.
	info, err := PluralFor(context.Background(), gvk, rc)
	if err != nil || info.Plural != "probes" || n.Load() != 1 {
		t.Fatalf("control: info %+v err %v after %d request(s); want probes after 1", info, err, n.Load())
	}
}
