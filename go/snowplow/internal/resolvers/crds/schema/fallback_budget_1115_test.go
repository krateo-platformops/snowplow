package schema

// fallback_budget_1115_test.go — #1115 final sweep: when the SA discovery
// singleton refuses the config (an impersonating rc, or nil), the CRD GET
// falls back to a per-call dynamic.NewClient whose mapper discovers on
// client-go's own context.TODO(). That client is bound to the request's page
// budget: with the budget spent, the fallback sends nothing; without a budget
// (background work) it sends as before.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"k8s.io/client-go/rest"
)

func fallbackBudgetServer(t *testing.T) (*rest.Config, *atomic.Int32) {
	t.Helper()
	var n atomic.Int32
	docs := map[string]string{
		"/api":    `{"kind":"APIVersions","versions":["v1"]}`,
		"/api/v1": `{"kind":"APIResourceList","groupVersion":"v1","resources":[]}`,
		"/apis": `{"kind":"APIGroupList","apiVersion":"v1","groups":[` +
			`{"name":"budget1115.example.io","versions":[{"groupVersion":"budget1115.example.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"budget1115.example.io/v1","version":"v1"}},` +
			`{"name":"apiextensions.k8s.io","versions":[{"groupVersion":"apiextensions.k8s.io/v1","version":"v1"}],"preferredVersion":{"groupVersion":"apiextensions.k8s.io/v1","version":"v1"}}]}`,
		"/apis/budget1115.example.io/v1": `{"kind":"APIResourceList","groupVersion":"budget1115.example.io/v1","resources":[{"name":"probes","singularName":"probe","namespaced":true,"kind":"Probe","verbs":["get","list"]}]}`,
		"/apis/apiextensions.k8s.io/v1":  `{"kind":"APIResourceList","groupVersion":"apiextensions.k8s.io/v1","resources":[{"name":"customresourcedefinitions","singularName":"customresourcedefinition","namespaced":false,"kind":"CustomResourceDefinition","verbs":["get","list"]}]}`,
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if d, ok := docs[r.URL.Path]; ok {
			_, _ = io.WriteString(w, d)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"kind":"Status","apiVersion":"v1","status":"Failure","code":404,"reason":"NotFound"}`)
	}))
	t.Cleanup(srv.Close)
	// Impersonating: the SA singleton refuses it, so the per-call fallback runs.
	return &rest.Config{Host: srv.URL, Impersonate: rest.ImpersonationConfig{UserName: "budget-1115-probe"}}, &n
}

func TestSchemaFallback1115_PerCallClientRunsInsideTheBudget(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetPluralsStoreForTest()
	t.Cleanup(cache.ResetPluralsStoreForTest)
	resetCRDSchemaMemoForTest()
	t.Cleanup(resetCRDSchemaMemoForTest)

	rc, n := fallbackBudgetServer(t)
	obj := map[string]any{
		"apiVersion": "budget1115.example.io/v1",
		"kind":       "Probe",
		"metadata":   map[string]any{"name": "p", "namespace": "ns"},
		"status":     map[string]any{widgetDataKey: map[string]any{}},
	}

	// Control, no budget: the plurals miss and the fallback's mapper and CRD
	// GET all send (the CRD is absent, so the call errors after sending).
	_ = ValidateObjectStatus(context.Background(), rc, obj)
	warm := n.Load()
	if warm == 0 {
		t.Fatalf("control: 0 requests sent; the fixture did not reach the server")
	}

	// Spent budget: the plural is now stored (no send), so the only sends left
	// are the fallback client's — bound, they are refused before the wire.
	resetCRDSchemaMemoForTest()
	spent := pagebudget.WithCustomer(context.Background(), time.Now().Add(-2*pagebudget.PageMaxGoal))
	err := ValidateObjectStatus(spent, rc, obj)
	if err == nil || n.Load() != warm {
		t.Fatalf("SPENT budget: err %v after %d new request(s); want an error and 0 sent", err, n.Load()-warm)
	}
}
