package schema

// fallback_budget_1115_test.go — #1115 final sweep, revised by #1318: when the
// SA discovery singleton refuses the config (an impersonating rc, or nil), the
// CRD GET falls back to a per-call dynamic.NewClient whose mapper discovers on
// client-go's own context.TODO(). #1115 bound that client to the request's page
// budget. Since #1318 the CRD read is the memo's shared refill, run under a
// DETACHED fill ctx (schema_fill.go), so the fallback client serves the flight,
// not the request: with the request's budget spent, the miss is still sent (and
// the memo refills when the CRD exists, TestIssue1318_SpentBudgetMissStillRefills).

import (
	"context"
	"errors"
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

func TestSchemaFallback1115_PerCallClientServesTheDetachedFill(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	cache.ResetPluralsStoreForTest()
	t.Cleanup(cache.ResetPluralsStoreForTest)
	resetCRDSchemaMemoForTest()
	t.Cleanup(resetCRDSchemaMemoForTest)
	resetCRDSchemaFlightsForTest()
	t.Cleanup(resetCRDSchemaFlightsForTest)

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
	// are the fallback client's — the fill's, outside the request's budget.
	resetCRDSchemaMemoForTest()
	spent := pagebudget.WithCustomer(context.Background(), time.Now().Add(-2*pagebudget.PageMaxGoal))
	err := ValidateObjectStatus(spent, rc, obj)
	if err == nil || n.Load() == warm {
		t.Fatalf("SPENT budget: err %v after %d new request(s); want the fill sent (the CRD is absent: an error)", err, n.Load()-warm)
	}
	if errors.Is(err, pagebudget.ErrExhausted) {
		t.Fatalf("SPENT budget: %v; the fill was cut by the request's budget", err)
	}
}
