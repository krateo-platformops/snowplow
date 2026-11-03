package middleware_test

// page_budget_1115_test.go — #1115 A8 (entry): UserConfig, where every customer
// request/response route enters, gives the request its page budget (arrival +
// the 10 s goal); a background self-loopback (#903's marker) gets none.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/server/use"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

func TestUserConfig1115_CustomerRequestGetsThePageBudget(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "true")
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	t.Setenv("TEST_MODE", "true")
	cache.ResetSecretsSnapshotForTest()
	cache.ResetSecretsInformerForTest()
	t.Cleanup(func() {
		cache.ResetSecretsSnapshotForTest()
		cache.ResetSecretsInformerForTest()
	})
	cli := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: testAuthnNS, Name: testUsername + "-clientconfig"},
		Data:       map[string][]byte{"server-url": []byte("https://alice.example/k8s"), "token": []byte("t")},
	})
	t.Cleanup(cache.SetSecretsClientForTest(cli))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cache.StartSecretsInformer(ctx, nil, testAuthnNS); err != nil {
		t.Fatal(err)
	}

	var got *pagebudget.Budget
	var has bool
	wrapped := use.NewChain(middleware.UserConfig(testKeys, testAuthnNS)).Then(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, has = pagebudget.From(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	tok := helperMakeToken(t, testUsername, time.Hour)

	for _, tc := range []struct {
		name       string
		background bool
	}{{"customer", false}, {"background self-loopback", true}} {
		t.Run(tc.name, func(t *testing.T) {
			got, has = nil, false
			r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
			r.Header.Set("Authorization", "Bearer "+tok)
			if tc.background {
				r.Header.Set(cache.BackgroundHeader, "1")
			}
			before := time.Now()
			rec := httptest.NewRecorder()
			wrapped.ServeHTTP(rec, r)
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
			}
			if tc.background {
				if has {
					t.Fatal("a background self-loopback carries a customer page budget")
				}
				return
			}
			if !has {
				t.Fatal("a customer request carries no page budget")
			}
			if d := got.Deadline().Sub(before); d < pagebudget.PageMaxGoal || d > pagebudget.PageMaxGoal+time.Second {
				t.Fatalf("deadline is arrival + %v, want + %v", d, pagebudget.PageMaxGoal)
			}
		})
	}
}
