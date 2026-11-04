package middleware_test

// page_budget_1115_test.go — #1115 A8 (entry): UserConfig, where every customer
// request/response route enters, gives the request its page budget (arrival +
// the 10 s goal); a background self-loopback (#903's marker) gets none.

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/server/use"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/slowcall"
)

// A7 (slow-call record, #1059) — the record opened upstream of UserConfig
// names what the budget UserConfig set did: budget=exhausted_connect.
func TestSlowCall1115_RecordCarriesTheBudgetOutcome(t *testing.T) {
	seedClientconfig1115(t)
	var buf bytes.Buffer
	var mu sync.Mutex
	log := slog.New(slog.NewJSONHandler(&lockedBuf{b: &buf, mu: &mu}, nil))
	mon := slowcall.NewMonitorForTest(0, log)
	mux := http.NewServeMux()
	// The route is resolved before the recorder runs, as tracing.NewServerHandler
	// does in main.go: an outer mux sets r.Pattern, then the recorder, then
	// UserConfig.
	mux.Handle("GET /call", mon.Middleware(middleware.UserConfig(testKeys, testAuthnNS)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, ok := pagebudget.From(r.Context())
		if !ok {
			t.Error("no budget under UserConfig")
		} else {
			b.Note(pagebudget.ExhaustedConnect)
		}
		time.Sleep(2 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))))
	r := httptest.NewRequest(http.MethodGet, "/call?x=1", nil)
	r.Header.Set("Authorization", "Bearer "+helperMakeToken(t, testUsername, time.Hour))
	mux.ServeHTTP(httptest.NewRecorder(), r)
	mu.Lock()
	out := buf.String()
	mu.Unlock()
	if !strings.Contains(out, "slowcall.request") {
		t.Fatalf("NON-VACUITY: no slow-call record emitted: %s", out)
	}
	if !strings.Contains(out, `"budget":"exhausted_connect"`) {
		t.Fatalf("the slow-call record does not carry budget=exhausted_connect: %s", out)
	}
}

type lockedBuf struct {
	b  *bytes.Buffer
	mu *sync.Mutex
}

func (l *lockedBuf) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func TestUserConfig1115_CustomerRequestGetsThePageBudget(t *testing.T) {
	seedClientconfig1115(t)

	var got *pagebudget.Budget
	var has bool
	wrapped := use.NewChain(middleware.UserConfig(testKeys, testAuthnNS)).Then(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, has = pagebudget.From(r.Context())
		w.WriteHeader(http.StatusOK)
	}))
	runBudgetCases1115(t, wrapped, &got, &has)
}

// seedClientconfig1115 serves testUsername's clientconfig from the secrets
// informer (the cache-hit path: no apiserver read).
func seedClientconfig1115(t *testing.T) {
	t.Helper()
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
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	if err := cache.StartSecretsInformer(ctx, nil, testAuthnNS); err != nil {
		t.Fatal(err)
	}
}

func runBudgetCases1115(t *testing.T, wrapped http.Handler, gotp **pagebudget.Budget, hasp *bool) {
	t.Helper()
	tok := helperMakeToken(t, testUsername, time.Hour)
	for _, tc := range []struct {
		name       string
		background bool
	}{{"customer", false}, {"background self-loopback", true}} {
		t.Run(tc.name, func(t *testing.T) {
			*gotp, *hasp = nil, false
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
				if *hasp {
					t.Fatal("a background self-loopback carries a customer page budget")
				}
				return
			}
			if !*hasp {
				t.Fatal("a customer request carries no page budget")
			}
			if d := (*gotp).Deadline().Sub(before); d < pagebudget.PageMaxGoal || d > pagebudget.PageMaxGoal+time.Second {
				t.Fatalf("deadline is arrival + %v, want + %v", d, pagebudget.PageMaxGoal)
			}
		})
	}
}
