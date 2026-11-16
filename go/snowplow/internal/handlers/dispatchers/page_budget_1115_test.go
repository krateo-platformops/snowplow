package dispatchers

// page_budget_1115_test.go — #1115 arms at the dispatcher:
//   - A3: a known-stale cell's #547 demand revalidation runs OUTSIDE the
//     customer's page budget (pagebudget.Detach), so a refresh whose connect
//     hangs is never failed by the deadline of the request that was already
//     served the stale copy;
//   - A7: the per-call record carries the budget outcome.

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

func spentCustomerRequest() *http.Request {
	// A customer request whose budget is already spent: if the revalidation
	// inherited it, every exchange it made would fail at once (not_sent).
	ctx := pagebudget.WithCustomer(context.Background(), time.Now().Add(-2*pagebudget.PageMaxGoal))
	return httptest.NewRequest(http.MethodGet, "/call?x=1", nil).WithContext(ctx)
}

func TestDemandRevalidation1115_A3_RunsOutsideTheCustomersBudget(t *testing.T) {
	req := spentCustomerRequest()
	key := "k1115-" + t.Name()
	tmpl := setDemandTemplate(key, req)
	t.Cleanup(func() { demandTemplates.Delete(key) })
	if _, ok := pagebudget.From(currentDemandTemplate(key, tmpl).Context()); ok {
		t.Fatal("the demand-revalidation template carries the served request's page budget")
	}

	var inAttempt context.Context
	h := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { inAttempt = r.Context() })
	demandRevalidateOnce(h, req, key, time.Now(), "test")
	if inAttempt == nil {
		t.Fatal("the attempt did not run")
	}
	if _, ok := pagebudget.From(inAttempt); ok {
		t.Fatal("a demand revalidation runs under the served customer's page budget")
	}
	if dl, ok := inAttempt.Deadline(); !ok || time.Until(dl) < time.Minute {
		t.Fatalf("the attempt's own bound is %v, want the write deadline (minutes)", dl)
	}
}

func TestPerCall1115_A7_RecordCarriesTheBudgetOutcome(t *testing.T) {
	var buf syncBuffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	ctx := pagebudget.WithCustomer(context.Background(), time.Now())
	b, _ := pagebudget.From(ctx)
	_, emit := beginPerCall(httptest.NewRequest(http.MethodGet, "/call", nil).WithContext(ctx), "restactions")
	b.Note(pagebudget.ExhaustedConnect)
	emit()
	_, emitBG := beginPerCall(httptest.NewRequest(http.MethodGet, "/call", nil), "widgets")
	emitBG()

	out := buf.String()
	if !strings.Contains(out, `"budget":"exhausted_connect"`) {
		t.Fatalf("the customer's per-call record does not carry budget=exhausted_connect: %s", out)
	}
	if !strings.Contains(out, `"budget":"none"`) {
		t.Fatalf("a request without a budget does not read budget=none: %s", out)
	}
}
