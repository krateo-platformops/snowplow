package api

// page_budget_r3_1115_test.go — #1122 review R3: a budget cut reads Timeout on
// EVERY branch. The branches that record the error's text (in-process
// resolve, content serve, internal rest config, discovery) record the 504
// Timeout Status instead, the value branch E records; any other error keeps
// its text.

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

func TestBudget1115_R3_EveryBranchRecordsTheTimeoutStatus(t *testing.T) {
	cut := fmt.Errorf("dispatch: %w", &url.Error{Op: "Get", URL: "https://h.example/x",
		Err: &pagebudget.ExhaustedError{Result: pagebudget.ExhaustedAnswer, Cause: "connection", Attempts: 1}})
	v := itemErrorValue(cut)
	m, ok := v.(map[string]any)
	if !ok || statusCodeOf(m["code"]) != http.StatusGatewayTimeout || m["reason"] != credhttp.BudgetStatusReason {
		t.Fatalf("errorKey value of a budget cut = %#v, want the 504 Timeout Status", v)
	}
	if strings.Contains(fmt.Sprint(m["message"]), "h.example") {
		t.Fatalf("the value names the host: %v", m["message"])
	}
	if got := stageReasonOf(v); got != StageReasonTimeout {
		t.Fatalf("stage reason %q, want Timeout", got)
	}
	if v := itemErrorValue(errors.New("boom")); v != "boom" {
		t.Fatalf("a non-budget error's value = %#v, want its text", v)
	}
}
