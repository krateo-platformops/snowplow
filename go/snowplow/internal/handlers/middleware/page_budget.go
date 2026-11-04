package middleware

// page_budget.go — #1115: where a customer request's page budget is set.

import (
	"net/http"
	"time"

	"github.com/krateo-platformops/snowplow/internal/cache"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"github.com/krateo-platformops/snowplow/internal/slowcall"
)

// withCustomerPageBudget gives a customer request its page budget: arrival +
// pagebudget.PageMaxGoal, shared by every network exchange it makes (credhttp).
// A background self-loopback — a non-customer resolve's hop, which carries
// cache.BackgroundHeader (#903) — gets none: no page waits on it. A forged
// header only takes the bound off the forger's own request, which is today's
// behaviour for it.
func withCustomerPageBudget(req *http.Request) *http.Request {
	if req.Header.Get(cache.BackgroundHeader) == "1" {
		return req
	}
	ctx := pagebudget.WithCustomer(req.Context(), time.Now())
	// The slow-call record (#1054) is opened upstream, before this ctx exists:
	// hand it the budget so its "budget" field reads what the budget did.
	slowcall.AttachBudget(ctx)
	return req.WithContext(ctx)
}
