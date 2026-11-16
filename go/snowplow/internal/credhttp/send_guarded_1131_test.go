package credhttp

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

// TestIssue1131_EverySendPathDialsThroughTheGuard pins the claim the #nosec G107,G704
// annotations in send.go make: every cli.Do in Send's paths leaves through dialguard's destination
// guard. Each path (a write without a page budget, a write inside one, a read inside one, and a
// read without one, through the retry client), on every client chain production sends on, aimed
// at an address the policy refuses, must get the policy's refusal. A Do that bypasses the guard
// (another client, a bare transport) dials instead, and this goes RED.
func TestIssue1131_EverySendPathDialsThroughTheGuard(t *testing.T) {
	clients := x1131Clients(t)
	clients["no transport"] = &http.Client{}
	const denied = "http://169.254.169.254"
	budgeted := func() context.Context { return pagebudget.WithCustomer(context.Background(), time.Now()) }
	paths := map[string]func(t *testing.T) *http.Request{
		"a write without a budget (send.go cli.Do)": func(t *testing.T) *http.Request { return x1131Put(t, denied) },
		"a write inside a budget (sendOnceBudgeted)": func(t *testing.T) *http.Request {
			return x1131Put(t, denied).WithContext(budgeted())
		},
		"a read inside a budget (budgetedTry)":       func(t *testing.T) *http.Request { return x1131Get(t, denied).WithContext(budgeted()) },
		"a read without a budget (the retry client)": func(t *testing.T) *http.Request { return x1131Get(t, denied) },
	}
	for pname, mk := range paths {
		for cname, cli := range clients {
			t.Run(pname+"/"+cname, func(t *testing.T) {
				resp, err := Send(SendSiteStage, cli, mk(t))
				if err == nil {
					_ = resp.Body.Close()
					t.Fatal("RED: the refused address answered")
				}
				x1131RequireRefused(t, err)
			})
		}
	}
}
