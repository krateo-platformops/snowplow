package middleware_test

// userconfig_budget_1115_test.go — #1115 review B1: the clientconfig re-read
// on an informer miss (the login-storm path) is a network exchange of the
// customer request and runs inside its page budget. A request whose budget is
// already spent never sends the re-read and answers 504 Timeout; with budget
// left the re-read is sent as before.

import (
	"net/http"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/handlers/middleware"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

func TestUserConfig1115_ReReadOnMissRunsInsideTheBudget(t *testing.T) {
	t.Setenv("CACHE_ENABLED", "false") // the informer never serves: every request re-reads
	t.Setenv("TEST_MODE", "true")
	asked := su994APIServer(t, su994Secret(su994User+"-clientconfig", su994Session))

	// Control: budget left — the re-read is sent and the request proceeds.
	if res := su994Serve(t, su994Token(t, su994Group)); res.code != http.StatusOK || len(asked()) != 1 {
		t.Fatalf("control: status %d, %d re-read(s); want 200 and 1: %s", res.code, len(asked()), res.body)
	}

	// Spent budget: never sent, 504 Timeout.
	restore := middleware.SetPageBudgetArrivalForTest(func() time.Time { return time.Now().Add(-2 * pagebudget.PageMaxGoal) })
	t.Cleanup(restore)
	res := su994Serve(t, su994Token(t, su994Group))
	if n := len(asked()); n != 1 {
		t.Fatalf("a request with a SPENT budget sent the clientconfig re-read (%d re-reads total, want still 1)", n)
	}
	if res.code != http.StatusGatewayTimeout || res.reached {
		t.Fatalf("spent budget: status %d, reached=%v; want 504 and the request stopped: %s", res.code, res.reached, res.body)
	}
}
