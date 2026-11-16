package handlers

// export_budget_1115_test.go — #1115 B2, owner ruling (Diego): "10 s per page".
// Each page an /export re-dispatches is its own resolve with its own page
// budget, exactly like a /call of it: 40 pages of 1 s finish in 40 s, and a
// stalled page fails after its own 10 s. The page's results are raised on the
// export request's budget. A plain /call is unchanged.

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/credhttp"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

// budgetedPagedCall is a /call target that serves `total` rows in pages and,
// for each page, makes one real network exchange through credhttp.Send on the
// page's own context (so the page's budget is what bounds it). A Send that
// fails answers the page like the /call lane does a failed stage: a 504.
func budgetedPagedCall(t *testing.T, total int, upstream string, deadlines *[]time.Time) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, ok := pagebudget.From(r.Context())
		if !ok {
			t.Errorf("a page carries no budget")
		} else {
			*deadlines = append(*deadlines, b.Deadline())
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		perPage, _ := strconv.Atoi(r.URL.Query().Get("perPage"))
		if upstream != "" {
			req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream+"?page="+strconv.Itoa(page), nil)
			resp, err := credhttp.Send(credhttp.SendSiteStage, &http.Client{Transport: &http.Transport{}}, req)
			if err != nil {
				st, _ := credhttp.BudgetStatus(err)
				code := http.StatusInternalServerError
				if st != nil {
					code = st.Code
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(code)
				fmt.Fprintf(w, `{"kind":"Status","status":"Failure","code":%d}`, code)
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		start, end := min((page-1)*perPage, total), min(page*perPage, total)
		items := ""
		for i := start; i < end; i++ {
			if i > start {
				items += ","
			}
			items += fmt.Sprintf(`{"id":%d}`, i)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"items":[%s]}`, items)
	})
}

func exportReq(arrival time.Time) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/export?resource=restactions&name=demo&format=json", nil)
	return req.WithContext(pagebudget.WithCustomer(req.Context(), arrival))
}

// Arm 1 — three pages, three budgets: each page's deadline is its own start
// + the goal, so page 3's is later than page 1's (not the request's).
func TestExport1115_EachPageHasItsOwnBudget(t *testing.T) {
	setExportPaging(t, 3, 100)
	var deadlines []time.Time
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(30 * time.Millisecond)
	}))
	t.Cleanup(slow.Close)
	arrival := time.Now()
	rec := httptest.NewRecorder()
	Export(budgetedPagedCall(t, 7, slow.URL, &deadlines)).ServeHTTP(rec, exportReq(arrival))
	if rec.Code != http.StatusOK || len(deadlines) != 3 {
		t.Fatalf("status %d after %d pages; want 200 and 3", rec.Code, len(deadlines))
	}
	if !deadlines[2].After(deadlines[0]) {
		t.Fatalf("page 3 deadline %v is not after page 1's %v: the pages share one budget", deadlines[2], deadlines[0])
	}
	if !deadlines[0].After(arrival.Add(pagebudget.PageMaxGoal)) && !deadlines[0].Equal(arrival.Add(pagebudget.PageMaxGoal)) {
		t.Fatalf("page 1 deadline %v is before the request's own", deadlines[0])
	}
}

// Arm 2 — 40 pages of 1 s each (40 s, four page budgets' worth) complete.
func TestExport1115_FortyOneSecondPagesComplete(t *testing.T) {
	if testing.Short() {
		t.Skip("40 s of real exchanges")
	}
	setExportPaging(t, 1, 100)
	var deadlines []time.Time
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(time.Second)
	}))
	t.Cleanup(second.Close)
	rec := httptest.NewRecorder()
	start := time.Now()
	Export(budgetedPagedCall(t, 39, second.URL, &deadlines)).ServeHTTP(rec, exportReq(start))
	if rec.Code != http.StatusOK {
		t.Fatalf("a 40 × 1 s export answered %d after %v (%d pages): %s", rec.Code, time.Since(start), len(deadlines), rec.Body.String())
	}
	if len(deadlines) != 40 {
		t.Fatalf("%d pages, want 40 (39 full pages and the short last one)", len(deadlines))
	}
}

// Arm 3 — a stalled page is cut at ITS 10 s (the page clock is injected so the
// cut lands 300 ms into the page) and the export answers as it does today for
// a failed page: the page's failure is passed through.
func TestExport1115_StalledPageCutsAtItsOwnBudget(t *testing.T) {
	setExportPaging(t, 3, 100)
	release := make(chan struct{})
	stall := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("page") == "2" {
			<-release
		}
	}))
	t.Cleanup(func() { close(release); stall.Close() })
	restore := pagebudget.SetResolveClockForTest(func() time.Time {
		return time.Now().Add(-pagebudget.PageMaxGoal + 300*time.Millisecond)
	})
	t.Cleanup(restore)
	var deadlines []time.Time
	req := exportReq(time.Now())
	rec := httptest.NewRecorder()
	start := time.Now()
	Export(budgetedPagedCall(t, 7, stall.URL, &deadlines)).ServeHTTP(rec, req)
	took := time.Since(start)
	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("export with a stalled page answered %d, want the page's 504 passed through: %s", rec.Code, rec.Body.String())
	}
	if took > 3*time.Second || len(deadlines) != 2 {
		t.Fatalf("took %v over %d pages; want the cut at page 2's own budget", took, len(deadlines))
	}
	if got := pagebudget.Outcome(req.Context()); got != pagebudget.ExhaustedAnswer {
		t.Fatalf("the export request's outcome is %v, want the page's exhausted_answer raised on it", got)
	}
}

// Arm 4 — a plain /call is unchanged: outside a re-dispatch nothing restarts
// the budget, and ForResolve without a customer budget is a no-op.
func TestExport1115_PlainCallKeepsItsArrivalBudget(t *testing.T) {
	arrival := time.Now().Add(-time.Second)
	ctx := pagebudget.WithCustomer(context.Background(), arrival)
	var got time.Time
	h := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		b, _ := pagebudget.From(r.Context())
		got = b.Deadline()
	})
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/call", nil).WithContext(ctx))
	if !got.Equal(arrival.Add(pagebudget.PageMaxGoal)) {
		t.Fatalf("a plain /call's deadline %v, want arrival + goal", got)
	}
	if bg := pagebudget.ForResolve(context.Background()); bg != context.Background() {
		t.Fatal("ForResolve gave background work a budget")
	}
}
