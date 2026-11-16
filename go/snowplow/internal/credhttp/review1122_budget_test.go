package credhttp

// review1122_budget_test.go — the #1122 reviewer's arms, adopted (#1115):
// R1 client-go Retry-After inside the budget, R2/R3 the budgeted 5xx retry,
// R4/R5 Retry-After honoured only when it fits, R6 the body readable after
// Send (they turn the review's R23 / R24 / R29 mutations RED).

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// R1 — a client-go client (Harden: BudgetTransport) on a customer request,
// against a server that always answers 429 Retry-After: 2. How long does the
// customer wait when only 300 ms of budget is left?
func TestReview1122_R1_ClientGoRetryAfterOverrunsTheBudget(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	dc, err := Dynamic(&rest.Config{Host: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx := budgetLeft(300 * time.Millisecond)
	start := time.Now()
	_, err = dc.Resource(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}).Namespace("ns").Get(ctx, "x", metav1.GetOptions{})
	took := time.Since(start)
	t.Logf("took=%v requests=%d err=%v outcome=%v", took, n.Load(), err, pagebudget.Outcome(ctx))
	if took > time.Second {
		t.Fatalf("a customer client-go read waited %v with 300ms of budget left (client-go's Retry-After sleep is outside the budget)", took)
	}
}

// R2 — the budgeted 5xx retry (#1115 §3: one retry after RetryClient's first
// backoff). A 503 then a 200: answered, 2 requests, outcome retried.
func TestReview1122_R2_Budgeted5xxRetriedOnce(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	seedReserve(t, 10*time.Millisecond)
	ctx := budgetLeft(5 * time.Second)
	resp, err := Send(SendSiteStage, srv.Client(), getReq(t, ctx, srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 || n.Load() != 2 || pagebudget.Outcome(ctx) != pagebudget.Retried {
		t.Fatalf("status %d, %d requests, outcome %v; want 200, 2, retried", resp.StatusCode, n.Load(), pagebudget.Outcome(ctx))
	}
}

// R3 — a 503 forever: exactly 2 requests (not RetryClient's 6).
func TestReview1122_R3_Budgeted5xxIsTwoAttemptsNotSix(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	seedReserve(t, 10*time.Millisecond)
	ctx := budgetLeft(5 * time.Second)
	resp, err := Send(SendSiteStage, srv.Client(), getReq(t, ctx, srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if n.Load() != 2 {
		t.Fatalf("%d requests, want 2", n.Load())
	}
}

// R4 — a 429 whose Retry-After does not fit the budget is NOT retried; the 429
// is returned at once (no sleep past the deadline).
func TestReview1122_R4_429RetryAfterBeyondBudgetNotWaited(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	t.Cleanup(srv.Close)
	seedReserve(t, 10*time.Millisecond)
	ctx := budgetLeft(2 * time.Second)
	start := time.Now()
	resp, err := Send(SendSiteStage, srv.Client(), getReq(t, ctx, srv.URL))
	took := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if n.Load() != 1 || resp.StatusCode != 429 || took > time.Second {
		t.Fatalf("%d requests, status %d, took %v; want 1, 429, at once", n.Load(), resp.StatusCode, took)
	}
}

// R5 — a 429 Retry-After: 1 that fits: retried once after ~1 s.
func TestReview1122_R5_429RetryAfterInsideBudgetRetried(t *testing.T) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	seedReserve(t, 10*time.Millisecond)
	ctx := budgetLeft(5 * time.Second)
	start := time.Now()
	resp, err := Send(SendSiteStage, srv.Client(), getReq(t, ctx, srv.URL))
	took := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if n.Load() != 2 || resp.StatusCode != 200 || took < 900*time.Millisecond {
		t.Fatalf("%d requests, status %d, took %v; want 2, 200, after ~1s", n.Load(), resp.StatusCode, took)
	}
}

// R6 — the body of a budgeted answer is readable after Send returns (the
// attempt's ctx ends on Close, not before).
func TestReview1122_R6_BodyReadableAfterSend(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.(http.Flusher).Flush()
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	seedReserve(t, 10*time.Millisecond)
	ctx := budgetLeft(5 * time.Second)
	resp, err := Send(SendSiteStage, srv.Client(), getReq(t, ctx, srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b := make([]byte, 64)
	k, rerr := resp.Body.Read(b)
	for rerr == nil && k == 0 {
		k, rerr = resp.Body.Read(b)
	}
	if string(b[:k]) != `{"ok":true}` {
		t.Fatalf("body %q err %v", b[:k], rerr)
	}
}
