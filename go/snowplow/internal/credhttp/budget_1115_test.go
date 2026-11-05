package credhttp

// budget_1115_test.go — #1115 arms: a customer request's network exchanges
// share the page's 10 s goal budget (owner ruling 2026-11-03). The budget's
// start is injected so a test leaves only a few hundred ms of it; the dialer is
// injected (it reports ConnectStart/ConnectDone like net.Dialer, so the #1101
// observer sees the phase).

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/krateo-platformops/plumbing/endpoints"
	"github.com/krateo-platformops/plumbing/http/request"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/ptr"
	"github.com/krateo-platformops/snowplow/internal/pagebudget"
)

// budgetLeft is a customer ctx whose page budget has `left` remaining.
func budgetLeft(left time.Duration) context.Context {
	return pagebudget.WithCustomer(context.Background(), time.Now().Add(left-pagebudget.PageMaxGoal))
}

// seedReserve sets the retry reserve to d (a full window of samples d), or
// clears it (d == 0: no sample measured).
func seedReserve(t *testing.T, d time.Duration) {
	t.Helper()
	freshAnswers.resetForTest()
	t.Cleanup(freshAnswers.resetForTest)
	for range reserveWindow {
		if d > 0 {
			freshAnswers.record(d)
		}
	}
}

// scriptedDialer is an injected dialer. Each dial takes the next step of its
// script ("hang", "refuse", "ok", or "slow:<dur>" = connect after dur); past the
// script it repeats the last step. A hung dial honours its ctx, and is released
// at test end, so nothing leaks.
type scriptedDialer struct {
	addr   string
	script []string
	dials  atomic.Int32
	done   chan struct{}
	once   sync.Once
	ctxDL  []time.Time
	mu     sync.Mutex
}

func newScriptedDialer(t *testing.T, addr string, script ...string) *scriptedDialer {
	d := &scriptedDialer{addr: addr, script: script, done: make(chan struct{})}
	t.Cleanup(func() { d.once.Do(func() { close(d.done) }) })
	return d
}

func (d *scriptedDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	i := int(d.dials.Add(1)) - 1
	step := d.script[min(i, len(d.script)-1)]
	dl, _ := ctx.Deadline()
	d.mu.Lock()
	d.ctxDL = append(d.ctxDL, dl)
	d.mu.Unlock()
	trace := httptrace.ContextClientTrace(ctx)
	if trace != nil && trace.ConnectStart != nil {
		trace.ConnectStart(network, d.addr)
	}
	end := func(err error) {
		if trace != nil && trace.ConnectDone != nil {
			trace.ConnectDone(network, d.addr, err)
		}
	}
	wait := time.Duration(0)
	switch {
	case step == "hang":
		select {
		case <-ctx.Done():
			end(ctx.Err())
			return nil, ctx.Err()
		case <-d.done:
			return nil, errors.New("released")
		}
	case step == "refuse":
		err := &net.OpError{Op: "dial", Net: network, Err: errors.New("connection refused")}
		end(err)
		return nil, err
	case strings.HasPrefix(step, "slow:"):
		wait, _ = time.ParseDuration(strings.TrimPrefix(step, "slow:"))
	}
	if wait > 0 {
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			end(ctx.Err())
			return nil, ctx.Err()
		case <-d.done:
			return nil, errors.New("released")
		}
	}
	c, err := (&net.Dialer{}).DialContext(context.Background(), network, d.addr)
	end(err)
	return c, err
}

func okServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	var n atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &n
}

func clientOn(d *scriptedDialer) *http.Client {
	return &http.Client{Transport: &http.Transport{DialContext: d.DialContext, DisableKeepAlives: false}}
}

func getReq(t *testing.T, ctx context.Context, url string) *http.Request {
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func asExhausted(t *testing.T, err error) *pagebudget.ExhaustedError {
	t.Helper()
	var e *pagebudget.ExhaustedError
	if !errors.As(err, &e) {
		t.Fatalf("err = %v (%T), want a *pagebudget.ExhaustedError", err, err)
	}
	if !errors.Is(err, pagebudget.ErrExhausted) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("%v does not match ErrExhausted and DeadlineExceeded", err)
	}
	return e
}

// A1 — a hung connect is abandoned at the budget, not after 30 s.
func TestBudget1115_A1_HungConnectAbandonedAtTheBudget(t *testing.T) {
	srv, _ := okServer(t)
	seedReserve(t, 0) // no sample: reserve = PageP95Goal (3 s) > what is left → no cut, no retry
	d := newScriptedDialer(t, srv.Listener.Addr().String(), "hang")
	ctx := budgetLeft(300 * time.Millisecond)
	start := time.Now()
	resp, err := Send(SendSiteStage, clientOn(d), getReq(t, ctx, srv.URL))
	took := time.Since(start)
	if resp != nil {
		t.Fatalf("a hung connect answered %d", resp.StatusCode)
	}
	e := asExhausted(t, err)
	if e.Result != pagebudget.ExhaustedConnect || e.Cause != RetryCauseDial {
		t.Fatalf("result %v cause %q, want exhausted_connect / dial (the phase-start mark)", e.Result, e.Cause)
	}
	if took > 2*time.Second {
		t.Fatalf("the hung connect held the request %v; the budget left 300ms", took)
	}
	if n := d.dials.Load(); n != 1 {
		t.Fatalf("%d dials, want 1: no retry fits", n)
	}
	if got := pagebudget.Outcome(ctx); got != pagebudget.ExhaustedConnect {
		t.Fatalf("request outcome %v, want exhausted_connect", got)
	}
	if strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatalf("the error names the host: %v", err)
	}
}

// A1 (dial) — the owned dialer bounds every connect at its start + the page
// goal, not client-go's 30 s, whatever request's values its ctx carries.
func TestBudget1115_A1_OwnedDialerGivesUpAtTheGoal(t *testing.T) {
	d := newScriptedDialer(t, "127.0.0.1:1", "hang")
	prevFn, prevNow := endpointDialFn, dialNow
	endpointDialFn = d.DialContext
	start := time.Now()
	dialNow = func() time.Time { return time.Now().Add(-pagebudget.PageMaxGoal + 300*time.Millisecond) }
	t.Cleanup(func() { endpointDialFn, dialNow = prevFn, prevNow })

	// The ctx carries a customer budget that ends long after: the dial must
	// not take it (nor be cut sooner by an earlier one, see A2d).
	ctx := budgetLeft(pagebudget.PageMaxGoal)
	done := make(chan error, 1)
	go func() { _, err := EndpointDialHolder.Dial(ctx, "tcp", "127.0.0.1:1"); done <- err }()
	select {
	case err := <-done:
		if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("dial err = %v, want its goal deadline", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the owned dialer did not give up at its start + PageMaxGoal")
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("dial took %v", took)
	}
	d.mu.Lock()
	dl := d.ctxDL[0]
	d.mu.Unlock()
	if want := start.Add(300 * time.Millisecond); dl.Sub(want).Abs() > 200*time.Millisecond {
		t.Fatalf("dial deadline %v, want ≈ start + goal (%v)", dl, want)
	}
}

// A2a — a hung first connect is abandoned at deadline − reserve and the one
// retry, on a fresh connection, answers inside the budget.
func TestBudget1115_A2a_HungConnectRetriedOnceInsideTheBudget(t *testing.T) {
	srv, served := okServer(t)
	seedReserve(t, 150*time.Millisecond)
	d := newScriptedDialer(t, srv.Listener.Addr().String(), "hang", "ok")
	before := retryCell(SendSiteStage, RetryCauseDial)
	ctx := budgetLeft(900 * time.Millisecond)
	start := time.Now()
	resp, err := Send(SendSiteStage, clientOn(d), getReq(t, ctx, srv.URL))
	if err != nil {
		t.Fatalf("send failed: %v", err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	took := time.Since(start)
	if n := d.dials.Load(); n != 2 || served.Load() != 1 {
		t.Fatalf("dials %d, served %d; want 2 dials (one fresh retry) and 1 answer", n, served.Load())
	}
	if took < 600*time.Millisecond || took > 2*time.Second {
		t.Fatalf("answered after %v; want the cut at ≈ 750ms (deadline − reserve), then the retry", took)
	}
	if got := retryCell(SendSiteStage, RetryCauseDial) - before; got != 1 {
		t.Fatalf("dial retries counted %d, want 1", got)
	}
	if got := pagebudget.Outcome(ctx); got != pagebudget.Retried {
		t.Fatalf("outcome %v, want retried", got)
	}
}

// A2b — no retry when the remaining budget does not cover the reserve.
func TestBudget1115_A2b_NoRetryWhenTheReserveDoesNotFit(t *testing.T) {
	srv, _ := okServer(t)
	seedReserve(t, 2*time.Second)
	d := newScriptedDialer(t, srv.Listener.Addr().String(), "refuse", "ok")
	resp, err := Send(SendSiteStage, clientOn(d), getReq(t, budgetLeft(time.Second), srv.URL))
	if resp != nil || err == nil {
		t.Fatalf("a refused connect with too little budget for a retry answered: %v", resp)
	}
	if n := d.dials.Load(); n != 1 {
		t.Fatalf("%d dials, want 1: 1 s left < the 2 s reserve", n)
	}
}

// A2c — the retry is ONE: a connect refused twice is dialled exactly twice,
// where the background path (RetryClient) dials six times.
func TestBudget1115_A2c_RetryIsOnceNotRetryClients(t *testing.T) {
	fastRetries(t, "5")
	srv, _ := okServer(t)
	seedReserve(t, 10*time.Millisecond)
	d := newScriptedDialer(t, srv.Listener.Addr().String(), "refuse")
	resp, err := Send(SendSiteStage, clientOn(d), getReq(t, budgetLeft(5*time.Second), srv.URL))
	if resp != nil || err == nil {
		t.Fatal("refused connects answered")
	}
	if n := d.dials.Load(); n != 2 {
		t.Fatalf("customer: %d dials, want 2 (one retry)", n)
	}
	bg := newScriptedDialer(t, srv.Listener.Addr().String(), "refuse")
	_, _ = Send(SendSiteStage, clientOn(bg), getReq(t, context.Background(), srv.URL))
	if n := bg.dials.Load(); n != 6 {
		t.Fatalf("background control: %d dials, want RetryClient's 6", n)
	}
}

// A2d — the dial is shared: one customer's deadline does not cut the dial
// another customer (with more budget) waits on.
func TestBudget1115_A2d_OneDeadlineDoesNotFailAnotherWaiter(t *testing.T) {
	srv, _ := okServer(t)
	seedReserve(t, 0)
	d := newScriptedDialer(t, srv.Listener.Addr().String(), "slow:900ms")
	prevFn := endpointDialFn
	endpointDialFn = d.DialContext
	t.Cleanup(func() { endpointDialFn = prevFn })
	// One connection per host: the second request waits on the first's dial,
	// as HTTP/2 waiters for one identity do.
	cli := &http.Client{Transport: &http.Transport{DialContext: EndpointDialHolder.Dial, MaxConnsPerHost: 1}}

	short := make(chan error, 1)
	go func() {
		resp, err := Send(SendSiteStage, cli, getReq(t, budgetLeft(300*time.Millisecond), srv.URL))
		if resp != nil {
			resp.Body.Close()
		}
		short <- err
	}()
	time.Sleep(50 * time.Millisecond)
	resp, err := Send(SendSiteStage, cli, getReq(t, budgetLeft(5*time.Second), srv.URL))
	if err != nil {
		t.Fatalf("the waiter with 5 s left failed: %v", err)
	}
	resp.Body.Close()
	if e := <-short; e == nil {
		t.Fatal("the 300 ms request answered before a 900 ms connect")
	}
	if n := d.dials.Load(); n != 1 {
		t.Fatalf("%d dials, want 1: the first request's dial must outlive its deadline and serve the second", n)
	}
}

// A2e (cold) — with no fresh connection measured, the reserve is the page p95
// goal: a hung connect with the whole budget left is cut at 10 s − 3 s and
// retried. Run on a short budget by injecting the budget clock.
func TestBudget1115_A2e_ColdReserveIsTheP95Goal(t *testing.T) {
	freshAnswers.resetForTest()
	if r, measured := RetryReserve(); measured || r != pagebudget.PageP95Goal {
		t.Fatalf("cold reserve %v measured=%v, want PageP95Goal unmeasured", r, measured)
	}
	srv, _ := okServer(t)
	d := newScriptedDialer(t, srv.Listener.Addr().String(), "hang", "ok")
	// 3.4 s left: the cut lands at deadline − 3 s ≈ 400 ms from now.
	ctx := budgetLeft(pagebudget.PageP95Goal + 400*time.Millisecond)
	start := time.Now()
	resp, err := Send(SendSiteStage, clientOn(d), getReq(t, ctx, srv.URL))
	if err != nil {
		t.Fatalf("cold send failed: %v", err)
	}
	resp.Body.Close()
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("answered after %v; the cold cut should land at ≈ 400ms", took)
	}
	if n := d.dials.Load(); n != 2 {
		t.Fatalf("%d dials, want 2", n)
	}
	freshAnswers.resetForTest()
}

// The reserve is data: the goal quantile (p95) of the recent fresh attempts, over
// the window derived from it (20).
func TestBudget1115_ReserveIsTheGoalQuantileOfRecentFreshAttempts(t *testing.T) {
	freshAnswers.resetForTest()
	t.Cleanup(freshAnswers.resetForTest)
	for i := range reserveWindow {
		freshAnswers.record(time.Duration(i+1) * time.Millisecond)
	}
	if r, ok := RetryReserve(); !ok || r != time.Duration(reserveWindow-1)*time.Millisecond || reserveWindow != 20 {
		t.Fatalf("reserve %v (measured %v), want 19ms = p95 of 1..20ms (window %d)", r, ok, reserveWindow)
	}
	// The observer feeds it: a fresh connection's answer is a sample, a reused
	// one is not.
	freshAnswers.resetForTest()
	srv, _ := okServer(t)
	cli := &http.Client{Transport: &http.Transport{}}
	for range 3 {
		resp, err := Send(SendSiteStage, cli, getReq(t, context.Background(), srv.URL))
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	freshAnswers.mu.Lock()
	n := freshAnswers.n
	freshAnswers.mu.Unlock()
	if n != 1 {
		t.Fatalf("%d samples from 3 sends on one kept connection, want 1 (only the fresh one)", n)
	}
}

// A5 — background work: no budget. An answer slower than a would-be budget
// completes; with a budget the same answer ends at it.
func TestBudget1115_A5_BackgroundHasNoExchangeDeadline(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(600 * time.Millisecond)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(slow.Close)
	cli := &http.Client{Transport: &http.Transport{}}
	resp, err := Send(SendSiteStage, cli, getReq(t, context.Background(), slow.URL))
	if err != nil {
		t.Fatalf("background send cut: %v", err)
	}
	resp.Body.Close()
	if pagebudget.Outcome(context.Background()) != pagebudget.None {
		t.Fatal("a background ctx reads a budget")
	}
	seedReserve(t, 50*time.Millisecond)
	_, err = Send(SendSiteStage, cli, getReq(t, budgetLeft(250*time.Millisecond), slow.URL))
	if e := asExhausted(t, err); e.Result != pagebudget.ExhaustedAnswer {
		t.Fatalf("customer result %v, want exhausted_answer", e.Result)
	}
}

// A6 — a write is sent once: written then unanswered → 1 request, ends at the
// budget, may have been applied; refused connect → not retried; spent budget →
// never dialled.
func TestBudget1115_A6_WriteNeverRetried(t *testing.T) {
	seedReserve(t, 10*time.Millisecond)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var got atomic.Int32
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r, err := http.ReadRequest(bufio.NewReader(c))
				if err != nil {
					return
				}
				_, _ = io.ReadAll(r.Body)
				got.Add(1)
				<-release // never answers
			}(c)
		}
	}()
	url := "http://" + ln.Addr().String() + "/x"
	ctx := budgetLeft(400 * time.Millisecond)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, url, strings.NewReader(`{"a":1}`))
	start := time.Now()
	_, err = Send(SendSiteStage, &http.Client{Transport: &http.Transport{}}, req)
	e := asExhausted(t, err)
	if e.Result != pagebudget.ExhaustedAnswer || !e.MayHaveBeenApplied {
		t.Fatalf("written write: %+v, want exhausted_answer, may have been applied", e)
	}
	if time.Since(start) > 2*time.Second || got.Load() != 1 {
		t.Fatalf("took %v, received %d; want ≈ 400ms and exactly 1", time.Since(start), got.Load())
	}

	d := newScriptedDialer(t, ln.Addr().String(), "refuse", "ok")
	req, _ = http.NewRequestWithContext(budgetLeft(5*time.Second), http.MethodPost, url, strings.NewReader(`{}`))
	if _, err := Send(SendSiteStage, clientOn(d), req); err == nil {
		t.Fatal("a refused write succeeded")
	}
	if n := d.dials.Load(); n != 1 {
		t.Fatalf("a write whose connect failed was dialled %d times, want 1", n)
	}

	spent := newScriptedDialer(t, ln.Addr().String(), "ok")
	req, _ = http.NewRequestWithContext(budgetLeft(-time.Millisecond), http.MethodDelete, url, nil)
	_, err = Send(SendSiteStage, clientOn(spent), req)
	if e := asExhausted(t, err); e.Result != pagebudget.NotSent || spent.dials.Load() != 0 {
		t.Fatalf("spent budget: %+v after %d dials, want not_sent and 0", e, spent.dials.Load())
	}
}

// A4 (credhttp) — the /call passthrough answers 504 Timeout, a Failure, with no
// host in it.
func TestBudget1115_A4_CallPassthroughAnswers504(t *testing.T) {
	seedReserve(t, 10*time.Millisecond)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { <-release }))
	t.Cleanup(func() { close(release); srv.Close() })
	st := Do(budgetLeft(300*time.Millisecond), request.RequestOptions{
		RequestInfo: request.RequestInfo{Path: "/api/v1/namespaces/ns/configmaps/x", Verb: ptr.To(http.MethodGet)},
		Endpoint:    &endpoints.Endpoint{ServerURL: srv.URL},
	})
	if st.Code != http.StatusGatewayTimeout || string(st.Reason) != BudgetStatusReason || st.Status != response.StatusFailure {
		t.Fatalf("passthrough answered %d %s %s: %s, want 504 Timeout Failure", st.Code, st.Reason, st.Status, st.Message)
	}
	if strings.Contains(st.Message, "127.0.0.1") {
		t.Fatalf("message names the host: %s", st.Message)
	}
}

// The client-go / Client layer: a round trip ends by the deadline, and one
// started after it is never sent.
func TestBudget1115_BudgetTransportBoundsEveryOtherExchange(t *testing.T) {
	srv, served := okServer(t)
	release := make(chan struct{})
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { <-release }))
	t.Cleanup(func() { close(release); hang.Close() })
	cli := Client(&http.Client{Transport: &http.Transport{}})
	start := time.Now()
	_, err := cli.Do(getReq(t, budgetLeft(300*time.Millisecond), hang.URL))
	if e := asExhausted(t, err); e.Result != pagebudget.ExhaustedAnswer || time.Since(start) > 2*time.Second {
		t.Fatalf("result %v after %v", e.Result, time.Since(start))
	}
	_, err = cli.Do(getReq(t, budgetLeft(-time.Millisecond), srv.URL))
	if e := asExhausted(t, err); e.Result != pagebudget.NotSent || served.Load() != 0 {
		t.Fatalf("spent budget: %v, served %d", e.Result, served.Load())
	}
	resp, err := cli.Do(getReq(t, context.Background(), srv.URL))
	if err != nil {
		t.Fatalf("background through Client: %v", err)
	}
	resp.Body.Close()
}

// A9 — a request on a KEPT connection that went silent (no connect at all, so
// no cut) is still ended by the budget, not by the HTTP/2 health check's
// 30 s + 15 s: the deadline bounds the whole exchange.
func TestBudget1115_A9_DeadKeptConnectionBoundedByTheBudget(t *testing.T) {
	seedReserve(t, 10*time.Millisecond)
	release := make(chan struct{})
	var seen sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, again := seen.LoadOrStore(r.RemoteAddr, true); again {
			<-release // the kept connection goes silent
			return
		}
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(func() { close(release); srv.Close() })
	d := newScriptedDialer(t, srv.Listener.Addr().String(), "ok")
	cli := clientOn(d)
	resp, err := Send(SendSiteStage, cli, getReq(t, context.Background(), srv.URL))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()

	start := time.Now()
	_, err = Send(SendSiteStage, cli, getReq(t, budgetLeft(400*time.Millisecond), srv.URL))
	if e := asExhausted(t, err); e.Result != pagebudget.ExhaustedAnswer {
		t.Fatalf("result %v, want exhausted_answer", e.Result)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("a dead kept connection held the request %v; the budget left 400ms", took)
	}
	if n := d.dials.Load(); n != 1 {
		t.Fatalf("%d dials, want 1: the second request rode the kept connection", n)
	}
}
