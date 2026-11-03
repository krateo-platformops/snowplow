package credhttp

// budget.go — #1115: credhttp runs every outbound exchange of a customer
// request inside the request's page budget (internal/pagebudget).
//
// credhttp builds every client the module uses (#723 guard), so this is the one
// layer that sees every exchange:
//   - Send (the stage fetch and the /call passthrough) applies the budget with
//     its own attempt policy (send.go);
//   - every other exchange — client-go clients (Harden) and http clients
//     (Client) — goes through budgetTransport, which runs each round trip under
//     the deadline, so it ends by it and one started after it is never sent.
//
// It also owns the dialer of the transports snowplow builds itself (the stage
// endpoint clients, #1101): every connect gives up PageMaxGoal after it starts,
// whoever started it, instead of client-go's 30 s. A dial is shared — another
// request may be waiting on it — so it is never cut at one customer's deadline;
// each waiter stops waiting at its own.
//
// And the retry reserve: how long a fresh connection needs to answer, measured
// on the attempts that opened one.

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/krateo-platformops/snowplow/internal/pagebudget"
	"k8s.io/client-go/transport"
)

// --- the exchange deadline --------------------------------------------------

// budgetedExchangeKey marks a request context whose exchange already runs under
// its budget (Send's attempts), so budgetTransport does not apply it twice.
type budgetedExchangeKey struct{}

// BudgetTransport runs every round trip of a budgeted request under the
// request's deadline (see the file comment). Requests without a budget pass
// through unchanged.
func BudgetTransport(rt http.RoundTripper) http.RoundTripper {
	if rt == nil {
		rt = http.DefaultTransport
	}
	if _, ok := rt.(*budgetTransport); ok {
		return rt
	}
	return &budgetTransport{next: rt}
}

type budgetTransport struct{ next http.RoundTripper }

func (t *budgetTransport) WrappedRoundTripper() http.RoundTripper { return t.next }

func (t *budgetTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()
	b, ok := pagebudget.From(ctx)
	if !ok || ctx.Value(budgetedExchangeKey{}) != nil {
		return t.next.RoundTrip(req)
	}
	if !budgetNow().Before(b.Deadline()) {
		if req.Body != nil {
			_ = req.Body.Close()
		}
		b.Note(pagebudget.NotSent)
		return nil, &pagebudget.ExhaustedError{Result: pagebudget.NotSent}
	}
	x, actx := beginBudgetedAttempt(ctx, b)
	resp, err := t.next.RoundTrip(req.WithContext(actx))
	if err != nil {
		if e := x.exhausted(err, 1, "", req.Method); e != nil {
			b.Note(e.Result)
			x.end()
			return nil, e
		}
		x.end()
		return nil, err
	}
	resp.Body = x.bodyEnding(resp.Body)
	return resp, nil
}

// budgetedAttempt is one attempt of a budgeted exchange: its context (the
// budget's deadline, plus an optional connect cut), and what httptrace saw of
// its connection, which decides how a cut is reported.
type budgetedAttempt struct {
	cancelD   context.CancelFunc
	cancelCut context.CancelCauseFunc
	ctx       context.Context
	mu        sync.Mutex
	gotConn   bool
	wrote     bool
	cut       bool
	timer     *time.Timer
}

// errConnectCut is the cause of an attempt Send stopped waiting on because it
// had no connection by the cut point (deadline − reserve).
var errConnectCut = &connectCutError{}

type connectCutError struct{}

func (*connectCutError) Error() string {
	return "connection attempt abandoned to leave the page budget's retry reserve"
}

func beginBudgetedAttempt(ctx context.Context, b *pagebudget.Budget) (*budgetedAttempt, context.Context) {
	x := &budgetedAttempt{}
	dctx, cancelD := context.WithDeadlineCause(ctx, b.Deadline(), pagebudget.ErrExhausted)
	cctx, cancelCut := context.WithCancelCause(dctx)
	x.cancelD, x.cancelCut = cancelD, cancelCut
	x.ctx = httptrace.WithClientTrace(context.WithValue(cctx, budgetedExchangeKey{}, true), &httptrace.ClientTrace{
		GotConn: func(httptrace.GotConnInfo) {
			x.mu.Lock()
			x.gotConn = true
			x.mu.Unlock()
		},
		WroteHeaders: func() {
			x.mu.Lock()
			x.wrote = true
			x.mu.Unlock()
		},
	})
	return x, x.ctx
}

// cutConnectAt stops the attempt's wait at `at` if it has no connection by
// then. The dial itself is not cut (it is shared; the dialer bounds it).
func (x *budgetedAttempt) cutConnectAt(at time.Time) {
	d := at.Sub(budgetNow())
	if d <= 0 {
		return
	}
	x.timer = time.AfterFunc(d, func() {
		x.mu.Lock()
		cut := !x.gotConn
		x.cut = cut
		x.mu.Unlock()
		if cut {
			x.cancelCut(errConnectCut)
		}
	})
}

func (x *budgetedAttempt) stopCut() {
	if x.timer != nil {
		x.timer.Stop()
	}
}

func (x *budgetedAttempt) end() {
	x.stopCut()
	x.cancelCut(nil)
	x.cancelD()
}

func (x *budgetedAttempt) connected() bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.gotConn
}

func (x *budgetedAttempt) wasCut() bool {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.cut
}

// exhausted turns a failed attempt the budget's DEADLINE ended into its
// *ExhaustedError (nil for any other failure, a connect cut included: the cut
// leaves the retry to Send). cause is the attempt's #1101 cause.
func (x *budgetedAttempt) exhausted(err error, attempts int, cause, method string) *pagebudget.ExhaustedError {
	if err == nil || context.Cause(x.ctx) != pagebudget.ErrExhausted {
		return nil
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	e := &pagebudget.ExhaustedError{Result: pagebudget.ExhaustedConnect, Cause: cause, Attempts: attempts}
	if x.gotConn {
		e.Result = pagebudget.ExhaustedAnswer
		e.MayHaveBeenApplied = x.wrote && !SafeMethod(method)
	}
	return e
}

// bodyEnding ends the attempt's context when the body is closed: the deadline
// bounds the body read too, and nothing outlives it.
func (x *budgetedAttempt) bodyEnding(body io.ReadCloser) io.ReadCloser {
	x.stopCut()
	if body == nil {
		x.end()
		return body
	}
	return &endOnClose{ReadCloser: body, end: x.end}
}

type endOnClose struct {
	io.ReadCloser
	once sync.Once
	end  func()
}

func (b *endOnClose) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.end)
	return err
}

// budgetNow is the budget clock (seamed in tests).
var budgetNow = time.Now

// --- the owned dialer ----------------------------------------------------------

// EndpointDialHolder is the dialer of every stage endpoint transport snowplow
// builds (#1101's client-go branch). One holder for all, so the transports stay
// exactly as cacheable as before (each still has its own explicit Proxy).
var EndpointDialHolder = &transport.DialHolder{Dial: endpointDial}

// endpointDialFn is the dialer the bound wraps (seamed in tests). KeepAlive is
// client-go's; the 30 s Timeout is gone: the bound below replaces it.
var endpointDialFn = (&net.Dialer{KeepAlive: 30 * time.Second}).DialContext

// dialNow is the dial clock (seamed in tests).
var dialNow = time.Now

// endpointDial bounds one connect at its start + PageMaxGoal: a connect slower
// than the page max cannot serve any page in time, and the refresh it may serve
// keeps #710's stale-to-push max ≤ 10 s only if it connects within it. The dial
// ctx carries the triggering request's values but not its cancellation (net/http
// detaches dials); that request's own budget is NOT applied here, because other
// requests may be waiting on this connection.
func endpointDial(ctx context.Context, network, addr string) (net.Conn, error) {
	dctx, cancel := context.WithDeadline(ctx, dialNow().Add(pagebudget.PageMaxGoal))
	defer cancel()
	return endpointDialFn(dctx, network, addr)
}

// --- the retry reserve -----------------------------------------------------------

// reserveWindow is how many recent fresh-connection attempts the reserve is
// computed over: enough that its 99th percentile is set by more than one
// sample, few enough to follow a network change.
const reserveWindow = 128

// reserveQuantile: the reserve is the time 99 % of recent fresh attempts
// answered within; robust to one slow outlier.
const reserveQuantile = 0.99

type reserveRing struct {
	mu      sync.Mutex
	samples [reserveWindow]time.Duration
	n       int // samples recorded, capped at reserveWindow
	next    int
	total   atomic.Uint64
}

var freshAnswers = &reserveRing{}

// record adds one fresh-connection attempt's time from asking for a connection
// to its first response byte.
func (r *reserveRing) record(d time.Duration) {
	if d <= 0 {
		return
	}
	r.mu.Lock()
	r.samples[r.next] = d
	r.next = (r.next + 1) % reserveWindow
	if r.n < reserveWindow {
		r.n++
	}
	r.mu.Unlock()
	r.total.Add(1)
}

// reserve is the retry reserve: the p99 of the recent fresh attempts, or — with
// none measured yet — PageP95Goal, the time the goal allows a healthy call to
// answer in. measured reports which.
func (r *reserveRing) reserve() (d time.Duration, measured bool) {
	r.mu.Lock()
	n := r.n
	s := slices.Clone(r.samples[:n])
	r.mu.Unlock()
	if n == 0 {
		return pagebudget.PageP95Goal, false
	}
	slices.Sort(s)
	i := int(float64(n)*reserveQuantile+0.999999) - 1
	i = min(max(i, 0), n-1)
	return s[i], true
}

func (r *reserveRing) resetForTest() {
	r.mu.Lock()
	r.n, r.next = 0, 0
	r.mu.Unlock()
}

// RetryReserve is the current retry reserve and whether it is measured (expvar).
func RetryReserve() (time.Duration, bool) { return freshAnswers.reserve() }

// RetryReserveSamples is how many fresh-connection attempts were ever measured.
func RetryReserveSamples() uint64 { return freshAnswers.total.Load() }
