package credhttp

// retry_loop_1509.go — the unbudgeted (background) send's retry loop,
// transcribed from plumbing's util.RetryClient.Do (retryPolicyTranscribedFrom,
// http/util/retry.go) so #1509 (a) applies to it too (owner ruling on #1509:
// both paths, one rule).
//
// TRANSCRIBED UNCHANGED: the attempt count (MaxRetries retries after the first
// attempt, read after every attempt, so the #1131 refusal still ends the loop
// by setting it to 0), the network-error and 5xx backoff (exponential from
// BaseBackoff, capped at MaxBackoff, jittered into [d/2, d)), the 429 wait
// (Retry-After seconds or date, else 2 s), each response's body closed before
// its retry, and the errors returned when the retries run out. The policy's
// numbers come from util.NewRetryClient itself (CLIENT_MAX_RETRIES,
// CLIENT_BASE_BACKOFF, CLIENT_MAX_BACKOFF), so they cannot drift.
//
// THE ONE DEVIATION (#1509 a): a 429 from a server that is leaving (its
// connection is closing, leaving_1509.go) is not counted as an attempt and is
// redone at once, on a fresh connection — once per send. The wait for an HTTP/2
// connection's close is the Retry-After wait itself, so no 429 waits longer
// than plumbing's loop would have. Every other 429 waits Retry-After as
// plumbing's does.
//
// NOT TRANSCRIBED, and why it changes nothing: RetryClient.Do first waits on
// its token bucket (CLIENT_QPS 30, CLIENT_BURST 45). Send built a RetryClient
// per send (#920: no client-side limit), so that bucket was always full and
// its Wait returned at once; and only safe methods reach this loop (a write is
// sent once, send.go), where RetryClient's own idempotent-method check also
// let every one through.

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"time"

	"github.com/krateo-platformops/plumbing/http/util"
)

// sendRetrying is RetryClient.Do (see the file comment) sending req on cli,
// with policy's numbers. cli's transport is the attempt observer, which may set
// policy.MaxRetries to 0 (#1131).
func sendRetrying(policy *util.RetryClient, cli *http.Client, req *http.Request) (*http.Response, error) {
	var lastErr error
	var resp *http.Response
	redone := false
	for attempt := 0; attempt <= policy.MaxRetries; attempt++ {
		var mu sync.Mutex
		var conn net.Conn
		ctx := httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
			GotConn: func(i httptrace.GotConnInfo) {
				mu.Lock()
				conn = i.Conn
				mu.Unlock()
			},
		})
		// #nosec G107,G704 -- every request leaves through dialguard's destination guard (#1131); the destination is the caller's by design (TestIssue1131_EverySendPathDialsThroughTheGuard)
		resp, lastErr = cli.Do(req.Clone(ctx))
		if lastErr != nil {
			if attempt < policy.MaxRetries {
				if err := sleepCtx(req.Context(), transcribedBackoff(attempt, policy.BaseBackoff, policy.MaxBackoff)); err != nil {
					return nil, err
				}
				continue
			}
			return nil, lastErr
		}
		if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			return resp, nil
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			wait := retryAfter(resp)
			var leaving <-chan struct{}
			if !redone {
				mu.Lock()
				c := conn
				mu.Unlock()
				closesNow, closed := leavingSignal(resp, c)
				switch {
				case closesNow:
					wait, leaving = 0, closedChan
				case closed != nil:
					leaving = closed
				}
			}
			_ = resp.Body.Close()
			if attempt < policy.MaxRetries || leaving != nil {
				left, err := sleepCtxOrLeaving(req.Context(), wait, leaving)
				if err != nil {
					return nil, err
				}
				if left {
					// #1509 (a): not an attempt. Redone at once, once.
					redone = true
					attempt--
					continue
				}
				if attempt < policy.MaxRetries {
					continue
				}
			}
			return resp, errors.New("too many retries after 429 Too Many Requests")
		}
		// 5xx
		_ = resp.Body.Close()
		if attempt < policy.MaxRetries {
			if err := sleepCtx(req.Context(), transcribedBackoff(attempt, policy.BaseBackoff, policy.MaxBackoff)); err != nil {
				return nil, err
			}
			continue
		}
		return resp, fmt.Errorf("server error after %d retries: %d", policy.MaxRetries, resp.StatusCode)
	}
	return resp, lastErr
}

// transcribedBackoff is RetryClient's backoff(attempt, base, max): base doubled
// per attempt, capped at max, jittered into [d/2, d) (crypto/rand where
// plumbing's is math/rand: the same distribution, see firstBackoff).
func transcribedBackoff(attempt int, base, maxB time.Duration) time.Duration {
	d := min(base*(1<<attempt), maxB)
	if d < 2 {
		return d
	}
	// rand.Int has two failures and neither can happen here. It panics for a bound <= 0, and the
	// bound d/2 is >= 1 because d >= 2. It returns an error only when its reader does, and since
	// Go 1.24 crypto/rand's Reader never returns one (a failure to read random data stops the
	// program instead; nothing in snowplow replaces rand.Reader). So the error is discarded, not
	// handled.
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(d)/2))
	return d/2 + time.Duration(n.Int64())
}

// sleepCtx is RetryClient's sleepWithContext.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sleepCtxOrLeaving is sleepCtx that also ends, with left = true, when
// leaving is closed.
func sleepCtxOrLeaving(ctx context.Context, d time.Duration, leaving <-chan struct{}) (left bool, err error) {
	if leaving == nil {
		return false, sleepCtx(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-leaving:
		return true, nil
	case <-t.C:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}
